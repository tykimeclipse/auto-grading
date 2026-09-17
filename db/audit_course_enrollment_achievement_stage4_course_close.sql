-- ============================================================================
-- audit_course_enrollment_achievement_stage4_course_close.sql
--
-- 4단계 SQL 배포 후 강좌 목록·종료 미리보기·종료/재개 RPC와 트리거 보호를 확인한다.
-- SELECT만 수행하며 데이터와 스키마를 변경하지 않는다.
-- 결과 6행의 section / details를 공유한다.
-- ============================================================================

with target_functions as (
  select
    n.nspname as schema_name,
    p.proname as function_name,
    pg_get_function_identity_arguments(p.oid) as identity_arguments,
    p.prosecdef as security_definer,
    md5(p.prosrc) as source_md5,
    p.prosrc,
    exists (
      select 1
      from aclexplode(coalesce(p.proacl, acldefault('f', p.proowner))) acl
      where acl.grantee = 0
        and acl.privilege_type = 'EXECUTE'
    ) as public_can_execute,
    has_function_privilege('anon', p.oid, 'EXECUTE') as anon_can_execute,
    has_function_privilege('authenticated', p.oid, 'EXECUTE') as authenticated_can_execute,
    has_function_privilege('service_role', p.oid, 'EXECUTE') as service_role_can_execute
  from pg_proc p
  join pg_namespace n on n.oid = p.pronamespace
  where n.nspname = 'auto_grading'
    and p.proname in (
      'teacher_list_course_catalog',
      'teacher_get_course_close_preview',
      'teacher_set_course_active',
      'trg_student_courses_require_active_course',
      'trg_attempts_sync_assignment_close_after'
    )
),
course_rollup as (
  select
    c.id as course_id,
    c.course_name,
    c.is_active,
    count(distinct sc.student_id) filter (
      where coalesce(sc.is_active, sc.ended_at is null)
    )::integer as active_student_count,
    count(distinct sc.id) filter (
      where coalesce(sc.is_active, sc.ended_at is null)
    )::integer as active_enrollment_row_count,
    (
      select count(*)::integer
      from auto_grading.assignments a
      where a.course_id = c.id
        and a.closed_at is null
    ) as open_assignment_count,
    (
      select count(*)::integer
      from auto_grading.assignments a
      where a.course_id = c.id
        and a.closed_reason = 'course_closed'
    ) as course_closed_assignment_count,
    (
      select count(*)::integer
      from auto_grading.attempts at
      where at.course_id = c.id
        and at.status = 'in_progress'
    ) as in_progress_count,
    (
      select count(*)::integer
      from auto_grading.attempts at
      where at.course_id = c.id
        and at.status = 'awaiting_retry'
    ) as awaiting_retry_count,
    (
      select count(*)::integer
      from auto_grading.attempts at
      where at.course_id = c.id
        and at.status = 'needs_review'
    ) as needs_review_count
  from auto_grading.courses c
  left join auto_grading.student_courses sc on sc.course_id = c.id
  group by c.id, c.course_name, c.is_active
),
audit_rows as (
  select
    10 as sort_order,
    'teacher_list_course_catalog'::text as section,
    coalesce(
      (
        select jsonb_build_object(
          'exists', true,
          'source_md5', f.source_md5,
          'security_definer', f.security_definer,
          'uses_direct_student_courses', f.prosrc ~ 'auto_grading.student_courses',
          'matches_shared_active_rule',
            f.prosrc ~* 'coalesce\s*\(\s*sc\.is_active\s*,\s*sc\.ended_at\s+is\s+null\s*\)',
          'does_not_depend_on_normalized_view',
            not (f.prosrc ~ 'v_student_courses_normalized'),
          'returns_active_student_count', f.prosrc ~ 'active_student_count',
          'public_can_execute', f.public_can_execute,
          'anon_can_execute', f.anon_can_execute,
          'authenticated_can_execute', f.authenticated_can_execute,
          'service_role_can_execute', f.service_role_can_execute
        )
        from target_functions f
        where f.function_name = 'teacher_list_course_catalog'
          and f.identity_arguments =
            'p_search text, p_only_active boolean, p_limit integer, p_offset integer'
      ),
      jsonb_build_object('exists', false)
    ) as details

  union all

  select
    15,
    'student_course_active_course_guard',
    coalesce(
      (
        select jsonb_build_object(
          'function_exists', true,
          'source_md5', f.source_md5,
          'security_definer', f.security_definer,
          'matches_shared_active_rule',
            f.prosrc ~* 'coalesce\s*\(\s*new\.is_active\s*,\s*new\.ended_at\s+is\s+null\s*\)',
          'locks_course_row', f.prosrc ~* 'for\s+share',
          'rejects_missing_course', f.prosrc ~ 'COURSE_NOT_FOUND',
          'rejects_inactive_course', f.prosrc ~ 'COURSE_INACTIVE',
          'trigger_exists', exists (
            select 1
            from pg_trigger t
            join pg_class cls on cls.oid = t.tgrelid
            join pg_namespace n on n.oid = cls.relnamespace
            where n.nspname = 'auto_grading'
              and cls.relname = 'student_courses'
              and t.tgname = 'trg_student_courses_require_active_course'
              and not t.tgisinternal
              and t.tgenabled <> 'D'
          ),
          'guards_student_id_updates', exists (
            select 1
            from pg_trigger t
            join pg_class cls on cls.oid = t.tgrelid
            join pg_namespace n on n.oid = cls.relnamespace
            where n.nspname = 'auto_grading'
              and cls.relname = 'student_courses'
              and t.tgname = 'trg_student_courses_require_active_course'
              and pg_get_triggerdef(t.oid, true) ~* 'update\s+of[^;]*student_id'
          ),
          'public_can_execute', f.public_can_execute,
          'anon_can_execute', f.anon_can_execute,
          'authenticated_can_execute', f.authenticated_can_execute,
          'service_role_can_execute', f.service_role_can_execute
        )
        from target_functions f
        where f.function_name = 'trg_student_courses_require_active_course'
          and f.identity_arguments = ''
      ),
      jsonb_build_object('function_exists', false, 'trigger_exists', false)
    )

  union all

  select
    20,
    'teacher_get_course_close_preview',
    coalesce(
      (
        select jsonb_build_object(
          'exists', true,
          'source_md5', f.source_md5,
          'security_definer', f.security_definer,
          'has_assert_admin', f.prosrc ~* 'assert_admin\s*\(',
          'uses_direct_student_courses', f.prosrc ~ 'auto_grading.student_courses',
          'matches_shared_active_rule',
            f.prosrc ~* 'coalesce\s*\(\s*s\.is_active\s*,\s*s\.ended_at\s+is\s+null\s*\)',
          'counts_open_assignments', f.prosrc ~ 'open_assignment_count',
          'counts_in_progress', f.prosrc ~ 'in_progress_count',
          'counts_awaiting_retry', f.prosrc ~ 'awaiting_retry_count',
          'contains_no_update', not (f.prosrc ~* '\mupdate\M'),
          'public_can_execute', f.public_can_execute,
          'anon_can_execute', f.anon_can_execute,
          'authenticated_can_execute', f.authenticated_can_execute,
          'service_role_can_execute', f.service_role_can_execute
        )
        from target_functions f
        where f.function_name = 'teacher_get_course_close_preview'
          and f.identity_arguments = 'p_course_ids uuid[]'
      ),
      jsonb_build_object('exists', false)
    )

  union all

  select
    30,
    'teacher_set_course_active',
    coalesce(
      (
        select jsonb_build_object(
          'exists', true,
          'source_md5', f.source_md5,
          'security_definer', f.security_definer,
          'has_assert_admin', f.prosrc ~* 'assert_admin\s*\(',
          'locks_course_rows', f.prosrc ~* 'for\s+update',
          'updates_courses', f.prosrc ~* 'update\s+auto_grading\.courses',
          'ends_active_enrollments', f.prosrc ~* 'update\s+auto_grading\.student_courses',
          'matches_shared_active_rule',
            f.prosrc ~* 'coalesce\s*\(\s*sc\.is_active\s*,\s*sc\.ended_at\s+is\s+null\s*\)',
          'closes_open_assignments', f.prosrc ~* 'update\s+auto_grading\.assignments',
          'uses_course_closed_reason', f.prosrc ~ 'course_closed',
          'does_not_update_attempts',
            not (f.prosrc ~* 'update\s+auto_grading\.attempts'),
          'does_not_auto_reactivate_enrollments',
            f.prosrc ~ '''reactivated_enrollment_count'', 0',
          'does_not_auto_reopen_assignments',
            f.prosrc ~ '''reopened_assignment_count'', 0',
          'public_can_execute', f.public_can_execute,
          'anon_can_execute', f.anon_can_execute,
          'authenticated_can_execute', f.authenticated_can_execute,
          'service_role_can_execute', f.service_role_can_execute
        )
        from target_functions f
        where f.function_name = 'teacher_set_course_active'
          and f.identity_arguments = 'p_course_ids uuid[], p_is_active boolean'
      ),
      jsonb_build_object('exists', false)
    )

  union all

  select
    40,
    'assignment_close_trigger_guard',
    coalesce(
      (
        select jsonb_build_object(
          'exists', true,
          'source_md5', f.source_md5,
          'preserves_non_auto_managed_close',
            f.prosrc ~* 'v_current_closed_at\s+is\s+not\s+null'
            and f.prosrc ~* 'not\s+v_current_is_auto_managed',
          'course_closed_is_not_auto_managed',
            not (f.prosrc ~* 'v_current_is_auto_managed\s*:=.*course_closed')
        )
        from target_functions f
        where f.function_name = 'trg_attempts_sync_assignment_close_after'
          and f.identity_arguments = ''
      ),
      jsonb_build_object('exists', false)
    )

  union all

  select
    50,
    'course_close_operational_rollup',
    coalesce(
      (
        select jsonb_agg(to_jsonb(x) order by x.is_active desc, x.course_name, x.course_id)
        from course_rollup x
      ),
      '[]'::jsonb
    )
)
select section, details
from audit_rows
order by sort_order;

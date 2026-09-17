-- ============================================================================
-- audit_course_enrollment_achievement_stage5_student_course_management.sql
--
-- 5단계 SQL 배포 후 학생 수강 조회·연결·종료 경로와 운영 무결성을 확인한다.
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
      'teacher_list_students_for_management',
      'teacher_get_student_detail',
      'teacher_attach_student_to_course',
      'teacher_deactivate_student_course',
      'trg_student_courses_require_active_course'
    )
),
student_course_rows as (
  select
    sc.id as student_course_id,
    sc.student_id,
    s.student_code,
    s.name as student_name,
    sc.course_id,
    c.course_name,
    c.is_active as course_is_active,
    coalesce(sc.is_active, sc.ended_at is null) as enrollment_is_active,
    sc.joined_at,
    sc.ended_at
  from auto_grading.student_courses sc
  join auto_grading.students s on s.id = sc.student_id
  join auto_grading.courses c on c.id = sc.course_id
),
student_rollup as (
  select
    s.id as student_id,
    s.student_code,
    s.name as student_name,
    count(scr.student_course_id)::integer as enrollment_history_count,
    count(scr.student_course_id) filter (
      where scr.enrollment_is_active and scr.course_is_active
    )::integer as active_course_count,
    coalesce(
      jsonb_agg(
        jsonb_build_object(
          'course_id', scr.course_id,
          'course_name', scr.course_name,
          'course_is_active', scr.course_is_active,
          'enrollment_is_active', scr.enrollment_is_active,
          'joined_at', scr.joined_at,
          'ended_at', scr.ended_at
        )
        order by scr.enrollment_is_active desc, scr.joined_at desc nulls last
      ) filter (where scr.student_course_id is not null),
      '[]'::jsonb
    ) as enrollments
  from auto_grading.students s
  left join student_course_rows scr on scr.student_id = s.id
  group by s.id, s.student_code, s.name
),
audit_rows as (
  select
    10 as sort_order,
    'teacher_list_students_for_management'::text as section,
    coalesce(
      (
        select jsonb_build_object(
          'exists', true,
          'source_md5', f.source_md5,
          'security_definer', f.security_definer,
          'has_assert_admin', f.prosrc ~* 'assert_admin\s*\(',
          'uses_direct_student_courses', f.prosrc ~ 'auto_grading.student_courses',
          'matches_shared_active_rule',
            f.prosrc ~* 'coalesce\s*\(\s*sc\.is_active\s*,\s*sc\.ended_at\s+is\s+null\s*\)',
          'excludes_inactive_courses', f.prosrc ~* 'and\s+c\.is_active',
          'public_can_execute', f.public_can_execute,
          'anon_can_execute', f.anon_can_execute,
          'authenticated_can_execute', f.authenticated_can_execute,
          'service_role_can_execute', f.service_role_can_execute
        )
        from target_functions f
        where f.function_name = 'teacher_list_students_for_management'
          and f.identity_arguments = ''
      ),
      jsonb_build_object('exists', false)
    ) as details

  union all

  select
    20,
    'teacher_get_student_detail',
    coalesce(
      (
        select jsonb_build_object(
          'exists', true,
          'source_md5', f.source_md5,
          'security_definer', f.security_definer,
          'has_assert_admin', f.prosrc ~* 'assert_admin\s*\(',
          'returns_student_course_id',
            f.prosrc ~* '''student_course_id''\s*,\s*sc\.id',
          'returns_course_id', f.prosrc ~* '''course_id''\s*,\s*c\.id',
          'returns_course_active_state', f.prosrc ~ '''course_is_active''',
          'returns_joined_at', f.prosrc ~ '''joined_at''',
          'returns_ended_at', f.prosrc ~ '''ended_at''',
          'matches_shared_active_rule',
            f.prosrc ~* 'coalesce\s*\(\s*sc\.is_active\s*,\s*sc\.ended_at\s+is\s+null\s*\)',
          'public_can_execute', f.public_can_execute,
          'anon_can_execute', f.anon_can_execute,
          'authenticated_can_execute', f.authenticated_can_execute,
          'service_role_can_execute', f.service_role_can_execute
        )
        from target_functions f
        where f.function_name = 'teacher_get_student_detail'
          and f.identity_arguments = 'p_student_id uuid'
      ),
      jsonb_build_object('exists', false)
    )

  union all

  select
    30,
    'teacher_attach_student_to_course',
    coalesce(
      (
        select jsonb_build_object(
          'exists', true,
          'source_md5', f.source_md5,
          'security_definer', f.security_definer,
          'has_assert_admin', f.prosrc ~* 'assert_admin\s*\(',
          'uses_advisory_lock', f.prosrc ~ 'pg_advisory_xact_lock',
          'inserts_new_history', f.prosrc ~* 'insert\s+into\s+auto_grading\.student_courses',
          'public_can_execute', f.public_can_execute,
          'anon_can_execute', f.anon_can_execute,
          'authenticated_can_execute', f.authenticated_can_execute,
          'service_role_can_execute', f.service_role_can_execute
        )
        from target_functions f
        where f.function_name = 'teacher_attach_student_to_course'
          and f.identity_arguments =
            'p_student_id uuid, p_course_id uuid, p_service_type text'
      ),
      jsonb_build_object('exists', false)
    )

  union all

  select
    40,
    'teacher_deactivate_student_course',
    coalesce(
      (
        select jsonb_build_object(
          'exists', true,
          'source_md5', f.source_md5,
          'security_definer', f.security_definer,
          'has_assert_admin', f.prosrc ~* 'assert_admin\s*\(',
          'uses_advisory_lock', f.prosrc ~ 'pg_advisory_xact_lock',
          'ends_active_history',
            f.prosrc ~* 'is_active\s*=\s*false'
            and f.prosrc ~* 'ended_at\s*=\s*coalesce',
          'public_can_execute', f.public_can_execute,
          'anon_can_execute', f.anon_can_execute,
          'authenticated_can_execute', f.authenticated_can_execute,
          'service_role_can_execute', f.service_role_can_execute
        )
        from target_functions f
        where f.function_name = 'teacher_deactivate_student_course'
          and f.identity_arguments = 'p_student_id uuid, p_course_id uuid'
      ),
      jsonb_build_object('exists', false)
    )

  union all

  select
    50,
    'student_course_active_course_guard',
    coalesce(
      (
        select jsonb_build_object(
          'function_exists', true,
          'source_md5', f.source_md5,
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
          'locks_course_row', f.prosrc ~* 'for\s+share',
          'rejects_inactive_course', f.prosrc ~ 'COURSE_INACTIVE'
        )
        from target_functions f
        where f.function_name = 'trg_student_courses_require_active_course'
          and f.identity_arguments = ''
      ),
      jsonb_build_object('function_exists', false, 'trigger_exists', false)
    )

  union all

  select
    60,
    'student_course_operational_integrity',
    jsonb_build_object(
      'total_enrollment_rows', (select count(*) from student_course_rows),
      'active_enrollment_rows', (
        select count(*) from student_course_rows where enrollment_is_active
      ),
      'active_enrollment_on_inactive_course', (
        select count(*)
        from student_course_rows
        where enrollment_is_active and not course_is_active
      ),
      'active_with_ended_at', (
        select count(*)
        from student_course_rows
        where enrollment_is_active and ended_at is not null
      ),
      'duplicate_active_student_course_pairs', (
        select count(*)
        from (
          select scr.student_id, scr.course_id
          from student_course_rows scr
          where scr.enrollment_is_active
          group by scr.student_id, scr.course_id
          having count(*) > 1
        ) duplicate_pairs
      ),
      'students', coalesce(
        (
          select jsonb_agg(to_jsonb(x) order by x.student_code, x.student_name)
          from student_rollup x
        ),
        '[]'::jsonb
      )
    )
)
select section, details
from audit_rows
order by sort_order;

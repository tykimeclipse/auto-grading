-- ============================================================================
-- audit_course_enrollment_achievement_stage3_write_paths.sql
--
-- 3단계 SQL 배포 후 쓰기 경로와 권한을 확인한다.
-- SELECT만 수행하며 데이터와 스키마를 변경하지 않는다.
-- 결과 5행의 section / details를 공유한다.
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
  where (
      n.nspname = 'auto_grading'
      and p.proname in (
        'teacher_issue_assignments',
        'start_attempt',
        'start_attempt_by_test_set',
        'teacher_upsert_manual_score'
      )
    )
    or (
      n.nspname = 'public'
      and p.proname = 'start_attempt_by_test_set'
    )
),
audit_rows as (
  select
    10 as sort_order,
    'teacher_issue_assignments'::text as section,
    coalesce(
      (
        select jsonb_build_object(
          'exists', true,
          'source_md5', f.source_md5,
          'security_definer', f.security_definer,
          'requires_course', f.prosrc ~ 'COURSE_REQUIRED',
          'rejects_inactive_course', f.prosrc ~ 'COURSE_INACTIVE',
          'checks_active_enrollment', f.prosrc ~ 'v_student_courses_normalized',
          'skips_other_course', f.prosrc ~ 'skipped_other_course',
          'prevents_reopen_retag', not (f.prosrc ~* 'set\s+course_id\s*=\s*coalesce'),
          'public_can_execute', f.public_can_execute,
          'anon_can_execute', f.anon_can_execute,
          'authenticated_can_execute', f.authenticated_can_execute,
          'service_role_can_execute', f.service_role_can_execute
        )
        from target_functions f
        where f.schema_name = 'auto_grading'
          and f.function_name = 'teacher_issue_assignments'
          and f.identity_arguments =
            'p_test_set_id uuid, p_student_ids uuid[], p_course_id uuid, p_purpose text, p_reopen_existing boolean'
      ),
      jsonb_build_object('exists', false)
    ) as details

  union all

  select
    20,
    'start_attempt',
    coalesce(
      (
        select jsonb_build_object(
          'exists', true,
          'source_md5', f.source_md5,
          'security_definer', f.security_definer,
          'stores_attempt_course_id',
            f.prosrc ~* 'insert\s+into\s+auto_grading\.attempts\s*\([^)]*course_id',
          'rejects_closed_assignment_for_new_attempt', f.prosrc ~ 'ASSIGNMENT_CLOSED',
          'requires_course_for_new_attempt', f.prosrc ~ 'COURSE_REQUIRED',
          'rejects_inactive_course_for_new_attempt', f.prosrc ~ 'COURSE_INACTIVE',
          'checks_active_enrollment_for_new_attempt', f.prosrc ~ 'STUDENT_NOT_ENROLLED_IN_COURSE',
          'returns_course_id', f.prosrc ~* '''course_id''\s*,\s*v_attempt\.course_id',
          'anon_can_execute', f.anon_can_execute,
          'authenticated_can_execute', f.authenticated_can_execute,
          'service_role_can_execute', f.service_role_can_execute
        )
        from target_functions f
        where f.schema_name = 'auto_grading'
          and f.function_name = 'start_attempt'
          and f.identity_arguments = 'p_assignment_id uuid, p_student_code text'
      ),
      jsonb_build_object('exists', false)
    )

  union all

  select
    30,
    'start_attempt_by_test_set',
    coalesce(
      (
        select jsonb_build_object(
          'exists', true,
          'source_md5', f.source_md5,
          'security_definer', f.security_definer,
          'resolves_one_active_course',
            f.prosrc ~ 'ACTIVE_COURSE_NOT_FOUND'
            and f.prosrc ~ 'MULTIPLE_ACTIVE_COURSES',
          'stores_assignment_course_id',
            f.prosrc ~* 'insert\s+into\s+auto_grading\.assignments\s*\([^)]*course_id',
          'reuses_existing_before_active_course_resolution',
            strpos(f.prosrc, 'if v_assignment_id is not null')
              < strpos(f.prosrc, 'count(distinct v.course_id)'),
          'public_can_execute', f.public_can_execute,
          'anon_can_execute', f.anon_can_execute,
          'authenticated_can_execute', f.authenticated_can_execute,
          'service_role_can_execute', f.service_role_can_execute
        )
        from target_functions f
        where f.schema_name = 'auto_grading'
          and f.function_name = 'start_attempt_by_test_set'
          and f.identity_arguments = 'p_test_set_id uuid, p_student_code text'
      ),
      jsonb_build_object('exists', false)
    )

  union all

  select
    40,
    'teacher_upsert_manual_score',
    coalesce(
      (
        select jsonb_build_object(
          'exists', true,
          'source_md5', f.source_md5,
          'security_definer', f.security_definer,
          'requires_course', f.prosrc ~ 'COURSE_REQUIRED',
          'rejects_inactive_course', f.prosrc ~ 'COURSE_INACTIVE',
          'checks_active_enrollment', f.prosrc ~ 'STUDENT_NOT_ENROLLED_IN_COURSE',
          'prevents_assignment_retag', f.prosrc ~ 'ASSIGNMENT_OTHER_COURSE',
          'prevents_attempt_retag', f.prosrc ~ 'ATTEMPT_OTHER_COURSE',
          'stores_attempt_course_id',
            f.prosrc ~* 'insert\s+into\s+auto_grading\.attempts\s*\([^)]*course_id',
          'public_can_execute', f.public_can_execute,
          'anon_can_execute', f.anon_can_execute,
          'authenticated_can_execute', f.authenticated_can_execute,
          'service_role_can_execute', f.service_role_can_execute
        )
        from target_functions f
        where f.schema_name = 'auto_grading'
          and f.function_name = 'teacher_upsert_manual_score'
          and f.identity_arguments =
            'p_test_set_id uuid, p_student_id uuid, p_first_correct_count integer, p_teacher_final_correct_count integer, p_event_date date, p_note text, p_course_id uuid'
      ),
      jsonb_build_object('exists', false)
    )

  union all

  select
    50,
    'public_start_attempt_by_test_set_wrapper',
    coalesce(
      (
        select jsonb_build_object(
          'exists', true,
          'source_md5', f.source_md5,
          'security_definer', f.security_definer,
          'calls_auto_grading_core', f.prosrc ~ 'auto_grading.start_attempt_by_test_set',
          'public_can_execute', f.public_can_execute,
          'anon_can_execute', f.anon_can_execute,
          'authenticated_can_execute', f.authenticated_can_execute,
          'service_role_can_execute', f.service_role_can_execute
        )
        from target_functions f
        where f.schema_name = 'public'
          and f.function_name = 'start_attempt_by_test_set'
          and f.identity_arguments = 'p_test_set_id uuid, p_student_code text'
      ),
      jsonb_build_object('exists', false)
    )
)
select section, details
from audit_rows
order by sort_order;

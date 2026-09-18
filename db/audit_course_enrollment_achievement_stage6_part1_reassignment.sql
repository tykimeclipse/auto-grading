-- ============================================================================
-- audit_course_enrollment_achievement_stage6_part1_reassignment.sql
--
-- 6단계 part 1 배포 후 attempt 귀속 트리거·재귀속 RPC·감사 경로를 확인한다.
-- SELECT만 수행하며 데이터와 스키마를 변경하지 않는다.
-- 결과 4행의 section / details를 공유한다.
-- ============================================================================

with target_functions as (
  select
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
    has_function_privilege('authenticated', p.oid, 'EXECUTE')
      as authenticated_can_execute,
    has_function_privilege('service_role', p.oid, 'EXECUTE')
      as service_role_can_execute
  from pg_proc p
  join pg_namespace n on n.oid = p.pronamespace
  where n.nspname = 'auto_grading'
    and p.proname in (
      'trg_attempts_require_assignment_attribution',
      'teacher_reassign_assignment_course',
      'teacher_list_assignment_course_reassignments'
    )
),
attempt_attribution as (
  select
    at.id as attempt_id,
    at.assignment_id,
    at.student_id as attempt_student_id,
    at.test_set_id as attempt_test_set_id,
    at.course_id as attempt_course_id,
    at.status,
    a.id as matched_assignment_id,
    a.student_id as assignment_student_id,
    a.test_set_id as assignment_test_set_id,
    a.course_id as assignment_course_id
  from auto_grading.attempts at
  left join auto_grading.assignments a on a.id = at.assignment_id
),
audit_rows as (
  select
    10 as sort_order,
    'attempt_assignment_attribution_guard'::text as section,
    coalesce(
      (
        select jsonb_build_object(
          'function_exists', true,
          'source_md5', f.source_md5,
          'security_definer', f.security_definer,
          'locks_assignment_row', f.prosrc ~* 'for\s+share',
          'checks_student', f.prosrc ~ 'ATTEMPT_STUDENT_MISMATCH',
          'checks_test_set', f.prosrc ~ 'ATTEMPT_TEST_SET_MISMATCH',
          'checks_course', f.prosrc ~ 'ATTEMPT_COURSE_MISMATCH',
          'trigger_exists', exists (
            select 1
            from pg_trigger t
            join pg_class cls on cls.oid = t.tgrelid
            join pg_namespace n on n.oid = cls.relnamespace
            where n.nspname = 'auto_grading'
              and cls.relname = 'attempts'
              and t.tgname = 'trg_attempts_require_assignment_attribution'
              and not t.tgisinternal
              and t.tgenabled <> 'D'
          ),
          'public_can_execute', f.public_can_execute,
          'anon_can_execute', f.anon_can_execute,
          'authenticated_can_execute', f.authenticated_can_execute,
          'service_role_can_execute', f.service_role_can_execute
        )
        from target_functions f
        where f.function_name = 'trg_attempts_require_assignment_attribution'
          and f.identity_arguments = ''
      ),
      jsonb_build_object('function_exists', false, 'trigger_exists', false)
    ) as details

  union all

  select
    20,
    'teacher_reassign_assignment_course',
    coalesce(
      (
        select jsonb_build_object(
          'exists', true,
          'source_md5', f.source_md5,
          'security_definer', f.security_definer,
          'has_assert_admin', f.prosrc ~* 'assert_admin\s*\(',
          'uses_advisory_lock', f.prosrc ~ 'pg_advisory_xact_lock',
          'requires_reason', f.prosrc ~ 'REASSIGN_REASON_REQUIRED',
          'requires_student_course_history',
            f.prosrc ~ 'TARGET_COURSE_NOT_IN_STUDENT_HISTORY',
          'blocks_active_attempts',
            f.prosrc ~ 'ACTIVE_ATTEMPT_REASSIGNMENT_BLOCKED',
          'updates_assignment',
            f.prosrc ~* 'update\s+auto_grading\.assignments',
          'updates_attempts', f.prosrc ~* 'update\s+auto_grading\.attempts',
          'writes_audit',
            f.prosrc ~* 'insert\s+into\s+auto_grading\.assignment_course_reassignments',
          'public_can_execute', f.public_can_execute,
          'anon_can_execute', f.anon_can_execute,
          'authenticated_can_execute', f.authenticated_can_execute,
          'service_role_can_execute', f.service_role_can_execute
        )
        from target_functions f
        where f.function_name = 'teacher_reassign_assignment_course'
          and f.identity_arguments =
            'p_assignment_id uuid, p_target_course_id uuid, p_reason text'
      ),
      jsonb_build_object('exists', false)
    )

  union all

  select
    30,
    'reassignment_audit_read_path',
    jsonb_build_object(
      'audit_table_exists', to_regclass(
        'auto_grading.assignment_course_reassignments'
      ) is not null,
      'audit_table_rls_enabled', coalesce((
        select cls.relrowsecurity
        from pg_class cls
        join pg_namespace n on n.oid = cls.relnamespace
        where n.nspname = 'auto_grading'
          and cls.relname = 'assignment_course_reassignments'
      ), false),
      'service_role_bypasses_rls', (
        select r.rolbypassrls
        from pg_roles r
        where r.rolname = 'service_role'
      ),
      'authenticated_bypasses_rls', (
        select r.rolbypassrls
        from pg_roles r
        where r.rolname = 'authenticated'
      ),
      'anon_can_select_audit', case
        when to_regclass('auto_grading.assignment_course_reassignments') is null
          then null
        else has_table_privilege(
          'anon',
          'auto_grading.assignment_course_reassignments',
          'SELECT'
        )
      end,
      'authenticated_can_select_audit', case
        when to_regclass('auto_grading.assignment_course_reassignments') is null
          then null
        else has_table_privilege(
          'authenticated',
          'auto_grading.assignment_course_reassignments',
          'SELECT'
        )
      end,
      'service_role_can_select_audit', case
        when to_regclass('auto_grading.assignment_course_reassignments') is null
          then null
        else has_table_privilege(
          'service_role',
          'auto_grading.assignment_course_reassignments',
          'SELECT'
        )
      end,
      'read_rpc', coalesce(
        (
          select jsonb_build_object(
            'exists', true,
            'source_md5', f.source_md5,
            'security_definer', f.security_definer,
            'has_assert_admin', f.prosrc ~* 'assert_admin\s*\(',
            'filters_student', f.prosrc ~ 'p_student_id is null',
            'filters_assignment', f.prosrc ~ 'p_assignment_id is null',
            'caps_limit_at_500', f.prosrc ~ '500',
            'public_can_execute', f.public_can_execute,
            'anon_can_execute', f.anon_can_execute,
            'authenticated_can_execute', f.authenticated_can_execute,
            'service_role_can_execute', f.service_role_can_execute
          )
          from target_functions f
          where f.function_name =
              'teacher_list_assignment_course_reassignments'
            and f.identity_arguments =
              'p_student_id uuid, p_assignment_id uuid, p_limit integer'
        ),
        jsonb_build_object('exists', false)
      ),
      'audit_row_count', case
        when to_regclass('auto_grading.assignment_course_reassignments') is null
          then null
        else (
          select count(*)
          from auto_grading.assignment_course_reassignments
        )
      end
    )

  union all

  select
    40,
    'reassignment_operational_integrity',
    jsonb_build_object(
      'attempt_without_assignment', count(*) filter (
        where aa.matched_assignment_id is null
      ),
      'student_mismatch', count(*) filter (
        where aa.matched_assignment_id is not null
          and aa.attempt_student_id is distinct from aa.assignment_student_id
      ),
      'test_set_mismatch', count(*) filter (
        where aa.matched_assignment_id is not null
          and aa.attempt_test_set_id is distinct from aa.assignment_test_set_id
      ),
      'course_mismatch', count(*) filter (
        where aa.matched_assignment_id is not null
          and aa.attempt_course_id is distinct from aa.assignment_course_id
      ),
      'active_attempt_count', count(*) filter (
        where aa.status in ('in_progress', 'awaiting_retry')
      )
    )
  from attempt_attribution aa
)
select section, details
from audit_rows
order by sort_order;

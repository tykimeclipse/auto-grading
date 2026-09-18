-- ============================================================================
-- audit_course_enrollment_achievement_stage6_part2b_postcutover.sql
--
-- 6단계 part 2B 배포 후 기존 조회 함수의 코어 전환과 호출 계약을 확인한다.
-- SELECT만 수행하며 데이터와 스키마를 변경하지 않는다.
-- 결과 4행의 section / details를 공유한다.
-- ============================================================================

with target_functions as (
  select
    p.proname as function_name,
    pg_get_function_identity_arguments(p.oid) as identity_arguments,
    pg_get_function_result(p.oid) as result_type,
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
      'get_student_stats_by_code',
      'get_student_assignment_history_by_code',
      'get_student_stats_by_token',
      'get_student_assignment_history_by_token',
      '_student_achievement_stats_core',
      '_student_achievement_history_core'
    )
),
active_students as (
  select s.id as student_id, s.student_code, s.name as student_name
  from auto_grading.students s
  where s.is_active = true
),
stats_postcutover as (
  select
    s.student_code,
    auto_grading.get_student_stats_by_code(s.student_code) as legacy_result,
    auto_grading._student_achievement_stats_core(
      s.student_id,
      'all',
      null
    ) -> 'stats' as core_result
  from active_students s
),
history_postcutover as (
  select
    s.student_code,
    (
      select coalesce(
        jsonb_agg(
          to_jsonb(h)
          order by h.last_activity_at desc, h.assignment_id desc
        ),
        '[]'::jsonb
      )
      from auto_grading.get_student_assignment_history_by_code(
        s.student_code,
        200
      ) h
    ) as legacy_result,
    (
      select coalesce(
        jsonb_agg(
          to_jsonb(h)
          order by h.last_activity_at desc, h.assignment_id desc
        ),
        '[]'::jsonb
      )
      from auto_grading._student_achievement_history_core(
        s.student_id,
        'all',
        null,
        200
      ) h
    ) as core_result
  from active_students s
),
audit_rows as (
  select
    10 as sort_order,
    'legacy_reader_core_cutover'::text as section,
    jsonb_build_object(
      'functions', coalesce((
        select jsonb_agg(
          jsonb_build_object(
            'function_name', f.function_name,
            'identity_arguments', f.identity_arguments,
            'result_type', f.result_type,
            'source_md5', f.source_md5,
            'security_definer', f.security_definer,
            'calls_stats_core',
              f.prosrc ~ '_student_achievement_stats_core',
            'calls_history_core',
              f.prosrc ~ '_student_achievement_history_core',
            'uses_all_scope', f.prosrc ~ '''all''',
            'public_can_execute', f.public_can_execute,
            'anon_can_execute', f.anon_can_execute,
            'authenticated_can_execute', f.authenticated_can_execute,
            'service_role_can_execute', f.service_role_can_execute
          )
          order by f.function_name
        )
        from target_functions f
        where f.function_name in (
          'get_student_stats_by_code',
          'get_student_assignment_history_by_code'
        )
      ), '[]'::jsonb),
      'expected_function_count', 2
    ) as details

  union all

  select
    20,
    'postcutover_stats_equivalence',
    jsonb_build_object(
      'compared_student_count', count(*),
      'mismatch_count', count(*) filter (
        where x.legacy_result is distinct from x.core_result
      ),
      'mismatch_student_codes', coalesce(
        jsonb_agg(x.student_code order by x.student_code) filter (
          where x.legacy_result is distinct from x.core_result
        ),
        '[]'::jsonb
      )
    )
  from stats_postcutover x

  union all

  select
    30,
    'postcutover_history_equivalence',
    jsonb_build_object(
      'compared_student_count', count(*),
      'mismatch_count', count(*) filter (
        where x.legacy_result is distinct from x.core_result
      ),
      'mismatch_student_codes', coalesce(
        jsonb_agg(x.student_code order by x.student_code) filter (
          where x.legacy_result is distinct from x.core_result
        ),
        '[]'::jsonb
      )
    )
  from history_postcutover x

  union all

  select
    40,
    'existing_public_token_wrappers',
    jsonb_build_object(
      'functions', coalesce((
        select jsonb_agg(
          jsonb_build_object(
            'function_name', f.function_name,
            'identity_arguments', f.identity_arguments,
            'result_type', f.result_type,
            'source_md5', f.source_md5,
            'security_definer', f.security_definer,
            'calls_legacy_reader',
              f.prosrc ~ 'get_student_stats_by_code'
              or f.prosrc ~ 'get_student_assignment_history_by_code',
            'validates_public_token', f.prosrc ~ 'student_public_links',
            'anon_can_execute', f.anon_can_execute,
            'authenticated_can_execute', f.authenticated_can_execute,
            'service_role_can_execute', f.service_role_can_execute
          )
          order by f.function_name
        )
        from target_functions f
        where f.function_name in (
          'get_student_stats_by_token',
          'get_student_assignment_history_by_token'
        )
      ), '[]'::jsonb),
      'expected_function_count', 2,
      'manual_followup',
        'SQL Editor에서 get_student_stats_by_code(''102'') / get_student_assignment_history_by_code(''102'', 200) 절대값을 확인한다. 현재 두 함수는 저장소 내 UI 호출자가 없어 화면 검증 경로가 없다.'
    )
)
select section, details
from audit_rows
order by sort_order;

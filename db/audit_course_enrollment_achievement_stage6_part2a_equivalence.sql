-- ============================================================================
-- audit_course_enrollment_achievement_stage6_part2a_equivalence.sql
--
-- 6단계 part 2A 배포 후 신규 코어/RPC와 기존 전체 누적 조회의 동등성을 검증한다.
-- SELECT만 수행하며 데이터와 스키마를 변경하지 않는다.
--
-- part 2B 진행 조건:
--   - old_new_stats_equivalence.mismatch_count = 0
--   - old_new_history_equivalence.mismatch_count = 0
--   - stats_scope_partition_integrity.mismatch_count = 0
--
-- 결과 7행의 section / details를 공유한다.
-- ============================================================================

with target_functions as (
  select
    p.proname as function_name,
    pg_get_function_identity_arguments(p.oid) as identity_arguments,
    pg_get_function_arguments(p.oid) as arguments_with_defaults,
    pg_get_function_result(p.oid) as result_type,
    p.pronargdefaults as default_argument_count,
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
      '_student_achievement_stats_core',
      '_student_achievement_history_core',
      'get_student_achievement_courses_by_code',
      'get_student_achievement_by_code',
      'get_student_achievement_courses_by_token',
      'get_student_achievement_by_token',
      'get_student_stats_by_code',
      'get_student_assignment_history_by_code'
    )
),
active_students as (
  -- 기존 stats 함수가 허용하는 정확한 도메인과 맞춘다.
  select s.id as student_id, s.student_code, s.name as student_name
  from auto_grading.students s
  where s.is_active = true
),
stats_comparison_base as (
  select
    s.student_code,
    s.student_name,
    auto_grading.get_student_stats_by_code(s.student_code) as old_stats,
    auto_grading.get_student_achievement_by_code(
      s.student_code,
      'all',
      null,
      200
    ) -> 'stats' as new_stats
  from active_students s
),
stats_mismatches as (
  select *
  from stats_comparison_base x
  where x.old_stats is distinct from x.new_stats
),
history_comparison_base as (
  select
    s.student_code,
    s.student_name,
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
    ) as old_history,
    auto_grading.get_student_achievement_by_code(
      s.student_code,
      'all',
      null,
      200
    ) -> 'history' as new_history
  from active_students s
),
history_mismatches as (
  select *
  from history_comparison_base x
  where x.old_history is distinct from x.new_history
),
partition_scopes as (
  -- 미지정 파티션은 기록이 0건이어도 항상 포함한다.
  select
    s.student_id,
    s.student_code,
    'unassigned'::text as scope_name,
    null::uuid as course_id
  from active_students s

  union all

  select distinct
    s.student_id,
    s.student_code,
    'course'::text,
    at.course_id
  from active_students s
  join auto_grading.attempts at on at.student_id = s.student_id
  where at.course_id is not null
),
partition_basis as (
  select
    ps.student_id,
    ps.student_code,
    auto_grading._student_achievement_stats_core(
      ps.student_id,
      ps.scope_name,
      ps.course_id
    ) -> 'basis' as basis
  from partition_scopes ps
),
partition_basis_sum as (
  select
    pb.student_id,
    pb.student_code,
    jsonb_build_object(
      'round1_correct_count', coalesce(sum(
        (pb.basis ->> 'round1_correct_count')::bigint
      ), 0),
      'round1_item_count', coalesce(sum(
        (pb.basis ->> 'round1_item_count')::bigint
      ), 0),
      'round2_correct_count', coalesce(sum(
        (pb.basis ->> 'round2_correct_count')::bigint
      ), 0),
      'round2_item_count', coalesce(sum(
        (pb.basis ->> 'round2_item_count')::bigint
      ), 0),
      'final_correct_count', coalesce(sum(
        (pb.basis ->> 'final_correct_count')::bigint
      ), 0),
      'final_item_count', coalesce(sum(
        (pb.basis ->> 'final_item_count')::bigint
      ), 0),
      'teacher_final_correct_count', coalesce(sum(
        (pb.basis ->> 'teacher_final_correct_count')::bigint
      ), 0),
      'teacher_final_item_count', coalesce(sum(
        (pb.basis ->> 'teacher_final_item_count')::bigint
      ), 0)
    ) as partition_sum
  from partition_basis pb
  group by pb.student_id, pb.student_code
),
basis_comparison as (
  select
    s.student_code,
    auto_grading._student_achievement_stats_core(
      s.student_id,
      'all',
      null
    ) -> 'basis' as all_basis,
    pbs.partition_sum
  from active_students s
  join partition_basis_sum pbs on pbs.student_id = s.student_id
),
basis_mismatches as (
  select *
  from basis_comparison x
  where x.all_basis is distinct from x.partition_sum
),
latest_attempt_rank_ties as (
  select
    at.assignment_id,
    coalesce(
      at.completed_at,
      at.round2_submitted_at,
      at.round1_submitted_at,
      at.updated_at,
      at.started_at
    ) as rank_at,
    count(*)::integer as tied_attempt_count
  from auto_grading.attempts at
  where at.assignment_id is not null
  group by
    at.assignment_id,
    coalesce(
      at.completed_at,
      at.round2_submitted_at,
      at.round1_submitted_at,
      at.updated_at,
      at.started_at
    )
  having count(*) > 1
),
student_102_results as (
  select
    auto_grading.get_student_achievement_courses_by_code('102') as courses,
    auto_grading.get_student_achievement_by_code(
      '102',
      'all',
      null,
      200
    ) as all_scope,
    auto_grading.get_student_achievement_by_code(
      '102',
      'course',
      '60afa69b-8082-4931-82a6-7ddd0862892b'::uuid,
      200
    ) as former_course,
    auto_grading.get_student_achievement_by_code(
      '102',
      'course',
      '866a2726-97a2-4345-bc9a-4b1b8a2e1762'::uuid,
      200
    ) as current_course,
    auto_grading.get_student_achievement_by_code(
      '102',
      'unassigned',
      null,
      200
    ) as unassigned
),
audit_rows as (
  select
    10 as sort_order,
    'private_achievement_cores'::text as section,
    jsonb_build_object(
      'functions', coalesce((
        select jsonb_agg(
          jsonb_build_object(
            'function_name', f.function_name,
            'identity_arguments', f.identity_arguments,
            'result_type', f.result_type,
            'source_md5', f.source_md5,
            'security_definer', f.security_definer,
            'validates_scope', f.prosrc ~ 'INVALID_SCOPE',
            'requires_course_id_for_course_scope',
              f.prosrc ~ 'COURSE_ID_REQUIRED_FOR_SCOPE',
            'all_scope_is_unfiltered', f.prosrc ~ 'v_scope = ''all''',
            'public_can_execute', f.public_can_execute,
            'anon_can_execute', f.anon_can_execute,
            'authenticated_can_execute', f.authenticated_can_execute,
            'service_role_can_execute', f.service_role_can_execute
          )
          order by f.function_name
        )
        from target_functions f
        where f.function_name in (
          '_student_achievement_stats_core',
          '_student_achievement_history_core'
        )
      ), '[]'::jsonb),
      'expected_function_count', 2
    ) as details

  union all

  select
    20,
    'scoped_achievement_rpcs',
    jsonb_build_object(
      'functions', coalesce((
        select jsonb_agg(
          jsonb_build_object(
            'function_name', f.function_name,
            'identity_arguments', f.identity_arguments,
            'arguments_with_defaults', f.arguments_with_defaults,
            'source_md5', f.source_md5,
            'security_definer', f.security_definer,
            'calls_stats_core',
              f.prosrc ~ '_student_achievement_stats_core',
            'calls_history_core',
              f.prosrc ~ '_student_achievement_history_core',
            'validates_public_token', f.prosrc ~ 'student_public_links',
            'default_argument_count', f.default_argument_count,
            'public_can_execute', f.public_can_execute,
            'anon_can_execute', f.anon_can_execute,
            'authenticated_can_execute', f.authenticated_can_execute,
            'service_role_can_execute', f.service_role_can_execute
          )
          order by f.function_name
        )
        from target_functions f
        where f.function_name in (
          'get_student_achievement_courses_by_code',
          'get_student_achievement_by_code',
          'get_student_achievement_courses_by_token',
          'get_student_achievement_by_token'
        )
      ), '[]'::jsonb),
      'expected_function_count', 4
    )

  union all

  select
    30,
    'old_new_stats_equivalence',
    jsonb_build_object(
      'compared_student_count', (select count(*) from stats_comparison_base),
      'mismatch_count', (select count(*) from stats_mismatches),
      'mismatches', coalesce((
        select jsonb_agg(
          jsonb_build_object(
            'student_code', x.student_code,
            'student_name', x.student_name,
            'old_stats', x.old_stats,
            'new_stats', x.new_stats
          )
          order by x.student_code
        )
        from stats_mismatches x
      ), '[]'::jsonb),
      'students_is_active_null_count', (
        select count(*)
        from auto_grading.students s
        where s.is_active is null
      )
    )

  union all

  select
    40,
    'old_new_history_equivalence',
    jsonb_build_object(
      'compared_student_count', (select count(*) from history_comparison_base),
      'mismatch_count', (select count(*) from history_mismatches),
      'mismatches', coalesce((
        select jsonb_agg(
          jsonb_build_object(
            'student_code', x.student_code,
            'student_name', x.student_name,
            'old_history', x.old_history,
            'new_history', x.new_history
          )
          order by x.student_code
        )
        from history_mismatches x
      ), '[]'::jsonb),
      'latest_attempt_rank_tie_group_count', (
        select count(*) from latest_attempt_rank_ties
      ),
      'latest_attempt_rank_ties', coalesce((
        select jsonb_agg(to_jsonb(x) order by x.assignment_id, x.rank_at)
        from latest_attempt_rank_ties x
      ), '[]'::jsonb)
    )

  union all

  select
    50,
    'stats_scope_partition_integrity',
    jsonb_build_object(
      'compared_student_count', (select count(*) from basis_comparison),
      'mismatch_count', (select count(*) from basis_mismatches),
      'mismatches', coalesce((
        select jsonb_agg(
          jsonb_build_object(
            'student_code', x.student_code,
            'all_basis', x.all_basis,
            'partition_sum', x.partition_sum
          )
          order by x.student_code
        )
        from basis_mismatches x
      ), '[]'::jsonb)
    )

  union all

  select
    60,
    'legacy_readers_still_independent',
    jsonb_build_object(
      'functions', coalesce((
        select jsonb_agg(
          jsonb_build_object(
            'function_name', f.function_name,
            'identity_arguments', f.identity_arguments,
            'result_type', f.result_type,
            'source_md5', f.source_md5,
            'calls_stats_core',
              f.prosrc ~ '_student_achievement_stats_core',
            'calls_history_core',
              f.prosrc ~ '_student_achievement_history_core'
          )
          order by f.function_name
        )
        from target_functions f
        where f.function_name in (
          'get_student_stats_by_code',
          'get_student_assignment_history_by_code'
        )
      ), '[]'::jsonb),
      'expected_calls_private_core_before_part2b', false
    )

  union all

  select
    70,
    'student_102_scoped_achievement_sample',
    jsonb_build_object(
      'courses', r.courses,
      'all_scope', jsonb_build_object(
        'stats', r.all_scope -> 'stats',
        'history_count', jsonb_array_length(r.all_scope -> 'history')
      ),
      'former_course', jsonb_build_object(
        'course', r.former_course -> 'course',
        'stats', r.former_course -> 'stats',
        'history_count', jsonb_array_length(r.former_course -> 'history')
      ),
      'current_course', jsonb_build_object(
        'course', r.current_course -> 'course',
        'stats', r.current_course -> 'stats',
        'history_count', jsonb_array_length(r.current_course -> 'history')
      ),
      'unassigned', jsonb_build_object(
        'stats', r.unassigned -> 'stats',
        'history_count', jsonb_array_length(r.unassigned -> 'history')
      )
    )
  from student_102_results r
)
select section, details
from audit_rows
order by sort_order;

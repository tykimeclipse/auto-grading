-- ============================================================================
-- audit_course_enrollment_achievement_stage7_part2_major_unit_achievement.sql
--
-- 7단계 part 2 배포 후 대단원별 성취도 RPC의 계약과 수치 정합성을 검증한다.
-- SELECT만 수행하며 데이터와 스키마를 변경하지 않는다.
--
-- 화면 구현 진행 조건:
--   - 모든 행의 blocking_issue_count = 0
--
-- 결과 5행의 section / blocking_issue_count / details를 공유한다.
-- ============================================================================

with
function_expectations as (
  select *
  from (values
    (
      '_student_major_unit_achievement_core'::text,
      'uuid, text, uuid'::text,
      false,
      false,
      false
    ),
    (
      'get_student_major_unit_achievement_by_code'::text,
      'text, text, uuid'::text,
      false,
      true,
      true
    ),
    (
      'get_student_major_unit_achievement_by_token'::text,
      'uuid, text, uuid'::text,
      true,
      true,
      true
    )
  ) as x(
    function_name,
    identity_arguments,
    expected_anon_execute,
    expected_authenticated_execute,
    expected_service_role_execute
  )
),
target_functions as (
  select
    p.proname as function_name,
    oidvectortypes(p.proargtypes) as identity_arguments,
    pg_get_function_result(p.oid) as result_type,
    p.prosecdef as security_definer,
    p.provolatile = 's' as is_stable,
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
      as service_role_can_execute,
    position('l.is_active = true' in p.prosrc) > 0
      as checks_active_link,
    position('l.expires_at > now()' in p.prosrc) > 0
      as checks_link_expiry,
    position('s.is_active = true' in p.prosrc) > 0
      as checks_active_student,
    md5(p.prosrc) as source_md5
  from pg_proc p
  join pg_namespace n on n.oid = p.pronamespace
  where n.nspname = 'auto_grading'
    and p.proname in (
      '_student_major_unit_achievement_core',
      'get_student_major_unit_achievement_by_code',
      'get_student_major_unit_achievement_by_token'
    )
),
function_contracts as (
  select
    e.function_name as expected_function_name,
    e.identity_arguments as expected_identity_arguments,
    f.function_name,
    f.identity_arguments,
    f.result_type,
    f.security_definer,
    f.is_stable,
    f.public_can_execute,
    f.anon_can_execute,
    f.authenticated_can_execute,
    f.service_role_can_execute,
    f.checks_active_link,
    f.checks_link_expiry,
    f.checks_active_student,
    f.source_md5,
    e.expected_anon_execute,
    e.expected_authenticated_execute,
    e.expected_service_role_execute,
    f.function_name is null
      or f.identity_arguments is distinct from e.identity_arguments
      or f.result_type is distinct from 'jsonb'
      or f.security_definer is distinct from true
      or f.is_stable is distinct from true
      or f.public_can_execute is distinct from false
      or f.anon_can_execute is distinct from e.expected_anon_execute
      or f.authenticated_can_execute
        is distinct from e.expected_authenticated_execute
      or f.service_role_can_execute
        is distinct from e.expected_service_role_execute
      or (
        e.function_name = 'get_student_major_unit_achievement_by_token'
        and (
          f.checks_active_link is distinct from true
          or f.checks_link_expiry is distinct from true
          or f.checks_active_student is distinct from true
        )
      )
      as has_issue
  from function_expectations e
  left join target_functions f
    on f.function_name = e.function_name
   and f.identity_arguments = e.identity_arguments
),
active_students as (
  select
    s.id as student_id,
    s.student_code,
    s.name as student_name
  from auto_grading.students s
  where s.is_active = true
),
student_course_keys as (
  select sc.student_id, sc.course_id
  from auto_grading.student_courses sc

  union

  select a.student_id, a.course_id
  from auto_grading.assignments a
  where a.course_id is not null

  union

  select at.student_id, at.course_id
  from auto_grading.attempts at
  where at.course_id is not null
),
scope_cases as (
  select
    s.student_id,
    s.student_code,
    s.student_name,
    'all'::text as scope_name,
    null::uuid as course_id
  from active_students s

  union all

  select
    s.student_id,
    s.student_code,
    s.student_name,
    'unassigned'::text,
    null::uuid
  from active_students s

  union all

  select distinct
    s.student_id,
    s.student_code,
    s.student_name,
    'course'::text,
    sk.course_id
  from active_students s
  join student_course_keys sk on sk.student_id = s.student_id
  join auto_grading.courses c on c.id = sk.course_id
),
scope_payloads as materialized (
  select
    sc.student_id,
    sc.student_code,
    sc.student_name,
    sc.scope_name,
    sc.course_id,
    auto_grading._student_major_unit_achievement_core(
      sc.student_id,
      sc.scope_name,
      sc.course_id
    ) as unit_payload,
    auto_grading._student_achievement_stats_core(
      sc.student_id,
      sc.scope_name,
      sc.course_id
    ) -> 'basis' as canonical_basis,
    auto_grading.get_student_major_unit_achievement_by_code(
      sc.student_code,
      sc.scope_name,
      sc.course_id
    ) as code_payload
  from scope_cases sc
),
scope_basis as (
  select
    sp.*,
    jsonb_build_object(
      'round1_correct_count',
        (sp.unit_payload #>> '{summary,scores,round1,correct_count}')::bigint,
      'round1_item_count',
        (sp.unit_payload #>> '{summary,scores,round1,item_count}')::bigint,
      'round2_correct_count',
        (sp.unit_payload #>> '{summary,scores,round2_reflected,correct_count}')::bigint,
      'round2_item_count',
        (sp.unit_payload #>> '{summary,scores,round2_reflected,item_count}')::bigint,
      'final_correct_count',
        (sp.unit_payload #>> '{summary,scores,final,correct_count}')::bigint,
      'final_item_count',
        (sp.unit_payload #>> '{summary,scores,final,item_count}')::bigint,
      'teacher_final_correct_count',
        (sp.unit_payload #>> '{summary,scores,teacher_final,correct_count}')::bigint,
      'teacher_final_item_count',
        (sp.unit_payload #>> '{summary,scores,teacher_final,item_count}')::bigint
    ) as unit_basis
  from scope_payloads sp
),
basis_mismatches as (
  select
    sb.student_code,
    sb.student_name,
    sb.scope_name,
    sb.course_id,
    sb.canonical_basis,
    sb.unit_basis
  from scope_basis sb
  where sb.canonical_basis is distinct from sb.unit_basis
),
scope_unit_sums as (
  select
    sp.student_code,
    sp.student_name,
    sp.scope_name,
    sp.course_id,
    (sp.unit_payload #>> '{summary,unit_count}')::integer
      as declared_unit_count,
    (sp.unit_payload #>> '{summary,test_count}')::integer
      as declared_test_count,
    sp.unit_payload #>> '{contract,tests_order}' as tests_order,
    sp.unit_payload #>> '{contract,tests_limit_selection}'
      as tests_limit_selection,
    (sp.unit_payload #>> '{contract,tests_per_unit_limit}')::integer
      as tests_per_unit_limit,
    sp.unit_payload #>> '{contract,date_timezone}' as date_timezone,
    count(u.unit_json)::integer as actual_unit_count,
    coalesce(sum((u.unit_json ->> 'test_count')::integer), 0)::integer
      as unit_test_count_sum
  from scope_payloads sp
  left join lateral jsonb_array_elements(sp.unit_payload -> 'units')
    as u(unit_json) on true
  group by
    sp.student_code,
    sp.student_name,
    sp.scope_name,
    sp.course_id,
    sp.unit_payload
),
scope_summary_mismatches as (
  select *
  from scope_unit_sums sus
  where sus.declared_unit_count is distinct from sus.actual_unit_count
     or sus.declared_test_count is distinct from sus.unit_test_count_sum
     or sus.tests_order is distinct from 'evaluated_at_asc'
     or sus.tests_limit_selection is distinct from 'most_recent'
     or sus.tests_per_unit_limit is distinct from 200
     or sus.date_timezone is distinct from 'Asia/Seoul'
),
unit_test_rows_raw as (
  select
    sp.student_code,
    sp.student_name,
    sp.scope_name,
    sp.course_id,
    u.unit_no,
    u.unit_json,
    (sp.unit_payload #>> '{contract,tests_per_unit_limit}')::integer
      as tests_per_unit_limit,
    t.test_no,
    (t.test_json ->> 'evaluated_at')::timestamptz as evaluated_at,
    (t.test_json ->> 'attempt_id')::uuid as attempt_id,
    t.test_json
  from scope_payloads sp
  cross join lateral jsonb_array_elements(sp.unit_payload -> 'units')
    with ordinality as u(unit_json, unit_no)
  cross join lateral jsonb_array_elements(u.unit_json -> 'tests')
    with ordinality as t(test_json, test_no)
),
unit_test_rows as (
  select
    x.*,
    lag(x.evaluated_at) over (
      partition by x.student_code, x.scope_name, x.course_id, x.unit_no
      order by x.test_no
    ) as previous_evaluated_at,
    lag(x.attempt_id) over (
      partition by x.student_code, x.scope_name, x.course_id, x.unit_no
      order by x.test_no
    ) as previous_attempt_id
  from unit_test_rows_raw x
),
unit_detail_sums as (
  select
    t.student_code,
    t.student_name,
    t.scope_name,
    t.course_id,
    t.unit_no,
    t.unit_json -> 'unit' as unit_key,
    t.tests_per_unit_limit,
    (t.unit_json ->> 'test_count')::integer as declared_test_count,
    (t.unit_json ->> 'returned_test_count')::integer
      as declared_returned_test_count,
    (t.unit_json ->> 'tests_truncated')::boolean
      as declared_tests_truncated,
    (t.unit_json ->> 'last_evaluated_at')::timestamptz
      as declared_last_evaluated_at,
    (t.unit_json ->> 'manual_test_count')::integer
      as declared_manual_test_count,
    (t.unit_json ->> 'round2_reflected_test_count')::integer
      as declared_round2_reflected_test_count,
    (t.unit_json ->> 'final_confirmed_test_count')::integer
      as declared_final_confirmed_test_count,
    (t.unit_json #>> '{scores,round1,correct_count}')::bigint
      as declared_round1_correct_count,
    (t.unit_json #>> '{scores,round1,item_count}')::bigint
      as declared_round1_item_count,
    (t.unit_json #>> '{scores,round2_reflected,correct_count}')::bigint
      as declared_round2_correct_count,
    (t.unit_json #>> '{scores,round2_reflected,item_count}')::bigint
      as declared_round2_item_count,
    (t.unit_json #>> '{scores,final,correct_count}')::bigint
      as declared_final_correct_count,
    (t.unit_json #>> '{scores,final,item_count}')::bigint
      as declared_final_item_count,
    (t.unit_json #>> '{scores,teacher_final,correct_count}')::bigint
      as declared_teacher_final_correct_count,
    (t.unit_json #>> '{scores,teacher_final,item_count}')::bigint
      as declared_teacher_final_item_count,
    (t.unit_json #>> '{retention,final_confirmed,test_count}')::integer
      as declared_retention_final_test_count,
    (t.unit_json #>> '{retention,round1,test_count}')::integer
      as declared_retention_round1_test_count,
    (t.unit_json #>> '{retention,final_confirmed,latest_evaluated_at}')::timestamptz
      as declared_final_latest_evaluated_at,
    (t.unit_json #>> '{retention,final_confirmed,days_since_latest}')::integer
      as declared_final_days_since_latest,
    (t.unit_json #>> '{retention,round1,latest_evaluated_at}')::timestamptz
      as declared_round1_latest_evaluated_at,
    (t.unit_json #>> '{retention,round1,days_since_latest}')::integer
      as declared_round1_days_since_latest,
    count(*)::integer as actual_returned_test_count,
    max(t.evaluated_at) as actual_last_evaluated_at,
    bool_or(
      t.previous_evaluated_at is not null
      and (
        t.evaluated_at < t.previous_evaluated_at
        or (
          t.evaluated_at = t.previous_evaluated_at
          and t.attempt_id < t.previous_attempt_id
        )
      )
    ) as has_order_issue,
    count(*) filter (
      where t.test_json ->> 'source_type' = 'manual'
    )::integer as actual_manual_test_count,
    count(*) filter (
      where t.test_json #>> '{round2_reflected,item_count}' is not null
    )::integer as actual_round2_reflected_test_count,
    count(*) filter (
      where (t.test_json #>> '{final,is_confirmed}')::boolean
    )::integer as actual_final_confirmed_test_count,
    count(*) filter (
      where t.test_json #>> '{final,score_percent}' is not null
    )::integer as actual_final_retention_test_count,
    count(*) filter (
      where t.test_json #>> '{round1,score_percent}' is not null
    )::integer as actual_round1_retention_test_count,
    coalesce(sum(
      (t.test_json #>> '{round1,correct_count}')::bigint
    ), 0)::bigint as actual_round1_correct_count,
    coalesce(sum(
      (t.test_json #>> '{round1,item_count}')::bigint
    ), 0)::bigint as actual_round1_item_count,
    coalesce(sum(
      (t.test_json #>> '{round2_reflected,correct_count}')::bigint
    ), 0)::bigint as actual_round2_correct_count,
    coalesce(sum(
      (t.test_json #>> '{round2_reflected,item_count}')::bigint
    ), 0)::bigint as actual_round2_item_count,
    coalesce(sum(
      (t.test_json #>> '{final,correct_count}')::bigint
    ), 0)::bigint as actual_final_correct_count,
    coalesce(sum(
      (t.test_json #>> '{final,item_count}')::bigint
    ), 0)::bigint as actual_final_item_count,
    coalesce(sum(
      (t.test_json #>> '{teacher_final,correct_count}')::bigint
    ), 0)::bigint as actual_teacher_final_correct_count,
    coalesce(sum(
      (t.test_json #>> '{teacher_final,item_count}')::bigint
    ), 0)::bigint as actual_teacher_final_item_count
  from unit_test_rows t
  group by
    t.student_code,
    t.student_name,
    t.scope_name,
    t.course_id,
    t.unit_no,
    t.unit_json,
    t.tests_per_unit_limit
),
unit_detail_mismatches as (
  select *
  from unit_detail_sums uds
  where uds.declared_returned_test_count
      is distinct from uds.actual_returned_test_count
     or uds.declared_returned_test_count is distinct from least(
       uds.declared_test_count,
       uds.tests_per_unit_limit
     )
     or uds.declared_tests_truncated
      is distinct from (
        uds.declared_test_count > uds.declared_returned_test_count
      )
     or uds.declared_last_evaluated_at
      is distinct from uds.actual_last_evaluated_at
     or uds.declared_final_days_since_latest is distinct from case
       when uds.declared_final_latest_evaluated_at is null then null
       else (now() at time zone 'Asia/Seoul')::date - (
         uds.declared_final_latest_evaluated_at at time zone 'Asia/Seoul'
       )::date
     end
     or uds.declared_round1_days_since_latest is distinct from case
       when uds.declared_round1_latest_evaluated_at is null then null
       else (now() at time zone 'Asia/Seoul')::date - (
         uds.declared_round1_latest_evaluated_at at time zone 'Asia/Seoul'
       )::date
     end
     or uds.has_order_issue
     or (
       not uds.declared_tests_truncated
       and row(
         uds.declared_test_count,
         uds.declared_manual_test_count,
         uds.declared_round2_reflected_test_count,
         uds.declared_final_confirmed_test_count,
         uds.declared_round1_correct_count,
         uds.declared_round1_item_count,
         uds.declared_round2_correct_count,
         uds.declared_round2_item_count,
         uds.declared_final_correct_count,
         uds.declared_final_item_count,
         uds.declared_teacher_final_correct_count,
         uds.declared_teacher_final_item_count,
         uds.declared_retention_final_test_count,
         uds.declared_retention_round1_test_count
       ) is distinct from row(
         uds.actual_returned_test_count,
         uds.actual_manual_test_count,
         uds.actual_round2_reflected_test_count,
         uds.actual_final_confirmed_test_count,
         uds.actual_round1_correct_count,
         uds.actual_round1_item_count,
         uds.actual_round2_correct_count,
         uds.actual_round2_item_count,
         uds.actual_final_correct_count,
         uds.actual_final_item_count,
         uds.actual_teacher_final_correct_count,
         uds.actual_teacher_final_item_count,
         uds.actual_final_retention_test_count,
         uds.actual_round1_retention_test_count
       )
     )
),
code_wrapper_mismatches as (
  select
    sp.student_code,
    sp.student_name,
    sp.scope_name,
    sp.course_id
  from scope_payloads sp
  where ((sp.code_payload - 'scope') - 'course')
    is distinct from sp.unit_payload
),
valid_public_links as (
  select
    l.public_token,
    s.student_code,
    s.student_name
  from auto_grading.student_public_links l
  join active_students s on s.student_id = l.student_id
  where l.is_active = true
    and (l.expires_at is null or l.expires_at > now())
),
token_wrapper_mismatches as (
  select
    vpl.student_code,
    vpl.student_name,
    vpl.public_token
  from valid_public_links vpl
  where auto_grading.get_student_major_unit_achievement_by_token(
    vpl.public_token,
    'all',
    null
  ) is distinct from auto_grading.get_student_major_unit_achievement_by_code(
    vpl.student_code,
    'all',
    null
  )
),
missing_public_token as materialized (
  select gen_random_uuid() as public_token
),
negative_token_cases as (
  select
    'missing_token'::text as case_type,
    m.public_token,
    null::text as student_code
  from missing_public_token m

  union all

  select
    'inactive_link',
    l.public_token,
    s.student_code
  from auto_grading.student_public_links l
  join auto_grading.students s on s.id = l.student_id
  where l.is_active = false

  union all

  select
    'expired_link',
    l.public_token,
    s.student_code
  from auto_grading.student_public_links l
  join auto_grading.students s on s.id = l.student_id
  where l.is_active = true
    and l.expires_at < now()
    and s.is_active = true

  union all

  select
    'inactive_student',
    l.public_token,
    s.student_code
  from auto_grading.student_public_links l
  join auto_grading.students s on s.id = l.student_id
  where l.is_active = true
    and (l.expires_at is null or l.expires_at > now())
    and s.is_active = false
),
negative_token_results as materialized (
  select
    ntc.case_type,
    ntc.public_token,
    ntc.student_code,
    auto_grading.get_student_major_unit_achievement_by_token(
      ntc.public_token,
      'all',
      null
    ) as payload
  from negative_token_cases ntc
),
negative_token_mismatches as (
  select *
  from negative_token_results ntr
  where ntr.payload is not null
),
audit_rows as (
  select
    10 as sort_order,
    'function_contract'::text as section,
    count(*) filter (where fc.has_issue)::integer as blocking_issue_count,
    jsonb_build_object(
      'expected_function_count', 3,
      'installed_function_count', count(*) filter (
        where fc.function_name is not null
      ),
      'issue_count', count(*) filter (where fc.has_issue),
      'functions', jsonb_agg(
        to_jsonb(fc) - 'has_issue'
        order by fc.expected_function_name
      )
    ) as details
  from function_contracts fc

  union all

  select
    20,
    'canonical_basis_equivalence',
    (select count(*)::integer from basis_mismatches),
    jsonb_build_object(
      'compared_scope_count', (select count(*) from scope_basis),
      'mismatch_count', (select count(*) from basis_mismatches),
      'mismatches', coalesce((
        select jsonb_agg(
          to_jsonb(x)
          order by x.student_code, x.scope_name, x.course_id
        )
        from basis_mismatches x
      ), '[]'::jsonb)
    )

  union all

  select
    30,
    'scope_summary_integrity',
    (select count(*)::integer from scope_summary_mismatches),
    jsonb_build_object(
      'compared_scope_count', (select count(*) from scope_unit_sums),
      'mismatch_count', (select count(*) from scope_summary_mismatches),
      'mismatches', coalesce((
        select jsonb_agg(
          to_jsonb(x)
          order by x.student_code, x.scope_name, x.course_id
        )
        from scope_summary_mismatches x
      ), '[]'::jsonb)
    )

  union all

  select
    40,
    'unit_detail_integrity',
    (select count(*)::integer from unit_detail_mismatches),
    jsonb_build_object(
      'compared_unit_count', (select count(*) from unit_detail_sums),
      'mismatch_count', (select count(*) from unit_detail_mismatches),
      'mismatches', coalesce((
        select jsonb_agg(
          to_jsonb(x)
          order by x.student_code, x.scope_name, x.course_id, x.unit_no
        )
        from unit_detail_mismatches x
      ), '[]'::jsonb)
    )

  union all

  select
    50,
    'wrapper_equivalence',
    (
      (select count(*) from code_wrapper_mismatches)
      + (select count(*) from token_wrapper_mismatches)
      + (select count(*) from negative_token_mismatches)
    )::integer,
    jsonb_build_object(
      'code_scope_count', (select count(*) from scope_payloads),
      'code_mismatch_count', (select count(*) from code_wrapper_mismatches),
      'valid_public_link_count', (select count(*) from valid_public_links),
      'token_mismatch_count', (select count(*) from token_wrapper_mismatches),
      'negative_token_case_count', (select count(*) from negative_token_results),
      'negative_token_case_counts', coalesce((
        select jsonb_object_agg(x.case_type, x.case_count)
        from (
          select ntr.case_type, count(*) as case_count
          from negative_token_results ntr
          group by ntr.case_type
        ) x
      ), '{}'::jsonb),
      'negative_token_mismatch_count', (
        select count(*) from negative_token_mismatches
      ),
      'code_mismatches', coalesce((
        select jsonb_agg(
          to_jsonb(x)
          order by x.student_code, x.scope_name, x.course_id
        )
        from code_wrapper_mismatches x
      ), '[]'::jsonb),
      'token_mismatches', coalesce((
        select jsonb_agg(to_jsonb(x) order by x.student_code)
        from token_wrapper_mismatches x
      ), '[]'::jsonb),
      'negative_token_mismatches', coalesce((
        select jsonb_agg(
          to_jsonb(x)
          order by x.case_type, x.student_code, x.public_token
        )
        from negative_token_mismatches x
      ), '[]'::jsonb)
    )
)
select
  ar.section,
  ar.blocking_issue_count,
  ar.details
from audit_rows ar
order by ar.sort_order;

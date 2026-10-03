-- ============================================================================
-- audit_teacher_list_assignments_filters_pagination_postdeploy.sql
--
-- teacher_list_assignments_v2.sql 배포 후 점검. 데이터 변경 없음.
--
-- 화면 배포 진행 조건:
--   - 모든 행의 blocking_issue_count = 0
--
-- 구성
--   1) DO 블록: 역할 전환(set local role)과 JWT 흉내(request.jwt.claims)로 실제 호출을 검증하고
--      결과를 세션 설정값 audit.teacher_list_assignments_runtime 에 담는다.
--      역할·JWT 는 블록이 끝나기 전에 원복한다. 조회만 하며 쓰기는 하지 않는다.
--   2) SELECT: 정적 계약(시그니처·권한·본문) + 1)의 결과를 함께 보고한다.
--
-- 섹션
--   10 function_contract  14개 인자 1개, 9개 인자 0개, plpgsql, security definer,
--                         assert_admin() 이 return query 보다 먼저, 집계 컬럼 존재,
--                         PUBLIC/anon/service_role = false, authenticated = true
--   20 runtime_checks     관리자 정상 / 일반 authenticated 42501 / anon·service_role 권한 거부 /
--                         발행 페이지 9개 인자 호출 정상 / 대단원 키 누락 22023 /
--                         집계가 페이지 나누기 전 전체 기준인지
--
-- v_admin_email 은 assert_admin.sql 허용 목록의 관리자 이메일이어야 한다.
-- ============================================================================

do $$
declare
  v_admin_email constant text := 'tykimeclipse@gmail.com';
  v_non_admin_email constant text := 'audit-non-admin@example.invalid';
  v_results jsonb := '[]'::jsonb;
  v_rows bigint;
  v_total bigint;
  v_open bigint;
  v_final bigint;
  v_counted_open bigint;
  v_counted_final bigint;
  v_page_total bigint;
  v_state text;
  v_msg text;
begin
  perform set_config('audit.teacher_list_assignments_runtime', '', false);

  -- 1) 허용된 관리자 이메일 + authenticated → 정상
  begin
    perform set_config('request.jwt.claims',
      json_build_object('email', v_admin_email, 'role', 'authenticated')::text, true);
    set local role authenticated;
    select count(*) into v_rows from auto_grading.teacher_list_assignments(p_limit => 1);
    reset role;
    v_results := v_results || jsonb_build_object(
      'check', 'admin_authenticated_call', 'passed', true, 'rows', v_rows);
  exception when others then
    get stacked diagnostics v_state = returned_sqlstate, v_msg = message_text;
    v_results := v_results || jsonb_build_object(
      'check', 'admin_authenticated_call', 'passed', false, 'sqlstate', v_state, 'message', v_msg);
  end;

  -- 2) 일반 authenticated 계정 → assert_admin 이 42501 로 차단
  begin
    perform set_config('request.jwt.claims',
      json_build_object('email', v_non_admin_email, 'role', 'authenticated')::text, true);
    set local role authenticated;
    select count(*) into v_rows from auto_grading.teacher_list_assignments(p_limit => 1);
    reset role;
    v_results := v_results || jsonb_build_object(
      'check', 'non_admin_authenticated_blocked', 'passed', false, 'message', 'call unexpectedly succeeded');
  exception when others then
    get stacked diagnostics v_state = returned_sqlstate, v_msg = message_text;
    v_results := v_results || jsonb_build_object(
      'check', 'non_admin_authenticated_blocked',
      'passed', v_state = '42501' and position('관리자 권한' in v_msg) > 0,
      'sqlstate', v_state, 'message', v_msg);
  end;

  -- 3) anon → 실행 권한 없음 (permission denied for function/schema)
  --    'permission denied to set role' 같은 검사 환경 오류는 통과로 치지 않는다.
  begin
    perform set_config('request.jwt.claims', json_build_object('role', 'anon')::text, true);
    set local role anon;
    select count(*) into v_rows from auto_grading.teacher_list_assignments(p_limit => 1);
    reset role;
    v_results := v_results || jsonb_build_object(
      'check', 'anon_execute_denied', 'passed', false, 'message', 'call unexpectedly succeeded');
  exception when others then
    get stacked diagnostics v_state = returned_sqlstate, v_msg = message_text;
    v_results := v_results || jsonb_build_object(
      'check', 'anon_execute_denied',
      'passed', v_state = '42501' and v_msg like 'permission denied for %',
      'sqlstate', v_state, 'message', v_msg);
  end;

  -- 4) service_role → 실행 권한 없음
  begin
    perform set_config('request.jwt.claims', json_build_object('role', 'service_role')::text, true);
    set local role service_role;
    select count(*) into v_rows from auto_grading.teacher_list_assignments(p_limit => 1);
    reset role;
    v_results := v_results || jsonb_build_object(
      'check', 'service_role_execute_denied', 'passed', false, 'message', 'call unexpectedly succeeded');
  exception when others then
    get stacked diagnostics v_state = returned_sqlstate, v_msg = message_text;
    v_results := v_results || jsonb_build_object(
      'check', 'service_role_execute_denied',
      'passed', v_state = '42501' and v_msg like 'permission denied for %',
      'sqlstate', v_state, 'message', v_msg);
  end;

  -- 5) 발행 페이지(teacher-assignments-linked-v3-issue-only-compact.html)와 같은 9개 인자 호출 → 정상
  begin
    perform set_config('request.jwt.claims',
      json_build_object('email', v_admin_email, 'role', 'authenticated')::text, true);
    set local role authenticated;
    select count(*) into v_rows
    from auto_grading.teacher_list_assignments(
      p_course_id => null,
      p_test_set_id => null,
      p_student_id => null,
      p_is_open => true,
      p_purpose => '숙제',
      p_status => null,
      p_search => null,
      p_limit => 100,
      p_offset => 0
    );
    reset role;
    v_results := v_results || jsonb_build_object(
      'check', 'issue_page_nine_arg_call', 'passed', true, 'rows', v_rows);
  exception when others then
    get stacked diagnostics v_state = returned_sqlstate, v_msg = message_text;
    v_results := v_results || jsonb_build_object(
      'check', 'issue_page_nine_arg_call', 'passed', false, 'sqlstate', v_state, 'message', v_msg);
  end;

  -- 6) 대단원 번호만 주고 교육과정·학년·과목이 빠지면 22023
  begin
    perform set_config('request.jwt.claims',
      json_build_object('email', v_admin_email, 'role', 'authenticated')::text, true);
    set local role authenticated;
    select count(*) into v_rows
    from auto_grading.teacher_list_assignments(p_limit => 1, p_major_unit_code => '1');
    reset role;
    v_results := v_results || jsonb_build_object(
      'check', 'major_unit_partial_key_rejected', 'passed', false, 'message', 'call unexpectedly succeeded');
  exception when others then
    get stacked diagnostics v_state = returned_sqlstate, v_msg = message_text;
    v_results := v_results || jsonb_build_object(
      'check', 'major_unit_partial_key_rejected',
      'passed', v_state = '22023' and position('MAJOR_UNIT_FILTER_REQUIRES_FULL_KEY' in v_msg) > 0,
      'sqlstate', v_state, 'message', v_msg);
  end;

  -- 7) 집계 컬럼이 페이지 나누기 전 전체 결과 기준인지
  --    전체를 한 번에 받아 직접 센 값과, 1건짜리 페이지가 돌려준 집계 값이 같아야 한다.
  --    직접 센 최종 확정 수는 함수 안의 규칙과 별개로 다시 계산한다.
  begin
    perform set_config('request.jwt.claims',
      json_build_object('email', v_admin_email, 'role', 'authenticated')::text, true);
    set local role authenticated;

    select
      count(*),
      count(*) filter (where r.is_open),
      count(*) filter (
        where r.has_teacher_final
           or (
             r.source_type is distinct from 'manual'
             and coalesce(r.total_items, 0) > 0
             and (
               r.round1_correct_count = r.total_items
               or r.round1_score_percent = 100
               or r.round2_correct_count = r.total_items
               or r.round2_score_percent = 100
             )
           )
      )
    into v_rows, v_counted_open, v_counted_final
    from auto_grading.teacher_list_assignments(p_limit => 1000000) r;

    select r.total_count, r.filtered_open_count, r.filtered_final_confirmed_count
    into v_total, v_open, v_final
    from auto_grading.teacher_list_assignments(p_limit => 1) r;

    select r.total_count into v_page_total
    from auto_grading.teacher_list_assignments(p_limit => 10, p_offset => 10) r
    limit 1;

    reset role;
    v_results := v_results || jsonb_build_object(
      'check', 'aggregates_are_pre_pagination',
      'passed',
        coalesce(v_total, 0) = v_rows
        and coalesce(v_open, 0) = v_counted_open
        and coalesce(v_final, 0) = v_counted_final
        and (v_page_total is null or v_page_total = v_rows),
      'counted_rows', v_rows,
      'counted_open', v_counted_open,
      'counted_final_confirmed', v_counted_final,
      'page1_total_count', v_total,
      'page1_filtered_open_count', v_open,
      'page1_filtered_final_confirmed_count', v_final,
      'page2_total_count', v_page_total);
  exception when others then
    get stacked diagnostics v_state = returned_sqlstate, v_msg = message_text;
    v_results := v_results || jsonb_build_object(
      'check', 'aggregates_are_pre_pagination', 'passed', false, 'sqlstate', v_state, 'message', v_msg);
  end;

  reset role;
  perform set_config('request.jwt.claims', '', true);
  perform set_config('audit.teacher_list_assignments_runtime', v_results::text, false);
end $$;

with
new_args as (
  select 'uuid, uuid, uuid, boolean, text, text, text, integer, integer, text, text, text, text, text'::text
    as identity_arguments
),
legacy_args as (
  select 'uuid, uuid, uuid, boolean, text, text, text, integer, integer'::text as identity_arguments
),
target_functions as (
  select
    p.oid,
    oidvectortypes(p.proargtypes) as identity_arguments,
    pg_get_function_result(p.oid) as result_type,
    l.lanname as language,
    p.prosecdef as security_definer,
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
  join pg_language l on l.oid = p.prolang
  where n.nspname = 'auto_grading'
    and p.proname = 'teacher_list_assignments'
),
expected_function as (
  select tf.*
  from target_functions tf
  where tf.identity_arguments = (select identity_arguments from new_args)
),
function_issues as (
  select 'expected_function_count'::text as issue
  where (select count(*) from expected_function) <> 1

  union all

  select 'legacy_nine_arg_overload_present'
  where exists (
    select 1 from target_functions tf
    where tf.identity_arguments = (select identity_arguments from legacy_args)
  )

  union all

  select 'unexpected_overload_present'
  where exists (
    select 1 from target_functions tf
    where tf.identity_arguments not in (
      (select identity_arguments from new_args),
      (select identity_arguments from legacy_args)
    )
  )

  union all

  select 'function_contract'
  from expected_function ef
  where ef.language is distinct from 'plpgsql'
     or ef.security_definer is distinct from true
     or ef.public_can_execute is distinct from false
     or ef.anon_can_execute is distinct from false
     or ef.service_role_can_execute is distinct from false
     or ef.authenticated_can_execute is distinct from true

  union all

  select 'function_result_contract'
  from expected_function ef
  where position('filtered_open_count bigint' in ef.result_type) = 0
     or position('filtered_final_confirmed_count bigint' in ef.result_type) = 0
     or position('reset_token uuid' in ef.result_type) = 0

  union all

  select 'function_body_contract'
  from expected_function ef
  where position('perform auto_grading.assert_admin()' in ef.prosrc) = 0
     or position('perform auto_grading.assert_admin()' in ef.prosrc)
        > position('return query' in ef.prosrc)
     or position('btrim(ts.source_category) = v_source_category' in ef.prosrc) = 0
     or position('left(ts.unit_code, 1) = v_major_unit_code' in ef.prosrc) = 0
),
runtime as (
  select nullif(current_setting('audit.teacher_list_assignments_runtime', true), '')::jsonb as results
),
runtime_checks as (
  select
    c.value ->> 'check' as check_name,
    coalesce((c.value ->> 'passed')::boolean, false) as passed,
    c.value as detail
  from runtime r
  cross join lateral jsonb_array_elements(coalesce(r.results, '[]'::jsonb)) c(value)
),
audit_rows as (
  select
    10 as sort_order,
    'function_contract'::text as section,
    (select count(*)::integer from function_issues) as blocking_issue_count,
    jsonb_build_object(
      'issues', coalesce((select jsonb_agg(fi.issue) from function_issues fi), '[]'::jsonb),
      'installed_overload_count', (select count(*) from target_functions),
      'functions', coalesce((
        select jsonb_agg(
          jsonb_build_object(
            'identity_arguments', tf.identity_arguments,
            'language', tf.language,
            'security_definer', tf.security_definer,
            'public_can_execute', tf.public_can_execute,
            'anon_can_execute', tf.anon_can_execute,
            'authenticated_can_execute', tf.authenticated_can_execute,
            'service_role_can_execute', tf.service_role_can_execute,
            'has_assert_admin', position('perform auto_grading.assert_admin()' in tf.prosrc) > 0,
            'result_type', tf.result_type
          )
          order by tf.identity_arguments
        )
        from target_functions tf
      ), '[]'::jsonb)
    ) as details

  union all

  select
    20,
    'runtime_checks',
    case
      when (select results from runtime) is null then 1
      else (select count(*)::integer from runtime_checks rc where not rc.passed)
        + case when (select count(*) from runtime_checks) <> 7 then 1 else 0 end
    end,
    jsonb_build_object(
      'expected_check_count', 7,
      'ran', (select results from runtime) is not null,
      'checks', coalesce((
        select jsonb_agg(rc.detail order by rc.check_name) from runtime_checks rc
      ), '[]'::jsonb)
    )
)
select
  ar.section,
  ar.blocking_issue_count,
  ar.details
from audit_rows ar
order by ar.sort_order;

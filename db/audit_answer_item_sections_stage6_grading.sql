-- ============================================================================
-- audit_answer_item_sections_stage6_grading.sql
--
-- 답안 입력폼 문항 섹션 기능 6단계에서 1·2차 제출/채점 함수가 표시번호가
-- 아닌 내부 item_no를 계속 사용하고, 라운드 전이와 저장 무결성이 유지되는지
-- 확인한다. SELECT만 수행하며 데이터와 스키마를 변경하지 않는다.
--
-- 이 단계는 채점 함수의 동작을 변경하지 않는다. display_item_no,
-- section_order, section_title, display_order는 화면 전용이며 제출 payload와
-- retry_item_nos/remaining_item_nos는 기존처럼 내부 item_no 계약을 유지한다.
--
-- 진행 조건:
--   - function_and_permission_contract의 두 함수가 각각 1개이고 계약 플래그가 true
--   - payload_and_internal_key_contract의 모든 함수별 계약 플래그가 true
--   - round1_transition_contract의 모든 보호·반환 플래그가 true
--   - round2_transition_contract의 모든 보호·반환 플래그가 true
--   - response_operational_integrity의 모든 위반·불일치 건수가 0
--
-- 결과 5행의 section / details를 공유한다.
-- ============================================================================

with target_functions as (
  select
    n.nspname as schema_name,
    p.proname as function_name,
    pg_get_function_identity_arguments(p.oid) as identity_arguments,
    pg_get_function_arguments(p.oid) as arguments_with_defaults,
    pg_get_function_result(p.oid) as result_type,
    p.prosecdef as security_definer,
    p.proconfig,
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
    and p.proname in ('submit_round1', 'submit_round2')
),
function_contract as (
  select
    f.*,
    f.identity_arguments = 'p_attempt_id uuid, p_responses jsonb'
      as has_expected_identity_arguments,
    f.result_type = 'jsonb' as returns_jsonb,
    f.prosrc ~* 'jsonb_typeof\s*\(\s*p_responses\s*\)\s*<>\s*''array'''
      as validates_json_array,
    f.prosrc ~* 'jsonb_to_recordset\s*\(\s*p_responses\s*\)'
      and f.prosrc ~* 'item_no\s+text\s*,\s*answer\s+text'
      as parses_internal_item_no_payload,
    f.prosrc ~* 'count\s*\(\s*distinct\s+item_no\s*\)'
      and f.prosrc ~ 'DUPLICATE_ITEM_NO_IN_PAYLOAD'
      as rejects_duplicate_item_nos,
    f.prosrc ~* 'ti\.test_set_id\s*=\s*v_attempt\.test_set_id\s*and\s*ti\.item_no\s*=\s*p\.item_no'
      as joins_internal_item_no,
    not (f.prosrc ~ 'display_item_no|section_order|section_title|display_order')
      as ignores_display_metadata,
    f.prosrc ~* 'insert\s+into\s+auto_grading\.responses\s*\([^;]*test_item_id'
      as writes_response_by_test_item_id,
    f.prosrc ~* 'on\s+conflict\s*\(\s*attempt_id\s*,\s*test_item_id\s*,\s*round_no\s*\)'
      as upserts_response_identity,
    f.prosrc ~* 'normalize_answer_key\s*\('
      and f.prosrc ~* 'selected_answer_normalized\s*=\s*ti\.answer_key_normalized'
      as uses_normalized_answer_grading,
    f.prosrc ~* 'where\s+a\.id\s*=\s*p_attempt_id\s*for\s+update'
      as locks_attempt
  from target_functions f
),
duplicate_response_keys as (
  select r.attempt_id, r.test_item_id, r.round_no
  from auto_grading.responses r
  group by r.attempt_id, r.test_item_id, r.round_no
  having count(*) > 1
),
duplicate_internal_item_nos as (
  select ti.test_set_id, ti.item_no
  from auto_grading.test_items ti
  group by ti.test_set_id, ti.item_no
  having count(*) > 1
),
automated_attempt_response_counts as (
  select
    a.id as attempt_id,
    a.status,
    a.current_round,
    a.total_items,
    a.round1_submitted_at,
    a.round2_submitted_at,
    count(r.id) filter (where r.round_no = 1)::integer
      as round1_response_count,
    count(r.id) filter (
      where r.round_no = 1
        and coalesce(r.is_correct, false) = false
    )::integer as round1_wrong_count,
    count(r.id) filter (where r.round_no = 2)::integer
      as round2_response_count
  from auto_grading.attempts a
  join auto_grading.test_sets ts on ts.id = a.test_set_id
  left join auto_grading.responses r on r.attempt_id = a.id
  where ts.source_type is distinct from 'manual'
  group by
    a.id,
    a.status,
    a.current_round,
    a.total_items,
    a.round1_submitted_at,
    a.round2_submitted_at
),
audit_rows as (
  select
    10 as sort_order,
    'function_and_permission_contract'::text as section,
    jsonb_build_object(
      'expected_function_count', 2,
      'actual_function_count', (select count(*) from target_functions),
      'expected_submit_round1_overload_count', 1,
      'actual_submit_round1_overload_count', (
        select count(*) from target_functions
        where function_name = 'submit_round1'
      ),
      'expected_submit_round2_overload_count', 1,
      'actual_submit_round2_overload_count', (
        select count(*) from target_functions
        where function_name = 'submit_round2'
      ),
      'expected_security_definer', true,
      'expected_anon_can_execute', true,
      'functions', coalesce((
        select jsonb_agg(
          jsonb_build_object(
            'function_name', f.function_name,
            'identity_arguments', f.identity_arguments,
            'arguments_with_defaults', f.arguments_with_defaults,
            'result_type', f.result_type,
            'source_md5', f.source_md5,
            'security_definer', f.security_definer,
            'proconfig', f.proconfig,
            'has_expected_identity_arguments', f.has_expected_identity_arguments,
            'returns_jsonb', f.returns_jsonb,
            'public_can_execute', f.public_can_execute,
            'anon_can_execute', f.anon_can_execute,
            'authenticated_can_execute', f.authenticated_can_execute,
            'service_role_can_execute', f.service_role_can_execute
          ) order by f.function_name, f.identity_arguments
        )
        from function_contract f
      ), '[]'::jsonb),
      'permission_contract',
        '학생 공개 화면이 anon 역할로 직접 호출하므로 anon EXECUTE 권한을 유지한다.'
    ) as details

  union all

  select
    20,
    'payload_and_internal_key_contract',
    jsonb_build_object(
      'expected_function_count', 2,
      'functions', coalesce((
        select jsonb_agg(
          jsonb_build_object(
            'function_name', f.function_name,
            'validates_json_array', f.validates_json_array,
            'parses_internal_item_no_payload', f.parses_internal_item_no_payload,
            'rejects_duplicate_item_nos', f.rejects_duplicate_item_nos,
            'joins_internal_item_no', f.joins_internal_item_no,
            'ignores_display_metadata', f.ignores_display_metadata,
            'writes_response_by_test_item_id', f.writes_response_by_test_item_id,
            'upserts_response_identity', f.upserts_response_identity,
            'uses_normalized_answer_grading', f.uses_normalized_answer_grading,
            'locks_attempt', f.locks_attempt
          ) order by f.function_name
        )
        from function_contract f
      ), '[]'::jsonb),
      'expected_all_function_flags', true,
      'submission_key', 'test_items.item_no',
      'storage_key', 'responses.test_item_id',
      'display_metadata_is_submission_key', false,
      'contract',
        '표시번호가 섹션마다 반복되어도 내부 item_no로 문항을 찾고 test_item_id로 응답을 저장한다.'
    )

  union all

  select
    30,
    'round1_transition_contract',
    coalesce((
      select jsonb_build_object(
        'function_exists', true,
        'requires_in_progress', f.prosrc ~ 'ATTEMPT_NOT_IN_PROGRESS',
        'requires_current_round_one', f.prosrc ~ 'ROUND1_ALREADY_SUBMITTED',
        'checks_test_set_item_count',
          f.prosrc ~ 'TEST_SET_ITEM_COUNT_INCONSISTENT',
        'requires_full_payload', f.prosrc ~ 'PAYLOAD_ITEM_COUNT_MISMATCH',
        'rejects_unknown_internal_item_no',
          f.prosrc ~ 'INVALID_ITEM_NO_IN_PAYLOAD',
        'returns_retry_item_nos', f.prosrc ~ '''retry_item_nos''',
        'retry_item_nos_use_internal_item_no',
          f.prosrc ~* 'array_agg\s*\(\s*ti\.item_no\s+order\s+by\s+ti\.item_no',
        'moves_wrong_answers_to_round_two',
          f.prosrc ~* 'v_next_status\s*:=\s*''awaiting_retry'''
          and f.prosrc ~* 'current_round\s*=\s*2',
        'completes_perfect_round_one',
          f.prosrc ~* 'v_next_status\s*:=\s*''completed'''
          and f.prosrc ~* 'completed_at\s*=\s*coalesce',
        'records_round1_submission_time',
          f.prosrc ~* 'round1_submitted_at\s*=\s*coalesce',
        'refreshes_attempt_summary',
          f.prosrc ~* 'refresh_attempt_summary\s*\(\s*p_attempt_id\s*\)',
        'expected_all_round1_flags', true,
        'retry_item_nos_semantics',
          '내부 item_no의 멤버십 집합이다. 화면 표시 순서나 표시번호로 사용하지 않는다.'
      )
      from function_contract f
      where f.function_name = 'submit_round1'
    ), jsonb_build_object('function_exists', false))

  union all

  select
    40,
    'round2_transition_contract',
    coalesce((
      select jsonb_build_object(
        'function_exists', true,
        'requires_awaiting_retry', f.prosrc ~ 'ATTEMPT_NOT_AWAITING_RETRY',
        'requires_current_round_two', f.prosrc ~ 'ROUND2_NOT_ALLOWED',
        'requires_round1_wrong_items', f.prosrc ~ 'NO_RETRY_ITEMS_FOUND',
        'requires_retry_payload_count',
          f.prosrc ~ 'PAYLOAD_ITEM_COUNT_MISMATCH_FOR_ROUND2',
        'accepts_only_round1_wrong_items',
          f.prosrc ~ 'INVALID_RETRY_ITEM_SET'
          and f.prosrc ~* 'r1\.round_no\s*=\s*1'
          and f.prosrc ~* 'r1\.is_correct\s*=\s*false',
        'returns_remaining_item_nos', f.prosrc ~ '''remaining_item_nos''',
        'remaining_item_nos_use_internal_item_no',
          f.prosrc ~* 'array_agg\s*\(\s*ti\.item_no\s+order\s+by\s+ti\.item_no',
        'completes_when_no_wrong_answer_remains',
          f.prosrc ~* 'v_next_status\s*:=\s*''completed''',
        'moves_remaining_wrong_answers_to_review',
          f.prosrc ~* 'v_next_status\s*:=\s*''needs_review''',
        'records_round2_submission_time',
          f.prosrc ~* 'round2_submitted_at\s*=\s*coalesce',
        'records_completion_time',
          f.prosrc ~* 'completed_at\s*=\s*coalesce',
        'refreshes_attempt_summary',
          f.prosrc ~* 'refresh_attempt_summary\s*\(\s*p_attempt_id\s*\)',
        'expected_all_round2_flags', true,
        'remaining_item_nos_semantics',
          '내부 item_no의 멤버십 집합이다. 화면 표시 순서나 표시번호로 사용하지 않는다.'
      )
      from function_contract f
      where f.function_name = 'submit_round2'
    ), jsonb_build_object('function_exists', false))

  union all

  select
    50,
    'response_operational_integrity',
    jsonb_build_object(
      'response_count', (select count(*) from auto_grading.responses),
      'response_without_attempt_count', (
        select count(*)
        from auto_grading.responses r
        left join auto_grading.attempts a on a.id = r.attempt_id
        where a.id is null
      ),
      'response_without_test_item_count', (
        select count(*)
        from auto_grading.responses r
        left join auto_grading.test_items ti on ti.id = r.test_item_id
        where ti.id is null
      ),
      'response_test_set_mismatch_count', (
        select count(*)
        from auto_grading.responses r
        join auto_grading.attempts a on a.id = r.attempt_id
        join auto_grading.test_items ti on ti.id = r.test_item_id
        where ti.test_set_id is distinct from a.test_set_id
      ),
      'duplicate_response_key_count', (
        select count(*) from duplicate_response_keys
      ),
      'duplicate_internal_item_no_pair_count', (
        select count(*) from duplicate_internal_item_nos
      ),
      'invalid_grading_round_count', (
        select count(*)
        from auto_grading.responses r
        where r.round_no not in (1, 2)
      ),
      'round2_without_round1_count', (
        select count(*)
        from auto_grading.responses r2
        left join auto_grading.responses r1
          on r1.attempt_id = r2.attempt_id
         and r1.test_item_id = r2.test_item_id
         and r1.round_no = 1
        where r2.round_no = 2
          and r1.id is null
      ),
      'round2_for_round1_correct_count', (
        select count(*)
        from auto_grading.responses r2
        join auto_grading.responses r1
          on r1.attempt_id = r2.attempt_id
         and r1.test_item_id = r2.test_item_id
         and r1.round_no = 1
        where r2.round_no = 2
          and r1.is_correct = true
      ),
      'submitted_round1_response_count_mismatch_count', (
        select count(*)
        from automated_attempt_response_counts x
        where x.round1_submitted_at is not null
          and x.round1_response_count is distinct from x.total_items
      ),
      'submitted_round2_response_count_mismatch_count', (
        select count(*)
        from automated_attempt_response_counts x
        where x.round2_submitted_at is not null
          and x.round2_response_count is distinct from x.round1_wrong_count
      ),
      'sectioned_response_missing_display_metadata_count', (
        select count(*)
        from auto_grading.responses r
        join auto_grading.test_items ti on ti.id = r.test_item_id
        where ti.section_title is not null
          and (
            ti.display_item_no is null
            or ti.section_order is null
            or ti.display_order is null
          )
      ),
      'expected_all_violation_counts', 0
    )
)
select section, details
from audit_rows
order by sort_order;

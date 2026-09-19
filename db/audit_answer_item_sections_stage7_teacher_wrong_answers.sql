-- ============================================================================
-- audit_answer_item_sections_stage7_teacher_wrong_answers.sql
--
-- 답안 입력폼 문항 섹션 기능 7단계 배포 후 교사용 오답조회 RPC의 표시
-- 메타데이터·정렬·권한 계약과 현재 오답 응답의 저장 무결성을 확인한다.
-- SELECT만 수행하며 데이터와 스키마를 변경하지 않는다.
--
-- 진행 조건:
--   - function_and_permission_contract의 함수 개수가 1이고 권한 계약이 일치
--   - wrong_answer_response_contract의 모든 반환 키 플래그가 true
--   - display_order_contract가 display_order, item_no 순서를 사용
--   - legacy_behavior_contract의 기존 관리자·오답조회 보호 플래그가 모두 true
--   - operational_integrity의 모든 위반·불일치 건수가 0
--
-- 결과 5행의 section / details를 공유한다.
-- ============================================================================

with target_function as (
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
    and p.proname = 'teacher_get_attempt_wrong_answers'
),
section_title_modes as (
  select
    ti.test_set_id,
    bool_or(ti.section_title is null) as has_untitled,
    bool_or(ti.section_title is not null) as has_titled
  from auto_grading.test_items ti
  group by ti.test_set_id
),
section_title_variants as (
  select
    ti.test_set_id,
    ti.section_order,
    count(distinct ti.section_title)::integer as title_count
  from auto_grading.test_items ti
  group by ti.test_set_id, ti.section_order
),
duplicate_display_item_nos as (
  select ti.test_set_id, ti.section_order, ti.display_item_no
  from auto_grading.test_items ti
  group by ti.test_set_id, ti.section_order, ti.display_item_no
  having count(*) > 1
),
duplicate_display_orders as (
  select ti.test_set_id, ti.display_order
  from auto_grading.test_items ti
  group by ti.test_set_id, ti.display_order
  having count(*) > 1
),
wrong_responses as (
  select
    r.id as response_id,
    r.attempt_id,
    r.round_no,
    r.selected_answer_normalized,
    a.test_set_id as attempt_test_set_id,
    ti.test_set_id as item_test_set_id,
    ti.item_no,
    ti.display_item_no,
    ti.section_order,
    ti.section_title,
    ti.display_order,
    ti.answer_key_normalized
  from auto_grading.responses r
  join auto_grading.attempts a on a.id = r.attempt_id
  join auto_grading.test_items ti on ti.id = r.test_item_id
  where r.is_correct = false
),
audit_rows as (
  select
    10 as sort_order,
    'function_and_permission_contract'::text as section,
    jsonb_build_object(
      'expected_overload_count', 1,
      'actual_overload_count', (select count(*) from target_function),
      'expected_identity_arguments', 'p_assignment_id uuid',
      'expected_result_type', 'jsonb',
      'expected_security_definer', true,
      'expected_public_can_execute', false,
      'expected_anon_can_execute', false,
      'expected_authenticated_can_execute', true,
      'expected_service_role_can_execute', true,
      'functions', coalesce((
        select jsonb_agg(
          jsonb_build_object(
            'schema_name', f.schema_name,
            'function_name', f.function_name,
            'identity_arguments', f.identity_arguments,
            'arguments_with_defaults', f.arguments_with_defaults,
            'result_type', f.result_type,
            'source_md5', f.source_md5,
            'security_definer', f.security_definer,
            'proconfig', f.proconfig,
            'public_can_execute', f.public_can_execute,
            'anon_can_execute', f.anon_can_execute,
            'authenticated_can_execute', f.authenticated_can_execute,
            'service_role_can_execute', f.service_role_can_execute
          ) order by f.identity_arguments
        )
        from target_function f
      ), '[]'::jsonb)
    ) as details

  union all

  select
    20,
    'wrong_answer_response_contract',
    coalesce((
      select jsonb_build_object(
        'function_exists', true,
        'returns_round_no', f.prosrc ~ '''round_no''',
        'returns_internal_item_no', f.prosrc ~ '''item_no''',
        'returns_display_item_no', f.prosrc ~ '''display_item_no''',
        'returns_section_order', f.prosrc ~ '''section_order''',
        'returns_section_title', f.prosrc ~ '''section_title''',
        'returns_display_order', f.prosrc ~ '''display_order''',
        'returns_selected_answer', f.prosrc ~ '''selected''',
        'returns_correct_answer', f.prosrc ~ '''correct''',
        'expected_all_response_keys', true,
        'internal_item_no_preserved', true,
        'display_metadata_contract',
          'item_no는 내부 식별자로 유지하고 표시 메타데이터 4개를 별도 반환한다.'
      )
      from target_function f
    ), jsonb_build_object('function_exists', false))

  union all

  select
    30,
    'display_order_contract',
    coalesce((
      select jsonb_build_object(
        'orders_by_round_display_order_internal_item_no',
          f.prosrc ~* 'order\s+by\s+r\.round_no\s*,\s*ti\.display_order\s*,\s*ti\.item_no',
        'orders_by_internal_item_no_only',
          f.prosrc ~* 'order\s+by\s+r\.round_no\s*,\s*ti\.item_no',
        'expected_orders_by_round_display_order_internal_item_no', true,
        'expected_orders_by_internal_item_no_only', false,
        'read_order_contract', 'round_no, display_order, item_no'
      )
      from target_function f
    ), jsonb_build_object('function_exists', false))

  union all

  select
    40,
    'legacy_behavior_contract',
    coalesce((
      select jsonb_build_object(
        'has_assert_admin', f.prosrc ~* 'assert_admin\s*\(',
        'validates_assignment_id',
          f.prosrc ~* 'p_assignment_id\s+is\s+null',
        'selects_latest_attempt',
          f.prosrc ~* 'order\s+by\s+a\.attempt_no\s+desc\s*,\s*a\.created_at\s+desc',
        'returns_empty_array_without_attempt',
          f.prosrc ~* 'return\s+''\[\]''::jsonb',
        'filters_incorrect_responses',
          f.prosrc ~* 'r\.is_correct\s*=\s*false',
        'uses_normalized_selected_answer',
          f.prosrc ~ 'selected_answer_normalized',
        'uses_normalized_correct_answer',
          f.prosrc ~ 'answer_key_normalized',
        'expected_all_legacy_behavior_flags', true
      )
      from target_function f
    ), jsonb_build_object('function_exists', false))

  union all

  select
    50,
    'operational_integrity',
    jsonb_build_object(
      'wrong_response_count', (select count(*) from wrong_responses),
      'sectioned_wrong_response_count', (
        select count(*)
        from wrong_responses x
        where x.section_title is not null
      ),
      'wrong_response_test_set_mismatch_count', (
        select count(*)
        from wrong_responses x
        where x.attempt_test_set_id is distinct from x.item_test_set_id
      ),
      'wrong_response_missing_display_metadata_count', (
        select count(*)
        from wrong_responses x
        where x.display_item_no is null
           or x.section_order is null
           or x.display_order is null
      ),
      'wrong_response_invalid_display_metadata_count', (
        select count(*)
        from wrong_responses x
        where x.item_no < 1
           or x.display_item_no < 1
           or x.section_order < 1
           or x.display_order < 1
      ),
      'wrong_response_selected_matches_correct_count', (
        select count(*)
        from wrong_responses x
        where x.selected_answer_normalized is not null
          and x.selected_answer_normalized = x.answer_key_normalized
      ),
      'wrong_response_invalid_round_count', (
        select count(*)
        from wrong_responses x
        where x.round_no not in (1, 2)
      ),
      'mixed_title_mode_test_set_count', (
        select count(*)
        from section_title_modes x
        where x.has_untitled and x.has_titled
      ),
      'section_title_mismatch_count', (
        select count(*)
        from section_title_variants x
        where x.title_count > 1
      ),
      'duplicate_display_item_no_pair_count', (
        select count(*) from duplicate_display_item_nos
      ),
      'duplicate_display_order_pair_count', (
        select count(*) from duplicate_display_orders
      ),
      'expected_all_violation_counts', 0
    )
)
select section, details
from audit_rows
order by sort_order;

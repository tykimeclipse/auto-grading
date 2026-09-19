-- ============================================================================
-- audit_answer_item_sections_stage3_create_rpc.sql
--
-- 답안 입력폼 문항 섹션 기능 3단계 배포 후 문제지 생성 RPC의 입력·기본값·
-- 저장·권한 계약과 기존 데이터 무결성을 확인한다.
-- SELECT만 수행하며 데이터와 스키마를 변경하지 않는다.
--
-- 진행 조건:
--   - create_rpc_contract의 expected_overload_count = actual_overload_count = 1
--   - public/anon/service_role 실행 권한은 false, authenticated는 true
--   - payload_default_and_write_contract의 모든 expected_* 값과 측정값이 일치
--   - payload_validation_contract의 모든 validation flag가 true
--   - current_storage_integrity의 모든 위반·불일치 건수가 0
--   - total_items_contract의 INSERT 집계·트리거·내부 item_no 계약이 유지
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
    and p.proname in (
      'create_test_set_from_json',
      'submit_round1',
      'submit_round2'
    )
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
duplicate_internal_item_nos as (
  select ti.test_set_id, ti.item_no
  from auto_grading.test_items ti
  group by ti.test_set_id, ti.item_no
  having count(*) > 1
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
test_set_item_counts as (
  select
    ts.id as test_set_id,
    ts.source_type,
    ts.total_items as stored_total_items,
    count(ti.id)::integer as actual_item_count
  from auto_grading.test_sets ts
  left join auto_grading.test_items ti on ti.test_set_id = ts.id
  group by ts.id, ts.source_type, ts.total_items
),
total_item_triggers as (
  select
    t.tgname as trigger_name,
    t.tgenabled as enabled,
    pg_get_triggerdef(t.oid) as definition
  from pg_trigger t
  join pg_class cls on cls.oid = t.tgrelid
  join pg_namespace n on n.oid = cls.relnamespace
  where n.nspname = 'auto_grading'
    and cls.relname = 'test_items'
    and not t.tgisinternal
    and t.tgname in (
      'trg_test_items_refresh_total_items_ins',
      'trg_test_items_refresh_total_items_del',
      'trg_test_items_refresh_total_items_upd'
    )
),
audit_rows as (
  select
    10 as sort_order,
    'create_rpc_contract'::text as section,
    jsonb_build_object(
      'expected_overload_count', 1,
      'actual_overload_count', (
        select count(*)
        from target_functions f
        where f.function_name = 'create_test_set_from_json'
      ),
      'functions', coalesce((
        select jsonb_agg(
          jsonb_build_object(
            'identity_arguments', f.identity_arguments,
            'arguments_with_defaults', f.arguments_with_defaults,
            'result_type', f.result_type,
            'source_md5', f.source_md5,
            'security_definer', f.security_definer,
            'proconfig', f.proconfig,
            'has_assert_admin', f.prosrc ~* 'assert_admin\s*\(',
            'public_can_execute', f.public_can_execute,
            'anon_can_execute', f.anon_can_execute,
            'authenticated_can_execute', f.authenticated_can_execute,
            'service_role_can_execute', f.service_role_can_execute
          ) order by f.identity_arguments
        )
        from target_functions f
        where f.function_name = 'create_test_set_from_json'
      ), '[]'::jsonb),
      'expected_security_definer', true,
      'expected_public_can_execute', false,
      'expected_anon_can_execute', false,
      'expected_authenticated_can_execute', true,
      'expected_service_role_can_execute', false
    ) as details

  union all

  select
    20,
    'payload_default_and_write_contract',
    coalesce((
      select jsonb_build_object(
        'function_exists', true,
        'parses_display_item_no', f.prosrc ~ '''display_item_no''',
        'parses_section_order', f.prosrc ~ '''section_order''',
        'parses_section_title', f.prosrc ~ '''section_title''',
        'parses_display_order', f.prosrc ~ '''display_order''',
        'writes_display_item_no',
          f.prosrc ~* 'insert\s+into\s+auto_grading\.test_items\s*\([^;]*display_item_no',
        'writes_section_order',
          f.prosrc ~* 'insert\s+into\s+auto_grading\.test_items\s*\([^;]*section_order',
        'writes_section_title',
          f.prosrc ~* 'insert\s+into\s+auto_grading\.test_items\s*\([^;]*section_title',
        'writes_display_order',
          f.prosrc ~* 'insert\s+into\s+auto_grading\.test_items\s*\([^;]*display_order',
        'defaults_display_item_no_to_item_no',
          f.prosrc ~* 'coalesce\s*\(\s*nullif\s*\(\s*btrim\s*\(\s*j\.item\s*->>\s*''display_item_no''[^;]*''item_no''',
        'defaults_section_order_to_one',
          f.prosrc ~* 'coalesce\s*\(\s*nullif\s*\(\s*btrim\s*\(\s*j\.item\s*->>\s*''section_order''[^;]*,\s*1\s*\)',
        'normalizes_blank_section_title_to_null',
          f.prosrc ~* 'nullif\s*\(\s*btrim\s*\(\s*j\.item\s*->>\s*''section_title''\s*\)\s*,\s*''''\s*\)',
        'defaults_display_order_to_item_no',
          f.prosrc ~* 'coalesce\s*\(\s*nullif\s*\(\s*btrim\s*\(\s*j\.item\s*->>\s*''display_order''[^;]*''item_no''',
        'preserves_payload_order',
          f.prosrc ~* 'with\s+ordinality[^;]*order\s+by\s+j\.input_order',
        'expected_parses_all_display_metadata', true,
        'expected_writes_all_display_metadata', true,
        'expected_legacy_defaults_present', true,
        'expected_preserves_payload_order', true,
        'legacy_default_contract', jsonb_build_object(
          'display_item_no', 'item_no',
          'section_order', 1,
          'section_title', null,
          'display_order', 'item_no'
        )
      )
      from target_functions f
      where f.function_name = 'create_test_set_from_json'
    ), jsonb_build_object('function_exists', false))

  union all

  select
    30,
    'payload_validation_contract',
    coalesce((
      select jsonb_build_object(
        'validates_object_rows',
          f.prosrc ~ 'P_ITEMS_ROWS_MUST_BE_OBJECTS',
        'validates_required_fields',
          f.prosrc ~ 'P_ITEMS_REQUIRED_FIELDS_MISSING',
        'validates_internal_item_no',
          f.prosrc ~ 'P_ITEMS_ITEM_NO_INVALID',
        'validates_display_metadata',
          f.prosrc ~ 'P_ITEMS_DISPLAY_METADATA_INVALID',
        'validates_choice_count',
          f.prosrc ~ 'P_ITEMS_CHOICE_COUNT_INVALID',
        'validates_section_title_120_characters',
          f.prosrc ~ 'P_ITEMS_SECTION_TITLE_INVALID'
          and f.prosrc ~* 'char_length[^;]*120',
        'section_title_limit_consistent',
          coalesce((
            select bool_and(
              pg_get_constraintdef(con.oid)
                ~* 'char_length\s*\(\s*section_title\s*\)'
              and pg_get_constraintdef(con.oid) ~ '120'
            )
            from pg_constraint con
            where con.conrelid = 'auto_grading.test_items'::regclass
              and con.conname = 'chk_test_items_section_title'
          ), false)
          and coalesce((
            select bool_and(
              rpc.prosrc ~* 'char_length\s*\(\s*btrim\s*\(\s*j\.item\s*->>\s*''section_title''\s*\)\s*\)\s*>\s*120'
            )
            from target_functions rpc
            where rpc.function_name = 'create_test_set_from_json'
          ), false),
        'expected_section_title_limit_consistent', true,
        'choice_count_limit_consistent',
          coalesce((
            select bool_and(
              pg_get_constraintdef(con.oid) ~* 'choice_count\s*>=\s*2'
              and pg_get_constraintdef(con.oid) ~* 'choice_count\s*<=\s*20'
            )
            from pg_constraint con
            where con.conrelid = 'auto_grading.test_items'::regclass
              and con.conname = 'chk_test_items_choice_count'
          ), false)
          and coalesce((
            select bool_and(
              rpc.prosrc ~* 'choice_count[^;]*not\s+between\s+2\s+and\s+20'
            )
            from target_functions rpc
            where rpc.function_name = 'create_test_set_from_json'
          ), false),
        'expected_choice_count_limit_consistent', true,
        'answer_key_not_empty_constraint_exists', exists (
          select 1
          from pg_constraint con
          where con.conrelid = 'auto_grading.test_items'::regclass
            and con.conname = 'chk_test_items_answer_key_normalized_not_empty'
        ),
        'expected_answer_key_not_empty_constraint_exists', true,
        'validates_internal_item_no_uniqueness',
          f.prosrc ~ 'P_ITEMS_ITEM_NO_DUPLICATE',
        'validates_section_display_item_no_uniqueness',
          f.prosrc ~ 'P_ITEMS_SECTION_DISPLAY_ITEM_NO_DUPLICATE',
        'validates_display_order_uniqueness',
          f.prosrc ~ 'P_ITEMS_DISPLAY_ORDER_DUPLICATE',
        'validates_title_mode',
          f.prosrc ~ 'TEST_ITEM_SECTION_TITLE_MODE_MISMATCH',
        'validates_untitled_section_order',
          f.prosrc ~ 'UNTITLED_TEST_ITEM_SECTION_ORDER_INVALID',
        'validates_section_title_consistency',
          f.prosrc ~ 'TEST_ITEM_SECTION_TITLE_MISMATCH',
        'validates_section_first_display_item_no',
          f.prosrc ~ 'SECTION_FIRST_DISPLAY_ITEM_NO_INVALID',
        'expected_all_validation_flags', true,
        'section_title_max_characters', 120,
        'sectioned_csv_requires_each_section_to_start_at_one', true,
        'titleless_csv_requires_first_item_no_one', false
      )
      from target_functions f
      where f.function_name = 'create_test_set_from_json'
    ), jsonb_build_object('function_exists', false))

  union all

  select
    40,
    'current_storage_integrity',
    jsonb_build_object(
      'test_item_count', (select count(*) from auto_grading.test_items),
      'metadata_null_count', (
        select count(*)
        from auto_grading.test_items ti
        where ti.display_item_no is null
           or ti.section_order is null
           or ti.display_order is null
      ),
      'invalid_positive_value_count', (
        select count(*)
        from auto_grading.test_items ti
        where ti.item_no < 1
           or ti.display_item_no < 1
           or ti.section_order < 1
           or ti.display_order < 1
      ),
      'invalid_section_title_count', (
        select count(*)
        from auto_grading.test_items ti
        where ti.section_title is not null
          and (
            ti.section_title <> btrim(ti.section_title)
            or char_length(ti.section_title) not between 1 and 120
          )
      ),
      'empty_normalized_answer_key_count', (
        select count(*)
        from auto_grading.test_items ti
        where ti.answer_key_normalized = ''
      ),
      'untitled_section_order_mismatch_count', (
        select count(*)
        from auto_grading.test_items ti
        where ti.section_title is null
          and ti.section_order <> 1
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
      'duplicate_internal_item_no_pair_count', (
        select count(*) from duplicate_internal_item_nos
      ),
      'duplicate_display_item_no_pair_count', (
        select count(*) from duplicate_display_item_nos
      ),
      'duplicate_display_order_pair_count', (
        select count(*) from duplicate_display_orders
      )
    )

  union all

  select
    50,
    'total_items_and_internal_key_contract',
    jsonb_build_object(
      'rpc_uses_insert_row_count', coalesce((
        select f.prosrc ~* 'get\s+diagnostics\s+v_item_count\s*=\s*row_count'
        from target_functions f
        where f.function_name = 'create_test_set_from_json'
      ), false),
      'expected_rpc_uses_insert_row_count', true,
      'expected_total_item_trigger_count', 3,
      'actual_total_item_trigger_count', (select count(*) from total_item_triggers),
      'total_item_triggers', coalesce((
        select jsonb_agg(to_jsonb(t) order by t.trigger_name)
        from total_item_triggers t
      ), '[]'::jsonb),
      'itemized_total_items_mismatch_count', (
        select count(*)
        from test_set_item_counts x
        where x.source_type is distinct from 'manual'
          and x.stored_total_items is distinct from x.actual_item_count
      ),
      'response_without_test_item_count', (
        select count(*)
        from auto_grading.responses r
        left join auto_grading.test_items ti on ti.id = r.test_item_id
        where ti.id is null
      ),
      'submit_functions_referencing_item_no_count', (
        select count(*)
        from target_functions f
        where f.function_name in ('submit_round1', 'submit_round2')
          and f.prosrc ~ 'item_no'
      ),
      'expected_submit_functions_referencing_item_no_count', 2,
      'item_no_mutation_allowed', false,
      'title_row_storage_contract',
        'CSV 제목 행은 p_items에서 제외되며 실제 INSERT 행만 total_items에 포함한다.'
    )
)
select section, details
from audit_rows
order by sort_order;

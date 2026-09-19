-- ============================================================================
-- audit_answer_item_sections_stage1_schema.sql
--
-- 답안 입력폼 문항 섹션 기능 1단계 배포 후 컬럼·백필·제약·호환 트리거를
-- 확인한다. SELECT만 수행하며 데이터와 스키마를 변경하지 않는다.
--
-- 진행 조건:
--   - schema_contract의 planned_column_count = 4, missing_column_count = 0
--   - backfill_integrity의 모든 *_mismatch_count / *_null_count = 0
--   - section_integrity의 모든 위반 건수 = 0
--   - trigger_contract의 expected_trigger_count = actual_trigger_count = 2
--   - operational_integrity의 orphan/mismatch 건수 = 0
--
-- 결과 6행의 section / details를 공유한다.
-- ============================================================================

with planned_columns as (
  select *
  from (values
    (10, 'display_item_no'::text, 'integer'::text, 'NO'::text),
    (20, 'section_order', 'integer', 'NO'),
    (30, 'section_title', 'text', 'YES'),
    (40, 'display_order', 'integer', 'NO')
  ) as x(sort_order, column_name, expected_data_type, expected_nullable)
),
column_inventory as (
  select
    pc.sort_order,
    pc.column_name,
    pc.expected_data_type,
    pc.expected_nullable,
    c.column_name is not null as exists,
    c.data_type,
    c.is_nullable,
    c.column_default,
    c.ordinal_position
  from planned_columns pc
  left join information_schema.columns c
    on c.table_schema = 'auto_grading'
   and c.table_name = 'test_items'
   and c.column_name = pc.column_name
),
target_constraints as (
  select
    con.conname as constraint_name,
    con.contype as constraint_type,
    con.convalidated as validated,
    pg_get_constraintdef(con.oid) as definition
  from pg_constraint con
  where con.conrelid = 'auto_grading.test_items'::regclass
    and con.conname in (
      'uq_test_items_test_set_item_no',
      'uq_test_items_test_set_section_display_item_no',
      'uq_test_items_test_set_display_order',
      'chk_test_items_item_no',
      'chk_test_items_display_item_no',
      'chk_test_items_section_order',
      'chk_test_items_section_title',
      'chk_test_items_untitled_section_order',
      'chk_test_items_display_order'
    )
),
target_functions as (
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
    has_function_privilege('authenticated', p.oid, 'EXECUTE')
      as authenticated_can_execute,
    has_function_privilege('service_role', p.oid, 'EXECUTE')
      as service_role_can_execute
  from pg_proc p
  join pg_namespace n on n.oid = p.pronamespace
  where n.nspname = 'auto_grading'
    and p.proname in (
      'trg_test_items_prepare_display_metadata',
      'trg_test_items_protect_identity',
      'create_test_set_from_json',
      'submit_round1',
      'submit_round2'
    )
),
target_triggers as (
  select
    t.tgname as trigger_name,
    t.tgenabled as enabled,
    p.proname as function_name,
    pg_get_triggerdef(t.oid) as definition
  from pg_trigger t
  join pg_class cls on cls.oid = t.tgrelid
  join pg_namespace n on n.oid = cls.relnamespace
  join pg_proc p on p.oid = t.tgfoid
  where n.nspname = 'auto_grading'
    and cls.relname = 'test_items'
    and not t.tgisinternal
    and t.tgname in (
      'trg_test_items_prepare_display_metadata',
      'trg_test_items_protect_identity'
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
duplicate_display_item_nos as (
  select
    ti.test_set_id,
    ti.section_order,
    ti.display_item_no,
    count(*)::integer as duplicate_count
  from auto_grading.test_items ti
  group by ti.test_set_id, ti.section_order, ti.display_item_no
  having count(*) > 1
),
duplicate_display_orders as (
  select
    ti.test_set_id,
    ti.display_order,
    count(*)::integer as duplicate_count
  from auto_grading.test_items ti
  group by ti.test_set_id, ti.display_order
  having count(*) > 1
),
test_set_item_counts as (
  select
    ts.id as test_set_id,
    ts.title,
    ts.source_type,
    ts.total_items as stored_total_items,
    count(ti.id)::integer as actual_item_count
  from auto_grading.test_sets ts
  left join auto_grading.test_items ti on ti.test_set_id = ts.id
  group by ts.id, ts.title, ts.source_type, ts.total_items
),
active_attempts as (
  select
    at.id as attempt_id,
    at.test_set_id,
    ts.title as test_title,
    at.status,
    at.current_round,
    at.total_items as attempt_total_items,
    ts.total_items as test_set_total_items,
    count(r.id)::integer as response_count,
    at.started_at,
    at.updated_at
  from auto_grading.attempts at
  join auto_grading.test_sets ts on ts.id = at.test_set_id
  left join auto_grading.responses r on r.attempt_id = at.id
  where at.status in ('in_progress', 'awaiting_retry')
  group by
    at.id,
    at.test_set_id,
    ts.title,
    at.status,
    at.current_round,
    at.total_items,
    ts.total_items,
    at.started_at,
    at.updated_at
),
audit_rows as (
  select
    10 as sort_order,
    'test_item_display_schema_contract'::text as section,
    jsonb_build_object(
      'planned_column_count', (select count(*) from planned_columns),
      'missing_column_count', (
        select count(*) from column_inventory c where not c.exists
      ),
      'column_type_or_nullability_mismatch_count', (
        select count(*)
        from column_inventory c
        where not c.exists
           or c.data_type is distinct from c.expected_data_type
           or c.is_nullable is distinct from c.expected_nullable
      ),
      'columns', coalesce((
        select jsonb_agg(to_jsonb(c) order by c.sort_order)
        from column_inventory c
      ), '[]'::jsonb),
      'expected_constraint_count', 9,
      'actual_constraint_count', (select count(*) from target_constraints),
      'unvalidated_constraint_count', (
        select count(*) from target_constraints c where not c.validated
      ),
      'constraints', coalesce((
        select jsonb_agg(to_jsonb(c) order by c.constraint_name)
        from target_constraints c
      ), '[]'::jsonb),
      'section_title_max_characters', 120,
      'section_title_length_unit', 'btrim 후 문자 수',
      'display_order_contract',
        'item_no는 불변이며 향후 문항 재배치는 display_order만 변경한다.'
    ) as details

  union all

  select
    20,
    'legacy_display_metadata_backfill_integrity',
    jsonb_build_object(
      'test_item_count', count(*),
      'display_item_no_null_count', count(*) filter (
        where ti.display_item_no is null
      ),
      'section_order_null_count', count(*) filter (
        where ti.section_order is null
      ),
      'display_order_null_count', count(*) filter (
        where ti.display_order is null
      ),
      'section_title_nonnull_count', count(*) filter (
        where ti.section_title is not null
      ),
      'display_item_no_item_no_mismatch_count', count(*) filter (
        where ti.display_item_no is distinct from ti.item_no
      ),
      'section_order_not_one_count', count(*) filter (
        where ti.section_order is distinct from 1
      ),
      'display_order_item_no_mismatch_count', count(*) filter (
        where ti.display_order is distinct from ti.item_no
      ),
      'expected_stage1_backfill', jsonb_build_object(
        'display_item_no', 'item_no',
        'section_order', 1,
        'section_title', null,
        'display_order', 'item_no'
      )
    )
  from auto_grading.test_items ti

  union all

  select
    30,
    'section_display_integrity',
    jsonb_build_object(
      'invalid_positive_value_count', (
        select count(*)
        from auto_grading.test_items ti
        where ti.display_item_no < 1
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
      'duplicate_display_item_no_pair_count', (
        select count(*) from duplicate_display_item_nos
      ),
      'duplicate_display_order_pair_count', (
        select count(*) from duplicate_display_orders
      )
    )

  union all

  select
    40,
    'test_item_display_trigger_contract',
    jsonb_build_object(
      'expected_trigger_count', 2,
      'actual_trigger_count', (select count(*) from target_triggers),
      'triggers', coalesce((
        select jsonb_agg(to_jsonb(t) order by t.trigger_name)
        from target_triggers t
      ), '[]'::jsonb),
      'functions', coalesce((
        select jsonb_agg(
          jsonb_build_object(
            'function_name', f.function_name,
            'identity_arguments', f.identity_arguments,
            'source_md5', f.source_md5,
            'security_definer', f.security_definer,
            'fills_display_item_no',
              f.prosrc ~ 'new\.display_item_no\s*:=' ,
            'fills_section_order',
              f.prosrc ~ 'new\.section_order\s*:=' ,
            'fills_display_order',
              f.prosrc ~ 'new\.display_order\s*:=' ,
            'mutates_item_no',
              f.prosrc ~ 'new\.item_no\s*:=' ,
            'locks_test_set_row', f.prosrc ~* 'for\s+update',
            'checks_title_mode',
              f.prosrc ~ 'TEST_ITEM_SECTION_TITLE_MODE_MISMATCH',
            'checks_section_title',
              f.prosrc ~ 'TEST_ITEM_SECTION_TITLE_MISMATCH',
            'protects_identity',
              f.prosrc ~ 'TEST_ITEM_IDENTITY_IMMUTABLE',
            'public_can_execute', f.public_can_execute,
            'anon_can_execute', f.anon_can_execute,
            'authenticated_can_execute', f.authenticated_can_execute,
            'service_role_can_execute', f.service_role_can_execute
          ) order by f.function_name
        )
        from target_functions f
        where f.function_name in (
          'trg_test_items_prepare_display_metadata',
          'trg_test_items_protect_identity'
        )
      ), '[]'::jsonb),
      'expected_function_count', 2,
      'actual_function_count', (
        select count(*)
        from target_functions f
        where f.function_name in (
          'trg_test_items_prepare_display_metadata',
          'trg_test_items_protect_identity'
        )
      ),
      'expected_mutates_item_no', false,
      'frontend_error_mapping_required', jsonb_build_array(
        'TEST_ITEM_SECTION_TITLE_MODE_MISMATCH',
        'UNTITLED_TEST_ITEM_SECTION_ORDER_INVALID',
        'TEST_ITEM_SECTION_TITLE_MISMATCH',
        'TEST_ITEM_IDENTITY_IMMUTABLE'
      )
    )

  union all

  select
    50,
    'legacy_write_path_compatibility',
    coalesce((
      select jsonb_build_object(
        'function_exists', true,
        'source_md5', f.source_md5,
        'security_definer', f.security_definer,
        'inserts_test_items',
          f.prosrc ~* 'insert\s+into\s+auto_grading\.test_items',
        'writes_display_item_no', f.prosrc ~ 'display_item_no',
        'writes_section_order', f.prosrc ~ 'section_order',
        'writes_section_title', f.prosrc ~ 'section_title',
        'writes_display_order', f.prosrc ~ 'display_order',
        'expected_new_metadata_written_by_legacy_rpc', false,
        'compatibility_mechanism',
          'trg_test_items_prepare_display_metadata가 생략된 표시 메타데이터를 보충한다.'
      )
      from target_functions f
      where f.function_name = 'create_test_set_from_json'
    ), jsonb_build_object('function_exists', false))

  union all

  select
    60,
    'stage1_operational_integrity',
    jsonb_build_object(
      'responses_total', (select count(*) from auto_grading.responses),
      'response_without_test_item_count', (
        select count(*)
        from auto_grading.responses r
        left join auto_grading.test_items ti on ti.id = r.test_item_id
        where ti.id is null
      ),
      'itemized_total_items_mismatch_count', (
        select count(*)
        from test_set_item_counts x
        where x.source_type is distinct from 'manual'
          and x.stored_total_items is distinct from x.actual_item_count
      ),
      'active_attempt_count', (select count(*) from active_attempts),
      'in_progress_count', (
        select count(*) from active_attempts x where x.status = 'in_progress'
      ),
      'awaiting_retry_count', (
        select count(*) from active_attempts x where x.status = 'awaiting_retry'
      ),
      'active_attempt_total_items_mismatch_count', (
        select count(*)
        from active_attempts x
        where x.attempt_total_items is distinct from x.test_set_total_items
      ),
      'active_attempts', coalesce((
        select jsonb_agg(to_jsonb(x) order by x.started_at, x.attempt_id)
        from active_attempts x
      ), '[]'::jsonb),
      'submit_functions_using_item_no_count', (
        select count(*)
        from target_functions f
        where f.function_name in ('submit_round1', 'submit_round2')
          and f.prosrc ~ 'item_no'
      ),
      'expected_submit_functions_using_item_no_count', 2
    )
)
select section, details
from audit_rows
order by sort_order;

-- ============================================================================
-- audit_answer_item_sections_stage0_preflight.sql
--
-- 답안 입력폼의 문항 섹션 타이틀·섹션별 표시번호 구현 전 운영 기준선을
-- 확인한다. SELECT만 수행하며 데이터와 스키마를 변경하지 않는다.
--
-- 확인 사항:
--   1. test_items 현재 컬럼·제약·인덱스·트리거와 예정 컬럼 존재 여부
--   2. responses가 test_items.id를 참조하고 item_no를 저장하지 않는지
--   3. 내부 item_no 시작값·빈칸 및 test_sets.total_items 운영 무결성
--   4. 기존 문항을 표시 메타데이터로 보정할 때의 기준값
--   5. 문제지 생성 RPC의 입력·권한 계약
--   6. 시험 시작 RPC의 문항 조회·정렬 계약
--   7. 1·2차 채점 및 교사용 오답조회에서 사용하는 문항 식별 계약
--   8. 오답노트가 문항번호 표시 변경의 직접 영향을 받지 않는지
--   9. 조회 경로 변경 시 보호해야 할 진행 중·재풀이 응시
--
-- 정책 기준:
--   - 기존 test_items.item_no는 응시 생성 후 변경하지 않는 불변 식별값이다.
--   - 제목 없는 기존 CSV는 현재 검증(양의 정수·중복 불가)을 유지한다.
--   - 제목 있는 신규 CSV만 각 섹션의 첫 표시번호를 1로 요구한다.
--   - 첫 번호 이후의 표시번호 빈칸(gap)은 허용한다.
--
-- 결과 9행의 section / details를 공유한다.
-- ============================================================================

with planned_columns as (
  select *
  from (values
    (10, 'item_no'::text, 'integer'::text, false,
      '기존 시험지 내부 채점용 고유번호'),
    (20, 'display_item_no', 'integer', false,
      '섹션 안에서 학생에게 표시할 번호'),
    (30, 'section_order', 'integer', false,
      '시험지 안에서 섹션이 나타나는 순서'),
    (40, 'section_title', 'text', true,
      'CSV 제목 행에서 가져온 섹션 제목'),
    (50, 'display_order', 'integer', false,
      '시험지 전체 출력 순서')
  ) as x(sort_order, column_name, expected_data_type, expected_nullable, purpose)
),
column_inventory as (
  select
    pc.sort_order,
    pc.column_name,
    pc.expected_data_type,
    pc.expected_nullable,
    pc.purpose,
    c.column_name is not null as exists,
    c.data_type,
    c.udt_name,
    c.is_nullable,
    c.column_default,
    c.ordinal_position
  from planned_columns pc
  left join information_schema.columns c
    on c.table_schema = 'auto_grading'
   and c.table_name = 'test_items'
   and c.column_name = pc.column_name
),
target_functions as (
  select
    n.nspname as schema_name,
    p.proname as function_name,
    pg_get_function_identity_arguments(p.oid) as identity_arguments,
    pg_get_function_arguments(p.oid) as arguments_with_defaults,
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
  where n.nspname in ('auto_grading', 'public')
    and p.proname in (
      'create_test_set_from_json',
      'start_attempt',
      'start_attempt_by_test_set',
      'submit_round1',
      'submit_round2',
      'teacher_get_attempt_wrong_answers'
    )
),
response_fk_targets as (
  select
    con.conname as constraint_name,
    con.confrelid as referenced_table_oid,
    con.confrelid::regclass::text as referenced_table,
    jsonb_agg(
      jsonb_build_object(
        'source_column', src_att.attname,
        'referenced_column', dst_att.attname
      ) order by src_key.ordinality
    ) as column_mapping
  from pg_constraint con
  join lateral unnest(con.conkey) with ordinality
    as src_key(attnum, ordinality) on true
  join lateral unnest(con.confkey) with ordinality
    as dst_key(attnum, ordinality)
    on dst_key.ordinality = src_key.ordinality
  join pg_attribute src_att
    on src_att.attrelid = con.conrelid
   and src_att.attnum = src_key.attnum
  join pg_attribute dst_att
    on dst_att.attrelid = con.confrelid
   and dst_att.attnum = dst_key.attnum
  where con.conrelid = to_regclass('auto_grading.responses')
    and con.contype = 'f'
  group by con.conname, con.confrelid
),
mistake_note_functions as (
  select
    p.proname as function_name,
    pg_get_function_identity_arguments(p.oid) as identity_arguments,
    md5(p.prosrc) as source_md5,
    p.prosrc ~ 'item_no' as mentions_item_no,
    p.prosrc ~ 'test_item_id' as mentions_test_item_id,
    p.prosrc ~ 'auto_grading\.responses|\sresponses\s' as mentions_responses
  from pg_proc p
  join pg_namespace n on n.oid = p.pronamespace
  where n.nspname = 'auto_grading'
    and (
      p.proname ~ 'mistake_note'
      or p.prosrc ~ 'auto_grading\.mistake_notes|\smistake_notes\s'
    )
),
test_item_snapshot as (
  -- 예정 컬럼이 아직 없어도 실행되도록 행 전체를 jsonb로 변환해 읽는다.
  select
    ti.id,
    ti.test_set_id,
    ti.item_no,
    ti.choice_count,
    to_jsonb(ti) ? 'display_item_no' as has_display_item_no_key,
    to_jsonb(ti) ? 'section_order' as has_section_order_key,
    to_jsonb(ti) ? 'section_title' as has_section_title_key,
    to_jsonb(ti) ? 'display_order' as has_display_order_key,
    case
      when to_jsonb(ti) ->> 'display_item_no' ~ '^\d+$'
        then (to_jsonb(ti) ->> 'display_item_no')::integer
      else null
    end as display_item_no,
    case
      when to_jsonb(ti) ->> 'section_order' ~ '^\d+$'
        then (to_jsonb(ti) ->> 'section_order')::integer
      else null
    end as section_order,
    nullif(btrim(to_jsonb(ti) ->> 'section_title'), '') as section_title,
    case
      when to_jsonb(ti) ->> 'display_order' ~ '^\d+$'
        then (to_jsonb(ti) ->> 'display_order')::integer
      else null
    end as display_order
  from auto_grading.test_items ti
),
attempt_counts as (
  select
    at.test_set_id,
    count(*)::integer as attempt_count,
    count(*) filter (
      where at.status in ('in_progress', 'awaiting_retry')
    )::integer as resumable_attempt_count,
    count(*) filter (where at.status = 'needs_review')::integer
      as needs_review_count,
    count(*) filter (where at.status = 'completed')::integer
      as completed_count
  from auto_grading.attempts at
  group by at.test_set_id
),
test_set_rollup as (
  select
    ts.id as test_set_id,
    ts.title,
    ts.source_type,
    ts.total_items as stored_total_items,
    count(ti.id)::integer as actual_item_count,
    count(distinct ti.item_no)::integer as distinct_item_no_count,
    min(ti.item_no) as min_item_no,
    max(ti.item_no) as max_item_no,
    case
      when count(ti.id) = 0 then true
      else min(ti.item_no) = 1
    end as first_item_no_is_one,
    case
      when count(ti.id) = 0 then false
      else max(ti.item_no) - min(ti.item_no) + 1
        <> count(distinct ti.item_no)
    end as item_no_has_gap,
    case
      when count(ti.id) = 0 then true
      else min(ti.item_no) = 1
        and max(ti.item_no) = count(ti.id)
        and count(distinct ti.item_no) = count(ti.id)
    end as item_no_is_contiguous,
    coalesce(ac.attempt_count, 0) as attempt_count,
    coalesce(ac.resumable_attempt_count, 0) as resumable_attempt_count,
    coalesce(ac.needs_review_count, 0) as needs_review_count,
    coalesce(ac.completed_count, 0) as completed_count
  from auto_grading.test_sets ts
  left join auto_grading.test_items ti on ti.test_set_id = ts.id
  left join attempt_counts ac on ac.test_set_id = ts.id
  group by
    ts.id,
    ts.title,
    ts.source_type,
    ts.total_items,
    ac.attempt_count,
    ac.resumable_attempt_count,
    ac.needs_review_count,
    ac.completed_count
),
duplicate_internal_item_nos as (
  select
    ti.test_set_id,
    ti.item_no,
    count(*)::integer as duplicate_count
  from auto_grading.test_items ti
  group by ti.test_set_id, ti.item_no
  having count(*) > 1
),
active_attempts as (
  select
    at.id as attempt_id,
    at.assignment_id,
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
    at.assignment_id,
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
    'test_items_schema_baseline'::text as section,
    jsonb_build_object(
      'table_exists', to_regclass('auto_grading.test_items') is not null,
      'rls_enabled', coalesce((
        select cls.relrowsecurity
        from pg_class cls
        join pg_namespace n on n.oid = cls.relnamespace
        where n.nspname = 'auto_grading'
          and cls.relname = 'test_items'
      ), false),
      'planned_columns', coalesce((
        select jsonb_agg(
          jsonb_build_object(
            'column_name', c.column_name,
            'purpose', c.purpose,
            'expected_data_type', c.expected_data_type,
            'expected_nullable', c.expected_nullable,
            'exists', c.exists,
            'data_type', c.data_type,
            'udt_name', c.udt_name,
            'is_nullable', c.is_nullable,
            'column_default', c.column_default,
            'ordinal_position', c.ordinal_position
          ) order by c.sort_order
        )
        from column_inventory c
      ), '[]'::jsonb),
      'constraints', coalesce((
        select jsonb_agg(
          jsonb_build_object(
            'constraint_name', con.conname,
            'constraint_type', con.contype,
            'definition', pg_get_constraintdef(con.oid)
          ) order by con.conname
        )
        from pg_constraint con
        join pg_class cls on cls.oid = con.conrelid
        join pg_namespace n on n.oid = cls.relnamespace
        where n.nspname = 'auto_grading'
          and cls.relname = 'test_items'
      ), '[]'::jsonb),
      'indexes', coalesce((
        select jsonb_agg(
          jsonb_build_object(
            'index_name', i.indexname,
            'index_definition', i.indexdef
          ) order by i.indexname
        )
        from pg_indexes i
        where i.schemaname = 'auto_grading'
          and i.tablename = 'test_items'
      ), '[]'::jsonb),
      'triggers', coalesce((
        select jsonb_agg(
          jsonb_build_object(
            'trigger_name', t.tgname,
            'enabled', t.tgenabled,
            'definition', pg_get_triggerdef(t.oid)
          ) order by t.tgname
        )
        from pg_trigger t
        join pg_class cls on cls.oid = t.tgrelid
        join pg_namespace n on n.oid = cls.relnamespace
        where n.nspname = 'auto_grading'
          and cls.relname = 'test_items'
          and not t.tgisinternal
      ), '[]'::jsonb)
    ) as details

  union all

  select
    15,
    'response_item_reference_contract',
    jsonb_build_object(
      'responses_has_item_no_column', exists (
        select 1
        from information_schema.columns c
        where c.table_schema = 'auto_grading'
          and c.table_name = 'responses'
          and c.column_name = 'item_no'
      ),
      'expected_responses_has_item_no_column', false,
      'responses_has_test_item_id_column', exists (
        select 1
        from information_schema.columns c
        where c.table_schema = 'auto_grading'
          and c.table_name = 'responses'
          and c.column_name = 'test_item_id'
      ),
      'response_fk_targets', coalesce((
        select jsonb_agg(
          jsonb_build_object(
            'constraint_name', x.constraint_name,
            'referenced_table', x.referenced_table,
            'column_mapping', x.column_mapping
          ) order by x.constraint_name
        )
        from response_fk_targets x
      ), '[]'::jsonb),
      'test_item_id_targets_test_items_id', exists (
        select 1
        from response_fk_targets x
        where x.referenced_table_oid = to_regclass('auto_grading.test_items')
          and x.column_mapping @> jsonb_build_array(
            jsonb_build_object(
              'source_column', 'test_item_id',
              'referenced_column', 'id'
            )
          )
      ),
      'response_count', (select count(*) from auto_grading.responses),
      'submit_functions_using_item_no_as_runtime_key', (
        select count(*)
        from target_functions f
        where f.schema_name = 'auto_grading'
          and f.function_name in ('submit_round1', 'submit_round2')
          and f.prosrc ~* 'ti\.item_no\s*=\s*p\.item_no'
      ),
      'expected_submit_functions_using_item_no_as_runtime_key', 2,
      'submit_functions_referencing_item_no', (
        select count(*)
        from target_functions f
        where f.schema_name = 'auto_grading'
          and f.function_name in ('submit_round1', 'submit_round2')
          and f.prosrc ~ 'item_no'
      ),
      'expected_submit_functions_referencing_item_no', 2,
      'existing_item_no_mutation_allowed', false,
      'contract',
        'responses는 test_items.id를 참조하지만 제출·재풀이 API는 item_no를 사용하므로 기존 item_no는 변경하지 않는다.'
    )

  union all

  select
    20,
    'test_item_operational_integrity',
    jsonb_build_object(
      'test_set_count', (select count(*) from auto_grading.test_sets),
      'itemized_test_set_count', (
        select count(*)
        from auto_grading.test_sets ts
        where ts.source_type is distinct from 'manual'
      ),
      'manual_test_set_count', (
        select count(*)
        from auto_grading.test_sets ts
        where ts.source_type = 'manual'
      ),
      'test_item_count', (select count(*) from test_item_snapshot),
      'invalid_internal_item_no_count', (
        select count(*) from test_item_snapshot ti where ti.item_no < 1
      ),
      'duplicate_internal_item_no_pair_count', (
        select count(*) from duplicate_internal_item_nos
      ),
      'duplicate_internal_item_no_details', coalesce((
        select jsonb_agg(to_jsonb(x) order by x.test_set_id, x.item_no)
        from duplicate_internal_item_nos x
      ), '[]'::jsonb),
      'first_item_no_not_one_count', (
        select count(*)
        from test_set_rollup x
        where x.source_type is distinct from 'manual'
          and x.actual_item_count > 0
          and not x.first_item_no_is_one
      ),
      'first_item_no_not_one_test_sets', coalesce((
        select jsonb_agg(
          jsonb_build_object(
            'test_set_id', x.test_set_id,
            'title', x.title,
            'min_item_no', x.min_item_no,
            'max_item_no', x.max_item_no,
            'actual_item_count', x.actual_item_count,
            'attempt_count', x.attempt_count
          ) order by x.title, x.test_set_id
        )
        from test_set_rollup x
        where x.source_type is distinct from 'manual'
          and x.actual_item_count > 0
          and not x.first_item_no_is_one
      ), '[]'::jsonb),
      'has_gap_count', (
        select count(*)
        from test_set_rollup x
        where x.source_type is distinct from 'manual'
          and x.actual_item_count > 0
          and x.item_no_has_gap
      ),
      'has_gap_test_sets', coalesce((
        select jsonb_agg(
          jsonb_build_object(
            'test_set_id', x.test_set_id,
            'title', x.title,
            'min_item_no', x.min_item_no,
            'max_item_no', x.max_item_no,
            'actual_item_count', x.actual_item_count,
            'distinct_item_no_count', x.distinct_item_no_count,
            'attempt_count', x.attempt_count
          ) order by x.title, x.test_set_id
        )
        from test_set_rollup x
        where x.source_type is distinct from 'manual'
          and x.actual_item_count > 0
          and x.item_no_has_gap
      ), '[]'::jsonb),
      'non_contiguous_itemized_test_set_count', (
        select count(*)
        from test_set_rollup x
        where x.source_type is distinct from 'manual'
          and not x.item_no_is_contiguous
      ),
      'non_contiguous_itemized_test_sets', coalesce((
        select jsonb_agg(
          jsonb_build_object(
            'test_set_id', x.test_set_id,
            'title', x.title,
            'stored_total_items', x.stored_total_items,
            'actual_item_count', x.actual_item_count,
            'distinct_item_no_count', x.distinct_item_no_count,
            'min_item_no', x.min_item_no,
            'max_item_no', x.max_item_no,
            'attempt_count', x.attempt_count
          ) order by x.title, x.test_set_id
        )
        from test_set_rollup x
        where x.source_type is distinct from 'manual'
          and not x.item_no_is_contiguous
      ), '[]'::jsonb),
      'csv_validation_policy', jsonb_build_object(
        'titleless_csv_requires_first_item_no_one', false,
        'titleless_csv_allows_gaps', true,
        'sectioned_csv_requires_each_section_to_start_at_one', true,
        'sectioned_csv_allows_gaps_after_first_item', true
      )
    )

  union all

  select
    30,
    'display_metadata_backfill_baseline',
    jsonb_build_object(
      'rows_to_backfill', (select count(*) from test_item_snapshot),
      'expected_backfill', jsonb_build_object(
        'display_item_no', 'item_no',
        'display_order', 'item_no',
        'section_order', 1,
        'section_title', null
      ),
      'display_item_no_column_exists', exists (
        select 1 from column_inventory c
        where c.column_name = 'display_item_no' and c.exists
      ),
      'section_order_column_exists', exists (
        select 1 from column_inventory c
        where c.column_name = 'section_order' and c.exists
      ),
      'section_title_column_exists', exists (
        select 1 from column_inventory c
        where c.column_name = 'section_title' and c.exists
      ),
      'display_order_column_exists', exists (
        select 1 from column_inventory c
        where c.column_name = 'display_order' and c.exists
      ),
      'rows_with_any_preexisting_display_metadata', (
        select count(*)
        from test_item_snapshot ti
        where ti.display_item_no is not null
           or ti.section_order is not null
           or ti.section_title is not null
           or ti.display_order is not null
      ),
      'itemized_total_items_mismatch_count', (
        select count(*)
        from test_set_rollup x
        where x.source_type is distinct from 'manual'
          and x.stored_total_items is distinct from x.actual_item_count
      ),
      'itemized_total_items_mismatches', coalesce((
        select jsonb_agg(
          jsonb_build_object(
            'test_set_id', x.test_set_id,
            'title', x.title,
            'source_type', x.source_type,
            'stored_total_items', x.stored_total_items,
            'actual_item_count', x.actual_item_count,
            'attempt_count', x.attempt_count
          ) order by x.title, x.test_set_id
        )
        from test_set_rollup x
        where x.source_type is distinct from 'manual'
          and x.stored_total_items is distinct from x.actual_item_count
      ), '[]'::jsonb),
      'manual_test_sets_without_test_items', coalesce((
        select jsonb_agg(
          jsonb_build_object(
            'test_set_id', x.test_set_id,
            'title', x.title,
            'stored_total_items', x.stored_total_items,
            'attempt_count', x.attempt_count
          ) order by x.title, x.test_set_id
        )
        from test_set_rollup x
        where x.source_type = 'manual'
          and x.actual_item_count = 0
      ), '[]'::jsonb)
    )

  union all

  select
    40,
    'test_set_creation_write_path_baseline',
    jsonb_build_object(
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
            'has_assert_admin', f.prosrc ~* 'assert_admin\s*\(',
            'parses_item_no', f.prosrc ~ '''item_no''|item_no\s+text',
            'parses_answer_key', f.prosrc ~ '''answer_key''|answer_key\s+text',
            'parses_display_item_no', f.prosrc ~ 'display_item_no',
            'parses_section_order', f.prosrc ~ 'section_order',
            'parses_section_title', f.prosrc ~ 'section_title',
            'parses_display_order', f.prosrc ~ 'display_order',
            'public_can_execute', f.public_can_execute,
            'anon_can_execute', f.anon_can_execute,
            'authenticated_can_execute', f.authenticated_can_execute,
            'service_role_can_execute', f.service_role_can_execute
          ) order by f.schema_name, f.identity_arguments
        )
        from target_functions f
        where f.function_name = 'create_test_set_from_json'
      ), '[]'::jsonb),
      'expected_auto_grading_overload_count', 1,
      'actual_auto_grading_overload_count', (
        select count(*)
        from target_functions f
        where f.schema_name = 'auto_grading'
          and f.function_name = 'create_test_set_from_json'
      )
    )

  union all

  select
    50,
    'attempt_item_read_path_baseline',
    jsonb_build_object(
      'functions', coalesce((
        select jsonb_agg(
          jsonb_build_object(
            'schema_name', f.schema_name,
            'function_name', f.function_name,
            'identity_arguments', f.identity_arguments,
            'result_type', f.result_type,
            'source_md5', f.source_md5,
            'security_definer', f.security_definer,
            'calls_start_attempt', f.prosrc ~ 'start_attempt\s*\(',
            'returns_items', f.prosrc ~ '''items''',
            'returns_item_no', f.prosrc ~ '''item_no''',
            'returns_display_item_no', f.prosrc ~ '''display_item_no''',
            'returns_section_order', f.prosrc ~ '''section_order''',
            'returns_section_title', f.prosrc ~ '''section_title''',
            'returns_display_order', f.prosrc ~ '''display_order''',
            'orders_by_internal_item_no',
              f.prosrc ~* 'order\s+by\s+(ti\.)?item_no',
            'orders_by_display_order',
              f.prosrc ~* 'order\s+by\s+(ti\.)?display_order',
            'public_can_execute', f.public_can_execute,
            'anon_can_execute', f.anon_can_execute,
            'authenticated_can_execute', f.authenticated_can_execute,
            'service_role_can_execute', f.service_role_can_execute
          ) order by f.schema_name, f.function_name, f.identity_arguments
        )
        from target_functions f
        where f.function_name in ('start_attempt', 'start_attempt_by_test_set')
      ), '[]'::jsonb),
      'auto_grading_start_attempt_count', (
        select count(*)
        from target_functions f
        where f.schema_name = 'auto_grading'
          and f.function_name = 'start_attempt'
      ),
      'public_wrapper_count', (
        select count(*)
        from target_functions f
        where f.schema_name = 'public'
          and f.function_name in ('start_attempt', 'start_attempt_by_test_set')
      )
    )

  union all

  select
    60,
    'grading_and_teacher_wrong_answer_contract',
    jsonb_build_object(
      'grading_functions', coalesce((
        select jsonb_agg(
          jsonb_build_object(
            'schema_name', f.schema_name,
            'function_name', f.function_name,
            'identity_arguments', f.identity_arguments,
            'source_md5', f.source_md5,
            'security_definer', f.security_definer,
            'parses_item_no_payload',
              f.prosrc ~* 'jsonb_to_recordset\s*\(\s*p_responses\s*\)',
            'joins_internal_item_no',
              f.prosrc ~* 'ti\.item_no\s*=\s*p\.item_no',
            'mentions_display_metadata',
              f.prosrc ~ 'display_item_no|section_order|section_title|display_order',
            'public_can_execute', f.public_can_execute,
            'anon_can_execute', f.anon_can_execute,
            'authenticated_can_execute', f.authenticated_can_execute,
            'service_role_can_execute', f.service_role_can_execute
          ) order by f.schema_name, f.function_name, f.identity_arguments
        )
        from target_functions f
        where f.function_name in ('submit_round1', 'submit_round2')
      ), '[]'::jsonb),
      'teacher_wrong_answer_function', coalesce((
        select jsonb_agg(
          jsonb_build_object(
            'schema_name', f.schema_name,
            'identity_arguments', f.identity_arguments,
            'source_md5', f.source_md5,
            'security_definer', f.security_definer,
            'has_assert_admin', f.prosrc ~* 'assert_admin\s*\(',
            'returns_internal_item_no', f.prosrc ~ '''item_no''',
            'returns_display_item_no', f.prosrc ~ '''display_item_no''',
            'returns_section_order', f.prosrc ~ '''section_order''',
            'returns_section_title', f.prosrc ~ '''section_title''',
            'returns_display_order', f.prosrc ~ '''display_order''',
            'public_can_execute', f.public_can_execute,
            'anon_can_execute', f.anon_can_execute,
            'authenticated_can_execute', f.authenticated_can_execute,
            'service_role_can_execute', f.service_role_can_execute
          ) order by f.schema_name, f.identity_arguments
        )
        from target_functions f
        where f.function_name = 'teacher_get_attempt_wrong_answers'
      ), '[]'::jsonb),
      'response_count', (select count(*) from auto_grading.responses),
      'response_without_test_item_count', (
        select count(*)
        from auto_grading.responses r
        left join auto_grading.test_items ti on ti.id = r.test_item_id
        where ti.id is null
      )
    )

  union all

  select
    65,
    'mistake_notes_item_number_impact',
    jsonb_build_object(
      'mistake_notes_table_exists',
        to_regclass('auto_grading.mistake_notes') is not null,
      'mistake_notes_has_item_no_column', exists (
        select 1
        from information_schema.columns c
        where c.table_schema = 'auto_grading'
          and c.table_name = 'mistake_notes'
          and c.column_name = 'item_no'
      ),
      'mistake_notes_has_test_item_id_column', exists (
        select 1
        from information_schema.columns c
        where c.table_schema = 'auto_grading'
          and c.table_name = 'mistake_notes'
          and c.column_name = 'test_item_id'
      ),
      'mistake_notes_fk_targets_test_items', exists (
        select 1
        from pg_constraint con
        where con.conrelid = to_regclass('auto_grading.mistake_notes')
          and con.contype = 'f'
          and con.confrelid = to_regclass('auto_grading.test_items')
      ),
      'related_functions', coalesce((
        select jsonb_agg(
          jsonb_build_object(
            'function_name', f.function_name,
            'identity_arguments', f.identity_arguments,
            'source_md5', f.source_md5,
            'mentions_item_no', f.mentions_item_no,
            'mentions_test_item_id', f.mentions_test_item_id,
            'mentions_responses', f.mentions_responses
          ) order by f.function_name, f.identity_arguments
        )
        from mistake_note_functions f
      ), '[]'::jsonb),
      'functions_mentioning_item_no_count', (
        select count(*)
        from mistake_note_functions f
        where f.mentions_item_no
      ),
      'functions_mentioning_test_item_id_count', (
        select count(*)
        from mistake_note_functions f
        where f.mentions_test_item_id
      ),
      'direct_item_number_impact_detected',
        exists (
          select 1
          from information_schema.columns c
          where c.table_schema = 'auto_grading'
            and c.table_name = 'mistake_notes'
            and c.column_name = 'item_no'
        )
        or exists (
          select 1
          from mistake_note_functions f
          where f.mentions_item_no
        ),
      'direct_item_number_impact_expected', false,
      'assessment',
        case
          when exists (
            select 1
            from information_schema.columns c
            where c.table_schema = 'auto_grading'
              and c.table_name = 'mistake_notes'
              and c.column_name = 'item_no'
          ) or exists (
            select 1
            from mistake_note_functions f
            where f.mentions_item_no
          ) then '문항번호 직접 의존이 감지되었으므로 구현 전 별도 검토가 필요하다.'
          else '오답노트는 문항별 번호를 저장·표시하지 않고 responses.test_item_id를 통한 오답 수 계산만 사용한다.'
        end
    )

  union all

  select
    70,
    'active_attempt_section_migration_guard',
    jsonb_build_object(
      'active_attempt_count', (select count(*) from active_attempts),
      'in_progress_count', (
        select count(*) from active_attempts x where x.status = 'in_progress'
      ),
      'awaiting_retry_count', (
        select count(*) from active_attempts x where x.status = 'awaiting_retry'
      ),
      'attempt_total_items_mismatch_count', (
        select count(*)
        from active_attempts x
        where x.attempt_total_items is distinct from x.test_set_total_items
      ),
      'items', coalesce((
        select jsonb_agg(to_jsonb(x) order by x.started_at, x.attempt_id)
        from active_attempts x
      ), '[]'::jsonb),
      'post_deploy_expectation',
        '진행 중·재풀이 응시는 동일한 내부 item_no로 복원되고 표시 메타데이터만 추가되어야 한다.'
    )
)
select section, details
from audit_rows
order by sort_order;

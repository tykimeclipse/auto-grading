-- ============================================================================
-- audit_answer_item_sections_stage8_final_regression.sql
--
-- 답안 입력폼 문항 섹션 기능의 최종 배포 상태를 한 번에 점검한다.
-- 1~7단계의 핵심 계약을 다시 확인하고 최종 release_ready를 반환한다.
-- SELECT만 수행하며 데이터와 스키마를 변경하지 않는다.
--
-- 최종 통과 조건:
--   - schema_contract.contract_passed = true
--   - rpc_contracts의 모든 *_contract_passed = true
--   - storage_integrity의 모든 위반·불일치 건수가 0
--   - active_attempt_integrity의 모든 위반·불일치 건수가 0
--   - final_release_gate.release_ready = true
--   - docs/answer-item-sections-stage8-regression-checklist.md의 12개 시나리오 통과
--
-- 결과 6행의 section / details를 공유한다.
-- ============================================================================

with planned_columns as (
  select *
  from (values
    ('item_no'::text, 'integer'::text, false),
    ('display_item_no', 'integer', false),
    ('section_order', 'integer', false),
    ('section_title', 'text', true),
    ('display_order', 'integer', false)
  ) as x(column_name, expected_data_type, expected_nullable)
),
column_inventory as (
  select
    pc.column_name,
    pc.expected_data_type,
    pc.expected_nullable,
    c.column_name is not null as column_exists,
    c.data_type,
    c.is_nullable,
    c.column_default,
    c.ordinal_position,
    c.column_name is not null
      and c.data_type = pc.expected_data_type
      and (c.is_nullable = 'YES') = pc.expected_nullable
      as contract_matches
  from planned_columns pc
  left join information_schema.columns c
    on c.table_schema = 'auto_grading'
   and c.table_name = 'test_items'
   and c.column_name = pc.column_name
),
planned_constraints as (
  select constraint_name
  from (values
    ('uq_test_items_test_set_item_no'::text),
    ('uq_test_items_test_set_section_display_item_no'),
    ('uq_test_items_test_set_display_order'),
    ('chk_test_items_display_item_no'),
    ('chk_test_items_section_order'),
    ('chk_test_items_section_title'),
    ('chk_test_items_untitled_section_order'),
    ('chk_test_items_display_order')
  ) as x(constraint_name)
),
constraint_inventory as (
  select
    pc.constraint_name,
    con.oid is not null as constraint_exists,
    con.convalidated,
    pg_get_constraintdef(con.oid) as definition
  from planned_constraints pc
  left join pg_constraint con
    on con.conrelid = 'auto_grading.test_items'::regclass
   and con.conname = pc.constraint_name
),
planned_triggers as (
  select trigger_name
  from (values
    ('trg_test_items_prepare_display_metadata'::text),
    ('trg_test_items_protect_identity')
  ) as x(trigger_name)
),
trigger_inventory as (
  select
    pt.trigger_name,
    t.oid is not null as trigger_exists,
    t.tgenabled as enabled,
    pg_get_triggerdef(t.oid) as definition
  from planned_triggers pt
  left join pg_trigger t
    on t.tgrelid = 'auto_grading.test_items'::regclass
   and t.tgname = pt.trigger_name
   and not t.tgisinternal
),
target_functions as (
  select
    n.nspname as schema_name,
    p.proname as function_name,
    pg_get_function_identity_arguments(p.oid) as identity_arguments,
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
      'start_attempt',
      'submit_round1',
      'submit_round2',
      'teacher_get_attempt_wrong_answers'
    )
),
function_counts as (
  select
    x.function_name,
    count(f.function_name)::integer as actual_count
  from (values
    ('create_test_set_from_json'::text),
    ('start_attempt'),
    ('submit_round1'),
    ('submit_round2'),
    ('teacher_get_attempt_wrong_answers')
  ) as x(function_name)
  left join target_functions f on f.function_name = x.function_name
  group by x.function_name
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
ranked_section_items as (
  select
    ti.test_set_id,
    ti.section_order,
    ti.section_title,
    ti.display_item_no,
    row_number() over (
      partition by ti.test_set_id, ti.section_order
      order by ti.display_order, ti.item_no
    ) as section_item_rank
  from auto_grading.test_items ti
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
duplicate_response_keys as (
  select r.attempt_id, r.test_item_id, r.round_no
  from auto_grading.responses r
  group by r.attempt_id, r.test_item_id, r.round_no
  having count(*) > 1
),
active_attempts as (
  select
    a.id as attempt_id,
    a.assignment_id,
    a.course_id,
    a.test_set_id,
    a.status,
    a.current_round,
    a.total_items as attempt_total_items,
    ts.total_items as test_set_total_items
  from auto_grading.attempts a
  join auto_grading.test_sets ts on ts.id = a.test_set_id
  where a.status in ('in_progress', 'awaiting_retry')
),
storage_metrics as (
  select
    (select count(*) from auto_grading.test_items) as test_item_count,
    (
      select count(*)
      from auto_grading.test_items ti
      where ti.display_item_no is null
         or ti.section_order is null
         or ti.display_order is null
    ) as metadata_null_count,
    (
      select count(*)
      from auto_grading.test_items ti
      where ti.item_no < 1
         or ti.display_item_no < 1
         or ti.section_order < 1
         or ti.display_order < 1
    ) as invalid_positive_value_count,
    (
      select count(*)
      from auto_grading.test_items ti
      where ti.section_title is not null
        and (
          ti.section_title <> btrim(ti.section_title)
          or char_length(ti.section_title) not between 1 and 120
        )
    ) as invalid_section_title_count,
    (
      select count(*)
      from auto_grading.test_items ti
      where ti.choice_count not between 2 and 20
    ) as invalid_choice_count,
    (
      select count(*)
      from auto_grading.test_items ti
      where ti.answer_key_normalized = ''
    ) as empty_normalized_answer_key_count,
    (
      select count(*)
      from section_title_modes x
      where x.has_untitled and x.has_titled
    ) as mixed_title_mode_test_set_count,
    (
      select count(*)
      from section_title_variants x
      where x.title_count > 1
    ) as section_title_mismatch_count,
    (
      select count(*)
      from ranked_section_items x
      where x.section_title is not null
        and x.section_item_rank = 1
        and x.display_item_no <> 1
    ) as titled_section_first_item_not_one_count,
    (select count(*) from duplicate_internal_item_nos)
      as duplicate_internal_item_no_pair_count,
    (select count(*) from duplicate_display_item_nos)
      as duplicate_display_item_no_pair_count,
    (select count(*) from duplicate_display_orders)
      as duplicate_display_order_pair_count,
    (
      select count(*)
      from test_set_item_counts x
      where x.source_type is distinct from 'manual'
        and x.stored_total_items is distinct from x.actual_item_count
    ) as itemized_total_items_mismatch_count,
    (
      select count(*)
      from auto_grading.responses r
      left join auto_grading.attempts a on a.id = r.attempt_id
      where a.id is null
    ) as response_without_attempt_count,
    (
      select count(*)
      from auto_grading.responses r
      left join auto_grading.test_items ti on ti.id = r.test_item_id
      where ti.id is null
    ) as response_without_test_item_count,
    (
      select count(*)
      from auto_grading.responses r
      join auto_grading.attempts a on a.id = r.attempt_id
      join auto_grading.test_items ti on ti.id = r.test_item_id
      where ti.test_set_id is distinct from a.test_set_id
    ) as response_test_set_mismatch_count,
    (select count(*) from duplicate_response_keys)
      as duplicate_response_key_count,
    (
      select count(*)
      from auto_grading.responses r
      where r.round_no not in (1, 2)
    ) as invalid_grading_round_count,
    (
      select count(*)
      from auto_grading.responses r
      join auto_grading.test_items ti on ti.id = r.test_item_id
      where r.is_correct = false
        and r.selected_answer_normalized is not null
        and r.selected_answer_normalized = ti.answer_key_normalized
    ) as incorrect_response_selected_matches_correct_count,
    (
      select count(*)
      from auto_grading.responses r2
      left join auto_grading.responses r1
        on r1.attempt_id = r2.attempt_id
       and r1.test_item_id = r2.test_item_id
       and r1.round_no = 1
      where r2.round_no = 2
        and r1.id is null
    ) as round2_without_round1_count,
    (
      select count(*)
      from auto_grading.responses r2
      join auto_grading.responses r1
        on r1.attempt_id = r2.attempt_id
       and r1.test_item_id = r2.test_item_id
       and r1.round_no = 1
      where r2.round_no = 2
        and r1.is_correct = true
    ) as round2_for_round1_correct_count
),
active_metrics as (
  select
    count(*) as active_attempt_count,
    count(*) filter (
      where attempt_total_items is distinct from test_set_total_items
    ) as active_attempt_total_items_mismatch_count,
    count(*) filter (where assignment_id is null)
      as active_attempt_missing_assignment_count,
    count(*) filter (where course_id is null)
      as active_attempt_missing_course_count,
    count(*) filter (
      where (status = 'in_progress' and current_round <> 1)
         or (status = 'awaiting_retry' and current_round <> 2)
    ) as active_attempt_round_state_mismatch_count
  from active_attempts
),
contract_status as (
  select
    coalesce((select bool_and(contract_matches) from column_inventory), false)
      and (
        select count(*)
        from constraint_inventory
        where constraint_exists and convalidated
      ) = 8
      and (
        select count(*)
        from trigger_inventory
        where trigger_exists and enabled <> 'D'
      ) = 2
      as schema_contract_passed,
    coalesce((
      select bool_and(
        f.security_definer
        and f.prosrc ~ '''display_item_no'''
        and f.prosrc ~ '''section_order'''
        and f.prosrc ~ '''section_title'''
        and f.prosrc ~ '''display_order'''
        and f.prosrc ~ 'P_ITEMS_DISPLAY_METADATA_INVALID'
        and f.prosrc ~ 'SECTION_FIRST_DISPLAY_ITEM_NO_INVALID'
        and not f.public_can_execute
        and not f.anon_can_execute
        and f.authenticated_can_execute
        and not f.service_role_can_execute
      )
      from target_functions f
      where f.function_name = 'create_test_set_from_json'
    ), false) as create_rpc_contract_passed,
    coalesce((
      select bool_and(
        f.security_definer
        and f.prosrc ~ '''display_item_no'''
        and f.prosrc ~ '''section_order'''
        and f.prosrc ~ '''section_title'''
        and f.prosrc ~ '''display_order'''
        and f.prosrc ~* 'order\s+by\s+ti\.display_order\s*,\s*ti\.item_no'
        and f.anon_can_execute
      )
      from target_functions f
      where f.function_name = 'start_attempt'
    ), false) as start_attempt_contract_passed,
    coalesce((
      select count(*) = 2
        and bool_and(
          f.security_definer
          and f.prosrc ~* 'ti\.item_no\s*=\s*p\.item_no'
          and not (f.prosrc ~ 'display_item_no|section_order|section_title|display_order')
          and f.prosrc ~* 'on\s+conflict\s*\(\s*attempt_id\s*,\s*test_item_id\s*,\s*round_no\s*\)'
          and f.anon_can_execute
        )
      from target_functions f
      where f.function_name in ('submit_round1', 'submit_round2')
    ), false) as grading_contract_passed,
    coalesce((
      select bool_and(
        f.security_definer
        and f.prosrc ~* 'assert_admin\s*\('
        and f.prosrc ~ '''display_item_no'''
        and f.prosrc ~ '''section_order'''
        and f.prosrc ~ '''section_title'''
        and f.prosrc ~ '''display_order'''
        and f.prosrc ~* 'order\s+by\s+r\.round_no\s*,\s*ti\.display_order\s*,\s*ti\.item_no'
        and not f.public_can_execute
        and not f.anon_can_execute
        and f.authenticated_can_execute
        and f.service_role_can_execute
      )
      from target_functions f
      where f.function_name = 'teacher_get_attempt_wrong_answers'
    ), false) as teacher_wrong_answer_contract_passed,
    (select bool_and(actual_count = 1) from function_counts)
      as expected_function_counts_passed
),
release_status as (
  select
    c.*,
    (
      s.metadata_null_count = 0
      and s.invalid_positive_value_count = 0
      and s.invalid_section_title_count = 0
      and s.invalid_choice_count = 0
      and s.empty_normalized_answer_key_count = 0
      and s.mixed_title_mode_test_set_count = 0
      and s.section_title_mismatch_count = 0
      and s.titled_section_first_item_not_one_count = 0
      and s.duplicate_internal_item_no_pair_count = 0
      and s.duplicate_display_item_no_pair_count = 0
      and s.duplicate_display_order_pair_count = 0
      and s.itemized_total_items_mismatch_count = 0
      and s.response_without_attempt_count = 0
      and s.response_without_test_item_count = 0
      and s.response_test_set_mismatch_count = 0
      and s.duplicate_response_key_count = 0
      and s.invalid_grading_round_count = 0
      and s.incorrect_response_selected_matches_correct_count = 0
      and s.round2_without_round1_count = 0
      and s.round2_for_round1_correct_count = 0
    ) as storage_integrity_passed,
    (
      a.active_attempt_total_items_mismatch_count = 0
      and a.active_attempt_missing_assignment_count = 0
      and a.active_attempt_missing_course_count = 0
      and a.active_attempt_round_state_mismatch_count = 0
    ) as active_attempt_integrity_passed
  from contract_status c
  cross join storage_metrics s
  cross join active_metrics a
),
final_status as (
  select
    r.*,
    (
      r.schema_contract_passed
      and r.create_rpc_contract_passed
      and r.start_attempt_contract_passed
      and r.grading_contract_passed
      and r.teacher_wrong_answer_contract_passed
      and r.expected_function_counts_passed
      and r.storage_integrity_passed
      and r.active_attempt_integrity_passed
    ) as release_ready
  from release_status r
),
audit_rows as (
  select
    10 as sort_order,
    'schema_contract'::text as section,
    jsonb_build_object(
      'contract_passed', (select schema_contract_passed from final_status),
      'columns', coalesce((
        select jsonb_agg(to_jsonb(x) order by x.ordinal_position)
        from column_inventory x
      ), '[]'::jsonb),
      'constraints', coalesce((
        select jsonb_agg(to_jsonb(x) order by x.constraint_name)
        from constraint_inventory x
      ), '[]'::jsonb),
      'triggers', coalesce((
        select jsonb_agg(to_jsonb(x) order by x.trigger_name)
        from trigger_inventory x
      ), '[]'::jsonb)
    ) as details

  union all

  select
    20,
    'rpc_contracts',
    jsonb_build_object(
      'expected_function_counts_passed',
        (select expected_function_counts_passed from final_status),
      'create_rpc_contract_passed',
        (select create_rpc_contract_passed from final_status),
      'start_attempt_contract_passed',
        (select start_attempt_contract_passed from final_status),
      'grading_contract_passed',
        (select grading_contract_passed from final_status),
      'teacher_wrong_answer_contract_passed',
        (select teacher_wrong_answer_contract_passed from final_status),
      'functions', coalesce((
        select jsonb_agg(
          jsonb_build_object(
            'function_name', f.function_name,
            'identity_arguments', f.identity_arguments,
            'result_type', f.result_type,
            'source_md5', f.source_md5,
            'security_definer', f.security_definer,
            'proconfig', f.proconfig,
            'public_can_execute', f.public_can_execute,
            'anon_can_execute', f.anon_can_execute,
            'authenticated_can_execute', f.authenticated_can_execute,
            'service_role_can_execute', f.service_role_can_execute
          ) order by f.function_name, f.identity_arguments
        )
        from target_functions f
      ), '[]'::jsonb)
    )

  union all

  select
    30,
    'storage_integrity',
    to_jsonb(s) || jsonb_build_object(
      'contract_passed', (select storage_integrity_passed from final_status),
      'expected_all_violation_counts', 0
    )
  from storage_metrics s

  union all

  select
    40,
    'active_attempt_integrity',
    to_jsonb(a) || jsonb_build_object(
      'contract_passed', (select active_attempt_integrity_passed from final_status),
      'expected_all_violation_counts', 0
    )
  from active_metrics a

  union all

  select
    50,
    'display_and_identity_policy',
    jsonb_build_object(
      'submission_and_grading_key', 'test_items.item_no',
      'response_storage_key', 'responses.test_item_id',
      'student_and_teacher_read_order', 'display_order, item_no',
      'sectioned_item_label', 'Level 2 - 3번',
      'titleless_item_label', '13번',
      'retry_item_nos_usage', 'internal item_no membership only',
      'item_no_mutation_allowed', false,
      'display_metadata_is_grading_key', false
    )

  union all

  select
    60,
    'final_release_gate',
    jsonb_build_object(
      'release_ready', f.release_ready,
      'schema_contract_passed', f.schema_contract_passed,
      'create_rpc_contract_passed', f.create_rpc_contract_passed,
      'start_attempt_contract_passed', f.start_attempt_contract_passed,
      'grading_contract_passed', f.grading_contract_passed,
      'teacher_wrong_answer_contract_passed',
        f.teacher_wrong_answer_contract_passed,
      'expected_function_counts_passed', f.expected_function_counts_passed,
      'storage_integrity_passed', f.storage_integrity_passed,
      'active_attempt_integrity_passed', f.active_attempt_integrity_passed,
      'manual_scenarios_required', 12,
      'manual_checklist',
        'docs/answer-item-sections-stage8-regression-checklist.md'
    )
  from final_status f
)
select section, details
from audit_rows
order by sort_order;

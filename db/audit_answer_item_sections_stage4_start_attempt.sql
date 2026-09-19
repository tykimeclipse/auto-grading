-- ============================================================================
-- audit_answer_item_sections_stage4_start_attempt.sql
--
-- 답안 입력폼 문항 섹션 기능 4단계 배포 후 start_attempt의 문항 응답·정렬·
-- 상태 복원 계약과 기존 운영 보호 로직을 확인한다.
-- SELECT만 수행하며 데이터와 스키마를 변경하지 않는다.
--
-- 진행 조건:
--   - function_and_wrapper_contract의 auto_grading.start_attempt가 1개
--   - item_response_contract의 신규 키 occurrence_count가 각각 2
--   - display_order_contract의 display_order_clause_count가 3, internal_only가 0
--   - state_and_course_guard_contract의 모든 보호 플래그가 true
--   - operational_integrity의 모든 위반·불일치 건수가 0
--
-- 결과 5행의 section / details를 공유한다.
-- ============================================================================

with target_functions as (
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
  where n.nspname in ('auto_grading', 'public')
    and p.proname in ('start_attempt', 'start_attempt_by_test_set')
),
start_attempt_source as (
  select f.*
  from target_functions f
  where f.schema_name = 'auto_grading'
    and f.function_name = 'start_attempt'
),
item_contract as (
  select
    f.*,
    (
      (length(f.prosrc) - length(replace(f.prosrc, '''item_no''', '')))
      / length('''item_no''')
    )::integer as item_no_key_count,
    (
      (length(f.prosrc) - length(replace(f.prosrc, '''display_item_no''', '')))
      / length('''display_item_no''')
    )::integer as display_item_no_key_count,
    (
      (length(f.prosrc) - length(replace(f.prosrc, '''section_order''', '')))
      / length('''section_order''')
    )::integer as section_order_key_count,
    (
      (length(f.prosrc) - length(replace(f.prosrc, '''section_title''', '')))
      / length('''section_title''')
    )::integer as section_title_key_count,
    (
      (length(f.prosrc) - length(replace(f.prosrc, '''display_order''', '')))
      / length('''display_order''')
    )::integer as display_order_key_count,
    (
      (length(f.prosrc) - length(replace(f.prosrc, '''choice_count''', '')))
      / length('''choice_count''')
    )::integer as choice_count_key_count,
    (
      (length(f.prosrc) - length(replace(f.prosrc, '''allows_multiple''', '')))
      / length('''allows_multiple''')
    )::integer as allows_multiple_key_count,
    (
      (length(f.prosrc) - length(replace(f.prosrc, '''selected_answer''', '')))
      / length('''selected_answer''')
    )::integer as selected_answer_key_count,
    (
      select count(*)::integer
      from regexp_matches(
        f.prosrc,
        'order\s+by\s+ti\.display_order\s*,\s*ti\.item_no',
        'gi'
      )
    ) as display_order_clause_count,
    (
      select count(*)::integer
      from regexp_matches(
        f.prosrc,
        'order\s+by\s+ti\.item_no',
        'gi'
      )
    ) as internal_only_order_clause_count
  from start_attempt_source f
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
active_attempts as (
  select
    at.id as attempt_id,
    at.assignment_id,
    at.course_id,
    at.test_set_id,
    at.status,
    at.current_round,
    at.total_items as attempt_total_items,
    ts.total_items as test_set_total_items,
    count(r.id) filter (where r.round_no = 1)::integer
      as round1_response_count,
    at.started_at,
    at.updated_at
  from auto_grading.attempts at
  join auto_grading.test_sets ts on ts.id = at.test_set_id
  left join auto_grading.responses r on r.attempt_id = at.id
  where at.status in ('in_progress', 'awaiting_retry')
  group by
    at.id,
    at.assignment_id,
    at.course_id,
    at.test_set_id,
    at.status,
    at.current_round,
    at.total_items,
    ts.total_items,
    at.started_at,
    at.updated_at
),
duplicate_round1_responses as (
  select r.attempt_id, r.test_item_id
  from auto_grading.responses r
  where r.round_no = 1
  group by r.attempt_id, r.test_item_id
  having count(*) > 1
),
audit_rows as (
  select
    10 as sort_order,
    'function_and_wrapper_contract'::text as section,
    jsonb_build_object(
      'expected_auto_grading_start_attempt_count', 1,
      'actual_auto_grading_start_attempt_count', (
        select count(*) from start_attempt_source
      ),
      'expected_public_wrapper_count', 2,
      'actual_public_wrapper_count', (
        select count(*)
        from target_functions f
        where f.schema_name = 'public'
          and f.function_name in ('start_attempt', 'start_attempt_by_test_set')
      ),
      'functions', coalesce((
        select jsonb_agg(
          jsonb_build_object(
            'schema_name', f.schema_name,
            'function_name', f.function_name,
            'identity_arguments', f.identity_arguments,
            'result_type', f.result_type,
            'source_md5', f.source_md5,
            'security_definer', f.security_definer,
            'proconfig', f.proconfig,
            'calls_start_attempt', f.prosrc ~ 'start_attempt\s*\(',
            'public_can_execute', f.public_can_execute,
            'anon_can_execute', f.anon_can_execute,
            'authenticated_can_execute', f.authenticated_can_execute,
            'service_role_can_execute', f.service_role_can_execute
          ) order by f.schema_name, f.function_name, f.identity_arguments
        )
        from target_functions f
      ), '[]'::jsonb)
    ) as details

  union all

  select
    20,
    'item_response_contract',
    coalesce((
      select jsonb_build_object(
        'function_exists', true,
        'item_no_key_occurrence_count', x.item_no_key_count,
        'display_item_no_key_occurrence_count', x.display_item_no_key_count,
        'section_order_key_occurrence_count', x.section_order_key_count,
        'section_title_key_occurrence_count', x.section_title_key_count,
        'display_order_key_occurrence_count', x.display_order_key_count,
        'choice_count_key_occurrence_count', x.choice_count_key_count,
        'allows_multiple_key_occurrence_count', x.allows_multiple_key_count,
        'selected_answer_key_occurrence_count', x.selected_answer_key_count,
        'expected_item_key_occurrence_count', 2,
        'expected_display_metadata_key_occurrence_count', 2,
        'expected_existing_item_metadata_key_occurrence_count', 2,
        'round1_restores_selected_answer',
          x.prosrc ~* 'selected_answer_raw',
        'round2_clears_selected_answer',
          x.prosrc ~* '''selected_answer''\s*,\s*''''',
        'returns_items', x.prosrc ~ '''items''',
        'returns_retry_item_nos', x.prosrc ~ '''retry_item_nos''',
        'contract',
          '1차와 2차 모두 내부 item_no와 표시 메타데이터 4개를 반환한다.'
      )
      from item_contract x
    ), jsonb_build_object('function_exists', false))

  union all

  select
    30,
    'display_order_contract',
    coalesce((
      select jsonb_build_object(
        'display_order_clause_count', x.display_order_clause_count,
        'expected_display_order_clause_count', 3,
        'internal_only_order_clause_count', x.internal_only_order_clause_count,
        'expected_internal_only_order_clause_count', 0,
        'orders_retry_item_nos_by_display_order',
          x.prosrc ~* 'array_agg\s*\(\s*ti\.item_no\s+order\s+by\s+ti\.display_order\s*,\s*ti\.item_no',
        'orders_round_items_by_display_order',
          x.display_order_clause_count >= 3,
        'expected_orders_retry_item_nos_by_display_order', true,
        'expected_orders_round_items_by_display_order', true,
        'read_order_contract', 'display_order, item_no'
      )
      from item_contract x
    ), jsonb_build_object('function_exists', false))

  union all

  select
    40,
    'state_and_course_guard_contract',
    coalesce((
      select jsonb_build_object(
        'handles_in_progress', f.prosrc ~ '''in_progress''',
        'handles_awaiting_retry', f.prosrc ~ '''awaiting_retry''',
        'handles_completed', f.prosrc ~ '''completed''',
        'handles_needs_review', f.prosrc ~ '''needs_review''',
        'checks_assignment_closed', f.prosrc ~ 'ASSIGNMENT_CLOSED',
        'requires_course', f.prosrc ~ 'COURSE_REQUIRED',
        'checks_course_active', f.prosrc ~ 'COURSE_INACTIVE',
        'checks_active_enrollment',
          f.prosrc ~ 'STUDENT_NOT_ENROLLED_IN_COURSE',
        'writes_assignment_id', f.prosrc ~ 'assignment_id',
        'writes_course_id', f.prosrc ~ 'course_id',
        'snapshots_total_items', f.prosrc ~ 'total_items',
        'locks_latest_attempt', f.prosrc ~* 'for\s+update',
        'expected_all_state_and_course_guards', true
      )
      from start_attempt_source f
    ), jsonb_build_object('function_exists', false))

  union all

  select
    50,
    'operational_integrity',
    jsonb_build_object(
      'test_item_count', (select count(*) from auto_grading.test_items),
      'metadata_null_count', (
        select count(*)
        from auto_grading.test_items ti
        where ti.display_item_no is null
           or ti.section_order is null
           or ti.display_order is null
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
      'titled_section_first_item_not_one_count', (
        select count(*)
        from ranked_section_items x
        where x.section_title is not null
          and x.section_item_rank = 1
          and x.display_item_no <> 1
      ),
      'duplicate_display_item_no_pair_count', (
        select count(*) from duplicate_display_item_nos
      ),
      'duplicate_display_order_pair_count', (
        select count(*) from duplicate_display_orders
      ),
      'duplicate_round1_response_pair_count', (
        select count(*) from duplicate_round1_responses
      ),
      'active_attempt_count', (select count(*) from active_attempts),
      'active_attempt_total_items_mismatch_count', (
        select count(*)
        from active_attempts x
        where x.attempt_total_items is distinct from x.test_set_total_items
      ),
      'active_attempt_missing_assignment_count', (
        select count(*)
        from active_attempts x
        where x.assignment_id is null
      ),
      'active_attempt_missing_course_count', (
        select count(*)
        from active_attempts x
        where x.course_id is null
      ),
      'active_attempts', coalesce((
        select jsonb_agg(to_jsonb(x) order by x.started_at, x.attempt_id)
        from active_attempts x
      ), '[]'::jsonb)
    )
)
select section, details
from audit_rows
order by sort_order;

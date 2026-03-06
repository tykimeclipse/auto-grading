create or replace function auto_grading.submit_round1(
  p_attempt_id uuid,
  p_responses jsonb
)
returns jsonb
language plpgsql
security definer
set search_path = auto_grading, public
as $$
declare
  v_attempt               auto_grading.attempts%rowtype;
  v_total_items           integer;
  v_actual_item_count     integer;
  v_payload_count         integer;
  v_distinct_item_count   integer;
  v_valid_item_count      integer;
  v_invalid_item_count    integer;
  v_correct_count         integer;
  v_wrong_count           integer;
  v_first_score_percent   numeric(5,2);
  v_next_status           text;
  v_wrong_item_nos        integer[];
begin
  if p_responses is null or jsonb_typeof(p_responses) <> 'array' then
    raise exception 'p_responses must be a JSON array'
      using errcode = '22023';
  end if;

  select a.*
    into v_attempt
  from auto_grading.attempts as a
  where a.id = p_attempt_id
  for update;

  if not found then
    raise exception 'ATTEMPT_NOT_FOUND'
      using errcode = 'P0001';
  end if;

  if v_attempt.status <> 'in_progress' then
    raise exception 'ATTEMPT_NOT_IN_PROGRESS'
      using errcode = 'P0001';
  end if;

  if coalesce(v_attempt.current_round, 1) <> 1 then
    raise exception 'ROUND1_ALREADY_SUBMITTED'
      using errcode = 'P0001';
  end if;

  select ts.total_items
    into v_total_items
  from auto_grading.test_sets as ts
  where ts.id = v_attempt.test_set_id;

  if v_total_items is null then
    raise exception 'TEST_SET_NOT_FOUND'
      using errcode = 'P0001';
  end if;

  if v_total_items <= 0 then
    raise exception 'EMPTY_TEST_SET'
      using errcode = 'P0001';
  end if;

  select count(*)
    into v_actual_item_count
  from auto_grading.test_items as ti
  where ti.test_set_id = v_attempt.test_set_id;

  if v_actual_item_count <> v_total_items then
    raise exception 'TEST_SET_ITEM_COUNT_INCONSISTENT'
      using errcode = 'P0001';
  end if;

  with parsed as (
    select
      (x.item_no)::integer as item_no,
      nullif(btrim(x.answer), '') as selected_answer_raw
    from jsonb_to_recordset(p_responses) as x(item_no text, answer text)
  )
  select
    count(*),
    count(distinct item_no)
  into
    v_payload_count,
    v_distinct_item_count
  from parsed;

  if v_payload_count <> v_distinct_item_count then
    raise exception 'DUPLICATE_ITEM_NO_IN_PAYLOAD'
      using errcode = 'P0001';
  end if;

  if v_payload_count <> v_total_items then
    raise exception 'PAYLOAD_ITEM_COUNT_MISMATCH'
      using errcode = 'P0001';
  end if;

  with parsed as (
    select
      (x.item_no)::integer as item_no,
      nullif(btrim(x.answer), '') as selected_answer_raw
    from jsonb_to_recordset(p_responses) as x(item_no text, answer text)
  )
  select count(*)
    into v_valid_item_count
  from parsed as p
  join auto_grading.test_items as ti
    on ti.test_set_id = v_attempt.test_set_id
   and ti.item_no = p.item_no;

  if v_valid_item_count <> v_payload_count then
    v_invalid_item_count := v_payload_count - v_valid_item_count;

    raise exception 'INVALID_ITEM_NO_IN_PAYLOAD: % item(s) not found in test set',
      v_invalid_item_count
      using errcode = 'P0001';
  end if;

  with parsed as (
    select
      (x.item_no)::integer as item_no,
      nullif(btrim(x.answer), '') as selected_answer_raw
    from jsonb_to_recordset(p_responses) as x(item_no text, answer text)
  ),
  normalized as (
    select
      ti.id as test_item_id,
      ti.item_no,
      p.selected_answer_raw,
      auto_grading.normalize_answer_key(p.selected_answer_raw) as selected_answer_normalized
    from parsed as p
    join auto_grading.test_items as ti
      on ti.test_set_id = v_attempt.test_set_id
     and ti.item_no = p.item_no
  )
  insert into auto_grading.responses (
    attempt_id,
    test_item_id,
    round_no,
    selected_answer_raw,
    selected_answer_normalized,
    is_correct
  )
  select
    p_attempt_id,
    n.test_item_id,
    1,
    n.selected_answer_raw,
    n.selected_answer_normalized,
    (
      n.selected_answer_normalized is not null
      and n.selected_answer_normalized = ti.answer_key_normalized
    ) as is_correct
  from normalized as n
  join auto_grading.test_items as ti
    on ti.id = n.test_item_id
  on conflict (attempt_id, test_item_id, round_no)
  do update
    set selected_answer_raw        = excluded.selected_answer_raw,
        selected_answer_normalized = excluded.selected_answer_normalized,
        is_correct                 = excluded.is_correct;

  select
    count(*) filter (where coalesce(r.is_correct, false) = true),
    coalesce(
      array_agg(ti.item_no order by ti.item_no)
      filter (where coalesce(r.is_correct, false) = false),
      '{}'::integer[]
    )
  into
    v_correct_count,
    v_wrong_item_nos
  from auto_grading.test_items as ti
  left join auto_grading.responses as r
    on r.attempt_id = p_attempt_id
   and r.test_item_id = ti.id
   and r.round_no = 1
  where ti.test_set_id = v_attempt.test_set_id;

  v_wrong_count := v_total_items - v_correct_count;

  v_first_score_percent := round(
    (v_correct_count::numeric / nullif(v_total_items::numeric, 0)) * 100,
    2
  );

  if v_wrong_count = 0 then
    v_next_status := 'completed';

    update auto_grading.attempts as a
       set current_round       = 1,
           first_score_percent = v_first_score_percent,
           final_score_percent = v_first_score_percent,
           status              = v_next_status
     where a.id = p_attempt_id;
  else
    v_next_status := 'awaiting_retry';

    update auto_grading.attempts as a
       set current_round       = 2,
           first_score_percent = v_first_score_percent,
           status              = v_next_status
     where a.id = p_attempt_id;
  end if;

  return jsonb_build_object(
    'attempt_id', p_attempt_id,
    'status', v_next_status,
    'current_round', case when v_wrong_count = 0 then 1 else 2 end,
    'total_items', v_total_items,
    'correct_count', v_correct_count,
    'wrong_count', v_wrong_count,
    'first_score_percent', v_first_score_percent,
    'retry_item_nos', to_jsonb(v_wrong_item_nos)
  );
end;
$$;
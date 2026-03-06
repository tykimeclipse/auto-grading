create or replace function auto_grading.submit_round2(
  p_attempt_id uuid,
  p_responses jsonb
)
returns jsonb
language plpgsql
security definer
set search_path = auto_grading, public
as $$
declare
  v_attempt                  auto_grading.attempts%rowtype;
  v_total_items              integer;
  v_payload_count            integer;
  v_distinct_item_count      integer;
  v_valid_retry_item_count   integer;
  v_round1_wrong_count       integer;
  v_round1_correct_count     integer;
  v_round2_correct_count     integer;
  v_final_correct_count      integer;
  v_wrong_count              integer;
  v_final_score_percent      numeric(5,2);
  v_next_status              text;
  v_remaining_item_nos       integer[];
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

  if v_attempt.status <> 'awaiting_retry' then
    raise exception 'ATTEMPT_NOT_AWAITING_RETRY'
      using errcode = 'P0001';
  end if;

  if coalesce(v_attempt.current_round, 0) <> 2 then
    raise exception 'ROUND2_NOT_ALLOWED'
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

  select
    count(*) filter (where r.is_correct = false),
    count(*) filter (where r.is_correct = true)
  into
    v_round1_wrong_count,
    v_round1_correct_count
  from auto_grading.responses as r
  join auto_grading.test_items as ti
    on ti.id = r.test_item_id
  where r.attempt_id = p_attempt_id
    and r.round_no = 1
    and ti.test_set_id = v_attempt.test_set_id;

  if v_round1_wrong_count = 0 then
    raise exception 'NO_RETRY_ITEMS_FOUND'
      using errcode = 'P0001';
  end if;

  if v_payload_count <> v_round1_wrong_count then
    raise exception 'PAYLOAD_ITEM_COUNT_MISMATCH_FOR_ROUND2'
      using errcode = 'P0001';
  end if;

  with parsed as (
    select
      (x.item_no)::integer as item_no,
      nullif(btrim(x.answer), '') as selected_answer_raw
    from jsonb_to_recordset(p_responses) as x(item_no text, answer text)
  )
  select count(*)
    into v_valid_retry_item_count
  from parsed as p
  join auto_grading.test_items as ti
    on ti.test_set_id = v_attempt.test_set_id
   and ti.item_no = p.item_no
  join auto_grading.responses as r1
    on r1.test_item_id = ti.id
   and r1.attempt_id = p_attempt_id
   and r1.round_no = 1
   and r1.is_correct = false;

  if v_valid_retry_item_count <> v_payload_count then
    raise exception 'INVALID_RETRY_ITEM_SET'
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
    join auto_grading.responses as r1
      on r1.test_item_id = ti.id
     and r1.attempt_id = p_attempt_id
     and r1.round_no = 1
     and r1.is_correct = false
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
    2,
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

  select count(*)
    into v_round2_correct_count
  from auto_grading.responses as r
  join auto_grading.test_items as ti
    on ti.id = r.test_item_id
  where r.attempt_id = p_attempt_id
    and r.round_no = 2
    and r.is_correct = true
    and ti.test_set_id = v_attempt.test_set_id;

  v_final_correct_count := v_round1_correct_count + v_round2_correct_count;
  v_wrong_count := v_total_items - v_final_correct_count;

  v_final_score_percent := round(
    (v_final_correct_count::numeric / nullif(v_total_items::numeric, 0)) * 100,
    2
  );

  select coalesce(
           array_agg(ti.item_no order by ti.item_no),
           '{}'::integer[]
         )
    into v_remaining_item_nos
  from auto_grading.responses as r1
  join auto_grading.test_items as ti
    on ti.id = r1.test_item_id
  -- 방어적 처리: round2 응답 row가 없는 비정상 상황도 오답으로 간주하기 위해 LEFT JOIN 사용
  left join auto_grading.responses as r2
    on r2.attempt_id = r1.attempt_id
   and r2.test_item_id = r1.test_item_id
   and r2.round_no = 2
  where r1.attempt_id = p_attempt_id
    and r1.round_no = 1
    and r1.is_correct = false
    and ti.test_set_id = v_attempt.test_set_id
    and coalesce(r2.is_correct, false) = false;

  if v_wrong_count = 0 then
    v_next_status := 'completed';
  else
    v_next_status := 'needs_review';
  end if;

  update auto_grading.attempts as a
     set current_round       = 2,
         final_score_percent = v_final_score_percent,
         status              = v_next_status
   where a.id = p_attempt_id;

  return jsonb_build_object(
    'attempt_id', p_attempt_id,
    'status', v_next_status,
    'current_round', 2,
    'total_items', v_total_items,
    'round1_correct_count', v_round1_correct_count,
    'round2_correct_count', v_round2_correct_count,
    'final_correct_count', v_final_correct_count,
    'wrong_count', v_wrong_count,
    'final_score_percent', v_final_score_percent,
    'remaining_item_nos', to_jsonb(v_remaining_item_nos)
  );
end;
$$;
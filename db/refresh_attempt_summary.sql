create or replace function auto_grading.refresh_attempt_summary(
  p_attempt_id uuid
)
returns void
language plpgsql
security definer
set search_path = auto_grading, public
as $$
declare
  v_total_items integer := 0;
  v_first_correct_count integer := 0;
  v_second_correct_count integer := 0;
  v_final_correct_count integer := 0;
begin
  if not exists (
    select 1
    from auto_grading.attempts
    where id = p_attempt_id
  ) then
    raise exception 'ATTEMPT_NOT_FOUND: %', p_attempt_id
      using errcode = 'P0002';
  end if;

  select
    a.total_items,
    count(*) filter (where r.round_no = 1 and r.is_correct = true),
    count(*) filter (where r.round_no = 2 and r.is_correct = true)
  into
    v_total_items,
    v_first_correct_count,
    v_second_correct_count
  from auto_grading.attempts a
  left join auto_grading.responses r
    on r.attempt_id = a.id
  where a.id = p_attempt_id
  group by a.total_items;

  v_final_correct_count := v_first_correct_count + v_second_correct_count;

  update auto_grading.attempts a
  set
    first_correct_count = v_first_correct_count,
    second_correct_count = v_second_correct_count,
    final_correct_count = v_final_correct_count,
    first_score_percent = case
      when a.total_items = 0 then null
      else round((v_first_correct_count::numeric / a.total_items::numeric) * 100, 2)
    end,
    final_score_percent = case
      when a.total_items = 0 then null
      else round((v_final_correct_count::numeric / a.total_items::numeric) * 100, 2)
    end,
    updated_at = now()
  where a.id = p_attempt_id;
end;
$$;
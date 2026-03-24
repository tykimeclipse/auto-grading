create or replace function auto_grading.trg_attempts_sync_assignment_close_after()
returns trigger
language plpgsql
security definer
set search_path to 'auto_grading', 'public'
as $function$
declare
  v_latest_attempt_id uuid;
  v_latest_attempt_created_at timestamptz;
  v_should_close boolean := false;
  v_close_reason text := null;

  v_current_closed_at timestamptz;
  v_current_closed_reason text;
  v_current_is_auto_managed boolean := false;

  v_new_closed_at timestamptz;
  v_new_closed_reason text;
begin
  if new.assignment_id is null then
    return null;
  end if;

  select
    m.attempt_id,
    m.attempt_created_at
  into
    v_latest_attempt_id,
    v_latest_attempt_created_at
  from auto_grading.get_latest_attempt_meta(new.assignment_id) m;

  if v_latest_attempt_id is null
     or v_latest_attempt_id <> new.id then
    return null;
  end if;

  if coalesce(new.first_score_percent, -1) >= 100 then
    v_should_close := true;
    v_close_reason := 'auto_completed_round1';
  elsif coalesce(new.final_score_percent, -1) >= 100 then
    v_should_close := true;
    v_close_reason := 'auto_completed_round2';
  else
    v_should_close := false;
    v_close_reason := null;
  end if;

  select
    a.closed_at,
    a.closed_reason
  into
    v_current_closed_at,
    v_current_closed_reason
  from auto_grading.assignments a
  where a.id = new.assignment_id
  for update;

  if not found then
    return null;
  end if;

  v_current_is_auto_managed :=
    coalesce(v_current_closed_reason, '') in (
      'auto_completed_round1',
      'auto_completed_round2',
      'teacher_review_completed'
    );

  if v_current_closed_at is not null
     and not v_current_is_auto_managed then
    v_new_closed_at := v_current_closed_at;
    v_new_closed_reason := v_current_closed_reason;
  else
    if v_should_close then
      v_new_closed_reason := v_close_reason;

      if v_current_closed_at is not null
         and v_current_closed_reason is not distinct from v_close_reason then
        v_new_closed_at := v_current_closed_at;
      else
        v_new_closed_at := now();
      end if;
    else
      v_new_closed_at := null;
      v_new_closed_reason := null;
    end if;
  end if;

  update auto_grading.assignments a
     set closed_at = v_new_closed_at,
         closed_reason = v_new_closed_reason,
         updated_at = now()
   where a.id = new.assignment_id;

  return null;
end;
$function$;
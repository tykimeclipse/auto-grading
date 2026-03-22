create or replace function auto_grading.get_latest_attempt_meta(p_assignment_id uuid)
returns table(
  attempt_id uuid,
  attempt_created_at timestamptz
)
language sql
security definer
set search_path to 'auto_grading', 'public'
as $function$
  select
    x.id as attempt_id,
    x.created_at as attempt_created_at
  from auto_grading.attempts x
  where x.assignment_id = p_assignment_id
  order by
    x.created_at desc nulls last,
    x.id desc
  limit 1;
$function$;

create or replace function auto_grading.trg_attempts_apply_auto_status_before()
returns trigger
language plpgsql
security definer
set search_path to 'auto_grading', 'public'
as $function$
declare
  v_latest_attempt_id uuid;
  v_latest_attempt_created_at timestamptz;
begin
  if new.assignment_id is null then
    return new;
  end if;

  if tg_op = 'INSERT' then
    new.created_at := coalesce(new.created_at, now());
  end if;

  select
    m.attempt_id,
    m.attempt_created_at
  into
    v_latest_attempt_id,
    v_latest_attempt_created_at
  from auto_grading.get_latest_attempt_meta(new.assignment_id) m;

  if tg_op = 'UPDATE' then
    if v_latest_attempt_id is not null
       and v_latest_attempt_id <> new.id then
      return new;
    end if;
  elsif tg_op = 'INSERT' then
    if v_latest_attempt_id is not null
       and (
         coalesce(v_latest_attempt_created_at, '-infinity'::timestamptz),
         coalesce(v_latest_attempt_id, '00000000-0000-0000-0000-000000000000'::uuid)
       ) > (
         coalesce(new.created_at, '-infinity'::timestamptz),
         coalesce(new.id, '00000000-0000-0000-0000-000000000000'::uuid)
       ) then
      return new;
    end if;
  end if;

  if coalesce(new.first_score_percent, -1) >= 100 then
    new.status := 'completed';
  elsif coalesce(new.final_score_percent, -1) >= 100 then
    new.status := 'completed';
  elsif new.teacher_final_score_percent is not null then
    new.status := 'completed';
  elsif tg_op = 'UPDATE'
    and coalesce(old.status::text, '') = 'completed' then
    new.status := 'needs_review';
  end if;

  return new;
end;
$function$;

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
  elsif new.teacher_final_score_percent is not null then
    v_should_close := true;
    v_close_reason := 'teacher_review_completed';
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

drop trigger if exists trg_attempts_apply_auto_status_before
  on auto_grading.attempts;

create trigger trg_attempts_apply_auto_status_before
before insert or update of first_score_percent, final_score_percent, teacher_final_score_percent
on auto_grading.attempts
for each row
execute function auto_grading.trg_attempts_apply_auto_status_before();

drop trigger if exists trg_attempts_sync_assignment_close_after
  on auto_grading.attempts;

create trigger trg_attempts_sync_assignment_close_after
after insert or update of first_score_percent, final_score_percent, teacher_final_score_percent
on auto_grading.attempts
for each row
execute function auto_grading.trg_attempts_sync_assignment_close_after();

comment on function auto_grading.get_latest_attempt_meta(uuid)
is 'assignment별 최신 attempt 1개를 created_at desc, id desc 기준으로 반환';

comment on function auto_grading.trg_attempts_apply_auto_status_before()
is '최신 attempt 기준 자동 상태 규칙. 1차100/2차100/teacher_final 저장 시 completed, 그 외 completed 원인이 사라지면 needs_review 복귀';

comment on function auto_grading.trg_attempts_sync_assignment_close_after()
is '최신 attempt 기준 assignment 자동 닫힘 규칙. 수동으로 닫힌 assignment는 자동 트리거가 reopen/overwrite하지 않음';



``` 기존 데이터 백필용: ``` sql
with
  latest as (
    select
      distinct on (at.assignment_id) at.assignment_id,
      case
        when coalesce(at.first_score_percent, -1) >= 100 then true
        when coalesce(at.final_score_percent, -1) >= 100 then true
        when at.teacher_final_score_percent is not null then true
        else false
      end as should_close,
      case
        when coalesce(at.first_score_percent, -1) >= 100 then 'auto_completed_round1'
        when coalesce(at.final_score_percent, -1) >= 100 then 'auto_completed_round2'
        when at.teacher_final_score_percent is not null then 'teacher_review_completed'
        else null
      end as close_reason
    from
      auto_grading.attempts at
    where
      at.assignment_id is not null
    order by
      at.assignment_id,
      at.created_at desc nulls last,
      at.id desc
  ),
  resolved as (
    select
      a.id as assignment_id,
      case
        when a.closed_at is not null
        and a.closed_reason not in (
          'auto_completed_round1',
          'auto_completed_round2',
          'teacher_review_completed'
        ) then a.closed_at
        when a.closed_at is not null
        and a.closed_reason is null then a.closed_at
        when l.should_close
        and a.closed_at is not null
        and a.closed_reason is not distinct
        from
          l.close_reason then a.closed_at
          when l.should_close then now()
          else null
      end as new_closed_at,
      case
        when a.closed_at is not null
        and a.closed_reason not in (
          'auto_completed_round1',
          'auto_completed_round2',
          'teacher_review_completed'
        ) then a.closed_reason
        when a.closed_at is not null
        and a.closed_reason is null then a.closed_reason
        when l.should_close then l.close_reason
        else null
      end as new_closed_reason
    from
      auto_grading.assignments a
      join latest l on l.assignment_id = a.id
  )
update
  auto_grading.assignments a
set
  closed_at = r.new_closed_at,
  closed_reason = r.new_closed_reason,
  updated_at = now()
from
  resolved r
where
  a.id = r.assignment_id
  and (
    a.closed_at is distinct
    from
      r.new_closed_at
      or a.closed_reason is distinct
    from
      r.new_closed_reason
  );


``` 확인용 조회: ``` sql
select
  a.id as assignment_id,
  a.closed_at,
  a.closed_reason,
  at.id as attempt_id,
  at.status,
  at.first_score_percent,
  at.final_score_percent,
  at.teacher_final_score_percent
from
  auto_grading.assignments a
  left join lateral (
    select
      x.*
    from
      auto_grading.attempts x
    where
      x.assignment_id = a.id
    order by
      x.created_at desc nulls last,
      x.id desc
    limit
      1
  ) at on true
where
  a.id = '여기에-assignment-id';
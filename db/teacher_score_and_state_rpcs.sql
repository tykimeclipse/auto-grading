-- ================================================================
-- teacher_score_and_state_rpcs.sql
-- teacher_list_assignments.sql 에서 분리한 2개 RPC
--
-- ※ teacher_list_assignments 함수는 teacher_list_assignments_v2.sql
--    에서 관리하므로 이 파일에는 포함하지 않음.
--    teacher_list_assignments.sql(v1) 전체 실행은 v2 함수를 덮어쓰므로
--    실행 금지.
--
-- 실행 순서:
--   1. assert_admin.sql          (선행 필수)
--   2. 이 파일                   (teacher_save_final_score, teacher_set_assignment_open_state)
-- ================================================================


-- ================================================================
-- 1. teacher_save_final_score
--    교사 최종 점수 저장 / 수정 / 삭제(clear)
--    - current/latest attempt 에만 허용
--    - completed / needs_review 상태에서만 저장 허용
--    - 입력은 correct_count 기준; score_percent는 total_items로 자동 계산
-- ================================================================
drop function if exists auto_grading.teacher_save_final_score(uuid, integer, text, boolean);

create or replace function auto_grading.teacher_save_final_score(
  p_attempt_id                  uuid,
  p_teacher_final_correct_count integer default null,
  p_teacher_final_note          text    default null,
  p_clear                       boolean default false
)
returns jsonb
language plpgsql
security definer
set search_path to 'auto_grading', 'public'
as $function$
declare
  v_total_items        integer;
  v_status             text;
  v_assignment_id      uuid;
  v_latest_attempt_id  uuid;
  v_result             auto_grading.attempts%rowtype;
  v_score              numeric(6,2);
begin
  perform auto_grading.assert_admin();
  if p_attempt_id is null then
    raise exception 'p_attempt_id is required';
  end if;

  select
    a.total_items,
    coalesce(to_jsonb(a)->>'status', ''),
    a.assignment_id
  into
    v_total_items,
    v_status,
    v_assignment_id
  from auto_grading.attempts a
  where a.id = p_attempt_id
  for update;

  if not found then
    raise exception 'attempt not found: %', p_attempt_id;
  end if;

  if v_assignment_id is null then
    raise exception 'assignment_id is missing for attempt %', p_attempt_id;
  end if;

  select x.id
    into v_latest_attempt_id
  from auto_grading.attempts x
  where x.assignment_id = v_assignment_id
  order by
    nullif(coalesce(to_jsonb(x)->>'created_at', ''), '')::timestamptz desc nulls last,
    x.id desc
  limit 1;

  if v_latest_attempt_id is null then
    raise exception 'latest attempt not found for assignment %', v_assignment_id;
  end if;

  if v_latest_attempt_id <> p_attempt_id then
    raise exception
      'teacher final can be saved only on current/latest attempt. latest_attempt_id=%, requested_attempt_id=%',
      v_latest_attempt_id,
      p_attempt_id;
  end if;

  if p_clear then
    update auto_grading.attempts
       set teacher_final_correct_count = null,
           teacher_final_score_percent = null,
           teacher_final_note          = null,
           teacher_final_updated_at    = now(),
           teacher_final_updated_by    = auth.uid(),
           updated_at                  = now()
     where id = p_attempt_id
     returning * into v_result;

    return jsonb_build_object(
      'ok',                          true,
      'attempt_id',                  v_result.id,
      'assignment_id',               v_result.assignment_id,
      'teacher_final_correct_count', v_result.teacher_final_correct_count,
      'teacher_final_score_percent', v_result.teacher_final_score_percent,
      'teacher_final_note',          v_result.teacher_final_note,
      'teacher_final_updated_at',    v_result.teacher_final_updated_at,
      'teacher_final_updated_by',    v_result.teacher_final_updated_by,
      'message',                     'teacher final score cleared'
    );
  end if;

  if v_status not in ('completed', 'needs_review') then
    raise exception
      'teacher final can be saved only for completed/needs_review attempts. current status=%',
      v_status;
  end if;

  if p_teacher_final_correct_count is null then
    raise exception 'p_teacher_final_correct_count is required when p_clear = false';
  end if;

  if v_total_items is null or v_total_items <= 0 then
    raise exception 'total_items is missing for attempt %', p_attempt_id;
  end if;

  if p_teacher_final_correct_count < 0 or p_teacher_final_correct_count > v_total_items then
    raise exception
      'teacher_final_correct_count must be between 0 and % (attempt_id=%)',
      v_total_items,
      p_attempt_id;
  end if;

  v_score := round((p_teacher_final_correct_count::numeric * 100.0) / v_total_items, 2);

  update auto_grading.attempts
     set teacher_final_correct_count = p_teacher_final_correct_count,
         teacher_final_score_percent = v_score,
         teacher_final_note          = nullif(btrim(p_teacher_final_note), ''),
         teacher_final_updated_at    = now(),
         teacher_final_updated_by    = auth.uid(),
         updated_at                  = now()
   where id = p_attempt_id
   returning * into v_result;

  return jsonb_build_object(
    'ok',                          true,
    'attempt_id',                  v_result.id,
    'assignment_id',               v_result.assignment_id,
    'total_items',                 v_result.total_items,
    'round1_correct_count',        v_result.first_correct_count,
    'round1_score_percent',        v_result.first_score_percent,
    'round2_correct_count',        v_result.final_correct_count,
    'round2_score_percent',        v_result.final_score_percent,
    'teacher_final_correct_count', v_result.teacher_final_correct_count,
    'teacher_final_score_percent', v_result.teacher_final_score_percent,
    'teacher_final_note',          v_result.teacher_final_note,
    'teacher_final_updated_at',    v_result.teacher_final_updated_at,
    'teacher_final_updated_by',    v_result.teacher_final_updated_by,
    'status',                      to_jsonb(v_result)->>'status',
    'message',                     'teacher final score saved'
  );

exception
  when others then
    return jsonb_build_object(
      'ok',         false,
      'attempt_id', p_attempt_id,
      'error',      sqlerrm
    );
end;
$function$;

grant execute on function auto_grading.teacher_save_final_score(uuid, integer, text, boolean)
  to authenticated, service_role;

comment on function auto_grading.teacher_save_final_score(uuid, integer, text, boolean)
  is '교사용 최종 점수 저장·수정·삭제. latest attempt + completed/needs_review 상태에서만 저장 허용. p_clear=true 시 teacher_final 전체 초기화.';


-- ================================================================
-- 2. teacher_set_assignment_open_state
--    assignment 열림 / 닫힘 토글
-- ================================================================
drop function if exists auto_grading.teacher_set_assignment_open_state(uuid, boolean, text);

create or replace function auto_grading.teacher_set_assignment_open_state(
  p_assignment_id uuid,
  p_is_open       boolean,
  p_closed_reason text default null
)
returns jsonb
language plpgsql
security definer
set search_path to 'auto_grading', 'public'
as $function$
declare
  v_result auto_grading.assignments%rowtype;
begin
  perform auto_grading.assert_admin();
  if p_assignment_id is null then
    raise exception 'p_assignment_id is required';
  end if;

  if p_is_open is null then
    raise exception 'p_is_open is required';
  end if;

  update auto_grading.assignments
     set closed_at     = case when p_is_open then null else now() end,
         closed_reason = case when p_is_open then null else nullif(btrim(p_closed_reason), '') end,
         updated_at    = now()
   where id = p_assignment_id
   returning * into v_result;

  if not found then
    raise exception 'assignment not found: %', p_assignment_id;
  end if;

  return jsonb_build_object(
    'ok',           true,
    'assignment_id', v_result.id,
    'is_open',      p_is_open,
    'closed_at',    v_result.closed_at,
    'closed_reason', v_result.closed_reason,
    'message',      case when p_is_open then 'assignment opened' else 'assignment closed' end
  );

exception
  when others then
    return jsonb_build_object(
      'ok',            false,
      'assignment_id', p_assignment_id,
      'error',         sqlerrm
    );
end;
$function$;

grant execute on function auto_grading.teacher_set_assignment_open_state(uuid, boolean, text)
  to authenticated, service_role;

comment on function auto_grading.teacher_set_assignment_open_state(uuid, boolean, text)
  is '교사용 assignment 열림/닫힘 전환. p_is_open=true → closed_at/closed_reason 초기화. p_is_open=false → closed_at=now(), closed_reason 저장.';

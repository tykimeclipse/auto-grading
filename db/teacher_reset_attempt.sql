-- ================================================================
-- teacher_reset_attempt_round2 : 2차 입력 초기화
-- teacher_reset_attempt_full   : 전체 초기화 (1차+2차)
--
-- 보안: assignment_id + reset_token 조합 검증
--   · reset_token 은 assignments.reset_token (UUID v4 랜덤값)
--   · 학생용 RPC 에는 노출되지 않으므로, 학생이 assignment_id 만
--     알아도 호출 불가
--   · add_assignments_reset_token.sql 을 먼저 실행해야 함
--
-- trigger 고려 사항
--   trg_attempts_apply_auto_status_before 는
--   first_score_percent / final_score_percent / teacher_final_score_percent
--   컬럼 UPDATE 시에만 발동한다.
--   점수 초기화(①)와 status·round 복원(②)을 두 단계로 분리해
--   ②에서 원하는 status 를 trigger 간섭 없이 직접 고정한다.
-- ================================================================

drop function if exists auto_grading.teacher_reset_attempt_round2(uuid, uuid);
drop function if exists auto_grading.teacher_reset_attempt_full(uuid, uuid);

-- 구 시그니처(token 없는 버전)도 제거
drop function if exists auto_grading.teacher_reset_attempt_round2(uuid);
drop function if exists auto_grading.teacher_reset_attempt_full(uuid);


-- ----------------------------------------------------------------
-- 2차 입력 초기화
-- 허용 상태: completed, needs_review
-- · 2차 responses 삭제
-- · attempt → awaiting_retry / current_round=2 복귀
-- · final_* 컬럼 0으로 초기화 (아직 최종 미확정 상태 명시)
-- · teacher_final 동시 초기화
-- · assignment 자동 재오픈
-- ----------------------------------------------------------------
create or replace function auto_grading.teacher_reset_attempt_round2(
  p_assignment_id uuid,
  p_reset_token   uuid
)
returns jsonb
language plpgsql
security definer
set search_path to 'auto_grading', 'public'
as $function$
declare
  v_attempt auto_grading.attempts%rowtype;
begin
  perform auto_grading.assert_admin();
  if p_assignment_id is null then
    raise exception 'p_assignment_id is required';
  end if;

  -- reset_token 검증
  if not exists (
    select 1
    from auto_grading.assignments
    where id           = p_assignment_id
      and reset_token  = p_reset_token
  ) then
    raise exception '유효하지 않은 reset_token입니다.';
  end if;

  -- 최신 attempt 행 잠금
  select at.*
  into v_attempt
  from auto_grading.attempts at
  where at.assignment_id = p_assignment_id
  order by at.created_at desc nulls last, at.id desc
  limit 1
  for update;

  if not found then
    raise exception '해당 assignment의 attempt가 없습니다.';
  end if;

  if v_attempt.status not in ('completed', 'needs_review') then
    raise exception '2차 초기화는 completed 또는 needs_review 상태에서만 가능합니다. (현재: %)',
      v_attempt.status;
  end if;

  if v_attempt.round2_submitted_at is null then
    raise exception '2차 제출 기록이 없습니다. 초기화할 2차 응답이 없습니다.';
  end if;

  -- 2차 응답 삭제
  delete from auto_grading.responses
  where attempt_id = v_attempt.id
    and round_no = 2;

  -- ① 점수·teacher_final 초기화
  --   final_score_percent = 0 : "최종 미확정" 상태를 명시 (awaiting_retry 가 통계 필터에서 제외됨)
  --   (final_score_percent 변경 → trigger 발동 → completed 였다면 needs_review 전이)
  update auto_grading.attempts
  set
    final_score_percent          = 0,
    second_correct_count         = 0,
    final_correct_count          = 0,
    incorrect_count_after_round2 = 0,
    teacher_final_correct_count  = null,
    teacher_final_score_percent  = null,
    teacher_final_note           = null,
    teacher_final_updated_at     = null,
    teacher_final_updated_by     = null
  where id = v_attempt.id;

  -- ② status·round 복원 (trigger 비대상 컬럼 → trigger 발동 없음)
  update auto_grading.attempts
  set
    status              = 'awaiting_retry',
    current_round       = 2,
    round2_submitted_at = null,
    completed_at        = null,
    reopen_count        = reopen_count + 1
  where id = v_attempt.id;

  -- assignment 재오픈
  -- (trg_attempts_sync_assignment_close_after 는 auto-managed 닫힘만 해제하므로
  --  수동 닫힘도 여기서 명시적으로 열어 줌)
  update auto_grading.assignments
  set
    closed_at     = null,
    closed_reason = null,
    updated_at    = now()
  where id = p_assignment_id;

  return jsonb_build_object(
    'ok',            true,
    'assignment_id', p_assignment_id,
    'attempt_id',    v_attempt.id,
    'reset_mode',    'round2',
    'message',       '2차 입력이 초기화되었습니다. 학생이 OMR 화면을 새로고침하면 다시 입력할 수 있습니다.'
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


-- ----------------------------------------------------------------
-- 전체 초기화 (1차+2차)
-- 허용 상태: in_progress, awaiting_retry, completed, needs_review
-- · 전체 responses 삭제
-- · attempt → in_progress / current_round=1 복귀
-- · teacher_final 동시 초기화
-- · assignment 자동 재오픈
-- ----------------------------------------------------------------
create or replace function auto_grading.teacher_reset_attempt_full(
  p_assignment_id uuid,
  p_reset_token   uuid
)
returns jsonb
language plpgsql
security definer
set search_path to 'auto_grading', 'public'
as $function$
declare
  v_attempt auto_grading.attempts%rowtype;
begin
  perform auto_grading.assert_admin();
  if p_assignment_id is null then
    raise exception 'p_assignment_id is required';
  end if;

  -- reset_token 검증
  if not exists (
    select 1
    from auto_grading.assignments
    where id           = p_assignment_id
      and reset_token  = p_reset_token
  ) then
    raise exception '유효하지 않은 reset_token입니다.';
  end if;

  -- 최신 attempt 행 잠금
  select at.*
  into v_attempt
  from auto_grading.attempts at
  where at.assignment_id = p_assignment_id
  order by at.created_at desc nulls last, at.id desc
  limit 1
  for update;

  if not found then
    raise exception '해당 assignment의 attempt가 없습니다.';
  end if;

  -- 전체 응답 삭제 (1차·2차 모두)
  delete from auto_grading.responses
  where attempt_id = v_attempt.id;

  -- ① 점수·teacher_final 초기화
  --   first/final_score_percent = 0 : NOT NULL 제약 대응
  --   통계 쿼리는 status 로 필터링하므로 in_progress 상태는 집계에서 제외됨
  --   (first_score_percent, final_score_percent 변경 → trigger 발동)
  update auto_grading.attempts
  set
    first_score_percent          = 0,
    final_score_percent          = 0,
    first_correct_count          = 0,
    second_correct_count         = 0,
    final_correct_count          = 0,
    unanswered_count_round1      = 0,
    incorrect_count_after_round2 = 0,
    teacher_final_correct_count  = null,
    teacher_final_score_percent  = null,
    teacher_final_note           = null,
    teacher_final_updated_at     = null,
    teacher_final_updated_by     = null
  where id = v_attempt.id;

  -- ② status·round 복원 (trigger 비대상 컬럼 → trigger 발동 없음)
  update auto_grading.attempts
  set
    status              = 'in_progress',
    current_round       = 1,
    round1_submitted_at = null,
    round2_submitted_at = null,
    completed_at        = null,
    reopen_count        = reopen_count + 1
  where id = v_attempt.id;

  -- assignment 재오픈
  update auto_grading.assignments
  set
    closed_at     = null,
    closed_reason = null,
    updated_at    = now()
  where id = p_assignment_id;

  return jsonb_build_object(
    'ok',            true,
    'assignment_id', p_assignment_id,
    'attempt_id',    v_attempt.id,
    'reset_mode',    'full',
    'message',       '1차부터 전체 초기화되었습니다. 학생이 OMR 화면을 새로고침하면 처음부터 다시 입력할 수 있습니다.'
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


grant execute on function auto_grading.teacher_reset_attempt_round2(uuid, uuid) to authenticated, service_role;
grant execute on function auto_grading.teacher_reset_attempt_full(uuid, uuid) to authenticated, service_role;

comment on function auto_grading.teacher_reset_attempt_round2(uuid, uuid)
is '교사용 2차 입력 초기화. assignment_id + reset_token 검증 후 2차 responses 삭제, awaiting_retry 복귀. teacher_final 동시 초기화. assignment 자동 재오픈.';

comment on function auto_grading.teacher_reset_attempt_full(uuid, uuid)
is '교사용 전체 초기화. assignment_id + reset_token 검증 후 전체 responses 삭제, in_progress/round1 복귀. teacher_final 동시 초기화. assignment 자동 재오픈.';

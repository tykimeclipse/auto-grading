-- ================================================================
-- teacher_finalize_attempt : 2차 점수를 최종 성취도로 확정하고 종료
--
-- 목적:
--   교사 리뷰 없이 마무리할 때 round2 점수(final_correct_count /
--   final_score_percent)를 teacher_final_* 로 복사하고 assignment를 닫는다.
--
-- 적용 조건:
--   · status = 'needs_review'              (2차 완료 but 미확정)
--   · teacher_final_correct_count IS NULL  (교사 수동 수정 없음)
--   · round2 데이터 존재 여부는 호출 전 프론트에서 보장
--
-- 보안:
--   assignment_id + reset_token 조합 검증 (reset 계열 RPC 동일 방식)
--
-- 최신 attempt 조회 기준:
--   order by created_at desc nulls last, id desc
--   → get_latest_attempt_meta() 및 teacher_reset_attempt.sql 과 동일한 기준.
--
-- closed_reason = 'teacher_confirmed' 트리거 상호작용:
--   trg_attempts_sync_assignment_close_after 의 auto-managed 목록
--   ('auto_completed_round1', 'auto_completed_round2', 'teacher_review_completed')
--   에 'teacher_confirmed' 는 포함되지 않는다. 따라서 이 함수가 assignment를 닫은 후,
--   이후 트리거가 발동해도 "수동 닫힘"으로 인식하여 재오픈·덮어쓰기하지 않는다.
--   이는 의도된 동작이다 (교사 확정 이후 자동 상태 변화에 의한 재오픈 방지).
--
-- 트리거 실행 순서 (attempts UPDATE 시):
--   1. trg_attempts_apply_auto_status_before : status 갱신
--      (teacher_final_score_percent 세팅 → 100점이면 completed 전이 가능)
--   2. 본 함수의 ② : assignments.closed_at = now(), closed_reason = 'teacher_confirmed'
--      트리거 실행(step 1) 이후 assignments를 명시 UPDATE하므로 충돌 없음.
-- ================================================================

drop function if exists auto_grading.teacher_finalize_attempt(uuid, uuid);

create or replace function auto_grading.teacher_finalize_attempt(
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
  -- 파라미터 검증
  if p_assignment_id is null then
    raise exception 'p_assignment_id is required';
  end if;

  -- reset_token 검증
  if not exists (
    select 1
    from auto_grading.assignments
    where id          = p_assignment_id
      and reset_token = p_reset_token
  ) then
    raise exception '유효하지 않은 reset_token입니다.';
  end if;

  -- 최신 attempt 조회
  -- 기준: created_at desc nulls last, id desc
  --   → get_latest_attempt_meta() / teacher_reset_attempt.sql 과 동일
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

  -- 적용 조건 체크
  if v_attempt.status <> 'needs_review' then
    raise exception '확정은 needs_review 상태에서만 가능합니다. (현재: %)',
      v_attempt.status;
  end if;

  if v_attempt.teacher_final_correct_count is not null then
    raise exception '이미 교사 최종점수가 입력되어 있습니다. 수정 기능을 사용해 주세요.';
  end if;

  if v_attempt.final_correct_count is null or v_attempt.final_score_percent is null then
    raise exception '2차 점수 데이터가 없습니다.';
  end if;

  -- ① teacher_final_* 에 round2 누적값 복사
  --   (teacher_final_score_percent 업데이트 → trg_attempts_apply_auto_status_before 발동)
  update auto_grading.attempts
  set
    teacher_final_correct_count = v_attempt.final_correct_count,
    teacher_final_score_percent = v_attempt.final_score_percent,
    teacher_final_updated_at    = now(),
    teacher_final_updated_by    = null  -- 현 단계에서는 교사 식별(로그인) 미사용
  where id = v_attempt.id;

  -- ② assignment 닫기
  --   closed_reason = 'teacher_confirmed' 는 트리거 auto-managed 목록 외부에 위치하므로
  --   이후 트리거가 이 값을 "수동 닫힘"으로 간주, 재오픈·덮어쓰기하지 않음 (의도된 동작).
  update auto_grading.assignments
  set
    closed_at     = now(),
    closed_reason = 'teacher_confirmed',
    updated_at    = now()
  where id = p_assignment_id;

  return jsonb_build_object(
    'ok',                          true,
    'assignment_id',               p_assignment_id,
    'attempt_id',                  v_attempt.id,
    'confirmed_correct_count',     v_attempt.final_correct_count,
    'confirmed_score_percent',     v_attempt.final_score_percent,
    'message',                     '2차 성취도를 최종 성취도로 확정하고 종료했습니다.'
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

grant execute on function auto_grading.teacher_finalize_attempt(uuid, uuid)
  to anon, authenticated, service_role;

comment on function auto_grading.teacher_finalize_attempt(uuid, uuid)
  is '교사용. needs_review 상태의 attempt에서 2차 점수를 teacher_final로 복사하고 assignment를 닫는다. closed_reason = ''teacher_confirmed'' 은 트리거 auto-managed 사유가 아니므로 이후 자동 재오픈되지 않는다.';

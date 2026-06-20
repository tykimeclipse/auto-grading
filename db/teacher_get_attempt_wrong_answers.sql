-- ================================================================
-- teacher_get_attempt_wrong_answers.sql
-- 과제 관리 화면(teacher-assignment-management)의 '맞은 갯수' 셀 hover 시
-- 해당 발행(assignment)의 틀린 문항 번호 / 학생답 / 정답을 라운드별로 조회.
--
-- lazy 호출 전용: 교사가 셀에 hover 할 때만 1건씩 조회된다.
-- security definer + assert_admin (관리자 화면), anon/public 차단.
--
-- 반환: jsonb 배열. 각 원소
--   { round_no, item_no, selected, correct }
--   - is_correct=false 인 응답만 (틀린 문항)
--   - selected 가 null 이면 무응답
--   - 프론트는 round_no 로 1차/2차 셀에 나눠 표로 렌더
-- 대상 attempt 는 해당 assignment 의 최신(attempt_no desc) 1건.
-- ================================================================

create or replace function auto_grading.teacher_get_attempt_wrong_answers(
  p_assignment_id uuid
)
returns jsonb
language plpgsql
security definer
set search_path to 'auto_grading', 'public'
as $function$
declare
  v_attempt_id uuid;
  v_result jsonb;
begin
  perform auto_grading.assert_admin();

  if p_assignment_id is null then
    raise exception 'p_assignment_id is required';
  end if;

  select a.id
    into v_attempt_id
  from auto_grading.attempts a
  where a.assignment_id = p_assignment_id
  order by a.attempt_no desc, a.created_at desc
  limit 1;

  if v_attempt_id is null then
    return '[]'::jsonb;
  end if;

  select coalesce(
           jsonb_agg(
             jsonb_build_object(
               'round_no', r.round_no,
               'item_no',  ti.item_no,
               'selected', r.selected_answer_normalized,
               'correct',  ti.answer_key_normalized
             )
             order by r.round_no, ti.item_no
           ),
           '[]'::jsonb
         )
    into v_result
  from auto_grading.responses r
  join auto_grading.test_items ti on ti.id = r.test_item_id
  where r.attempt_id = v_attempt_id
    and r.is_correct = false;

  return v_result;
end;
$function$;

-- 관리자 전용 조회: anon/public 차단, authenticated 만 호출 가능
revoke execute on function auto_grading.teacher_get_attempt_wrong_answers(uuid) from public, anon;
grant  execute on function auto_grading.teacher_get_attempt_wrong_answers(uuid) to authenticated;

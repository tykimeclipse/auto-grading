-- ================================================================
-- get_student_stats_by_code
-- 학생 성취도 상단 카드(전체 누적 1차/2차/최종) 조회 계약.
--
-- 6단계 part 2B 이후 집계 공식은
-- auto_grading._student_achievement_stats_core 에서 단일 관리한다.
-- 이 파일은 기존 함수 이름·인자·반환 payload를 그대로 보존한다.
-- 2026-09-18 현재 저장소 내 UI 호출자는 없다. 공개 성취도·학생 관리는
-- 신규 범위 RPC를 사용하며, 이 함수는 롤백 안전망으로 유지해 코어에 위임한다.
-- 선행 배포: course_enrollment_achievement_stage6_part2a_course_achievement.sql
-- ================================================================
create or replace function auto_grading.get_student_stats_by_code(
  p_student_code text
)
returns jsonb
language plpgsql
security definer
set search_path to 'auto_grading', 'public'
as $function$
declare
  v_student_id uuid;
begin
  -- 기존 계약과 동일하게 is_active = true인 학생만 허용한다.
  select s.id
    into v_student_id
  from auto_grading.students s
  where s.student_code = p_student_code
    and s.is_active = true;

  if v_student_id is null then
    raise exception 'STUDENT_NOT_FOUND'
      using errcode = 'P0001';
  end if;

  return auto_grading._student_achievement_stats_core(
    v_student_id,
    'all',
    null
  ) -> 'stats';
end;
$function$;

-- by_code는 관리자(authenticated)만 직접 호출. 공개 페이지는 token 래퍼 경유.
-- student_code가 enumerable하므로 anon 노출 금지.
revoke execute on function auto_grading.get_student_stats_by_code(text)
  from anon, public;
grant execute on function auto_grading.get_student_stats_by_code(text)
  to authenticated;

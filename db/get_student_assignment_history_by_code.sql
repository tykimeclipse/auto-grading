-- ================================================================
-- get_student_assignment_history_by_code
-- 학생 성취도 "시험 기록" 표의 기존 조회 계약.
--
-- 6단계 part 2B 이후 시험 기록 공식은
-- auto_grading._student_achievement_history_core 에서 단일 관리한다.
-- 이 파일은 기존 함수 이름·인자·RETURNS TABLE을 그대로 보존한다.
-- 2026-09-18 현재 저장소 내 UI 호출자는 없다. 공개 성취도·학생 관리는
-- 신규 범위 RPC를 사용하며, 이 함수는 롤백 안전망으로 유지해 코어에 위임한다.
-- 선행 배포: course_enrollment_achievement_stage6_part2a_course_achievement.sql
-- ================================================================
create or replace function auto_grading.get_student_assignment_history_by_code(
  p_student_code text,
  p_limit integer default 200
)
returns table (
  assignment_id uuid,
  test_set_id uuid,
  test_title text,
  source_type text,
  assigned_at timestamptz,
  event_date date,
  total_items integer,
  round1_correct_count integer,
  round1_score_percent numeric(5,1),
  round2_correct_count integer,
  round2_score_percent numeric(5,1),
  final_correct_count integer,
  final_score_percent numeric(5,1),
  teacher_final_score_percent numeric(5,1),
  last_activity_at timestamptz
)
language plpgsql
security definer
set search_path to 'auto_grading', 'public'
as $function$
declare
  v_student_id uuid;
  v_student_code text;
begin
  v_student_code := trim(p_student_code);

  if v_student_code is null or v_student_code = '' then
    raise exception 'student_code is required';
  end if;

  select s.id
    into v_student_id
  from auto_grading.students s
  where s.student_code = v_student_code
    and coalesce(s.is_active, true) = true
  limit 1;

  if v_student_id is null then
    raise exception '등록되지 않은 학생 코드입니다: %', v_student_code;
  end if;

  return query
  select h.*
  from auto_grading._student_achievement_history_core(
    v_student_id,
    'all',
    null,
    p_limit
  ) h;
end;
$function$;

-- by_code는 관리자(authenticated)만 직접 호출. 공개 페이지는 token 래퍼 경유.
revoke execute on function auto_grading.get_student_assignment_history_by_code(
  text,
  integer
) from anon, public;
grant execute on function auto_grading.get_student_assignment_history_by_code(
  text,
  integer
) to authenticated;

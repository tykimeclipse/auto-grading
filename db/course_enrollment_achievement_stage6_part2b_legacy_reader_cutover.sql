-- ============================================================================
-- course_enrollment_achievement_stage6_part2b_legacy_reader_cutover.sql
--
-- 6단계 part 2B: 기존 전체 누적 조회 함수를 검증된 공통 코어에 연결한다.
--
-- 실행 전 필수 조건:
--   audit_course_enrollment_achievement_stage6_part2a_equivalence.sql의
--   아래 세 mismatch_count가 모두 0이어야 한다.
--     1. old_new_stats_equivalence
--     2. old_new_history_equivalence
--     3. stats_scope_partition_integrity
--
-- 함수 이름·인자·반환형은 변경하지 않는다. 기존 공개 토큰 래퍼와 프론트 호출
-- 계약을 그대로 유지하면서 중복된 집계 공식을 공통 코어로 단일화한다.
-- ============================================================================

begin;

-- --------------------------------------------------------------------------
-- 1. 기존 전체 누적 통계 함수 → 공통 통계 코어
-- --------------------------------------------------------------------------
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

revoke execute on function auto_grading.get_student_stats_by_code(text)
  from anon, public;
grant execute on function auto_grading.get_student_stats_by_code(text)
  to authenticated;

-- --------------------------------------------------------------------------
-- 2. 기존 전체 시험 기록 함수 → 공통 history 코어
--
-- RETURNS TABLE 선언을 포함한 기존 시그니처를 그대로 유지한다. DROP하지 않으므로
-- 토큰 래퍼 등 기존 의존 객체도 유지된다.
-- --------------------------------------------------------------------------
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

revoke execute on function auto_grading.get_student_assignment_history_by_code(
  text,
  integer
) from anon, public;
grant execute on function auto_grading.get_student_assignment_history_by_code(
  text,
  integer
) to authenticated;

comment on function auto_grading.get_student_stats_by_code(text) is
  '기존 전체 누적 통계 계약. 검증된 공통 stats 코어의 all 범위에 위임.';

comment on function auto_grading.get_student_assignment_history_by_code(
  text,
  integer
) is '기존 전체 시험 기록 계약. 검증된 공통 history 코어의 all 범위에 위임.';

notify pgrst, 'reload schema';

commit;

create or replace function auto_grading.start_attempt(
  p_assignment_id uuid,
  p_student_code text
)
returns table (
  attempt_id uuid,
  student_id uuid,
  student_name text,
  test_set_id uuid,
  test_title text,
  status text,
  current_round integer,
  first_score_percent numeric,
  final_score_percent numeric,
  message text,
  should_lock boolean
)
language plpgsql
as $$
declare
  v_assignment auto_grading.assignments%rowtype;
  v_student auto_grading.students%rowtype;
  v_attempt auto_grading.attempts%rowtype;
  v_test_title text;
  v_total_items integer;
  v_next_attempt_no integer;
begin
  -- 1. assignment 확인
  select a.*
  into v_assignment
  from auto_grading.assignments as a
  where a.id = p_assignment_id
    and a.is_active = true
    and a.status <> 'archived';

  if not found then
    raise exception 'INVALID_ASSIGNMENT: assignment_id가 없거나 비활성입니다';
  end if;

  -- 2. student_code 확인
  select s.*
  into v_student
  from auto_grading.students as s
  where s.student_code = btrim(p_student_code)
    and s.is_active = true;

  if not found then
    raise exception 'STUDENT_NOT_FOUND: student_code를 찾을 수 없습니다';
  end if;

  -- 3. assignment 대상 학생과 일치하는지 확인
  if v_assignment.student_id is not null
     and v_assignment.student_id <> v_student.id then
    raise exception 'WRONG_STUDENT: 이 시험은 해당 학생에게 배정되지 않았습니다';
  end if;

  -- 4. 시험 정보 / total_items 확보
  select ts.title, ts.total_items
  into v_test_title, v_total_items
  from auto_grading.test_sets as ts
  where ts.id = v_assignment.test_set_id
    and ts.is_active = true;

  if not found then
    raise exception 'INVALID_TEST_SET: 시험 정보를 찾을 수 없습니다';
  end if;

  if v_total_items is null or v_total_items = 0 then
    raise exception 'INVALID_TEST_SET: 문항이 없는 시험입니다';
  end if;

  -- 5. 가장 최근 attempt 조회
  select a.*
  into v_attempt
  from auto_grading.attempts as a
  where a.student_id = v_student.id
    and a.assignment_id = v_assignment.id
  order by a.attempt_no desc
  limit 1;

  -- 6. 기존 attempt 상태별 처리
  if found then
    if v_attempt.status = 'completed' then
      attempt_id := v_attempt.id;
      student_id := v_student.id;
      student_name := v_student.name;
      test_set_id := v_attempt.test_set_id;
      test_title := v_test_title;
      status := v_attempt.status;
      current_round := v_attempt.current_round;
      first_score_percent := v_attempt.first_score_percent;
      final_score_percent := v_attempt.final_score_percent;
      message := '이미 제출이 완료된 시험입니다. 수정이 필요하면 선생님께 말씀하세요.';
      should_lock := true;
      return next;
      return;

    elsif v_attempt.status = 'needs_review' then
      attempt_id := v_attempt.id;
      student_id := v_student.id;
      student_name := v_student.name;
      test_set_id := v_attempt.test_set_id;
      test_title := v_test_title;
      status := v_attempt.status;
      current_round := v_attempt.current_round;
      first_score_percent := v_attempt.first_score_percent;
      final_score_percent := v_attempt.final_score_percent;
      message := '남은 문항은 선생님이 다음 수업에서 함께 확인해 드릴 거예요.';
      should_lock := true;
      return next;
      return;

    elsif v_attempt.status = 'in_progress' then
      attempt_id := v_attempt.id;
      student_id := v_student.id;
      student_name := v_student.name;
      test_set_id := v_attempt.test_set_id;
      test_title := v_test_title;
      status := v_attempt.status;
      current_round := v_attempt.current_round;
      first_score_percent := v_attempt.first_score_percent;
      final_score_percent := v_attempt.final_score_percent;
      message := '이전에 작성 중이던 답안을 이어서 입력합니다.';
      should_lock := false;
      return next;
      return;

    elsif v_attempt.status = 'awaiting_retry' then
      attempt_id := v_attempt.id;
      student_id := v_student.id;
      student_name := v_student.name;
      test_set_id := v_attempt.test_set_id;
      test_title := v_test_title;
      status := v_attempt.status;
      current_round := v_attempt.current_round;
      first_score_percent := v_attempt.first_score_percent;
      final_score_percent := v_attempt.final_score_percent;
      message := '1차 채점이 완료되었습니다. 틀린 문항만 다시 입력하세요.';
      should_lock := false;
      return next;
      return;

    else
      raise exception 'INVALID_ATTEMPT_STATE: 지원하지 않는 attempt 상태입니다';
    end if;
  end if;

  -- 7. 기존 attempt가 없으면 새 attempt 생성 시도
  v_next_attempt_no := auto_grading.get_next_attempt_no(v_student.id, v_assignment.test_set_id);

  insert into auto_grading.attempts (
    student_id,
    test_set_id,
    assignment_id,
    attempt_no,
    status,
    max_rounds,
    current_round,
    total_items,
    started_at
  )
  values (
    v_student.id,
    v_assignment.test_set_id,
    v_assignment.id,
    v_next_attempt_no,
    'in_progress',
    2,
    1,
    v_total_items,
    now()
  )
  on conflict on constraint uq_attempts_student_test_set_attempt_no
  do nothing;

  -- 8. insert 성공 여부와 관계없이 최신 attempt 재조회
  select a.*
  into v_attempt
  from auto_grading.attempts as a
  where a.student_id = v_student.id
    and a.assignment_id = v_assignment.id
  order by a.attempt_no desc
  limit 1;

  if not found then
    raise exception 'ATTEMPT_CREATE_FAILED: attempt 생성에 실패했습니다';
  end if;

  attempt_id := v_attempt.id;
  student_id := v_student.id;
  student_name := v_student.name;
  test_set_id := v_attempt.test_set_id;
  test_title := v_test_title;
  status := v_attempt.status;
  current_round := v_attempt.current_round;
  first_score_percent := v_attempt.first_score_percent;
  final_score_percent := v_attempt.final_score_percent;
  message := '새로운 답안 입력을 시작합니다.';
  should_lock := false;
  return next;
  return;
end;
$$;
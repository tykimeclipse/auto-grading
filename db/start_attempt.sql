create or replace function auto_grading.start_attempt(
  p_assignment_id uuid,
  p_student_code text
)
returns jsonb
language plpgsql
security definer
set search_path = auto_grading, public
as $$
declare
  v_assignment           auto_grading.assignments%rowtype;
  v_student              auto_grading.students%rowtype;
  v_test_set             auto_grading.test_sets%rowtype;
  v_attempt              auto_grading.attempts%rowtype;
  v_next_attempt_no      integer;
  v_retry_item_nos       integer[] := '{}'::integer[];
  v_items_json           jsonb := '[]'::jsonb;
  v_message              text;
  v_should_lock          boolean := false;
  v_return_current_round integer;
begin
  if p_assignment_id is null then
    raise exception 'INVALID_ASSIGNMENT'
      using errcode = 'P0001';
  end if;

  if p_student_code is null or btrim(p_student_code) = '' then
    raise exception 'INVALID_STUDENT_CODE'
      using errcode = 'P0001';
  end if;

  select a.*
    into v_assignment
  from auto_grading.assignments as a
  where a.id = p_assignment_id;

  if not found then
    raise exception 'INVALID_ASSIGNMENT'
      using errcode = 'P0001';
  end if;

  select s.*
    into v_student
  from auto_grading.students as s
  where s.student_code = p_student_code;

  if not found then
    raise exception 'STUDENT_NOT_FOUND'
      using errcode = 'P0001';
  end if;

  if v_assignment.student_id <> v_student.id then
    raise exception 'WRONG_STUDENT'
      using errcode = 'P0001';
  end if;

  select ts.*
    into v_test_set
  from auto_grading.test_sets as ts
  where ts.id = v_assignment.test_set_id;

  if not found then
    raise exception 'TEST_SET_NOT_FOUND'
      using errcode = 'P0001';
  end if;

  /*
    현재 학생+시험세트에 대한 최신 attempt 조회
    기존 row가 있으면 FOR UPDATE로 잠금
  */
  select a.*
    into v_attempt
  from auto_grading.attempts as a
  where a.student_id = v_assignment.student_id
    and a.test_set_id = v_assignment.test_set_id
  order by a.attempt_no desc
  limit 1
  for update;

  /*
    최신 attempt가 없으면 새 attempt 생성
    동시 요청 충돌 시 unique_violation을 잡고 최신 attempt를 다시 읽어옴
  */
  if not found then
    v_next_attempt_no := 1;

    begin
      insert into auto_grading.attempts (
        student_id,
        test_set_id,
        attempt_no,
        status,
        current_round,
        first_score_percent,
        final_score_percent
      )
      values (
        v_assignment.student_id,
        v_assignment.test_set_id,
        v_next_attempt_no,
        'in_progress',
        1,
        0,
        0
      )
      returning *
      into v_attempt;
    exception
      when unique_violation then
        select a.*
          into v_attempt
        from auto_grading.attempts as a
        where a.student_id = v_assignment.student_id
          and a.test_set_id = v_assignment.test_set_id
        order by a.attempt_no desc
        limit 1
        for update;

        if not found then
          raise exception 'ATTEMPT_CREATE_FAILED'
            using errcode = 'P0001';
        end if;
    end;
  end if;

  /*
    이하부터는 v_attempt의 실제 상태를 기준으로 공통 분기 처리
  */

  if v_attempt.status in ('completed', 'needs_review') then
    v_should_lock := true;
    v_return_current_round := coalesce(v_attempt.current_round, 1);
    v_message := '이미 제출이 완료된 시험입니다. 수정이 필요하면 선생님께 말씀하세요.';
    v_retry_item_nos := '{}'::integer[];
    v_items_json := '[]'::jsonb;

  elsif v_attempt.status = 'awaiting_retry' then
    v_should_lock := false;
    v_return_current_round := 2;
    v_message := '틀린 문항만 다시 입력하세요.';

    select coalesce(
             array_agg(ti.item_no order by ti.item_no),
             '{}'::integer[]
           )
      into v_retry_item_nos
    from auto_grading.responses as r
    join auto_grading.test_items as ti
      on ti.id = r.test_item_id
    where r.attempt_id = v_attempt.id
      and r.round_no = 1
      and r.is_correct = false
      and ti.test_set_id = v_test_set.id;

    /*
      정책: round2 복구 시 이전 오답은 보이지 않도록 selected_answer는 빈 문자열
    */
    select coalesce(
             jsonb_agg(
               jsonb_build_object(
                 'item_no', ti.item_no,
                 'choice_count', ti.choice_count,
                 'allows_multiple', position('|' in coalesce(ti.answer_key_normalized, '')) > 0,
                 'selected_answer', ''
               )
               order by ti.item_no
             ),
             '[]'::jsonb
           )
      into v_items_json
    from auto_grading.test_items as ti
    where ti.test_set_id = v_test_set.id;

  elsif v_attempt.status = 'in_progress' then
    v_should_lock := false;
    v_return_current_round := 1;
    v_message := case
      when coalesce(v_attempt.attempt_no, 0) = 1
        then '새로운 답안 입력을 시작합니다.'
      else '이전에 입력하던 답안을 이어서 불러왔습니다.'
    end;
    v_retry_item_nos := '{}'::integer[];

    /*
      정책: round1 진행 중 복구 시 저장된 답안은 selected_answer로 복원
    */
    select coalesce(
             jsonb_agg(
               jsonb_build_object(
                 'item_no', ti.item_no,
                 'choice_count', ti.choice_count,
                 'allows_multiple', position('|' in coalesce(ti.answer_key_normalized, '')) > 0,
                 'selected_answer', coalesce(r.selected_answer_raw, '')
               )
               order by ti.item_no
             ),
             '[]'::jsonb
           )
      into v_items_json
    from auto_grading.test_items as ti
    left join auto_grading.responses as r
      on r.attempt_id = v_attempt.id
     and r.test_item_id = ti.id
     and r.round_no = 1
    where ti.test_set_id = v_test_set.id;

  else
    raise exception 'UNSUPPORTED_ATTEMPT_STATUS: %', v_attempt.status
      using errcode = 'P0001';
  end if;

  return jsonb_build_object(
    'attempt_id', v_attempt.id,
    'student_id', v_student.id,
    'student_name', v_student.name,
    'test_set_id', v_test_set.id,
    'title', v_test_set.title,
    'status', v_attempt.status,
    'current_round', v_return_current_round,
    'first_score_percent', coalesce(v_attempt.first_score_percent, 0),
    'final_score_percent', coalesce(v_attempt.final_score_percent, 0),
    'message', v_message,
    'should_lock', v_should_lock,
    'total_items', v_test_set.total_items,
    'retry_item_nos', to_jsonb(v_retry_item_nos),
    'items', v_items_json
  );
end;
$$;
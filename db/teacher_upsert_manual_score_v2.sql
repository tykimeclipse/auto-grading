-- ============================================================================
-- teacher_upsert_manual_score_v2.sql
--
-- 3단계: 수동 성적 입력의 강좌 필수화 및 attempt 강좌 스냅샷 저장.
-- 기존 manual_assessment.sql 전체를 재실행하지 않고 이 함수만 교체한다.
-- assert_admin.sql 및 2단계 attempts.course_id 배포가 선행되어야 한다.
-- ============================================================================

begin;

drop function if exists auto_grading.teacher_upsert_manual_score(
  uuid, uuid, integer, integer, date, text
);

create or replace function auto_grading.teacher_upsert_manual_score(
  p_test_set_id                 uuid,
  p_student_id                  uuid,
  p_first_correct_count         integer,
  p_teacher_final_correct_count integer default null,
  p_event_date                  date    default null,
  p_note                        text    default null,
  p_course_id                   uuid    default null
)
returns jsonb
language plpgsql
security definer
set search_path to 'auto_grading', 'public'
as $function$
declare
  v_total                       integer;
  v_source                      text;
  v_assignment_id               uuid;
  v_assignment_course_id        uuid;
  v_attempt_id                  uuid;
  v_attempt_course_id           uuid;
  v_event                       timestamptz;
  v_first_pct                   numeric(5,2);
  v_tf_pct                      numeric(5,2);
  v_finalize                    boolean := (p_teacher_final_correct_count is not null);
  v_course_is_active            boolean;
  v_has_active_enrollment       boolean;
begin
  perform auto_grading.assert_admin();

  if p_test_set_id is null or p_student_id is null then
    raise exception 'p_test_set_id 와 p_student_id 는 필수입니다.';
  end if;

  if p_course_id is null then
    raise exception 'COURSE_REQUIRED';
  end if;

  select c.is_active
    into v_course_is_active
  from auto_grading.courses c
  where c.id = p_course_id
  for share;

  if not found then
    raise exception 'course not found: %', p_course_id;
  end if;

  if not coalesce(v_course_is_active, false) then
    raise exception 'COURSE_INACTIVE';
  end if;

  select exists (
    select 1
    from auto_grading.v_student_courses_normalized sc
    where sc.student_id = p_student_id
      and sc.course_id = p_course_id
      and sc.is_active
  ) into v_has_active_enrollment;

  if not coalesce(v_has_active_enrollment, false) then
    raise exception 'STUDENT_NOT_ENROLLED_IN_COURSE';
  end if;

  select total_items, source_type
    into v_total, v_source
  from auto_grading.test_sets
  where id = p_test_set_id;

  if not found then
    raise exception 'test_set 을 찾을 수 없습니다: %', p_test_set_id;
  end if;

  if v_source is distinct from 'manual' then
    raise exception '수동 점수 입력은 source_type=manual 시험에만 가능합니다. (현재: %)', v_source;
  end if;

  if v_total is null or v_total <= 0 then
    raise exception 'test_set 의 total_items 가 올바르지 않습니다.';
  end if;

  if p_first_correct_count is null
     or p_first_correct_count < 0
     or p_first_correct_count > v_total then
    raise exception '1차 정답수는 0~% 사이여야 합니다.', v_total;
  end if;

  if v_finalize
     and (p_teacher_final_correct_count < 0 or p_teacher_final_correct_count > v_total) then
    raise exception '최종 정답수는 0~% 사이여야 합니다.', v_total;
  end if;

  v_event     := coalesce(p_event_date::timestamptz, now());
  v_first_pct := round(p_first_correct_count::numeric * 100 / v_total, 2);
  v_tf_pct    := case
    when v_finalize then round(p_teacher_final_correct_count::numeric * 100 / v_total, 2)
    else null
  end;

  -- 동일 학생·시험의 기존 assignment를 다른 강좌로 자동 이동시키지 않는다.
  select a.id, a.course_id
    into v_assignment_id, v_assignment_course_id
  from auto_grading.assignments a
  where a.student_id = p_student_id
    and a.test_set_id = p_test_set_id
  for update;

  if v_assignment_id is not null then
    if v_assignment_course_id is distinct from p_course_id then
      raise exception 'ASSIGNMENT_OTHER_COURSE';
    end if;

    update auto_grading.assignments
    set updated_at = now()
    where id = v_assignment_id;
  else
    begin
      insert into auto_grading.assignments (
        student_id,
        test_set_id,
        course_id,
        assigned_at,
        status
      ) values (
        p_student_id,
        p_test_set_id,
        p_course_id,
        v_event,
        'assigned'
      )
      returning id, course_id
        into v_assignment_id, v_assignment_course_id;
    exception
      when unique_violation then
        select a.id, a.course_id
          into v_assignment_id, v_assignment_course_id
        from auto_grading.assignments a
        where a.student_id = p_student_id
          and a.test_set_id = p_test_set_id
        for update;

        if v_assignment_id is null then
          raise exception 'ASSIGNMENT_UNAVAILABLE_RETRY';
        end if;

        if v_assignment_course_id is distinct from p_course_id then
          raise exception 'ASSIGNMENT_OTHER_COURSE';
        end if;
    end;
  end if;

  select at.id, at.course_id
    into v_attempt_id, v_attempt_course_id
  from auto_grading.attempts at
  where at.assignment_id = v_assignment_id
  order by at.created_at asc nulls first, at.id asc
  limit 1
  for update;

  if v_attempt_id is not null
     and v_attempt_course_id is distinct from p_course_id then
    raise exception 'ATTEMPT_OTHER_COURSE';
  end if;

  if v_attempt_id is null then
    insert into auto_grading.attempts (
      student_id,
      test_set_id,
      assignment_id,
      course_id,
      attempt_no,
      max_rounds,
      current_round,
      status,
      total_items,
      first_correct_count,
      first_score_percent,
      final_correct_count,
      final_score_percent,
      teacher_final_correct_count,
      teacher_final_score_percent,
      teacher_final_note,
      teacher_final_updated_at,
      teacher_final_updated_by,
      round1_submitted_at,
      started_at
    ) values (
      p_student_id,
      p_test_set_id,
      v_assignment_id,
      p_course_id,
      1,
      1,
      1,
      'needs_review',
      v_total,
      p_first_correct_count,
      v_first_pct,
      p_first_correct_count,
      v_first_pct,
      p_teacher_final_correct_count,
      v_tf_pct,
      case when v_finalize then nullif(btrim(p_note), '') else null end,
      case when v_finalize then now() else null end,
      case when v_finalize then auth.uid() else null end,
      v_event,
      v_event
    )
    returning id into v_attempt_id;
  else
    update auto_grading.attempts
    set
      total_items                 = v_total,
      first_correct_count         = p_first_correct_count,
      first_score_percent         = v_first_pct,
      final_correct_count         = p_first_correct_count,
      final_score_percent         = v_first_pct,
      teacher_final_correct_count = p_teacher_final_correct_count,
      teacher_final_score_percent = v_tf_pct,
      teacher_final_note          = case when v_finalize then nullif(btrim(p_note), '') else null end,
      teacher_final_updated_at    = case when v_finalize then now() else null end,
      teacher_final_updated_by    = case when v_finalize then auth.uid() else null end,
      status                      = 'needs_review',
      round1_submitted_at         = v_event
    where id = v_attempt_id;
  end if;

  return jsonb_build_object(
    'ok',                          true,
    'assignment_id',               v_assignment_id,
    'attempt_id',                  v_attempt_id,
    'course_id',                   p_course_id,
    'total_items',                 v_total,
    'first_correct_count',         p_first_correct_count,
    'first_score_percent',         v_first_pct,
    'teacher_final_correct_count', p_teacher_final_correct_count,
    'teacher_final_score_percent', v_tf_pct,
    'finalized',                   v_finalize
  );

exception
  when others then
    return jsonb_build_object('ok', false, 'error', sqlerrm);
end;
$function$;

revoke execute on function auto_grading.teacher_upsert_manual_score(
  uuid, uuid, integer, integer, date, text, uuid
) from public, anon;
grant execute on function auto_grading.teacher_upsert_manual_score(
  uuid, uuid, integer, integer, date, text, uuid
) to authenticated, service_role;

comment on function auto_grading.teacher_upsert_manual_score(
  uuid, uuid, integer, integer, date, text, uuid
) is
  '교사용 수동 성적 입력. 활성 강좌와 수강을 필수 검증하고 assignment/attempt 강좌를 자동 재태깅하지 않는다.';

commit;

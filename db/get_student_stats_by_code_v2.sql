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
  v_total_solved integer := 0;
  v_first_correct integer := 0;
  v_round2_correct integer := 0;
  v_final_correct integer := 0;
  v_final_items integer := 0;      -- 최종 확정된 attempt의 총 문항수 (분모)
  v_teacher_final_correct integer;
  v_teacher_final_items integer;
begin
  /*
    전제:
    1) students.student_code 는 unique
    2) 누적 최종 성취도(final_accuracy)는 "확정된" attempt만 포함:
         - status = 'completed'                          → 자동 100점 완료 → 확정
         - teacher_final_correct_count IS NOT NULL       → 교사 확정 → 확정
         - status = 'needs_review' + teacher_final null  → 미확정 → 제외
    3) round1/round2_accuracy는 completed + needs_review 전체 포함
  */
  select id
    into v_student_id
  from auto_grading.students
  where student_code = p_student_code
    and is_active = true;

  if v_student_id is null then
    raise exception 'STUDENT_NOT_FOUND'
      using errcode = 'P0001';
  end if;

  select
    -- 1차·2차 누적용: completed + needs_review 전체
    coalesce(sum(a.total_items), 0),
    coalesce(sum(a.first_correct_count), 0),
    coalesce(sum(a.final_correct_count), 0),

    -- 최종 확정 정답수: 교사 확정 또는 자동 100점 완료 attempt만
    coalesce(sum(
      case
        when a.teacher_final_correct_count is not null then a.teacher_final_correct_count
        when a.status = 'completed'                    then a.final_correct_count
        else null
      end
    ), 0),

    -- 최종 확정 분모: 위와 동일 조건의 총 문항수
    coalesce(sum(
      case
        when a.teacher_final_correct_count is not null then a.total_items
        when a.status = 'completed'                    then a.total_items
        else null
      end
    ), 0),

    -- teacher_final 단독 집계 (teacher_final_accuracy용)
    sum(case when a.teacher_final_correct_count is not null then a.teacher_final_correct_count end),
    sum(case when a.teacher_final_correct_count is not null then a.total_items end)

  into
    v_total_solved,
    v_first_correct,
    v_round2_correct,
    v_final_correct,
    v_final_items,
    v_teacher_final_correct,
    v_teacher_final_items

  from auto_grading.attempts a
  where a.student_id = v_student_id
    and a.status in ('completed', 'needs_review');

  return jsonb_build_object(
    'total_solved',
    v_total_solved,

    'round1_accuracy',
    case
      when v_total_solved = 0 then null
      else round(
        (v_first_correct::numeric / v_total_solved::numeric) * 100,
        2
      )
    end,

    'round2_accuracy',
    case
      when v_total_solved = 0 then null
      else round(
        (v_round2_correct::numeric / v_total_solved::numeric) * 100,
        2
      )
    end,

    -- 분모를 v_total_solved 대신 v_final_items 사용
    'final_accuracy',
    case
      when v_final_items = 0 then null
      else round(
        (v_final_correct::numeric / v_final_items::numeric) * 100,
        2
      )
    end,

    'teacher_final_accuracy',
    case
      when v_teacher_final_items is null or v_teacher_final_items = 0 then null
      else round(
        (v_teacher_final_correct::numeric / v_teacher_final_items::numeric) * 100,
        2
      )
    end
  );
end;
$function$;

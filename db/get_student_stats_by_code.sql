CREATE 
OR REPLACE FUNCTION auto_grading.get_student_stats_by_code(p_student_code text) RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER
SET
  search_path TO 'auto_grading',
  'public' AS $function $declare v_student_id uuid;


v_total_solved integer: = 0;


v_first_correct integer: = 0;


v_final_correct integer: = 0;


begin
/* 전제: 1) students.student_code 는 unique 2) final_correct_count = 최종 정답 수 3) 통계에는 완료된 attempt만 포함 - completed - needs_review */
select
  id into v_student_id
from
  auto_grading.students
where
  student_code = p_student_code
  and is_active = true;


if v_student_id is null then raise exception 'STUDENT_NOT_FOUND' using errcode = 'P0001';


end if;


select
  coalesce(sum(a.total_items), 0),
  coalesce(sum(a.first_correct_count), 0),
  coalesce(sum(a.final_correct_count), 0) into v_total_solved,
  v_first_correct,
  v_final_correct
from
  auto_grading.attempts a
where
  a.student_id = v_student_id
  and a.status in ('completed', 'needs_review');


return jsonb_build_object(
  'total_solved',
  v_total_solved,
  'round1_accuracy',
  case
    when v_total_solved = 0 then null
    else round(
      (v_first_correct:: numeric / v_total_solved:: numeric) * 100,
      2
    )
  end,
  'final_accuracy',
  case
    when v_total_solved = 0 then null
    else round(
      (v_final_correct:: numeric / v_total_solved:: numeric) * 100,
      2
    )
  end
);


end;


$function $
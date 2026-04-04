-- =========================================================
-- student_course_type write path unified
-- 목적
-- 1) 학생-수강과정 쓰기 경로를 student_course_type 기준으로 통일
-- 2) 기존 3번째 인자(service_type / enrollment_type 흔적)도 호환 처리
-- 3) 내부 호환용 service_type은 student_course_type에서 자동 파생
-- =========================================================

create or replace function auto_grading.teacher_attach_student_to_course(
  p_student_id uuid,
  p_course_id uuid,
  p_student_course_type text
)
returns jsonb
language plpgsql
security definer
set search_path to 'auto_grading', 'public'
as $function$
declare
  v_row_id uuid;
  v_input text;
  v_student_course_type text;
  v_service_type text;
begin
  if p_student_id is null then
    raise exception 'p_student_id is required';
  end if;

  if p_course_id is null then
    raise exception 'p_course_id is required';
  end if;

  v_input := nullif(btrim(coalesce(p_student_course_type, '')), '');
  if v_input is null then
    raise exception 'p_student_course_type is required';
  end if;

  v_student_course_type := case
    when v_input in ('academy', '정규', '학원') then '학원'
    when v_input in ('tutoring', '과외') then '과외'
    when v_input = '내신' then '내신'
    when v_input = '특강' then '특강'
    else null
  end;

  if v_student_course_type is null then
    raise exception 'invalid p_student_course_type: %', p_student_course_type;
  end if;

  v_service_type := case
    when v_student_course_type = '과외' then 'tutoring'
    else 'academy'
  end;

  perform 1
  from auto_grading.students s
  where s.id = p_student_id;

  if not found then
    raise exception 'student not found: %', p_student_id;
  end if;

  perform 1
  from auto_grading.courses c
  where c.id = p_course_id;

  if not found then
    raise exception 'course not found: %', p_course_id;
  end if;

  perform pg_advisory_xact_lock(
    hashtextextended(p_student_id::text || ':' || p_course_id::text, 0)
  );

  update auto_grading.student_courses
     set student_course_type = v_student_course_type,
         service_type = v_service_type,
         is_active = true,
         updated_at = now()
   where student_id = p_student_id
     and course_id = p_course_id
     and is_active = true
   returning id into v_row_id;

  if v_row_id is not null then
    return jsonb_build_object(
      'ok', true,
      'mode', 'updated',
      'student_id', p_student_id,
      'course_id', p_course_id,
      'student_course_type', v_student_course_type,
      'service_type', v_service_type
    );
  end if;

  insert into auto_grading.student_courses (
    student_id,
    course_id,
    student_course_type,
    service_type,
    is_active,
    joined_at,
    created_at,
    updated_at
  ) values (
    p_student_id,
    p_course_id,
    v_student_course_type,
    v_service_type,
    true,
    now(),
    now(),
    now()
  )
  returning id into v_row_id;

  return jsonb_build_object(
    'ok', true,
    'mode', 'inserted',
    'student_id', p_student_id,
    'course_id', p_course_id,
    'student_course_type', v_student_course_type,
    'service_type', v_service_type
  );
end;
$function$;

create or replace function auto_grading.teacher_deactivate_student_course(
  p_student_id uuid,
  p_course_id uuid
)
returns jsonb
language plpgsql
security definer
set search_path to 'auto_grading', 'public'
as $function$
declare
  v_count integer;
begin
  if p_student_id is null then
    raise exception 'p_student_id is required';
  end if;

  if p_course_id is null then
    raise exception 'p_course_id is required';
  end if;

  update auto_grading.student_courses
     set is_active = false,
         updated_at = now()
   where student_id = p_student_id
     and course_id = p_course_id
     and is_active = true;

  get diagnostics v_count = row_count;

  return jsonb_build_object(
    'ok', true,
    'student_id', p_student_id,
    'course_id', p_course_id,
    'deactivated_count', v_count
  );
end;
$function$;

grant execute on function auto_grading.teacher_attach_student_to_course(uuid, uuid, text) to authenticated;
grant execute on function auto_grading.teacher_deactivate_student_course(uuid, uuid) to authenticated;

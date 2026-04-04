-- =========================================================
-- student_course_type write path unified v2 (dual-write)
-- 목적
-- 1) 학생-수강과정 쓰기 경로를 student_course_type 기준으로 통일
-- 2) 전환기 동안 enrollment_type도 함께 기록(dual-write)
-- 3) 내부 호환용 service_type은 student_course_type에서 자동 파생
-- 4) 비활성화 시 ended_at(또는 종료 컬럼)도 함께 기록
-- 5) 컬럼 존재 여부를 점검해 덜 정리된 환경에서도 최대한 안전하게 동작
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
  v_enrollment_type text;
  v_update_sql text;
  v_insert_cols text := 'student_id, course_id';
  v_insert_vals text := '$1, $2';
  v_has_student_course_type boolean;
  v_has_service_type boolean;
  v_has_enrollment_type boolean;
  v_has_is_active boolean;
  v_has_joined_at boolean;
  v_has_started_at boolean;
  v_has_start_at boolean;
  v_has_start_date boolean;
  v_has_created_at boolean;
  v_has_updated_at boolean;
  v_has_ended_at boolean;
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

  -- 전환기 dual-write 매핑
  -- 기존 enrollment_type 도메인: 정규 / 내신 / 과외
  v_enrollment_type := case
    when v_student_course_type = '과외' then '과외'
    when v_student_course_type = '내신' then '내신'
    else '정규' -- 학원, 특강은 임시로 정규에 매핑
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

  select exists (
           select 1 from information_schema.columns
           where table_schema = 'auto_grading' and table_name = 'student_courses' and column_name = 'student_course_type'
         ),
         exists (
           select 1 from information_schema.columns
           where table_schema = 'auto_grading' and table_name = 'student_courses' and column_name = 'service_type'
         ),
         exists (
           select 1 from information_schema.columns
           where table_schema = 'auto_grading' and table_name = 'student_courses' and column_name = 'enrollment_type'
         ),
         exists (
           select 1 from information_schema.columns
           where table_schema = 'auto_grading' and table_name = 'student_courses' and column_name = 'is_active'
         ),
         exists (
           select 1 from information_schema.columns
           where table_schema = 'auto_grading' and table_name = 'student_courses' and column_name = 'joined_at'
         ),
         exists (
           select 1 from information_schema.columns
           where table_schema = 'auto_grading' and table_name = 'student_courses' and column_name = 'started_at'
         ),
         exists (
           select 1 from information_schema.columns
           where table_schema = 'auto_grading' and table_name = 'student_courses' and column_name = 'start_at'
         ),
         exists (
           select 1 from information_schema.columns
           where table_schema = 'auto_grading' and table_name = 'student_courses' and column_name = 'start_date'
         ),
         exists (
           select 1 from information_schema.columns
           where table_schema = 'auto_grading' and table_name = 'student_courses' and column_name = 'created_at'
         ),
         exists (
           select 1 from information_schema.columns
           where table_schema = 'auto_grading' and table_name = 'student_courses' and column_name = 'updated_at'
         ),
         exists (
           select 1 from information_schema.columns
           where table_schema = 'auto_grading' and table_name = 'student_courses' and column_name = 'ended_at'
         )
    into v_has_student_course_type,
         v_has_service_type,
         v_has_enrollment_type,
         v_has_is_active,
         v_has_joined_at,
         v_has_started_at,
         v_has_start_at,
         v_has_start_date,
         v_has_created_at,
         v_has_updated_at,
         v_has_ended_at;

  perform pg_advisory_xact_lock(
    hashtextextended(p_student_id::text || ':' || p_course_id::text, 0)
  );

  v_update_sql := 'update auto_grading.student_courses set ';

  if v_has_student_course_type then
    v_update_sql := v_update_sql || 'student_course_type = $1, ';
  end if;

  if v_has_service_type then
    v_update_sql := v_update_sql || 'service_type = $2, ';
  end if;

  if v_has_enrollment_type then
    v_update_sql := v_update_sql || 'enrollment_type = $3, ';
  end if;

  if v_has_is_active then
    v_update_sql := v_update_sql || 'is_active = true, ';
  end if;

  if v_has_ended_at then
    v_update_sql := v_update_sql || 'ended_at = null, ';
  end if;

  if v_has_updated_at then
    v_update_sql := v_update_sql || 'updated_at = now(), ';
  end if;

  -- 뒤의 ', ' 제거
  v_update_sql := regexp_replace(v_update_sql, ',\s*$', '');
  v_update_sql := v_update_sql || ' where student_id = $4 and course_id = $5';

  if v_has_is_active then
    v_update_sql := v_update_sql || ' and is_active = true';
  end if;

  v_update_sql := v_update_sql || ' returning id';

  execute v_update_sql
    using v_student_course_type, v_service_type, v_enrollment_type, p_student_id, p_course_id
    into v_row_id;

  if v_row_id is not null then
    return jsonb_build_object(
      'ok', true,
      'mode', 'updated',
      'student_id', p_student_id,
      'course_id', p_course_id,
      'student_course_type', v_student_course_type,
      'service_type', v_service_type,
      'enrollment_type', v_enrollment_type
    );
  end if;

  if v_has_student_course_type then
    v_insert_cols := v_insert_cols || ', student_course_type';
    v_insert_vals := v_insert_vals || ', $3';
  end if;

  if v_has_service_type then
    v_insert_cols := v_insert_cols || ', service_type';
    v_insert_vals := v_insert_vals || ', $4';
  end if;

  if v_has_enrollment_type then
    v_insert_cols := v_insert_cols || ', enrollment_type';
    v_insert_vals := v_insert_vals || ', $5';
  end if;

  if v_has_is_active then
    v_insert_cols := v_insert_cols || ', is_active';
    v_insert_vals := v_insert_vals || ', true';
  end if;

  if v_has_joined_at then
    v_insert_cols := v_insert_cols || ', joined_at';
    v_insert_vals := v_insert_vals || ', now()';
  elsif v_has_started_at then
    v_insert_cols := v_insert_cols || ', started_at';
    v_insert_vals := v_insert_vals || ', now()';
  elsif v_has_start_at then
    v_insert_cols := v_insert_cols || ', start_at';
    v_insert_vals := v_insert_vals || ', now()';
  elsif v_has_start_date then
    v_insert_cols := v_insert_cols || ', start_date';
    v_insert_vals := v_insert_vals || ', current_date';
  end if;

  if v_has_created_at then
    v_insert_cols := v_insert_cols || ', created_at';
    v_insert_vals := v_insert_vals || ', now()';
  end if;

  if v_has_updated_at then
    v_insert_cols := v_insert_cols || ', updated_at';
    v_insert_vals := v_insert_vals || ', now()';
  end if;

  execute format(
    'insert into auto_grading.student_courses (%s) values (%s) returning id',
    v_insert_cols,
    v_insert_vals
  )
  using p_student_id, p_course_id, v_student_course_type, v_service_type, v_enrollment_type
  into v_row_id;

  return jsonb_build_object(
    'ok', true,
    'mode', 'inserted',
    'student_id', p_student_id,
    'course_id', p_course_id,
    'student_course_type', v_student_course_type,
    'service_type', v_service_type,
    'enrollment_type', v_enrollment_type
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
  v_has_is_active boolean;
  v_has_updated_at boolean;
  v_has_ended_at boolean;
  v_update_sql text := 'update auto_grading.student_courses set ';
begin
  if p_student_id is null then
    raise exception 'p_student_id is required';
  end if;

  if p_course_id is null then
    raise exception 'p_course_id is required';
  end if;

  select exists (
           select 1 from information_schema.columns
           where table_schema = 'auto_grading' and table_name = 'student_courses' and column_name = 'is_active'
         ),
         exists (
           select 1 from information_schema.columns
           where table_schema = 'auto_grading' and table_name = 'student_courses' and column_name = 'updated_at'
         ),
         exists (
           select 1 from information_schema.columns
           where table_schema = 'auto_grading' and table_name = 'student_courses' and column_name = 'ended_at'
         )
    into v_has_is_active,
         v_has_updated_at,
         v_has_ended_at;

  if v_has_is_active then
    v_update_sql := v_update_sql || 'is_active = false, ';
  end if;

  if v_has_ended_at then
    v_update_sql := v_update_sql || 'ended_at = coalesce(ended_at, now()), ';
  end if;

  if v_has_updated_at then
    v_update_sql := v_update_sql || 'updated_at = now(), ';
  end if;

  v_update_sql := regexp_replace(v_update_sql, ',\s*$', '');
  v_update_sql := v_update_sql || ' where student_id = $1 and course_id = $2';

  if v_has_is_active then
    v_update_sql := v_update_sql || ' and is_active = true';
  end if;

  execute v_update_sql using p_student_id, p_course_id;

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

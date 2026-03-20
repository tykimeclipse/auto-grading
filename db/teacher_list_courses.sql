-- =========================================================
-- 0) student_courses.service_type 보강
-- =========================================================
alter table auto_grading.student_courses
  add column if not exists service_type text;

alter table auto_grading.student_courses
  drop constraint if exists student_courses_service_type_chk;

alter table auto_grading.student_courses
  add constraint student_courses_service_type_chk
  check (
    service_type is null
    or service_type in ('academy', 'tutoring')
  );

comment on column auto_grading.student_courses.service_type
is '수업 서비스 유형. academy=학원, tutoring=과외';

create index if not exists idx_student_courses_service_type
  on auto_grading.student_courses (service_type);

-- =========================================================
-- 1) 정규화 뷰 생성
--    - v_student_courses_normalized:
--      active 판정 / joined_at / service_type / enrollment_type 공통화
--    - v_courses_normalized:
--      과정 표시명 공통화
-- =========================================================
do $$
declare
  v_service_expr text := 'null::text';
  v_enrollment_expr text := 'null::text';
  v_joined_expr text := 'null::timestamptz';
  v_row_ts_expr text := 'null::timestamptz';
  v_end_expr text := 'null::timestamptz';
  v_is_active_expr text := 'true';
begin
  if exists (
    select 1
    from information_schema.columns
    where table_schema = 'auto_grading'
      and table_name = 'student_courses'
      and column_name = 'service_type'
  ) then
    v_service_expr := 'sc.service_type';
  end if;

  if exists (
    select 1
    from information_schema.columns
    where table_schema = 'auto_grading'
      and table_name = 'student_courses'
      and column_name = 'enrollment_type'
  ) then
    v_enrollment_expr := 'sc.enrollment_type';
  end if;

  if exists (
    select 1
    from information_schema.columns
    where table_schema = 'auto_grading'
      and table_name = 'student_courses'
      and column_name = 'joined_at'
  ) then
    v_joined_expr := 'sc.joined_at::timestamptz';
  elsif exists (
    select 1
    from information_schema.columns
    where table_schema = 'auto_grading'
      and table_name = 'student_courses'
      and column_name = 'started_at'
  ) then
    v_joined_expr := 'sc.started_at::timestamptz';
  elsif exists (
    select 1
    from information_schema.columns
    where table_schema = 'auto_grading'
      and table_name = 'student_courses'
      and column_name = 'start_at'
  ) then
    v_joined_expr := 'sc.start_at::timestamptz';
  elsif exists (
    select 1
    from information_schema.columns
    where table_schema = 'auto_grading'
      and table_name = 'student_courses'
      and column_name = 'start_date'
  ) then
    v_joined_expr := 'sc.start_date::timestamptz';
  elsif exists (
    select 1
    from information_schema.columns
    where table_schema = 'auto_grading'
      and table_name = 'student_courses'
      and column_name = 'created_at'
  ) then
    v_joined_expr := 'sc.created_at::timestamptz';
  end if;

  if exists (
    select 1
    from information_schema.columns
    where table_schema = 'auto_grading'
      and table_name = 'student_courses'
      and column_name = 'updated_at'
  ) then
    v_row_ts_expr := 'sc.updated_at::timestamptz';
  else
    v_row_ts_expr := v_joined_expr;
  end if;

  if v_row_ts_expr = 'null::timestamptz'
     and exists (
       select 1
       from information_schema.columns
       where table_schema = 'auto_grading'
         and table_name = 'student_courses'
         and column_name = 'created_at'
     ) then
    v_row_ts_expr := 'sc.created_at::timestamptz';
  end if;

  if exists (
    select 1
    from information_schema.columns
    where table_schema = 'auto_grading'
      and table_name = 'student_courses'
      and column_name = 'closed_at'
  ) then
    v_end_expr := 'sc.closed_at::timestamptz';
  elsif exists (
    select 1
    from information_schema.columns
    where table_schema = 'auto_grading'
      and table_name = 'student_courses'
      and column_name = 'ended_at'
  ) then
    v_end_expr := 'sc.ended_at::timestamptz';
  elsif exists (
    select 1
    from information_schema.columns
    where table_schema = 'auto_grading'
      and table_name = 'student_courses'
      and column_name = 'end_at'
  ) then
    v_end_expr := 'sc.end_at::timestamptz';
  elsif exists (
    select 1
    from information_schema.columns
    where table_schema = 'auto_grading'
      and table_name = 'student_courses'
      and column_name = 'end_date'
  ) then
    v_end_expr := 'sc.end_date::timestamptz';
  end if;

  if exists (
    select 1
    from information_schema.columns
    where table_schema = 'auto_grading'
      and table_name = 'student_courses'
      and column_name = 'is_active'
  ) then
    if v_end_expr <> 'null::timestamptz' then
      v_is_active_expr := format(
        'coalesce(sc.is_active, (%s is null))',
        v_end_expr
      );
    else
      v_is_active_expr := 'coalesce(sc.is_active, true)';
    end if;
  else
    if v_end_expr <> 'null::timestamptz' then
      v_is_active_expr := format('(%s is null)', v_end_expr);
    else
      v_is_active_expr := 'true';
    end if;
  end if;

  execute 'drop view if exists auto_grading.v_student_courses_normalized';
  execute format(
    $sql$
    create view auto_grading.v_student_courses_normalized as
    select
      sc.ctid as row_pointer,
      sc.student_id,
      sc.course_id,
      %s as service_type,
      %s as enrollment_type,
      %s as joined_at,
      %s as row_ts,
      %s as is_active
    from auto_grading.student_courses sc
    $sql$,
    v_service_expr,
    v_enrollment_expr,
    v_joined_expr,
    v_row_ts_expr,
    v_is_active_expr
  );
end $$;

do $$
declare
  v_course_name_expr text := 'c.id::text';
begin
  if exists (
    select 1
    from information_schema.columns
    where table_schema = 'auto_grading'
      and table_name = 'courses'
      and column_name = 'course_name'
  ) then
    v_course_name_expr := 'c.course_name';
  elsif exists (
    select 1
    from information_schema.columns
    where table_schema = 'auto_grading'
      and table_name = 'courses'
      and column_name = 'title'
  ) then
    v_course_name_expr := 'c.title';
  elsif exists (
    select 1
    from information_schema.columns
    where table_schema = 'auto_grading'
      and table_name = 'courses'
      and column_name = 'name'
  ) then
    v_course_name_expr := 'c.name';
  end if;

  execute 'drop view if exists auto_grading.v_courses_normalized';
  execute format(
    $sql$
    create view auto_grading.v_courses_normalized as
    select
      c.id as course_id,
      %s as course_name
    from auto_grading.courses c
    $sql$,
    v_course_name_expr
  );
end $$;

-- =========================================================
-- 2) teacher_list_courses 수정본
--    - to_jsonb 제거
--    - active 판정은 정규화 뷰 사용
--    - p_only_active는 호환성 때문에 이름 유지
--      (실제 의미: 활성 학생이 1명 이상 있는 과정만)
-- =========================================================
create or replace function auto_grading.teacher_list_courses(
  p_search text default null,
  p_only_active boolean default true,
  p_limit integer default 100,
  p_offset integer default 0
)
returns table(
  total_count bigint,
  course_id uuid,
  course_name text,
  active_student_count integer,
  total_student_count integer,
  academy_student_count integer,
  tutoring_student_count integer,
  last_joined_at timestamptz
)
language sql
security definer
set search_path to 'auto_grading', 'public'
as $function$
with sc_summary as (
  select
    v.course_id,
    count(distinct v.student_id)::integer as total_student_count,
    count(distinct v.student_id) filter (where v.is_active)::integer as active_student_count,
    count(distinct v.student_id) filter (
      where v.is_active and v.service_type = 'academy'
    )::integer as academy_student_count,
    count(distinct v.student_id) filter (
      where v.is_active and v.service_type = 'tutoring'
    )::integer as tutoring_student_count,
    max(v.joined_at) as last_joined_at
  from auto_grading.v_student_courses_normalized v
  group by v.course_id
),
base as (
  select
    c.course_id,
    c.course_name,
    coalesce(s.active_student_count, 0) as active_student_count,
    coalesce(s.total_student_count, 0) as total_student_count,
    coalesce(s.academy_student_count, 0) as academy_student_count,
    coalesce(s.tutoring_student_count, 0) as tutoring_student_count,
    s.last_joined_at
  from auto_grading.v_courses_normalized c
  left join sc_summary s
    on s.course_id = c.course_id
  where 1 = 1
    and (
      p_search is null
      or btrim(p_search) = ''
      or c.course_name ilike '%' || p_search || '%'
    )
    and (
      not p_only_active
      or coalesce(s.active_student_count, 0) > 0
    )
)
select
  count(*) over() as total_count,
  b.course_id,
  b.course_name,
  b.active_student_count,
  b.total_student_count,
  b.academy_student_count,
  b.tutoring_student_count,
  b.last_joined_at
from base b
order by
  b.course_name asc nulls last,
  b.course_id asc
limit greatest(coalesce(p_limit, 100), 1)
offset greatest(coalesce(p_offset, 0), 0);
$function$;

grant execute on function auto_grading.teacher_list_courses(
  text, boolean, integer, integer
) to authenticated;

-- =========================================================
-- 3) teacher_attach_student_to_course 수정본
--    - service_type 필수
--    - advisory lock으로 student_id + course_id attach 직렬화
--    - broad exception 제거
--    - active row가 있으면 해당 row만 update
--    - 없으면 insert
-- =========================================================
create or replace function auto_grading.teacher_attach_student_to_course(
  p_student_id uuid,
  p_course_id uuid,
  p_service_type text
)
returns jsonb
language plpgsql
security definer
set search_path to 'auto_grading', 'public'
as $function$
declare
  v_row_pointer tid;
  v_update_sql text;
  v_insert_cols text := 'student_id, course_id, service_type';
  v_insert_vals text := '$1, $2, $3';
begin
  if p_student_id is null then
    raise exception 'p_student_id is required';
  end if;

  if p_course_id is null then
    raise exception 'p_course_id is required';
  end if;

  if p_service_type is null then
    raise exception 'p_service_type is required';
  end if;

  if p_service_type not in ('academy', 'tutoring') then
    raise exception 'p_service_type must be academy or tutoring';
  end if;

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

  select v.row_pointer
    into v_row_pointer
  from auto_grading.v_student_courses_normalized v
  where v.student_id = p_student_id
    and v.course_id = p_course_id
    and v.is_active
  order by
    v.joined_at desc nulls last,
    v.row_ts desc nulls last
  limit 1;

  if v_row_pointer is not null then
    v_update_sql := 'update auto_grading.student_courses set service_type = $1';

    if exists (
      select 1
      from information_schema.columns
      where table_schema = 'auto_grading'
        and table_name = 'student_courses'
        and column_name = 'updated_at'
    ) then
      v_update_sql := v_update_sql || ', updated_at = now()';
    end if;

    v_update_sql := v_update_sql || ' where ctid = $2';

    execute v_update_sql using p_service_type, v_row_pointer;

    return jsonb_build_object(
      'ok', true,
      'student_id', p_student_id,
      'course_id', p_course_id,
      'service_type', p_service_type,
      'message', 'active student_course updated'
    );
  end if;

  if exists (
    select 1
    from information_schema.columns
    where table_schema = 'auto_grading'
      and table_name = 'student_courses'
      and column_name = 'is_active'
  ) then
    v_insert_cols := v_insert_cols || ', is_active';
    v_insert_vals := v_insert_vals || ', true';
  end if;

  if exists (
    select 1
    from information_schema.columns
    where table_schema = 'auto_grading'
      and table_name = 'student_courses'
      and column_name = 'joined_at'
  ) then
    v_insert_cols := v_insert_cols || ', joined_at';
    v_insert_vals := v_insert_vals || ', now()';
  elsif exists (
    select 1
    from information_schema.columns
    where table_schema = 'auto_grading'
      and table_name = 'student_courses'
      and column_name = 'started_at'
  ) then
    v_insert_cols := v_insert_cols || ', started_at';
    v_insert_vals := v_insert_vals || ', now()';
  elsif exists (
    select 1
    from information_schema.columns
    where table_schema = 'auto_grading'
      and table_name = 'student_courses'
      and column_name = 'start_at'
  ) then
    v_insert_cols := v_insert_cols || ', start_at';
    v_insert_vals := v_insert_vals || ', now()';
  elsif exists (
    select 1
    from information_schema.columns
    where table_schema = 'auto_grading'
      and table_name = 'student_courses'
      and column_name = 'start_date'
  ) then
    v_insert_cols := v_insert_cols || ', start_date';
    v_insert_vals := v_insert_vals || ', current_date';
  end if;

  if exists (
    select 1
    from information_schema.columns
    where table_schema = 'auto_grading'
      and table_name = 'student_courses'
      and column_name = 'created_at'
  ) then
    v_insert_cols := v_insert_cols || ', created_at';
    v_insert_vals := v_insert_vals || ', now()';
  end if;

  if exists (
    select 1
    from information_schema.columns
    where table_schema = 'auto_grading'
      and table_name = 'student_courses'
      and column_name = 'updated_at'
  ) then
    v_insert_cols := v_insert_cols || ', updated_at';
    v_insert_vals := v_insert_vals || ', now()';
  end if;

  execute format(
    'insert into auto_grading.student_courses (%s) values (%s)',
    v_insert_cols,
    v_insert_vals
  )
  using p_student_id, p_course_id, p_service_type;

  return jsonb_build_object(
    'ok', true,
    'student_id', p_student_id,
    'course_id', p_course_id,
    'service_type', p_service_type,
    'message', 'student attached to course'
  );
end;
$function$;

grant execute on function auto_grading.teacher_attach_student_to_course(
  uuid, uuid, text
) to authenticated;
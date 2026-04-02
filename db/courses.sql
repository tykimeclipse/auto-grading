begin;

create extension if not exists pgcrypto;

create table if not exists auto_grading.courses (
  id uuid primary key default gen_random_uuid()
);

alter table auto_grading.courses
  add column if not exists course_code integer,
  add column if not exists course_name text,
  add column if not exists open_year integer,
  add column if not exists start_date date,
  add column if not exists end_date date,
  add column if not exists course_type text,
  add column if not exists note text,
  add column if not exists is_active boolean not null default true,
  add column if not exists created_at timestamptz not null default now(),
  add column if not exists updated_at timestamptz not null default now();

update auto_grading.courses
set is_active = true
where is_active is null;

do $$
begin
  if exists (
    select 1
    from auto_grading.courses
    where course_code is null
       or course_name is null
       or open_year is null
  ) then
    raise exception 'courses 테이블에 course_code/course_name/open_year 중 null 값이 있습니다. 먼저 정리한 뒤 다시 실행해 주세요.';
  end if;
end $$;

alter table auto_grading.courses
  alter column course_code set not null,
  alter column course_name set not null,
  alter column open_year set not null;

do $$
begin
  if not exists (
    select 1
    from pg_constraint
    where conname = 'courses_course_code_unique'
      and conrelid = 'auto_grading.courses'::regclass
  ) then
    alter table auto_grading.courses
      add constraint courses_course_code_unique unique (course_code);
  end if;

  if not exists (
    select 1
    from pg_constraint
    where conname = 'courses_open_year_chk'
      and conrelid = 'auto_grading.courses'::regclass
  ) then
    alter table auto_grading.courses
      add constraint courses_open_year_chk
      check (open_year between 2000 and 2100);
  end if;

  if not exists (
    select 1
    from pg_constraint
    where conname = 'courses_date_range_chk'
      and conrelid = 'auto_grading.courses'::regclass
  ) then
    alter table auto_grading.courses
      add constraint courses_date_range_chk
      check (start_date is null or end_date is null or end_date >= start_date);
  end if;

  if not exists (
    select 1
    from pg_constraint
    where conname = 'courses_course_type_chk'
      and conrelid = 'auto_grading.courses'::regclass
  ) then
    alter table auto_grading.courses
      add constraint courses_course_type_chk
      check (course_type is null or course_type in ('과외', '학원', '내신', '특강'));
  end if;
end $$;

create index if not exists idx_courses_course_name
  on auto_grading.courses (course_name);

create index if not exists idx_courses_open_year
  on auto_grading.courses (open_year);

create index if not exists idx_courses_course_type
  on auto_grading.courses (course_type);

create index if not exists idx_courses_is_active
  on auto_grading.courses (is_active);

create sequence if not exists auto_grading.course_code_seq start 101;

do $$
declare
  v_next integer;
begin
  select greatest(coalesce(max(course_code), 100) + 1, 101)
    into v_next
  from auto_grading.courses;

  perform setval('auto_grading.course_code_seq', v_next, false);
end $$;

create or replace function auto_grading.set_updated_at()
returns trigger
language plpgsql
as $function$
begin
  new.updated_at := now();
  return new;
end;
$function$;

drop trigger if exists trg_courses_updated_at on auto_grading.courses;
create trigger trg_courses_updated_at
before update on auto_grading.courses
for each row
execute function auto_grading.set_updated_at();

create or replace function auto_grading.teacher_get_next_course_code()
returns integer
language plpgsql
security definer
set search_path to 'auto_grading', 'public'
as $function$
declare
  v_next integer;
begin
  select greatest(
    coalesce((select max(course_code) from auto_grading.courses), 100) + 1,
    101
  )
  into v_next;

  return v_next;
end;
$function$;

create or replace function auto_grading.teacher_create_course(
  p_course_name text,
  p_open_year integer,
  p_start_date date default null,
  p_end_date date default null,
  p_course_type text default null,
  p_note text default null
)
returns jsonb
language plpgsql
security definer
set search_path to 'auto_grading', 'public'
as $function$
declare
  v_course auto_grading.courses%rowtype;
  v_course_code integer;
  v_try_count integer := 0;
  v_next integer;
begin
  if nullif(btrim(p_course_name), '') is null then
    raise exception 'p_course_name is required';
  end if;

  if p_open_year is null then
    raise exception 'p_open_year is required';
  end if;

  if p_open_year < 2000 or p_open_year > 2100 then
    raise exception 'p_open_year must be between 2000 and 2100';
  end if;

  if p_start_date is not null and p_end_date is not null and p_end_date < p_start_date then
    raise exception 'p_end_date must be greater than or equal to p_start_date';
  end if;

  if p_course_type is not null and p_course_type not in ('과외', '학원', '내신', '특강') then
    raise exception 'p_course_type must be one of 과외, 학원, 내신, 특강';
  end if;

  loop
    v_try_count := v_try_count + 1;

    select greatest(coalesce(max(course_code), 100) + 1, 101)
      into v_next
    from auto_grading.courses;

    perform setval('auto_grading.course_code_seq', v_next, false);

    v_course_code := nextval('auto_grading.course_code_seq');

    begin
      insert into auto_grading.courses (
        course_code,
        course_name,
        open_year,
        start_date,
        end_date,
        course_type,
        note,
        is_active
      )
      values (
        v_course_code,
        nullif(btrim(p_course_name), ''),
        p_open_year,
        p_start_date,
        p_end_date,
        p_course_type,
        nullif(btrim(p_note), ''),
        true
      )
      returning * into v_course;

      exit;
    exception
      when unique_violation then
        if v_try_count >= 5 then
          raise exception 'course_code allocation failed after % attempts', v_try_count;
        end if;
    end;
  end loop;

  return jsonb_build_object(
    'ok', true,
    'course_id', v_course.id,
    'course_code', v_course.course_code,
    'course_name', v_course.course_name,
    'open_year', v_course.open_year,
    'start_date', v_course.start_date,
    'end_date', v_course.end_date,
    'course_type', v_course.course_type,
    'note', v_course.note,
    'is_active', v_course.is_active
  );
end;
$function$;

create or replace function auto_grading.teacher_list_course_catalog(
  p_search text default null,
  p_only_active boolean default false,
  p_limit integer default 100,
  p_offset integer default 0
)
returns table(
  total_count bigint,
  course_id uuid,
  course_code integer,
  course_name text,
  open_year integer,
  start_date date,
  end_date date,
  course_type text,
  note text,
  is_active boolean,
  created_at timestamptz
)
language sql
security definer
set search_path to 'auto_grading', 'public'
as $function$
with base as (
  select
    c.id as course_id,
    c.course_code,
    c.course_name,
    c.open_year,
    c.start_date,
    c.end_date,
    c.course_type,
    c.note,
    c.is_active,
    c.created_at
  from auto_grading.courses c
  where 1 = 1
    and (
      p_search is null
      or btrim(p_search) = ''
      or coalesce(c.course_name, '') ilike '%' || p_search || '%'
      or c.course_code::text ilike '%' || p_search || '%'
      or coalesce(c.course_type, '') ilike '%' || p_search || '%'
    )
    and (
      not p_only_active
      or c.is_active = true
    )
)
select
  count(*) over() as total_count,
  b.course_id,
  b.course_code,
  b.course_name,
  b.open_year,
  b.start_date,
  b.end_date,
  b.course_type,
  b.note,
  b.is_active,
  b.created_at
from base b
order by
  b.open_year desc,
  b.course_code desc,
  b.course_name asc
limit greatest(coalesce(p_limit, 100), 1)
offset greatest(coalesce(p_offset, 0), 0);
$function$;

grant execute on function auto_grading.teacher_get_next_course_code() to authenticated;
grant execute on function auto_grading.teacher_create_course(text, integer, date, date, text, text) to authenticated;
grant execute on function auto_grading.teacher_list_course_catalog(text, boolean, integer, integer) to authenticated;

comment on function auto_grading.teacher_get_next_course_code()
is '참고용 다음 수강코드 조회. 동시성 상황에서는 실제 저장 결과와 다를 수 있으므로 확정값으로 사용하지 않는다.';

comment on function auto_grading.teacher_create_course(text, integer, date, date, text, text)
is '수강과정 등록 RPC. 저장 직전 sequence를 max(course_code)+1 기준으로 보정하고 unique_violation 발생 시 최대 5회 재시도한다.';

commit;
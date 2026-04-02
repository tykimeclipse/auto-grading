begin;

create extension if not exists pgcrypto;

-- =========================================================
-- 1) 기존 courses 테이블에 새 컬럼 안전 추가
--    - 기존 운영 구조 유지
--    - course_code(text), grade_level(text), subject_group(text) 보존
--    - 새 요구사항은 course_no / open_year / start_date / end_date / course_type / note 로 추가
-- =========================================================
alter table auto_grading.courses
  add column if not exists course_no integer,
  add column if not exists open_year integer,
  add column if not exists start_date date,
  add column if not exists end_date date,
  add column if not exists course_type text,
  add column if not exists note text;

-- =========================================================
-- 2) 기본 제약 추가
-- =========================================================
do $$
begin
  if not exists (
    select 1
    from pg_constraint
    where conname = 'courses_open_year_chk'
      and conrelid = 'auto_grading.courses'::regclass
  ) then
    alter table auto_grading.courses
      add constraint courses_open_year_chk
      check (open_year is null or open_year between 2000 and 2100);
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

create index if not exists idx_courses_course_no
  on auto_grading.courses (course_no);

create index if not exists idx_courses_open_year
  on auto_grading.courses (open_year);

create index if not exists idx_courses_course_type
  on auto_grading.courses (course_type);

-- =========================================================
-- 3) course_no 시퀀스 준비
-- =========================================================
create sequence if not exists auto_grading.course_no_seq start 101;

-- =========================================================
-- 4) 기존 null course_no 백필
--    - 운영 테이블에 이미 row가 있어도 안전하게 101부터 채움
-- =========================================================
with base as (
  select greatest(coalesce(max(course_no), 100), 100) as max_no
  from auto_grading.courses
),
missing as (
  select
    id,
    row_number() over (order by created_at nulls last, id) as rn
  from auto_grading.courses
  where course_no is null
)
update auto_grading.courses c
set course_no = base.max_no + missing.rn
from base, missing
where c.id = missing.id;

-- =========================================================
-- 5) course_no 중복 검사
--    - 중복이 있으면 unique 제약 추가 전에 중단
-- =========================================================
do $$
begin
  if exists (
    select 1
    from auto_grading.courses
    where course_no is not null
    group by course_no
    having count(*) > 1
  ) then
    raise exception 'courses.course_no 중복 데이터가 있어 마이그레이션을 계속할 수 없습니다. 중복 정리 후 다시 실행해 주세요.';
  end if;
end $$;

-- =========================================================
-- 6) course_no 제약 추가
--    - null 백필 + 중복 검사 이후에만 적용
-- =========================================================
do $$
begin
  if exists (
    select 1
    from auto_grading.courses
    where course_no is null
  ) then
    raise exception 'courses.course_no null 값이 남아 있어 not null 제약을 적용할 수 없습니다.';
  end if;
end $$;

alter table auto_grading.courses
  alter column course_no set not null;

do $$
begin
  if not exists (
    select 1
    from pg_constraint
    where conname = 'courses_course_no_unique'
      and conrelid = 'auto_grading.courses'::regclass
  ) then
    alter table auto_grading.courses
      add constraint courses_course_no_unique unique (course_no);
  end if;
end $$;

-- =========================================================
-- 7) 시퀀스 위치 보정
-- =========================================================
do $$
declare
  v_next integer;
begin
  select greatest(coalesce(max(course_no), 100) + 1, 101)
    into v_next
  from auto_grading.courses;

  perform setval('auto_grading.course_no_seq', v_next, false);
end $$;

-- =========================================================
-- 8) courses 전용 updated_at 트리거 함수
--    - 공용 set_updated_at() 재정의하지 않음
--    - updated_at 컬럼이 있을 때만 trigger 생성
-- =========================================================
create or replace function auto_grading.set_courses_updated_at()
returns trigger
language plpgsql
as $function$
begin
  new.updated_at := now();
  return new;
end;
$function$;

drop trigger if exists trg_courses_updated_at on auto_grading.courses;

do $$
begin
  if exists (
    select 1
    from information_schema.columns
    where table_schema = 'auto_grading'
      and table_name = 'courses'
      and column_name = 'updated_at'
  ) then
    create trigger trg_courses_updated_at
    before update on auto_grading.courses
    for each row
    execute function auto_grading.set_courses_updated_at();
  end if;
end $$;

-- =========================================================
-- 9) 참고용 다음 course_no 조회
--    - 확정값이 아니라 preview 용
-- =========================================================
create or replace function auto_grading.teacher_get_next_course_no()
returns integer
language plpgsql
security definer
set search_path to 'auto_grading', 'public'
as $function$
declare
  v_next integer;
begin
  select greatest(coalesce(max(course_no), 100) + 1, 101)
    into v_next
  from auto_grading.courses;

  return v_next;
end;
$function$;

-- =========================================================
-- 10) 과정 등록 RPC
--     - 존재하는 컬럼만 insert
--     - 기존 운영 테이블 구조 차이를 동적으로 흡수
--     - grade_level / subject_group 이 NOT NULL이면 course_name에서 추론
-- =========================================================
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
  v_course_no integer;
  v_try_count integer := 0;
  v_next integer;

  v_has_course_no boolean;
  v_has_course_code boolean;
  v_has_course_name boolean;
  v_has_open_year boolean;
  v_has_start_date boolean;
  v_has_end_date boolean;
  v_has_course_type boolean;
  v_has_note boolean;
  v_has_is_active boolean;
  v_has_created_at boolean;
  v_has_updated_at boolean;
  v_has_grade_level boolean;
  v_has_subject_group boolean;

  v_grade_level_nullable boolean := true;
  v_subject_group_nullable boolean := true;

  v_inferred_grade_level text;
  v_inferred_subject_group text;

  v_cols text[] := array[]::text[];
  v_vals text[] := array[]::text[];
  v_sql text;
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

  -- 컬럼 존재 여부 점검
  select exists (
    select 1 from information_schema.columns
    where table_schema = 'auto_grading' and table_name = 'courses' and column_name = 'course_no'
  ) into v_has_course_no;

  select exists (
    select 1 from information_schema.columns
    where table_schema = 'auto_grading' and table_name = 'courses' and column_name = 'course_code'
  ) into v_has_course_code;

  select exists (
    select 1 from information_schema.columns
    where table_schema = 'auto_grading' and table_name = 'courses' and column_name = 'course_name'
  ) into v_has_course_name;

  select exists (
    select 1 from information_schema.columns
    where table_schema = 'auto_grading' and table_name = 'courses' and column_name = 'open_year'
  ) into v_has_open_year;

  select exists (
    select 1 from information_schema.columns
    where table_schema = 'auto_grading' and table_name = 'courses' and column_name = 'start_date'
  ) into v_has_start_date;

  select exists (
    select 1 from information_schema.columns
    where table_schema = 'auto_grading' and table_name = 'courses' and column_name = 'end_date'
  ) into v_has_end_date;

  select exists (
    select 1 from information_schema.columns
    where table_schema = 'auto_grading' and table_name = 'courses' and column_name = 'course_type'
  ) into v_has_course_type;

  select exists (
    select 1 from information_schema.columns
    where table_schema = 'auto_grading' and table_name = 'courses' and column_name = 'note'
  ) into v_has_note;

  select exists (
    select 1 from information_schema.columns
    where table_schema = 'auto_grading' and table_name = 'courses' and column_name = 'is_active'
  ) into v_has_is_active;

  select exists (
    select 1 from information_schema.columns
    where table_schema = 'auto_grading' and table_name = 'courses' and column_name = 'created_at'
  ) into v_has_created_at;

  select exists (
    select 1 from information_schema.columns
    where table_schema = 'auto_grading' and table_name = 'courses' and column_name = 'updated_at'
  ) into v_has_updated_at;

  select exists (
    select 1 from information_schema.columns
    where table_schema = 'auto_grading' and table_name = 'courses' and column_name = 'grade_level'
  ) into v_has_grade_level;

  select exists (
    select 1 from information_schema.columns
    where table_schema = 'auto_grading' and table_name = 'courses' and column_name = 'subject_group'
  ) into v_has_subject_group;

  if v_has_grade_level then
    select case when is_nullable = 'YES' then true else false end
      into v_grade_level_nullable
    from information_schema.columns
    where table_schema = 'auto_grading'
      and table_name = 'courses'
      and column_name = 'grade_level';
  end if;

  if v_has_subject_group then
    select case when is_nullable = 'YES' then true else false end
      into v_subject_group_nullable
    from information_schema.columns
    where table_schema = 'auto_grading'
      and table_name = 'courses'
      and column_name = 'subject_group';
  end if;

  -- 제목에서 학년 추론
  v_inferred_grade_level :=
    case
      when p_course_name like '%중1%' then 'M1'
      when p_course_name like '%중2%' then 'M2'
      when p_course_name like '%중3%' then 'M3'
      when p_course_name like '%고1%' then 'H1'
      when p_course_name like '%고2%' then 'H2'
      when p_course_name like '%고3%' then 'H3'
      else null
    end;

  -- 제목에서 과목군 추론
  v_inferred_subject_group :=
    case
      when p_course_name like '%과학%' then '과학'
      when p_course_name like '%수학%' then '수학'
      when p_course_name like '%영어%' then '영어'
      when p_course_name like '%국어%' then '국어'
      when p_course_name like '%사회%' then '사회'
      else null
    end;

  if v_has_grade_level and not v_grade_level_nullable and v_inferred_grade_level is null then
    raise exception 'grade_level 컬럼이 NOT NULL인데 course_name에서 학년을 추론할 수 없습니다. 제목에 중1/중2/중3/고1/고2/고3 중 하나가 포함되어야 합니다.';
  end if;

  if v_has_subject_group and not v_subject_group_nullable and v_inferred_subject_group is null then
    raise exception 'subject_group 컬럼이 NOT NULL인데 course_name에서 과목군을 추론할 수 없습니다.';
  end if;

  perform pg_advisory_xact_lock(hashtextextended('auto_grading.courses.course_no', 0));

  loop
    v_try_count := v_try_count + 1;

    select greatest(coalesce(max(course_no), 100) + 1, 101)
      into v_next
    from auto_grading.courses;

    perform setval('auto_grading.course_no_seq', v_next, false);
    v_course_no := nextval('auto_grading.course_no_seq');

    v_cols := array[]::text[];
    v_vals := array[]::text[];

    if v_has_course_no then
      v_cols := array_append(v_cols, format('%I', 'course_no'));
      v_vals := array_append(v_vals, format('%L', v_course_no));
    end if;

    if v_has_course_name then
      v_cols := array_append(v_cols, format('%I', 'course_name'));
      v_vals := array_append(v_vals, format('%L', nullif(btrim(p_course_name), '')));
    end if;

    if v_has_open_year then
      v_cols := array_append(v_cols, format('%I', 'open_year'));
      v_vals := array_append(v_vals, format('%L', p_open_year));
    end if;

    if v_has_start_date then
      v_cols := array_append(v_cols, format('%I', 'start_date'));
      v_vals := array_append(v_vals, case when p_start_date is null then 'null' else format('%L', p_start_date) end);
    end if;

    if v_has_end_date then
      v_cols := array_append(v_cols, format('%I', 'end_date'));
      v_vals := array_append(v_vals, case when p_end_date is null then 'null' else format('%L', p_end_date) end);
    end if;

    if v_has_course_type then
      v_cols := array_append(v_cols, format('%I', 'course_type'));
      v_vals := array_append(v_vals, case when p_course_type is null then 'null' else format('%L', p_course_type) end);
    end if;

    if v_has_note then
      v_cols := array_append(v_cols, format('%I', 'note'));
      v_vals := array_append(v_vals, case when nullif(btrim(p_note), '') is null then 'null' else format('%L', nullif(btrim(p_note), '')) end);
    end if;

    if v_has_is_active then
      v_cols := array_append(v_cols, format('%I', 'is_active'));
      v_vals := array_append(v_vals, 'true');
    end if;

    if v_has_course_code then
      v_cols := array_append(v_cols, format('%I', 'course_code'));
      v_vals := array_append(v_vals, format('%L', 'COURSE-' || v_course_no::text));
    end if;

    if v_has_grade_level then
      v_cols := array_append(v_cols, format('%I', 'grade_level'));
      v_vals := array_append(v_vals, case when v_inferred_grade_level is null then 'null' else format('%L', v_inferred_grade_level) end);
    end if;

    if v_has_subject_group then
      v_cols := array_append(v_cols, format('%I', 'subject_group'));
      v_vals := array_append(v_vals, case when v_inferred_subject_group is null then 'null' else format('%L', v_inferred_subject_group) end);
    end if;

    if v_has_created_at then
      v_cols := array_append(v_cols, format('%I', 'created_at'));
      v_vals := array_append(v_vals, 'now()');
    end if;

    if v_has_updated_at then
      v_cols := array_append(v_cols, format('%I', 'updated_at'));
      v_vals := array_append(v_vals, 'now()');
    end if;

    v_sql := format(
      'insert into auto_grading.courses (%s) values (%s) returning *',
      array_to_string(v_cols, ', '),
      array_to_string(v_vals, ', ')
    );

    begin
      execute v_sql into v_course;
      exit;
    exception
      when unique_violation then
        if v_try_count >= 5 then
          raise exception 'course_no allocation failed after % attempts', v_try_count;
        end if;
    end;
  end loop;

  return jsonb_build_object(
    'ok', true,
    'course_id', v_course.id,
    'course_no', v_course.course_no,
    'course_code', v_course.course_code,
    'course_name', v_course.course_name,
    'grade_level', v_course.grade_level,
    'subject_group', v_course.subject_group,
    'open_year', v_course.open_year,
    'start_date', v_course.start_date,
    'end_date', v_course.end_date,
    'course_type', v_course.course_type,
    'note', v_course.note,
    'is_active', v_course.is_active
  );
end;
$function$;

-- =========================================================
-- 11) 과정 목록 조회 RPC
--     - 등록 페이지에 필요한 컬럼만 반환
--     - legacy 의존성 최소화
-- =========================================================
create or replace function auto_grading.teacher_list_course_catalog(
  p_search text default null,
  p_only_active boolean default false,
  p_limit integer default 100,
  p_offset integer default 0
)
returns table(
  total_count bigint,
  course_id uuid,
  course_no integer,
  course_code text,
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
    c.course_no,
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
      or coalesce(c.course_code, '') ilike '%' || p_search || '%'
      or coalesce(c.course_no::text, '') ilike '%' || p_search || '%'
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
  b.course_no,
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
  b.open_year desc nulls last,
  b.course_no desc nulls last,
  b.course_name asc
limit greatest(coalesce(p_limit, 100), 1)
offset greatest(coalesce(p_offset, 0), 0);
$function$;

grant execute on function auto_grading.teacher_get_next_course_no() to authenticated;
grant execute on function auto_grading.teacher_create_course(text, integer, date, date, text, text) to authenticated;
grant execute on function auto_grading.teacher_list_course_catalog(text, boolean, integer, integer) to authenticated;

comment on function auto_grading.teacher_get_next_course_no()
is '참고용 다음 수강번호 조회. 실제 확정 번호는 teacher_create_course 반환값을 따른다.';

comment on function auto_grading.teacher_create_course(text, integer, date, date, text, text)
is '현재 운영 courses 테이블 구조에 맞춘 안전한 등록 RPC. 존재하는 컬럼만 insert 하며, grade_level/subject_group이 NOT NULL이면 제목에서 추론한다.';

comment on function auto_grading.teacher_list_course_catalog(text, boolean, integer, integer)
is '과정 등록/조회 화면용 목록 RPC. 등록 페이지에 필요한 컬럼만 반환한다.';

commit;
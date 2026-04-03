begin;

-- 1) student_courses에 새 관계 변수 추가
alter table auto_grading.student_courses
  add column if not exists student_course_type text;

comment on column auto_grading.student_courses.student_course_type
is '학생-수강과정 관계 변수. 허용값: 학원, 과외, 내신, 특강';

-- 2) 허용값 체크 제약 추가
do $$
begin
  if not exists (
    select 1
    from pg_constraint
    where conname = 'chk_student_courses_student_course_type'
      and conrelid = 'auto_grading.student_courses'::regclass
  ) then
    alter table auto_grading.student_courses
      add constraint chk_student_courses_student_course_type
      check (
        student_course_type is null
        or student_course_type in ('학원', '과외', '내신', '특강')
      );
  end if;
end
$$;

commit;

-- 확인용 1: 컬럼 생성 여부
select
  column_name,
  data_type,
  is_nullable
from information_schema.columns
where table_schema = 'auto_grading'
  and table_name = 'student_courses'
  and column_name = 'student_course_type';

-- 확인용 2: 제약 생성 여부
select
  conname,
  pg_get_constraintdef(oid) as constraint_def
from pg_constraint
where conrelid = 'auto_grading.student_courses'::regclass
  and conname = 'chk_student_courses_student_course_type';

-- 확인용 3: 현재 데이터 분포
select
  student_course_type,
  count(*) as cnt
from auto_grading.student_courses
group by student_course_type
order by student_course_type;
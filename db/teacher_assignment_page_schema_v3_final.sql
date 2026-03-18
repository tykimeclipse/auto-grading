begin;

create schema if not exists auto_grading;

--------------------------------------------------
-- 1) courses : 수강과정 마스터
--------------------------------------------------
create table if not exists auto_grading.courses (
  id uuid primary key default gen_random_uuid(),
  course_code text not null unique,
  course_name text not null,
  grade_level text not null,
  subject_group text null,
  is_active boolean not null default true,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  constraint courses_grade_level_chk
    check (grade_level in ('M1', 'M2', 'M3', 'H1', 'H2', 'H3'))
);

create index if not exists idx_courses_grade_level
  on auto_grading.courses (grade_level);

create index if not exists idx_courses_is_active
  on auto_grading.courses (is_active);

--------------------------------------------------
-- 2) student_courses : 학생-수강과정 연결
--    수강 이력 허용
--    활성 상태에서만 (student_id, course_id) 중복 방지
--------------------------------------------------
create table if not exists auto_grading.student_courses (
  id uuid primary key default gen_random_uuid(),
  student_id uuid not null references auto_grading.students(id) on delete cascade,
  course_id uuid not null references auto_grading.courses(id) on delete cascade,
  enrollment_type text not null default '정규',
  is_active boolean not null default true,
  joined_at timestamptz not null default now(),
  ended_at timestamptz null,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  constraint student_courses_enrollment_type_chk
    check (enrollment_type in ('정규', '내신', '과외'))
);

create index if not exists idx_student_courses_student_id
  on auto_grading.student_courses (student_id);

create index if not exists idx_student_courses_course_id
  on auto_grading.student_courses (course_id);

create index if not exists idx_student_courses_is_active
  on auto_grading.student_courses (is_active);

create index if not exists idx_student_courses_enrollment_type
  on auto_grading.student_courses (enrollment_type);

create unique index if not exists uq_student_courses_active
  on auto_grading.student_courses (student_id, course_id)
  where is_active = true;

--------------------------------------------------
-- 3) assignments 보강
--    기존 status / 기존 unique(student_id, test_set_id) 유지
--    즉 이번 단계에서는 course_id는 통계/조회용 태그 성격
--------------------------------------------------
alter table auto_grading.assignments
  add column if not exists course_id uuid null
    references auto_grading.courses(id) on delete set null;

alter table auto_grading.assignments
  add column if not exists purpose text null;

alter table auto_grading.assignments
  add column if not exists closed_at timestamptz null;

alter table auto_grading.assignments
  add column if not exists closed_reason text null;

alter table auto_grading.assignments
  add column if not exists updated_at timestamptz null;

alter table auto_grading.assignments
  alter column updated_at set default now();

do $$
begin
  if not exists (
    select 1
    from pg_constraint
    where conname = 'assignments_purpose_chk'
      and connamespace = 'auto_grading'::regnamespace
  ) then
    alter table auto_grading.assignments
      add constraint assignments_purpose_chk
      check (purpose is null or purpose in ('숙제', '클리닉', '보충'));
  end if;
end $$;

create index if not exists idx_assignments_course_id
  on auto_grading.assignments (course_id);

create index if not exists idx_assignments_purpose
  on auto_grading.assignments (purpose);

create index if not exists idx_assignments_assigned_at_desc
  on auto_grading.assignments (assigned_at desc);

--------------------------------------------------
-- 4) attempts 보강
--    의미 정리
--    first_*         = 학생 1차 성취도
--    second_*        = 2차에서 추가로 맞춘 개수(기존 모델 유지)
--    final_*         = 학생 2차까지 입력 후 누적 성취도
--    teacher_final_* = 교사가 최종 검토 후 확정한 성취도
--------------------------------------------------
alter table auto_grading.attempts
  add column if not exists teacher_final_correct_count integer null;

alter table auto_grading.attempts
  add column if not exists teacher_final_score_percent numeric(5,2) null;

alter table auto_grading.attempts
  add column if not exists teacher_final_note text null;

alter table auto_grading.attempts
  add column if not exists teacher_final_updated_at timestamptz null;

alter table auto_grading.attempts
  add column if not exists teacher_final_updated_by text null;

alter table auto_grading.attempts
  add column if not exists updated_at timestamptz null;

alter table auto_grading.attempts
  alter column updated_at set default now();

do $$
begin
  if not exists (
    select 1
    from pg_constraint
    where conname = 'attempts_teacher_final_score_percent_chk'
      and connamespace = 'auto_grading'::regnamespace
  ) then
    alter table auto_grading.attempts
      add constraint attempts_teacher_final_score_percent_chk
      check (
        teacher_final_score_percent is null
        or (teacher_final_score_percent >= 0 and teacher_final_score_percent <= 100)
      );
  end if;
end $$;

do $$
begin
  if not exists (
    select 1
    from pg_constraint
    where conname = 'attempts_teacher_final_correct_count_chk'
      and connamespace = 'auto_grading'::regnamespace
  ) then
    alter table auto_grading.attempts
      add constraint attempts_teacher_final_correct_count_chk
      check (
        teacher_final_correct_count is null
        or teacher_final_correct_count >= 0
      );
  end if;
end $$;

do $$
begin
  if not exists (
    select 1
    from pg_constraint
    where conname = 'attempts_teacher_final_pair_chk'
      and connamespace = 'auto_grading'::regnamespace
  ) then
    alter table auto_grading.attempts
      add constraint attempts_teacher_final_pair_chk
      check (
        (teacher_final_correct_count is null and teacher_final_score_percent is null)
        or
        (teacher_final_correct_count is not null and teacher_final_score_percent is not null)
      );
  end if;
end $$;

create index if not exists idx_attempts_assignment_id
  on auto_grading.attempts (assignment_id);

create index if not exists idx_attempts_student_id
  on auto_grading.attempts (student_id);

create index if not exists idx_attempts_teacher_final_updated_at
  on auto_grading.attempts (teacher_final_updated_at desc);

--------------------------------------------------
-- 5) updated_at 트리거 함수
--------------------------------------------------
create or replace function auto_grading.set_updated_at()
returns trigger
language plpgsql
as $$
begin
  new.updated_at = now();
  return new;
end;
$$;

do $$
begin
  if not exists (
    select 1
    from pg_trigger
    where tgname = 'trg_courses_set_updated_at'
  ) then
    create trigger trg_courses_set_updated_at
    before update on auto_grading.courses
    for each row
    execute function auto_grading.set_updated_at();
  end if;
end $$;

do $$
begin
  if not exists (
    select 1
    from pg_trigger
    where tgname = 'trg_student_courses_set_updated_at'
  ) then
    create trigger trg_student_courses_set_updated_at
    before update on auto_grading.student_courses
    for each row
    execute function auto_grading.set_updated_at();
  end if;
end $$;

do $$
begin
  if not exists (
    select 1
    from pg_trigger
    where tgname = 'trg_assignments_set_updated_at'
  ) then
    create trigger trg_assignments_set_updated_at
    before update on auto_grading.assignments
    for each row
    execute function auto_grading.set_updated_at();
  end if;
end $$;

do $$
begin
  if not exists (
    select 1
    from pg_trigger
    where tgname = 'trg_attempts_set_updated_at'
  ) then
    create trigger trg_attempts_set_updated_at
    before update on auto_grading.attempts
    for each row
    execute function auto_grading.set_updated_at();
  end if;
end $$;

--------------------------------------------------
-- 6) 컬럼 주석
--------------------------------------------------
comment on column auto_grading.student_courses.ended_at
is '학생이 해당 수강과정에서 비활성화/종료된 시점. 수강과정 이력용.';

comment on column auto_grading.assignments.course_id
is '교사용 발행 페이지에서 assignment를 특정 수강과정에 연결하기 위한 태그. 기존 데이터는 null일 수 있음.';

comment on column auto_grading.assignments.closed_at
is '개별 assignment가 비활성화/종료된 시점. 시험 발행 건 단위 종료 시점.';

comment on column auto_grading.assignments.closed_reason
is 'assignment 종료 사유. 예: 2차 완료, 교사 중단, 오발행 정리';

comment on column auto_grading.assignments.purpose
is '발행 용도. 숙제/클리닉/보충';

comment on column auto_grading.attempts.teacher_final_correct_count
is '교사가 최종 검토 후 확정한 정답 수. 학생 2차 누적 결과(final_*)와 분리 저장.';

comment on column auto_grading.attempts.teacher_final_score_percent
is '교사가 최종 검토 후 확정한 최종 성취도 퍼센트. 학생/학부모 화면에서는 이 값이 있으면 2차 성취도로 우선 표시한다.';

comment on column auto_grading.attempts.teacher_final_note
is '교사 최종 보정 사유 또는 메모';

comment on column auto_grading.attempts.teacher_final_updated_at
is '교사가 최종 성취도를 마지막으로 수정한 시점';

comment on column auto_grading.attempts.teacher_final_updated_by
is '교사가 최종 성취도를 수정한 내부 사용자 식별자 문자열. 표시 이름 대신 변하지 않는 ID를 저장하는 것을 권장.';

comment on column auto_grading.attempts.updated_at
is 'attempt 행 자체가 수정된 시점. 기존 행은 null일 수 있으며, 마지막 활동 시점과 동일하다고 가정하면 안 됨.';

commit;
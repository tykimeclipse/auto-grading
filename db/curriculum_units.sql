begin;

create extension if not exists pgcrypto;

-- 1) test_sets에 기준 식별용 컬럼만 추가
alter table auto_grading.test_sets
  add column if not exists curriculum_version text,
  add column if not exists unit_code text,
  add column if not exists source_category text;

-- 2) curriculum_units 기준 테이블 생성
create table if not exists auto_grading.curriculum_units (
  id uuid primary key default gen_random_uuid(),

  grade_level text not null,
  curriculum_version text not null,
  subject text not null,

  unit_code text not null,

  major_unit_code text not null,
  major_unit_name text not null,

  minor_unit_code text not null,
  minor_unit_name text not null,

  nano_unit_code text not null,
  nano_unit_name text not null,

  is_active boolean not null default true,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),

  constraint curriculum_units_unique
    unique (grade_level, curriculum_version, subject, unit_code)
);

-- 3) check constraint 추가
do $$
begin
  if not exists (
    select 1
    from pg_constraint
    where conname = 'curriculum_units_unit_code_format_chk'
      and conrelid = 'auto_grading.curriculum_units'::regclass
  ) then
    alter table auto_grading.curriculum_units
      add constraint curriculum_units_unit_code_format_chk
      check (unit_code ~ '^\d{3}$');
  end if;

  if not exists (
    select 1
    from pg_constraint
    where conname = 'curriculum_units_major_unit_code_format_chk'
      and conrelid = 'auto_grading.curriculum_units'::regclass
  ) then
    alter table auto_grading.curriculum_units
      add constraint curriculum_units_major_unit_code_format_chk
      check (major_unit_code ~ '^\d+$');
  end if;

  if not exists (
    select 1
    from pg_constraint
    where conname = 'curriculum_units_minor_unit_code_format_chk'
      and conrelid = 'auto_grading.curriculum_units'::regclass
  ) then
    alter table auto_grading.curriculum_units
      add constraint curriculum_units_minor_unit_code_format_chk
      check (minor_unit_code ~ '^\d+$');
  end if;

  if not exists (
    select 1
    from pg_constraint
    where conname = 'curriculum_units_nano_unit_code_format_chk'
      and conrelid = 'auto_grading.curriculum_units'::regclass
  ) then
    alter table auto_grading.curriculum_units
      add constraint curriculum_units_nano_unit_code_format_chk
      check (nano_unit_code ~ '^\d+$');
  end if;
end $$;

-- 4) updated_at 자동 갱신 함수
create or replace function auto_grading.set_updated_at()
returns trigger
language plpgsql
as $function$
begin
  new.updated_at := now();
  return new;
end;
$function$;

-- 5) curriculum_units updated_at 트리거
drop trigger if exists trg_curriculum_units_set_updated_at
on auto_grading.curriculum_units;

create trigger trg_curriculum_units_set_updated_at
before update on auto_grading.curriculum_units
for each row
execute function auto_grading.set_updated_at();

-- 6) curriculum_units 조회용 인덱스
create index if not exists idx_curriculum_units_lookup
  on auto_grading.curriculum_units (grade_level, curriculum_version, subject, unit_code);

create index if not exists idx_curriculum_units_active_lookup
  on auto_grading.curriculum_units (grade_level, curriculum_version, subject, unit_code)
  where is_active = true;

-- 7) test_sets 쪽도 최소 정합성 check 추가
do $$
begin
  if not exists (
    select 1
    from pg_constraint
    where conname = 'test_sets_unit_code_format_chk'
      and conrelid = 'auto_grading.test_sets'::regclass
  ) then
    alter table auto_grading.test_sets
      add constraint test_sets_unit_code_format_chk
      check (unit_code is null or unit_code ~ '^\d{3}$');
  end if;
end $$;

-- 8) test_sets -> curriculum_units 복합 FK
do $$
begin
  if not exists (
    select 1
    from pg_constraint
    where conname = 'test_sets_curriculum_units_fk'
      and conrelid = 'auto_grading.test_sets'::regclass
  ) then
    alter table auto_grading.test_sets
      add constraint test_sets_curriculum_units_fk
      foreign key (grade_level, curriculum_version, subject, unit_code)
      references auto_grading.curriculum_units (grade_level, curriculum_version, subject, unit_code)
      on update cascade
      on delete restrict
      deferrable initially immediate;
  end if;
end $$;

commit;
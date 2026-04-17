begin;

create extension if not exists pgcrypto;

do $$
begin
  if to_regnamespace('auto_grading') is null then
    raise exception 'schema auto_grading does not exist. run schema.sql first';
  end if;

  if to_regclass('auto_grading.test_sets') is null then
    raise exception 'table auto_grading.test_sets does not exist. run schema.sql first';
  end if;
end $$;

alter table auto_grading.test_sets
  add column if not exists curriculum_version text,
  add column if not exists unit_code text,
  add column if not exists source_category text;

create table if not exists auto_grading.curriculum_units (
  id uuid primary key default gen_random_uuid(),

  grade_level text not null,
  curriculum_version text not null,
  subject text not null,

  unit_code text not null,
  unit_level text not null default 'nano',

  major_unit_code text not null,
  major_unit_name text not null,

  minor_unit_code text not null,
  minor_unit_name text,

  nano_unit_code text not null,
  nano_unit_name text,

  is_active boolean not null default true,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),

  constraint curriculum_units_unique
    unique (grade_level, curriculum_version, subject, unit_code)
);

alter table auto_grading.curriculum_units
  add column if not exists unit_level text;

alter table auto_grading.curriculum_units
  alter column unit_level set default 'nano';

alter table auto_grading.curriculum_units
  alter column minor_unit_name drop not null;

alter table auto_grading.curriculum_units
  alter column nano_unit_name drop not null;

update auto_grading.curriculum_units
set unit_level = case
  when coalesce(nullif(btrim(nano_unit_code), ''), '0') <> '0' then 'nano'
  when coalesce(nullif(btrim(minor_unit_code), ''), '0') <> '0' then 'middle'
  else 'major'
end
where unit_level is null
   or btrim(unit_level) = '';

alter table auto_grading.curriculum_units
  alter column unit_level set not null;

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

  if not exists (
    select 1
    from pg_constraint
    where conname = 'curriculum_units_unit_level_chk'
      and conrelid = 'auto_grading.curriculum_units'::regclass
  ) then
    alter table auto_grading.curriculum_units
      add constraint curriculum_units_unit_level_chk
      check (unit_level in ('major', 'middle', 'nano'));
  end if;

  if not exists (
    select 1
    from pg_constraint
    where conname = 'curriculum_units_hierarchy_chk'
      and conrelid = 'auto_grading.curriculum_units'::regclass
  ) then
    alter table auto_grading.curriculum_units
      add constraint curriculum_units_hierarchy_chk
      check (
        major_unit_code ~ '^[1-9]$'
        and minor_unit_code ~ '^[0-9]$'
        and nano_unit_code ~ '^[0-9]$'
        and unit_code = major_unit_code || minor_unit_code || nano_unit_code
        and (
          (
            unit_level = 'major'
            and minor_unit_code = '0'
            and nano_unit_code = '0'
            and minor_unit_name is null
            and nano_unit_name is null
          )
          or (
            unit_level = 'middle'
            and minor_unit_code <> '0'
            and nano_unit_code = '0'
            and minor_unit_name is not null
            and nano_unit_name is null
          )
          or (
            unit_level = 'nano'
            and minor_unit_code <> '0'
            and nano_unit_code <> '0'
            and minor_unit_name is not null
            and nano_unit_name is not null
          )
        )
      );
  end if;
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

drop trigger if exists trg_curriculum_units_set_updated_at
on auto_grading.curriculum_units;

create trigger trg_curriculum_units_set_updated_at
before update on auto_grading.curriculum_units
for each row
execute function auto_grading.set_updated_at();

create index if not exists idx_curriculum_units_active_lookup
  on auto_grading.curriculum_units (grade_level, curriculum_version, subject, unit_code)
  where is_active = true;

create or replace function auto_grading.upsert_curriculum_unit(
  p_grade_level text,
  p_curriculum_version text,
  p_subject text,
  p_major_unit_code text,
  p_major_unit_name text,
  p_minor_unit_code text default '0',
  p_minor_unit_name text default null,
  p_nano_unit_code text default '0',
  p_nano_unit_name text default null,
  p_is_active boolean default true
)
returns jsonb
language plpgsql
security definer
set search_path to 'auto_grading', 'public'
as $function$
declare
  v_grade_level text := nullif(btrim(p_grade_level), '');
  v_curriculum_version text := nullif(btrim(p_curriculum_version), '');
  v_subject text := nullif(btrim(p_subject), '');
  v_major_code text := coalesce(nullif(regexp_replace(coalesce(p_major_unit_code, ''), '\D', '', 'g'), ''), '0');
  v_minor_code text := coalesce(nullif(regexp_replace(coalesce(p_minor_unit_code, ''), '\D', '', 'g'), ''), '0');
  v_nano_code text := coalesce(nullif(regexp_replace(coalesce(p_nano_unit_code, ''), '\D', '', 'g'), ''), '0');
  v_major_name text := nullif(btrim(p_major_unit_name), '');
  v_minor_name text := nullif(btrim(p_minor_unit_name), '');
  v_nano_name text := nullif(btrim(p_nano_unit_name), '');
  v_unit_code text;
  v_unit_level text;
  v_row auto_grading.curriculum_units%rowtype;
begin
  if v_grade_level is null or v_curriculum_version is null or v_subject is null then
    raise exception 'grade_level, curriculum_version, subject are required';
  end if;

  if v_major_name is null then
    raise exception 'major_unit_name is required';
  end if;

  if v_major_code !~ '^[1-9]$' then
    raise exception 'major_unit_code must be a single digit 1-9';
  end if;

  if v_minor_code !~ '^[0-9]$' then
    raise exception 'minor_unit_code must be a single digit 0-9';
  end if;

  if v_nano_code !~ '^[0-9]$' then
    raise exception 'nano_unit_code must be a single digit 0-9';
  end if;

  if v_minor_code = '0' and v_nano_code <> '0' then
    raise exception 'nano_unit_code cannot be set when minor_unit_code is 0';
  end if;

  if v_minor_code = '0' then
    v_unit_level := 'major';
    v_minor_name := null;
    v_nano_name := null;
  elsif v_nano_code = '0' then
    v_unit_level := 'middle';
    if v_minor_name is null then
      raise exception 'minor_unit_name is required for middle level rows';
    end if;
    v_nano_name := null;
  else
    v_unit_level := 'nano';
    if v_minor_name is null then
      raise exception 'minor_unit_name is required for nano level rows';
    end if;
    if v_nano_name is null then
      raise exception 'nano_unit_name is required for nano level rows';
    end if;
  end if;

  v_unit_code := v_major_code || v_minor_code || v_nano_code;

  insert into auto_grading.curriculum_units (
    grade_level,
    curriculum_version,
    subject,
    unit_code,
    unit_level,
    major_unit_code,
    major_unit_name,
    minor_unit_code,
    minor_unit_name,
    nano_unit_code,
    nano_unit_name,
    is_active
  )
  values (
    v_grade_level,
    v_curriculum_version,
    v_subject,
    v_unit_code,
    v_unit_level,
    v_major_code,
    v_major_name,
    v_minor_code,
    v_minor_name,
    v_nano_code,
    v_nano_name,
    coalesce(p_is_active, true)
  )
  on conflict (grade_level, curriculum_version, subject, unit_code)
  do update set
    unit_level = excluded.unit_level,
    major_unit_code = excluded.major_unit_code,
    major_unit_name = excluded.major_unit_name,
    minor_unit_code = excluded.minor_unit_code,
    minor_unit_name = excluded.minor_unit_name,
    nano_unit_code = excluded.nano_unit_code,
    nano_unit_name = excluded.nano_unit_name,
    is_active = excluded.is_active,
    updated_at = now()
  returning * into v_row;

  return jsonb_build_object(
    'ok', true,
    'unit_code', v_row.unit_code,
    'unit_level', v_row.unit_level,
    'grade_level', v_row.grade_level,
    'curriculum_version', v_row.curriculum_version,
    'subject', v_row.subject
  );
end;
$function$;

create or replace function auto_grading.upsert_curriculum_unit_hierarchy(
  p_grade_level text,
  p_curriculum_version text,
  p_subject text,
  p_major_unit_code text,
  p_major_unit_name text,
  p_minor_unit_code text default null,
  p_minor_unit_name text default null,
  p_nano_unit_code text default null,
  p_nano_unit_name text default null,
  p_is_active boolean default true
)
returns jsonb
language plpgsql
security definer
set search_path to 'auto_grading', 'public'
as $function$
declare
  v_major_code text := coalesce(nullif(regexp_replace(coalesce(p_major_unit_code, ''), '\D', '', 'g'), ''), '0');
  v_minor_code text := coalesce(nullif(regexp_replace(coalesce(p_minor_unit_code, ''), '\D', '', 'g'), ''), '0');
  v_nano_code text := coalesce(nullif(regexp_replace(coalesce(p_nano_unit_code, ''), '\D', '', 'g'), ''), '0');
  v_items jsonb := '[]'::jsonb;
begin
  v_items := v_items || jsonb_build_array(
    auto_grading.upsert_curriculum_unit(
      p_grade_level,
      p_curriculum_version,
      p_subject,
      v_major_code,
      p_major_unit_name,
      '0',
      null,
      '0',
      null,
      p_is_active
    )
  );

  if v_minor_code <> '0' then
    v_items := v_items || jsonb_build_array(
      auto_grading.upsert_curriculum_unit(
        p_grade_level,
        p_curriculum_version,
        p_subject,
        v_major_code,
        p_major_unit_name,
        v_minor_code,
        p_minor_unit_name,
        '0',
        null,
        p_is_active
      )
    );
  end if;

  if v_minor_code <> '0' and v_nano_code <> '0' then
    v_items := v_items || jsonb_build_array(
      auto_grading.upsert_curriculum_unit(
        p_grade_level,
        p_curriculum_version,
        p_subject,
        v_major_code,
        p_major_unit_name,
        v_minor_code,
        p_minor_unit_name,
        v_nano_code,
        p_nano_unit_name,
        p_is_active
      )
    );
  end if;

  return jsonb_build_object(
    'ok', true,
    'items', v_items
  );
end;
$function$;

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

  if not exists (
    select 1
    from pg_constraint
    where conname = 'test_sets_curriculum_ref_all_or_none_chk'
      and conrelid = 'auto_grading.test_sets'::regclass
  ) then
    alter table auto_grading.test_sets
      add constraint test_sets_curriculum_ref_all_or_none_chk
      check (
        (
          grade_level is null
          and curriculum_version is null
          and subject is null
          and unit_code is null
        )
        or
        (
          grade_level is not null
          and curriculum_version is not null
          and subject is not null
          and unit_code is not null
        )
      );
  end if;
end $$;

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

grant execute on function auto_grading.upsert_curriculum_unit(
  text, text, text, text, text, text, text, text, text, boolean
) to authenticated;

grant execute on function auto_grading.upsert_curriculum_unit_hierarchy(
  text, text, text, text, text, text, text, text, text, boolean
) to authenticated;

commit;

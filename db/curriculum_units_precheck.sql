-- curriculum_units.sql 재실행 전 점검용 쿼리
-- 목적:
-- 1) auto_grading.curriculum_units 기존 데이터가 새 계층 제약을 통과할지 확인
-- 2) 문제가 되는 row를 실행 전에 먼저 찾기

-- =========================================================
-- 0. 기본 존재 여부 확인
-- =========================================================
select
  to_regnamespace('auto_grading') as schema_regnamespace,
  to_regclass('auto_grading.curriculum_units') as curriculum_units_table,
  to_regclass('auto_grading.test_sets') as test_sets_table;


-- =========================================================
-- 1. 현재 데이터 개요
-- =========================================================
select
  count(*) as total_rows,
  count(*) filter (where coalesce(is_active, true)) as active_rows
from auto_grading.curriculum_units;

select
  grade_level,
  curriculum_version,
  subject,
  count(*) as row_count
from auto_grading.curriculum_units
group by grade_level, curriculum_version, subject
order by grade_level, curriculum_version, subject;


-- =========================================================
-- 2. 새 스크립트 기준 예상 unit_level 미리 보기
--    참고:
--    - nano_unit_code != 0 이면 nano
--    - 아니고 minor_unit_code != 0 이면 middle
--    - 둘 다 0 이면 major
-- =========================================================
select
  grade_level,
  curriculum_version,
  subject,
  unit_code,
  major_unit_code,
  major_unit_name,
  minor_unit_code,
  minor_unit_name,
  nano_unit_code,
  nano_unit_name,
  case
    when coalesce(nullif(btrim(nano_unit_code), ''), '0') <> '0' then 'nano'
    when coalesce(nullif(btrim(minor_unit_code), ''), '0') <> '0' then 'middle'
    else 'major'
  end as inferred_unit_level
from auto_grading.curriculum_units
order by grade_level, curriculum_version, subject, unit_code;


-- =========================================================
-- 3. 형식 오류 후보
--    아래 결과가 0건이면 좋음
-- =========================================================

-- 3-1. unit_code가 3자리가 아닌 row
select *
from auto_grading.curriculum_units
where coalesce(unit_code, '') !~ '^\d{3}$'
order by grade_level, curriculum_version, subject, unit_code;

-- 3-2. major/minor/nano 코드가 단일 자리 숫자 규칙에 안 맞는 row
select *
from auto_grading.curriculum_units
where coalesce(major_unit_code, '') !~ '^[1-9]$'
   or coalesce(minor_unit_code, '') !~ '^[0-9]$'
   or coalesce(nano_unit_code, '') !~ '^[0-9]$'
order by grade_level, curriculum_version, subject, unit_code;

-- 3-3. unit_code와 major/minor/nano 조합이 일치하지 않는 row
select *
from auto_grading.curriculum_units
where coalesce(unit_code, '') <> coalesce(major_unit_code, '') || coalesce(minor_unit_code, '') || coalesce(nano_unit_code, '')
order by grade_level, curriculum_version, subject, unit_code;

-- 3-4. minor가 0인데 nano가 0이 아닌 잘못된 계층 row
select *
from auto_grading.curriculum_units
where coalesce(minor_unit_code, '') = '0'
  and coalesce(nano_unit_code, '') <> '0'
order by grade_level, curriculum_version, subject, unit_code;


-- =========================================================
-- 4. 새 hierarchy check에 걸릴 row 점검
--    아래 결과가 0건이면 좋음
-- =========================================================
with prepared as (
  select
    cu.*,
    case
      when coalesce(nullif(btrim(cu.nano_unit_code), ''), '0') <> '0' then 'nano'
      when coalesce(nullif(btrim(cu.minor_unit_code), ''), '0') <> '0' then 'middle'
      else 'major'
    end as inferred_unit_level
  from auto_grading.curriculum_units cu
)
select
  grade_level,
  curriculum_version,
  subject,
  unit_code,
  major_unit_code,
  major_unit_name,
  minor_unit_code,
  minor_unit_name,
  nano_unit_code,
  nano_unit_name,
  inferred_unit_level,
  case
    when coalesce(major_unit_code, '') !~ '^[1-9]$' then 'major_unit_code format'
    when coalesce(minor_unit_code, '') !~ '^[0-9]$' then 'minor_unit_code format'
    when coalesce(nano_unit_code, '') !~ '^[0-9]$' then 'nano_unit_code format'
    when coalesce(unit_code, '') !~ '^\d{3}$' then 'unit_code format'
    when coalesce(unit_code, '') <> coalesce(major_unit_code, '') || coalesce(minor_unit_code, '') || coalesce(nano_unit_code, '') then 'unit_code mismatch'
    when inferred_unit_level = 'major'
      and not (
        minor_unit_code = '0'
        and nano_unit_code = '0'
        and minor_unit_name is null
        and nano_unit_name is null
      ) then 'major-level naming/code mismatch'
    when inferred_unit_level = 'middle'
      and not (
        minor_unit_code <> '0'
        and nano_unit_code = '0'
        and minor_unit_name is not null
        and nano_unit_name is null
      ) then 'middle-level naming/code mismatch'
    when inferred_unit_level = 'nano'
      and not (
        minor_unit_code <> '0'
        and nano_unit_code <> '0'
        and minor_unit_name is not null
        and nano_unit_name is not null
      ) then 'nano-level naming/code mismatch'
    else null
  end as failure_reason
from prepared
where
  coalesce(major_unit_code, '') !~ '^[1-9]$'
  or coalesce(minor_unit_code, '') !~ '^[0-9]$'
  or coalesce(nano_unit_code, '') !~ '^[0-9]$'
  or coalesce(unit_code, '') !~ '^\d{3}$'
  or coalesce(unit_code, '') <> coalesce(major_unit_code, '') || coalesce(minor_unit_code, '') || coalesce(nano_unit_code, '')
  or (
    inferred_unit_level = 'major'
    and not (
      minor_unit_code = '0'
      and nano_unit_code = '0'
      and minor_unit_name is null
      and nano_unit_name is null
    )
  )
  or (
    inferred_unit_level = 'middle'
    and not (
      minor_unit_code <> '0'
      and nano_unit_code = '0'
      and minor_unit_name is not null
      and nano_unit_name is null
    )
  )
  or (
    inferred_unit_level = 'nano'
    and not (
      minor_unit_code <> '0'
      and nano_unit_code <> '0'
      and minor_unit_name is not null
      and nano_unit_name is not null
    )
  )
order by grade_level, curriculum_version, subject, unit_code;


-- =========================================================
-- 5. 요약 카운트
--    bad_row_count = 0 이면 새 계층 제약 기준으로는 통과 가능성이 높음
-- =========================================================
with prepared as (
  select
    cu.*,
    case
      when coalesce(nullif(btrim(cu.nano_unit_code), ''), '0') <> '0' then 'nano'
      when coalesce(nullif(btrim(cu.minor_unit_code), ''), '0') <> '0' then 'middle'
      else 'major'
    end as inferred_unit_level
  from auto_grading.curriculum_units cu
),
bad as (
  select 1
  from prepared
  where
    coalesce(major_unit_code, '') !~ '^[1-9]$'
    or coalesce(minor_unit_code, '') !~ '^[0-9]$'
    or coalesce(nano_unit_code, '') !~ '^[0-9]$'
    or coalesce(unit_code, '') !~ '^\d{3}$'
    or coalesce(unit_code, '') <> coalesce(major_unit_code, '') || coalesce(minor_unit_code, '') || coalesce(nano_unit_code, '')
    or (
      inferred_unit_level = 'major'
      and not (
        minor_unit_code = '0'
        and nano_unit_code = '0'
        and minor_unit_name is null
        and nano_unit_name is null
      )
    )
    or (
      inferred_unit_level = 'middle'
      and not (
        minor_unit_code <> '0'
        and nano_unit_code = '0'
        and minor_unit_name is not null
        and nano_unit_name is null
      )
    )
    or (
      inferred_unit_level = 'nano'
      and not (
        minor_unit_code <> '0'
        and nano_unit_code <> '0'
        and minor_unit_name is not null
        and nano_unit_name is not null
      )
    )
)
select
  (select count(*) from auto_grading.curriculum_units) as total_rows,
  count(*) as bad_row_count
from bad;

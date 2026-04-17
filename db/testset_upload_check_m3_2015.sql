-- 문제지 등록 테스트 전/후 점검용 SQL
-- 대상:
--   - grade_level = M3
--   - curriculum_version = 2015
--   - subject = 중3과학


-- =========================================================
-- 1. 등록 전: curriculum_units 기본 적재 확인
-- =========================================================
select
  grade_level,
  curriculum_version,
  subject,
  count(*) as row_count,
  count(*) filter (where unit_level = 'major') as major_rows,
  count(*) filter (where unit_level = 'middle') as middle_rows,
  count(*) filter (where unit_level = 'nano') as nano_rows
from auto_grading.curriculum_units
where grade_level = 'M3'
  and curriculum_version = '2015'
  and subject = '중3과학'
group by grade_level, curriculum_version, subject;


-- =========================================================
-- 2. 등록 전: 대표 코드 존재 여부 확인
--    필요하면 unit_code 값을 더 추가해서 확인
-- =========================================================
select
  unit_code,
  unit_level,
  major_unit_name,
  minor_unit_name,
  nano_unit_name,
  is_active
from auto_grading.curriculum_units
where grade_level = 'M3'
  and curriculum_version = '2015'
  and subject = '중3과학'
  and unit_code in ('100', '110', '111', '112', '120', '121', '500', '520', '521')
order by unit_code;


-- =========================================================
-- 3. 등록 후: 최근 등록된 test_sets 확인
--    문제지 등록 직후 실행해서 마지막 등록 결과를 확인
-- =========================================================
select
  ts.id as test_set_id,
  ts.title,
  ts.grade_level,
  ts.curriculum_version,
  ts.subject,
  ts.unit_code,
  ts.source_type,
  ts.source_category,
  ts.created_at
from auto_grading.test_sets ts
where ts.grade_level = 'M3'
  and ts.curriculum_version = '2015'
  and ts.subject = '중3과학'
order by ts.created_at desc nulls last, ts.id desc
limit 20;


-- =========================================================
-- 4. 등록 후: test_sets -> curriculum_units 조인 확인
--    unit_code가 기대한 단원과 정확히 연결됐는지 검증
-- =========================================================
select
  ts.id as test_set_id,
  ts.title,
  ts.unit_code,
  cu.unit_level,
  cu.major_unit_name,
  cu.minor_unit_name,
  cu.nano_unit_name,
  ts.created_at
from auto_grading.test_sets ts
join auto_grading.curriculum_units cu
  on cu.grade_level = ts.grade_level
 and cu.curriculum_version = ts.curriculum_version
 and cu.subject = ts.subject
 and cu.unit_code = ts.unit_code
where ts.grade_level = 'M3'
  and ts.curriculum_version = '2015'
  and ts.subject = '중3과학'
order by ts.created_at desc nulls last, ts.id desc
limit 20;


-- =========================================================
-- 5. 등록 후: test_items 개수까지 함께 확인
--    각 문제지가 몇 문항으로 저장됐는지 점검
-- =========================================================
select
  ts.id as test_set_id,
  ts.title,
  ts.unit_code,
  count(ti.id) as item_count,
  min(ti.item_no) as min_item_no,
  max(ti.item_no) as max_item_no,
  ts.created_at
from auto_grading.test_sets ts
left join auto_grading.test_items ti
  on ti.test_set_id = ts.id
where ts.grade_level = 'M3'
  and ts.curriculum_version = '2015'
  and ts.subject = '중3과학'
group by ts.id, ts.title, ts.unit_code, ts.created_at
order by ts.created_at desc nulls last, ts.id desc
limit 20;


-- =========================================================
-- 6. 필요 시: 특정 문제지 제목으로 좁혀서 확인
--    아래 '족보' 부분을 원하는 제목 일부로 바꿔 사용
-- =========================================================
select
  ts.id as test_set_id,
  ts.title,
  ts.unit_code,
  cu.unit_level,
  cu.major_unit_name,
  cu.minor_unit_name,
  cu.nano_unit_name,
  count(ti.id) as item_count,
  ts.created_at
from auto_grading.test_sets ts
join auto_grading.curriculum_units cu
  on cu.grade_level = ts.grade_level
 and cu.curriculum_version = ts.curriculum_version
 and cu.subject = ts.subject
 and cu.unit_code = ts.unit_code
left join auto_grading.test_items ti
  on ti.test_set_id = ts.id
where ts.grade_level = 'M3'
  and ts.curriculum_version = '2015'
  and ts.subject = '중3과학'
  and ts.title ilike '%족보%'
group by
  ts.id,
  ts.title,
  ts.unit_code,
  cu.unit_level,
  cu.major_unit_name,
  cu.minor_unit_name,
  cu.nano_unit_name,
  ts.created_at
order by ts.created_at desc nulls last, ts.id desc;

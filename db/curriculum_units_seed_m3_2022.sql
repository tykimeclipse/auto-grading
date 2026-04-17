begin;

-- =========================================================
-- curriculum_units seed
-- 대상:
--   - grade_level: M3
--   - curriculum_version: 2022
--   - subject: 중3과학
--
-- 특징:
--   - upsert_curriculum_unit_hierarchy(...) 기반이라 재실행 가능
--   - 소단원 1개를 넣으면 해당 대단원/중단원 row도 함께 보장됨
--   - 예: 111 입력 시 100, 110, 111 row가 정리됨
-- =========================================================

-- ---------------------------------------------------------
-- 대단원 1. 화학반응의 규칙과 에너지
-- ---------------------------------------------------------

-- 111 / 112
select auto_grading.upsert_curriculum_unit_hierarchy(
  p_grade_level := 'M3',
  p_curriculum_version := '2022',
  p_subject := '중3과학',
  p_major_unit_code := '1',
  p_major_unit_name := '화학반응의 규칙과 에너지',
  p_minor_unit_code := '1',
  p_minor_unit_name := '물질변화와 화학반응식',
  p_nano_unit_code := '1',
  p_nano_unit_name := '물리변화와 화학변화'
);

select auto_grading.upsert_curriculum_unit_hierarchy(
  p_grade_level := 'M3',
  p_curriculum_version := '2022',
  p_subject := '중3과학',
  p_major_unit_code := '1',
  p_major_unit_name := '화학반응의 규칙과 에너지',
  p_minor_unit_code := '1',
  p_minor_unit_name := '물질변화와 화학반응식',
  p_nano_unit_code := '2',
  p_nano_unit_name := '화학반응과 화학반응식'
);

-- 121 / 122
select auto_grading.upsert_curriculum_unit_hierarchy(
  p_grade_level := 'M3',
  p_curriculum_version := '2022',
  p_subject := '중3과학',
  p_major_unit_code := '1',
  p_major_unit_name := '화학반응의 규칙과 에너지',
  p_minor_unit_code := '2',
  p_minor_unit_name := '화학반응의 규칙',
  p_nano_unit_code := '1',
  p_nano_unit_name := '질량보존 법칙'
);

select auto_grading.upsert_curriculum_unit_hierarchy(
  p_grade_level := 'M3',
  p_curriculum_version := '2022',
  p_subject := '중3과학',
  p_major_unit_code := '1',
  p_major_unit_name := '화학반응의 규칙과 에너지',
  p_minor_unit_code := '2',
  p_minor_unit_name := '화학반응의 규칙',
  p_nano_unit_code := '2',
  p_nano_unit_name := '일정성분비 법칙'
);


-- ---------------------------------------------------------
-- 대단원 2. 여러 가지 화학반응
-- 필요 시 아래 블록을 복사해서 계속 추가
-- ---------------------------------------------------------

select auto_grading.upsert_curriculum_unit_hierarchy(
  p_grade_level := 'M3',
  p_curriculum_version := '2022',
  p_subject := '중3과학',
  p_major_unit_code := '2',
  p_major_unit_name := '여러 가지 화학반응',
  p_minor_unit_code := '1',
  p_minor_unit_name := '산과 염기',
  p_nano_unit_code := '1',
  p_nano_unit_name := '산과 염기의 성질'
);

select auto_grading.upsert_curriculum_unit_hierarchy(
  p_grade_level := 'M3',
  p_curriculum_version := '2022',
  p_subject := '중3과학',
  p_major_unit_code := '2',
  p_major_unit_name := '여러 가지 화학반응',
  p_minor_unit_code := '1',
  p_minor_unit_name := '산과 염기',
  p_nano_unit_code := '2',
  p_nano_unit_name := '중화 반응'
);

select auto_grading.upsert_curriculum_unit_hierarchy(
  p_grade_level := 'M3',
  p_curriculum_version := '2022',
  p_subject := '중3과학',
  p_major_unit_code := '2',
  p_major_unit_name := '여러 가지 화학반응',
  p_minor_unit_code := '2',
  p_minor_unit_name := '산화와 환원',
  p_nano_unit_code := '1',
  p_nano_unit_name := '산화와 환원 반응'
);


-- =========================================================
-- 결과 확인
-- =========================================================
select
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
from auto_grading.curriculum_units
where grade_level = 'M3'
  and curriculum_version = '2022'
  and subject = '중3과학'
order by unit_code;

commit;

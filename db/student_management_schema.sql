-- ================================================================
-- student_management_schema.sql
-- students 테이블 컬럼 보강
--
-- 기존 student_table_expand.sql 적용 여부와 무관하게
-- ADD COLUMN IF NOT EXISTS로 멱등성 보장
-- ================================================================

alter table auto_grading.students
  add column if not exists gender text;

alter table auto_grading.students
  add column if not exists student_phone text;

-- address : 거주지역 (상세 주소가 아닌 지역명 수준, 예: 상계동)
alter table auto_grading.students
  add column if not exists address text;

comment on column auto_grading.students.gender
  is '성별 (남 / 여 / 기타/미입력)';
comment on column auto_grading.students.student_phone
  is '학생 전화번호';
comment on column auto_grading.students.address
  is '거주지역 (예: 상계동, 중계동). student-management 페이지에서 관리.';

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

-- address : 도로명/지번 기본 주소 (주소 검색으로 선택)
alter table auto_grading.students
  add column if not exists address text;

-- address_detail : 상세 주소 (아파트 동·호수 등, 직접 입력, 선택)
alter table auto_grading.students
  add column if not exists address_detail text;

comment on column auto_grading.students.gender
  is '성별 (남 / 여 / 기타/미입력)';
comment on column auto_grading.students.student_phone
  is '학생 전화번호';
comment on column auto_grading.students.address
  is '기본 주소. 도로명/지번 주소 검색으로 자동 입력. 예: 경기 성남시 분당구 판교역로 235';
comment on column auto_grading.students.address_detail
  is '상세 주소. 직접 입력. 예: ○○아파트 101동 1203호. 과외 학생에게 주로 사용. null 허용.';

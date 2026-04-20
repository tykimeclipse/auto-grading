-- ================================================================
-- assignments 테이블에 reset_token 컬럼 추가
--
-- 목적: teacher_reset_attempt_* RPC 호출 시 assignment_id 단독이 아닌
--       assignment_id + reset_token 조합을 요구해 무단 리셋 방지
--
-- 특성:
--   · UUID v4 랜덤값이므로 assignment_id를 알아도 추측 불가
--   · 학생용 RPC(start_attempt 등)에는 노출되지 않음
--   · 기존 행은 gen_random_uuid()로 자동 채워짐 (backfill 불필요)
-- ================================================================

alter table auto_grading.assignments
  add column if not exists reset_token uuid not null default gen_random_uuid();

comment on column auto_grading.assignments.reset_token
is '교사용 입력초기화 RPC 호출 시 2차 인증에 사용하는 랜덤 토큰. 학생용 RPC에는 노출하지 않는다.';

-- ============================================================================
-- audit_course_enrollment_achievement_readiness_trigger_guard.sql
--
-- 1단계-E: 배포된 assignment 자동 닫힘 트리거 함수 본문 최종 확인.
--
-- 운영 DB의 함수 해시가 저장소 정의와 달라 실제 본문을 확인하기 위한 쿼리다.
-- pg_catalog의 함수 1개만 조회하며 운영 데이터는 전혀 읽지 않는다.
-- SELECT만 수행하며 데이터와 스키마를 변경하지 않는다.
-- 결과 1행 전체를 공유한다.
-- ============================================================================

select
  n.nspname as schema_name,
  p.proname as function_name,
  md5(p.prosrc) as source_md5,
  length(p.prosrc) as source_length,
  p.prosrc ~* 'v_current_is_auto_managed' as has_auto_managed_reason_list,
  (
    p.prosrc ~* 'v_current_closed_at\s+is\s+not\s+null'
    and p.prosrc ~* 'not\s+v_current_is_auto_managed'
  ) as preserves_non_auto_managed_close,
  p.prosrc ~* 'auto_completed_round1' as handles_auto_completed_round1,
  p.prosrc ~* 'auto_completed_round2' as handles_auto_completed_round2,
  p.prosrc ~* 'teacher_review_completed' as handles_teacher_review_completed,
  p.prosrc ~* 'course_closed' as explicitly_references_course_closed,
  pg_get_functiondef(p.oid) as deployed_function_definition
from pg_proc p
join pg_namespace n on n.oid = p.pronamespace
where n.nspname = 'auto_grading'
  and p.proname = 'trg_attempts_sync_assignment_close_after'
  and pg_get_function_identity_arguments(p.oid) = '';

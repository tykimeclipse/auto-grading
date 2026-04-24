-- ================================================================
-- revoke_anon_from_teacher_rpcs.sql
-- 교사/관리자 전용 RPC에서 anon 역할 제거
--
-- 배경:
--   admin.html + Supabase Auth 도입으로 관리자 페이지는
--   authenticated 세션이 보장됨. anon 접근은 불필요하며
--   보안상 제거해야 함.
--
-- 제외 대상 (anon 유지):
--   start_attempt_by_test_set — 학생용, 비로그인 호출 허용
-- ================================================================

-- ── student_management_rpcs ───────────────────────────────────
revoke execute
  on function auto_grading.teacher_list_students_for_management()
  from anon;

revoke execute
  on function auto_grading.teacher_get_student_detail(uuid)
  from anon;

revoke execute
  on function auto_grading.teacher_update_student_metadata(uuid, text, text, text, text, text, text)
  from anon;

revoke execute
  on function auto_grading.teacher_set_student_active_state(uuid, boolean)
  from anon;

revoke execute
  on function auto_grading.teacher_delete_student_safely(uuid)
  from anon;

-- ── teacher_finalize_attempt ──────────────────────────────────
revoke execute
  on function auto_grading.teacher_finalize_attempt(uuid, uuid)
  from anon;

-- ── teacher_reset_attempt ─────────────────────────────────────
-- 기존에 anon만 있었으므로 revoke 후 authenticated, service_role 재부여
revoke execute
  on function auto_grading.teacher_reset_attempt_round2(uuid, uuid)
  from anon;

revoke execute
  on function auto_grading.teacher_reset_attempt_full(uuid, uuid)
  from anon;

grant execute
  on function auto_grading.teacher_reset_attempt_round2(uuid, uuid)
  to authenticated, service_role;

grant execute
  on function auto_grading.teacher_reset_attempt_full(uuid, uuid)
  to authenticated, service_role;

-- ── teacher_list_assignments ──────────────────────────────────
revoke execute
  on function auto_grading.teacher_list_assignments(
    uuid, uuid, uuid, boolean, text, text, text, integer, integer
  )
  from anon;

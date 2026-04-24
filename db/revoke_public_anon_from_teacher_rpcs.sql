-- ================================================================
-- revoke_public_anon_from_teacher_rpcs.sql
-- auto_grading 스키마의 teacher_* 함수 전체에서
-- PUBLIC 및 anon 실행 권한 제거
--
-- 배경:
--   PostgreSQL은 함수 생성 시 PUBLIC에 EXECUTE를 기본 부여함.
--   REVOKE FROM anon만 해도 PUBLIC이 남으면 anon은 여전히 실행 가능.
--   → PUBLIC + anon 모두 명시적으로 REVOKE 해야 완전히 차단됨.
--
-- DO 블록: auto_grading.teacher_* 전체를 자동 처리
--   - 시그니처를 일일이 쓰지 않아도 됨
--   - 향후 추가된 teacher_* 함수는 별도 처리 필요
--
-- 제외 (영향 없음):
--   public.start_attempt_by_test_set  — 학생용, 별도 스키마
--   auto_grading.get_student_*         — teacher_ 접두어 아님
--   auto_grading.submit_round*         — teacher_ 접두어 아님
-- ================================================================

-- ── Step 1: teacher_* 전체에서 PUBLIC · anon REVOKE ──────────────
DO $$
DECLARE
  r RECORD;
BEGIN
  FOR r IN
    SELECT p.oid::regprocedure AS func_sig
    FROM   pg_proc p
    JOIN   pg_namespace n ON n.oid = p.pronamespace
    WHERE  n.nspname = 'auto_grading'
      AND  p.proname LIKE 'teacher_%'
  LOOP
    -- PUBLIC 제거: 사실상 모든 역할의 기본 실행 권한 차단
    EXECUTE format('REVOKE EXECUTE ON FUNCTION %s FROM PUBLIC', r.func_sig);
    -- anon 명시적 제거: 이전에 명시적으로 grant된 경우 대비
    EXECUTE format('REVOKE EXECUTE ON FUNCTION %s FROM anon',   r.func_sig);
  END LOOP;
END $$;


-- ── Step 2: authenticated · service_role 명시적 재부여 ───────────
-- DO 블록은 PUBLIC·anon만 revoke하므로 authenticated는 유지되지만,
-- 소스 파일 재실행 시 누락을 방지하기 위해 명시적으로 재부여.

-- student_management_rpcs
grant execute on function auto_grading.teacher_list_students_for_management()
  to authenticated, service_role;
grant execute on function auto_grading.teacher_get_student_detail(uuid)
  to authenticated, service_role;
grant execute on function auto_grading.teacher_update_student_metadata(uuid, text, text, text, text, text, text)
  to authenticated, service_role;
grant execute on function auto_grading.teacher_set_student_active_state(uuid, boolean)
  to authenticated, service_role;
grant execute on function auto_grading.teacher_delete_student_safely(uuid)
  to authenticated, service_role;

-- teacher_finalize_attempt
grant execute on function auto_grading.teacher_finalize_attempt(uuid, uuid)
  to authenticated, service_role;

-- teacher_reset_attempt
grant execute on function auto_grading.teacher_reset_attempt_round2(uuid, uuid)
  to authenticated, service_role;
grant execute on function auto_grading.teacher_reset_attempt_full(uuid, uuid)
  to authenticated, service_role;

-- teacher_list_assignments (v2 시그니처 — 현재 운용 버전)
grant execute on function auto_grading.teacher_list_assignments(
  uuid, uuid, uuid, boolean, text, text, text, integer, integer
) to authenticated, service_role;

-- teacher_save_final_score, teacher_set_assignment_open_state
grant execute on function auto_grading.teacher_save_final_score(uuid, integer, text, boolean)
  to authenticated, service_role;
grant execute on function auto_grading.teacher_set_assignment_open_state(uuid, boolean, text)
  to authenticated, service_role;

-- teacher_issue_assignments
grant execute on function auto_grading.teacher_issue_assignments(
  uuid, uuid[], uuid, text, boolean
) to authenticated, service_role;

-- teacher_list_courses, teacher_attach_student_to_course
grant execute on function auto_grading.teacher_list_courses(text, boolean, integer, integer)
  to authenticated, service_role;
grant execute on function auto_grading.teacher_attach_student_to_course(uuid, uuid, text)
  to authenticated, service_role;

-- teacher_search_test_sets, teacher_get_test_set_overview
grant execute on function auto_grading.teacher_search_test_sets(text, text, text, text, boolean, integer, integer)
  to authenticated, service_role;
grant execute on function auto_grading.teacher_get_test_set_overview(uuid, text)
  to authenticated, service_role;

-- teacher_list_students_for_assignment (v2 시그니처)
grant execute on function auto_grading.teacher_list_students_for_assignment(
  uuid, uuid, text, text, text, text, boolean, boolean, integer, integer
) to authenticated, service_role;

-- teacher_get_next_course_no, teacher_create_course, teacher_list_course_catalog
grant execute on function auto_grading.teacher_get_next_course_no()
  to authenticated, service_role;
grant execute on function auto_grading.teacher_create_course(text, integer, date, date, text, text)
  to authenticated, service_role;
grant execute on function auto_grading.teacher_list_course_catalog(text, boolean, integer, integer)
  to authenticated, service_role;

-- teacher_deactivate_student_course
grant execute on function auto_grading.teacher_deactivate_student_course(uuid, uuid)
  to authenticated, service_role;

-- teacher_delete_courses: 소스 파일 미존재(대시보드 생성 추정), authenticated는 이미 부여됨
-- → DO 블록이 PUBLIC·anon revoke 처리, 별도 grant 불필요


-- ── Step 3: 검증 쿼리 (실행 후 여기에 붙여서 확인) ──────────────
-- 아래 결과에 grantee = 'anon' 또는 'PUBLIC' 행이 없어야 정상.
--
-- select routine_name, grantee, privilege_type
-- from   information_schema.routine_privileges
-- where  routine_schema = 'auto_grading'
--   and  routine_name   like 'teacher_%'
--   and  grantee        in ('anon', 'PUBLIC')
-- order  by routine_name, grantee;

-- ================================================================
-- mistake_notes_omr_bridge_grant_dev.sql
--
-- ╔══════════════════════════════════════════════════════════════╗
-- ║ ⚠️ 로컬 / 테스트 환경 전용 — 운영 DB 에 절대 적용 금지         ║
-- ╚══════════════════════════════════════════════════════════════╝
--
-- create_mistake_note_via_omr_bridge 는 student_public_links 의 범용
-- 공개 토큰을 반환하는 과도기 브리지다 (db/mistake_notes_omr_bridge.sql
-- 상단 보안 게이트 주석 참조). 그래서 본체 파일은 anon 에 grant 하지
-- 않는다. 이 파일은 *로컬/테스트* 에서 OMR 흐름을 검증할 때만 anon 에
-- 실행 권한을 부여한다.
--
-- 운영/외부 배포 전 필수 보안 게이트 (db/mistake_notes_omr_bridge.sql
-- 헤더 참조) 를 모두 통과하기 전에는 이 파일을 운영 DB 에서 실행하지
-- 않는다. 통과 후에는 OMR bridge 자체를 note-scoped token 방식으로
-- 대체하고 이 파일을 삭제한다.
--
-- ── 운영 DB 일괄 실행 사고 방지 가드 ──────────────────────────────
-- 배포 스크립트가 db/*.sql 을 일괄 실행하더라도, 이 파일은
-- auto_grading._dev_grant_marker 가 존재할 때만 동작한다.
-- 로컬/테스트 DB 에서 *최초 1회만* 아래를 수동 실행:
--   create table if not exists auto_grading._dev_grant_marker();
-- 운영 DB 에는 이 marker 를 만들지 않으므로, 배포 스크립트가 실수로
-- 이 파일을 실행해도 아래 가드에서 abort 되어 grant 가 적용되지 않는다.
-- ================================================================

do $$
begin
  -- DEV 환경 marker 확인 — 운영 DB 일괄 실행 사고 방지
  if to_regclass('auto_grading._dev_grant_marker') is null then
    raise exception 'DEV 전용 grant 파일입니다. 운영 DB 실행 금지. 로컬/테스트라면 먼저 "create table auto_grading._dev_grant_marker();" 를 실행하세요.';
  end if;
  -- 의존 함수 존재 확인
  if to_regprocedure('auto_grading.create_mistake_note_via_omr_bridge(uuid, text, uuid)') is null then
    raise exception 'create_mistake_note_via_omr_bridge 가 없습니다. db/mistake_notes_omr_bridge.sql 먼저 실행하세요.';
  end if;
end $$;

grant execute on function auto_grading.create_mistake_note_via_omr_bridge(uuid, text, uuid)
  to anon, authenticated;

notify pgrst, 'reload schema';

-- 적용 확인:
-- select grantee, privilege_type
--   from information_schema.routine_privileges
--  where routine_schema = 'auto_grading'
--    and routine_name   = 'create_mistake_note_via_omr_bridge';

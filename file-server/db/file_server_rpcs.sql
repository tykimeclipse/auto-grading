-- ================================================================
-- file-server 전용 RPC
--
-- 파일서버가 anon 키로 호출하는 함수들.
-- service_role 키는 파일서버에 두지 않는다 (Phase 0 B안 정책).
--
-- 함수:
--   get_student_id_by_token(uuid) returns uuid
--     · 파일서버가 업로드 시 token → student_id 변환에 사용.
--     · 학생 정보는 UUID 1 개만 노출 (이름/학년 등 미노출).
--     · 유효하지 않은 token 은 null 반환 (raise 금지).
-- ================================================================


-- ── Step 0: 전제 조건 ────────────────────────────────────────────
do $$
begin
  if to_regprocedure('auto_grading._resolve_student_id_by_token(uuid)') is null then
    raise exception '_resolve_student_id_by_token 가 없습니다. db/mistake_notes_rpc.sql 먼저 실행하세요.';
  end if;
end $$;


-- ── Step 1: get_student_id_by_token ──────────────────────────────
drop function if exists auto_grading.get_student_id_by_token(uuid);

create or replace function auto_grading.get_student_id_by_token(
  p_token uuid
)
returns uuid
language sql
stable
security definer
set search_path to 'auto_grading', 'public'
as $$
  select auto_grading._resolve_student_id_by_token(p_token);
$$;

grant execute on function auto_grading.get_student_id_by_token(uuid)
  to anon, authenticated, service_role;

comment on function auto_grading.get_student_id_by_token(uuid)
  is '파일서버용. 토큰으로 student_id 만 반환. 유효하지 않으면 null. 다른 학생 정보 미노출.';


-- ── Step 2: PostgREST 스키마 캐시 reload ─────────────────────────
notify pgrst, 'reload schema';


-- ── 검증 ─────────────────────────────────────────────────────────
-- 1) 유효한 token 으로 호출 → student_id 반환
-- 2) 잘못된 token 으로 호출 → null 반환 (에러 아님)
-- 3) PostgREST 노출 확인:
--    curl -X POST <SUPABASE_URL>/rest/v1/rpc/get_student_id_by_token \
--      -H "apikey: <ANON_KEY>" -H "Content-Type: application/json" \
--      -H "Accept-Profile: auto_grading" \
--      -d '{"p_token":"<UUID>"}'

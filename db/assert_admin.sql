-- ================================================================
-- assert_admin.sql
-- 관리자 이메일 검증 공통 함수
--
-- 사용법: 각 teacher_* RPC 함수 본문 맨 첫 줄에
--   PERFORM auto_grading.assert_admin();
--
-- 동작:
--   auth.email()이 ALLOWED 목록에 없으면 insufficient_privilege 예외 발생
--   → 호출한 RPC의 exception 핸들러에서 {ok:false, error:'...'} 반환
--
-- 관리자 이메일 추가 시 이 파일만 수정 후 재실행하면 됨
-- ================================================================

drop function if exists auto_grading.assert_admin();

create or replace function auto_grading.assert_admin()
returns void
language plpgsql
security definer
set search_path to 'auto_grading', 'public'
as $$
declare
  -- auth.jwt() ->> 'email' 사용: SECURITY DEFINER 환경에서도 안정적으로 호출자 이메일 확인
  -- lower()로 정규화하여 대소문자 차이 방지
  v_email text := lower(coalesce(auth.jwt() ->> 'email', ''));
begin
  if v_email not in (
    'tykimeclipse@gmail.com'
    -- 추가 관리자 이메일은 여기에 쉼표로 구분하여 추가 (소문자로 기재)
    -- 'another_admin@example.com'
  ) then
    raise exception '관리자 권한이 없습니다. (email: %)', coalesce(nullif(v_email, ''), '(null)')
      using errcode = 'insufficient_privilege';
  end if;
end;
$$;

-- assert_admin 자체는 authenticated만 호출 가능 (anon 차단)
-- PUBLIC도 명시적으로 제거
revoke execute on function auto_grading.assert_admin() from public;
revoke execute on function auto_grading.assert_admin() from anon;
grant  execute on function auto_grading.assert_admin() to authenticated, service_role;

comment on function auto_grading.assert_admin()
  is '관리자 이메일 검증. auth.jwt() ->> ''email'' 을 변수에 담아 허용 목록과 비교. 불일치 시 insufficient_privilege 예외 발생. 각 teacher_* RPC 첫 줄에 PERFORM auto_grading.assert_admin(); 으로 호출.';

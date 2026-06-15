-- ================================================================
-- teacher_get_test_set_detail.sql
-- 선택된 시험의 기본 메타(과목/단원코드/출처/문항수 등) 단건 조회 RPC
--
-- 배경: test_sets 는 RLS 가 켜져 있고 authenticated 에 SELECT 정책이 없어
--       프론트의 직접 .from('test_sets').select() 가 빈 결과를 반환한다.
--       (teacher_search_test_sets / teacher_get_test_set_overview 는
--        subject/unit_code 를 반환하지 않아 이 RPC 로 보완)
--
-- security definer + assert_admin: 관리자만 호출 가능하게 게이트하고
--       함수는 소유자 권한으로 돌아 RLS 를 우회한다.
-- ================================================================

create or replace function auto_grading.teacher_get_test_set_detail(
  p_test_set_id uuid
)
returns table(
  id uuid,
  subject text,
  unit_code text,
  source_category text,
  source_name text,
  source_type text,
  total_items integer,
  updated_at timestamptz,
  created_at timestamptz
)
language plpgsql
security definer
set search_path to 'auto_grading', 'public'
as $function$
begin
  perform auto_grading.assert_admin();

  return query
  select
    ts.id,
    ts.subject,
    ts.unit_code,
    ts.source_category,
    ts.source_name,
    ts.source_type,
    ts.total_items,
    ts.updated_at,
    ts.created_at
  from auto_grading.test_sets ts
  where ts.id = p_test_set_id;
end;
$function$;

-- 관리자 전용 읽기 RPC: anon/public 차단, authenticated 만 호출 가능
-- (실제 관리자 검증은 함수 본문 첫 줄 assert_admin() 에서 수행)
revoke execute on function auto_grading.teacher_get_test_set_detail(uuid) from public, anon;
grant  execute on function auto_grading.teacher_get_test_set_detail(uuid) to authenticated;

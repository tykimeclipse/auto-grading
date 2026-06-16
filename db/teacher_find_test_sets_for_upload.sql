-- ================================================================
-- teacher_find_test_sets_for_upload.sql
-- 문제지 업로드 페이지(testset-upload-v3)의 "검색 모드" 전용 조회 RPC
--
-- 배경: test_sets / test_items 는 RLS 가 켜져 있고 정책이 0건이라
--       프론트의 직접 .from('test_sets').select(... test_items(count)) 가
--       항상 빈 결과를 반환한다(검색 모드가 "결과 없음"으로만 동작).
--
-- 두 가지 모드:
--   1) p_test_set_id 가 주어지면 그 ID 단건만 조회 (다른 조건 무시)
--   2) 아니면 curriculum_version/grade_level/subject/unit_code/source_category
--      정확 일치 + (선택) 제목 부분일치로 검색
--
-- security definer + assert_admin: 관리자만 호출, 함수는 소유자 권한으로
--       돌아 RLS 를 우회한다.
-- ================================================================

create or replace function auto_grading.teacher_find_test_sets_for_upload(
  p_test_set_id uuid default null,
  p_curriculum_version text default null,
  p_grade_level text default null,
  p_subject text default null,
  p_unit_code text default null,
  p_source_category text default null,
  p_title text default null
)
returns table(
  id uuid,
  title text,
  curriculum_version text,
  grade_level text,
  subject text,
  unit_code text,
  source_type text,
  source_category text,
  source_name text,
  item_count integer
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
    ts.title,
    ts.curriculum_version,
    ts.grade_level,
    ts.subject,
    ts.unit_code,
    ts.source_type,
    ts.source_category,
    ts.source_name,
    (
      select count(*)::integer
      from auto_grading.test_items ti
      where ti.test_set_id = ts.id
    ) as item_count
  from auto_grading.test_sets ts
  where
    case
      when p_test_set_id is not null then ts.id = p_test_set_id
      else
        ts.curriculum_version = p_curriculum_version
        and ts.grade_level     = p_grade_level
        and ts.subject         = p_subject
        and ts.unit_code       = p_unit_code
        and ts.source_category = p_source_category
        and (
          nullif(btrim(p_title), '') is null
          or ts.title ilike '%' || btrim(p_title) || '%'
        )
    end
  order by ts.created_at desc;
end;
$function$;

-- 관리자 전용 검색 RPC: anon/public 차단, authenticated 만 호출 가능
-- (실제 관리자 검증은 함수 본문 첫 줄 assert_admin() 에서 수행)
revoke execute on function auto_grading.teacher_find_test_sets_for_upload(
  uuid, text, text, text, text, text, text
) from public, anon;

grant execute on function auto_grading.teacher_find_test_sets_for_upload(
  uuid, text, text, text, text, text, text
) to authenticated;

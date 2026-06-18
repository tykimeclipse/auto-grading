-- ================================================================
-- student_stats_history_by_token.sql
-- 학생 성취도 공개 페이지(student-achievement-v2)용 토큰 기반 조회 RPC 2종
--
-- 배경: 성취도 페이지는 공개 토큰으로 stats/history 를 조회하도록 설계됐는데
--       (get_student_*_by_token), 실제로는 _by_code 버전만 존재하고
--       _by_token 버전이 만들어진 적이 없어 호출이 PGRST202(함수 없음)로
--       실패 → 페이지에 "조회 중 오류가 발생했습니다." 가 떴다.
--
-- 설계: get_student_by_public_token 과 동일한 토큰 검증(active + 만료 + 학생활성)
--       으로 token → student_code 를 해석한 뒤, 기존 _by_code 함수에 위임한다.
--       security definer + anon grant (로그인 없는 공개 페이지에서 호출).
-- ================================================================

-- 과거 대시보드에서 직접 만들어진 구버전이 남아 있을 수 있다(반환 타입 불일치 →
-- create or replace 가 42P13 으로 거부됨). 재실행 가능하도록 먼저 정리한다.
drop function if exists auto_grading.get_student_stats_by_token(uuid);
drop function if exists auto_grading.get_student_assignment_history_by_token(uuid, integer);
-- 과거 단일 인자 변형(p_limit 없음)이 남아 있으면 overload/캐시 혼선이 나므로 함께 정리
drop function if exists auto_grading.get_student_assignment_history_by_token(uuid);

-- 1) 통계: token → student_code → get_student_stats_by_code
create or replace function auto_grading.get_student_stats_by_token(
  p_token uuid
)
returns jsonb
language plpgsql
security definer
set search_path to 'auto_grading', 'public'
as $function$
declare
  v_code text;
begin
  select s.student_code
    into v_code
  from auto_grading.student_public_links l
  join auto_grading.students s on s.id = l.student_id
  where l.public_token = p_token
    and l.is_active = true
    and (l.expires_at is null or l.expires_at > now())
    and s.is_active = true;

  if v_code is null then
    return null;  -- 유효하지 않은/만료된 토큰
  end if;

  return auto_grading.get_student_stats_by_code(v_code);
end;
$function$;

-- 2) 응시 이력: token → student_code → get_student_assignment_history_by_code
--    배포된 _by_code 의 컬럼 구성이 repo 와 다를 수 있어(드리프트), returns table 로
--    컬럼을 고정하면 구조 불일치(42804)로 400 이 난다. jsonb 로 통째로 통과시켜
--    배포본 컬럼이 무엇이든 안전하게 전달한다. (프론트는 배열로 받아 그대로 렌더)
create or replace function auto_grading.get_student_assignment_history_by_token(
  p_token uuid,
  p_limit integer default 200
)
returns jsonb
language plpgsql
security definer
set search_path to 'auto_grading', 'public'
as $function$
declare
  v_code text;
  v_result jsonb;
begin
  select s.student_code
    into v_code
  from auto_grading.student_public_links l
  join auto_grading.students s on s.id = l.student_id
  where l.public_token = p_token
    and l.is_active = true
    and (l.expires_at is null or l.expires_at > now())
    and s.is_active = true;

  if v_code is null then
    return '[]'::jsonb;  -- 유효하지 않은/만료된 토큰 → 빈 배열
  end if;

  select coalesce(jsonb_agg(to_jsonb(t)), '[]'::jsonb)
    into v_result
  from auto_grading.get_student_assignment_history_by_code(v_code, p_limit) t;

  return v_result;
end;
$function$;

-- 공개 페이지(anon)에서 호출. get_student_by_public_token 과 동일한 grant 패턴.
grant execute on function auto_grading.get_student_stats_by_token(uuid)
  to anon, authenticated, service_role;

grant execute on function auto_grading.get_student_assignment_history_by_token(uuid, integer)
  to anon, authenticated, service_role;

-- =========================================================
-- start_attempt_by_test_set.sql
-- 목적:
--   학생이 test_set_id 기반 URL로 진입했을 때
--   student_code를 받아 assignment를 자동 생성/재사용하고,
--   기존 auto_grading.start_attempt(...)를 호출해
--   OMR 입력을 시작하게 한다.
--
-- 전제:
--   - students.student_code 는 실제 학생 식별에 사용됨
--   - assignments 는 학생-시험 배정 레코드
--   - 같은 학생-같은 시험에 대해 활성 assignment는 1개만 허용
--
-- 배포 전 체크:
--   1) 활성 assignment 중복 여부 확인
--   2) assigned_by 기존 값 패턴 확인
--   3) 기존 public.start_attempt wrapper의 보안 속성 확인
-- =========================================================


-- =========================================================
-- 0. 사전 점검 1: 활성 assignment 중복 여부 확인
--    결과가 0행이어야 partial unique index 생성 가능
-- =========================================================
select
  student_id,
  test_set_id,
  count(*) as active_count
from auto_grading.assignments
where is_active = true
group by student_id, test_set_id
having count(*) > 1;


-- =========================================================
-- 0-1. 사전 점검 2: assigned_by 기존 값 패턴 확인
--     현재는 text 컬럼이므로 'self_service' 사용 가능
--     단, 기존 값들이 관리자 UUID/이메일 등이라면
--     장기적으로 assignment_source 분리 검토 가능
-- =========================================================
select distinct assigned_by
from auto_grading.assignments
limit 20;


-- =========================================================
-- 1. 활성 assignment만 유니크 보장하는 partial unique index
--    같은 학생-같은 시험에 대해 활성 assignment는 1개만 허용
--    비활성 assignment는 과거 이력으로 남길 수 있음
-- =========================================================
create unique index if not exists uq_assignments_student_testset_active
on auto_grading.assignments (student_id, test_set_id)
where is_active = true;


-- =========================================================
-- 2. 시험 시작 시 assignment 자동 생성/재사용 RPC
--    입력: test_set_id + student_code
--    출력: 기존 start_attempt와 동일한 jsonb
-- =========================================================
create or replace function auto_grading.start_attempt_by_test_set(
  p_test_set_id uuid,
  p_student_code text
)
returns jsonb
language plpgsql
security definer
as $$
declare
  v_student_id uuid;
  v_assignment_id uuid;
  v_test_set_exists boolean;
begin
  -- -------------------------------------------------------
  -- 2-1. 학생 확인
  -- -------------------------------------------------------
  select s.id
    into v_student_id
  from auto_grading.students s
  where s.student_code = p_student_code
    and s.is_active = true
  limit 1;

  if v_student_id is null then
    raise exception 'STUDENT_NOT_FOUND';
  end if;

  -- -------------------------------------------------------
  -- 2-2. test_set 유효성 확인
  --     현재는 존재 여부만 확인
  --     TODO:
  --       - 공개 여부
  --       - 시작/종료 시각
  --       - 학년/반 접근 정책
  -- -------------------------------------------------------
  select exists(
    select 1
    from auto_grading.test_sets ts
    where ts.id = p_test_set_id
      -- TODO 예시:
      -- and ts.is_published = true
      -- and now() between ts.opens_at and ts.closes_at
  )
    into v_test_set_exists;

  if not v_test_set_exists then
    raise exception 'INVALID_TEST_SET';
  end if;

  -- -------------------------------------------------------
  -- 2-3. 기존 활성 assignment 재사용 시도
  -- -------------------------------------------------------
  select a.id
    into v_assignment_id
  from auto_grading.assignments a
  where a.student_id = v_student_id
    and a.test_set_id = p_test_set_id
    and a.is_active = true
  order by a.created_at desc
  limit 1;

  -- -------------------------------------------------------
  -- 2-4. 없으면 새 assignment 생성
  --     경쟁 상황에서 unique_violation이 나면 재조회
  --
  --     주의:
  --     다른 트랜잭션이 막 insert 후 아직 commit되지 않았다면
  --     재조회에서도 못 잡힐 수 있으므로,
  --     그 경우 프론트가 재시도 가능한 에러명을 반환한다.
  -- -------------------------------------------------------
  if v_assignment_id is null then
    begin
      insert into auto_grading.assignments (
        student_id,
        test_set_id,
        assigned_by,
        status,
        is_active
      )
      values (
        v_student_id,
        p_test_set_id,
        'self_service', -- TODO: 장기적으로 assignment_source 컬럼 분리 가능
        'assigned',     -- TODO: 향후 공개/예약 정책에 따라 초기 status 재검토 가능
        true
      )
      returning id into v_assignment_id;

    exception
      when unique_violation then
        select a.id
          into v_assignment_id
        from auto_grading.assignments a
        where a.student_id = v_student_id
          and a.test_set_id = p_test_set_id
          and a.is_active = true
        order by a.created_at desc
        limit 1;
    end;
  end if;

  if v_assignment_id is null then
    raise exception 'ASSIGNMENT_UNAVAILABLE_RETRY';
  end if;

  -- -------------------------------------------------------
  -- 2-5. 기존 검증 완료된 start_attempt 재사용
  -- -------------------------------------------------------
  return auto_grading.start_attempt(
    p_assignment_id := v_assignment_id,
    p_student_code := p_student_code
  );
end;
$$;


-- =========================================================
-- 3. public wrapper 생성
--    프론트에서 supabase.rpc('start_attempt_by_test_set', ...)
--    호출 가능하도록 wrapper 제공
--
--    참고:
--    기존 public.start_attempt wrapper와 보안 정책을 맞추는 것이 권장됨
-- =========================================================
create or replace function public.start_attempt_by_test_set(
  p_test_set_id uuid,
  p_student_code text
)
returns jsonb
language sql
security definer
as $$
  select auto_grading.start_attempt_by_test_set(p_test_set_id, p_student_code);
$$;


-- =========================================================
-- 4. anon 실행 권한 부여
-- =========================================================
grant execute on function public.start_attempt_by_test_set(uuid, text) to anon;


-- =========================================================
-- 5. 수동 테스트
--    실제 존재하는 test_set_id / student_code로 실행
-- =========================================================
select public.start_attempt_by_test_set(
  '668e3d4a-20e6-463c-b644-8a712e9f3006'::uuid,
  'S003'::text
);
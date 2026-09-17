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
--   - UNIQUE(student_id, test_set_id)에 따라 같은 학생-같은 시험의
--     assignment는 전체 기간에 1개만 허용됨
--
-- 3단계 정책:
--   - 기존 assignment/attempt는 그대로 재사용하여 진행 중 응시를 보호함
--   - 신규 assignment는 활성 강좌가 정확히 1개일 때만 생성함
--   - 신규 assignment에는 course_id를 반드시 저장함
-- =========================================================


-- =========================================================
-- 2. 시험 시작 시 assignment 자동 생성/재사용 RPC
--    입력: test_set_id + student_code
--    출력: 기존 start_attempt와 동일한 jsonb
-- =========================================================
begin;

create or replace function auto_grading.start_attempt_by_test_set(
  p_test_set_id uuid,
  p_student_code text
)
returns jsonb
language plpgsql
security definer
set search_path to 'auto_grading', 'public'
as $function$
declare
  v_student_id uuid;
  v_assignment_id uuid;
  v_test_set_exists boolean;
  v_active_course_count integer := 0;
  v_active_course_id uuid;
begin
  if p_test_set_id is null then
    raise exception 'INVALID_TEST_SET' using errcode = 'P0001';
  end if;

  if p_student_code is null or btrim(p_student_code) = '' then
    raise exception 'INVALID_STUDENT_CODE' using errcode = 'P0001';
  end if;

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
  -- 2-3. 기존 assignment가 있으면 먼저 재사용한다.
  --      이미 진행 중인 응시는 강좌가 종료된 뒤에도 마칠 수 있어야 하므로,
  --      활성 강좌 해석보다 기존 assignment 확인을 먼저 한다.
  -- -------------------------------------------------------
  select a.id
    into v_assignment_id
  from auto_grading.assignments a
  where a.student_id = v_student_id
    and a.test_set_id = p_test_set_id
  order by a.created_at desc
  limit 1;

  if v_assignment_id is not null then
    return auto_grading.start_attempt(
      p_assignment_id := v_assignment_id,
      p_student_code := p_student_code
    );
  end if;

  -- -------------------------------------------------------
  -- 2-4. 신규 assignment는 학생의 활성 강좌가 정확히 1개일 때만 생성
  --      과정과 수강 연결이 모두 활성인 강좌만 후보로 인정한다.
  -- -------------------------------------------------------
  select
    count(distinct sc.course_id)::integer,
    min(sc.course_id::text)::uuid
    into v_active_course_count, v_active_course_id
  from auto_grading.student_courses sc
  join auto_grading.courses c
    on c.id = sc.course_id
   and c.is_active
  where sc.student_id = v_student_id
    and coalesce(sc.is_active, sc.ended_at is null);

  if v_active_course_count = 0 then
    raise exception 'ACTIVE_COURSE_NOT_FOUND' using errcode = 'P0001';
  end if;

  if v_active_course_count > 1 then
    raise exception 'MULTIPLE_ACTIVE_COURSES' using errcode = 'P0001';
  end if;

  -- 강좌 종료와 신규 assignment 생성의 동시 실행을 막는다.
  perform 1
  from auto_grading.courses c
  where c.id = v_active_course_id
    and c.is_active
  for share;

  if not found then
    raise exception 'COURSE_INACTIVE' using errcode = 'P0001';
  end if;

  -- -------------------------------------------------------
  -- 2-5. 신규 assignment 생성
  --     경쟁 상황에서 unique_violation이 나면 재조회
  --
  --     주의:
  --     다른 트랜잭션이 막 insert 후 아직 commit되지 않았다면
  --     재조회에서도 못 잡힐 수 있으므로,
  --     그 경우 프론트가 재시도 가능한 에러명을 반환한다.
  -- -------------------------------------------------------
  begin
    insert into auto_grading.assignments (
      student_id,
      test_set_id,
      course_id,
      assigned_by,
      status,
      is_active
    )
    values (
      v_student_id,
      p_test_set_id,
      v_active_course_id,
      'self_service',
      'assigned',
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
      order by a.created_at desc
      limit 1;
  end;

  if v_assignment_id is null then
    raise exception 'ASSIGNMENT_UNAVAILABLE_RETRY';
  end if;

  -- -------------------------------------------------------
  -- 2-6. 기존 검증 완료된 start_attempt 재사용
  -- -------------------------------------------------------
  return auto_grading.start_attempt(
    p_assignment_id := v_assignment_id,
    p_student_code := p_student_code
  );
end;
$function$;


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
set search_path to 'public', 'auto_grading'
as $function$
  select auto_grading.start_attempt_by_test_set(p_test_set_id, p_student_code);
$function$;


-- =========================================================
-- 4. anon 실행 권한 부여
-- =========================================================
revoke execute on function auto_grading.start_attempt_by_test_set(uuid, text)
  from public, anon;
grant execute on function auto_grading.start_attempt_by_test_set(uuid, text)
  to authenticated, service_role;

revoke execute on function public.start_attempt_by_test_set(uuid, text)
  from public;
grant execute on function public.start_attempt_by_test_set(uuid, text)
  to anon, authenticated, service_role;

commit;

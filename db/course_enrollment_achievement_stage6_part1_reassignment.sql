-- ============================================================================
-- course_enrollment_achievement_stage6_part1_reassignment.sql
--
-- 6단계 part 1: assignment 강좌 재귀속 보호와 감사 이력.
--
-- 배포 순서:
--   1. 이 파일 실행
--   2. audit_course_enrollment_achievement_stage6_part1_reassignment.sql 실행
--   3. part 2a 진행
--
-- 정책:
--   - attempt의 학생·시험·강좌 귀속은 assignment와 항상 같아야 한다.
--   - 진행 중(in_progress) 또는 2차 대기(awaiting_retry) attempt가 있으면
--     assignment를 다른 강좌로 재귀속하지 않는다.
--   - 대상 강좌는 해당 학생의 수강 이력에 존재해야 한다.
--   - 미지정(NULL)은 정상 목적지가 아니므로 target course는 필수다.
--   - assignment와 연결 attempt를 한 트랜잭션에서 함께 변경하고 감사한다.
-- ============================================================================

begin;

-- --------------------------------------------------------------------------
-- 1. attempt ↔ assignment 귀속 일치 강제
-- --------------------------------------------------------------------------
create or replace function auto_grading.trg_attempts_require_assignment_attribution()
returns trigger
language plpgsql
security definer
set search_path to 'auto_grading', 'public'
as $function$
declare
  v_assignment_student_id uuid;
  v_assignment_test_set_id uuid;
  v_assignment_course_id uuid;
begin
  -- assignment가 없는 과거 특수 행은 기존 동작을 유지한다. 3단계 이후 정상
  -- 생성 경로는 assignment_id를 필수로 저장한다.
  if new.assignment_id is null then
    return new;
  end if;

  select a.student_id, a.test_set_id, a.course_id
    into
      v_assignment_student_id,
      v_assignment_test_set_id,
      v_assignment_course_id
  from auto_grading.assignments a
  where a.id = new.assignment_id
  for share;

  if not found then
    raise exception 'ASSIGNMENT_NOT_FOUND';
  end if;

  if new.student_id is distinct from v_assignment_student_id then
    raise exception 'ATTEMPT_STUDENT_MISMATCH';
  end if;

  if new.test_set_id is distinct from v_assignment_test_set_id then
    raise exception 'ATTEMPT_TEST_SET_MISMATCH';
  end if;

  if new.course_id is distinct from v_assignment_course_id then
    raise exception 'ATTEMPT_COURSE_MISMATCH';
  end if;

  return new;
end;
$function$;

drop trigger if exists trg_attempts_require_assignment_attribution
  on auto_grading.attempts;

create trigger trg_attempts_require_assignment_attribution
before insert or update of assignment_id, student_id, test_set_id, course_id
on auto_grading.attempts
for each row
execute function auto_grading.trg_attempts_require_assignment_attribution();

revoke execute on function auto_grading.trg_attempts_require_assignment_attribution()
  from public, anon, authenticated, service_role;

-- --------------------------------------------------------------------------
-- 2. 명시적 재귀속 감사 테이블
-- --------------------------------------------------------------------------
create table if not exists auto_grading.assignment_course_reassignments (
  id uuid primary key default gen_random_uuid(),
  assignment_id uuid not null,
  student_id uuid not null,
  student_code text not null,
  student_name text not null,
  test_set_id uuid not null,
  test_title text not null,
  from_course_id uuid,
  from_course_name text,
  to_course_id uuid not null,
  to_course_name text not null,
  attempt_count integer not null default 0,
  reason text not null,
  changed_by uuid,
  changed_by_email text,
  changed_at timestamptz not null default now(),
  constraint chk_assignment_course_reassignments_changed
    check (from_course_id is distinct from to_course_id),
  constraint chk_assignment_course_reassignments_attempt_count
    check (attempt_count >= 0),
  constraint chk_assignment_course_reassignments_reason
    check (btrim(reason) <> '')
);

create index if not exists idx_assignment_course_reassignments_assignment
  on auto_grading.assignment_course_reassignments(assignment_id, changed_at desc);

create index if not exists idx_assignment_course_reassignments_student
  on auto_grading.assignment_course_reassignments(student_id, changed_at desc);

alter table auto_grading.assignment_course_reassignments enable row level security;

revoke all on table auto_grading.assignment_course_reassignments
  from public, anon, authenticated;
grant select on table auto_grading.assignment_course_reassignments
  to service_role;

-- --------------------------------------------------------------------------
-- 3. 관리자용 재귀속 감사 이력 조회
-- --------------------------------------------------------------------------
create or replace function auto_grading.teacher_list_assignment_course_reassignments(
  p_student_id uuid default null,
  p_assignment_id uuid default null,
  p_limit integer default 100
)
returns jsonb
language plpgsql
security definer
set search_path to 'auto_grading', 'public'
as $function$
declare
  v_result jsonb;
begin
  perform auto_grading.assert_admin();

  select coalesce(
    jsonb_agg(
      to_jsonb(x)
      order by x.changed_at desc, x.id desc
    ),
    '[]'::jsonb
  )
  into v_result
  from (
    select
      r.id,
      r.assignment_id,
      r.student_id,
      r.student_code,
      r.student_name,
      r.test_set_id,
      r.test_title,
      r.from_course_id,
      r.from_course_name,
      r.to_course_id,
      r.to_course_name,
      r.attempt_count,
      r.reason,
      r.changed_by,
      r.changed_by_email,
      r.changed_at
    from auto_grading.assignment_course_reassignments r
    where (p_student_id is null or r.student_id = p_student_id)
      and (p_assignment_id is null or r.assignment_id = p_assignment_id)
    order by r.changed_at desc, r.id desc
    limit least(greatest(coalesce(p_limit, 100), 1), 500)
  ) x;

  return v_result;
end;
$function$;

revoke execute on function auto_grading.teacher_list_assignment_course_reassignments(
  uuid,
  uuid,
  integer
) from public, anon;
grant execute on function auto_grading.teacher_list_assignment_course_reassignments(
  uuid,
  uuid,
  integer
) to authenticated, service_role;

-- --------------------------------------------------------------------------
-- 4. 관리자 명시적 assignment 강좌 재귀속
-- --------------------------------------------------------------------------
create or replace function auto_grading.teacher_reassign_assignment_course(
  p_assignment_id uuid,
  p_target_course_id uuid,
  p_reason text
)
returns jsonb
language plpgsql
security definer
set search_path to 'auto_grading', 'public'
as $function$
declare
  v_initial_course_id uuid;
  v_student_id uuid;
  v_student_code text;
  v_student_name text;
  v_test_set_id uuid;
  v_test_title text;
  v_from_course_id uuid;
  v_from_course_name text;
  v_to_course_name text;
  v_attempt_count integer := 0;
  v_active_attempt_count integer := 0;
  v_mismatch_count integer := 0;
  v_rows_updated integer := 0;
  v_audit_id uuid;
  v_changed_at timestamptz := now();
begin
  perform auto_grading.assert_admin();

  if p_assignment_id is null then
    raise exception 'ASSIGNMENT_ID_REQUIRED';
  end if;

  if p_target_course_id is null then
    raise exception 'TARGET_COURSE_REQUIRED';
  end if;

  if nullif(btrim(p_reason), '') is null then
    raise exception 'REASSIGN_REASON_REQUIRED';
  end if;

  -- 같은 assignment를 대상으로 한 요청끼리 먼저 직렬화한다.
  perform pg_advisory_xact_lock(hashtextextended(p_assignment_id::text, 0));

  select a.course_id
    into v_initial_course_id
  from auto_grading.assignments a
  where a.id = p_assignment_id;

  if not found then
    raise exception 'ASSIGNMENT_NOT_FOUND';
  end if;

  -- 강좌 종료 RPC와 같은 course -> assignment 잠금 순서를 사용한다.
  perform 1
  from auto_grading.courses c
  where c.id in (p_target_course_id, v_initial_course_id)
  order by c.id
  for share;

  select c.course_name
    into v_to_course_name
  from auto_grading.courses c
  where c.id = p_target_course_id;

  if not found then
    raise exception 'COURSE_NOT_FOUND';
  end if;

  -- 제출/교사확정 경로가 attempt -> assignment 순서로 잠글 수 있으므로,
  -- 기존 attempt를 먼저 잠가 교차 잠금 가능성을 줄인다.
  perform 1
  from auto_grading.attempts at
  where at.assignment_id = p_assignment_id
  order by at.id
  for update;

  select
    a.student_id,
    s.student_code,
    s.name,
    a.test_set_id,
    ts.title,
    a.course_id,
    c.course_name
  into
    v_student_id,
    v_student_code,
    v_student_name,
    v_test_set_id,
    v_test_title,
    v_from_course_id,
    v_from_course_name
  from auto_grading.assignments a
  join auto_grading.students s on s.id = a.student_id
  join auto_grading.test_sets ts on ts.id = a.test_set_id
  left join auto_grading.courses c on c.id = a.course_id
  where a.id = p_assignment_id
  for update of a;

  if not found then
    raise exception 'ASSIGNMENT_NOT_FOUND';
  end if;

  if v_from_course_id is distinct from v_initial_course_id then
    raise exception 'ASSIGNMENT_COURSE_CHANGED_RETRY';
  end if;

  if v_from_course_id = p_target_course_id then
    return jsonb_build_object(
      'ok', true,
      'changed', false,
      'assignment_id', p_assignment_id,
      'course_id', v_from_course_id,
      'course_name', v_from_course_name,
      'message', 'assignment is already assigned to the target course'
    );
  end if;

  if not exists (
    select 1
    from auto_grading.student_courses sc
    where sc.student_id = v_student_id
      and sc.course_id = p_target_course_id
  ) then
    raise exception 'TARGET_COURSE_NOT_IN_STUDENT_HISTORY';
  end if;

  -- assignment 잠금 전에 새로 만들어져 대기하던 attempt까지 포함해 다시 잠근다.
  perform 1
  from auto_grading.attempts at
  where at.assignment_id = p_assignment_id
  order by at.id
  for update;

  select
    count(*)::integer,
    count(*) filter (
      where at.status in ('in_progress', 'awaiting_retry')
    )::integer,
    count(*) filter (
      where at.student_id is distinct from v_student_id
        or at.test_set_id is distinct from v_test_set_id
        or at.course_id is distinct from v_from_course_id
    )::integer
  into
    v_attempt_count,
    v_active_attempt_count,
    v_mismatch_count
  from auto_grading.attempts at
  where at.assignment_id = p_assignment_id;

  if v_active_attempt_count > 0 then
    raise exception 'ACTIVE_ATTEMPT_REASSIGNMENT_BLOCKED: %', v_active_attempt_count;
  end if;

  if v_mismatch_count > 0 then
    raise exception 'ASSIGNMENT_ATTEMPT_ATTRIBUTION_MISMATCH: %', v_mismatch_count;
  end if;

  update auto_grading.assignments a
  set course_id = p_target_course_id,
      updated_at = v_changed_at
  where a.id = p_assignment_id
    and a.course_id is not distinct from v_from_course_id;

  get diagnostics v_rows_updated = row_count;

  if v_rows_updated <> 1 then
    raise exception 'ASSIGNMENT_REASSIGN_UPDATE_FAILED';
  end if;

  update auto_grading.attempts at
  set course_id = p_target_course_id,
      updated_at = v_changed_at
  where at.assignment_id = p_assignment_id;

  get diagnostics v_rows_updated = row_count;

  if v_rows_updated <> v_attempt_count then
    raise exception
      'ATTEMPT_REASSIGN_UPDATE_COUNT_MISMATCH: expected=%, updated=%',
      v_attempt_count,
      v_rows_updated;
  end if;

  insert into auto_grading.assignment_course_reassignments (
    assignment_id,
    student_id,
    student_code,
    student_name,
    test_set_id,
    test_title,
    from_course_id,
    from_course_name,
    to_course_id,
    to_course_name,
    attempt_count,
    reason,
    changed_by,
    changed_by_email,
    changed_at
  ) values (
    p_assignment_id,
    v_student_id,
    v_student_code,
    v_student_name,
    v_test_set_id,
    v_test_title,
    v_from_course_id,
    v_from_course_name,
    p_target_course_id,
    v_to_course_name,
    v_attempt_count,
    btrim(p_reason),
    auth.uid(),
    nullif(auth.jwt() ->> 'email', ''),
    v_changed_at
  )
  returning id into v_audit_id;

  return jsonb_build_object(
    'ok', true,
    'changed', true,
    'audit_id', v_audit_id,
    'assignment_id', p_assignment_id,
    'student_id', v_student_id,
    'student_code', v_student_code,
    'student_name', v_student_name,
    'test_set_id', v_test_set_id,
    'test_title', v_test_title,
    'from_course_id', v_from_course_id,
    'from_course_name', v_from_course_name,
    'to_course_id', p_target_course_id,
    'to_course_name', v_to_course_name,
    'attempt_count', v_attempt_count,
    'reason', btrim(p_reason),
    'changed_at', v_changed_at
  );
end;
$function$;

revoke execute on function auto_grading.teacher_reassign_assignment_course(uuid, uuid, text)
  from public, anon;
grant execute on function auto_grading.teacher_reassign_assignment_course(uuid, uuid, text)
  to authenticated, service_role;

comment on function auto_grading.teacher_list_assignment_course_reassignments(
  uuid,
  uuid,
  integer
) is '관리자 전용. 학생·assignment 필터로 강좌 재귀속 감사 이력을 최신순 조회.';

comment on function auto_grading.teacher_reassign_assignment_course(uuid, uuid, text) is
  '관리자 전용. 진행 중 응시가 없는 assignment와 연결 attempt를 학생 수강이력 내 다른 강좌로 원자적 재귀속하고 감사 이력을 남김.';

notify pgrst, 'reload schema';

commit;

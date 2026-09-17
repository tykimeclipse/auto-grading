-- ============================================================================
-- course_enrollment_achievement_stage4_course_close.sql
--
-- 4단계: 강좌 종료/재개 백엔드.
--
-- 구현 정책:
--   - 종료 전 teacher_get_course_close_preview 로 활성 수강생·열린 과제·
--     진행 중 응시 건수를 확인한다.
--   - 종료 시 courses.is_active=false, 활성 student_courses 종료,
--     열린 assignments를 closed_reason='course_closed'로 마감한다.
--   - 이미 시작된 in_progress / awaiting_retry attempt는 변경하지 않는다.
--     3단계 start_attempt/제출 경로에 따라 기존 응시는 계속 완료할 수 있다.
--   - 재개 시 courses.is_active=true만 적용한다. 과거 수강생과 과제는
--     자동 재활성화/재오픈하지 않는다.
--   - 활성 수강 판정은 모든 경로에서
--     coalesce(student_courses.is_active, student_courses.ended_at is null)로 통일한다.
--   - 이 파일은 검토 후 Supabase SQL Editor에서 원장님이 직접 실행한다.
-- ============================================================================

begin;

-- --------------------------------------------------------------------------
-- 1. 강좌 목록: 활성 수강생 수를 3단계와 같은 직접 조회식으로 통일
-- --------------------------------------------------------------------------
-- 반환 컬럼(active_student_count) 추가로 시그니처가 바뀌므로 먼저 drop한다.
-- 구버전과 이미 최신인 버전 모두 같은 절차로 안전하게 교체된다.
drop function if exists auto_grading.teacher_list_course_catalog(
  text, boolean, integer, integer
);

create or replace function auto_grading.teacher_list_course_catalog(
  p_search text default null,
  p_only_active boolean default false,
  p_limit integer default 100,
  p_offset integer default 0
)
returns table(
  total_count bigint,
  course_id uuid,
  course_no integer,
  course_code text,
  course_name text,
  open_year integer,
  start_date date,
  end_date date,
  course_type text,
  note text,
  is_active boolean,
  active_student_count integer,
  created_at timestamptz
)
language sql
security definer
set search_path to 'auto_grading', 'public'
as $function$
with sc_counts as (
  select
    sc.course_id,
    count(distinct sc.student_id) filter (
      where coalesce(sc.is_active, sc.ended_at is null)
    )::integer as active_student_count
  from auto_grading.student_courses sc
  group by sc.course_id
),
base as (
  select
    c.id as course_id,
    c.course_no,
    c.course_code,
    c.course_name,
    c.open_year,
    c.start_date,
    c.end_date,
    c.course_type,
    c.note,
    c.is_active,
    coalesce(sc.active_student_count, 0) as active_student_count,
    c.created_at
  from auto_grading.courses c
  left join sc_counts sc on sc.course_id = c.id
  where (
      p_search is null
      or btrim(p_search) = ''
      or coalesce(c.course_name, '') ilike '%' || p_search || '%'
      or coalesce(c.course_code, '') ilike '%' || p_search || '%'
      or coalesce(c.course_no::text, '') ilike '%' || p_search || '%'
      or coalesce(c.course_type, '') ilike '%' || p_search || '%'
    )
    and (
      not p_only_active
      or c.is_active = true
    )
)
select
  count(*) over() as total_count,
  b.course_id,
  b.course_no,
  b.course_code,
  b.course_name,
  b.open_year,
  b.start_date,
  b.end_date,
  b.course_type,
  b.note,
  b.is_active,
  b.active_student_count,
  b.created_at
from base b
order by
  b.open_year desc nulls last,
  b.course_no desc nulls last,
  b.course_name asc
limit greatest(coalesce(p_limit, 100), 1)
offset greatest(coalesce(p_offset, 0), 0);
$function$;

-- --------------------------------------------------------------------------
-- 2. 종료된 강좌에 활성 수강이 새로 연결되는 것을 DB에서 차단
--
-- teacher_attach_student_to_course를 비롯한 모든 쓰기 경로에 같은 규칙을
-- 적용한다. 강좌 행의 FOR SHARE 잠금은 아래 종료 RPC의 FOR UPDATE 잠금과
-- 직렬화되어, 동시 처리 중에도 종료 강좌에 활성 수강이 남지 않게 한다.
-- --------------------------------------------------------------------------
create or replace function auto_grading.trg_student_courses_require_active_course()
returns trigger
language plpgsql
security definer
set search_path to 'auto_grading', 'public'
as $function$
declare
  v_course_is_active boolean;
begin
  if coalesce(new.is_active, new.ended_at is null) then
    select c.is_active
      into v_course_is_active
    from auto_grading.courses c
    where c.id = new.course_id
    for share;

    if not found then
      raise exception 'COURSE_NOT_FOUND';
    end if;

    if not coalesce(v_course_is_active, false) then
      raise exception 'COURSE_INACTIVE';
    end if;
  end if;

  return new;
end;
$function$;

drop trigger if exists trg_student_courses_require_active_course
  on auto_grading.student_courses;

create trigger trg_student_courses_require_active_course
before insert or update of student_id, course_id, is_active, ended_at
on auto_grading.student_courses
for each row
execute function auto_grading.trg_student_courses_require_active_course();

-- --------------------------------------------------------------------------
-- 3. 종료 확인창용 미리보기. 데이터는 변경하지 않는다.
-- --------------------------------------------------------------------------
create or replace function auto_grading.teacher_get_course_close_preview(
  p_course_ids uuid[]
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

  if coalesce(cardinality(p_course_ids), 0) = 0 then
    raise exception 'p_course_ids is required';
  end if;

  perform 1
  from unnest(p_course_ids) as x(course_id)
  where x.course_id is not null
  limit 1;

  if not found then
    raise exception 'p_course_ids must contain at least one non-null uuid';
  end if;

  with input_ids as (
    select distinct x.course_id
    from unnest(p_course_ids) as x(course_id)
    where x.course_id is not null
  ),
  course_rows as (
    select
      i.course_id,
      c.id is not null as found,
      c.course_name,
      c.is_active,
      coalesce(sc.active_student_count, 0) as active_student_count,
      coalesce(sc.active_enrollment_row_count, 0) as active_enrollment_row_count,
      coalesce(a.open_assignment_count, 0) as open_assignment_count,
      coalesce(atc.in_progress_count, 0) as in_progress_count,
      coalesce(atc.awaiting_retry_count, 0) as awaiting_retry_count,
      coalesce(atc.needs_review_count, 0) as needs_review_count
    from input_ids i
    left join auto_grading.courses c on c.id = i.course_id
    left join lateral (
      select
        count(distinct s.student_id)::integer as active_student_count,
        count(*)::integer as active_enrollment_row_count
      from auto_grading.student_courses s
      where s.course_id = c.id
        and coalesce(s.is_active, s.ended_at is null)
    ) sc on c.id is not null
    left join lateral (
      select count(*)::integer as open_assignment_count
      from auto_grading.assignments x
      where x.course_id = c.id
        and x.closed_at is null
    ) a on c.id is not null
    left join lateral (
      select
        count(*) filter (where x.status = 'in_progress')::integer as in_progress_count,
        count(*) filter (where x.status = 'awaiting_retry')::integer as awaiting_retry_count,
        count(*) filter (where x.status = 'needs_review')::integer as needs_review_count
      from auto_grading.attempts x
      where x.course_id = c.id
    ) atc on c.id is not null
  )
  select jsonb_build_object(
    'ok', true,
    'requested_count', count(*)::integer,
    'existing_count', count(*) filter (where cr.found)::integer,
    'not_found_count', count(*) filter (where not cr.found)::integer,
    'active_course_count', count(*) filter (where cr.found and cr.is_active)::integer,
    'already_inactive_course_count', count(*) filter (
      where cr.found and not coalesce(cr.is_active, false)
    )::integer,
    'active_student_count', coalesce(sum(cr.active_student_count), 0)::integer,
    'active_enrollment_row_count', coalesce(sum(cr.active_enrollment_row_count), 0)::integer,
    'open_assignment_count', coalesce(sum(cr.open_assignment_count), 0)::integer,
    'in_progress_count', coalesce(sum(cr.in_progress_count), 0)::integer,
    'awaiting_retry_count', coalesce(sum(cr.awaiting_retry_count), 0)::integer,
    'needs_review_count', coalesce(sum(cr.needs_review_count), 0)::integer,
    'items', coalesce(
      jsonb_agg(
        jsonb_strip_nulls(
          jsonb_build_object(
            'course_id', cr.course_id,
            'found', cr.found,
            'course_name', cr.course_name,
            'is_active', cr.is_active,
            'active_student_count', cr.active_student_count,
            'active_enrollment_row_count', cr.active_enrollment_row_count,
            'open_assignment_count', cr.open_assignment_count,
            'in_progress_count', cr.in_progress_count,
            'awaiting_retry_count', cr.awaiting_retry_count,
            'needs_review_count', cr.needs_review_count
          )
        )
        order by cr.course_name nulls last, cr.course_id
      ),
      '[]'::jsonb
    )
  )
  into v_result
  from course_rows cr;

  return v_result;
end;
$function$;

-- --------------------------------------------------------------------------
-- 4. 강좌 종료/재개
-- --------------------------------------------------------------------------
create or replace function auto_grading.teacher_set_course_active(
  p_course_ids uuid[],
  p_is_active boolean
)
returns jsonb
language plpgsql
security definer
set search_path to 'auto_grading', 'public'
as $function$
declare
  v_requested_count integer := 0;
  v_existing_count integer := 0;
  v_updated_count integer := 0;
  v_active_student_count integer := 0;
  v_active_enrollment_row_count integer := 0;
  v_open_assignment_count integer := 0;
  v_in_progress_count integer := 0;
  v_awaiting_retry_count integer := 0;
  v_needs_review_count integer := 0;
  v_enrollments_ended_count integer := 0;
  v_assignments_closed_count integer := 0;
  v_items jsonb := '[]'::jsonb;
begin
  perform auto_grading.assert_admin();

  if p_is_active is null then
    raise exception 'p_is_active is required';
  end if;

  if coalesce(cardinality(p_course_ids), 0) = 0 then
    raise exception 'p_course_ids is required';
  end if;

  drop table if exists pg_temp.tmp_course_action_input;
  drop table if exists pg_temp.tmp_course_action;

  create temporary table pg_temp.tmp_course_action_input (
    course_id uuid primary key
  ) on commit drop;

  create temporary table pg_temp.tmp_course_action (
    course_id uuid primary key,
    course_name text,
    previous_is_active boolean,
    active_student_count integer not null default 0,
    active_enrollment_row_count integer not null default 0,
    open_assignment_count integer not null default 0,
    in_progress_count integer not null default 0,
    awaiting_retry_count integer not null default 0,
    needs_review_count integer not null default 0
  ) on commit drop;

  insert into pg_temp.tmp_course_action_input(course_id)
  select distinct x.course_id
  from unnest(p_course_ids) as x(course_id)
  where x.course_id is not null;

  select count(*)::integer
    into v_requested_count
  from pg_temp.tmp_course_action_input;

  if v_requested_count = 0 then
    raise exception 'p_course_ids must contain at least one non-null uuid';
  end if;

  -- 신규 발행/응시 RPC의 FOR SHARE와 직렬화한다.
  perform c.id
  from auto_grading.courses c
  join pg_temp.tmp_course_action_input i on i.course_id = c.id
  order by c.id
  for update;

  insert into pg_temp.tmp_course_action (
    course_id,
    course_name,
    previous_is_active,
    active_student_count,
    active_enrollment_row_count,
    open_assignment_count,
    in_progress_count,
    awaiting_retry_count,
    needs_review_count
  )
  select
    c.id,
    c.course_name,
    c.is_active,
    (
      select count(distinct sc.student_id)::integer
      from auto_grading.student_courses sc
      where sc.course_id = c.id
        and coalesce(sc.is_active, sc.ended_at is null)
    ),
    (
      select count(*)::integer
      from auto_grading.student_courses sc
      where sc.course_id = c.id
        and coalesce(sc.is_active, sc.ended_at is null)
    ),
    (
      select count(*)::integer
      from auto_grading.assignments a
      where a.course_id = c.id
        and a.closed_at is null
    ),
    (
      select count(*)::integer
      from auto_grading.attempts at
      where at.course_id = c.id
        and at.status = 'in_progress'
    ),
    (
      select count(*)::integer
      from auto_grading.attempts at
      where at.course_id = c.id
        and at.status = 'awaiting_retry'
    ),
    (
      select count(*)::integer
      from auto_grading.attempts at
      where at.course_id = c.id
        and at.status = 'needs_review'
    )
  from auto_grading.courses c
  join pg_temp.tmp_course_action_input i on i.course_id = c.id;

  select count(*)::integer
    into v_existing_count
  from pg_temp.tmp_course_action;

  select
    count(*) filter (where a.previous_is_active is distinct from p_is_active)::integer,
    coalesce(sum(a.active_student_count), 0)::integer,
    coalesce(sum(a.active_enrollment_row_count), 0)::integer,
    coalesce(sum(a.open_assignment_count), 0)::integer,
    coalesce(sum(a.in_progress_count), 0)::integer,
    coalesce(sum(a.awaiting_retry_count), 0)::integer,
    coalesce(sum(a.needs_review_count), 0)::integer
  into
    v_updated_count,
    v_active_student_count,
    v_active_enrollment_row_count,
    v_open_assignment_count,
    v_in_progress_count,
    v_awaiting_retry_count,
    v_needs_review_count
  from pg_temp.tmp_course_action a;

  if not p_is_active then
    update auto_grading.student_courses sc
    set
      is_active = false,
      ended_at = coalesce(sc.ended_at, now()),
      updated_at = now()
    from pg_temp.tmp_course_action a
    where sc.course_id = a.course_id
      and coalesce(sc.is_active, sc.ended_at is null);

    get diagnostics v_enrollments_ended_count = row_count;

    update auto_grading.assignments x
    set
      closed_at = now(),
      closed_reason = 'course_closed',
      updated_at = now()
    from pg_temp.tmp_course_action a
    where x.course_id = a.course_id
      and x.closed_at is null;

    get diagnostics v_assignments_closed_count = row_count;
  end if;

  update auto_grading.courses c
  set
    is_active = p_is_active,
    end_date = case
      when not p_is_active and c.end_date is null then current_date
      else c.end_date
    end
  from pg_temp.tmp_course_action a
  where c.id = a.course_id
    and c.is_active is distinct from p_is_active;

  select coalesce(
    jsonb_agg(
      jsonb_build_object(
        'course_id', a.course_id,
        'course_name', a.course_name,
        'previous_is_active', a.previous_is_active,
        'is_active', p_is_active,
        'course_updated', a.previous_is_active is distinct from p_is_active,
        'active_student_count', a.active_student_count,
        'active_enrollment_row_count', a.active_enrollment_row_count,
        'open_assignment_count', a.open_assignment_count,
        'in_progress_count', a.in_progress_count,
        'awaiting_retry_count', a.awaiting_retry_count,
        'needs_review_count', a.needs_review_count
      )
      order by a.course_name, a.course_id
    ),
    '[]'::jsonb
  )
  into v_items
  from pg_temp.tmp_course_action a;

  return jsonb_build_object(
    'ok', true,
    'is_active', p_is_active,
    'requested_count', v_requested_count,
    'existing_count', v_existing_count,
    'updated_count', v_updated_count,
    'unchanged_count', v_existing_count - v_updated_count,
    'skipped_not_found_count', v_requested_count - v_existing_count,
    'active_student_count', v_active_student_count,
    'active_enrollment_row_count', v_active_enrollment_row_count,
    'open_assignment_count', v_open_assignment_count,
    'in_progress_count', v_in_progress_count,
    'awaiting_retry_count', v_awaiting_retry_count,
    'needs_review_count', v_needs_review_count,
    'enrollments_ended_count', v_enrollments_ended_count,
    'assignments_closed_count', v_assignments_closed_count,
    -- 재개 정책상 아래 두 값은 계산 결과가 아니라 항상 0이다.
    'reactivated_enrollment_count', 0,
    'reopened_assignment_count', 0,
    'items', v_items
  );
end;
$function$;

-- --------------------------------------------------------------------------
-- 5. 권한 및 설명
-- --------------------------------------------------------------------------
revoke execute on function auto_grading.teacher_list_course_catalog(
  text, boolean, integer, integer
) from public, anon;
grant execute on function auto_grading.teacher_list_course_catalog(
  text, boolean, integer, integer
) to authenticated, service_role;

revoke execute on function auto_grading.teacher_get_course_close_preview(uuid[])
  from public, anon;
grant execute on function auto_grading.teacher_get_course_close_preview(uuid[])
  to authenticated, service_role;

revoke execute on function auto_grading.teacher_set_course_active(uuid[], boolean)
  from public, anon;
grant execute on function auto_grading.teacher_set_course_active(uuid[], boolean)
  to authenticated, service_role;

revoke execute on function auto_grading.trg_student_courses_require_active_course()
  from public, anon, authenticated, service_role;

comment on function auto_grading.teacher_list_course_catalog(
  text, boolean, integer, integer
) is
  '강좌 목록. 활성 수강생 수는 student_courses 직접 조회와 공통 활성 판정식으로 집계한다.';

comment on function auto_grading.teacher_get_course_close_preview(uuid[]) is
  '강좌 종료 전 확인용. 활성 수강생·열린 과제·진행 중 응시 건수를 반환하며 데이터는 변경하지 않는다.';

comment on function auto_grading.teacher_set_course_active(uuid[], boolean) is
  '강좌 종료/재개. 종료 시 활성 수강과 열린 과제를 함께 마감하고 기존 진행 응시는 유지한다. 재개 시 수강과 과제를 자동 복원하지 않는다.';

comment on function auto_grading.trg_student_courses_require_active_course() is
  '활성 수강 생성·재활성화 시 강좌 활성 여부를 잠금과 함께 검증한다.';

-- 반환형 변경으로 drop/create한 RPC를 PostgREST가 즉시 다시 인식하게 한다.
notify pgrst, 'reload schema';

commit;

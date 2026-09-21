-- ============================================================================
-- audit_course_enrollment_achievement_stage7_part3_history_event_date_timezone.sql
--
-- 7단계 part 3 배포 후 기존 시험 기록의 event_date가 호출 세션 TimeZone과
-- 무관하게 Asia/Seoul 날짜로 계산되는지 검증한다.
-- SELECT만 수행하며 데이터와 스키마를 변경하지 않는다.
--
-- 실행 권한:
--   - 비공개 코어를 직접 호출하므로 함수 소유자 세션에서 실행한다.
--
-- 통과 조건:
--   - function_contract.blocking_issue_count = 0
--   - history_event_date_integrity.blocking_issue_count = 0
--   - timezone_impact_information은 정보성 행이다.
-- ============================================================================

with
target_function as (
  select
    p.proname as function_name,
    pg_get_function_identity_arguments(p.oid) as identity_arguments,
    pg_get_function_result(p.oid) as result_type,
    p.prosecdef as security_definer,
    p.provolatile = 's' as is_stable,
    position(
      'at time zone ''Asia/Seoul'''
      in p.prosrc
    ) > 0 as uses_seoul_event_date,
    md5(p.prosrc) as source_md5
  from pg_proc p
  join pg_namespace n on n.oid = p.pronamespace
  where n.nspname = 'auto_grading'
    and p.proname = '_student_achievement_history_core'
    and oidvectortypes(p.proargtypes) = 'uuid, text, uuid, integer'
),
active_students as (
  select
    s.id as student_id,
    s.student_code,
    s.name as student_name
  from auto_grading.students s
  where coalesce(s.is_active, true)
),
base_assignments as (
  select
    s.student_id,
    s.student_code,
    s.student_name,
    a.id as assignment_id,
    coalesce(a.assigned_at, a.created_at) as assigned_at
  from active_students s
  join auto_grading.assignments a on a.student_id = s.student_id
  where a.closed_at is null
     or exists (
       select 1
       from auto_grading.attempts x
       where x.assignment_id = a.id
         and x.status in ('completed', 'needs_review')
     )
),
ranked_attempts as (
  select
    a.student_id,
    a.assignment_id,
    a.started_at,
    a.round1_submitted_at,
    a.round2_submitted_at,
    a.completed_at,
    a.updated_at,
    row_number() over (
      partition by a.assignment_id
      order by coalesce(
        a.completed_at,
        a.round2_submitted_at,
        a.round1_submitted_at,
        a.updated_at,
        a.started_at
      ) desc
    ) as rn
  from auto_grading.attempts a
  join active_students s on s.student_id = a.student_id
  where a.assignment_id is not null
),
expected_history as (
  select
    ba.student_id,
    ba.student_code,
    ba.student_name,
    ba.assignment_id,
    coalesce(
      ra.completed_at,
      ra.round2_submitted_at,
      ra.round1_submitted_at,
      ra.started_at,
      ba.assigned_at
    ) as event_timestamp,
    (
      coalesce(
        ra.completed_at,
        ra.round2_submitted_at,
        ra.round1_submitted_at,
        ra.started_at,
        ba.assigned_at
      ) at time zone 'Asia/Seoul'
    )::date as expected_event_date
  from base_assignments ba
  left join ranked_attempts ra
    on ra.student_id = ba.student_id
   and ra.assignment_id = ba.assignment_id
   and ra.rn = 1
),
returned_history as materialized (
  select
    s.student_id,
    s.student_code,
    s.student_name,
    h.assignment_id,
    h.event_date as returned_event_date
  from active_students s
  cross join lateral auto_grading._student_achievement_history_core(
    s.student_id,
    'all',
    null,
    2147483647
  ) h
),
history_comparison as (
  select
    coalesce(eh.student_id, rh.student_id) as student_id,
    coalesce(eh.student_code, rh.student_code) as student_code,
    coalesce(eh.student_name, rh.student_name) as student_name,
    coalesce(eh.assignment_id, rh.assignment_id) as assignment_id,
    eh.event_timestamp,
    eh.expected_event_date,
    rh.returned_event_date,
    eh.assignment_id is null as missing_expected_row,
    rh.assignment_id is null as missing_returned_row
  from expected_history eh
  full join returned_history rh
    on rh.student_id = eh.student_id
   and rh.assignment_id = eh.assignment_id
),
history_mismatches as (
  select *
  from history_comparison hc
  where hc.missing_expected_row
     or hc.missing_returned_row
     or hc.returned_event_date is distinct from hc.expected_event_date
),
utc_seoul_date_differences as (
  select
    eh.student_code,
    eh.student_name,
    eh.assignment_id,
    eh.event_timestamp,
    (eh.event_timestamp at time zone 'UTC')::date as utc_event_date,
    eh.expected_event_date as seoul_event_date
  from expected_history eh
  where (eh.event_timestamp at time zone 'UTC')::date
    is distinct from eh.expected_event_date
),
role_timezones as (
  select
    r.rolname,
    r.rolconfig
  from pg_roles r
  where r.rolname in ('anon', 'authenticated', 'authenticator')
),
audit_rows as (
  select
    10 as sort_order,
    'function_contract'::text as section,
    case
      when count(*) = 1
       and bool_and(tf.result_type like 'TABLE(%')
       and bool_and(position('event_date date' in tf.result_type) > 0)
       and bool_and(tf.security_definer)
       and bool_and(tf.is_stable)
       and bool_and(tf.uses_seoul_event_date)
        then 0
      else 1
    end::integer as blocking_issue_count,
    jsonb_build_object(
      'expected_function_count', 1,
      'installed_function_count', count(*),
      'functions', coalesce(
        jsonb_agg(to_jsonb(tf) order by tf.function_name),
        '[]'::jsonb
      )
    ) as details
  from target_function tf

  union all

  select
    20,
    'history_event_date_integrity',
    (select count(*)::integer from history_mismatches),
    jsonb_build_object(
      'compared_history_count', (select count(*) from history_comparison),
      'mismatch_count', (select count(*) from history_mismatches),
      'mismatches', coalesce((
        select jsonb_agg(
          to_jsonb(x)
          order by x.student_code, x.assignment_id
        )
        from (
          select *
          from history_mismatches
          order by student_code, assignment_id
          limit 50
        ) x
      ), '[]'::jsonb),
      'rule', 'event_date = (selected event timestamp at time zone Asia/Seoul)::date'
    )

  union all

  select
    30,
    'timezone_impact_information',
    0,
    jsonb_build_object(
      'audit_session_timezone', current_setting('TimeZone'),
      'utc_vs_seoul_date_difference_count', (
        select count(*) from utc_seoul_date_differences
      ),
      'difference_samples', coalesce((
        select jsonb_agg(
          to_jsonb(x)
          order by x.event_timestamp desc, x.assignment_id
        )
        from (
          select *
          from utc_seoul_date_differences
          order by event_timestamp desc, assignment_id
          limit 50
        ) x
      ), '[]'::jsonb),
      'role_configs', coalesce((
        select jsonb_agg(to_jsonb(rt) order by rt.rolname)
        from role_timezones rt
      ), '[]'::jsonb),
      'policy', 'informational only: the function must remain correct regardless of session or role TimeZone'
    )
)
select
  ar.section,
  ar.blocking_issue_count,
  ar.details
from audit_rows ar
order by ar.sort_order;

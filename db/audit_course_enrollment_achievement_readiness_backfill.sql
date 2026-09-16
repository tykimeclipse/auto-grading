-- ============================================================================
-- audit_course_enrollment_achievement_readiness_backfill.sql
--
-- 1단계-D: 강좌 미지정 과제/응시의 backfill 후보와 운영 트리거 점검.
--
-- 중요:
--   - SELECT만 수행하며 데이터와 스키마를 변경하지 않는다.
--   - 현재 확인된 과제 55건, 응시 41건, 수강 5건만 대상으로 한다.
--   - 자동 backfill을 실행하지 않고 후보의 날짜 적합성만 집계한다.
--   - Supabase 프로젝트가 Healthy일 때 이 파일 전체를 한 번만 실행한다.
--   - 결과로 반환되는 3행의 section, details 값을 공유한다.
-- ============================================================================

with
active_course_candidates as (
  select
    sc.student_id,
    count(*) as active_course_count,
    min(sc.course_id::text)::uuid as candidate_course_id,
    min(sc.joined_at) as candidate_joined_at
  from auto_grading.student_courses sc
  where sc.is_active
  group by sc.student_id
),
attempts_by_assignment as (
  select
    at.assignment_id,
    count(*) as attempt_count,
    count(*) filter (where at.status = 'in_progress') as in_progress_count,
    count(*) filter (where at.status = 'awaiting_retry') as awaiting_retry_count,
    count(*) filter (where at.status = 'completed') as completed_count,
    count(*) filter (where at.status = 'needs_review') as needs_review_count,
    min(at.started_at) as first_attempt_started_at,
    max(at.started_at) as last_attempt_started_at
  from auto_grading.attempts at
  where at.assignment_id is not null
  group by at.assignment_id
),
backfill_candidates as (
  select
    a.id as assignment_id,
    a.student_id,
    a.assigned_at,
    coalesce(acc.active_course_count, 0) as active_course_count,
    case
      when acc.active_course_count = 1 then acc.candidate_course_id
      else null
    end as candidate_course_id,
    case
      when acc.active_course_count = 1 then acc.candidate_joined_at
      else null
    end as candidate_joined_at,
    c.course_name as candidate_course_name,
    c.start_date as candidate_start_date,
    c.end_date as candidate_end_date,
    coalesce(aba.attempt_count, 0) as attempt_count,
    coalesce(aba.in_progress_count, 0) as in_progress_count,
    coalesce(aba.awaiting_retry_count, 0) as awaiting_retry_count,
    coalesce(aba.completed_count, 0) as completed_count,
    coalesce(aba.needs_review_count, 0) as needs_review_count,
    aba.first_attempt_started_at,
    aba.last_attempt_started_at
  from auto_grading.assignments a
  left join active_course_candidates acc
    on acc.student_id = a.student_id
  left join auto_grading.courses c
    on c.id = case
      when acc.active_course_count = 1 then acc.candidate_course_id
      else null
    end
  left join attempts_by_assignment aba
    on aba.assignment_id = a.id
  where a.course_id is null
),
backfill_by_course as (
  select
    bc.candidate_course_id as course_id,
    bc.candidate_course_name as course_name,
    bc.candidate_start_date as start_date,
    bc.candidate_end_date as end_date,
    count(*) as assignment_count,
    count(*) filter (where bc.attempt_count > 0) as assignments_with_attempt,
    sum(bc.attempt_count) as attempt_count,
    sum(bc.in_progress_count) as in_progress_count,
    sum(bc.awaiting_retry_count) as awaiting_retry_count,
    sum(bc.completed_count) as completed_count,
    sum(bc.needs_review_count) as needs_review_count,
    count(*) filter (
      where bc.candidate_start_date is not null
        and bc.assigned_at::date < bc.candidate_start_date
    ) as assignment_before_course_start,
    count(*) filter (
      where bc.candidate_end_date is not null
        and bc.assigned_at::date > bc.candidate_end_date
    ) as assignment_after_course_end,
    count(*) filter (
      where bc.candidate_start_date is not null
        and bc.candidate_end_date is not null
        and bc.assigned_at::date between bc.candidate_start_date and bc.candidate_end_date
    ) as assignment_within_course_dates,
    count(*) filter (
      where bc.first_attempt_started_at is not null
        and bc.candidate_start_date is not null
        and bc.first_attempt_started_at::date < bc.candidate_start_date
    ) as attempt_before_course_start,
    count(*) filter (
      where bc.last_attempt_started_at is not null
        and bc.candidate_end_date is not null
        and bc.last_attempt_started_at::date > bc.candidate_end_date
    ) as attempt_after_course_end,
    count(*) filter (
      where bc.first_attempt_started_at is not null
        and bc.candidate_start_date is not null
        and bc.candidate_end_date is not null
        and bc.first_attempt_started_at::date between bc.candidate_start_date and bc.candidate_end_date
    ) as attempt_within_course_dates
  from backfill_candidates bc
  where bc.active_course_count = 1
  group by
    bc.candidate_course_id,
    bc.candidate_course_name,
    bc.candidate_start_date,
    bc.candidate_end_date
),
attempt_trigger_inventory as (
  select
    t.tgname as trigger_name,
    t.tgenabled as enabled_mode,
    pg_get_triggerdef(t.oid, true) as trigger_definition,
    p.proname as function_name,
    md5(p.prosrc) as source_md5,
    p.prosrc ~* 'update\s+(auto_grading\.)?assignments' as updates_assignments,
    p.prosrc ~* 'course_closed' as references_course_closed
  from pg_trigger t
  join pg_class cls on cls.oid = t.tgrelid
  join pg_namespace n on n.oid = cls.relnamespace
  join pg_proc p on p.oid = t.tgfoid
  where n.nspname = 'auto_grading'
    and cls.relname = 'attempts'
    and not t.tgisinternal
),
audit_rows as (
  select
    10 as sort_order,
    'backfill_candidate_summary'::text as section,
    jsonb_build_object(
      'unassigned_assignments', count(*),
      'assignments_with_attempt', count(*) filter (where attempt_count > 0),
      'assignments_without_attempt', count(*) filter (where attempt_count = 0),
      'candidate_with_one_active_course', count(*) filter (where active_course_count = 1),
      'candidate_with_zero_active_courses', count(*) filter (where active_course_count = 0),
      'candidate_with_multiple_active_courses', count(*) filter (where active_course_count > 1),
      'assignment_before_candidate_joined_at', count(*) filter (
        where active_course_count = 1
          and candidate_joined_at is not null
          and assigned_at < candidate_joined_at
      ),
      'attempts_on_candidate_assignments', coalesce(sum(attempt_count), 0)
    ) as details
  from backfill_candidates

  union all

  select
    20,
    'backfill_candidates_by_course',
    coalesce(
      (select jsonb_agg(to_jsonb(x) order by x.start_date nulls last, x.course_name, x.course_id) from backfill_by_course x),
      '[]'::jsonb
    )

  union all

  select
    30,
    'attempt_trigger_inventory',
    coalesce(
      (select jsonb_agg(to_jsonb(x) order by x.trigger_name) from attempt_trigger_inventory x),
      '[]'::jsonb
    )
)
select section, details
from audit_rows
order by sort_order;

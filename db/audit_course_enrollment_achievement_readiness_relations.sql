-- ============================================================================
-- audit_course_enrollment_achievement_readiness_relations.sql
--
-- 1단계-C: 응시-과제-강좌 귀속 관계와 활성 수강 분포 점검.
--
-- 중요:
--   - SELECT만 수행하며 데이터와 스키마를 변경하지 않는다.
--   - attempts.assignment_id -> assignments.id 기본키 조인만 사용한다.
--   - 현재 운영 데이터(응시 41건, 과제 55건) 규모를 확인한 뒤 작성했다.
--   - Supabase 프로젝트가 Healthy일 때 이 파일 전체를 한 번만 실행한다.
--   - 결과로 반환되는 3행의 section, details 값을 공유한다.
-- ============================================================================

with
student_active_course_counts as (
  select
    s.id as student_id,
    count(sc.id) filter (where sc.is_active) as active_course_count
  from auto_grading.students s
  left join auto_grading.student_courses sc
    on sc.student_id = s.id
  group by s.id
),
attempt_assignment as (
  select
    at.id as attempt_id,
    at.student_id as attempt_student_id,
    at.test_set_id as attempt_test_set_id,
    at.status as attempt_status,
    a.id as matched_assignment_id,
    a.student_id as assignment_student_id,
    a.test_set_id as assignment_test_set_id,
    a.course_id as assignment_course_id,
    coalesce(sac.active_course_count, 0) as current_active_course_count
  from auto_grading.attempts at
  left join auto_grading.assignments a
    on a.id = at.assignment_id
  left join student_active_course_counts sac
    on sac.student_id = at.student_id
),
course_enrollment_counts as (
  select
    sc.course_id,
    count(*) filter (where sc.is_active) as active_student_count,
    count(*) filter (where not sc.is_active) as ended_enrollment_count
  from auto_grading.student_courses sc
  group by sc.course_id
),
course_assignment_counts as (
  select
    a.course_id,
    count(*) as assignment_count,
    count(*) filter (where a.closed_at is null) as open_assignment_count,
    count(*) filter (where a.closed_at is not null) as closed_assignment_count
  from auto_grading.assignments a
  where a.course_id is not null
  group by a.course_id
),
course_attempt_counts as (
  select
    a.course_id,
    count(*) as attempt_count,
    count(*) filter (where at.status = 'in_progress') as in_progress_count,
    count(*) filter (where at.status = 'awaiting_retry') as awaiting_retry_count,
    count(*) filter (where at.status = 'completed') as completed_count,
    count(*) filter (where at.status = 'needs_review') as needs_review_count
  from auto_grading.attempts at
  join auto_grading.assignments a
    on a.id = at.assignment_id
  where a.course_id is not null
  group by a.course_id
),
course_rollup as (
  select
    c.id as course_id,
    c.course_name,
    c.is_active,
    c.start_date,
    c.end_date,
    coalesce(cec.active_student_count, 0) as active_student_count,
    coalesce(cec.ended_enrollment_count, 0) as ended_enrollment_count,
    coalesce(cac.assignment_count, 0) as assignment_count,
    coalesce(cac.open_assignment_count, 0) as open_assignment_count,
    coalesce(cac.closed_assignment_count, 0) as closed_assignment_count,
    coalesce(cat.attempt_count, 0) as attempt_count,
    coalesce(cat.in_progress_count, 0) as in_progress_count,
    coalesce(cat.awaiting_retry_count, 0) as awaiting_retry_count,
    coalesce(cat.completed_count, 0) as completed_count,
    coalesce(cat.needs_review_count, 0) as needs_review_count
  from auto_grading.courses c
  left join course_enrollment_counts cec on cec.course_id = c.id
  left join course_assignment_counts cac on cac.course_id = c.id
  left join course_attempt_counts cat on cat.course_id = c.id
),
audit_rows as (
  select
    10 as sort_order,
    'attempt_course_resolution'::text as section,
    jsonb_build_object(
      'total_attempts', count(*),
      'assignment_match_missing', count(*) filter (where matched_assignment_id is null),
      'assignment_identity_mismatch', count(*) filter (
        where matched_assignment_id is not null
          and (
            attempt_student_id is distinct from assignment_student_id
            or attempt_test_set_id is distinct from assignment_test_set_id
          )
      ),
      'course_resolved_from_assignment', count(*) filter (where assignment_course_id is not null),
      'course_unresolved', count(*) filter (where assignment_course_id is null),
      'finished_course_unresolved', count(*) filter (
        where attempt_status in ('completed', 'needs_review')
          and assignment_course_id is null
      ),
      'in_progress_course_unresolved', count(*) filter (
        where attempt_status = 'in_progress'
          and assignment_course_id is null
      ),
      'awaiting_retry_course_unresolved', count(*) filter (
        where attempt_status = 'awaiting_retry'
          and assignment_course_id is null
      ),
      'unresolved_with_one_current_active_course', count(*) filter (
        where assignment_course_id is null
          and current_active_course_count = 1
      ),
      'unresolved_with_zero_current_active_courses', count(*) filter (
        where assignment_course_id is null
          and current_active_course_count = 0
      ),
      'unresolved_with_multiple_current_active_courses', count(*) filter (
        where assignment_course_id is null
          and current_active_course_count > 1
      )
    ) as details
  from attempt_assignment

  union all

  select
    20,
    'student_active_course_distribution',
    jsonb_build_object(
      'students_total', count(*),
      'students_with_zero_active_courses', count(*) filter (where active_course_count = 0),
      'students_with_one_active_course', count(*) filter (where active_course_count = 1),
      'students_with_multiple_active_courses', count(*) filter (where active_course_count > 1),
      'maximum_active_courses_for_one_student', coalesce(max(active_course_count), 0)
    )
  from student_active_course_counts

  union all

  select
    30,
    'course_rollup',
    coalesce(
      (select jsonb_agg(to_jsonb(x) order by x.start_date nulls last, x.course_name, x.course_id) from course_rollup x),
      '[]'::jsonb
    )
)
select section, details
from audit_rows
order by sort_order;

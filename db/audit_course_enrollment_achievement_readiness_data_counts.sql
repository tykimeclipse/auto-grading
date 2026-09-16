-- ============================================================================
-- audit_course_enrollment_achievement_readiness_data_counts.sql
--
-- 1단계-B: 운영 데이터의 정확한 건수와 상태 분포 점검.
--
-- 중요:
--   - SELECT만 수행하며 데이터와 스키마를 변경하지 않는다.
--   - 테이블 간 JOIN, 정렬, to_jsonb(행 전체 변환)를 사용하지 않는다.
--   - 대상 테이블 5개를 각각 한 번만 순차 스캔한다.
--   - Supabase 프로젝트가 Healthy일 때 이 파일 전체를 한 번만 실행한다.
--   - 결과로 반환되는 1행의 details 값을 공유한다.
-- ============================================================================

with
student_counts as (
  select
    count(*) as total,
    count(*) filter (where is_active) as active,
    count(*) filter (where not is_active) as inactive
  from auto_grading.students
),
course_counts as (
  select
    count(*) as total,
    count(*) filter (where is_active) as active,
    count(*) filter (where not is_active) as inactive
  from auto_grading.courses
),
student_course_counts as (
  select
    count(*) as total,
    count(*) filter (where is_active) as active,
    count(*) filter (where not is_active) as inactive,
    count(*) filter (where is_active and ended_at is not null) as active_with_ended_at,
    count(*) filter (where not is_active and ended_at is null) as inactive_without_ended_at
  from auto_grading.student_courses
),
assignment_counts as (
  select
    count(*) as total,
    count(*) filter (where course_id is null) as course_unassigned,
    count(*) filter (where course_id is not null) as course_assigned,
    count(*) filter (where closed_at is null) as open,
    count(*) filter (where closed_at is not null) as closed,
    count(*) filter (where is_active) as active,
    count(*) filter (where not is_active) as inactive
  from auto_grading.assignments
),
attempt_counts as (
  select
    count(*) as total,
    count(*) filter (where assignment_id is null) as assignment_unassigned,
    count(*) filter (where assignment_id is not null) as assignment_assigned,
    count(*) filter (where status = 'in_progress') as in_progress,
    count(*) filter (where status = 'awaiting_retry') as awaiting_retry,
    count(*) filter (where status = 'completed') as completed,
    count(*) filter (where status = 'needs_review') as needs_review,
    count(*) filter (where teacher_final_correct_count is not null) as teacher_finalized
  from auto_grading.attempts
)
select jsonb_build_object(
  'students', (select to_jsonb(x) from student_counts x),
  'courses', (select to_jsonb(x) from course_counts x),
  'student_courses', (select to_jsonb(x) from student_course_counts x),
  'assignments', (select to_jsonb(x) from assignment_counts x),
  'attempts', (select to_jsonb(x) from attempt_counts x)
) as details;

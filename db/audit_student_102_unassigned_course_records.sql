-- ============================================================================
-- audit_student_102_unassigned_course_records.sql
--
-- 학생코드 102(조아윤)의 강좌 미지정 assignment / attempt 점검.
--
-- 판정 기준:
--   - 학생 등록 시점: students.created_at
--   - 정식 서비스 시작 시점: 최초 student_courses.joined_at
--
-- SELECT만 수행하며 데이터와 스키마를 변경하지 않는다.
-- 현재 대상 학생의 소량 데이터만 조회한다.
-- 결과로 반환되는 3행의 section, details 값을 공유한다.
-- ============================================================================

with
target_student as (
  select
    s.id as student_id,
    s.student_code,
    s.name,
    s.created_at as student_registered_at
  from auto_grading.students s
  where s.student_code = '102'
),
enrollment_rows as (
  select
    ts.student_id,
    sc.id as student_course_id,
    sc.course_id,
    c.course_name,
    c.start_date,
    c.end_date,
    c.is_active as course_is_active,
    sc.is_active as enrollment_is_active,
    sc.joined_at,
    sc.ended_at
  from target_student ts
  left join auto_grading.student_courses sc
    on sc.student_id = ts.student_id
  left join auto_grading.courses c
    on c.id = sc.course_id
),
student_context as (
  select
    ts.student_id,
    ts.student_code,
    ts.name,
    ts.student_registered_at,
    min(er.joined_at) as first_course_joined_at,
    count(er.student_course_id) as enrollment_count,
    count(er.student_course_id) filter (where er.enrollment_is_active) as active_enrollment_count
  from target_student ts
  left join enrollment_rows er
    on er.student_id = ts.student_id
  group by
    ts.student_id,
    ts.student_code,
    ts.name,
    ts.student_registered_at
),
attempts_by_assignment as (
  select
    at.assignment_id,
    count(*) as attempt_count,
    jsonb_agg(
      jsonb_build_object(
        'attempt_id', at.id,
        'status', at.status,
        'attempt_no', at.attempt_no,
        'started_at', at.started_at,
        'completed_at', at.completed_at,
        'teacher_finalized', at.teacher_final_correct_count is not null
      )
      order by at.started_at, at.id
    ) as attempts
  from auto_grading.attempts at
  join target_student ts
    on ts.student_id = at.student_id
  where at.assignment_id is not null
  group by at.assignment_id
),
assignment_rows as (
  select
    a.id as assignment_id,
    a.course_id,
    a.assigned_at,
    a.closed_at,
    a.closed_reason,
    a.status as assignment_status,
    tset.id as test_set_id,
    tset.title as test_title,
    tset.source_type,
    coalesce(aba.attempt_count, 0) as attempt_count,
    coalesce(aba.attempts, '[]'::jsonb) as attempts,
    sc.student_registered_at,
    sc.first_course_joined_at,
    a.assigned_at >= sc.student_registered_at as issued_after_student_registration,
    case
      when sc.first_course_joined_at is null then null
      else a.assigned_at >= sc.first_course_joined_at
    end as issued_after_service_start
  from target_student ts
  join student_context sc
    on sc.student_id = ts.student_id
  join auto_grading.assignments a
    on a.student_id = ts.student_id
  join auto_grading.test_sets tset
    on tset.id = a.test_set_id
  left join attempts_by_assignment aba
    on aba.assignment_id = a.id
),
audit_rows as (
  select
    10 as sort_order,
    'student_and_service_baseline'::text as section,
    coalesce(
      (
        select jsonb_build_object(
          'student_code', sc.student_code,
          'student_name', sc.name,
          'student_registered_at', sc.student_registered_at,
          'service_start_basis', 'first_student_course_joined_at',
          'first_course_joined_at', sc.first_course_joined_at,
          'enrollment_count', sc.enrollment_count,
          'active_enrollment_count', sc.active_enrollment_count,
          'enrollments', coalesce(
            (
              select jsonb_agg(
                jsonb_build_object(
                  'student_course_id', er.student_course_id,
                  'course_id', er.course_id,
                  'course_name', er.course_name,
                  'course_start_date', er.start_date,
                  'course_end_date', er.end_date,
                  'course_is_active', er.course_is_active,
                  'enrollment_is_active', er.enrollment_is_active,
                  'joined_at', er.joined_at,
                  'ended_at', er.ended_at
                )
                order by er.joined_at, er.student_course_id
              ) filter (where er.student_course_id is not null)
              from enrollment_rows er
              where er.student_id = sc.student_id
            ),
            '[]'::jsonb
          )
        )
        from student_context sc
      ),
      jsonb_build_object('student_found', false, 'student_code', '102')
    ) as details

  union all

  select
    20,
    'assignment_course_summary',
    jsonb_build_object(
      'total_assignments', count(*),
      'course_assigned', count(*) filter (where course_id is not null),
      'course_unassigned', count(*) filter (where course_id is null),
      'unassigned_after_student_registration', count(*) filter (
        where course_id is null and issued_after_student_registration
      ),
      'unassigned_after_service_start', count(*) filter (
        where course_id is null and issued_after_service_start
      ),
      'unassigned_with_attempt', count(*) filter (
        where course_id is null and attempt_count > 0
      ),
      'attempts_on_unassigned_assignments', coalesce(
        sum(attempt_count) filter (where course_id is null),
        0
      ),
      'first_unassigned_at', min(assigned_at) filter (where course_id is null),
      'last_unassigned_at', max(assigned_at) filter (where course_id is null)
    )
  from assignment_rows

  union all

  select
    30,
    'unassigned_assignment_details',
    coalesce(
      (
        select jsonb_agg(
          jsonb_build_object(
            'assignment_id', ar.assignment_id,
            'test_set_id', ar.test_set_id,
            'test_title', ar.test_title,
            'source_type', ar.source_type,
            'assigned_at', ar.assigned_at,
            'assignment_status', ar.assignment_status,
            'closed_at', ar.closed_at,
            'closed_reason', ar.closed_reason,
            'issued_after_student_registration', ar.issued_after_student_registration,
            'issued_after_service_start', ar.issued_after_service_start,
            'attempt_count', ar.attempt_count,
            'attempts', ar.attempts
          )
          order by ar.assigned_at, ar.assignment_id
        )
        from assignment_rows ar
        where ar.course_id is null
      ),
      '[]'::jsonb
    )
)
select section, details
from audit_rows
order by sort_order;

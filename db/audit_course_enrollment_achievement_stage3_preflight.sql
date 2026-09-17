-- ============================================================================
-- audit_course_enrollment_achievement_stage3_preflight.sql
--
-- 3단계 쓰기 경로 SQL 배포 전 운영 데이터 점검.
-- SELECT만 수행하며 데이터와 스키마를 변경하지 않는다.
--
-- 확인 사항:
--   1. student_courses.is_active / ended_at 컬럼 존재 여부
--   2. student_courses.is_active IS NULL 행 존재 여부
--   3. course_id가 없는 legacy assignment의 학생별 분포
--   4. 그중 열려 있고 아직 attempt가 없는 assignment 상세
--
-- 중요:
--   활성 판정 점검은 to_jsonb(sc)로 컬럼을 읽는다. 따라서 ended_at 컬럼이
--   실제로 없어도 이 진단 SQL 자체는 실패하지 않고 has_ended_at=false를 반환한다.
--
-- 결과 3행의 section / details를 공유한다.
-- ============================================================================

with student_courses_columns as (
  select
    coalesce(jsonb_agg(c.column_name order by c.ordinal_position), '[]'::jsonb) as columns,
    coalesce(bool_or(c.column_name = 'is_active'), false) as has_is_active,
    coalesce(bool_or(c.column_name = 'ended_at'), false) as has_ended_at
  from information_schema.columns c
  where c.table_schema = 'auto_grading'
    and c.table_name = 'student_courses'
),
student_course_activity as (
  select
    sc.id,
    nullif(to_jsonb(sc)->>'is_active', '')::boolean as stored_is_active,
    coalesce(
      nullif(to_jsonb(sc)->>'is_active', '')::boolean,
      nullif(to_jsonb(sc)->>'ended_at', '') is null
    ) as active_by_shared_rule,
    nullif(to_jsonb(sc)->>'ended_at', '') as ended_at_text
  from auto_grading.student_courses sc
),
legacy_unassigned_assignments as (
  select
    a.id as assignment_id,
    s.student_code,
    s.name as student_name,
    a.test_set_id,
    ts.title as test_title,
    a.assigned_at,
    a.closed_at,
    exists (
      select 1
      from auto_grading.attempts at
      where at.assignment_id = a.id
    ) as has_attempt
  from auto_grading.assignments a
  join auto_grading.students s on s.id = a.student_id
  left join auto_grading.test_sets ts on ts.id = a.test_set_id
  where a.course_id is null
),
legacy_student_rollup as (
  select
    student_code,
    student_name,
    count(*)::integer as assignment_count,
    count(*) filter (where closed_at is null and not has_attempt)::integer
      as open_without_attempt_count,
    count(*) filter (where has_attempt)::integer as with_attempt_count
  from legacy_unassigned_assignments
  group by student_code, student_name
),
audit_rows as (
  select
    5 as sort_order,
    'student_courses_column_presence'::text as section,
    jsonb_build_object(
      'columns', scc.columns,
      'has_is_active', scc.has_is_active,
      'has_ended_at', scc.has_ended_at,
      'expected_has_is_active', true,
      'expected_has_ended_at', true
    ) as details
  from student_courses_columns scc

  union all

  select
    10 as sort_order,
    'student_course_active_integrity'::text as section,
    jsonb_build_object(
      'total_enrollments', count(*),
      'is_active_null_count', count(*) filter (where sca.stored_is_active is null),
      'active_by_shared_rule', count(*) filter (where sca.active_by_shared_rule),
      'inactive_by_shared_rule', count(*) filter (where not sca.active_by_shared_rule),
      'active_with_ended_at_count', count(*) filter (
        where sca.active_by_shared_rule
          and sca.ended_at_text is not null
      ),
      'expected_is_active_null_count', 0
    ) as details
  from student_course_activity sca

  union all

  select
    20,
    'legacy_course_unassigned_assignments',
    jsonb_build_object(
      'total', count(*),
      'open_without_attempt', count(*) filter (
        where lua.closed_at is null and not lua.has_attempt
      ),
      'with_attempt', count(*) filter (where lua.has_attempt),
      'student_rollup', coalesce(
        (
          select jsonb_agg(to_jsonb(x) order by x.student_code, x.student_name)
          from legacy_student_rollup x
        ),
        '[]'::jsonb
      ),
      'open_without_attempt_details', coalesce(
        (
          select jsonb_agg(
            jsonb_build_object(
              'assignment_id', x.assignment_id,
              'student_code', x.student_code,
              'student_name', x.student_name,
              'test_set_id', x.test_set_id,
              'test_title', x.test_title,
              'assigned_at', x.assigned_at
            )
            order by x.student_code, x.assigned_at, x.assignment_id
          )
          from legacy_unassigned_assignments x
          where x.closed_at is null
            and not x.has_attempt
        ),
        '[]'::jsonb
      )
    )
  from legacy_unassigned_assignments lua
)
select section, details
from audit_rows
order by sort_order;

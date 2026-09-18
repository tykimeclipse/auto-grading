-- ============================================================================
-- audit_course_enrollment_achievement_stage6_preflight.sql
--
-- 6단계 강좌별 성취도 조회·관리자 명시 재귀속 구현 전 운영 데이터 점검.
-- SELECT만 수행하며 데이터와 스키마를 변경하지 않는다.
--
-- 확인 사항:
--   1. 기존 성취도·과제 조회 함수와 course_id 스냅샷 컬럼 존재 여부
--   2. assignment ↔ attempt 학생·시험·강좌 귀속 무결성
--   3. 강좌별 assignment·attempt·완료 성취도 분포
--   4. 학생 102의 종료/활성 수강 및 강좌별 기록 분리 상태
--   5. 명시적 재귀속 시 보호해야 할 진행 중 응시 목록
--
-- 결과 5행의 section / details를 공유한다.
-- ============================================================================

with target_functions as (
  select
    n.nspname as schema_name,
    p.proname as function_name,
    pg_get_function_identity_arguments(p.oid) as identity_arguments,
    p.prosecdef as security_definer,
    md5(p.prosrc) as source_md5,
    has_function_privilege('anon', p.oid, 'EXECUTE') as anon_can_execute,
    has_function_privilege('authenticated', p.oid, 'EXECUTE') as authenticated_can_execute,
    has_function_privilege('service_role', p.oid, 'EXECUTE') as service_role_can_execute
  from pg_proc p
  join pg_namespace n on n.oid = p.pronamespace
  where n.nspname = 'auto_grading'
    and p.proname in (
      'get_student_stats_by_code',
      'get_student_assignment_history_by_code',
      'get_student_stats_by_token',
      'get_student_assignment_history_by_token',
      'teacher_list_assignments'
    )
),
attempt_attribution as (
  select
    at.id as attempt_id,
    at.assignment_id,
    at.student_id as attempt_student_id,
    at.test_set_id as attempt_test_set_id,
    at.course_id as attempt_course_id,
    at.status,
    at.started_at,
    a.id as matched_assignment_id,
    a.student_id as assignment_student_id,
    a.test_set_id as assignment_test_set_id,
    a.course_id as assignment_course_id
  from auto_grading.attempts at
  left join auto_grading.assignments a on a.id = at.assignment_id
),
assignment_rollup as (
  select
    a.course_id,
    count(*)::integer as assignment_count,
    count(*) filter (where a.closed_at is null)::integer as open_assignment_count,
    count(*) filter (where a.closed_reason = 'course_closed')::integer
      as course_closed_assignment_count
  from auto_grading.assignments a
  group by a.course_id
),
attempt_rollup as (
  select
    at.course_id,
    count(*)::integer as attempt_count,
    count(*) filter (where at.status = 'in_progress')::integer as in_progress_count,
    count(*) filter (where at.status = 'awaiting_retry')::integer as awaiting_retry_count,
    count(*) filter (where at.status = 'needs_review')::integer as needs_review_count,
    count(*) filter (where at.status = 'completed')::integer as completed_count,
    count(*) filter (where at.status in ('completed', 'needs_review'))::integer
      as achievement_attempt_count,
    coalesce(
      sum(at.total_items) filter (where at.status in ('completed', 'needs_review')),
      0
    )::integer as round1_item_count,
    case
      when coalesce(
        sum(at.total_items) filter (where at.status in ('completed', 'needs_review')),
        0
      ) = 0 then null
      else round(
        sum(at.first_correct_count) filter (
          where at.status in ('completed', 'needs_review')
        )::numeric
        / sum(at.total_items) filter (
          where at.status in ('completed', 'needs_review')
        )::numeric * 100,
        2
      )
    end as round1_accuracy,
    case
      when coalesce(
        sum(at.total_items) filter (
          where at.status in ('completed', 'needs_review')
            and ts.source_type is distinct from 'manual'
        ),
        0
      ) = 0 then null
      else round(
        sum(at.final_correct_count) filter (
          where at.status in ('completed', 'needs_review')
            and ts.source_type is distinct from 'manual'
        )::numeric
        / sum(at.total_items) filter (
          where at.status in ('completed', 'needs_review')
            and ts.source_type is distinct from 'manual'
        )::numeric * 100,
        2
      )
    end as round2_accuracy,
    case
      when coalesce(sum(
        case
          when at.teacher_final_correct_count is not null then at.total_items
          when ts.source_type = 'manual' then null
          when at.status = 'completed' then at.total_items
          else null
        end
      ), 0) = 0 then null
      else round(
        sum(
          case
            when at.teacher_final_correct_count is not null
              then at.teacher_final_correct_count
            when ts.source_type = 'manual' then null
            when at.status = 'completed' then at.final_correct_count
            else null
          end
        )::numeric
        / sum(
          case
            when at.teacher_final_correct_count is not null then at.total_items
            when ts.source_type = 'manual' then null
            when at.status = 'completed' then at.total_items
            else null
          end
        )::numeric * 100,
        2
      )
    end as final_accuracy
  from auto_grading.attempts at
  join auto_grading.test_sets ts on ts.id = at.test_set_id
  group by at.course_id
),
enrollment_rollup as (
  select
    sc.course_id,
    count(*)::integer as enrollment_history_count,
    count(*) filter (
      where coalesce(sc.is_active, sc.ended_at is null)
    )::integer as active_enrollment_count
  from auto_grading.student_courses sc
  group by sc.course_id
),
course_rollup as (
  select
    c.id as course_id,
    c.course_name,
    c.is_active as course_is_active,
    coalesce(er.enrollment_history_count, 0) as enrollment_history_count,
    coalesce(er.active_enrollment_count, 0) as active_enrollment_count,
    coalesce(ar.assignment_count, 0) as assignment_count,
    coalesce(ar.open_assignment_count, 0) as open_assignment_count,
    coalesce(ar.course_closed_assignment_count, 0) as course_closed_assignment_count,
    coalesce(atr.attempt_count, 0) as attempt_count,
    coalesce(atr.in_progress_count, 0) as in_progress_count,
    coalesce(atr.awaiting_retry_count, 0) as awaiting_retry_count,
    coalesce(atr.needs_review_count, 0) as needs_review_count,
    coalesce(atr.completed_count, 0) as completed_count,
    coalesce(atr.achievement_attempt_count, 0) as achievement_attempt_count,
    coalesce(atr.round1_item_count, 0) as round1_item_count,
    atr.round1_accuracy,
    atr.round2_accuracy,
    atr.final_accuracy
  from auto_grading.courses c
  left join enrollment_rollup er on er.course_id = c.id
  left join assignment_rollup ar on ar.course_id = c.id
  left join attempt_rollup atr on atr.course_id = c.id
),
target_student as (
  select s.id, s.student_code, s.name
  from auto_grading.students s
  where s.student_code = '102'
),
student_102_course_rows as (
  select
    c.id as course_id,
    c.course_name,
    c.is_active as course_is_active,
    exists (
      select 1
      from auto_grading.student_courses sc
      join target_student s on s.id = sc.student_id
      where sc.course_id = c.id
        and coalesce(sc.is_active, sc.ended_at is null)
    ) as enrollment_is_active,
    (
      select count(*)::integer
      from auto_grading.assignments a
      join target_student s on s.id = a.student_id
      where a.course_id = c.id
    ) as assignment_count,
    (
      select count(*)::integer
      from auto_grading.attempts at
      join target_student s on s.id = at.student_id
      where at.course_id = c.id
    ) as attempt_count,
    (
      select count(*)::integer
      from auto_grading.attempts at
      join target_student s on s.id = at.student_id
      where at.course_id = c.id
        and at.status in ('completed', 'needs_review')
    ) as achievement_attempt_count
  from auto_grading.courses c
  where exists (
      select 1
      from auto_grading.student_courses sc
      join target_student s on s.id = sc.student_id
      where sc.course_id = c.id
    )
    or exists (
      select 1
      from auto_grading.assignments a
      join target_student s on s.id = a.student_id
      where a.course_id = c.id
    )
    or exists (
      select 1
      from auto_grading.attempts at
      join target_student s on s.id = at.student_id
      where at.course_id = c.id
    )
),
active_attempts as (
  select
    aa.attempt_id,
    aa.assignment_id,
    s.student_code,
    s.name as student_name,
    ts.title as test_title,
    aa.status,
    aa.attempt_course_id,
    ac.course_name as attempt_course_name,
    aa.assignment_course_id,
    asc_course.course_name as assignment_course_name,
    aa.started_at
  from attempt_attribution aa
  join auto_grading.students s on s.id = aa.attempt_student_id
  join auto_grading.test_sets ts on ts.id = aa.attempt_test_set_id
  left join auto_grading.courses ac on ac.id = aa.attempt_course_id
  left join auto_grading.courses asc_course on asc_course.id = aa.assignment_course_id
  where aa.status in ('in_progress', 'awaiting_retry')
),
audit_rows as (
  select
    10 as sort_order,
    'stage6_schema_and_function_baseline'::text as section,
    jsonb_build_object(
      'assignments_course_id_exists', exists (
        select 1 from information_schema.columns
        where table_schema = 'auto_grading'
          and table_name = 'assignments'
          and column_name = 'course_id'
      ),
      'attempts_course_id_exists', exists (
        select 1 from information_schema.columns
        where table_schema = 'auto_grading'
          and table_name = 'attempts'
          and column_name = 'course_id'
      ),
      'functions', coalesce(
        (
          select jsonb_agg(to_jsonb(x) order by x.function_name, x.identity_arguments)
          from target_functions x
        ),
        '[]'::jsonb
      )
    ) as details

  union all

  select
    20,
    'assignment_attempt_course_integrity',
    jsonb_build_object(
      'assignments_total', (select count(*) from auto_grading.assignments),
      'assignments_course_unassigned', (
        select count(*) from auto_grading.assignments where course_id is null
      ),
      'attempts_total', (select count(*) from auto_grading.attempts),
      'attempts_course_unassigned', (
        select count(*) from auto_grading.attempts where course_id is null
      ),
      'attempt_without_assignment', count(*) filter (
        where aa.matched_assignment_id is null
      ),
      'student_mismatch', count(*) filter (
        where aa.matched_assignment_id is not null
          and aa.attempt_student_id is distinct from aa.assignment_student_id
      ),
      'test_set_mismatch', count(*) filter (
        where aa.matched_assignment_id is not null
          and aa.attempt_test_set_id is distinct from aa.assignment_test_set_id
      ),
      'course_mismatch', count(*) filter (
        where aa.matched_assignment_id is not null
          and aa.attempt_course_id is distinct from aa.assignment_course_id
      ),
      'course_mismatch_details', coalesce(
        jsonb_agg(
          jsonb_build_object(
            'attempt_id', aa.attempt_id,
            'assignment_id', aa.assignment_id,
            'attempt_course_id', aa.attempt_course_id,
            'assignment_course_id', aa.assignment_course_id,
            'status', aa.status
          ) order by aa.started_at, aa.attempt_id
        ) filter (
          where aa.matched_assignment_id is not null
            and aa.attempt_course_id is distinct from aa.assignment_course_id
        ),
        '[]'::jsonb
      )
    )
  from attempt_attribution aa

  union all

  select
    30,
    'course_achievement_rollup',
    coalesce(
      (
        select jsonb_agg(
          to_jsonb(x)
          order by x.course_is_active desc, x.course_name, x.course_id
        )
        from course_rollup x
      ),
      '[]'::jsonb
    )

  union all

  select
    40,
    'student_102_course_state',
    coalesce(
      (
        select jsonb_build_object(
          'student_code', s.student_code,
          'student_name', s.name,
          'course_rows', coalesce(
            (
              select jsonb_agg(
                to_jsonb(x)
                order by x.enrollment_is_active desc, x.course_name, x.course_id
              )
              from student_102_course_rows x
            ),
            '[]'::jsonb
          ),
          'unassigned_assignments', (
            select count(*)
            from auto_grading.assignments a
            where a.student_id = s.id and a.course_id is null
          ),
          'unassigned_attempts', (
            select count(*)
            from auto_grading.attempts at
            where at.student_id = s.id and at.course_id is null
          )
        )
        from target_student s
      ),
      jsonb_build_object('student_found', false, 'student_code', '102')
    )

  union all

  select
    50,
    'active_attempt_reassignment_guard',
    jsonb_build_object(
      'active_attempt_count', count(*),
      'in_progress_count', count(*) filter (where status = 'in_progress'),
      'awaiting_retry_count', count(*) filter (where status = 'awaiting_retry'),
      'items', coalesce(
        jsonb_agg(to_jsonb(x) order by x.started_at, x.attempt_id),
        '[]'::jsonb
      )
    )
  from active_attempts x
)
select section, details
from audit_rows
order by sort_order;

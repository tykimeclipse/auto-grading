-- ============================================================================
-- course_enrollment_achievement_stage7_part3_history_event_date_timezone.sql
--
-- 7단계 part 3: 기존 시험 기록과 대단원별 상세의 평가일 기준을 통일한다.
--
-- 변경 범위:
--   - _student_achievement_history_core의 event_date만 Asia/Seoul 기준으로 고정
--   - 반환 컬럼·점수 산식·정렬·권한은 변경하지 않음
--
-- 배포 후 실행:
--   1. audit_course_enrollment_achievement_stage7_part3_history_event_date_timezone.sql
--   2. audit_course_enrollment_achievement_stage6_part2b_postcutover.sql
-- ============================================================================

begin;

create or replace function auto_grading._student_achievement_history_core(
  p_student_id uuid,
  p_scope text,
  p_course_id uuid,
  p_limit integer
)
returns table (
  assignment_id uuid,
  test_set_id uuid,
  test_title text,
  source_type text,
  assigned_at timestamptz,
  event_date date,
  total_items integer,
  round1_correct_count integer,
  round1_score_percent numeric(5,1),
  round2_correct_count integer,
  round2_score_percent numeric(5,1),
  final_correct_count integer,
  final_score_percent numeric(5,1),
  teacher_final_score_percent numeric(5,1),
  last_activity_at timestamptz
)
language plpgsql
stable
security definer
set search_path to 'auto_grading', 'public'
as $function$
declare
  v_scope text := lower(btrim(coalesce(p_scope, '')));
begin
  if p_student_id is null then
    raise exception 'STUDENT_ID_REQUIRED'
      using errcode = 'P0001';
  end if;

  if v_scope not in ('all', 'course', 'unassigned') then
    raise exception 'INVALID_SCOPE'
      using errcode = 'P0001';
  end if;

  if v_scope = 'course' and p_course_id is null then
    raise exception 'COURSE_ID_REQUIRED_FOR_SCOPE'
      using errcode = 'P0001';
  end if;

  return query
  with base_assignments as (
    select
      a.id as assignment_id,
      a.test_set_id,
      coalesce(a.assigned_at, a.created_at) as assigned_at,
      a.updated_at as assignment_updated_at
    from auto_grading.assignments a
    where a.student_id = p_student_id
      and (
        a.closed_at is null
        or exists (
          select 1
          from auto_grading.attempts x
          where x.assignment_id = a.id
            and x.status in ('completed', 'needs_review')
        )
      )
      and (
        v_scope = 'all'
        or (v_scope = 'course' and a.course_id = p_course_id)
        or (v_scope = 'unassigned' and a.course_id is null)
      )
  ),
  test_item_counts as (
    select
      ti.test_set_id,
      count(*)::integer as item_count
    from auto_grading.test_items ti
    where ti.test_set_id in (
      select distinct ba.test_set_id
      from base_assignments ba
    )
    group by ti.test_set_id
  ),
  ranked_attempts as (
    select
      at.assignment_id,
      at.total_items as attempt_total_items,
      at.first_correct_count,
      at.final_correct_count,
      at.teacher_final_correct_count,
      at.first_score_percent,
      at.final_score_percent,
      at.teacher_final_score_percent,
      at.started_at,
      at.round1_submitted_at,
      at.round2_submitted_at,
      at.completed_at,
      at.updated_at,
      row_number() over (
        partition by at.assignment_id
        order by coalesce(
          at.completed_at,
          at.round2_submitted_at,
          at.round1_submitted_at,
          at.updated_at,
          at.started_at
        ) desc
      ) as rn
    from auto_grading.attempts at
    where at.student_id = p_student_id
      and (
        v_scope = 'all'
        or (v_scope = 'course' and at.course_id = p_course_id)
        or (v_scope = 'unassigned' and at.course_id is null)
      )
  ),
  latest_attempt as (
    select *
    from ranked_attempts
    where rn = 1
  ),
  history_base as (
    select
      ba.assignment_id,
      ba.test_set_id,
      ts.title as test_title,
      ts.source_type,
      ba.assigned_at,
      (
        coalesce(
          la.completed_at,
          la.round2_submitted_at,
          la.round1_submitted_at,
          la.started_at,
          ba.assigned_at
        ) at time zone 'Asia/Seoul'
      )::date as event_date,
      coalesce(
        nullif(tic.item_count, 0),
        la.attempt_total_items,
        ts.total_items,
        0
      ) as total_items,
      la.first_correct_count as round1_correct_count,
      case
        when la.round1_submitted_at is not null
          and la.first_score_percent is not null
          then round(la.first_score_percent::numeric, 1)
        when la.round1_submitted_at is not null
          and la.first_correct_count is not null
          and coalesce(
            nullif(tic.item_count, 0),
            la.attempt_total_items,
            ts.total_items,
            0
          ) > 0
          then round(
            (la.first_correct_count::numeric * 100)
            / coalesce(
              nullif(tic.item_count, 0),
              la.attempt_total_items,
              ts.total_items
            ),
            1
          )
        else null
      end as round1_score_percent,
      case
        when ts.source_type = 'manual' then null
        else la.final_correct_count
      end as round2_correct_count,
      case
        when ts.source_type = 'manual' then null
        when la.round2_submitted_at is not null
          and la.final_score_percent is not null
          then round(la.final_score_percent::numeric, 1)
        when la.round2_submitted_at is not null
          and la.final_correct_count is not null
          and coalesce(
            nullif(tic.item_count, 0),
            la.attempt_total_items,
            ts.total_items,
            0
          ) > 0
          then round(
            (la.final_correct_count::numeric * 100)
            / coalesce(
              nullif(tic.item_count, 0),
              la.attempt_total_items,
              ts.total_items
            ),
            1
          )
        else null
      end as round2_score_percent,
      case
        when ts.source_type = 'manual' then la.teacher_final_correct_count
        else coalesce(la.teacher_final_correct_count, la.final_correct_count)
      end as final_correct_count,
      case
        when ts.source_type = 'manual' then
          case
            when la.teacher_final_score_percent is not null
              then round(la.teacher_final_score_percent::numeric, 1)
            else null
          end
        else
          case
            when coalesce(
              la.teacher_final_score_percent,
              la.final_score_percent
            ) is not null
              then round(coalesce(
                la.teacher_final_score_percent,
                la.final_score_percent
              )::numeric, 1)
            when coalesce(
              la.teacher_final_correct_count,
              la.final_correct_count
            ) is not null
              and coalesce(
                nullif(tic.item_count, 0),
                la.attempt_total_items,
                ts.total_items,
                0
              ) > 0
              then round(
                (coalesce(
                  la.teacher_final_correct_count,
                  la.final_correct_count
                )::numeric * 100)
                / coalesce(
                  nullif(tic.item_count, 0),
                  la.attempt_total_items,
                  ts.total_items
                ),
                1
              )
            else null
          end
      end as final_score_percent,
      case
        when la.teacher_final_score_percent is not null
          then round(la.teacher_final_score_percent::numeric, 1)
        when la.teacher_final_correct_count is not null
          and coalesce(
            nullif(tic.item_count, 0),
            la.attempt_total_items,
            ts.total_items,
            0
          ) > 0
          then round(
            (la.teacher_final_correct_count::numeric * 100)
            / coalesce(
              nullif(tic.item_count, 0),
              la.attempt_total_items,
              ts.total_items
            ),
            1
          )
        else null
      end as teacher_final_score_percent,
      coalesce(
        la.completed_at,
        la.round2_submitted_at,
        la.round1_submitted_at,
        la.updated_at,
        ba.assignment_updated_at,
        ba.assigned_at
      ) as last_activity_at
    from base_assignments ba
    left join latest_attempt la on la.assignment_id = ba.assignment_id
    left join auto_grading.test_sets ts on ts.id = ba.test_set_id
    left join test_item_counts tic on tic.test_set_id = ba.test_set_id
  )
  select
    hb.assignment_id,
    hb.test_set_id,
    hb.test_title,
    hb.source_type,
    hb.assigned_at,
    hb.event_date,
    hb.total_items,
    hb.round1_correct_count,
    hb.round1_score_percent,
    hb.round2_correct_count,
    hb.round2_score_percent,
    hb.final_correct_count,
    hb.final_score_percent,
    hb.teacher_final_score_percent,
    hb.last_activity_at
  from history_base hb
  order by hb.last_activity_at desc, hb.assignment_id desc
  limit greatest(coalesce(p_limit, 200), 1);
end;
$function$;

revoke execute on function auto_grading._student_achievement_history_core(
  uuid,
  text,
  uuid,
  integer
) from public, anon, authenticated, service_role;

comment on function auto_grading._student_achievement_history_core(
  uuid,
  text,
  uuid,
  integer
) is '비공개 공통 코어. 기존 시험 기록 반환 계약을 유지하며 범위별 기록과 Asia/Seoul 기준 평가일을 반환.';

notify pgrst, 'reload schema';

commit;


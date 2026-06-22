-- ================================================================
-- get_student_assignment_history_by_code
-- 학생 성취도 "시험 기록" 표(assignment 단위, 최신 attempt 기준).
--
-- 수동시험(source_type='manual') 대응:
--   · total_items : test_items 행이 없으므로 count=0 → nullif 로 0을 NULL 처리한 뒤
--                   attempt.total_items → test_sets.total_items 순으로 폴백.
--   · 2차 칼럼   : 수동시험은 2차가 없으므로 항상 NULL('-')로 반환.
--   · 최종 칼럼  : 수동시험은 teacher_final 만 사용(미확정 전 NULL → '-').
--                  (자동시험은 기존대로 teacher_final → final 폴백)
--   · source_type 를 반환해 프론트가 수동시험을 식별(배지/2차'-'/OMR숨김)하게 한다.
-- ================================================================
drop function if exists auto_grading.get_student_assignment_history_by_code(text, integer);

create or replace function auto_grading.get_student_assignment_history_by_code(
    p_student_code text,
    p_limit integer default 200
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
security definer
set search_path = auto_grading, public
as $$
declare
    v_student_id uuid;
    v_student_code text;
begin
    v_student_code := trim(p_student_code);

    if v_student_code is null or v_student_code = '' then
        raise exception 'student_code is required';
    end if;

    select s.id
      into v_student_id
      from auto_grading.students s
     where s.student_code = v_student_code
       and coalesce(s.is_active, true) = true
     limit 1;

    if v_student_id is null then
        raise exception '등록되지 않은 학생 코드입니다: %', v_student_code;
    end if;

    return query
    with base_assignments as (
        select
            a.id as assignment_id,
            a.test_set_id,
            coalesce(a.assigned_at, a.created_at) as assigned_at,
            a.updated_at as assignment_updated_at
        from auto_grading.assignments a
        where a.student_id = v_student_id
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
        where at.student_id = v_student_id
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
            ts.source_type as source_type,
            (ts.source_type = 'manual') as is_manual,
            ba.assigned_at,
            coalesce(
                la.completed_at,
                la.round2_submitted_at,
                la.round1_submitted_at,
                la.started_at,
                ba.assigned_at
            )::date as event_date,
            -- 총 문항수: test_items count(0이면 NULL) → attempt.total_items → test_sets.total_items
            coalesce(
                nullif(tic.item_count, 0),
                la.attempt_total_items,
                ts.total_items,
                0
            ) as total_items,
            la.first_correct_count as round1_correct_count,
            case
                when la.round1_submitted_at is not null and la.first_score_percent is not null
                then round(la.first_score_percent::numeric, 1)
                when la.round1_submitted_at is not null and la.first_correct_count is not null
                     and coalesce(nullif(tic.item_count, 0), la.attempt_total_items, ts.total_items, 0) > 0
                then round((la.first_correct_count::numeric * 100) / coalesce(nullif(tic.item_count, 0), la.attempt_total_items, ts.total_items), 1)
                else null
            end as round1_score_percent,
            -- 2차: 수동시험은 항상 NULL
            case when ts.source_type = 'manual' then null
                 else la.final_correct_count end as round2_correct_count,
            case
                when ts.source_type = 'manual' then null
                when la.round2_submitted_at is not null and la.final_score_percent is not null
                then round(la.final_score_percent::numeric, 1)
                when la.round2_submitted_at is not null and la.final_correct_count is not null
                     and coalesce(nullif(tic.item_count, 0), la.attempt_total_items, ts.total_items, 0) > 0
                then round((la.final_correct_count::numeric * 100) / coalesce(nullif(tic.item_count, 0), la.attempt_total_items, ts.total_items), 1)
                else null
            end as round2_score_percent,
            -- 최종 정답수: 수동은 teacher_final 만, 자동은 teacher_final → final 폴백
            case
                when ts.source_type = 'manual' then la.teacher_final_correct_count
                else coalesce(la.teacher_final_correct_count, la.final_correct_count)
            end as final_correct_count,
            -- 최종 성취도: 수동은 teacher_final 만, 자동은 기존 로직
            case
                when ts.source_type = 'manual' then
                    case when la.teacher_final_score_percent is not null
                         then round(la.teacher_final_score_percent::numeric, 1)
                         else null end
                else
                    case
                        when coalesce(la.teacher_final_score_percent, la.final_score_percent) is not null
                        then round(coalesce(la.teacher_final_score_percent, la.final_score_percent)::numeric, 1)
                        when coalesce(la.teacher_final_correct_count, la.final_correct_count) is not null
                             and coalesce(nullif(tic.item_count, 0), la.attempt_total_items, ts.total_items, 0) > 0
                        then round((coalesce(la.teacher_final_correct_count, la.final_correct_count)::numeric * 100)
                                   / coalesce(nullif(tic.item_count, 0), la.attempt_total_items, ts.total_items), 1)
                        else null
                    end
            end as final_score_percent,
            case
                when la.teacher_final_score_percent is not null
                then round(la.teacher_final_score_percent::numeric, 1)
                when la.teacher_final_correct_count is not null
                     and coalesce(nullif(tic.item_count, 0), la.attempt_total_items, ts.total_items, 0) > 0
                then round((la.teacher_final_correct_count::numeric * 100) / coalesce(nullif(tic.item_count, 0), la.attempt_total_items, ts.total_items), 1)
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
        left join latest_attempt la
            on la.assignment_id = ba.assignment_id
        left join auto_grading.test_sets ts
            on ts.id = ba.test_set_id
        left join test_item_counts tic
            on tic.test_set_id = ba.test_set_id
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
$$;

-- by_code 는 관리자(authenticated)만 직접 호출. 공개 페이지는 token 래퍼 경유.
revoke execute on function auto_grading.get_student_assignment_history_by_code(text, integer) from anon, public;
grant  execute on function auto_grading.get_student_assignment_history_by_code(text, integer) to authenticated;

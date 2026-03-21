-- =========================================================
-- 0) 정적 정규화 뷰
--    - 이제부터는 production schema를 명시적으로 사용
--    - test_sets: title, source, grade_level, total_items, created_at, is_active
-- =========================================================

create or replace view auto_grading.v_test_sets_normalized as
select
  ts.id as test_set_id,
  ts.title as test_title,
  ts.source as test_source,
  ts.grade_level,
  ts.total_items,
  ts.created_at,
  ts.is_active
from auto_grading.test_sets ts;

comment on view auto_grading.v_test_sets_normalized
is '교사용 시험 검색/상세용 정규화 뷰';

create or replace view auto_grading.v_latest_attempts as
select distinct on (at.assignment_id)
  at.assignment_id,
  at.id as attempt_id,
  coalesce(at.status, 'not_started') as attempt_status,
  at.first_score_percent as round1_score_percent,
  at.final_score_percent as round2_score_percent,
  coalesce(at.teacher_final_score_percent, at.final_score_percent) as final_score_percent,
  at.teacher_final_score_percent,
  at.created_at as attempt_created_at
from auto_grading.attempts at
where at.assignment_id is not null
order by
  at.assignment_id,
  at.created_at desc nulls last,
  at.id desc;

comment on view auto_grading.v_latest_attempts
is 'assignment별 최신 attempt 1개. round2_score_percent는 교사 보정 전 학생 2차 누적 점수, final_score_percent는 teacher_final 반영 최종 점수';

-- =========================================================
-- 1) 시험 목록 검색 RPC
--    - p_purpose가 null이 아니면, 해당 purpose로 발행 이력이 있는 시험만 반환
--    - avg_round2_score = 교사 보정 전 학생 2차 누적 평균
--    - avg_final_score  = teacher_final 반영 최종 평균
--    - 평균은 ended 상태(needs_review, completed)만 포함
-- =========================================================

create or replace function auto_grading.teacher_search_test_sets(
  p_search text default null,
  p_grade_level text default null,
  p_source text default null,
  p_purpose text default null,
  p_only_active boolean default null,
  p_limit integer default 100,
  p_offset integer default 0
)
returns table(
  total_count bigint,
  test_set_id uuid,
  test_title text,
  test_source text,
  grade_level text,
  total_items integer,
  assignment_count integer,
  open_assignment_count integer,
  started_assignment_count integer,
  needs_review_count integer,
  completed_assignment_count integer,
  ended_assignment_count integer,
  avg_round1_score numeric,
  avg_round2_score numeric,
  avg_final_score numeric,
  last_used_at timestamptz
)
language sql
security definer
set search_path to 'auto_grading', 'public'
as $function$
with params as (
  select nullif(btrim(p_search), '') as q
),
candidate_test_sets as (
  select
    v.test_set_id,
    v.test_title,
    v.test_source,
    v.grade_level,
    v.total_items,
    v.created_at,
    v.is_active
  from auto_grading.v_test_sets_normalized v
  cross join params p
  where 1 = 1
    and (p_only_active is null or v.is_active = p_only_active)
    and (p_grade_level is null or v.grade_level = p_grade_level)
    and (p_source is null or v.test_source = p_source)
    and (
      p.q is null
      or v.test_title ilike '%' || p.q || '%'
      or coalesce(v.test_source, '') ilike '%' || p.q || '%'
    )
),
relevant_assignments as (
  select
    a.id as assignment_id,
    a.test_set_id,
    a.closed_at,
    a.created_at
  from auto_grading.assignments a
  join candidate_test_sets c
    on c.test_set_id = a.test_set_id
  where p_purpose is null
     or a.purpose = p_purpose
),
usage_stats as (
  select
    ra.test_set_id,
    count(*)::integer as assignment_count,
    count(*) filter (where ra.closed_at is null)::integer as open_assignment_count,
    count(*) filter (
      where coalesce(la.attempt_status, 'not_started') <> 'not_started'
    )::integer as started_assignment_count,
    count(*) filter (
      where la.attempt_status = 'needs_review'
    )::integer as needs_review_count,
    count(*) filter (
      where la.attempt_status = 'completed'
    )::integer as completed_assignment_count,
    count(*) filter (
      where la.attempt_status in ('needs_review', 'completed')
    )::integer as ended_assignment_count,
    round(
      avg(la.round1_score_percent) filter (
        where la.attempt_status in ('needs_review', 'completed')
      )::numeric,
      2
    ) as avg_round1_score,
    round(
      avg(la.round2_score_percent) filter (
        where la.attempt_status in ('needs_review', 'completed')
      )::numeric,
      2
    ) as avg_round2_score,
    round(
      avg(la.final_score_percent) filter (
        where la.attempt_status in ('needs_review', 'completed')
      )::numeric,
      2
    ) as avg_final_score,
    max(ra.created_at) as last_used_at
  from relevant_assignments ra
  left join auto_grading.v_latest_attempts la
    on la.assignment_id = ra.assignment_id
  group by ra.test_set_id
),
base as (
  select
    c.test_set_id,
    c.test_title,
    c.test_source,
    c.grade_level,
    c.total_items,
    c.created_at,
    coalesce(u.assignment_count, 0) as assignment_count,
    coalesce(u.open_assignment_count, 0) as open_assignment_count,
    coalesce(u.started_assignment_count, 0) as started_assignment_count,
    coalesce(u.needs_review_count, 0) as needs_review_count,
    coalesce(u.completed_assignment_count, 0) as completed_assignment_count,
    coalesce(u.ended_assignment_count, 0) as ended_assignment_count,
    u.avg_round1_score,
    u.avg_round2_score,
    u.avg_final_score,
    u.last_used_at
  from candidate_test_sets c
  left join usage_stats u
    on u.test_set_id = c.test_set_id
  where p_purpose is null
     or u.test_set_id is not null
)
select
  count(*) over() as total_count,
  b.test_set_id,
  b.test_title,
  b.test_source,
  b.grade_level,
  b.total_items,
  b.assignment_count,
  b.open_assignment_count,
  b.started_assignment_count,
  b.needs_review_count,
  b.completed_assignment_count,
  b.ended_assignment_count,
  b.avg_round1_score,
  b.avg_round2_score,
  b.avg_final_score,
  b.last_used_at
from base b
cross join params p
order by
  case
    when p.q is not null and b.test_title ilike p.q || '%' then 0
    else 1
  end asc,
  b.last_used_at desc nulls last,
  b.created_at desc nulls last,
  b.test_title asc,
  b.test_set_id asc
limit greatest(coalesce(p_limit, 100), 1)
offset greatest(coalesce(p_offset, 0), 0);
$function$;

grant execute on function auto_grading.teacher_search_test_sets(
  text, text, text, text, boolean, integer, integer
) to authenticated;

-- =========================================================
-- 2) 선택한 시험 상세 요약 RPC
--    - search와 동일한 정의 사용
--    - avg_round2_score = 교사 보정 전 학생 2차 누적 평균
--    - avg_final_score  = teacher_final 반영 최종 평균
--    - 평균은 ended 상태(needs_review, completed)만 포함
-- =========================================================

create or replace function auto_grading.teacher_get_test_set_overview(
  p_test_set_id uuid,
  p_purpose text default null
)
returns table(
  test_set_id uuid,
  test_title text,
  test_source text,
  grade_level text,
  total_items integer,
  assignment_count integer,
  open_assignment_count integer,
  started_assignment_count integer,
  not_started_assignment_count integer,
  in_progress_count integer,
  needs_review_count integer,
  completed_count integer,
  ended_assignment_count integer,
  avg_round1_score numeric,
  avg_round2_score numeric,
  avg_final_score numeric,
  last_used_at timestamptz
)
language sql
security definer
set search_path to 'auto_grading', 'public'
as $function$
with usage_base as (
  select
    a.id as assignment_id,
    a.test_set_id,
    a.closed_at,
    a.created_at,
    coalesce(la.attempt_status, 'not_started') as attempt_status,
    la.round1_score_percent,
    la.round2_score_percent,
    la.final_score_percent
  from auto_grading.assignments a
  left join auto_grading.v_latest_attempts la
    on la.assignment_id = a.id
  where a.test_set_id = p_test_set_id
    and (p_purpose is null or a.purpose = p_purpose)
)
select
  v.test_set_id,
  v.test_title,
  v.test_source,
  v.grade_level,
  v.total_items,
  count(u.assignment_id)::integer as assignment_count,
  count(u.assignment_id) filter (where u.closed_at is null)::integer as open_assignment_count,
  count(u.assignment_id) filter (where u.attempt_status <> 'not_started')::integer as started_assignment_count,
  count(u.assignment_id) filter (where u.attempt_status = 'not_started')::integer as not_started_assignment_count,
  count(u.assignment_id) filter (where u.attempt_status = 'in_progress')::integer as in_progress_count,
  count(u.assignment_id) filter (where u.attempt_status = 'needs_review')::integer as needs_review_count,
  count(u.assignment_id) filter (where u.attempt_status = 'completed')::integer as completed_count,
  count(u.assignment_id) filter (where u.attempt_status in ('needs_review', 'completed'))::integer as ended_assignment_count,
  round(
    avg(u.round1_score_percent) filter (
      where u.attempt_status in ('needs_review', 'completed')
    )::numeric,
    2
  ) as avg_round1_score,
  round(
    avg(u.round2_score_percent) filter (
      where u.attempt_status in ('needs_review', 'completed')
    )::numeric,
    2
  ) as avg_round2_score,
  round(
    avg(u.final_score_percent) filter (
      where u.attempt_status in ('needs_review', 'completed')
    )::numeric,
    2
  ) as avg_final_score,
  max(u.created_at) as last_used_at
from auto_grading.v_test_sets_normalized v
left join usage_base u
  on u.test_set_id = v.test_set_id
where v.test_set_id = p_test_set_id
group by
  v.test_set_id,
  v.test_title,
  v.test_source,
  v.grade_level,
  v.total_items;
$function$;

grant execute on function auto_grading.teacher_get_test_set_overview(
  uuid, text
) to authenticated;
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
  select
    nullif(btrim(p_search), '') as search_q,
    nullif(btrim(p_grade_level), '') as grade_q,
    nullif(btrim(p_source), '') as source_q,
    nullif(btrim(p_purpose), '') as purpose_q
),
candidate_test_sets as (
  select
    v.test_set_id,
    v.test_title,
    v.test_source,
    v.grade_level,
    v.total_items,
    v.created_at,
    v.is_active,
    p.search_q
  from auto_grading.v_test_sets_normalized v
  cross join params p
  where 1 = 1
    and (p_only_active is null or v.is_active = p_only_active)
    and (p.grade_q is null or v.grade_level = p.grade_q)
    and (p.source_q is null or v.test_source = p.source_q)
    and (
      p.search_q is null
      or v.test_title ilike '%' || p.search_q || '%'
      or coalesce(v.test_source, '') ilike '%' || p.search_q || '%'
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
  cross join params p
  where p.purpose_q is null
     or a.purpose = p.purpose_q
),
filtered_test_sets as (
  select
    c.*
  from candidate_test_sets c
  cross join params p
  where p.purpose_q is null

  union all

  select
    c.*
  from candidate_test_sets c
  join (
    select distinct
      ra.test_set_id
    from relevant_assignments ra
  ) ra_ids
    on ra_ids.test_set_id = c.test_set_id
  cross join params p
  where p.purpose_q is not null
),
latest_attempt as (
  select distinct on (at.assignment_id)
    at.assignment_id,
    coalesce(at.status, 'not_started') as attempt_status,
    at.first_score_percent as round1_score_percent,
    at.final_score_percent as round2_score_percent,
    coalesce(at.teacher_final_score_percent, at.final_score_percent) as final_score_percent
  from auto_grading.attempts at
  join relevant_assignments ra
    on ra.assignment_id = at.assignment_id
  order by
    at.assignment_id,
    at.created_at desc nulls last,
    at.id desc
),
usage_stats as (
  select
    ra.test_set_id,
    count(*)::integer as assignment_count,
    count(*) filter (where ra.closed_at is null)::integer as open_assignment_count,
    count(*) filter (where la.attempt_status <> 'not_started')::integer as started_assignment_count,
    count(*) filter (where la.attempt_status = 'needs_review')::integer as needs_review_count,
    count(*) filter (where la.attempt_status = 'completed')::integer as completed_assignment_count,
    count(*) filter (where la.attempt_status in ('needs_review', 'completed'))::integer as ended_assignment_count,
    round(
      (avg(la.round1_score_percent) filter (
        where la.attempt_status in ('needs_review', 'completed')
      ))::numeric,
      2
    ) as avg_round1_score,
    round(
      (avg(la.round2_score_percent) filter (
        where la.attempt_status in ('needs_review', 'completed')
      ))::numeric,
      2
    ) as avg_round2_score,
    round(
      (avg(la.final_score_percent) filter (
        where la.attempt_status in ('needs_review', 'completed')
      ))::numeric,
      2
    ) as avg_final_score,
    max(ra.created_at) as last_used_at
  from relevant_assignments ra
  left join latest_attempt la
    on la.assignment_id = ra.assignment_id
  group by ra.test_set_id
),
base as (
  select
    f.test_set_id,
    f.test_title,
    f.test_source,
    f.grade_level,
    f.total_items,
    f.created_at,
    f.search_q,
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
  from filtered_test_sets f
  left join usage_stats u
    on u.test_set_id = f.test_set_id
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
order by
  case
    when b.search_q is not null and b.test_title ilike b.search_q || '%' then 0
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

comment on function auto_grading.teacher_search_test_sets(
  text, text, text, text, boolean, integer, integer
)
is '교사용 시험 검색 RPC. purpose 필터는 relevant_assignments 단계에서만 적용되고, p_purpose가 있을 때는 그 purpose 발행 이력이 있는 시험만 filtered_test_sets에 남긴다. last_used_at = 마지막 assignment 발행(created_at) 시각. avg_round2_score = 교사 보정 전 학생 2차 누적 평균, avg_final_score = teacher_final 반영 최종 평균';


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
  awaiting_retry_count integer,
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
with params as (
  select nullif(btrim(p_purpose), '') as purpose_q
),
relevant_assignments as (
  select
    a.id as assignment_id,
    a.test_set_id,
    a.closed_at,
    a.created_at
  from auto_grading.assignments a
  cross join params p
  where a.test_set_id = p_test_set_id
    and (p.purpose_q is null or a.purpose = p.purpose_q)
),
latest_attempt as (
  select distinct on (at.assignment_id)
    at.assignment_id,
    coalesce(at.status, 'not_started') as attempt_status,
    at.first_score_percent as round1_score_percent,
    at.final_score_percent as round2_score_percent,
    coalesce(at.teacher_final_score_percent, at.final_score_percent) as final_score_percent
  from auto_grading.attempts at
  join relevant_assignments ra
    on ra.assignment_id = at.assignment_id
  order by
    at.assignment_id,
    at.created_at desc nulls last,
    at.id desc
),
usage_base as (
  select
    ra.assignment_id,
    ra.test_set_id,
    ra.closed_at,
    ra.created_at,
    coalesce(la.attempt_status, 'not_started') as attempt_status,
    la.round1_score_percent,
    la.round2_score_percent,
    la.final_score_percent
  from relevant_assignments ra
  left join latest_attempt la
    on la.assignment_id = ra.assignment_id
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
  count(u.assignment_id) filter (where u.attempt_status = 'awaiting_retry')::integer as awaiting_retry_count,
  count(u.assignment_id) filter (where u.attempt_status = 'needs_review')::integer as needs_review_count,
  count(u.assignment_id) filter (where u.attempt_status = 'completed')::integer as completed_count,
  count(u.assignment_id) filter (where u.attempt_status in ('needs_review', 'completed'))::integer as ended_assignment_count,
  round(
    (avg(u.round1_score_percent) filter (
      where u.attempt_status in ('needs_review', 'completed')
    ))::numeric,
    2
  ) as avg_round1_score,
  round(
    (avg(u.round2_score_percent) filter (
      where u.attempt_status in ('needs_review', 'completed')
    ))::numeric,
    2
  ) as avg_round2_score,
  round(
    (avg(u.final_score_percent) filter (
      where u.attempt_status in ('needs_review', 'completed')
    ))::numeric,
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

comment on function auto_grading.teacher_get_test_set_overview(
  uuid, text
)
is '교사용 시험 상세 요약 RPC. last_used_at = 마지막 assignment 발행(created_at) 시각. avg_round2_score = 교사 보정 전 학생 2차 누적 평균, avg_final_score = teacher_final 반영 최종 평균';
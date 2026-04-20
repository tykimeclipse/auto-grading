-- =========================================================
-- v2) teacher_list_assignments
-- 변경 목적
-- - 학생 학년 표준 필드를 grade_level로 통일
-- - student_grade 계산에서 grade_level 우선 사용
-- - 구 필드(grade, school_grade, student_grade)는 fallback으로만 유지
-- =========================================================

-- =========================================================
-- 1) 교사용 발행현황 / 결과 리스트 RPC v2
-- returns table 변경(reset_token 추가) 시 signature가 바뀌므로 DROP 필요
-- =========================================================
drop function if exists auto_grading.teacher_list_assignments(
  uuid, uuid, uuid, boolean, text, text, text, integer, integer
);

create or replace function auto_grading.teacher_list_assignments(
  p_course_id uuid default null,
  p_test_set_id uuid default null,
  p_student_id uuid default null,
  p_is_open boolean default null,
  p_purpose text default null,
  p_status text default null,
  p_search text default null,
  p_limit integer default 100,
  p_offset integer default 0
)
returns table(
  total_count bigint,
  assignment_id uuid,
  attempt_id uuid,
  student_id uuid,
  student_code text,
  student_name text,
  student_grade text,
  course_id uuid,
  course_name text,
  test_set_id uuid,
  test_title text,
  test_source text,
  purpose text,
  assigned_at timestamptz,
  is_open boolean,
  closed_at timestamptz,
  closed_reason text,
  status text,
  total_items integer,
  round1_correct_count integer,
  round1_score_percent numeric,
  round2_correct_count integer,
  round2_score_percent numeric,
  final_correct_count integer,
  final_score_percent numeric,
  has_teacher_final boolean,
  teacher_final_note text,
  teacher_final_updated_at timestamptz,
  last_activity_at timestamptz,
  reset_token uuid
)
language sql
security definer
set search_path to 'auto_grading', 'public'
as $function$
with latest_attempt as (
  select distinct on (at.assignment_id)
    at.assignment_id,
    at.id as attempt_id,
    at.total_items,
    at.first_correct_count,
    at.first_score_percent,
    at.final_correct_count,
    at.final_score_percent,
    at.teacher_final_correct_count,
    at.teacher_final_score_percent,
    at.teacher_final_note,
    at.teacher_final_updated_at,
    coalesce(to_jsonb(at)->>'status', 'not_started') as status,
    nullif(coalesce(to_jsonb(at)->>'created_at', ''), '')::timestamptz as attempt_created_at,
    nullif(
      coalesce(to_jsonb(at)->>'updated_at', to_jsonb(at)->>'created_at'),
      ''
    )::timestamptz as last_activity_at
  from auto_grading.attempts at
  where at.assignment_id is not null
  order by
    at.assignment_id,
    nullif(coalesce(to_jsonb(at)->>'created_at', ''), '')::timestamptz desc nulls last,
    at.id desc
),
base as (
  select
    a.id as assignment_id,
    la.attempt_id,
    a.student_id,
    coalesce(
      to_jsonb(s)->>'student_code',
      to_jsonb(s)->>'code'
    ) as student_code,
    coalesce(
      to_jsonb(s)->>'name',
      to_jsonb(s)->>'student_name',
      to_jsonb(s)->>'full_name'
    ) as student_name,
    nullif(
      coalesce(
        to_jsonb(s)->>'grade_level',
        to_jsonb(s)->>'grade',
        to_jsonb(s)->>'school_grade',
        to_jsonb(s)->>'student_grade',
        ''
      ),
      ''
    ) as student_grade,
    a.course_id,
    coalesce(
      to_jsonb(c)->>'name',
      to_jsonb(c)->>'course_name',
      to_jsonb(c)->>'title'
    ) as course_name,
    a.test_set_id,
    coalesce(
      to_jsonb(ts)->>'title',
      to_jsonb(ts)->>'name'
    ) as test_title,
    coalesce(
      to_jsonb(ts)->>'source',
      to_jsonb(ts)->>'source_name',
      to_jsonb(ts)->>'origin'
    ) as test_source,
    coalesce(a.purpose, '') as purpose,
    nullif(
      coalesce(
        to_jsonb(a)->>'created_at',
        to_jsonb(a)->>'issued_at',
        to_jsonb(a)->>'assigned_at'
      ),
      ''
    )::timestamptz as assigned_at,
    (nullif(coalesce(to_jsonb(a)->>'closed_at', ''), '') is null) as is_open,
    nullif(coalesce(to_jsonb(a)->>'closed_at', ''), '')::timestamptz as closed_at,
    nullif(coalesce(to_jsonb(a)->>'closed_reason', ''), '') as closed_reason,
    coalesce(la.status, 'not_started') as status,
    coalesce(
      la.total_items,
      case
        when coalesce(
          to_jsonb(ts)->>'total_items',
          to_jsonb(ts)->>'item_count',
          to_jsonb(ts)->>'question_count'
        ) ~ '^\d+$'
        then coalesce(
          to_jsonb(ts)->>'total_items',
          to_jsonb(ts)->>'item_count',
          to_jsonb(ts)->>'question_count'
        )::integer
        else null
      end
    ) as total_items,
    la.first_correct_count as round1_correct_count,
    la.first_score_percent as round1_score_percent,
    la.final_correct_count as round2_correct_count,
    la.final_score_percent as round2_score_percent,
    la.teacher_final_correct_count as final_correct_count,
    la.teacher_final_score_percent as final_score_percent,
    (la.teacher_final_correct_count is not null and la.teacher_final_score_percent is not null) as has_teacher_final,
    la.teacher_final_note,
    la.teacher_final_updated_at,
    coalesce(
      la.last_activity_at,
      nullif(
        coalesce(to_jsonb(a)->>'updated_at', to_jsonb(a)->>'created_at'),
        ''
      )::timestamptz
    ) as last_activity_at,
    a.reset_token
  from auto_grading.assignments a
  join auto_grading.students s
    on s.id = a.student_id
  join auto_grading.test_sets ts
    on ts.id = a.test_set_id
  left join auto_grading.courses c
    on c.id = a.course_id
  left join latest_attempt la
    on la.assignment_id = a.id
  where 1=1
    and (p_course_id is null or a.course_id = p_course_id)
    and (p_test_set_id is null or a.test_set_id = p_test_set_id)
    and (p_student_id is null or a.student_id = p_student_id)
    and (p_purpose is null or a.purpose = p_purpose)
    and (
      p_is_open is null
      or ((nullif(coalesce(to_jsonb(a)->>'closed_at', ''), '') is null) = p_is_open)
    )
    and (
      p_status is null
      or coalesce(la.status, 'not_started') = p_status
    )
    and (
      p_search is null
      or btrim(p_search) = ''
      or coalesce(to_jsonb(s)->>'student_code', to_jsonb(s)->>'code', '') ilike '%' || p_search || '%'
      or coalesce(to_jsonb(s)->>'name', to_jsonb(s)->>'student_name', to_jsonb(s)->>'full_name', '') ilike '%' || p_search || '%'
      or coalesce(to_jsonb(ts)->>'title', to_jsonb(ts)->>'name', '') ilike '%' || p_search || '%'
      or coalesce(to_jsonb(ts)->>'source', to_jsonb(ts)->>'source_name', to_jsonb(ts)->>'origin', '') ilike '%' || p_search || '%'
      or coalesce(to_jsonb(c)->>'name', to_jsonb(c)->>'course_name', to_jsonb(c)->>'title', '') ilike '%' || p_search || '%'
    )
)
select
  count(*) over() as total_count,
  b.assignment_id,
  b.attempt_id,
  b.student_id,
  b.student_code,
  b.student_name,
  b.student_grade,
  b.course_id,
  b.course_name,
  b.test_set_id,
  b.test_title,
  b.test_source,
  b.purpose,
  b.assigned_at,
  b.is_open,
  b.closed_at,
  b.closed_reason,
  b.status,
  b.total_items,
  b.round1_correct_count,
  b.round1_score_percent,
  b.round2_correct_count,
  b.round2_score_percent,
  b.final_correct_count,
  b.final_score_percent,
  b.has_teacher_final,
  b.teacher_final_note,
  b.teacher_final_updated_at,
  b.last_activity_at,
  b.reset_token
from base b
order by
  b.assigned_at desc nulls last,
  b.last_activity_at desc nulls last,
  b.assignment_id desc
limit greatest(coalesce(p_limit, 100), 1)
offset greatest(coalesce(p_offset, 0), 0);
$function$;

grant execute on function auto_grading.teacher_list_assignments(
  uuid, uuid, uuid, boolean, text, text, text, integer, integer
) to authenticated, anon, service_role;

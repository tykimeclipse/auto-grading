-- =========================================================
-- v2) teacher_list_students_for_assignment
-- 변경 목적
-- - 학생 학년 표준 필드를 grade_level로 통일
-- - student_grade 계산 및 p_grade 필터에서 grade_level 우선 사용
-- - 구 필드(grade, school_grade, student_grade)는 fallback으로만 유지
-- =========================================================

-- =========================================================
-- 0) student_courses에 service_type(academy/tutoring) 보강
--    이미 있으면 그대로 통과
-- =========================================================
alter table auto_grading.student_courses
  add column if not exists service_type text;
alter table auto_grading.student_courses
  drop constraint if exists student_courses_service_type_chk;
alter table auto_grading.student_courses
  add constraint student_courses_service_type_chk
  check (
    service_type is null
    or service_type in ('academy', 'tutoring')
  );
comment on column auto_grading.student_courses.service_type
is '수업 서비스 유형. academy=학원, tutoring=과외';
create index if not exists idx_student_courses_service_type
  on auto_grading.student_courses (service_type);

-- =========================================================
-- 1) 기존 함수 제거
-- =========================================================
drop function if exists auto_grading.teacher_list_students_for_assignment(
  uuid, uuid, text, text, text, boolean, boolean, integer, integer
);
drop function if exists auto_grading.teacher_list_students_for_assignment(
  uuid, uuid, text, text, text, text, boolean, boolean, integer, integer
);

-- =========================================================
-- 2) 학생 선택 영역용 RPC v2
--    - student_course_type 우선
--    - 학생 학년은 grade_level 우선
-- =========================================================
create function auto_grading.teacher_list_students_for_assignment(
  p_course_id uuid default null,
  p_test_set_id uuid default null,
  p_search text default null,
  p_grade text default null,
  p_service_type text default null,
  p_enrollment_type text default null,
  p_only_active boolean default true,
  p_only_not_assigned boolean default false,
  p_limit integer default 100,
  p_offset integer default 0
)
returns table(
  total_count bigint,
  student_id uuid,
  student_code text,
  student_name text,
  student_grade text,
  course_id uuid,
  course_name text,
  service_type text,
  student_course_type text,
  enrollment_type text,
  joined_at timestamptz,
  is_active boolean,
  active_assignment_count integer,
  course_active_assignment_count integer,
  same_test_assignment_id uuid,
  assigned_same_test_count integer,
  has_same_test_assignment boolean,
  has_same_test_open_assignment boolean,
  course_assignment_count integer,
  course_attempt_count integer,
  course_finalized_count integer,
  course_round1_avg numeric,
  course_round2_avg numeric,
  course_final_avg numeric,
  last_assignment_at timestamptz,
  last_activity_at timestamptz
)
language sql
security definer
set search_path to 'auto_grading', 'public'
as $function$
with sc_base as (
  select
    sc.student_id,
    sc.course_id,
    case
      when lower(coalesce(to_jsonb(sc)->>'is_active', '')) in ('true', 'false')
        then (to_jsonb(sc)->>'is_active')::boolean
      when nullif(
        coalesce(
          to_jsonb(sc)->>'closed_at',
          to_jsonb(sc)->>'ended_at',
          to_jsonb(sc)->>'end_at',
          to_jsonb(sc)->>'end_date',
          ''
        ),
        ''
      ) is null
        then true
      else false
    end as is_active,
    nullif(
      coalesce(
        to_jsonb(sc)->>'service_type',
        to_jsonb(sc)->>'teaching_mode',
        to_jsonb(sc)->>'service_kind',
        ''
      ),
      ''
    ) as service_type,
    nullif(
      case
        when coalesce(
          to_jsonb(sc)->>'student_course_type',
          to_jsonb(sc)->>'enrollment_type',
          to_jsonb(sc)->>'course_type',
          to_jsonb(sc)->>'study_type',
          to_jsonb(sc)->>'kind',
          ''
        ) = '정규' then '학원'
        else coalesce(
          to_jsonb(sc)->>'student_course_type',
          to_jsonb(sc)->>'enrollment_type',
          to_jsonb(sc)->>'course_type',
          to_jsonb(sc)->>'study_type',
          to_jsonb(sc)->>'kind',
          ''
        )
      end,
      ''
    ) as student_course_type,
    nullif(
      coalesce(
        to_jsonb(sc)->>'created_at',
        to_jsonb(sc)->>'joined_at',
        to_jsonb(sc)->>'started_at',
        to_jsonb(sc)->>'start_at',
        to_jsonb(sc)->>'start_date',
        ''
      ),
      ''
    )::timestamptz as joined_at,
    nullif(
      coalesce(
        to_jsonb(sc)->>'updated_at',
        to_jsonb(sc)->>'created_at',
        to_jsonb(sc)->>'joined_at',
        to_jsonb(sc)->>'started_at',
        ''
      ),
      ''
    )::timestamptz as row_ts
  from auto_grading.student_courses sc
),
sc_scoped as (
  select
    scb.*,
    row_number() over (
      partition by scb.student_id, scb.course_id
      order by
        case when scb.is_active then 0 else 1 end,
        scb.joined_at desc nulls last,
        scb.row_ts desc nulls last,
        scb.service_type asc nulls last,
        scb.student_course_type asc nulls last
    ) as rn
  from sc_base scb
  where (not p_only_active or scb.is_active)
    and (p_course_id is null or scb.course_id = p_course_id)
    and (p_service_type is null or scb.service_type = p_service_type)
    and (p_enrollment_type is null or scb.student_course_type = p_enrollment_type)
),
student_scope as (
  select
    scs.student_id,
    scs.course_id,
    scs.is_active,
    scs.service_type,
    scs.student_course_type,
    scs.joined_at
  from sc_scoped scs
  where scs.rn = 1

  union all

  select
    s.id as student_id,
    null::uuid as course_id,
    true as is_active,
    null::text as service_type,
    null::text as student_course_type,
    null::timestamptz as joined_at
  from auto_grading.students s
  where p_course_id is null
    and p_service_type is null
    and p_enrollment_type is null
    and not exists (
      select 1
      from auto_grading.student_courses sc
      where sc.student_id = s.id
    )
),
latest_attempt as (
  select distinct on (at.assignment_id)
    at.assignment_id,
    at.id as attempt_id,
    at.first_score_percent,
    at.final_score_percent,
    at.teacher_final_score_percent,
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
course_stats as (
  select
    a.student_id,
    a.course_id,
    count(*)::integer as course_assignment_count,
    count(*) filter (where la.attempt_id is not null)::integer as course_attempt_count,
    count(*) filter (where la.teacher_final_score_percent is not null)::integer as course_finalized_count,
    round(avg(la.first_score_percent)::numeric, 2) as course_round1_avg,
    round(avg(la.final_score_percent)::numeric, 2) as course_round2_avg,
    round(avg(coalesce(la.teacher_final_score_percent, la.final_score_percent))::numeric, 2) as course_final_avg,
    max(
      nullif(
        coalesce(
          to_jsonb(a)->>'created_at',
          to_jsonb(a)->>'assigned_at',
          to_jsonb(a)->>'issued_at',
          ''
        ),
        ''
      )::timestamptz
    ) as last_assignment_at,
    max(
      coalesce(
        la.last_activity_at,
        nullif(
          coalesce(to_jsonb(a)->>'updated_at', to_jsonb(a)->>'created_at'),
          ''
        )::timestamptz
      )
    ) as last_activity_at
  from auto_grading.assignments a
  left join latest_attempt la
    on la.assignment_id = a.id
  group by
    a.student_id,
    a.course_id
),
student_active_counts as (
  select
    a.student_id,
    count(*) filter (
      where nullif(coalesce(to_jsonb(a)->>'closed_at', ''), '') is null
    )::integer as active_assignment_count
  from auto_grading.assignments a
  group by a.student_id
),
course_active_counts as (
  select
    a.student_id,
    a.course_id,
    count(*) filter (
      where nullif(coalesce(to_jsonb(a)->>'closed_at', ''), '') is null
    )::integer as course_active_assignment_count
  from auto_grading.assignments a
  group by
    a.student_id,
    a.course_id
),
same_test_ranked as (
  select
    a.student_id,
    a.id as assignment_id,
    row_number() over (
      partition by a.student_id
      order by
        nullif(
          coalesce(
            to_jsonb(a)->>'created_at',
            to_jsonb(a)->>'assigned_at',
            to_jsonb(a)->>'issued_at',
            ''
          ),
          ''
        )::timestamptz desc nulls last,
        a.id desc
    ) as rn,
    count(*) over (
      partition by a.student_id
    )::integer as assigned_same_test_count,
    count(*) filter (
      where nullif(coalesce(to_jsonb(a)->>'closed_at', ''), '') is null
    ) over (
      partition by a.student_id
    )::integer as assigned_same_test_open_count
  from auto_grading.assignments a
  where p_test_set_id is not null
    and a.test_set_id = p_test_set_id
),
same_test as (
  select
    student_id,
    assignment_id as same_test_assignment_id,
    assigned_same_test_count,
    assigned_same_test_open_count
  from same_test_ranked
  where rn = 1
),
base as (
  select
    ss.student_id,
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
    ss.course_id,
    coalesce(
      to_jsonb(c)->>'name',
      to_jsonb(c)->>'course_name',
      to_jsonb(c)->>'title'
    ) as course_name,
    ss.service_type,
    ss.student_course_type,
    ss.student_course_type as enrollment_type,
    ss.joined_at,
    ss.is_active,
    coalesce(sac.active_assignment_count, 0) as active_assignment_count,
    coalesce(cac.course_active_assignment_count, 0) as course_active_assignment_count,
    st.same_test_assignment_id,
    coalesce(st.assigned_same_test_count, 0) as assigned_same_test_count,
    (coalesce(st.assigned_same_test_count, 0) > 0) as has_same_test_assignment,
    (coalesce(st.assigned_same_test_open_count, 0) > 0) as has_same_test_open_assignment,
    coalesce(cs.course_assignment_count, 0) as course_assignment_count,
    coalesce(cs.course_attempt_count, 0) as course_attempt_count,
    coalesce(cs.course_finalized_count, 0) as course_finalized_count,
    cs.course_round1_avg,
    cs.course_round2_avg,
    cs.course_final_avg,
    cs.last_assignment_at,
    cs.last_activity_at
  from student_scope ss
  join auto_grading.students s
    on s.id = ss.student_id
  left join auto_grading.courses c
    on c.id = ss.course_id
  left join student_active_counts sac
    on sac.student_id = ss.student_id
  left join course_active_counts cac
    on cac.student_id = ss.student_id
   and cac.course_id is not distinct from ss.course_id
  left join same_test st
    on st.student_id = ss.student_id
  left join course_stats cs
    on cs.student_id = ss.student_id
   and cs.course_id is not distinct from ss.course_id
  where 1=1
    and (
      p_grade is null
      or nullif(
        coalesce(
          to_jsonb(s)->>'grade_level',
          to_jsonb(s)->>'grade',
          to_jsonb(s)->>'school_grade',
          to_jsonb(s)->>'student_grade',
          ''
        ),
        ''
      ) = p_grade
    )
    and (
      p_search is null
      or btrim(p_search) = ''
      or coalesce(to_jsonb(s)->>'student_code', to_jsonb(s)->>'code', '') ilike '%' || p_search || '%'
      or coalesce(to_jsonb(s)->>'name', to_jsonb(s)->>'student_name', to_jsonb(s)->>'full_name', '') ilike '%' || p_search || '%'
      or coalesce(to_jsonb(c)->>'name', to_jsonb(c)->>'course_name', to_jsonb(c)->>'title', '') ilike '%' || p_search || '%'
    )
    and (
      not p_only_not_assigned
      or coalesce(st.assigned_same_test_count, 0) = 0
    )
)
select
  count(*) over() as total_count,
  b.student_id,
  b.student_code,
  b.student_name,
  b.student_grade,
  b.course_id,
  b.course_name,
  b.service_type,
  b.student_course_type,
  b.enrollment_type,
  b.joined_at,
  b.is_active,
  b.active_assignment_count,
  b.course_active_assignment_count,
  b.same_test_assignment_id,
  b.assigned_same_test_count,
  b.has_same_test_assignment,
  b.has_same_test_open_assignment,
  b.course_assignment_count,
  b.course_attempt_count,
  b.course_finalized_count,
  b.course_round1_avg,
  b.course_round2_avg,
  b.course_final_avg,
  b.last_assignment_at,
  b.last_activity_at
from base b
order by
  case
    when p_test_set_id is not null and b.has_same_test_assignment then 1
    else 0
  end asc,
  b.student_grade asc nulls last,
  b.student_name asc,
  b.student_id asc
limit greatest(coalesce(p_limit, 100), 1)
offset greatest(coalesce(p_offset, 0), 0);
$function$;

grant execute on function auto_grading.teacher_list_students_for_assignment(
  uuid, uuid, text, text, text, text, boolean, boolean, integer, integer
) to authenticated;

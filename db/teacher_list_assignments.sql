-- =========================================================
-- 교사용 RPC 수정본 전체
-- 반영 내용
-- 1) latest attempt 선정 기준: updated_at -> created_at
-- 2) teacher final 저장 조건:
--    - current/latest attempt 에만 허용
--    - completed / needs_review 상태에서만 허용
-- =========================================================

-- =========================================================
-- 1) 교사용 발행현황 / 결과 리스트 RPC
--    - assignment 1개당 "최신 attempt 1개"만 보여줌
--    - 최신 선정 기준은 created_at desc, id desc
--    - 교사용이므로
--      1차 = first_*
--      2차 = final_*
--      최종 = teacher_final_*
-- =========================================================
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
  last_activity_at timestamptz
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
        to_jsonb(s)->>'grade',
        to_jsonb(s)->>'school_grade',
        to_jsonb(s)->>'student_grade'
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
    ) as last_activity_at
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
  b.last_activity_at
from base b
order by
  b.assigned_at desc nulls last,
  b.last_activity_at desc nulls last,
  b.assignment_id desc
limit greatest(coalesce(p_limit, 100), 1)
offset greatest(coalesce(p_offset, 0), 0);
$function$;


-- =========================================================
-- 2) 교사 최종 점수 저장 / 수정 / 삭제(clear)
--    - current/latest attempt 에만 허용
--    - completed / needs_review 상태에서만 저장 허용
--    - 입력은 correct_count 기준
--    - score_percent는 total_items로 자동 계산
-- =========================================================
create or replace function auto_grading.teacher_save_final_score(
  p_attempt_id uuid,
  p_teacher_final_correct_count integer default null,
  p_teacher_final_note text default null,
  p_clear boolean default false
)
returns jsonb
language plpgsql
security definer
set search_path to 'auto_grading', 'public'
as $function$
declare
  v_total_items integer;
  v_status text;
  v_assignment_id uuid;
  v_latest_attempt_id uuid;
  v_result auto_grading.attempts%rowtype;
  v_score numeric(6,2);
begin
  perform auto_grading.assert_admin();
  if p_attempt_id is null then
    raise exception 'p_attempt_id is required';
  end if;

  select
    a.total_items,
    coalesce(to_jsonb(a)->>'status', ''),
    a.assignment_id
  into
    v_total_items,
    v_status,
    v_assignment_id
  from auto_grading.attempts a
  where a.id = p_attempt_id
  for update;

  if not found then
    raise exception 'attempt not found: %', p_attempt_id;
  end if;

  if v_assignment_id is null then
    raise exception 'assignment_id is missing for attempt %', p_attempt_id;
  end if;

  select x.id
    into v_latest_attempt_id
  from auto_grading.attempts x
  where x.assignment_id = v_assignment_id
  order by
    nullif(coalesce(to_jsonb(x)->>'created_at', ''), '')::timestamptz desc nulls last,
    x.id desc
  limit 1;

  if v_latest_attempt_id is null then
    raise exception 'latest attempt not found for assignment %', v_assignment_id;
  end if;

  if v_latest_attempt_id <> p_attempt_id then
    raise exception
      'teacher final can be saved only on current/latest attempt. latest_attempt_id=%, requested_attempt_id=%',
      v_latest_attempt_id,
      p_attempt_id;
  end if;

  if p_clear then
    update auto_grading.attempts
       set teacher_final_correct_count = null,
           teacher_final_score_percent = null,
           teacher_final_note = null,
           teacher_final_updated_at = now(),
           teacher_final_updated_by = auth.uid(),
           updated_at = now()
     where id = p_attempt_id
     returning * into v_result;

    return jsonb_build_object(
      'ok', true,
      'attempt_id', v_result.id,
      'assignment_id', v_result.assignment_id,
      'teacher_final_correct_count', v_result.teacher_final_correct_count,
      'teacher_final_score_percent', v_result.teacher_final_score_percent,
      'teacher_final_note', v_result.teacher_final_note,
      'teacher_final_updated_at', v_result.teacher_final_updated_at,
      'teacher_final_updated_by', v_result.teacher_final_updated_by,
      'message', 'teacher final score cleared'
    );
  end if;

  if v_status not in ('completed', 'needs_review') then
    raise exception
      'teacher final can be saved only for completed/needs_review attempts. current status=%',
      v_status;
  end if;

  if p_teacher_final_correct_count is null then
    raise exception 'p_teacher_final_correct_count is required when p_clear = false';
  end if;

  if v_total_items is null or v_total_items <= 0 then
    raise exception 'total_items is missing for attempt %', p_attempt_id;
  end if;

  if p_teacher_final_correct_count < 0 or p_teacher_final_correct_count > v_total_items then
    raise exception
      'teacher_final_correct_count must be between 0 and % (attempt_id=%)',
      v_total_items,
      p_attempt_id;
  end if;

  v_score := round((p_teacher_final_correct_count::numeric * 100.0) / v_total_items, 2);

  update auto_grading.attempts
     set teacher_final_correct_count = p_teacher_final_correct_count,
         teacher_final_score_percent = v_score,
         teacher_final_note = nullif(btrim(p_teacher_final_note), ''),
         teacher_final_updated_at = now(),
         teacher_final_updated_by = auth.uid(),
         updated_at = now()
   where id = p_attempt_id
   returning * into v_result;

  return jsonb_build_object(
    'ok', true,
    'attempt_id', v_result.id,
    'assignment_id', v_result.assignment_id,
    'total_items', v_result.total_items,
    'round1_correct_count', v_result.first_correct_count,
    'round1_score_percent', v_result.first_score_percent,
    'round2_correct_count', v_result.final_correct_count,
    'round2_score_percent', v_result.final_score_percent,
    'teacher_final_correct_count', v_result.teacher_final_correct_count,
    'teacher_final_score_percent', v_result.teacher_final_score_percent,
    'teacher_final_note', v_result.teacher_final_note,
    'teacher_final_updated_at', v_result.teacher_final_updated_at,
    'teacher_final_updated_by', v_result.teacher_final_updated_by,
    'status', to_jsonb(v_result)->>'status',
    'message', 'teacher final score saved'
  );

exception
  when others then
    return jsonb_build_object(
      'ok', false,
      'attempt_id', p_attempt_id,
      'error', sqlerrm
    );
end;
$function$;


-- =========================================================
-- 3) assignment 열림 / 닫힘 토글
-- =========================================================
create or replace function auto_grading.teacher_set_assignment_open_state(
  p_assignment_id uuid,
  p_is_open boolean,
  p_closed_reason text default null
)
returns jsonb
language plpgsql
security definer
set search_path to 'auto_grading', 'public'
as $function$
declare
  v_result auto_grading.assignments%rowtype;
begin
  perform auto_grading.assert_admin();
  if p_assignment_id is null then
    raise exception 'p_assignment_id is required';
  end if;

  if p_is_open is null then
    raise exception 'p_is_open is required';
  end if;

  update auto_grading.assignments
     set closed_at = case when p_is_open then null else now() end,
         closed_reason = case when p_is_open then null else nullif(btrim(p_closed_reason), '') end,
         updated_at = now()
   where id = p_assignment_id
   returning * into v_result;

  if not found then
    raise exception 'assignment not found: %', p_assignment_id;
  end if;

  return jsonb_build_object(
    'ok', true,
    'assignment_id', v_result.id,
    'is_open', p_is_open,
    'closed_at', v_result.closed_at,
    'closed_reason', v_result.closed_reason,
    'message', case when p_is_open then 'assignment opened' else 'assignment closed' end
  );

exception
  when others then
    return jsonb_build_object(
      'ok', false,
      'assignment_id', p_assignment_id,
      'error', sqlerrm
    );
end;
$function$;


-- =========================================================
-- 실행 권한
-- =========================================================
grant execute on function auto_grading.teacher_list_assignments(
  uuid, uuid, uuid, boolean, text, text, text, integer, integer
) to authenticated;

grant execute on function auto_grading.teacher_save_final_score(
  uuid, integer, text, boolean
) to authenticated;

grant execute on function auto_grading.teacher_set_assignment_open_state(
  uuid, boolean, text
) to authenticated;


-- =========================================================
-- 권장 인덱스
-- latest attempt / current attempt 조회 성능용
-- 이미 있으면 생략 가능
-- =========================================================
create index if not exists idx_attempts_assignment_created_id_desc
  on auto_grading.attempts (assignment_id, created_at desc, id desc);

create index if not exists idx_assignments_course_test_student
  on auto_grading.assignments (course_id, test_set_id, student_id);
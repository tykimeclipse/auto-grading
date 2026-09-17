begin;

create or replace function auto_grading.teacher_issue_assignments(
  p_test_set_id uuid,
  p_student_ids uuid[],
  p_course_id uuid default null,
  p_purpose text default null,
  p_reopen_existing boolean default false
)
returns jsonb
language plpgsql
security definer
set search_path to 'auto_grading', 'public'
as $function$
declare
  v_requested_count integer := 0;
  v_processed_count integer := 0;
  v_created_count integer := 0;
  v_reopened_count integer := 0;
  v_skipped_existing_open_count integer := 0;
  v_skipped_existing_closed_count integer := 0;
  v_skipped_existing_count integer := 0;
  v_skipped_course_unassigned_count integer := 0;
  v_skipped_other_course_count integer := 0;
  v_skipped_not_found_count integer := 0;
  v_skipped_not_in_course_count integer := 0;
  v_items jsonb := '[]'::jsonb;
  v_course_is_active boolean;
begin
  perform auto_grading.assert_admin();
  if p_test_set_id is null then
    raise exception 'p_test_set_id is required';
  end if;

  if coalesce(cardinality(p_student_ids), 0) = 0 then
    raise exception 'p_student_ids is required';
  end if;

  if p_course_id is null then
    raise exception 'COURSE_REQUIRED' using errcode = 'P0001';
  end if;

  perform 1
  from auto_grading.test_sets ts
  where ts.id = p_test_set_id;

  if not found then
    raise exception 'test_set not found: %', p_test_set_id;
  end if;

  select c.is_active
    into v_course_is_active
  from auto_grading.courses c
  where c.id = p_course_id
  for share;

  if not found then
    raise exception 'course not found: %', p_course_id;
  end if;

  if not coalesce(v_course_is_active, false) then
    raise exception 'COURSE_INACTIVE' using errcode = 'P0001';
  end if;

  drop table if exists pg_temp.tmp_teacher_issue_input;
  drop table if exists pg_temp.tmp_teacher_issue_result;

  create temporary table pg_temp.tmp_teacher_issue_input (
    ord bigint,
    student_id uuid primary key,
    student_exists boolean default false,
    in_course boolean default false,
    existing_assignment_id uuid,
    existing_course_id uuid,
    existing_closed_at timestamptz
  ) on commit drop;

  create temporary table pg_temp.tmp_teacher_issue_result (
    ord bigint,
    student_id uuid,
    assignment_id uuid,
    course_id uuid,
    purpose text,
    action text
  ) on commit drop;

  insert into pg_temp.tmp_teacher_issue_input (
    ord,
    student_id
  )
  select
    min(u.ord) as ord,
    u.student_id
  from unnest(p_student_ids) with ordinality as u(student_id, ord)
  where u.student_id is not null
  group by u.student_id;

  select count(*)
    into v_requested_count
  from pg_temp.tmp_teacher_issue_input;

  if v_requested_count = 0 then
    raise exception 'p_student_ids must contain at least one non-null uuid';
  end if;

  update pg_temp.tmp_teacher_issue_input i
     set student_exists = true
    from auto_grading.students s
   where s.id = i.student_id;

  update pg_temp.tmp_teacher_issue_input i
     set in_course = true
    from auto_grading.student_courses sc
   where sc.student_id = i.student_id
     and sc.course_id = p_course_id
     and coalesce(sc.is_active, sc.ended_at is null);

  update pg_temp.tmp_teacher_issue_input i
     set existing_assignment_id = e.assignment_id,
         existing_course_id = e.course_id,
         existing_closed_at = e.closed_at
    from (
      select distinct on (a.student_id)
        a.student_id,
        a.id as assignment_id,
        a.course_id,
        a.closed_at
      from auto_grading.assignments a
      join pg_temp.tmp_teacher_issue_input t
        on t.student_id = a.student_id
      where a.test_set_id = p_test_set_id
      order by
        a.student_id,
        a.created_at desc nulls last,
        a.id desc
    ) e
   where e.student_id = i.student_id;

  with inserted as (
    insert into auto_grading.assignments (
      student_id,
      test_set_id,
      course_id,
      purpose,
      updated_at
    )
    select
      i.student_id,
      p_test_set_id,
      p_course_id,
      p_purpose,
      now()
    from pg_temp.tmp_teacher_issue_input i
    where i.student_exists
      and i.in_course
      and i.existing_assignment_id is null
    on conflict do nothing
    returning
      student_id,
      id as assignment_id
  )
  insert into pg_temp.tmp_teacher_issue_result (
    ord,
    student_id,
    assignment_id,
    course_id,
    purpose,
    action
  )
  select
    i.ord,
    i.student_id,
    ins.assignment_id,
    p_course_id,
    p_purpose,
    'created'
  from pg_temp.tmp_teacher_issue_input i
  join inserted ins
    on ins.student_id = i.student_id;

  update pg_temp.tmp_teacher_issue_input i
     set existing_assignment_id = e.assignment_id,
         existing_course_id = e.course_id,
         existing_closed_at = e.closed_at
    from (
      select distinct on (a.student_id)
        a.student_id,
        a.id as assignment_id,
        a.course_id,
        a.closed_at
      from auto_grading.assignments a
      join pg_temp.tmp_teacher_issue_input t
        on t.student_id = a.student_id
      left join pg_temp.tmp_teacher_issue_result r
        on r.student_id = t.student_id
      where a.test_set_id = p_test_set_id
        and t.existing_assignment_id is null
        and r.student_id is null
      order by
        a.student_id,
        a.created_at desc nulls last,
        a.id desc
    ) e
   where e.student_id = i.student_id;

  if p_reopen_existing then
    with reopened as (
      update auto_grading.assignments a
         set purpose = coalesce(p_purpose, a.purpose),
             closed_at = null,
             closed_reason = null,
             updated_at = now()
        from pg_temp.tmp_teacher_issue_input i
       where a.id = i.existing_assignment_id
         and i.student_exists
         and i.in_course
         and i.existing_assignment_id is not null
         and i.existing_course_id = p_course_id
         and i.existing_closed_at is not null
         and not exists (
           select 1
           from pg_temp.tmp_teacher_issue_result r
           where r.student_id = i.student_id
         )
      returning
        a.student_id,
        a.id as assignment_id
    )
    insert into pg_temp.tmp_teacher_issue_result (
      ord,
      student_id,
      assignment_id,
      course_id,
      purpose,
      action
    )
    select
      i.ord,
      i.student_id,
      r.assignment_id,
      p_course_id,
      p_purpose,
      'reopened_existing'
    from pg_temp.tmp_teacher_issue_input i
    join reopened r
      on r.student_id = i.student_id;
  end if;

  insert into pg_temp.tmp_teacher_issue_result (
    ord,
    student_id,
    assignment_id,
    course_id,
    purpose,
    action
  )
  select
    i.ord,
    i.student_id,
    null,
    p_course_id,
    p_purpose,
    'skipped_student_not_found'
  from pg_temp.tmp_teacher_issue_input i
  where not i.student_exists
    and not exists (
      select 1
      from pg_temp.tmp_teacher_issue_result r
      where r.student_id = i.student_id
    );

  insert into pg_temp.tmp_teacher_issue_result (
    ord,
    student_id,
    assignment_id,
    course_id,
    purpose,
    action
  )
  select
    i.ord,
    i.student_id,
    null,
    p_course_id,
    p_purpose,
    'skipped_not_in_course'
  from pg_temp.tmp_teacher_issue_input i
  where i.student_exists
    and not i.in_course
    and not exists (
      select 1
      from pg_temp.tmp_teacher_issue_result r
      where r.student_id = i.student_id
    );

  -- legacy 미지정 assignment는 과거 attempt 유무와 관계없이 자동 태깅하지 않는다.
  -- 신규 배포 후에는 모든 생성 경로에서 course_id가 필수이므로 새로 늘어나지 않는다.
  insert into pg_temp.tmp_teacher_issue_result (
    ord,
    student_id,
    assignment_id,
    course_id,
    purpose,
    action
  )
  select
    i.ord,
    i.student_id,
    i.existing_assignment_id,
    null,
    p_purpose,
    'skipped_course_unassigned'
  from pg_temp.tmp_teacher_issue_input i
  where i.student_exists
    and i.in_course
    and i.existing_assignment_id is not null
    and i.existing_course_id is null
    and not exists (
      select 1
      from pg_temp.tmp_teacher_issue_result r
      where r.student_id = i.student_id
    );

  insert into pg_temp.tmp_teacher_issue_result (
    ord,
    student_id,
    assignment_id,
    course_id,
    purpose,
    action
  )
  select
    i.ord,
    i.student_id,
    i.existing_assignment_id,
    i.existing_course_id,
    p_purpose,
    'skipped_other_course'
  from pg_temp.tmp_teacher_issue_input i
  where i.student_exists
    and i.in_course
    and i.existing_assignment_id is not null
    and i.existing_course_id is not null
    and i.existing_course_id <> p_course_id
    and not exists (
      select 1
      from pg_temp.tmp_teacher_issue_result r
      where r.student_id = i.student_id
    );

  insert into pg_temp.tmp_teacher_issue_result (
    ord,
    student_id,
    assignment_id,
    course_id,
    purpose,
    action
  )
  select
    i.ord,
    i.student_id,
    i.existing_assignment_id,
    p_course_id,
    p_purpose,
    'skipped_existing_open'
  from pg_temp.tmp_teacher_issue_input i
  where i.student_exists
    and i.in_course
    and i.existing_assignment_id is not null
    and i.existing_course_id = p_course_id
    and i.existing_closed_at is null
    and not exists (
      select 1
      from pg_temp.tmp_teacher_issue_result r
      where r.student_id = i.student_id
    );

  insert into pg_temp.tmp_teacher_issue_result (
    ord,
    student_id,
    assignment_id,
    course_id,
    purpose,
    action
  )
  select
    i.ord,
    i.student_id,
    i.existing_assignment_id,
    p_course_id,
    p_purpose,
    'skipped_existing_closed'
  from pg_temp.tmp_teacher_issue_input i
  where i.student_exists
    and i.in_course
    and i.existing_assignment_id is not null
    and i.existing_course_id = p_course_id
    and i.existing_closed_at is not null
    and not p_reopen_existing
    and not exists (
      select 1
      from pg_temp.tmp_teacher_issue_result r
      where r.student_id = i.student_id
    );

  select
    count(*)::integer,
    count(*) filter (where action = 'created')::integer,
    count(*) filter (where action = 'reopened_existing')::integer,
    count(*) filter (where action = 'skipped_existing_open')::integer,
    count(*) filter (where action = 'skipped_existing_closed')::integer,
    count(*) filter (
      where action in (
        'skipped_existing_open',
        'skipped_existing_closed',
        'skipped_course_unassigned',
        'skipped_other_course'
      )
    )::integer,
    count(*) filter (where action = 'skipped_course_unassigned')::integer,
    count(*) filter (where action = 'skipped_other_course')::integer,
    count(*) filter (where action = 'skipped_student_not_found')::integer,
    count(*) filter (where action = 'skipped_not_in_course')::integer,
    coalesce(
      jsonb_agg(
        jsonb_strip_nulls(
          jsonb_build_object(
            'student_id', student_id,
            'assignment_id', assignment_id,
            'course_id', course_id,
            'purpose', purpose,
            'action', action,
            'message', case
              when action = 'skipped_course_unassigned' then
                '강좌 미지정 과거 과제입니다. 이 건은 발행되지 않았습니다. 과거 데이터이므로 개발자에게 일회성 보정을 요청하십시오.'
              else null
            end
          )
        )
        order by ord
      ),
      '[]'::jsonb
    )
  into
    v_processed_count,
    v_created_count,
    v_reopened_count,
    v_skipped_existing_open_count,
    v_skipped_existing_closed_count,
    v_skipped_existing_count,
    v_skipped_course_unassigned_count,
    v_skipped_other_course_count,
    v_skipped_not_found_count,
    v_skipped_not_in_course_count,
    v_items
  from pg_temp.tmp_teacher_issue_result;

  if v_processed_count <> v_requested_count then
    raise exception
      'internal mismatch: processed_count(%) <> requested_count(%)',
      v_processed_count,
      v_requested_count;
  end if;

  return jsonb_build_object(
    'ok', true,
    'test_set_id', p_test_set_id,
    'course_id', p_course_id,
    'purpose', p_purpose,
    'requested_count', v_requested_count,
    'processed_count', v_processed_count,
    'created_count', v_created_count,
    'reopened_count', v_reopened_count,
    'skipped_existing_open_count', v_skipped_existing_open_count,
    'skipped_existing_closed_count', v_skipped_existing_closed_count,
    'skipped_existing_count', v_skipped_existing_count,
    'skipped_course_unassigned_count', v_skipped_course_unassigned_count,
    'skipped_course_unassigned_message', case
      when v_skipped_course_unassigned_count > 0 then format(
        '강좌 미지정 과거 과제 %s건 — 이 건은 발행되지 않았습니다. 과거 데이터이므로 개발자에게 일회성 보정을 요청하십시오.',
        v_skipped_course_unassigned_count
      )
      else null
    end,
    'skipped_other_course_count', v_skipped_other_course_count,
    'skipped_student_not_found_count', v_skipped_not_found_count,
    'skipped_not_in_course_count', v_skipped_not_in_course_count,
    'items', v_items
  );
end;
$function$;

revoke execute on function auto_grading.teacher_issue_assignments(
  uuid, uuid[], uuid, text, boolean
) from public, anon;
grant execute on function auto_grading.teacher_issue_assignments(
  uuid, uuid[], uuid, text, boolean
) to authenticated, service_role;

commit;

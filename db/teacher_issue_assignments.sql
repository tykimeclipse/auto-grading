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
  v_rec record;
  v_student_id uuid;
  v_existing_assignment_id uuid;
  v_existing_closed_at timestamptz;
  v_created_assignment_id uuid;

  v_requested_count integer := 0;
  v_processed_count integer := 0;
  v_created_count integer := 0;
  v_reopened_count integer := 0;
  v_skipped_existing_count integer := 0;
  v_skipped_not_found_count integer := 0;
  v_skipped_not_in_course_count integer := 0;

  v_items jsonb := '[]'::jsonb;
begin
  if p_test_set_id is null then
    raise exception 'p_test_set_id is required';
  end if;

  if coalesce(cardinality(p_student_ids), 0) = 0 then
    raise exception 'p_student_ids is required';
  end if;

  perform 1
  from auto_grading.test_sets ts
  where ts.id = p_test_set_id;

  if not found then
    raise exception 'test_set not found: %', p_test_set_id;
  end if;

  if p_course_id is not null then
    perform 1
    from auto_grading.courses c
    where c.id = p_course_id;

    if not found then
      raise exception 'course not found: %', p_course_id;
    end if;
  end if;

  select count(*)
    into v_requested_count
  from (
    select distinct u.student_id
    from unnest(p_student_ids) as u(student_id)
    where u.student_id is not null
  ) q;

  for v_rec in
    select x.student_id
    from (
      select
        u.student_id,
        min(u.ord) as ord
      from unnest(p_student_ids) with ordinality as u(student_id, ord)
      where u.student_id is not null
      group by u.student_id
    ) x
    order by x.ord
  loop
    v_student_id := v_rec.student_id;
    v_processed_count := v_processed_count + 1;

    perform 1
    from auto_grading.students s
    where s.id = v_student_id;

    if not found then
      v_skipped_not_found_count := v_skipped_not_found_count + 1;
      v_items := v_items || jsonb_build_array(
        jsonb_build_object(
          'student_id', v_student_id,
          'action', 'skipped_student_not_found'
        )
      );
      continue;
    end if;

    if p_course_id is not null then
      perform 1
      from auto_grading.v_student_courses_normalized v
      where v.student_id = v_student_id
        and v.course_id = p_course_id
        and v.is_active;

      if not found then
        v_skipped_not_in_course_count := v_skipped_not_in_course_count + 1;
        v_items := v_items || jsonb_build_array(
          jsonb_build_object(
            'student_id', v_student_id,
            'course_id', p_course_id,
            'action', 'skipped_not_in_course'
          )
        );
        continue;
      end if;
    end if;

    perform pg_advisory_xact_lock(
      hashtext(v_student_id::text),
      hashtext(p_test_set_id::text)
    );

    v_existing_assignment_id := null;
    v_existing_closed_at := null;

    select
      a.id,
      a.closed_at
    into
      v_existing_assignment_id,
      v_existing_closed_at
    from auto_grading.assignments a
    where a.student_id = v_student_id
      and a.test_set_id = p_test_set_id
    order by
      a.created_at desc nulls last,
      a.id desc
    limit 1
    for update;

    if v_existing_assignment_id is not null then
      if p_reopen_existing then
        update auto_grading.assignments a
           set course_id = coalesce(p_course_id, a.course_id),
               purpose = coalesce(p_purpose, a.purpose),
               closed_at = null,
               closed_reason = null,
               updated_at = now()
         where a.id = v_existing_assignment_id;

        v_reopened_count := v_reopened_count + 1;
        v_items := v_items || jsonb_build_array(
          jsonb_build_object(
            'student_id', v_student_id,
            'assignment_id', v_existing_assignment_id,
            'course_id', p_course_id,
            'purpose', p_purpose,
            'action',
              case
                when v_existing_closed_at is null then 'updated_existing'
                else 'reopened_existing'
              end
          )
        );
      else
        v_skipped_existing_count := v_skipped_existing_count + 1;
        v_items := v_items || jsonb_build_array(
          jsonb_build_object(
            'student_id', v_student_id,
            'assignment_id', v_existing_assignment_id,
            'action', 'skipped_existing'
          )
        );
      end if;

      continue;
    end if;

    insert into auto_grading.assignments (
      student_id,
      test_set_id,
      course_id,
      purpose,
      updated_at
    )
    values (
      v_student_id,
      p_test_set_id,
      p_course_id,
      p_purpose,
      now()
    )
    returning id
      into v_created_assignment_id;

    v_created_count := v_created_count + 1;
    v_items := v_items || jsonb_build_array(
      jsonb_build_object(
        'student_id', v_student_id,
        'assignment_id', v_created_assignment_id,
        'course_id', p_course_id,
        'purpose', p_purpose,
        'action', 'created'
      )
    );
  end loop;

  return jsonb_build_object(
    'ok', true,
    'test_set_id', p_test_set_id,
    'course_id', p_course_id,
    'purpose', p_purpose,
    'requested_count', v_requested_count,
    'processed_count', v_processed_count,
    'created_count', v_created_count,
    'reopened_count', v_reopened_count,
    'skipped_existing_count', v_skipped_existing_count,
    'skipped_student_not_found_count', v_skipped_not_found_count,
    'skipped_not_in_course_count', v_skipped_not_in_course_count,
    'items', v_items
  );
end;
$function$;

grant execute on function auto_grading.teacher_issue_assignments(
  uuid, uuid[], uuid, text, boolean
) to authenticated;
create or replace function auto_grading.teacher_update_test_set_metadata(
  p_test_set_id uuid,
  p_title text,
  p_curriculum_version text,
  p_grade_level text,
  p_subject text,
  p_unit_code text,
  p_source_category text,
  p_source_name text
)
returns jsonb
language plpgsql
security definer
set search_path to 'auto_grading', 'public'
as $function$
declare
  v_row auto_grading.test_sets%rowtype;
begin
  if p_test_set_id is null then
    raise exception 'p_test_set_id is required';
  end if;

  if nullif(btrim(p_title), '') is null then
    raise exception 'title is required';
  end if;

  if (
    (p_grade_level is null and p_curriculum_version is null and p_subject is null and p_unit_code is null)
    or
    (p_grade_level is not null and p_curriculum_version is not null and p_subject is not null and p_unit_code is not null)
  ) is not true then
    raise exception 'grade_level, curriculum_version, subject, unit_code must be all null or all filled';
  end if;

  if p_unit_code is not null and p_unit_code !~ '^[0-9]{3}$' then
    raise exception 'unit_code must be 3 digits';
  end if;

  if p_grade_level is not null then
    if not exists (
      select 1
      from auto_grading.curriculum_units cu
      where cu.grade_level = p_grade_level
        and cu.curriculum_version = p_curriculum_version
        and cu.subject = p_subject
        and cu.unit_code = p_unit_code
    ) then
      raise exception 'curriculum unit not found for (%, %, %, %)',
        p_grade_level, p_curriculum_version, p_subject, p_unit_code;
    end if;
  end if;

  update auto_grading.test_sets
     set title = nullif(btrim(p_title), ''),
         curriculum_version = p_curriculum_version,
         grade_level = p_grade_level,
         subject = p_subject,
         unit_code = p_unit_code,
         source_category = nullif(btrim(p_source_category), ''),
         source_name = nullif(btrim(p_source_name), ''),
         updated_at = now()
   where id = p_test_set_id
   returning * into v_row;

  if not found then
    raise exception 'test_set not found: %', p_test_set_id;
  end if;

  return jsonb_build_object(
    'ok', true,
    'test_set_id', v_row.id,
    'title', v_row.title,
    'curriculum_version', v_row.curriculum_version,
    'grade_level', v_row.grade_level,
    'subject', v_row.subject,
    'unit_code', v_row.unit_code,
    'source_category', v_row.source_category,
    'source_name', v_row.source_name,
    'updated_at', v_row.updated_at,
    'message', 'test_set metadata updated'
  );

exception
  when others then
    return jsonb_build_object(
      'ok', false,
      'test_set_id', p_test_set_id,
      'error', sqlerrm
    );
end;
$function$;

grant execute on function auto_grading.teacher_update_test_set_metadata(uuid, text, text, text, text, text, text, text)
to authenticated;

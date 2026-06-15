create or replace function auto_grading.create_test_set_from_json(
  p_title text,
  p_original_filename text,
  p_items jsonb,
  p_source_type text default 'csv_upload',
  p_source_name text default null,
  p_subject text default null,
  p_grade_level text default null,
  p_major_unit text default null,
  p_minor_unit text default null,
  p_default_choice_count integer default 5,
  p_curriculum_version text default null,
  p_unit_code text default null,
  p_source_category text default null
)
returns table(test_set_id uuid, title text, inserted_items integer)
language plpgsql
security definer
set search_path to 'auto_grading', 'public'
as $function$
declare
  v_test_set_id uuid;
  v_item_count integer;
  v_duplicate_count integer;
  v_invalid_count integer;
  v_source_category text;
begin
  perform auto_grading.assert_admin();

  if p_title is null or btrim(p_title) = '' then
    raise exception 'p_title is required';
  end if;

  if p_items is null or jsonb_typeof(p_items) <> 'array' or jsonb_array_length(p_items) = 0 then
    raise exception 'p_items must be a non-empty json array';
  end if;

  if p_default_choice_count is null or p_default_choice_count < 2 or p_default_choice_count > 20 then
    raise exception 'p_default_choice_count must be between 2 and 20';
  end if;

  -- curriculum 참조 컬럼은 전부 같이 입력하거나 전부 비워야 함
  if (
    (nullif(btrim(p_grade_level), '') is null)
    or (nullif(btrim(p_curriculum_version), '') is null)
    or (nullif(btrim(p_subject), '') is null)
    or (nullif(btrim(p_unit_code), '') is null)
  ) then
    if not (
      nullif(btrim(p_grade_level), '') is null
      and nullif(btrim(p_curriculum_version), '') is null
      and nullif(btrim(p_subject), '') is null
      and nullif(btrim(p_unit_code), '') is null
    ) then
      raise exception 'p_grade_level, p_curriculum_version, p_subject, p_unit_code must be all provided together or all null';
    end if;
  end if;

  if nullif(btrim(p_unit_code), '') is not null
     and btrim(p_unit_code) !~ '^\d{3}$' then
    raise exception 'p_unit_code must be a 3-digit code';
  end if;

  v_source_category := coalesce(
    nullif(btrim(p_source_category), ''),
    nullif(btrim(p_source_type), ''),
    'csv_upload'
  );

  -- item_no / answer_key 필수 검사
  select count(*)
  into v_invalid_count
  from jsonb_to_recordset(p_items) as x(
    item_no text,
    choice_count text,
    answer_key text,
    topic_tag text,
    note text
  )
  where coalesce(btrim(item_no), '') = ''
     or coalesce(btrim(answer_key), '') = '';

  if v_invalid_count > 0 then
    raise exception 'Some rows are missing required fields: item_no or answer_key';
  end if;

  -- item_no 중복 검사
  with parsed as (
    select btrim(item_no)::int as item_no
    from jsonb_to_recordset(p_items) as x(
      item_no text,
      choice_count text,
      answer_key text,
      topic_tag text,
      note text
    )
  ),
  dup as (
    select item_no
    from parsed
    group by item_no
    having count(*) > 1
  )
  select count(*)
  into v_duplicate_count
  from dup;

  if v_duplicate_count > 0 then
    raise exception 'Duplicate item_no exists in p_items';
  end if;

  insert into auto_grading.test_sets (
    title,
    source_type,
    source_name,
    source_category,
    original_filename,
    subject,
    grade_level,
    curriculum_version,
    unit_code,
    major_unit,
    minor_unit,
    default_choice_count
  )
  values (
    btrim(p_title),
    coalesce(nullif(btrim(p_source_type), ''), 'csv_upload'),
    nullif(btrim(p_source_name), ''),
    v_source_category,
    nullif(btrim(p_original_filename), ''),
    nullif(btrim(p_subject), ''),
    nullif(btrim(p_grade_level), ''),
    nullif(btrim(p_curriculum_version), ''),
    nullif(btrim(p_unit_code), ''),
    nullif(btrim(p_major_unit), ''),
    nullif(btrim(p_minor_unit), ''),
    p_default_choice_count
  )
  returning id into v_test_set_id;

  insert into auto_grading.test_items (
    test_set_id,
    item_no,
    choice_count,
    answer_key_raw,
    topic_tag,
    note
  )
  select
    v_test_set_id,
    btrim(x.item_no)::int,
    case
      when nullif(btrim(x.choice_count), '') is null then p_default_choice_count
      else btrim(x.choice_count)::int
    end as choice_count,
    btrim(x.answer_key),
    nullif(btrim(x.topic_tag), ''),
    nullif(btrim(x.note), '')
  from jsonb_to_recordset(p_items) as x(
    item_no text,
    choice_count text,
    answer_key text,
    topic_tag text,
    note text
  );

  get diagnostics v_item_count = row_count;

  return query
  select
    v_test_set_id,
    p_title,
    v_item_count;
end;
$function$;

-- 관리자 전용 쓰기 RPC: anon/public 차단, authenticated 만 호출 가능
-- (실제 관리자 검증은 함수 본문 첫 줄 assert_admin() 에서 수행)
-- service_role 은 grant 대상에서 제외: assert_admin 이 auth.jwt()->>'email' 을 보는데
-- service_role 키 호출에는 email claim 이 없어 어차피 통과하지 못하며,
-- 서버측 작업은 테이블에 직접 쓰면 RLS 를 우회한다. (teacher_* RPC 컨벤션과 일치)
revoke execute on function auto_grading.create_test_set_from_json(
  text, text, jsonb, text, text, text, text, text, text, integer, text, text, text
) from public, anon;

grant execute on function auto_grading.create_test_set_from_json(
  text, text, jsonb, text, text, text, text, text, text, integer, text, text, text
) to authenticated;
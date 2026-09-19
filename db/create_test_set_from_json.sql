-- ============================================================================
-- create_test_set_from_json.sql
--
-- 문제지와 실제 문항을 생성한다. CSV 제목 행은 프론트에서 제외하며,
-- p_items에는 채점 대상 문항만 전달한다.
--
-- 표시 메타데이터 호환 기본값:
--   display_item_no -> item_no
--   section_order   -> 1
--   section_title   -> null
--   display_order   -> item_no
--
-- item_no는 제출·재풀이 API가 사용하는 내부 불변 식별자다. 이 함수는
-- 전달받은 item_no를 저장하며 기존 시험지의 item_no를 변경하지 않는다.
-- ============================================================================

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
  v_section_display_duplicate_count integer;
  v_display_order_duplicate_count integer;
  v_invalid_count integer;
  v_source_category text;
  v_has_titled_items boolean;
  v_has_untitled_items boolean;
begin
  perform auto_grading.assert_admin();

  if p_title is null or btrim(p_title) = '' then
    raise exception 'p_title is required';
  end if;

  if p_items is null
     or jsonb_typeof(p_items) <> 'array'
     or jsonb_array_length(p_items) = 0 then
    raise exception 'p_items must be a non-empty json array';
  end if;

  if p_default_choice_count is null
     or p_default_choice_count < 2
     or p_default_choice_count > 20 then
    raise exception 'p_default_choice_count must be between 2 and 20';
  end if;

  -- curriculum 참조 컬럼은 전부 같이 입력하거나 전부 비워야 한다.
  if (
    nullif(btrim(p_grade_level), '') is null
    or nullif(btrim(p_curriculum_version), '') is null
    or nullif(btrim(p_subject), '') is null
    or nullif(btrim(p_unit_code), '') is null
  ) then
    if not (
      nullif(btrim(p_grade_level), '') is null
      and nullif(btrim(p_curriculum_version), '') is null
      and nullif(btrim(p_subject), '') is null
      and nullif(btrim(p_unit_code), '') is null
    ) then
      raise exception
        'p_grade_level, p_curriculum_version, p_subject, p_unit_code must be all provided together or all null';
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

  -- 배열 원소는 모두 문항 객체여야 한다. 제목 행은 p_items에 포함하지 않는다.
  select count(*)::integer
  into v_invalid_count
  from jsonb_array_elements(p_items) as j(item)
  where jsonb_typeof(j.item) <> 'object';

  if v_invalid_count > 0 then
    raise exception 'P_ITEMS_ROWS_MUST_BE_OBJECTS';
  end if;

  -- item_no / answer_key 필수 검사.
  select count(*)::integer
  into v_invalid_count
  from jsonb_array_elements(p_items) as j(item)
  where nullif(btrim(j.item ->> 'item_no'), '') is null
     or nullif(btrim(j.item ->> 'answer_key'), '') is null;

  if v_invalid_count > 0 then
    raise exception 'P_ITEMS_REQUIRED_FIELDS_MISSING';
  end if;

  -- item_no는 PostgreSQL integer 범위의 양의 정수여야 한다.
  select count(*)::integer
  into v_invalid_count
  from jsonb_array_elements(p_items) as j(item)
  where case
    when btrim(j.item ->> 'item_no') !~ '^\d+$' then true
    else (btrim(j.item ->> 'item_no'))::numeric
      not between 1 and 2147483647
  end;

  if v_invalid_count > 0 then
    raise exception 'P_ITEMS_ITEM_NO_INVALID';
  end if;

  -- 선택 표시 메타데이터는 누락·null·빈 문자열이면 호환 기본값을 쓴다.
  -- 값이 있으면 PostgreSQL integer 범위의 양의 정수인지 먼저 확인한다.
  select count(*)::integer
  into v_invalid_count
  from jsonb_array_elements(p_items) as j(item)
  cross join lateral (
    values
      ('display_item_no', j.item ->> 'display_item_no'),
      ('section_order', j.item ->> 'section_order'),
      ('display_order', j.item ->> 'display_order')
  ) as metadata(field_name, field_value)
  where case
    when nullif(btrim(metadata.field_value), '') is null then false
    when btrim(metadata.field_value) !~ '^\d+$' then true
    else (btrim(metadata.field_value))::numeric
      not between 1 and 2147483647
  end;

  if v_invalid_count > 0 then
    raise exception 'P_ITEMS_DISPLAY_METADATA_INVALID';
  end if;

  -- 문항별 choice_count도 명시된 경우 2~20 범위여야 한다.
  select count(*)::integer
  into v_invalid_count
  from jsonb_array_elements(p_items) as j(item)
  where case
    when nullif(btrim(j.item ->> 'choice_count'), '') is null then false
    when btrim(j.item ->> 'choice_count') !~ '^\d+$' then true
    else (btrim(j.item ->> 'choice_count'))::numeric not between 2 and 20
  end;

  if v_invalid_count > 0 then
    raise exception 'P_ITEMS_CHOICE_COUNT_INVALID';
  end if;

  -- DB CHECK와 동일하게 btrim 후 문자 수 120자를 상한으로 사용한다.
  select count(*)::integer
  into v_invalid_count
  from jsonb_array_elements(p_items) as j(item)
  where nullif(btrim(j.item ->> 'section_title'), '') is not null
    and char_length(btrim(j.item ->> 'section_title')) > 120;

  if v_invalid_count > 0 then
    raise exception 'P_ITEMS_SECTION_TITLE_INVALID';
  end if;

  -- 호환 기본값을 적용한 뒤 내부번호·표시번호·출력순서 계약을 검증한다.
  with parsed as (
    select
      btrim(j.item ->> 'item_no')::integer as item_no,
      coalesce(
        nullif(btrim(j.item ->> 'display_item_no'), '')::integer,
        btrim(j.item ->> 'item_no')::integer
      ) as display_item_no,
      coalesce(
        nullif(btrim(j.item ->> 'section_order'), '')::integer,
        1
      ) as section_order,
      nullif(btrim(j.item ->> 'section_title'), '') as section_title,
      coalesce(
        nullif(btrim(j.item ->> 'display_order'), '')::integer,
        btrim(j.item ->> 'item_no')::integer
      ) as display_order
    from jsonb_array_elements(p_items) as j(item)
  )
  select
    (count(*) - count(distinct p.item_no))::integer,
    (
      count(*)
      - count(distinct (p.section_order, p.display_item_no))
    )::integer,
    (count(*) - count(distinct p.display_order))::integer,
    bool_or(p.section_title is not null),
    bool_or(p.section_title is null)
  into
    v_duplicate_count,
    v_section_display_duplicate_count,
    v_display_order_duplicate_count,
    v_has_titled_items,
    v_has_untitled_items
  from parsed p;

  if v_duplicate_count > 0 then
    raise exception 'P_ITEMS_ITEM_NO_DUPLICATE';
  end if;

  if v_section_display_duplicate_count > 0 then
    raise exception 'P_ITEMS_SECTION_DISPLAY_ITEM_NO_DUPLICATE';
  end if;

  if v_display_order_duplicate_count > 0 then
    raise exception 'P_ITEMS_DISPLAY_ORDER_DUPLICATE';
  end if;

  if v_has_titled_items and v_has_untitled_items then
    raise exception 'TEST_ITEM_SECTION_TITLE_MODE_MISMATCH';
  end if;

  with parsed as (
    select
      coalesce(
        nullif(btrim(j.item ->> 'section_order'), '')::integer,
        1
      ) as section_order,
      nullif(btrim(j.item ->> 'section_title'), '') as section_title
    from jsonb_array_elements(p_items) as j(item)
  )
  select count(*)::integer
  into v_invalid_count
  from parsed p
  where p.section_title is null
    and p.section_order <> 1;

  if v_invalid_count > 0 then
    raise exception 'UNTITLED_TEST_ITEM_SECTION_ORDER_INVALID';
  end if;

  with parsed as (
    select
      coalesce(
        nullif(btrim(j.item ->> 'section_order'), '')::integer,
        1
      ) as section_order,
      nullif(btrim(j.item ->> 'section_title'), '') as section_title
    from jsonb_array_elements(p_items) as j(item)
  )
  select count(*)::integer
  into v_invalid_count
  from (
    select p.section_order
    from parsed p
    group by p.section_order
    having count(distinct p.section_title) > 1
  ) mismatched_sections;

  if v_invalid_count > 0 then
    raise exception 'TEST_ITEM_SECTION_TITLE_MISMATCH';
  end if;

  -- 제목 있는 시험지는 각 섹션의 첫 출력 문항이 표시번호 1이어야 한다.
  with parsed as (
    select
      btrim(j.item ->> 'item_no')::integer as item_no,
      coalesce(
        nullif(btrim(j.item ->> 'display_item_no'), '')::integer,
        btrim(j.item ->> 'item_no')::integer
      ) as display_item_no,
      coalesce(
        nullif(btrim(j.item ->> 'section_order'), '')::integer,
        1
      ) as section_order,
      nullif(btrim(j.item ->> 'section_title'), '') as section_title,
      coalesce(
        nullif(btrim(j.item ->> 'display_order'), '')::integer,
        btrim(j.item ->> 'item_no')::integer
      ) as display_order
    from jsonb_array_elements(p_items) as j(item)
  ),
  ranked as (
    select
      p.*,
      row_number() over (
        partition by p.section_order
        order by p.display_order, p.item_no
      ) as section_item_rank
    from parsed p
  )
  select count(*)::integer
  into v_invalid_count
  from ranked r
  where r.section_title is not null
    and r.section_item_rank = 1
    and r.display_item_no <> 1;

  if v_invalid_count > 0 then
    raise exception 'SECTION_FIRST_DISPLAY_ITEM_NO_INVALID';
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
    display_item_no,
    section_order,
    section_title,
    display_order,
    choice_count,
    answer_key_raw,
    topic_tag,
    note
  )
  select
    v_test_set_id,
    btrim(j.item ->> 'item_no')::integer,
    coalesce(
      nullif(btrim(j.item ->> 'display_item_no'), '')::integer,
      btrim(j.item ->> 'item_no')::integer
    ),
    coalesce(
      nullif(btrim(j.item ->> 'section_order'), '')::integer,
      1
    ),
    nullif(btrim(j.item ->> 'section_title'), ''),
    coalesce(
      nullif(btrim(j.item ->> 'display_order'), '')::integer,
      btrim(j.item ->> 'item_no')::integer
    ),
    case
      when nullif(btrim(j.item ->> 'choice_count'), '') is null
        then p_default_choice_count
      else btrim(j.item ->> 'choice_count')::integer
    end as choice_count,
    btrim(j.item ->> 'answer_key'),
    nullif(btrim(j.item ->> 'topic_tag'), ''),
    nullif(btrim(j.item ->> 'note'), '')
  from jsonb_array_elements(p_items) with ordinality as j(item, input_order)
  order by j.input_order;

  -- 제목 행은 p_items에 없으므로 실제 INSERT 행 수가 곧 total_items 기준이다.
  get diagnostics v_item_count = row_count;

  return query
  select
    v_test_set_id,
    btrim(p_title),
    v_item_count;
end;
$function$;

-- 관리자 전용 쓰기 RPC: anon/public 차단, authenticated만 호출 가능.
-- 실제 관리자 검증은 함수 본문 첫 줄 assert_admin()에서 수행한다.
-- service_role은 grant 대상에서 제외한다.
revoke execute on function auto_grading.create_test_set_from_json(
  text, text, jsonb, text, text, text, text, text, text, integer, text, text, text
) from public, anon, service_role;

grant execute on function auto_grading.create_test_set_from_json(
  text, text, jsonb, text, text, text, text, text, text, integer, text, text, text
) to authenticated;

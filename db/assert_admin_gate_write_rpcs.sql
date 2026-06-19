-- ================================================================
-- assert_admin_gate_write_rpcs.sql
-- Tier 1 보안 게이트: 관리자 전용 쓰기 RPC 들에 assert_admin() 추가
--
-- 배경: auth 가 프로젝트 전역이고(다른 앱과 공유) 가입이 열려 있어,
--       관리자가 아닌 authenticated 사용자도 존재한다. 아래 쓰기 RPC 들은
--       security definer + authenticated grant 인데 assert_admin() 게이트가
--       없어, 비관리자가 직접 호출해 강좌/학생-강좌/문제지 메타/단원을
--       변경할 수 있는 권한상승 노출이 있었다.
--
-- 처리: 운영 DB 의 현재 본문(pg_get_functiondef)을 그대로 두고, 최상위 begin
--       바로 다음에 `perform auto_grading.assert_admin();` 한 줄만 삽입해
--       재생성한다. (버전 파일 난립 때문에 repo 추측 대신 운영 본문 기준)
--
-- 적용 후: notify pgrst, 'reload schema';
-- 전제: auto_grading.assert_admin() 가 이미 배포되어 있어야 한다.
--
-- ⚠️ 이 파일의 성격: 운영 DB 덤프 기반 "hotfix/보안 보정용" 마이그레이션이다.
--    fresh DB 재현용 정본(canonical)이 아니다 — 각 함수의 본문/주석/seed 실행
--    순서는 원본 파일들에 흩어져 있다. 본 파일은 "운영본에 게이트를 덧입히는" 용도.
--
-- ⚠️ seed/마이그레이션 주의: upsert_curriculum_unit* 등에 게이트가 생기면,
--    SQL 에디터나 seed 스크립트(예: curriculum_units_seed_m3_2022.sql)가 이
--    RPC 를 "직접" 호출할 때 JWT email 이 없어 assert_admin() 에서 막힌다.
--    (운영 관리자 페이지는 requireAuth 로그인 → JWT email 있음 → 정상)
--    seed 재실행이 필요하면: ① 이 게이트 적용 전에 먼저 돌리거나,
--    ② seed 가 RPC 대신 테이블에 직접 insert 하도록 하거나,
--    ③ assert_admin() 을 "auth.jwt() 가 null(=직접 DB 실행)이면 통과"하도록
--       보강(별도 작업). 직접 DB 접근은 이미 신뢰 컨텍스트라 API 보안엔 영향 없음.
-- ================================================================

-- ────────────────────────────────────────────────────────────────
-- 1) teacher_create_course
-- ────────────────────────────────────────────────────────────────
CREATE OR REPLACE FUNCTION auto_grading.teacher_create_course(p_course_name text, p_open_year integer, p_start_date date DEFAULT NULL::date, p_end_date date DEFAULT NULL::date, p_course_type text DEFAULT NULL::text, p_note text DEFAULT NULL::text)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'auto_grading', 'public'
AS $function$
declare
  v_course auto_grading.courses%rowtype;
  v_course_no integer;
  v_try_count integer := 0;
  v_next integer;

  v_has_course_no boolean;
  v_has_course_code boolean;
  v_has_course_name boolean;
  v_has_open_year boolean;
  v_has_start_date boolean;
  v_has_end_date boolean;
  v_has_course_type boolean;
  v_has_note boolean;
  v_has_is_active boolean;
  v_has_created_at boolean;
  v_has_updated_at boolean;
  v_has_grade_level boolean;
  v_has_subject_group boolean;

  v_grade_level_nullable boolean := true;
  v_subject_group_nullable boolean := true;

  v_inferred_grade_level text;
  v_inferred_subject_group text;

  v_cols text[] := array[]::text[];
  v_vals text[] := array[]::text[];
  v_sql text;
begin
  perform auto_grading.assert_admin();

  if nullif(btrim(p_course_name), '') is null then
    raise exception 'p_course_name is required';
  end if;

  if p_open_year is null then
    raise exception 'p_open_year is required';
  end if;

  if p_open_year < 2000 or p_open_year > 2100 then
    raise exception 'p_open_year must be between 2000 and 2100';
  end if;

  if p_start_date is not null and p_end_date is not null and p_end_date < p_start_date then
    raise exception 'p_end_date must be greater than or equal to p_start_date';
  end if;

  if p_course_type is not null and p_course_type not in ('과외', '학원', '내신', '특강') then
    raise exception 'p_course_type must be one of 과외, 학원, 내신, 특강';
  end if;

  -- 컬럼 존재 여부 점검
  select exists (
    select 1 from information_schema.columns
    where table_schema = 'auto_grading' and table_name = 'courses' and column_name = 'course_no'
  ) into v_has_course_no;

  select exists (
    select 1 from information_schema.columns
    where table_schema = 'auto_grading' and table_name = 'courses' and column_name = 'course_code'
  ) into v_has_course_code;

  select exists (
    select 1 from information_schema.columns
    where table_schema = 'auto_grading' and table_name = 'courses' and column_name = 'course_name'
  ) into v_has_course_name;

  select exists (
    select 1 from information_schema.columns
    where table_schema = 'auto_grading' and table_name = 'courses' and column_name = 'open_year'
  ) into v_has_open_year;

  select exists (
    select 1 from information_schema.columns
    where table_schema = 'auto_grading' and table_name = 'courses' and column_name = 'start_date'
  ) into v_has_start_date;

  select exists (
    select 1 from information_schema.columns
    where table_schema = 'auto_grading' and table_name = 'courses' and column_name = 'end_date'
  ) into v_has_end_date;

  select exists (
    select 1 from information_schema.columns
    where table_schema = 'auto_grading' and table_name = 'courses' and column_name = 'course_type'
  ) into v_has_course_type;

  select exists (
    select 1 from information_schema.columns
    where table_schema = 'auto_grading' and table_name = 'courses' and column_name = 'note'
  ) into v_has_note;

  select exists (
    select 1 from information_schema.columns
    where table_schema = 'auto_grading' and table_name = 'courses' and column_name = 'is_active'
  ) into v_has_is_active;

  select exists (
    select 1 from information_schema.columns
    where table_schema = 'auto_grading' and table_name = 'courses' and column_name = 'created_at'
  ) into v_has_created_at;

  select exists (
    select 1 from information_schema.columns
    where table_schema = 'auto_grading' and table_name = 'courses' and column_name = 'updated_at'
  ) into v_has_updated_at;

  select exists (
    select 1 from information_schema.columns
    where table_schema = 'auto_grading' and table_name = 'courses' and column_name = 'grade_level'
  ) into v_has_grade_level;

  select exists (
    select 1 from information_schema.columns
    where table_schema = 'auto_grading' and table_name = 'courses' and column_name = 'subject_group'
  ) into v_has_subject_group;

  if v_has_grade_level then
    select case when is_nullable = 'YES' then true else false end
      into v_grade_level_nullable
    from information_schema.columns
    where table_schema = 'auto_grading'
      and table_name = 'courses'
      and column_name = 'grade_level';
  end if;

  if v_has_subject_group then
    select case when is_nullable = 'YES' then true else false end
      into v_subject_group_nullable
    from information_schema.columns
    where table_schema = 'auto_grading'
      and table_name = 'courses'
      and column_name = 'subject_group';
  end if;

  -- 제목에서 학년 추론
  v_inferred_grade_level :=
    case
      when p_course_name like '%중1%' then 'M1'
      when p_course_name like '%중2%' then 'M2'
      when p_course_name like '%중3%' then 'M3'
      when p_course_name like '%고1%' then 'H1'
      when p_course_name like '%고2%' then 'H2'
      when p_course_name like '%고3%' then 'H3'
      else null
    end;

  -- 제목에서 과목군 추론
  v_inferred_subject_group :=
    case
      when p_course_name like '%과학%' then '과학'
      when p_course_name like '%수학%' then '수학'
      when p_course_name like '%영어%' then '영어'
      when p_course_name like '%국어%' then '국어'
      when p_course_name like '%사회%' then '사회'
      else null
    end;

  if v_has_grade_level and not v_grade_level_nullable and v_inferred_grade_level is null then
    raise exception 'grade_level 컬럼이 NOT NULL인데 course_name에서 학년을 추론할 수 없습니다. 제목에 중1/중2/중3/고1/고2/고3 중 하나가 포함되어야 합니다.';
  end if;

  if v_has_subject_group and not v_subject_group_nullable and v_inferred_subject_group is null then
    raise exception 'subject_group 컬럼이 NOT NULL인데 course_name에서 과목군을 추론할 수 없습니다.';
  end if;

  perform pg_advisory_xact_lock(hashtextextended('auto_grading.courses.course_no', 0));

  loop
    v_try_count := v_try_count + 1;

    select greatest(coalesce(max(course_no), 100) + 1, 101)
      into v_next
    from auto_grading.courses;

    perform setval('auto_grading.course_no_seq', v_next, false);
    v_course_no := nextval('auto_grading.course_no_seq');

    v_cols := array[]::text[];
    v_vals := array[]::text[];

    if v_has_course_no then
      v_cols := array_append(v_cols, format('%I', 'course_no'));
      v_vals := array_append(v_vals, format('%L', v_course_no));
    end if;

    if v_has_course_name then
      v_cols := array_append(v_cols, format('%I', 'course_name'));
      v_vals := array_append(v_vals, format('%L', nullif(btrim(p_course_name), '')));
    end if;

    if v_has_open_year then
      v_cols := array_append(v_cols, format('%I', 'open_year'));
      v_vals := array_append(v_vals, format('%L', p_open_year));
    end if;

    if v_has_start_date then
      v_cols := array_append(v_cols, format('%I', 'start_date'));
      v_vals := array_append(v_vals, case when p_start_date is null then 'null' else format('%L', p_start_date) end);
    end if;

    if v_has_end_date then
      v_cols := array_append(v_cols, format('%I', 'end_date'));
      v_vals := array_append(v_vals, case when p_end_date is null then 'null' else format('%L', p_end_date) end);
    end if;

    if v_has_course_type then
      v_cols := array_append(v_cols, format('%I', 'course_type'));
      v_vals := array_append(v_vals, case when p_course_type is null then 'null' else format('%L', p_course_type) end);
    end if;

    if v_has_note then
      v_cols := array_append(v_cols, format('%I', 'note'));
      v_vals := array_append(v_vals, case when nullif(btrim(p_note), '') is null then 'null' else format('%L', nullif(btrim(p_note), '')) end);
    end if;

    if v_has_is_active then
      v_cols := array_append(v_cols, format('%I', 'is_active'));
      v_vals := array_append(v_vals, 'true');
    end if;

    if v_has_course_code then
      v_cols := array_append(v_cols, format('%I', 'course_code'));
      v_vals := array_append(v_vals, format('%L', 'COURSE-' || v_course_no::text));
    end if;

    if v_has_grade_level then
      v_cols := array_append(v_cols, format('%I', 'grade_level'));
      v_vals := array_append(v_vals, case when v_inferred_grade_level is null then 'null' else format('%L', v_inferred_grade_level) end);
    end if;

    if v_has_subject_group then
      v_cols := array_append(v_cols, format('%I', 'subject_group'));
      v_vals := array_append(v_vals, case when v_inferred_subject_group is null then 'null' else format('%L', v_inferred_subject_group) end);
    end if;

    if v_has_created_at then
      v_cols := array_append(v_cols, format('%I', 'created_at'));
      v_vals := array_append(v_vals, 'now()');
    end if;

    if v_has_updated_at then
      v_cols := array_append(v_cols, format('%I', 'updated_at'));
      v_vals := array_append(v_vals, 'now()');
    end if;

    v_sql := format(
      'insert into auto_grading.courses (%s) values (%s) returning *',
      array_to_string(v_cols, ', '),
      array_to_string(v_vals, ', ')
    );

    begin
      execute v_sql into v_course;
      exit;
    exception
      when unique_violation then
        if v_try_count >= 5 then
          raise exception 'course_no allocation failed after % attempts', v_try_count;
        end if;
    end;
  end loop;

  return jsonb_build_object(
    'ok', true,
    'course_id', v_course.id,
    'course_no', v_course.course_no,
    'course_code', v_course.course_code,
    'course_name', v_course.course_name,
    'grade_level', v_course.grade_level,
    'subject_group', v_course.subject_group,
    'open_year', v_course.open_year,
    'start_date', v_course.start_date,
    'end_date', v_course.end_date,
    'course_type', v_course.course_type,
    'note', v_course.note,
    'is_active', v_course.is_active
  );
end;
$function$;

-- ────────────────────────────────────────────────────────────────
-- 2) teacher_deactivate_student_course
-- ────────────────────────────────────────────────────────────────
CREATE OR REPLACE FUNCTION auto_grading.teacher_deactivate_student_course(p_student_id uuid, p_course_id uuid)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'auto_grading', 'public'
AS $function$
declare
  v_active_ctid tid;
  v_has_is_active boolean;
  v_has_updated_at boolean;
  v_has_ended_at boolean;
  v_has_closed_at boolean;
  v_has_end_at boolean;
  v_has_end_date boolean;
  v_update_sql text := 'update auto_grading.student_courses set ';
  v_update_set_count integer := 0;
begin
  perform auto_grading.assert_admin();

  if p_student_id is null then
    raise exception 'p_student_id is required';
  end if;

  if p_course_id is null then
    raise exception 'p_course_id is required';
  end if;

  select exists (
           select 1 from information_schema.columns
           where table_schema = 'auto_grading' and table_name = 'student_courses' and column_name = 'is_active'
         ),
         exists (
           select 1 from information_schema.columns
           where table_schema = 'auto_grading' and table_name = 'student_courses' and column_name = 'updated_at'
         ),
         exists (
           select 1 from information_schema.columns
           where table_schema = 'auto_grading' and table_name = 'student_courses' and column_name = 'ended_at'
         ),
         exists (
           select 1 from information_schema.columns
           where table_schema = 'auto_grading' and table_name = 'student_courses' and column_name = 'closed_at'
         ),
         exists (
           select 1 from information_schema.columns
           where table_schema = 'auto_grading' and table_name = 'student_courses' and column_name = 'end_at'
         ),
         exists (
           select 1 from information_schema.columns
           where table_schema = 'auto_grading' and table_name = 'student_courses' and column_name = 'end_date'
         )
    into v_has_is_active,
         v_has_updated_at,
         v_has_ended_at,
         v_has_closed_at,
         v_has_end_at,
         v_has_end_date;

  perform pg_advisory_xact_lock(
    hashtextextended(p_student_id::text || ':' || p_course_id::text, 0)
  );

  -- 현재 활성 row가 있을 때만 종료 처리한다.
  select sc.ctid
    into v_active_ctid
  from auto_grading.student_courses sc
  where sc.student_id = p_student_id
    and sc.course_id = p_course_id
    and (
      case
        when v_has_is_active and lower(coalesce(to_jsonb(sc)->>'is_active', '')) in ('true', 'false')
          then (to_jsonb(sc)->>'is_active')::boolean
        else (
          nullif(
            coalesce(
              to_jsonb(sc)->>'closed_at',
              to_jsonb(sc)->>'ended_at',
              to_jsonb(sc)->>'end_at',
              to_jsonb(sc)->>'end_date',
              ''
            ),
            ''
          ) is null
        )
      end
    )
  order by
    nullif(
      coalesce(
        to_jsonb(sc)->>'joined_at',
        to_jsonb(sc)->>'started_at',
        to_jsonb(sc)->>'start_at',
        to_jsonb(sc)->>'start_date',
        to_jsonb(sc)->>'created_at',
        ''
      ),
      ''
    )::timestamptz desc nulls last,
    nullif(
      coalesce(
        to_jsonb(sc)->>'updated_at',
        to_jsonb(sc)->>'created_at',
        ''
      ),
      ''
    )::timestamptz desc nulls last,
    sc.ctid desc
  limit 1;

  if v_active_ctid is null then
    return jsonb_build_object(
      'ok', true,
      'student_id', p_student_id,
      'course_id', p_course_id,
      'deactivated_count', 0,
      'message', 'no active student_course found'
    );
  end if;

  if v_has_is_active then
    v_update_sql := v_update_sql || 'is_active = false, ';
    v_update_set_count := v_update_set_count + 1;
  end if;

  if v_has_ended_at then
    v_update_sql := v_update_sql || 'ended_at = coalesce(ended_at, now()), ';
    v_update_set_count := v_update_set_count + 1;
  end if;

  if v_has_closed_at then
    v_update_sql := v_update_sql || 'closed_at = coalesce(closed_at, now()), ';
    v_update_set_count := v_update_set_count + 1;
  end if;

  if v_has_end_at then
    v_update_sql := v_update_sql || 'end_at = coalesce(end_at, now()), ';
    v_update_set_count := v_update_set_count + 1;
  end if;

  if v_has_end_date then
    v_update_sql := v_update_sql || 'end_date = coalesce(end_date, current_date), ';
    v_update_set_count := v_update_set_count + 1;
  end if;

  if v_has_updated_at then
    v_update_sql := v_update_sql || 'updated_at = now(), ';
    v_update_set_count := v_update_set_count + 1;
  end if;

  if v_update_set_count = 0 then
    raise exception 'no mutable columns available for student_course deactivation';
  end if;

  v_update_sql := regexp_replace(v_update_sql, ',\s*$', '');
  v_update_sql := v_update_sql || ' where ctid = $1';

  execute v_update_sql using v_active_ctid;

  return jsonb_build_object(
    'ok', true,
    'student_id', p_student_id,
    'course_id', p_course_id,
    'deactivated_count', 1,
    'message', 'active student_course deactivated'
  );
end;
$function$;

-- ────────────────────────────────────────────────────────────────
-- 3) teacher_delete_courses
-- ────────────────────────────────────────────────────────────────
CREATE OR REPLACE FUNCTION auto_grading.teacher_delete_courses(p_course_ids uuid[])
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'auto_grading', 'public'
AS $function$
declare
  v_requested_count integer := 0;
  v_deleted_count integer := 0;
  v_skipped_in_use_count integer := 0;
  v_skipped_not_found_count integer := 0;
  v_items jsonb := '[]'::jsonb;
begin
  perform auto_grading.assert_admin();

  if coalesce(cardinality(p_course_ids), 0) = 0 then
    raise exception 'p_course_ids is required';
  end if;

  with input_ids as (
    select distinct x.course_id
    from unnest(p_course_ids) as x(course_id)
    where x.course_id is not null
  ),
  course_base as (
    select
      i.course_id,
      c.id is not null as course_exists,
      exists (
        select 1
        from auto_grading.student_courses sc
        where sc.course_id = i.course_id
      ) as used_by_student_courses,
      exists (
        select 1
        from auto_grading.assignments a
        where a.course_id = i.course_id
      ) as used_by_assignments
    from input_ids i
    left join auto_grading.courses c
      on c.id = i.course_id
  ),
  deletable as (
    select cb.course_id
    from course_base cb
    where cb.course_exists
      and not cb.used_by_student_courses
      and not cb.used_by_assignments
  ),
  deleted as (
    delete from auto_grading.courses c
    where c.id in (select course_id from deletable)
    returning c.id
  ),
  result_rows as (
    select
      cb.course_id,
      case
        when not cb.course_exists then 'skipped_not_found'
        when cb.used_by_student_courses or cb.used_by_assignments then 'skipped_in_use'
        when d.id is not null then 'deleted'
        else 'skipped_unknown'
      end as action
    from course_base cb
    left join deleted d
      on d.id = cb.course_id
  )
  select
    (select count(*) from input_ids)::integer,
    count(*) filter (where action = 'deleted')::integer,
    count(*) filter (where action = 'skipped_in_use')::integer,
    count(*) filter (where action = 'skipped_not_found')::integer,
    coalesce(
      jsonb_agg(
        jsonb_build_object(
          'course_id', course_id,
          'action', action
        )
      ),
      '[]'::jsonb
    )
  into
    v_requested_count,
    v_deleted_count,
    v_skipped_in_use_count,
    v_skipped_not_found_count,
    v_items
  from result_rows;

  return jsonb_build_object(
    'ok', true,
    'requested_count', v_requested_count,
    'deleted_count', v_deleted_count,
    'skipped_in_use_count', v_skipped_in_use_count,
    'skipped_not_found_count', v_skipped_not_found_count,
    'items', v_items
  );
end;
$function$;

-- ────────────────────────────────────────────────────────────────
-- 4) teacher_get_next_course_no
-- ────────────────────────────────────────────────────────────────
CREATE OR REPLACE FUNCTION auto_grading.teacher_get_next_course_no()
 RETURNS integer
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'auto_grading', 'public'
AS $function$
declare
  v_next integer;
begin
  perform auto_grading.assert_admin();

  select greatest(coalesce(max(course_no), 100) + 1, 101)
    into v_next
  from auto_grading.courses;

  return v_next;
end;
$function$;

-- ────────────────────────────────────────────────────────────────
-- 5) teacher_update_test_set_metadata
--    (주의: 본문 끝 exception when others 가 assert_admin 예외도 잡아
--     {ok:false, error:'관리자 권한...'} 로 반환한다. 쓰기는 차단되므로
--     보안상 안전하며, 프론트는 ok=false 를 에러로 처리한다.)
-- ────────────────────────────────────────────────────────────────
CREATE OR REPLACE FUNCTION auto_grading.teacher_update_test_set_metadata(p_test_set_id uuid, p_title text, p_curriculum_version text, p_grade_level text, p_subject text, p_unit_code text, p_source_category text, p_source_name text)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'auto_grading', 'public'
AS $function$
declare
  v_row auto_grading.test_sets%rowtype;
begin
  perform auto_grading.assert_admin();

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

-- ────────────────────────────────────────────────────────────────
-- 6) upsert_curriculum_unit
-- ────────────────────────────────────────────────────────────────
CREATE OR REPLACE FUNCTION auto_grading.upsert_curriculum_unit(p_grade_level text, p_curriculum_version text, p_subject text, p_major_unit_code text, p_major_unit_name text, p_minor_unit_code text DEFAULT '0'::text, p_minor_unit_name text DEFAULT NULL::text, p_nano_unit_code text DEFAULT '0'::text, p_nano_unit_name text DEFAULT NULL::text, p_is_active boolean DEFAULT true)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'auto_grading', 'public'
AS $function$
declare
  v_grade_level text := nullif(btrim(p_grade_level), '');
  v_curriculum_version text := nullif(btrim(p_curriculum_version), '');
  v_subject text := nullif(btrim(p_subject), '');
  v_major_code text := coalesce(nullif(regexp_replace(coalesce(p_major_unit_code, ''), '\D', '', 'g'), ''), '0');
  v_minor_code text := coalesce(nullif(regexp_replace(coalesce(p_minor_unit_code, ''), '\D', '', 'g'), ''), '0');
  v_nano_code text := coalesce(nullif(regexp_replace(coalesce(p_nano_unit_code, ''), '\D', '', 'g'), ''), '0');
  v_major_name text := nullif(btrim(p_major_unit_name), '');
  v_minor_name text := nullif(btrim(p_minor_unit_name), '');
  v_nano_name text := nullif(btrim(p_nano_unit_name), '');
  v_unit_code text;
  v_unit_level text;
  v_row auto_grading.curriculum_units%rowtype;
begin
  perform auto_grading.assert_admin();

  if v_grade_level is null or v_curriculum_version is null or v_subject is null then
    raise exception 'grade_level, curriculum_version, subject are required';
  end if;

  if v_major_name is null then
    raise exception 'major_unit_name is required';
  end if;

  if v_major_code !~ '^[1-9]$' then
    raise exception 'major_unit_code must be a single digit 1-9';
  end if;

  if v_minor_code !~ '^[0-9]$' then
    raise exception 'minor_unit_code must be a single digit 0-9';
  end if;

  if v_nano_code !~ '^[0-9]$' then
    raise exception 'nano_unit_code must be a single digit 0-9';
  end if;

  if v_minor_code = '0' and v_nano_code <> '0' then
    raise exception 'nano_unit_code cannot be set when minor_unit_code is 0';
  end if;

  if v_minor_code = '0' then
    v_unit_level := 'major';
    v_minor_name := null;
    v_nano_name := null;
  elsif v_nano_code = '0' then
    v_unit_level := 'middle';
    if v_minor_name is null then
      raise exception 'minor_unit_name is required for middle level rows';
    end if;
    v_nano_name := null;
  else
    v_unit_level := 'nano';
    if v_minor_name is null then
      raise exception 'minor_unit_name is required for nano level rows';
    end if;
    if v_nano_name is null then
      raise exception 'nano_unit_name is required for nano level rows';
    end if;
  end if;

  v_unit_code := v_major_code || v_minor_code || v_nano_code;

  insert into auto_grading.curriculum_units (
    grade_level,
    curriculum_version,
    subject,
    unit_code,
    unit_level,
    major_unit_code,
    major_unit_name,
    minor_unit_code,
    minor_unit_name,
    nano_unit_code,
    nano_unit_name,
    is_active
  )
  values (
    v_grade_level,
    v_curriculum_version,
    v_subject,
    v_unit_code,
    v_unit_level,
    v_major_code,
    v_major_name,
    v_minor_code,
    v_minor_name,
    v_nano_code,
    v_nano_name,
    coalesce(p_is_active, true)
  )
  on conflict (grade_level, curriculum_version, subject, unit_code)
  do update set
    unit_level = excluded.unit_level,
    major_unit_code = excluded.major_unit_code,
    major_unit_name = excluded.major_unit_name,
    minor_unit_code = excluded.minor_unit_code,
    minor_unit_name = excluded.minor_unit_name,
    nano_unit_code = excluded.nano_unit_code,
    nano_unit_name = excluded.nano_unit_name,
    is_active = excluded.is_active,
    updated_at = now()
  returning * into v_row;

  return jsonb_build_object(
    'ok', true,
    'unit_code', v_row.unit_code,
    'unit_level', v_row.unit_level,
    'grade_level', v_row.grade_level,
    'curriculum_version', v_row.curriculum_version,
    'subject', v_row.subject
  );
end;
$function$;

-- ────────────────────────────────────────────────────────────────
-- 7) upsert_curriculum_unit_hierarchy
--    (내부에서 upsert_curriculum_unit 을 호출 → 거기서도 assert_admin 이
--     한 번 더 돌지만 같은 호출자 JWT 라 무해. definer 안에서도 auth.jwt()
--     는 원호출자 기준이라 게이트 정상 동작.)
-- ────────────────────────────────────────────────────────────────
CREATE OR REPLACE FUNCTION auto_grading.upsert_curriculum_unit_hierarchy(p_grade_level text, p_curriculum_version text, p_subject text, p_major_unit_code text, p_major_unit_name text, p_minor_unit_code text DEFAULT NULL::text, p_minor_unit_name text DEFAULT NULL::text, p_nano_unit_code text DEFAULT NULL::text, p_nano_unit_name text DEFAULT NULL::text, p_is_active boolean DEFAULT true)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'auto_grading', 'public'
AS $function$
declare
  v_major_code text := coalesce(nullif(regexp_replace(coalesce(p_major_unit_code, ''), '\D', '', 'g'), ''), '0');
  v_minor_code text := coalesce(nullif(regexp_replace(coalesce(p_minor_unit_code, ''), '\D', '', 'g'), ''), '0');
  v_nano_code text := coalesce(nullif(regexp_replace(coalesce(p_nano_unit_code, ''), '\D', '', 'g'), ''), '0');
  v_items jsonb := '[]'::jsonb;
begin
  perform auto_grading.assert_admin();

  v_items := v_items || jsonb_build_array(
    auto_grading.upsert_curriculum_unit(
      p_grade_level,
      p_curriculum_version,
      p_subject,
      v_major_code,
      p_major_unit_name,
      '0',
      null,
      '0',
      null,
      p_is_active
    )
  );

  if v_minor_code <> '0' then
    v_items := v_items || jsonb_build_array(
      auto_grading.upsert_curriculum_unit(
        p_grade_level,
        p_curriculum_version,
        p_subject,
        v_major_code,
        p_major_unit_name,
        v_minor_code,
        p_minor_unit_name,
        '0',
        null,
        p_is_active
      )
    );
  end if;

  if v_minor_code <> '0' and v_nano_code <> '0' then
    v_items := v_items || jsonb_build_array(
      auto_grading.upsert_curriculum_unit(
        p_grade_level,
        p_curriculum_version,
        p_subject,
        v_major_code,
        p_major_unit_name,
        v_minor_code,
        p_minor_unit_name,
        v_nano_code,
        p_nano_unit_name,
        p_is_active
      )
    );
  end if;

  return jsonb_build_object(
    'ok', true,
    'items', v_items
  );
end;
$function$;

-- ────────────────────────────────────────────────────────────────
-- 8) teacher_attach_student_to_course
--    (teacher_register_student_with_course 가 내부에서 이 함수를 호출한다.
--     그쪽도 definer + assert_admin 이라, 같은 호출자 JWT 로 중복 검사만 될 뿐
--     정상 동작한다.)
-- ────────────────────────────────────────────────────────────────
CREATE OR REPLACE FUNCTION auto_grading.teacher_attach_student_to_course(p_student_id uuid, p_course_id uuid, p_service_type text)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'auto_grading', 'public'
AS $function$
declare
  v_active_ctid tid;
  v_input text;
  v_student_course_type text;
  v_service_type text;
  v_enrollment_type text;

  v_has_student_course_type boolean;
  v_has_service_type boolean;
  v_has_enrollment_type boolean;
  v_has_is_active boolean;
  v_has_joined_at boolean;
  v_has_started_at boolean;
  v_has_start_at boolean;
  v_has_start_date boolean;
  v_has_created_at boolean;
  v_has_updated_at boolean;
  v_has_ended_at boolean;
  v_has_closed_at boolean;
  v_has_end_at boolean;
  v_has_end_date boolean;

  v_update_sql text := 'update auto_grading.student_courses set ';
  v_update_set_count integer := 0;
  v_insert_cols text := 'student_id, course_id';
  v_insert_vals text := '$1, $2';
begin
  perform auto_grading.assert_admin();

  if p_student_id is null then
    raise exception 'p_student_id is required';
  end if;

  if p_course_id is null then
    raise exception 'p_course_id is required';
  end if;

  v_input := nullif(btrim(coalesce(p_service_type, '')), '');
  if v_input is null then
    raise exception 'p_service_type (student_course_type input) is required';
  end if;

  v_student_course_type := case
    when v_input in ('academy', '정규', '학원') then '학원'
    when v_input in ('tutoring', '과외') then '과외'
    when v_input = '내신' then '내신'
    when v_input = '특강' then '특강'
    else null
  end;

  if v_student_course_type is null then
    raise exception 'invalid p_service_type (student_course_type input): %', p_service_type;
  end if;

  v_service_type := case
    when v_student_course_type = '과외' then 'tutoring'
    else 'academy'
  end;

  -- 전환기 dual-write 매핑
  v_enrollment_type := case
    when v_student_course_type = '과외' then '과외'
    when v_student_course_type = '내신' then '내신'
    else '정규' -- 학원, 특강은 임시로 정규에 매핑
  end;

  perform 1
  from auto_grading.students s
  where s.id = p_student_id;

  if not found then
    raise exception 'student not found: %', p_student_id;
  end if;

  perform 1
  from auto_grading.courses c
  where c.id = p_course_id;

  if not found then
    raise exception 'course not found: %', p_course_id;
  end if;

  select exists (
           select 1 from information_schema.columns
           where table_schema = 'auto_grading' and table_name = 'student_courses' and column_name = 'student_course_type'
         ),
         exists (
           select 1 from information_schema.columns
           where table_schema = 'auto_grading' and table_name = 'student_courses' and column_name = 'service_type'
         ),
         exists (
           select 1 from information_schema.columns
           where table_schema = 'auto_grading' and table_name = 'student_courses' and column_name = 'enrollment_type'
         ),
         exists (
           select 1 from information_schema.columns
           where table_schema = 'auto_grading' and table_name = 'student_courses' and column_name = 'is_active'
         ),
         exists (
           select 1 from information_schema.columns
           where table_schema = 'auto_grading' and table_name = 'student_courses' and column_name = 'joined_at'
         ),
         exists (
           select 1 from information_schema.columns
           where table_schema = 'auto_grading' and table_name = 'student_courses' and column_name = 'started_at'
         ),
         exists (
           select 1 from information_schema.columns
           where table_schema = 'auto_grading' and table_name = 'student_courses' and column_name = 'start_at'
         ),
         exists (
           select 1 from information_schema.columns
           where table_schema = 'auto_grading' and table_name = 'student_courses' and column_name = 'start_date'
         ),
         exists (
           select 1 from information_schema.columns
           where table_schema = 'auto_grading' and table_name = 'student_courses' and column_name = 'created_at'
         ),
         exists (
           select 1 from information_schema.columns
           where table_schema = 'auto_grading' and table_name = 'student_courses' and column_name = 'updated_at'
         ),
         exists (
           select 1 from information_schema.columns
           where table_schema = 'auto_grading' and table_name = 'student_courses' and column_name = 'ended_at'
         ),
         exists (
           select 1 from information_schema.columns
           where table_schema = 'auto_grading' and table_name = 'student_courses' and column_name = 'closed_at'
         ),
         exists (
           select 1 from information_schema.columns
           where table_schema = 'auto_grading' and table_name = 'student_courses' and column_name = 'end_at'
         ),
         exists (
           select 1 from information_schema.columns
           where table_schema = 'auto_grading' and table_name = 'student_courses' and column_name = 'end_date'
         )
    into v_has_student_course_type,
         v_has_service_type,
         v_has_enrollment_type,
         v_has_is_active,
         v_has_joined_at,
         v_has_started_at,
         v_has_start_at,
         v_has_start_date,
         v_has_created_at,
         v_has_updated_at,
         v_has_ended_at,
         v_has_closed_at,
         v_has_end_at,
         v_has_end_date;

  perform pg_advisory_xact_lock(
    hashtextextended(p_student_id::text || ':' || p_course_id::text, 0)
  );

  -- 활성 row만 update 대상으로 삼는다.
  -- 활성 row가 없으면 과거 inactive 이력을 재사용하지 않고 새 row를 insert 한다.
  select sc.ctid
    into v_active_ctid
  from auto_grading.student_courses sc
  where sc.student_id = p_student_id
    and sc.course_id = p_course_id
    and (
      case
        when v_has_is_active and lower(coalesce(to_jsonb(sc)->>'is_active', '')) in ('true', 'false')
          then (to_jsonb(sc)->>'is_active')::boolean
        else (
          nullif(
            coalesce(
              to_jsonb(sc)->>'closed_at',
              to_jsonb(sc)->>'ended_at',
              to_jsonb(sc)->>'end_at',
              to_jsonb(sc)->>'end_date',
              ''
            ),
            ''
          ) is null
        )
      end
    )
  order by
    nullif(
      coalesce(
        to_jsonb(sc)->>'joined_at',
        to_jsonb(sc)->>'started_at',
        to_jsonb(sc)->>'start_at',
        to_jsonb(sc)->>'start_date',
        to_jsonb(sc)->>'created_at',
        ''
      ),
      ''
    )::timestamptz desc nulls last,
    nullif(
      coalesce(
        to_jsonb(sc)->>'updated_at',
        to_jsonb(sc)->>'created_at',
        ''
      ),
      ''
    )::timestamptz desc nulls last,
    sc.ctid desc
  limit 1;

  if v_active_ctid is not null then
    if v_has_student_course_type then
      v_update_sql := v_update_sql || 'student_course_type = $1, ';
      v_update_set_count := v_update_set_count + 1;
    end if;

    if v_has_service_type then
      v_update_sql := v_update_sql || 'service_type = $2, ';
      v_update_set_count := v_update_set_count + 1;
    end if;

    if v_has_enrollment_type then
      v_update_sql := v_update_sql || 'enrollment_type = $3, ';
      v_update_set_count := v_update_set_count + 1;
    end if;

    -- active row update 경로에서도 불일치 종료 흔적이 남아 있으면 정리한다.
    if v_has_is_active then
      v_update_sql := v_update_sql || 'is_active = true, ';
      v_update_set_count := v_update_set_count + 1;
    end if;

    if v_has_ended_at then
      v_update_sql := v_update_sql || 'ended_at = null, ';
      v_update_set_count := v_update_set_count + 1;
    end if;

    if v_has_closed_at then
      v_update_sql := v_update_sql || 'closed_at = null, ';
      v_update_set_count := v_update_set_count + 1;
    end if;

    if v_has_end_at then
      v_update_sql := v_update_sql || 'end_at = null, ';
      v_update_set_count := v_update_set_count + 1;
    end if;

    if v_has_end_date then
      v_update_sql := v_update_sql || 'end_date = null, ';
      v_update_set_count := v_update_set_count + 1;
    end if;

    if v_has_updated_at then
      v_update_sql := v_update_sql || 'updated_at = now(), ';
      v_update_set_count := v_update_set_count + 1;
    end if;

    if v_update_set_count = 0 then
      raise exception 'no mutable columns available for active student_course update';
    end if;

    v_update_sql := regexp_replace(v_update_sql, ',\s*$', '');
    v_update_sql := v_update_sql || ' where ctid = $4';

    execute v_update_sql
      using v_student_course_type, v_service_type, v_enrollment_type, v_active_ctid;

    return jsonb_build_object(
      'ok', true,
      'mode', 'updated_active',
      'student_id', p_student_id,
      'course_id', p_course_id,
      'student_course_type', v_student_course_type,
      'service_type', v_service_type,
      'enrollment_type', v_enrollment_type
    );
  end if;

  if v_has_student_course_type then
    v_insert_cols := v_insert_cols || ', student_course_type';
    v_insert_vals := v_insert_vals || ', $3';
  end if;

  if v_has_service_type then
    v_insert_cols := v_insert_cols || ', service_type';
    v_insert_vals := v_insert_vals || ', $4';
  end if;

  if v_has_enrollment_type then
    v_insert_cols := v_insert_cols || ', enrollment_type';
    v_insert_vals := v_insert_vals || ', $5';
  end if;

  if v_has_is_active then
    v_insert_cols := v_insert_cols || ', is_active';
    v_insert_vals := v_insert_vals || ', true';
  end if;

  if v_has_joined_at then
    v_insert_cols := v_insert_cols || ', joined_at';
    v_insert_vals := v_insert_vals || ', now()';
  elsif v_has_started_at then
    v_insert_cols := v_insert_cols || ', started_at';
    v_insert_vals := v_insert_vals || ', now()';
  elsif v_has_start_at then
    v_insert_cols := v_insert_cols || ', start_at';
    v_insert_vals := v_insert_vals || ', now()';
  elsif v_has_start_date then
    v_insert_cols := v_insert_cols || ', start_date';
    v_insert_vals := v_insert_vals || ', current_date';
  end if;

  if v_has_created_at then
    v_insert_cols := v_insert_cols || ', created_at';
    v_insert_vals := v_insert_vals || ', now()';
  end if;

  if v_has_updated_at then
    v_insert_cols := v_insert_cols || ', updated_at';
    v_insert_vals := v_insert_vals || ', now()';
  end if;

  execute format(
    'insert into auto_grading.student_courses (%s) values (%s)',
    v_insert_cols,
    v_insert_vals
  )
  using p_student_id, p_course_id, v_student_course_type, v_service_type, v_enrollment_type;

  return jsonb_build_object(
    'ok', true,
    'mode', 'inserted_new_history',
    'student_id', p_student_id,
    'course_id', p_course_id,
    'student_course_type', v_student_course_type,
    'service_type', v_service_type,
    'enrollment_type', v_enrollment_type
  );
end;
$function$;

-- ────────────────────────────────────────────────────────────────
-- 9) create_test_set_from_json: invoker 구버전(10-arg) 중복 오버로드 제거
--    (13-arg security definer 버전이 정본 — 그대로 둔다)
-- ────────────────────────────────────────────────────────────────
drop function if exists auto_grading.create_test_set_from_json(
  text, text, jsonb, text, text, text, text, text, text, integer
);

-- ────────────────────────────────────────────────────────────────
-- 권한 위생: CREATE OR REPLACE 는 기존 grant 를 유지하므로, 함수 생성 시
-- 기본 부여되는 PUBLIC(및 잔존 anon) execute 를 명시적으로 회수하고
-- authenticated 에만 부여한다. (assert_admin 이 1차 방어, grant 가 2차 방어)
-- 내부 호출(register→attach, hierarchy→upsert)은 definer 가 owner 권한으로
-- 실행하므로 grant 회수와 무관하게 정상 동작한다.
-- ────────────────────────────────────────────────────────────────
revoke execute on function auto_grading.teacher_create_course(text, integer, date, date, text, text) from public, anon;
grant  execute on function auto_grading.teacher_create_course(text, integer, date, date, text, text) to authenticated;

revoke execute on function auto_grading.teacher_deactivate_student_course(uuid, uuid) from public, anon;
grant  execute on function auto_grading.teacher_deactivate_student_course(uuid, uuid) to authenticated;

revoke execute on function auto_grading.teacher_delete_courses(uuid[]) from public, anon;
grant  execute on function auto_grading.teacher_delete_courses(uuid[]) to authenticated;

revoke execute on function auto_grading.teacher_get_next_course_no() from public, anon;
grant  execute on function auto_grading.teacher_get_next_course_no() to authenticated;

revoke execute on function auto_grading.teacher_update_test_set_metadata(uuid, text, text, text, text, text, text, text) from public, anon;
grant  execute on function auto_grading.teacher_update_test_set_metadata(uuid, text, text, text, text, text, text, text) to authenticated;

revoke execute on function auto_grading.upsert_curriculum_unit(text, text, text, text, text, text, text, text, text, boolean) from public, anon;
grant  execute on function auto_grading.upsert_curriculum_unit(text, text, text, text, text, text, text, text, text, boolean) to authenticated;

revoke execute on function auto_grading.upsert_curriculum_unit_hierarchy(text, text, text, text, text, text, text, text, text, boolean) from public, anon;
grant  execute on function auto_grading.upsert_curriculum_unit_hierarchy(text, text, text, text, text, text, text, text, text, boolean) to authenticated;

revoke execute on function auto_grading.teacher_attach_student_to_course(uuid, uuid, text) from public, anon;
grant  execute on function auto_grading.teacher_attach_student_to_course(uuid, uuid, text) to authenticated;

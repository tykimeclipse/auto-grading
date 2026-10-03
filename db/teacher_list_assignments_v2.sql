-- =========================================================
-- teacher_list_assignments — 권위 정의 파일 (이 파일만 실행한다)
--
-- 변경 이력
-- - v2: 학생 학년 표준 필드를 grade_level로 통일
--       (grade_level 우선, 구 필드 grade/school_grade/student_grade는 fallback)
-- - 2026-10-03: 발행상황 관리 페이지 필터·페이지네이션 대응
--   * 출처 필터 p_source_category (test_sets.source_category, 앞뒤 공백 무시)
--     source_category 가 없는 수동 시험은 특정 출처 선택 시 제외되고 '전체'에서만 보인다.
--   * 대단원 필터 p_curriculum_version + p_grade_level + p_subject + p_major_unit_code
--     대단원 키 = 교육과정 + 학년 + 과목 + unit_code 앞자리
--     (curriculum_units_hierarchy_chk: unit_code = major||minor||nano, major 1자리,
--      test_sets_curriculum_units_fk: test_sets → curriculum_units 4컬럼 복합 FK)
--   * 페이지 나누기 전, 현재 필터가 적용된 전체 결과 기준 집계 반환
--     filtered_open_count / filtered_final_confirmed_count (total_count 와 같은 범위)
--   * 정렬 고정: assigned_at desc → test_title → student_code → assignment_id
--     (점수 수정으로 last_activity_at 이 바뀌어도 행이 다른 페이지로 이동하지 않도록)
--   * PL/pgSQL 전환 + assert_admin() 관리자 검사 (예외를 잡지 않고 그대로 전달)
--   * 실행 권한: authenticated 만 (PUBLIC / anon / service_role 회수)
--
-- 소비자
--   - frontend/teacher-assignment-management.html
--   - frontend/teacher-assignments-linked-v3-issue-only-compact.html (기존 9개 인자만 사용)
--
-- ※ teacher_list_assignments.sql(v1)은 실행 금지 레거시다.
--
-- 실행 순서: assert_admin.sql (선행 필수) → 이 파일
-- 파일 전체가 하나의 트랜잭션이다. 생성 직후 관리자 JWT로 smoke 호출을 하며,
-- 실패하면 전체가 롤백되어 기존 함수가 그대로 남는다.
-- 배포 전후 점검:
--   배포 전 audit_teacher_list_assignments_filters_pagination_preflight.sql
--   배포 후 audit_teacher_list_assignments_filters_pagination_postdeploy.sql
-- =========================================================

begin;

-- 구 9개 인자 시그니처와 (재실행 대비) 현재 14개 인자 시그니처를 모두 제거한다.
-- returns table 이 바뀌므로 create or replace 로는 교체할 수 없다.
drop function if exists auto_grading.teacher_list_assignments(
  uuid, uuid, uuid, boolean, text, text, text, integer, integer
);

drop function if exists auto_grading.teacher_list_assignments(
  uuid, uuid, uuid, boolean, text, text, text, integer, integer,
  text, text, text, text, text
);

create function auto_grading.teacher_list_assignments(
  p_course_id uuid default null,
  p_test_set_id uuid default null,
  p_student_id uuid default null,
  p_is_open boolean default null,
  p_purpose text default null,
  p_status text default null,
  p_search text default null,
  p_limit integer default 100,
  p_offset integer default 0,
  p_source_category text default null,
  p_curriculum_version text default null,
  p_grade_level text default null,
  p_subject text default null,
  p_major_unit_code text default null
)
returns table(
  total_count bigint,
  filtered_open_count bigint,
  filtered_final_confirmed_count bigint,
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
  source_type text,
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
  reset_token uuid,
  round1_submitted_at timestamptz,
  round2_submitted_at timestamptz
)
language plpgsql
security definer
set search_path to 'auto_grading', 'public'
as $function$
-- 주의: PL/pgSQL 에서는 returns table 컬럼명이 변수로도 취급된다.
-- 아래 쿼리의 모든 컬럼 참조는 반드시 테이블/CTE 별칭으로 한정한다.
declare
  v_source_category text;
  v_curriculum_version text;
  v_grade_level text;
  v_subject text;
  v_major_unit_code text;
begin
  perform auto_grading.assert_admin();

  v_source_category := nullif(btrim(p_source_category), '');
  v_curriculum_version := nullif(btrim(p_curriculum_version), '');
  v_grade_level := nullif(btrim(p_grade_level), '');
  v_subject := nullif(btrim(p_subject), '');
  v_major_unit_code := nullif(btrim(p_major_unit_code), '');

  -- 대단원 번호는 교육과정·학년·과목이 함께 있어야 하나의 대단원을 가리킨다.
  if v_major_unit_code is not null
     and (v_curriculum_version is null or v_grade_level is null or v_subject is null) then
    raise exception 'MAJOR_UNIT_FILTER_REQUIRES_FULL_KEY'
      using errcode = 'invalid_parameter_value',
            detail = 'p_major_unit_code 는 p_curriculum_version, p_grade_level, p_subject 와 함께 지정해야 합니다.';
  end if;

  return query
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
      at.round1_submitted_at,
      at.round2_submitted_at,
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
      to_jsonb(ts)->>'source_type' as source_type,
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
      a.reset_token,
      la.round1_submitted_at,
      la.round2_submitted_at
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
      and (v_source_category is null or btrim(ts.source_category) = v_source_category)
      and (v_curriculum_version is null or ts.curriculum_version = v_curriculum_version)
      and (v_grade_level is null or ts.grade_level = v_grade_level)
      and (v_subject is null or ts.subject = v_subject)
      and (v_major_unit_code is null or left(ts.unit_code, 1) = v_major_unit_code)
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
    count(*) filter (where b.is_open) over() as filtered_open_count,
    -- 최종 확정 규칙: teacher-assignment-management.html 의 derivedFinalDisplay()
    -- (+ isPerfectScore / isManualRow) 와 글자 그대로 같은 규칙이다. 바꿀 때 두 곳을 함께 고친다.
    --   교사 최종점수 있음 → 확정
    --   수동시험 → 교사 최종점수 없으면 미확정
    --   총 문항수 > 0 이고 (1차 정답수 = 총 문항수 또는 1차 100%
    --                     또는 2차 정답수 = 총 문항수 또는 2차 100%) → 자동 확정
    count(*) filter (
      where b.has_teacher_final
         or (
           b.source_type is distinct from 'manual'
           and coalesce(b.total_items, 0) > 0
           and (
             b.round1_correct_count = b.total_items
             or b.round1_score_percent = 100
             or b.round2_correct_count = b.total_items
             or b.round2_score_percent = 100
           )
         )
    ) over() as filtered_final_confirmed_count,
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
    b.source_type,
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
    b.reset_token,
    b.round1_submitted_at,
    b.round2_submitted_at
  from base b
  order by
    b.assigned_at desc nulls last,
    b.test_title asc nulls last,
    b.student_code asc nulls last,
    b.assignment_id desc
  limit greatest(coalesce(p_limit, 100), 1)
  offset greatest(coalesce(p_offset, 0), 0);
end;
$function$;

revoke execute on function auto_grading.teacher_list_assignments(
  uuid, uuid, uuid, boolean, text, text, text, integer, integer,
  text, text, text, text, text
) from public, anon, service_role;

grant execute on function auto_grading.teacher_list_assignments(
  uuid, uuid, uuid, boolean, text, text, text, integer, integer,
  text, text, text, text, text
) to authenticated;

comment on function auto_grading.teacher_list_assignments(
  uuid, uuid, uuid, boolean, text, text, text, integer, integer,
  text, text, text, text, text
)
is '교사용 발행현황 목록 RPC. assert_admin() 관리자 전용. total_count / filtered_open_count / filtered_final_confirmed_count 는 현재 필터가 적용된 결과 전체(페이지 나누기 전) 기준. 출처 필터는 btrim(source_category) 일치, 대단원 필터는 교육과정+학년+과목+unit_code 앞자리. 정렬: assigned_at desc, test_title, student_code, assignment_id.';

-- ---------------------------------------------------------
-- smoke 호출 (commit 전)
-- RETURN QUERY 의 결과 구조 불일치나 컬럼명/변수 충돌은 함수 생성 시점이 아니라
-- 첫 호출에서 드러난다. 관리자 JWT 를 이 트랜잭션 안에서만 흉내 내 한 번 호출하고,
-- 실패하면 트랜잭션 전체가 롤백되어 기존 함수가 그대로 남는다.
-- v_admin_email 은 assert_admin.sql 허용 목록의 관리자 이메일이어야 한다.
-- ---------------------------------------------------------
do $$
declare
  v_admin_email constant text := 'tykimeclipse@gmail.com';
  v_row_count bigint;
begin
  perform set_config(
    'request.jwt.claims',
    json_build_object('email', v_admin_email, 'role', 'authenticated')::text,
    true
  );

  select count(*) into v_row_count
  from auto_grading.teacher_list_assignments(p_limit => 1);

  select count(*) into v_row_count
  from auto_grading.teacher_list_assignments(
    p_limit => 1,
    p_source_category => '__smoke__',
    p_curriculum_version => '__smoke__',
    p_grade_level => '__smoke__',
    p_subject => '__smoke__',
    p_major_unit_code => '1'
  );

  perform set_config('request.jwt.claims', '', true);
end $$;

notify pgrst, 'reload schema';

commit;

-- ================================================================
-- student_management_rpcs.sql
-- 학생 종합 관리 페이지용 RPC 모음
--
-- 1. teacher_list_students_for_management  : 학생 목록 + 수강강좌명 일괄 조회
-- 2. teacher_get_student_detail            : 학생 상세 정보 + 수강이력
-- 3. teacher_update_student_metadata       : 학생 메타정보 수정
-- 4. teacher_set_student_active_state      : 활성/휴원 토글
-- 5. teacher_delete_student_safely         : 연결 데이터 확인 후 안전 삭제
-- ================================================================


-- ----------------------------------------------------------------
-- 1. teacher_list_students_for_management
--    학생 전체 목록 + 활성 수강강좌명 (프론트에서 필터/정렬)
-- ----------------------------------------------------------------
drop function if exists auto_grading.teacher_list_students_for_management();

create or replace function auto_grading.teacher_list_students_for_management()
returns jsonb
language plpgsql                  -- SQL → PL/pgSQL: PERFORM assert_admin() 삽입을 위해 변환
security definer
set search_path to 'auto_grading', 'public'
as $function$
declare
  v_result jsonb;
begin
  perform auto_grading.assert_admin();

  select coalesce(
    jsonb_agg(
      jsonb_build_object(
        'id',           s.id,
        'student_code', s.student_code,
        'name',         s.name,
        'gender',       s.gender,
        'grade_level',  s.grade_level,
        'is_active',    s.is_active,
        'student_phone',s.student_phone,
        'parent_phone', s.parent_phone,
        'address',        s.address,
        'address_detail', s.address_detail,
        'created_at',   s.created_at,
        'updated_at',   s.updated_at,
        'course_names', (
          select string_agg(c.course_name, ', ' order by c.course_name)
          from   auto_grading.student_courses sc
          join   auto_grading.courses c on c.id = sc.course_id
          where  sc.student_id = s.id
            and  sc.is_active  = true
        )
      )
      order by s.created_at desc nulls last, s.id desc
    ),
    '[]'::jsonb
  )
    into v_result
  from auto_grading.students s;

  return v_result;

exception
  when others then
    return jsonb_build_object('ok', false, 'error', sqlerrm);
end;
$function$;

grant execute on function auto_grading.teacher_list_students_for_management()
  to authenticated, service_role;

comment on function auto_grading.teacher_list_students_for_management()
  is '학생 종합 관리용. 전체 학생 목록과 활성 수강강좌명을 jsonb 배열로 반환. 필터/정렬은 프론트에서 처리.';


-- ----------------------------------------------------------------
-- 2. teacher_get_student_detail
--    학생 기본정보 + 수강이력(활성/비활성 전체)
--    성취도 통계·시험이력은 기존 RPC를 프론트에서 별도 호출
-- ----------------------------------------------------------------
drop function if exists auto_grading.teacher_get_student_detail(uuid);

create or replace function auto_grading.teacher_get_student_detail(
  p_student_id uuid
)
returns jsonb
language plpgsql
security definer
set search_path to 'auto_grading', 'public'
as $function$
declare
  v_student jsonb;
  v_courses jsonb;
begin
  perform auto_grading.assert_admin();
  if p_student_id is null then
    raise exception 'p_student_id is required';
  end if;

  select jsonb_build_object(
    'id',            s.id,
    'student_code',  s.student_code,
    'name',          s.name,
    'gender',        s.gender,
    'grade_level',   s.grade_level,
    'is_active',     s.is_active,
    'student_phone',  s.student_phone,
    'parent_phone',   s.parent_phone,
    'address',        s.address,
    'address_detail', s.address_detail,
    'created_at',     s.created_at,
    'updated_at',     s.updated_at
  )
    into v_student
  from auto_grading.students s
  where s.id = p_student_id;

  if v_student is null then
    raise exception '학생을 찾을 수 없습니다. (id: %)', p_student_id;
  end if;

  -- 수강이력: 활성·비활성 전체, 최신순
  select coalesce(
    jsonb_agg(
      jsonb_build_object(
        'course_name',          c.course_name,
        'student_course_type',  sc.student_course_type,
        'is_active',            sc.is_active,
        'created_at',           sc.created_at
      )
      order by sc.is_active desc, sc.created_at desc nulls last
    ),
    '[]'::jsonb
  )
    into v_courses
  from auto_grading.student_courses sc
  join auto_grading.courses c on c.id = sc.course_id
  where sc.student_id = p_student_id;

  -- ok:true를 명시하여 프론트에서 에러 분기와 일관성 확보
  return jsonb_build_object(
    'ok',      true,
    'student', v_student,
    'courses', v_courses
  );

exception
  when others then
    return jsonb_build_object('ok', false, 'error', sqlerrm);
end;
$function$;

grant execute on function auto_grading.teacher_get_student_detail(uuid)
  to authenticated, service_role;

comment on function auto_grading.teacher_get_student_detail(uuid)
  is '학생 기본정보 + 수강이력(전체) 반환. 성취도/시험이력은 기존 RPC를 프론트에서 별도 호출.';


-- ----------------------------------------------------------------
-- 3. teacher_update_student_metadata
--    이름·성별·학년·전화번호·주소 수정
-- ----------------------------------------------------------------
-- 구 시그니처(address_detail 없는 버전) 제거
drop function if exists auto_grading.teacher_update_student_metadata(uuid, text, text, text, text, text, text);
drop function if exists auto_grading.teacher_update_student_metadata(uuid, text, text, text, text, text, text, text);

create or replace function auto_grading.teacher_update_student_metadata(
  p_student_id     uuid,
  p_name           text  default null,
  p_gender         text  default null,
  p_grade_level    text  default null,
  p_student_phone  text  default null,
  p_parent_phone   text  default null,
  p_address        text  default null,
  p_address_detail text  default null
)
returns jsonb
language plpgsql
security definer
set search_path to 'auto_grading', 'public'
as $function$
declare
  v_name text;
begin
  perform auto_grading.assert_admin();
  if p_student_id is null then
    raise exception 'p_student_id is required';
  end if;

  -- 이름은 필수: 파라미터가 null이면 기존 값 유지
  select coalesce(nullif(trim(p_name), ''), name)
    into v_name
  from auto_grading.students
  where id = p_student_id;

  if not found then
    raise exception '학생을 찾을 수 없습니다.';
  end if;

  update auto_grading.students
  set
    name           = v_name,
    gender         = p_gender,                              -- null 허용: null = 미입력/모름으로 되돌리기
    grade_level    = coalesce(p_grade_level, grade_level),  -- 필수 성격 강함: null이면 기존값 유지
    student_phone  = p_student_phone,    -- null 허용: null 전달 시 DB도 null (값 삭제)
    parent_phone   = p_parent_phone,     -- null 허용
    address        = p_address,          -- null 허용
    address_detail = p_address_detail,   -- null 허용
    updated_at     = now()
  where id = p_student_id;

  return jsonb_build_object(
    'ok',         true,
    'student_id', p_student_id,
    'name',       v_name
  );

exception
  when others then
    return jsonb_build_object('ok', false, 'error', sqlerrm);
end;
$function$;

grant execute on function auto_grading.teacher_update_student_metadata(uuid, text, text, text, text, text, text, text)
  to authenticated, service_role;

comment on function auto_grading.teacher_update_student_metadata(uuid, text, text, text, text, text, text, text)
  is '학생 메타정보 수정. student_code는 변경 불가. phone·address·address_detail은 null 저장 가능(지움 반영).';


-- ----------------------------------------------------------------
-- 4. teacher_set_student_active_state
--    활성(수강) / 휴원(비활성) 토글
-- ----------------------------------------------------------------
drop function if exists auto_grading.teacher_set_student_active_state(uuid, boolean);

create or replace function auto_grading.teacher_set_student_active_state(
  p_student_id uuid,
  p_is_active  boolean
)
returns jsonb
language plpgsql
security definer
set search_path to 'auto_grading', 'public'
as $function$
begin
  perform auto_grading.assert_admin();
  if p_student_id is null or p_is_active is null then
    raise exception 'p_student_id and p_is_active are required';
  end if;

  update auto_grading.students
  set
    is_active  = p_is_active,
    updated_at = now()
  where id = p_student_id;

  if not found then
    raise exception '학생을 찾을 수 없습니다.';
  end if;

  return jsonb_build_object(
    'ok',         true,
    'student_id', p_student_id,
    'is_active',  p_is_active
  );

exception
  when others then
    return jsonb_build_object('ok', false, 'error', sqlerrm);
end;
$function$;

grant execute on function auto_grading.teacher_set_student_active_state(uuid, boolean)
  to authenticated, service_role;

comment on function auto_grading.teacher_set_student_active_state(uuid, boolean)
  is '학생 활성/휴원 전환. is_active=false는 휴원/일시정지. 완전 삭제는 teacher_delete_student_safely 사용.';


-- ----------------------------------------------------------------
-- 5. teacher_delete_student_safely
--    연결 데이터(attempts, student_public_links, student_courses) 확인 후
--    데이터 없을 때만 삭제. 있으면 blocked 반환.
-- ----------------------------------------------------------------
drop function if exists auto_grading.teacher_delete_student_safely(uuid);

create or replace function auto_grading.teacher_delete_student_safely(
  p_student_id uuid
)
returns jsonb
language plpgsql
security definer
set search_path to 'auto_grading', 'public'
as $function$
declare
  v_attempt_count   integer;
  v_course_count    integer;
  v_token_count     integer;
  v_student_name    text;
begin
  perform auto_grading.assert_admin();
  if p_student_id is null then
    raise exception 'p_student_id is required';
  end if;

  select name into v_student_name
  from auto_grading.students
  where id = p_student_id;

  if not found then
    raise exception '학생을 찾을 수 없습니다.';
  end if;

  -- 연결 데이터 카운트
  -- TODO: 향후 payments(납부이력), attendance(출석이력) 테이블 추가 시 여기에 카운트 체크 삽입
  select count(*) into v_attempt_count
  from auto_grading.attempts
  where student_id = p_student_id;

  select count(*) into v_course_count
  from auto_grading.student_courses
  where student_id = p_student_id;

  select count(*) into v_token_count
  from auto_grading.student_public_links
  where student_id = p_student_id;

  -- 연결 데이터가 있으면 삭제 차단
  if v_attempt_count > 0 or v_course_count > 0 or v_token_count > 0 then
    return jsonb_build_object(
      'ok',      false,
      'blocked', true,
      'name',    v_student_name,
      'counts',  jsonb_build_object(
        'attempts',      v_attempt_count,
        'courses',       v_course_count,
        'public_links',  v_token_count
      ),
      'error',   format(
        '연결 데이터가 있어 삭제할 수 없습니다. (시험기록 %s건, 수강이력 %s건, 링크 %s건)',
        v_attempt_count, v_course_count, v_token_count
      )
    );
  end if;

  -- 연결 데이터 없으면 삭제
  delete from auto_grading.students
  where id = p_student_id;

  return jsonb_build_object(
    'ok',   true,
    'name', v_student_name
  );

exception
  when others then
    return jsonb_build_object('ok', false, 'blocked', false, 'error', sqlerrm);
end;
$function$;

grant execute on function auto_grading.teacher_delete_student_safely(uuid)
  to authenticated, service_role;

comment on function auto_grading.teacher_delete_student_safely(uuid)
  is '완전 퇴원/오등록 삭제용. attempts·student_courses·public_links 데이터 존재 시 삭제 차단. 연결 데이터 없을 때만 삭제.';

-- ============================================================================
-- course_enrollment_achievement_stage5_student_course_management.sql
--
-- 5단계: 기존 학생의 수강 강좌 연결/종료 UI를 위한 조회 RPC 보완.
--
-- 변경 범위:
--   - 학생 목록의 활성 강좌명을 공통 활성 판정식으로 집계한다.
--   - 학생 상세 수강이력에 course_id, 강좌 활성 상태, joined_at, ended_at을
--     추가하고 student_course_id로 각 이력 행을 안정적으로 식별한다.
--   - 연결/종료 쓰기는 이미 배포된 teacher_attach_student_to_course와
--     teacher_deactivate_student_course를 그대로 사용한다.
--   - 비활성 강좌 연결은 4단계 DB 트리거가 최종 차단한다.
--
-- 이 파일은 검토 후 Supabase SQL Editor에서 원장님이 직접 실행한다.
-- ============================================================================

begin;

create or replace function auto_grading.teacher_list_students_for_management()
returns jsonb
language plpgsql
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
        'id',             s.id,
        'student_code',   s.student_code,
        'name',           s.name,
        'gender',         s.gender,
        'grade_level',    s.grade_level,
        'is_active',      s.is_active,
        'student_phone',  s.student_phone,
        'parent_phone',   s.parent_phone,
        'address',        s.address,
        'address_detail', s.address_detail,
        'created_at',     s.created_at,
        'updated_at',     s.updated_at,
        'course_names', (
          select string_agg(distinct c.course_name, ', ' order by c.course_name)
          from auto_grading.student_courses sc
          join auto_grading.courses c on c.id = sc.course_id
          where sc.student_id = s.id
            and coalesce(sc.is_active, sc.ended_at is null)
            and c.is_active
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
    'id',             s.id,
    'student_code',   s.student_code,
    'name',           s.name,
    'gender',         s.gender,
    'grade_level',    s.grade_level,
    'is_active',      s.is_active,
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

  select coalesce(
    jsonb_agg(
      jsonb_build_object(
        'student_course_id',   sc.id,
        'course_id',           c.id,
        'course_name',         c.course_name,
        'course_is_active',    c.is_active,
        'student_course_type', sc.student_course_type,
        'is_active',           coalesce(sc.is_active, sc.ended_at is null),
        'joined_at',           sc.joined_at,
        'ended_at',            sc.ended_at,
        'created_at',          sc.created_at
      )
      order by
        coalesce(sc.is_active, sc.ended_at is null) desc,
        coalesce(sc.joined_at, sc.created_at) desc nulls last,
        sc.id desc
    ),
    '[]'::jsonb
  )
  into v_courses
  from auto_grading.student_courses sc
  join auto_grading.courses c on c.id = sc.course_id
  where sc.student_id = p_student_id;

  return jsonb_build_object(
    'ok', true,
    'student', v_student,
    'courses', v_courses
  );

exception
  when others then
    return jsonb_build_object('ok', false, 'error', sqlerrm);
end;
$function$;

revoke execute on function auto_grading.teacher_list_students_for_management()
  from public, anon;
grant execute on function auto_grading.teacher_list_students_for_management()
  to authenticated, service_role;

revoke execute on function auto_grading.teacher_get_student_detail(uuid)
  from public, anon;
grant execute on function auto_grading.teacher_get_student_detail(uuid)
  to authenticated, service_role;

comment on function auto_grading.teacher_list_students_for_management() is
  '학생 관리 목록. 활성 강좌명은 공통 수강 활성 판정식과 활성 강좌 기준으로 반환한다.';

comment on function auto_grading.teacher_get_student_detail(uuid) is
  '학생 기본정보 + 수강이력 UUID·강좌 UUID·강좌 상태·수강 시작/종료일을 포함한 전체 수강이력 반환.';

notify pgrst, 'reload schema';

commit;

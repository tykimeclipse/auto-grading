-- ================================================================
-- teacher_register_student_with_course.sql
-- 학생 등록(+ 선택 강좌 연결) 단일 트랜잭션 RPC
--
-- 배경: student-registration-v8 페이지가 students 를 직접
--       SELECT(중복확인)/INSERT/DELETE(롤백) 했는데, students 는 RLS on +
--       authenticated 허용 정책이 없어(anon block-all 만 존재) default-deny 로
--       막힌다. 이 RPC 로 묶어 RLS 를 우회하고 원자성을 보장한다.
--
-- 설계 포인트:
--  - security definer + assert_admin(): 관리자만 호출, 소유자 권한으로 RLS 우회
--  - student_code 는 v8 규칙(101~999, 3자리) 보존. student_no = student_code::int
--  - 강좌 연결은 기존 정본 함수 teacher_attach_student_to_course 를 호출해
--    student_course_type/service_type/enrollment_type dual-write 로직을 재사용
--    (인라인 복제 시 운영 스키마와 어긋날 위험이 커서 호출 전략 채택)
--  - 실패는 전부 예외 전파 → 같은 트랜잭션이라 학생 insert 까지 자동 롤백.
--    (ok:false 를 정상 반환으로 삼키지 않는다)
--
-- ⚠️ 적용 전제 2가지:
--  (1) students.student_no 컬럼이 있어야 함 — base schema.sql 에는 없고
--      student_table_expand.sql 적용 후 생긴다. (운영 DB 에는 이미 있을 가능성 높음)
--  (2) 운영 DB 의 teacher_attach_student_to_course(uuid,uuid,text) 가 "현재 정본"이어야 함.
--      파일이 여러 버전(v2~v5b, teacher_list_courses.sql 내 구버전 등)이라 마지막 적용본이
--      무엇인지 불명확하다. 특히 teacher_list_courses.sql 의 구버전은 academy/tutoring 만
--      허용해, 프론트가 넘기는 학원/과외/내신/특강 입력이 실패하고 학생 insert 까지 롤백될 수
--      있다. 적용 전 아래로 운영 본문을 반드시 확인할 것:
--        select pg_get_functiondef(
--          'auto_grading.teacher_attach_student_to_course(uuid,uuid,text)'::regprocedure);
-- ================================================================

create or replace function auto_grading.teacher_register_student_with_course(
  p_student_code text,
  p_name text,
  p_grade_level text,
  p_course_id uuid default null,
  p_service_type text default null
)
returns jsonb
language plpgsql
security definer
set search_path to 'auto_grading', 'public'
as $function$
declare
  v_name  text := btrim(coalesce(p_name, ''));
  v_code  text := btrim(coalesce(p_student_code, ''));
  v_grade text := btrim(coalesce(p_grade_level, ''));
  v_student_id uuid;
  v_row   auto_grading.students%rowtype;
  v_attach jsonb;
begin
  perform auto_grading.assert_admin();

  -- 이름 필수
  if v_name = '' then
    raise exception '학생 이름을 입력해 주세요.' using errcode = 'P0001';
  end if;

  -- student_code 규칙: 101~999, 3자리 (v8)
  -- regex 를 먼저 단독 검사한 뒤에만 ::int 캐스트 (OR 단축평가는 보장되지 않으므로
  --  비숫자 입력에서 cast 오류 대신 친절한 P0001 메시지가 나가도록 if 를 분리)
  if v_code !~ '^\d{3}$' then
    raise exception 'student_code 는 3자리 숫자여야 합니다: %', p_student_code
      using errcode = 'P0001';
  end if;
  if v_code::int < 101 then
    raise exception 'student_code 는 101 이상이어야 합니다: %', p_student_code
      using errcode = 'P0001';
  end if;

  -- grade_level 필수 + 허용값
  if v_grade = '' then
    raise exception '학년을 선택해 주세요.' using errcode = 'P0001';
  end if;
  if v_grade not in ('E1','E2','E3','E4','E5','E6','M1','M2','M3','H1','H2','H3') then
    raise exception '허용되지 않는 학년 값입니다: %', p_grade_level using errcode = 'P0001';
  end if;

  -- 강좌 연결을 요청한 경우: service_type 필수 + 강좌 존재 확인(명확한 에러용)
  if p_course_id is not null then
    if nullif(btrim(coalesce(p_service_type, '')), '') is null then
      raise exception '수강 형태(service_type)를 선택해 주세요.' using errcode = 'P0001';
    end if;
    if not exists (select 1 from auto_grading.courses c where c.id = p_course_id) then
      raise exception 'course not found: %', p_course_id using errcode = 'P0001';
    end if;
  end if;

  -- 학생 insert (student_no = student_code::int — v8 규칙)
  begin
    insert into auto_grading.students (student_code, student_no, name, grade_level, is_active)
    values (v_code, v_code::int, v_name, v_grade, true)
    returning * into v_row;
  exception
    when unique_violation then
      -- student_code / student_no 둘 다 unique. 같은 숫자로 저장하므로 한 메시지로 안내.
      raise exception '이미 사용 중인 student_code/student_no 입니다: %', v_code
        using errcode = 'P0001';
  end;

  v_student_id := v_row.id;

  -- 선택 강좌 연결: 같은 트랜잭션. 실패 시 예외 전파 → 학생 insert 까지 롤백.
  if p_course_id is not null then
    v_attach := auto_grading.teacher_attach_student_to_course(
      v_student_id, p_course_id, p_service_type
    );
    -- 기존 함수는 실패 시 raise 하지만, 방어적으로 ok=false 반환도 예외로 승격.
    if coalesce(v_attach->>'ok', 'true') = 'false' then
      raise exception '강좌 연결 실패: %', coalesce(v_attach->>'error', '(원인 미상)')
        using errcode = 'P0001';
    end if;
  end if;

  return jsonb_build_object(
    'ok', true,
    'id', v_row.id,
    'student_code', v_row.student_code,
    'name', v_row.name,
    'grade_level', v_row.grade_level,
    'is_active', v_row.is_active,
    'created_at', v_row.created_at,
    'course_linked', (p_course_id is not null)
  );
end;
$function$;

-- 관리자 전용 쓰기 RPC: anon/public 차단, authenticated 만 호출 가능.
-- service_role 은 제외 — assert_admin() 이 auth.jwt()->>'email' 기반이라
-- service_role 키 호출에는 email claim 이 없어 어차피 통과하지 못하며,
-- 서버측 작업은 테이블 직접 쓰기로 RLS 를 우회한다. (teacher_* RPC 컨벤션 일치)
revoke execute on function auto_grading.teacher_register_student_with_course(
  text, text, text, uuid, text
) from public, anon;

grant execute on function auto_grading.teacher_register_student_with_course(
  text, text, text, uuid, text
) to authenticated;

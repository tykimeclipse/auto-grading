-- ============================================================================
-- course_enrollment_achievement_stage7_part4_manual_test_metadata.sql
--
-- 7단계 part 4: 신규 수동 시험에 교육과정·학년·과목·단원 메타데이터를
-- 필수화한다. 기존 미분류 수동 시험은 변경하지 않고 단원 미지정 이력으로
-- 유지한다.
--
-- 중요:
--   - PostgREST 오버로드 모호성을 막기 위해 4-arg 구버전을 먼저 제거한다.
--   - 신규 발행에는 활성 curriculum_units 행만 허용한다.
--   - 과거 시험의 소급 분류는 teacher_update_test_set_metadata를 사용한다.
-- ============================================================================

begin;

drop function if exists auto_grading.teacher_create_manual_test_set(
  text,
  integer,
  text,
  text
);

create or replace function auto_grading.teacher_create_manual_test_set(
  p_title              text,
  p_total_items        integer,
  p_grade_level        text,
  p_curriculum_version text,
  p_subject            text,
  p_unit_code          text
)
returns jsonb
language plpgsql
security definer
set search_path to 'auto_grading', 'public'
as $function$
declare
  v_id uuid;
  v_grade_level text := nullif(btrim(p_grade_level), '');
  v_curriculum_version text := nullif(btrim(p_curriculum_version), '');
  v_subject text := nullif(btrim(p_subject), '');
  v_unit_code text := nullif(btrim(p_unit_code), '');
  v_unit_level text;
  v_major_unit_code text;
  v_major_unit_name text;
begin
  perform auto_grading.assert_admin();

  if coalesce(btrim(p_title), '') = '' then
    raise exception '시험명을 입력하세요.';
  end if;

  if p_total_items is null or p_total_items <= 0 then
    raise exception '총 문항수는 1 이상이어야 합니다.';
  end if;

  if v_grade_level is null
     or v_curriculum_version is null
     or v_subject is null
     or v_unit_code is null then
    raise exception '교육과정, 학년, 과목, 단원을 모두 선택하세요.';
  end if;

  if v_unit_code !~ '^[0-9]{3}$' then
    raise exception '단원 코드는 3자리 숫자여야 합니다.';
  end if;

  select
    cu.unit_level,
    cu.major_unit_code,
    cu.major_unit_name
  into
    v_unit_level,
    v_major_unit_code,
    v_major_unit_name
  from auto_grading.curriculum_units cu
  where cu.grade_level = v_grade_level
    and cu.curriculum_version = v_curriculum_version
    and cu.subject = v_subject
    and cu.unit_code = v_unit_code
    and cu.is_active = true;

  if not found then
    raise exception '선택한 활성 교육과정 단원을 찾을 수 없습니다.';
  end if;

  insert into auto_grading.test_sets(
    title,
    source_type,
    total_items,
    grade_level,
    curriculum_version,
    subject,
    unit_code,
    is_active
  ) values (
    btrim(p_title),
    'manual',
    p_total_items,
    v_grade_level,
    v_curriculum_version,
    v_subject,
    v_unit_code,
    true
  )
  returning id into v_id;

  return jsonb_build_object(
    'ok', true,
    'test_set_id', v_id,
    'title', btrim(p_title),
    'total_items', p_total_items,
    'grade_level', v_grade_level,
    'curriculum_version', v_curriculum_version,
    'subject', v_subject,
    'unit_code', v_unit_code,
    'unit_level', v_unit_level,
    'major_unit_code', v_major_unit_code,
    'major_unit_name', v_major_unit_name
  );

exception
  when others then
    return jsonb_build_object('ok', false, 'error', sqlerrm);
end;
$function$;

revoke execute on function auto_grading.teacher_create_manual_test_set(
  text,
  integer,
  text,
  text,
  text,
  text
) from public, anon, service_role;

grant execute on function auto_grading.teacher_create_manual_test_set(
  text,
  integer,
  text,
  text,
  text,
  text
) to authenticated;

comment on function auto_grading.teacher_create_manual_test_set(
  text,
  integer,
  text,
  text,
  text,
  text
) is '교사용. 활성 교육과정·학년·과목·단원을 필수로 연결해 수동 시험 1건을 발행.';

notify pgrst, 'reload schema';

commit;

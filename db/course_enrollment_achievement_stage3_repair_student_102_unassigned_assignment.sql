-- ============================================================================
-- course_enrollment_achievement_stage3_repair_student_102_unassigned_assignment.sql
--
-- 3단계 배포 전 일회성 보정:
--   학생코드 102 조아윤의 2026-09-16 신규 assignment 1건에
--   실제 수강 강좌를 지정한다.
--
-- 대상 assignment:
--   c21810a5-ed03-4e36-a28f-d925d00027ea
--   Z(발전) M2-500 식물과 에너지(01)
--
-- 안전 원칙:
--   - 대상 학생·시험·강좌를 UUID와 이름으로 모두 확인한다.
--   - 대상 assignment가 열려 있고 attempt가 없을 때만 보정한다.
--   - 학생의 활성 수강 강좌가 대상 강좌 1건뿐일 때만 보정한다.
--   - 다른 학생과 다른 legacy 미지정 assignment는 건드리지 않는다.
--   - 강좌와 수강이 활성인 동안 재실행해도 중복 변경되지 않는다.
--   - 반드시 4단계 강좌 마감 전에 실행한다.
-- ============================================================================

begin;

do $$
declare
  v_student_id uuid;
  v_assignment_id constant uuid := 'c21810a5-ed03-4e36-a28f-d925d00027ea';
  v_test_set_id constant uuid := '0a583591-35c4-40b9-a530-f4a78c7758c9';
  v_course_id constant uuid := '60afa69b-8082-4931-82a6-7ddd0862892b';
  v_expected_test_title constant text := 'Z(발전) M2-500 식물과 에너지(01)';
  v_expected_course_name constant text := '(26)중2과학-내신(1학기-기말)';
  v_current_course_id uuid;
  v_closed_at timestamptz;
  v_active_course_count integer := 0;
  v_target_active_course_count integer := 0;
  v_rows_updated integer := 0;
begin
  select s.id
    into v_student_id
  from auto_grading.students s
  where s.student_code = '102'
    and s.name = '조아윤';

  if v_student_id is null then
    raise exception '중단: 학생코드 102 조아윤을 찾을 수 없습니다.';
  end if;

  perform 1
  from auto_grading.courses c
  where c.id = v_course_id
    and c.course_name = v_expected_course_name;

  if not found then
    raise exception '중단: 대상 강좌 UUID 또는 강좌명이 예상과 다릅니다.';
  end if;

  perform 1
  from auto_grading.test_sets ts
  where ts.id = v_test_set_id
    and ts.title = v_expected_test_title;

  if not found then
    raise exception '중단: 대상 시험 UUID 또는 제목이 예상과 다릅니다.';
  end if;

  select
    count(distinct sc.course_id)::integer,
    count(distinct sc.course_id) filter (where sc.course_id = v_course_id)::integer
    into v_active_course_count, v_target_active_course_count
  from auto_grading.student_courses sc
  join auto_grading.courses c
    on c.id = sc.course_id
   and c.is_active
  where sc.student_id = v_student_id
    and coalesce(sc.is_active, sc.ended_at is null);

  if v_active_course_count <> 1 or v_target_active_course_count <> 1 then
    raise exception
      '중단: 학생 102의 활성 수강 강좌가 대상 강좌 1건이 아닙니다. active=%, target=%',
      v_active_course_count,
      v_target_active_course_count;
  end if;

  select a.course_id, a.closed_at
    into v_current_course_id, v_closed_at
  from auto_grading.assignments a
  where a.id = v_assignment_id
    and a.student_id = v_student_id
    and a.test_set_id = v_test_set_id
  for update;

  if not found then
    raise exception '중단: 대상 assignment의 학생 또는 시험 연결이 예상과 다릅니다.';
  end if;

  if v_closed_at is not null then
    raise exception '중단: 대상 assignment가 이미 종료돼 있습니다. closed_at=%', v_closed_at;
  end if;

  if v_current_course_id is not null and v_current_course_id <> v_course_id then
    raise exception '중단: 대상 assignment가 이미 다른 강좌에 지정돼 있습니다. course_id=%',
      v_current_course_id;
  end if;

  -- 아직 강좌를 지정해야 하는 상태에서 attempt가 생겼다면 자동 보정하지 않는다.
  -- 이미 대상 강좌로 보정된 뒤의 재실행은 attempt 유무와 관계없이 허용한다.
  if v_current_course_id is null and exists (
    select 1
    from auto_grading.attempts at
    where at.assignment_id = v_assignment_id
  ) then
    raise exception '중단: 대상 assignment에 attempt가 생성돼 있어 자동 보정할 수 없습니다.';
  end if;

  update auto_grading.assignments a
  set
    course_id = v_course_id,
    updated_at = now()
  where a.id = v_assignment_id
    and a.course_id is null;

  get diagnostics v_rows_updated = row_count;

  if v_rows_updated not in (0, 1) then
    raise exception '중단: assignment 보정 건수가 예상과 다릅니다. updated=%', v_rows_updated;
  end if;

  perform 1
  from auto_grading.assignments a
  where a.id = v_assignment_id
    and a.student_id = v_student_id
    and a.test_set_id = v_test_set_id
    and a.course_id = v_course_id
    and a.closed_at is null;

  if not found then
    raise exception '중단: 보정 후 assignment 검증에 실패했습니다.';
  end if;

  raise notice '학생 102 미지정 assignment 보정 완료: %건 갱신', v_rows_updated;
end;
$$;

commit;

-- 실행 결과 확인: target_course=true, attempt_count=0 이어야 한다.
select jsonb_build_object(
  'assignment_id', a.id,
  'student_code', s.student_code,
  'student_name', s.name,
  'test_title', ts.title,
  'course_id', a.course_id,
  'course_name', c.course_name,
  'target_course', a.course_id = '60afa69b-8082-4931-82a6-7ddd0862892b'::uuid,
  'closed_at', a.closed_at,
  'attempt_count', (
    select count(*)
    from auto_grading.attempts at
    where at.assignment_id = a.id
  )
) as details
from auto_grading.assignments a
join auto_grading.students s on s.id = a.student_id
join auto_grading.test_sets ts on ts.id = a.test_set_id
left join auto_grading.courses c on c.id = a.course_id
where a.id = 'c21810a5-ed03-4e36-a28f-d925d00027ea';

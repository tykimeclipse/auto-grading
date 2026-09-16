-- ============================================================================
-- course_enrollment_achievement_stage2_attempt_course_snapshot.sql
--
-- 2단계: 응시 강좌 스냅샷 컬럼 추가 + 학생 102의 강좌 미지정 기록 보정.
--
-- 실행 전제(2026-09-11 운영 DB 감사 결과):
--   - 학생코드 102는 조아윤 학생 1명이다.
--   - 이 학생의 수강 이력은 60afa69b-8082-4931-82a6-7ddd0862892b
--     '(26)중2과학-내신(1학기-기말)' 단 1건이다.
--   - 운영 확인과 감사 결과에 따라 이 학생의 모든 assignment·attempt 는
--     이 강좌에 속한다. 별도 기간 판정은 사용하지 않는다.
--   - assignment 32건(13건 지정 / 19건 미지정), attempt 22건.
--   - attempt 22건은 모두 동일 학생·시험의 assignment에 정상 연결되어 있다.
--
-- 안전 원칙:
--   - 다른 학생과 가짜 강좌의 미지정 기록은 건드리지 않는다.
--   - 수강 이력이 1건이 아니거나 감사 건수와 다르면 전체를 중단한다.
--   - 한 트랜잭션으로 처리하므로 중간 오류 시 전체 롤백된다.
--   - 정상 완료 후 재실행해도 중복 변경되지 않는다.
--   - attempts.course_id 는 과거 데이터 때문에 nullable 로 유지한다.
--   - 채점 트리거는 update of first/final/teacher_final_score_percent 로
--     한정되어 있어, course_id 만 갱신하는 이 스크립트로는 발동하지 않는다.
--   - 이 파일은 Supabase SQL Editor 에서 검토 후 원장님이 직접 실행한다.
-- ============================================================================

begin;

-- --------------------------------------------------------------------------
-- 1. 응시 당시 강좌를 보존할 스냅샷 컬럼
-- --------------------------------------------------------------------------
alter table auto_grading.attempts
  add column if not exists course_id uuid null;

do $$
begin
  if not exists (
    select 1
    from pg_constraint con
    where con.conrelid = 'auto_grading.attempts'::regclass
      and con.conname = 'attempts_course_id_fkey'
  ) then
    alter table auto_grading.attempts
      add constraint attempts_course_id_fkey
      foreign key (course_id)
      references auto_grading.courses(id)
      on delete restrict;
  end if;
end;
$$;

create index if not exists idx_attempts_course_id
  on auto_grading.attempts (course_id);

comment on column auto_grading.attempts.course_id is
  '응시 시작 시점의 강좌 스냅샷. 이후 자동 재태깅하지 않으며 관리자 명시 보정만 허용한다.';

-- --------------------------------------------------------------------------
-- 2. 학생 102의 강좌 미지정 기록 보정
-- --------------------------------------------------------------------------
do $$
declare
  v_student_id uuid;
  v_student_name text;
  v_course_id constant uuid := '60afa69b-8082-4931-82a6-7ddd0862892b';
  v_expected_course_name constant text := '(26)중2과학-내신(1학기-기말)';
  v_course_name text;
  v_enrollment_total integer := 0;
  v_enrollment_target integer := 0;
  v_assignment_total integer := 0;
  v_assignment_tagged integer := 0;
  v_assignment_unassigned integer := 0;
  v_attempt_total integer := 0;
  v_attempt_tagged integer := 0;
  v_assignment_rows_updated integer := 0;
  v_attempt_rows_updated integer := 0;
begin
  -- ── 대상 학생 확인 ──────────────────────────────────────────────────────
  select s.id, s.name
    into v_student_id, v_student_name
  from auto_grading.students s
  where s.student_code = '102';

  if v_student_id is null then
    raise exception '중단: 학생코드 102를 찾을 수 없습니다.';
  end if;

  if v_student_name is distinct from '조아윤' then
    raise exception '중단: 학생코드 102의 이름이 예상과 다릅니다. 현재 이름=%', v_student_name;
  end if;

  -- ── 대상 강좌 확인 ──────────────────────────────────────────────────────
  select c.course_name
    into v_course_name
  from auto_grading.courses c
  where c.id = v_course_id;

  if v_course_name is null then
    raise exception '중단: 대상 강좌 %를 찾을 수 없습니다.', v_course_id;
  end if;

  if v_course_name is distinct from v_expected_course_name then
    raise exception '중단: 대상 강좌명이 예상과 다릅니다. 현재 강좌명=%', v_course_name;
  end if;

  -- ── 핵심 안전장치: 수강 이력이 대상 강좌 1건뿐이어야 한다 ───────────────
  --    이 조건이 성립할 때만 "미지정 = 전부 이 강좌" 보정이 정당하다.
  select
    count(*),
    count(*) filter (where sc.course_id = v_course_id)
    into v_enrollment_total, v_enrollment_target
  from auto_grading.student_courses sc
  where sc.student_id = v_student_id;

  if v_enrollment_total <> 1 or v_enrollment_target <> 1 then
    raise exception
      '중단: 학생 102의 수강 이력이 대상 강좌 1건이 아닙니다. total=%, target=%',
      v_enrollment_total, v_enrollment_target;
  end if;

  -- ── assignment 현황 검증 ────────────────────────────────────────────────
  select
    count(*),
    count(*) filter (where a.course_id = v_course_id),
    count(*) filter (where a.course_id is null)
    into v_assignment_total, v_assignment_tagged, v_assignment_unassigned
  from auto_grading.assignments a
  where a.student_id = v_student_id;

  -- 최초 실행 상태(32/13/19) 또는 이미 적용된 상태(32/32/0)만 허용한다.
  if not (
    v_assignment_total = 32
    and (
      (v_assignment_tagged = 13 and v_assignment_unassigned = 19)
      or
      (v_assignment_tagged = 32 and v_assignment_unassigned = 0)
    )
  ) then
    raise exception
      '중단: 학생 102 assignment 현황이 감사 결과와 다릅니다. total=%, tagged=%, unassigned=%',
      v_assignment_total, v_assignment_tagged, v_assignment_unassigned;
  end if;

  if exists (
    select 1
    from auto_grading.assignments a
    where a.student_id = v_student_id
      and a.course_id is not null
      and a.course_id <> v_course_id
  ) then
    raise exception '중단: 학생 102에게 대상 강좌가 아닌 assignment가 존재합니다.';
  end if;

  -- ── assignment 보정 ─────────────────────────────────────────────────────
  update auto_grading.assignments a
  set course_id = v_course_id
  where a.student_id = v_student_id
    and a.course_id is null;

  get diagnostics v_assignment_rows_updated = row_count;

  if v_assignment_rows_updated not in (0, 19) then
    raise exception '중단: assignment 보정 건수가 예상과 다릅니다. updated=%',
      v_assignment_rows_updated;
  end if;

  -- ── attempt 현황 검증 ───────────────────────────────────────────────────
  select count(*)
    into v_attempt_total
  from auto_grading.attempts at
  where at.student_id = v_student_id;

  if v_attempt_total <> 22 then
    raise exception '중단: 학생 102 attempt 건수가 감사 결과와 다릅니다. total=%',
      v_attempt_total;
  end if;

  if exists (
    select 1
    from auto_grading.attempts at
    left join auto_grading.assignments a on a.id = at.assignment_id
    where at.student_id = v_student_id
      and (
        a.id is null
        or a.student_id is distinct from at.student_id
        or a.test_set_id is distinct from at.test_set_id
        or a.course_id is distinct from v_course_id
      )
  ) then
    raise exception '중단: 학생 102 attempt와 assignment의 연결 또는 강좌 귀속이 예상과 다릅니다.';
  end if;

  if exists (
    select 1
    from auto_grading.attempts at
    where at.student_id = v_student_id
      and at.course_id is not null
      and at.course_id <> v_course_id
  ) then
    raise exception '중단: 학생 102에게 대상 강좌가 아닌 attempt 스냅샷이 존재합니다.';
  end if;

  -- ── attempt 보정 ────────────────────────────────────────────────────────
  --    assignment 보정 결과를 근거로 응시 시점 강좌 스냅샷을 채운다.
  update auto_grading.attempts at
  set course_id = v_course_id
  from auto_grading.assignments a
  where at.student_id = v_student_id
    and at.course_id is null
    and a.id = at.assignment_id
    and a.student_id = at.student_id
    and a.test_set_id = at.test_set_id
    and a.course_id = v_course_id;

  get diagnostics v_attempt_rows_updated = row_count;

  if v_attempt_rows_updated not in (0, 22) then
    raise exception '중단: attempt 보정 건수가 예상과 다릅니다. updated=%',
      v_attempt_rows_updated;
  end if;

  -- ── 최종 검증 ───────────────────────────────────────────────────────────
  select count(*)
    into v_assignment_tagged
  from auto_grading.assignments a
  where a.student_id = v_student_id
    and a.course_id = v_course_id;

  select count(*)
    into v_attempt_tagged
  from auto_grading.attempts at
  where at.student_id = v_student_id
    and at.course_id = v_course_id;

  if v_assignment_tagged <> 32 or v_attempt_tagged <> 22 then
    raise exception
      '중단: 보정 후 검증에 실패했습니다. assignments=%, attempts=%',
      v_assignment_tagged, v_attempt_tagged;
  end if;

  raise notice
    '2단계 보정 완료: assignment %건 갱신, attempt %건 갱신',
    v_assignment_rows_updated, v_attempt_rows_updated;
end;
$$;

commit;

-- --------------------------------------------------------------------------
-- 3. 실행 결과 확인(예상: assignments 32/32/0, attempts 22/22/0)
-- --------------------------------------------------------------------------
with
target_student as (
  select s.id
  from auto_grading.students s
  where s.student_code = '102'
    and s.name = '조아윤'
),
target_course as (
  select c.id
  from auto_grading.courses c
  where c.id = '60afa69b-8082-4931-82a6-7ddd0862892b'
    and c.course_name = '(26)중2과학-내신(1학기-기말)'
)
select jsonb_build_object(
  'attempts_course_id_column_exists', exists (
    select 1
    from information_schema.columns col
    where col.table_schema = 'auto_grading'
      and col.table_name = 'attempts'
      and col.column_name = 'course_id'
  ),
  'student_102_assignments_total', (
    select count(*)
    from auto_grading.assignments a
    join target_student s on s.id = a.student_id
  ),
  'student_102_assignments_target_course', (
    select count(*)
    from auto_grading.assignments a
    join target_student s on s.id = a.student_id
    join target_course c on c.id = a.course_id
  ),
  'student_102_assignments_unassigned', (
    select count(*)
    from auto_grading.assignments a
    join target_student s on s.id = a.student_id
    where a.course_id is null
  ),
  'student_102_attempts_total', (
    select count(*)
    from auto_grading.attempts at
    join target_student s on s.id = at.student_id
  ),
  'student_102_attempts_target_course', (
    select count(*)
    from auto_grading.attempts at
    join target_student s on s.id = at.student_id
    join target_course c on c.id = at.course_id
  ),
  'student_102_attempts_unassigned', (
    select count(*)
    from auto_grading.attempts at
    join target_student s on s.id = at.student_id
    where at.course_id is null
  )
) as details;

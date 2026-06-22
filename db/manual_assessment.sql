-- ================================================================
-- manual_assessment.sql
-- 수동 시험(오프라인 평가) 성취도 입력 기능
--
-- 배경/설계:
--   복습퀴즈 · 단답형/주관식 시험 · 내신대비 등 "정답지 업로드 → OMR 자동채점"
--   경로를 거치지 않는 평가를 교사가 수동으로 발행·배정하고 점수를 입력해
--   학생 성취도에 합산한다.
--
--   별도 테이블/별도 집계 경로를 만들지 않고 기존 test_set → assignment →
--   attempt 3단 구조를 그대로 재사용한다. 자동채점과 구분하기 위한 유일한
--   표식은 test_sets.source_type = 'manual' 이다.
--
-- 데이터 규약(수동 attempt):
--   max_rounds          = 1
--   first_*             = 1차(최초 채점) 점수
--   final_*             = first_* 와 동일값 복사
--                         (2차 제외 판단은 '값'이 아니라 source_type='manual'로
--                          한다. 아직 수정되지 않은 구형 조회에서 2차가 0으로
--                          보이는 피해를 줄이기 위한 방어적 복사)
--   teacher_final_*     = 교사가 오답처리 후 확정한 최종 점수(미확정 전 NULL)
--   round1_submitted_at = 평가일(이력의 응시일/event_date 로 사용)
--   round2_submitted_at = 항상 NULL (수동시험은 2차 없음)
--
-- 상태 흐름:
--   1차만 입력            → status = needs_review (1차 반영, 최종 미반영)
--   최종 확정(teacher_final) → 트리거가 status = completed 로 승격(최종 반영)
--   ※ 수동시험은 1차 100점이어도 자동완료되지 않는다. 최종은 오직 teacher_final 로만
--     확정된다(상태 트리거가 source_type='manual' 을 보고 자동완료/자동닫힘을 건너뜀).
--     → stats 최종/이력 최종/관리·학생관리 화면이 모두 일관되게 '미확정'으로 처리.
--
-- 점수 기준:
--   누적 성취도는 영구적으로 "정답 문항수 / 총 문항수" 기준. 배점/부분점수는
--   지원하지 않는다(정답수 컬럼이 integer). 총점 기반 시험은 문항수로 환산해 입력.
--
-- 실행 순서: assert_admin.sql 선행 필수.
-- ================================================================

-- ----------------------------------------------------------------
-- 0) source_type 에 'manual' 허용 (idempotent)
-- ----------------------------------------------------------------
alter table auto_grading.test_sets
  drop constraint if exists chk_test_sets_source_type;

alter table auto_grading.test_sets
  add constraint chk_test_sets_source_type
  check (source_type in ('csv_upload', 'printed_sheet', 'published_book', 'problem_bank', 'manual'));


-- ----------------------------------------------------------------
-- 1) teacher_create_manual_test_set
--    수동 시험(문항 없는 test_set) 1건 발행
-- ----------------------------------------------------------------
create or replace function auto_grading.teacher_create_manual_test_set(
  p_title       text,
  p_total_items integer,
  p_grade_level text default null,
  p_subject     text default null
)
returns jsonb
language plpgsql
security definer
set search_path to 'auto_grading', 'public'
as $function$
declare
  v_id uuid;
begin
  perform auto_grading.assert_admin();

  if coalesce(btrim(p_title), '') = '' then
    raise exception '시험명을 입력하세요.';
  end if;
  if p_total_items is null or p_total_items <= 0 then
    raise exception '총 문항수는 1 이상이어야 합니다.';
  end if;

  insert into auto_grading.test_sets(
    title, source_type, total_items, grade_level, subject, is_active
  ) values (
    btrim(p_title), 'manual', p_total_items, p_grade_level, nullif(btrim(p_subject), ''), true
  )
  returning id into v_id;

  return jsonb_build_object(
    'ok',          true,
    'test_set_id', v_id,
    'title',       btrim(p_title),
    'total_items', p_total_items
  );

exception
  when others then
    return jsonb_build_object('ok', false, 'error', sqlerrm);
end;
$function$;


-- ----------------------------------------------------------------
-- 2) teacher_upsert_manual_score
--    학생 1명의 수동 점수 입력/수정.
--    assignment 당 attempt 1건을 유지하며 갱신한다(새 attempt를 만들지 않음).
--    → 통계는 전 attempt 합산 / 이력은 최신 attempt만 보여주므로, 수정 시
--      새 attempt를 만들면 통계 중복합산 + 이력 불일치가 발생한다.
--
--    p_teacher_final_correct_count:
--      NULL  → 1차만 입력(미확정, needs_review)
--      값 있음 → 오답처리 후 최종 확정(트리거가 completed 로 승격)
-- ----------------------------------------------------------------
-- 인자 추가(p_course_id) 시 6-arg 구버전이 오버로드로 남지 않도록 먼저 제거
drop function if exists auto_grading.teacher_upsert_manual_score(uuid, uuid, integer, integer, date, text);

create or replace function auto_grading.teacher_upsert_manual_score(
  p_test_set_id                 uuid,
  p_student_id                  uuid,
  p_first_correct_count         integer,
  p_teacher_final_correct_count integer default null,
  p_event_date                  date    default null,
  p_note                        text    default null,
  p_course_id                   uuid    default null
)
returns jsonb
language plpgsql
security definer
set search_path to 'auto_grading', 'public'
as $function$
declare
  v_total          integer;
  v_source         text;
  v_assignment_id  uuid;
  v_attempt_id     uuid;
  v_event          timestamptz;
  v_first_pct      numeric(5,2);
  v_tf_pct         numeric(5,2);
  v_finalize       boolean := (p_teacher_final_correct_count is not null);
begin
  perform auto_grading.assert_admin();

  if p_test_set_id is null or p_student_id is null then
    raise exception 'p_test_set_id 와 p_student_id 는 필수입니다.';
  end if;

  -- 수동 시험만 허용 + 총 문항수 확보(단일 진실원천: test_set)
  select total_items, source_type
    into v_total, v_source
  from auto_grading.test_sets
  where id = p_test_set_id;

  if not found then
    raise exception 'test_set 을 찾을 수 없습니다: %', p_test_set_id;
  end if;
  if v_source is distinct from 'manual' then
    raise exception '수동 점수 입력은 source_type=manual 시험에만 가능합니다. (현재: %)', v_source;
  end if;
  if v_total is null or v_total <= 0 then
    raise exception 'test_set 의 total_items 가 올바르지 않습니다.';
  end if;

  -- 입력 검증(문항수 기준)
  if p_first_correct_count is null
     or p_first_correct_count < 0
     or p_first_correct_count > v_total then
    raise exception '1차 정답수는 0~% 사이여야 합니다.', v_total;
  end if;
  if v_finalize
     and (p_teacher_final_correct_count < 0 or p_teacher_final_correct_count > v_total) then
    raise exception '최종 정답수는 0~% 사이여야 합니다.', v_total;
  end if;

  v_event     := coalesce(p_event_date::timestamptz, now());
  v_first_pct := round(p_first_correct_count::numeric * 100 / v_total, 2);
  v_tf_pct    := case when v_finalize
                      then round(p_teacher_final_correct_count::numeric * 100 / v_total, 2)
                      else null end;

  -- assignment upsert (unique student_id, test_set_id)
  -- course_id 를 함께 저장해 과정 필터/과정별 집계에 포함되게 한다.
  -- 재저장 시 course_id 미전달이면 기존 값 보존(coalesce).
  insert into auto_grading.assignments(student_id, test_set_id, course_id, assigned_at, status)
  values (p_student_id, p_test_set_id, p_course_id, v_event, 'assigned')
  on conflict (student_id, test_set_id) do update
    set course_id  = coalesce(excluded.course_id, auto_grading.assignments.course_id),
        updated_at = now()
  returning id into v_assignment_id;

  -- 기존 manual attempt 조회(assignment 당 1건 유지)
  select id
    into v_attempt_id
  from auto_grading.attempts
  where assignment_id = v_assignment_id
  order by created_at asc nulls first, id asc
  limit 1
  for update;

  if v_attempt_id is null then
    -- 신규: status='needs_review' 로 넣는다. BEFORE 트리거는 수동시험의 경우
    --       teacher_final 입력 시에만 completed 로 승격한다(1차 100점 자동완료 미적용).
    insert into auto_grading.attempts(
      student_id, test_set_id, assignment_id,
      attempt_no, max_rounds, current_round, status, total_items,
      first_correct_count, first_score_percent,
      final_correct_count, final_score_percent,
      teacher_final_correct_count, teacher_final_score_percent,
      teacher_final_note, teacher_final_updated_at, teacher_final_updated_by,
      round1_submitted_at, started_at
    ) values (
      p_student_id, p_test_set_id, v_assignment_id,
      1, 1, 1, 'needs_review', v_total,
      p_first_correct_count, v_first_pct,
      p_first_correct_count, v_first_pct,   -- final_* = first_* 복사(방어적)
      p_teacher_final_correct_count, v_tf_pct,
      case when v_finalize then nullif(btrim(p_note), '') else null end,
      case when v_finalize then now() else null end,
      case when v_finalize then auth.uid() else null end,
      v_event, v_event
    )
    returning id into v_attempt_id;
  else
    -- 수정: 같은 attempt UPDATE. 점수 컬럼 갱신 → BEFORE 트리거가 status 재계산.
    update auto_grading.attempts
    set
      total_items                 = v_total,
      first_correct_count         = p_first_correct_count,
      first_score_percent         = v_first_pct,
      final_correct_count         = p_first_correct_count,
      final_score_percent         = v_first_pct,
      teacher_final_correct_count = p_teacher_final_correct_count,
      teacher_final_score_percent = v_tf_pct,
      teacher_final_note          = case when v_finalize then nullif(btrim(p_note), '') else null end,
      teacher_final_updated_at    = case when v_finalize then now() else null end,
      teacher_final_updated_by    = case when v_finalize then auth.uid() else null end,
      status                      = 'needs_review',  -- 트리거가 completed 로 승격/유지 결정
      round1_submitted_at         = v_event
    where id = v_attempt_id;
  end if;

  return jsonb_build_object(
    'ok',                          true,
    'assignment_id',               v_assignment_id,
    'attempt_id',                  v_attempt_id,
    'total_items',                 v_total,
    'first_correct_count',         p_first_correct_count,
    'first_score_percent',         v_first_pct,
    'teacher_final_correct_count', p_teacher_final_correct_count,
    'teacher_final_score_percent', v_tf_pct,
    'finalized',                   v_finalize
  );

exception
  when others then
    return jsonb_build_object('ok', false, 'error', sqlerrm);
end;
$function$;


-- ----------------------------------------------------------------
-- 3) 권한
--    RLS 켜진 테이블에 쓰는 admin 전용 RPC.
--    security definer + assert_admin() 로 게이트하고 authenticated 에만 부여.
--    (default privileges 로 authenticated 에 자동 부여되더라도 명시)
-- ----------------------------------------------------------------
revoke execute on function auto_grading.teacher_create_manual_test_set(text, integer, text, text) from public, anon;
grant  execute on function auto_grading.teacher_create_manual_test_set(text, integer, text, text) to authenticated;

revoke execute on function auto_grading.teacher_upsert_manual_score(uuid, uuid, integer, integer, date, text, uuid) from public, anon;
grant  execute on function auto_grading.teacher_upsert_manual_score(uuid, uuid, integer, integer, date, text, uuid) to authenticated;

comment on function auto_grading.teacher_create_manual_test_set(text, integer, text, text)
  is '교사용. 수동 시험(source_type=manual, 문항 없는 test_set) 1건 발행.';

comment on function auto_grading.teacher_upsert_manual_score(uuid, uuid, integer, integer, date, text, uuid)
  is '교사용. 수동 시험 학생별 점수 입력/수정. assignment당 attempt 1건 유지(upsert). teacher_final 입력 시 최종 확정. course_id 저장으로 과정별 집계 포함. 문항수 기준만 지원.';

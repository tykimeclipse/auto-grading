-- ================================================================
-- get_student_stats_by_code
-- 학생 성취도 상단 카드(누적 1차/2차/최종) 집계.
--
-- 누적 성취도 규칙(문항수 기준 = 정답 문항수 / 총 문항수):
--   · 집계 대상 attempt = status in ('completed', 'needs_review')
--   · 누적 1차  : 자동+수동 전부.  분자 Σfirst_correct_count / 분모 Σtotal_items
--   · 누적 2차  : 자동시험만(수동 제외, 분자·분모 모두).
--                 수동시험은 2차가 없으므로 분모를 별도(v_round2_items)로 둔다.
--                 → 수동시험이 2차 평균을 희석하지 못하게 한다.
--   · 누적 최종 : "확정된" attempt만.
--                 확정 = teacher_final_correct_count IS NOT NULL  (교사 확정)
--                      OR status='completed'                     (1차 100점 자동완료 등)
--                 수동시험은 최종 확정 시 teacher_final 이 채워지므로 포함된다.
--
-- 수동/자동 구분: test_sets.source_type = 'manual'
-- ================================================================
create or replace function auto_grading.get_student_stats_by_code(
  p_student_code text
)
returns jsonb
language plpgsql
security definer
set search_path to 'auto_grading', 'public'
as $function$
declare
  v_student_id            uuid;
  v_total_solved          integer := 0;   -- 총 풀이 문항수(= 누적 1차 분모)
  v_first_correct         integer := 0;   -- 누적 1차 분자
  v_round2_correct        integer := 0;   -- 누적 2차 분자 (수동 제외)
  v_round2_items          integer := 0;   -- 누적 2차 분모 (수동 제외)
  v_final_correct         integer := 0;   -- 누적 최종 분자 (확정만)
  v_final_items           integer := 0;   -- 누적 최종 분모 (확정만)
  v_teacher_final_correct integer;        -- teacher_final 단독 분자
  v_teacher_final_items   integer;        -- teacher_final 단독 분모
begin
  select id
    into v_student_id
  from auto_grading.students
  where student_code = p_student_code
    and is_active = true;

  if v_student_id is null then
    raise exception 'STUDENT_NOT_FOUND'
      using errcode = 'P0001';
  end if;

  select
    -- 총 풀이 문항수 / 누적 1차 분모
    coalesce(sum(a.total_items), 0),
    -- 누적 1차 분자
    coalesce(sum(a.first_correct_count), 0),

    -- 누적 2차 분자·분모: 수동시험 제외
    coalesce(sum(a.final_correct_count) filter (where ts.source_type is distinct from 'manual'), 0),
    coalesce(sum(a.total_items)        filter (where ts.source_type is distinct from 'manual'), 0),

    -- 누적 최종 분자: 확정된 attempt만 (teacher_final 우선, 없으면 completed 의 final).
    --   수동시험은 teacher_final 이 없으면 (1차 100점으로 completed 여도) 최종에서 제외.
    coalesce(sum(
      case
        when a.teacher_final_correct_count is not null then a.teacher_final_correct_count
        when ts.source_type = 'manual'                 then null
        when a.status = 'completed'                    then a.final_correct_count
        else null
      end
    ), 0),
    -- 누적 최종 분모: 위와 동일 조건의 총 문항수
    coalesce(sum(
      case
        when a.teacher_final_correct_count is not null then a.total_items
        when ts.source_type = 'manual'                 then null
        when a.status = 'completed'                    then a.total_items
        else null
      end
    ), 0),

    -- teacher_final 단독 집계
    sum(case when a.teacher_final_correct_count is not null then a.teacher_final_correct_count end),
    sum(case when a.teacher_final_correct_count is not null then a.total_items end)

  into
    v_total_solved,
    v_first_correct,
    v_round2_correct,
    v_round2_items,
    v_final_correct,
    v_final_items,
    v_teacher_final_correct,
    v_teacher_final_items

  from auto_grading.attempts a
  join auto_grading.test_sets ts on ts.id = a.test_set_id
  where a.student_id = v_student_id
    and a.status in ('completed', 'needs_review');

  return jsonb_build_object(
    'total_solved',
    v_total_solved,

    'round1_accuracy',
    case
      when v_total_solved = 0 then null
      else round((v_first_correct::numeric / v_total_solved::numeric) * 100, 2)
    end,

    'round2_accuracy',
    case
      when v_round2_items = 0 then null
      else round((v_round2_correct::numeric / v_round2_items::numeric) * 100, 2)
    end,

    'final_accuracy',
    case
      when v_final_items = 0 then null
      else round((v_final_correct::numeric / v_final_items::numeric) * 100, 2)
    end,

    'teacher_final_accuracy',
    case
      when v_teacher_final_items is null or v_teacher_final_items = 0 then null
      else round((v_teacher_final_correct::numeric / v_teacher_final_items::numeric) * 100, 2)
    end
  );
end;
$function$;

-- by_code 는 관리자(authenticated)만 직접 호출. 공개 페이지는 token 래퍼 경유.
-- (student_code 가 enumerable 하므로 anon 노출 금지)
revoke execute on function auto_grading.get_student_stats_by_code(text) from anon, public;
grant  execute on function auto_grading.get_student_stats_by_code(text) to authenticated;

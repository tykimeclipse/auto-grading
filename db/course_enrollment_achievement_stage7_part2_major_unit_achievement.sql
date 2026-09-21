-- ============================================================================
-- course_enrollment_achievement_stage7_part2_major_unit_achievement.sql
--
-- 7단계 part 2: 대단원별 누적 성취도 조회 코어와 공개 RPC.
--
-- 집계 계약:
--   - 대상 attempt: status in ('completed', 'needs_review')
--   - 범위: 기존 all/course/unassigned 강좌 범위 계약을 그대로 사용
--   - 대단원 키: grade_level + curriculum_version + subject + major_unit_code
--   - 단원 메타데이터가 모두 null이면 하나의 '단원 미지정' 묶음으로 반환
--   - 테스트 수: test_set 수가 아니라 attempt 수
--   - 모든 성취도 분모: 응시 당시 스냅샷인 attempts.total_items
--   - 2차 반영: manual 제외, final_correct_count / total_items
--   - 최종: 기존 _student_achievement_stats_core 규칙과 동일
--   - curriculum_units.is_active 여부와 무관하게 과거 기록 포함
--
-- payload:
--   scope/course + contract + summary + units[]
--   units[].tests[]까지 한 번에 반환하여 단원 클릭 시 추가 RPC가 필요 없다.
--   요약·추세는 전체 평가 기준이며, tests는 최근 200건을 시간 오름차순 반환한다.
-- ============================================================================

begin;

-- --------------------------------------------------------------------------
-- 1. 비공개 대단원별 성취도 코어
-- --------------------------------------------------------------------------
create or replace function auto_grading._student_major_unit_achievement_core(
  p_student_id uuid,
  p_scope text,
  p_course_id uuid
)
returns jsonb
language plpgsql
stable
security definer
set search_path to 'auto_grading', 'public'
as $function$
declare
  v_scope text := lower(btrim(coalesce(p_scope, '')));
  v_tests_per_unit_limit constant integer := 200;
  v_result jsonb;
begin
  if p_student_id is null then
    raise exception 'STUDENT_ID_REQUIRED'
      using errcode = 'P0001';
  end if;

  if v_scope not in ('all', 'course', 'unassigned') then
    raise exception 'INVALID_SCOPE'
      using errcode = 'P0001';
  end if;

  if v_scope = 'course' and p_course_id is null then
    raise exception 'COURSE_ID_REQUIRED_FOR_SCOPE'
      using errcode = 'P0001';
  end if;

  with major_unit_labels as (
    select
      cu.grade_level,
      cu.curriculum_version,
      cu.subject,
      cu.major_unit_code,
      coalesce(
        max(cu.major_unit_name) filter (where cu.unit_level = 'major'),
        max(cu.major_unit_name)
      ) as major_unit_name
    from auto_grading.curriculum_units cu
    group by
      cu.grade_level,
      cu.curriculum_version,
      cu.subject,
      cu.major_unit_code
  ),
  eligible_attempts as (
    select
      at.id as attempt_id,
      at.assignment_id,
      at.test_set_id,
      at.course_id,
      c.course_name,
      ts.title as test_title,
      ts.source_type,
      at.status,
      coalesce(
        at.completed_at,
        at.round2_submitted_at,
        at.round1_submitted_at,
        at.updated_at,
        at.started_at
      ) as evaluated_at,
      ts.unit_code is null as is_unit_unassigned,
      ts.grade_level,
      ts.curriculum_version,
      ts.subject,
      ts.unit_code,
      cu.major_unit_code,
      coalesce(mul.major_unit_name, cu.major_unit_name) as major_unit_name,
      cu.is_active as curriculum_unit_is_active,
      at.total_items,
      at.first_correct_count as round1_correct_count,
      at.total_items as round1_item_count,
      case
        when at.total_items > 0 then round(
          at.first_correct_count::numeric * 100 / at.total_items,
          1
        )
        else null
      end as round1_score_percent,
      case
        when ts.source_type = 'manual' then null
        else at.final_correct_count
      end as round2_reflected_correct_count,
      case
        when ts.source_type = 'manual' then null
        else at.total_items
      end as round2_reflected_item_count,
      case
        when ts.source_type is distinct from 'manual'
          and at.total_items > 0
          then round(
            at.final_correct_count::numeric * 100 / at.total_items,
            1
          )
        else null
      end as round2_reflected_score_percent,
      case
        when at.teacher_final_correct_count is not null
          then at.teacher_final_correct_count
        when ts.source_type = 'manual' then null
        when at.status = 'completed' then at.final_correct_count
        else null
      end as final_correct_count,
      case
        when at.teacher_final_correct_count is not null then at.total_items
        when ts.source_type = 'manual' then null
        when at.status = 'completed' then at.total_items
        else null
      end as final_item_count,
      case
        when at.total_items <= 0 then null
        when at.teacher_final_correct_count is not null then round(
          at.teacher_final_correct_count::numeric * 100 / at.total_items,
          1
        )
        when ts.source_type = 'manual' then null
        when at.status = 'completed' then round(
          at.final_correct_count::numeric * 100 / at.total_items,
          1
        )
        else null
      end as final_score_percent,
      at.teacher_final_correct_count,
      case
        when at.teacher_final_correct_count is not null
          and at.total_items > 0
          then round(
            at.teacher_final_correct_count::numeric * 100 / at.total_items,
            1
          )
        else null
      end as teacher_final_score_percent
    from auto_grading.attempts at
    join auto_grading.test_sets ts on ts.id = at.test_set_id
    left join auto_grading.curriculum_units cu
      on cu.grade_level = ts.grade_level
     and cu.curriculum_version = ts.curriculum_version
     and cu.subject = ts.subject
     and cu.unit_code = ts.unit_code
    left join major_unit_labels mul
      on mul.grade_level = cu.grade_level
     and mul.curriculum_version = cu.curriculum_version
     and mul.subject = cu.subject
     and mul.major_unit_code = cu.major_unit_code
    left join auto_grading.courses c on c.id = at.course_id
    where at.student_id = p_student_id
      and at.status in ('completed', 'needs_review')
      and (
        v_scope = 'all'
        or (v_scope = 'course' and at.course_id = p_course_id)
        or (v_scope = 'unassigned' and at.course_id is null)
      )
  ),
  ranked_attempts as (
    select
      ea.*,
      row_number() over (
        partition by
          ea.is_unit_unassigned,
          ea.grade_level,
          ea.curriculum_version,
          ea.subject,
          ea.major_unit_code
        order by ea.evaluated_at desc, ea.attempt_id desc
      ) as detail_recent_rank
    from eligible_attempts ea
  ),
  unit_rollups as (
    select
      ea.is_unit_unassigned,
      ea.grade_level,
      ea.curriculum_version,
      ea.subject,
      ea.major_unit_code,
      ea.major_unit_name,
      count(*)::integer as test_count,
      least(count(*), v_tests_per_unit_limit)::integer
        as returned_test_count,
      count(*) > v_tests_per_unit_limit as tests_truncated,
      count(*) filter (where ea.source_type = 'manual')::integer
        as manual_test_count,
      count(*) filter (
        where ea.round2_reflected_item_count is not null
      )::integer as round2_reflected_test_count,
      count(*) filter (where ea.final_item_count is not null)::integer
        as final_confirmed_test_count,
      min(ea.evaluated_at) as first_evaluated_at,
      max(ea.evaluated_at) as last_evaluated_at,
      coalesce(sum(ea.round1_correct_count), 0)::bigint
        as round1_correct_count,
      coalesce(sum(ea.round1_item_count), 0)::bigint
        as round1_item_count,
      coalesce(sum(ea.round2_reflected_correct_count), 0)::bigint
        as round2_reflected_correct_count,
      coalesce(sum(ea.round2_reflected_item_count), 0)::bigint
        as round2_reflected_item_count,
      coalesce(sum(ea.final_correct_count), 0)::bigint
        as final_correct_count,
      coalesce(sum(ea.final_item_count), 0)::bigint
        as final_item_count,
      coalesce(sum(ea.teacher_final_correct_count), 0)::bigint
        as teacher_final_correct_count,
      coalesce(sum(ea.total_items) filter (
        where ea.teacher_final_correct_count is not null
      ), 0)::bigint as teacher_final_item_count,
      array_agg(
        ea.final_score_percent
        order by ea.evaluated_at, ea.attempt_id
      ) filter (
        where ea.final_score_percent is not null
      ) as final_scores_asc,
      array_agg(
        ea.final_score_percent
        order by ea.evaluated_at desc, ea.attempt_id desc
      ) filter (
        where ea.final_score_percent is not null
      ) as final_scores_desc,
      array_agg(
        ea.evaluated_at
        order by ea.evaluated_at, ea.attempt_id
      ) filter (
        where ea.final_score_percent is not null
      ) as final_dates_asc,
      array_agg(
        ea.evaluated_at
        order by ea.evaluated_at desc, ea.attempt_id desc
      ) filter (
        where ea.final_score_percent is not null
      ) as final_dates_desc,
      array_agg(
        ea.round1_score_percent
        order by ea.evaluated_at, ea.attempt_id
      ) filter (
        where ea.round1_score_percent is not null
      ) as round1_scores_asc,
      array_agg(
        ea.round1_score_percent
        order by ea.evaluated_at desc, ea.attempt_id desc
      ) filter (
        where ea.round1_score_percent is not null
      ) as round1_scores_desc,
      array_agg(
        ea.evaluated_at
        order by ea.evaluated_at, ea.attempt_id
      ) filter (
        where ea.round1_score_percent is not null
      ) as round1_dates_asc,
      array_agg(
        ea.evaluated_at
        order by ea.evaluated_at desc, ea.attempt_id desc
      ) filter (
        where ea.round1_score_percent is not null
      ) as round1_dates_desc,
      jsonb_agg(
        jsonb_build_object(
          'attempt_id', ea.attempt_id,
          'assignment_id', ea.assignment_id,
          'test_set_id', ea.test_set_id,
          'course_id', ea.course_id,
          'course_name', ea.course_name,
          'test_title', ea.test_title,
          'source_type', ea.source_type,
          'status', ea.status,
          'evaluated_at', ea.evaluated_at,
          'event_date', (
            ea.evaluated_at at time zone 'Asia/Seoul'
          )::date,
          'unit_code', ea.unit_code,
          'curriculum_unit_is_active', ea.curriculum_unit_is_active,
          'total_items', ea.total_items,
          'round1', jsonb_build_object(
            'correct_count', ea.round1_correct_count,
            'item_count', ea.round1_item_count,
            'score_percent', ea.round1_score_percent
          ),
          'round2_reflected', jsonb_build_object(
            'correct_count', ea.round2_reflected_correct_count,
            'item_count', ea.round2_reflected_item_count,
            'score_percent', ea.round2_reflected_score_percent
          ),
          'final', jsonb_build_object(
            'correct_count', ea.final_correct_count,
            'item_count', ea.final_item_count,
            'score_percent', ea.final_score_percent,
            'is_confirmed', ea.final_item_count is not null
          ),
          'teacher_final', jsonb_build_object(
            'correct_count', ea.teacher_final_correct_count,
            'item_count', case
              when ea.teacher_final_correct_count is not null
                then ea.total_items
              else null
            end,
            'score_percent', ea.teacher_final_score_percent
          )
        )
        order by ea.evaluated_at, ea.attempt_id
      ) filter (
        where ea.detail_recent_rank <= v_tests_per_unit_limit
      ) as tests
    from ranked_attempts ea
    group by
      ea.is_unit_unassigned,
      ea.grade_level,
      ea.curriculum_version,
      ea.subject,
      ea.major_unit_code,
      ea.major_unit_name
  ),
  unit_rows as (
    select
      ur.*,
      case
        when ur.round1_item_count = 0 then null
        else round(
          ur.round1_correct_count::numeric * 100 / ur.round1_item_count,
          2
        )
      end as round1_score_percent,
      case
        when ur.round2_reflected_item_count = 0 then null
        else round(
          ur.round2_reflected_correct_count::numeric
          * 100 / ur.round2_reflected_item_count,
          2
        )
      end as round2_reflected_score_percent,
      case
        when ur.final_item_count = 0 then null
        else round(
          ur.final_correct_count::numeric * 100 / ur.final_item_count,
          2
        )
      end as final_score_percent,
      case
        when ur.teacher_final_item_count = 0 then null
        else round(
          ur.teacher_final_correct_count::numeric
          * 100 / ur.teacher_final_item_count,
          2
        )
      end as teacher_final_score_percent
    from unit_rollups ur
  ),
  overall_rollup as (
    select
      count(*)::integer as test_count,
      count(*) filter (where ea.is_unit_unassigned)::integer
        as unassigned_test_count,
      count(*) filter (where not ea.is_unit_unassigned)::integer
        as classified_test_count,
      min(ea.evaluated_at) as first_evaluated_at,
      max(ea.evaluated_at) as last_evaluated_at,
      coalesce(sum(ea.round1_correct_count), 0)::bigint
        as round1_correct_count,
      coalesce(sum(ea.round1_item_count), 0)::bigint
        as round1_item_count,
      coalesce(sum(ea.round2_reflected_correct_count), 0)::bigint
        as round2_reflected_correct_count,
      coalesce(sum(ea.round2_reflected_item_count), 0)::bigint
        as round2_reflected_item_count,
      coalesce(sum(ea.final_correct_count), 0)::bigint
        as final_correct_count,
      coalesce(sum(ea.final_item_count), 0)::bigint
        as final_item_count,
      coalesce(sum(ea.teacher_final_correct_count), 0)::bigint
        as teacher_final_correct_count,
      coalesce(sum(ea.total_items) filter (
        where ea.teacher_final_correct_count is not null
      ), 0)::bigint as teacher_final_item_count
    from eligible_attempts ea
  )
  select jsonb_build_object(
    'contract', jsonb_build_object(
      'group_key', jsonb_build_array(
        'grade_level',
        'curriculum_version',
        'subject',
        'major_unit_code'
      ),
      'test_count_basis', 'attempt',
      'item_count_basis', 'attempts.total_items',
      'round2_label', '2차 반영',
      'tests_order', 'evaluated_at_asc',
      'tests_limit_selection', 'most_recent',
      'tests_per_unit_limit', v_tests_per_unit_limit,
      'date_timezone', 'Asia/Seoul',
      'retention_final_basis', 'confirmed final scores only',
      'retention_auxiliary_basis', 'round1 scores'
    ),
    'summary', jsonb_build_object(
      'unit_count', (select count(*) from unit_rows),
      'classified_unit_count', (
        select count(*) from unit_rows ur where not ur.is_unit_unassigned
      ),
      'has_unassigned_unit', exists (
        select 1 from unit_rows ur where ur.is_unit_unassigned
      ),
      'test_count', o.test_count,
      'classified_test_count', o.classified_test_count,
      'unassigned_test_count', o.unassigned_test_count,
      'first_evaluated_at', o.first_evaluated_at,
      'last_evaluated_at', o.last_evaluated_at,
      'scores', jsonb_build_object(
        'round1', jsonb_build_object(
          'correct_count', o.round1_correct_count,
          'item_count', o.round1_item_count,
          'score_percent', case
            when o.round1_item_count = 0 then null
            else round(
              o.round1_correct_count::numeric * 100 / o.round1_item_count,
              2
            )
          end
        ),
        'round2_reflected', jsonb_build_object(
          'correct_count', o.round2_reflected_correct_count,
          'item_count', o.round2_reflected_item_count,
          'score_percent', case
            when o.round2_reflected_item_count = 0 then null
            else round(
              o.round2_reflected_correct_count::numeric
              * 100 / o.round2_reflected_item_count,
              2
            )
          end
        ),
        'final', jsonb_build_object(
          'correct_count', o.final_correct_count,
          'item_count', o.final_item_count,
          'score_percent', case
            when o.final_item_count = 0 then null
            else round(
              o.final_correct_count::numeric * 100 / o.final_item_count,
              2
            )
          end
        ),
        'teacher_final', jsonb_build_object(
          'correct_count', o.teacher_final_correct_count,
          'item_count', o.teacher_final_item_count,
          'score_percent', case
            when o.teacher_final_item_count = 0 then null
            else round(
              o.teacher_final_correct_count::numeric
              * 100 / o.teacher_final_item_count,
              2
            )
          end
        )
      )
    ),
    'units', coalesce((
      select jsonb_agg(
        jsonb_build_object(
          'unit', jsonb_build_object(
            'is_unassigned', ur.is_unit_unassigned,
            'grade_level', ur.grade_level,
            'curriculum_version', ur.curriculum_version,
            'subject', ur.subject,
            'major_unit_code', ur.major_unit_code,
            'major_unit_name', case
              when ur.is_unit_unassigned then '단원 미지정'
              else ur.major_unit_name
            end
          ),
          'test_count', ur.test_count,
          'returned_test_count', ur.returned_test_count,
          'tests_truncated', ur.tests_truncated,
          'manual_test_count', ur.manual_test_count,
          'round2_reflected_test_count', ur.round2_reflected_test_count,
          'final_confirmed_test_count', ur.final_confirmed_test_count,
          'first_evaluated_at', ur.first_evaluated_at,
          'last_evaluated_at', ur.last_evaluated_at,
          'scores', jsonb_build_object(
            'round1', jsonb_build_object(
              'correct_count', ur.round1_correct_count,
              'item_count', ur.round1_item_count,
              'score_percent', ur.round1_score_percent
            ),
            'round2_reflected', jsonb_build_object(
              'correct_count', ur.round2_reflected_correct_count,
              'item_count', ur.round2_reflected_item_count,
              'score_percent', ur.round2_reflected_score_percent
            ),
            'final', jsonb_build_object(
              'correct_count', ur.final_correct_count,
              'item_count', ur.final_item_count,
              'score_percent', ur.final_score_percent
            ),
            'teacher_final', jsonb_build_object(
              'correct_count', ur.teacher_final_correct_count,
              'item_count', ur.teacher_final_item_count,
              'score_percent', ur.teacher_final_score_percent
            )
          ),
          'retention', jsonb_build_object(
            'final_confirmed', jsonb_build_object(
              'test_count', coalesce(cardinality(ur.final_scores_asc), 0),
              'first_score_percent', (ur.final_scores_asc)[1],
              'previous_score_percent', (ur.final_scores_desc)[2],
              'latest_score_percent', (ur.final_scores_desc)[1],
              'change_from_first_percentage_points', case
                when coalesce(cardinality(ur.final_scores_asc), 0) < 2 then null
                else round(
                  (ur.final_scores_desc)[1] - (ur.final_scores_asc)[1],
                  1
                )
              end,
              'change_from_previous_percentage_points', case
                when coalesce(cardinality(ur.final_scores_desc), 0) < 2 then null
                else round(
                  (ur.final_scores_desc)[1] - (ur.final_scores_desc)[2],
                  1
                )
              end,
              'first_evaluated_at', (ur.final_dates_asc)[1],
              'previous_evaluated_at', (ur.final_dates_desc)[2],
              'latest_evaluated_at', (ur.final_dates_desc)[1],
              'days_since_latest', case
                when coalesce(cardinality(ur.final_dates_desc), 0) = 0
                  then null
                else (now() at time zone 'Asia/Seoul')::date
                  - (
                    (ur.final_dates_desc)[1]
                    at time zone 'Asia/Seoul'
                  )::date
              end,
              'days_from_first', case
                when coalesce(cardinality(ur.final_dates_asc), 0) < 2 then null
                else (
                  (ur.final_dates_desc)[1]
                  at time zone 'Asia/Seoul'
                )::date - (
                  (ur.final_dates_asc)[1]
                  at time zone 'Asia/Seoul'
                )::date
              end,
              'days_from_previous', case
                when coalesce(cardinality(ur.final_dates_desc), 0) < 2 then null
                else (
                  (ur.final_dates_desc)[1]
                  at time zone 'Asia/Seoul'
                )::date - (
                  (ur.final_dates_desc)[2]
                  at time zone 'Asia/Seoul'
                )::date
              end
            ),
            'round1', jsonb_build_object(
              'test_count', coalesce(cardinality(ur.round1_scores_asc), 0),
              'first_score_percent', (ur.round1_scores_asc)[1],
              'previous_score_percent', (ur.round1_scores_desc)[2],
              'latest_score_percent', (ur.round1_scores_desc)[1],
              'change_from_first_percentage_points', case
                when coalesce(cardinality(ur.round1_scores_asc), 0) < 2 then null
                else round(
                  (ur.round1_scores_desc)[1] - (ur.round1_scores_asc)[1],
                  1
                )
              end,
              'change_from_previous_percentage_points', case
                when coalesce(cardinality(ur.round1_scores_desc), 0) < 2 then null
                else round(
                  (ur.round1_scores_desc)[1] - (ur.round1_scores_desc)[2],
                  1
                )
              end,
              'first_evaluated_at', (ur.round1_dates_asc)[1],
              'previous_evaluated_at', (ur.round1_dates_desc)[2],
              'latest_evaluated_at', (ur.round1_dates_desc)[1],
              'days_since_latest', case
                when coalesce(cardinality(ur.round1_dates_desc), 0) = 0
                  then null
                else (now() at time zone 'Asia/Seoul')::date
                  - (
                    (ur.round1_dates_desc)[1]
                    at time zone 'Asia/Seoul'
                  )::date
              end,
              'days_from_first', case
                when coalesce(cardinality(ur.round1_dates_asc), 0) < 2 then null
                else (
                  (ur.round1_dates_desc)[1]
                  at time zone 'Asia/Seoul'
                )::date - (
                  (ur.round1_dates_asc)[1]
                  at time zone 'Asia/Seoul'
                )::date
              end,
              'days_from_previous', case
                when coalesce(cardinality(ur.round1_dates_desc), 0) < 2 then null
                else (
                  (ur.round1_dates_desc)[1]
                  at time zone 'Asia/Seoul'
                )::date - (
                  (ur.round1_dates_desc)[2]
                  at time zone 'Asia/Seoul'
                )::date
              end
            )
          ),
          'tests', ur.tests
        )
        order by
          ur.is_unit_unassigned,
          ur.grade_level,
          ur.curriculum_version,
          ur.subject,
          ur.major_unit_code
      )
      from unit_rows ur
    ), '[]'::jsonb)
  )
  into v_result
  from overall_rollup o;

  return v_result;
end;
$function$;

revoke execute on function auto_grading._student_major_unit_achievement_core(
  uuid,
  text,
  uuid
) from public, anon, authenticated, service_role;

-- --------------------------------------------------------------------------
-- 2. 관리자 직접 조회용 학생코드 RPC
-- --------------------------------------------------------------------------
create or replace function auto_grading.get_student_major_unit_achievement_by_code(
  p_student_code text,
  p_scope text,
  p_course_id uuid
)
returns jsonb
language plpgsql
stable
security definer
set search_path to 'auto_grading', 'public'
as $function$
declare
  v_student_id uuid;
  v_scope text := lower(btrim(coalesce(p_scope, '')));
  v_course_name text;
  v_course_is_active boolean;
  v_is_unassigned boolean := false;
  v_payload jsonb;
begin
  select s.id
    into v_student_id
  from auto_grading.students s
  where s.student_code = btrim(p_student_code)
    and coalesce(s.is_active, true);

  if v_student_id is null then
    raise exception 'STUDENT_NOT_FOUND'
      using errcode = 'P0001';
  end if;

  if v_scope not in ('all', 'course', 'unassigned') then
    raise exception 'INVALID_SCOPE'
      using errcode = 'P0001';
  end if;

  if v_scope = 'course' and p_course_id is null then
    raise exception 'COURSE_ID_REQUIRED_FOR_SCOPE'
      using errcode = 'P0001';
  end if;

  if v_scope = 'all' then
    v_course_name := '전체 누적';
    v_course_is_active := null;
  elsif v_scope = 'unassigned' then
    v_course_name := '강좌 미지정 과거 기록';
    v_course_is_active := false;
    v_is_unassigned := true;
  else
    select c.course_name, c.is_active
      into v_course_name, v_course_is_active
    from auto_grading.courses c
    where c.id = p_course_id;

    if not found then
      raise exception 'COURSE_NOT_FOUND'
        using errcode = 'P0001';
    end if;

    if not (
      exists (
        select 1
        from auto_grading.student_courses sc
        where sc.student_id = v_student_id
          and sc.course_id = p_course_id
      )
      or exists (
        select 1
        from auto_grading.assignments a
        where a.student_id = v_student_id
          and a.course_id = p_course_id
      )
      or exists (
        select 1
        from auto_grading.attempts at
        where at.student_id = v_student_id
          and at.course_id = p_course_id
      )
    ) then
      raise exception 'ACHIEVEMENT_COURSE_NOT_FOUND'
        using errcode = 'P0001';
    end if;
  end if;

  v_payload := auto_grading._student_major_unit_achievement_core(
    v_student_id,
    v_scope,
    p_course_id
  );

  return jsonb_build_object(
    'scope', v_scope,
    'course', jsonb_build_object(
      'course_id', case when v_scope = 'course' then p_course_id else null end,
      'course_name', v_course_name,
      'course_is_active', v_course_is_active,
      'is_unassigned', v_is_unassigned
    )
  ) || v_payload;
end;
$function$;

revoke execute on function auto_grading.get_student_major_unit_achievement_by_code(
  text,
  text,
  uuid
) from public, anon;
grant execute on function auto_grading.get_student_major_unit_achievement_by_code(
  text,
  text,
  uuid
) to authenticated, service_role;

-- --------------------------------------------------------------------------
-- 3. 학생 공개 링크용 토큰 RPC
-- --------------------------------------------------------------------------
create or replace function auto_grading.get_student_major_unit_achievement_by_token(
  p_token uuid,
  p_scope text,
  p_course_id uuid
)
returns jsonb
language plpgsql
stable
security definer
set search_path to 'auto_grading', 'public'
as $function$
declare
  v_code text;
begin
  select s.student_code
    into v_code
  from auto_grading.student_public_links l
  join auto_grading.students s on s.id = l.student_id
  where l.public_token = p_token
    and l.is_active = true
    and (l.expires_at is null or l.expires_at > now())
    and s.is_active = true;

  if v_code is null then
    return null;
  end if;

  return auto_grading.get_student_major_unit_achievement_by_code(
    v_code,
    p_scope,
    p_course_id
  );
end;
$function$;

revoke execute on function auto_grading.get_student_major_unit_achievement_by_token(
  uuid,
  text,
  uuid
) from public;
grant execute on function auto_grading.get_student_major_unit_achievement_by_token(
  uuid,
  text,
  uuid
) to anon, authenticated, service_role;

comment on function auto_grading._student_major_unit_achievement_core(
  uuid,
  text,
  uuid
) is '비공개 코어. 강좌 범위별 대단원 요약·테스트 상세·반복 평가 추세를 jsonb로 반환.';

comment on function auto_grading.get_student_major_unit_achievement_by_code(
  text,
  text,
  uuid
) is '학생코드와 all/course/unassigned 범위로 대단원별 누적 성취도를 반환. 관리자 직접 조회용.';

comment on function auto_grading.get_student_major_unit_achievement_by_token(
  uuid,
  text,
  uuid
) is '공개 토큰 검증 후 대단원별 누적 성취도를 반환.';

notify pgrst, 'reload schema';

commit;

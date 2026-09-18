-- ============================================================================
-- course_enrollment_achievement_stage6_part2a_course_achievement.sql
--
-- 6단계 part 2A: 공통 성취도 코어 + 범위가 명시된 신규 조회 RPC.
--
-- 중요:
--   - 이 단계에서는 기존 get_student_stats_by_code 및
--     get_student_assignment_history_by_code 본문을 변경하지 않는다.
--   - 배포 후 audit_course_enrollment_achievement_stage6_part2a_equivalence.sql
--     결과에서 구·신 통계/시험 기록 불일치가 모두 0인지 확인한다.
--   - 확인 전에는 part 2B를 실행하지 않는다.
--
-- p_scope 계약:
--   all        : p_course_id를 무시하고 학생의 모든 attempt/assignment 조회
--   course     : p_course_id 필수, 해당 강좌만 조회
--   unassigned : p_course_id를 무시하고 course_id is null인 과거 기록만 조회
-- ============================================================================

begin;

-- --------------------------------------------------------------------------
-- 1. 비공개 통계 코어
--
-- stats는 기존 공개 payload와 동일한 5개 키만 유지한다.
-- basis는 감사에서 분자·분모 파티션 정합성을 검증하기 위한 내부 값이다.
-- --------------------------------------------------------------------------
create or replace function auto_grading._student_achievement_stats_core(
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
  v_total_solved integer := 0;
  v_first_correct integer := 0;
  v_round2_correct integer := 0;
  v_round2_items integer := 0;
  v_final_correct integer := 0;
  v_final_items integer := 0;
  v_teacher_final_correct integer := 0;
  v_teacher_final_items integer := 0;
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

  select
    coalesce(sum(at.total_items), 0),
    coalesce(sum(at.first_correct_count), 0),
    coalesce(sum(at.final_correct_count) filter (
      where ts.source_type is distinct from 'manual'
    ), 0),
    coalesce(sum(at.total_items) filter (
      where ts.source_type is distinct from 'manual'
    ), 0),
    coalesce(sum(
      case
        when at.teacher_final_correct_count is not null
          then at.teacher_final_correct_count
        when ts.source_type = 'manual' then null
        when at.status = 'completed' then at.final_correct_count
        else null
      end
    ), 0),
    coalesce(sum(
      case
        when at.teacher_final_correct_count is not null then at.total_items
        when ts.source_type = 'manual' then null
        when at.status = 'completed' then at.total_items
        else null
      end
    ), 0),
    coalesce(sum(
      case
        when at.teacher_final_correct_count is not null
          then at.teacher_final_correct_count
        else null
      end
    ), 0),
    coalesce(sum(
      case
        when at.teacher_final_correct_count is not null then at.total_items
        else null
      end
    ), 0)
  into
    v_total_solved,
    v_first_correct,
    v_round2_correct,
    v_round2_items,
    v_final_correct,
    v_final_items,
    v_teacher_final_correct,
    v_teacher_final_items
  from auto_grading.attempts at
  join auto_grading.test_sets ts on ts.id = at.test_set_id
  where at.student_id = p_student_id
    and at.status in ('completed', 'needs_review')
    and (
      v_scope = 'all'
      or (v_scope = 'course' and at.course_id = p_course_id)
      or (v_scope = 'unassigned' and at.course_id is null)
    );

  return jsonb_build_object(
    'stats', jsonb_build_object(
      'total_solved', v_total_solved,
      'round1_accuracy', case
        when v_total_solved = 0 then null
        else round(
          (v_first_correct::numeric / v_total_solved::numeric) * 100,
          2
        )
      end,
      'round2_accuracy', case
        when v_round2_items = 0 then null
        else round(
          (v_round2_correct::numeric / v_round2_items::numeric) * 100,
          2
        )
      end,
      'final_accuracy', case
        when v_final_items = 0 then null
        else round(
          (v_final_correct::numeric / v_final_items::numeric) * 100,
          2
        )
      end,
      'teacher_final_accuracy', case
        when v_teacher_final_items = 0 then null
        else round(
          (
            v_teacher_final_correct::numeric
            / v_teacher_final_items::numeric
          ) * 100,
          2
        )
      end
    ),
    'basis', jsonb_build_object(
      'round1_correct_count', v_first_correct,
      'round1_item_count', v_total_solved,
      'round2_correct_count', v_round2_correct,
      'round2_item_count', v_round2_items,
      'final_correct_count', v_final_correct,
      'final_item_count', v_final_items,
      'teacher_final_correct_count', v_teacher_final_correct,
      'teacher_final_item_count', v_teacher_final_items
    )
  );
end;
$function$;

revoke execute on function auto_grading._student_achievement_stats_core(
  uuid,
  text,
  uuid
) from public, anon, authenticated, service_role;

-- --------------------------------------------------------------------------
-- 2. 비공개 시험 기록 코어
--
-- 기존 get_student_assignment_history_by_code의 반환 컬럼·타입·폴백·정렬을
-- 그대로 유지한다. stats는 attempt 기반, history는 assignment 기반이라는 기존
-- 비대칭도 의도적으로 보존한다.
-- --------------------------------------------------------------------------
create or replace function auto_grading._student_achievement_history_core(
  p_student_id uuid,
  p_scope text,
  p_course_id uuid,
  p_limit integer
)
returns table (
  assignment_id uuid,
  test_set_id uuid,
  test_title text,
  source_type text,
  assigned_at timestamptz,
  event_date date,
  total_items integer,
  round1_correct_count integer,
  round1_score_percent numeric(5,1),
  round2_correct_count integer,
  round2_score_percent numeric(5,1),
  final_correct_count integer,
  final_score_percent numeric(5,1),
  teacher_final_score_percent numeric(5,1),
  last_activity_at timestamptz
)
language plpgsql
stable
security definer
set search_path to 'auto_grading', 'public'
as $function$
declare
  v_scope text := lower(btrim(coalesce(p_scope, '')));
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

  return query
  with base_assignments as (
    select
      a.id as assignment_id,
      a.test_set_id,
      coalesce(a.assigned_at, a.created_at) as assigned_at,
      a.updated_at as assignment_updated_at
    from auto_grading.assignments a
    where a.student_id = p_student_id
      and (
        v_scope = 'all'
        or (v_scope = 'course' and a.course_id = p_course_id)
        or (v_scope = 'unassigned' and a.course_id is null)
      )
  ),
  test_item_counts as (
    select
      ti.test_set_id,
      count(*)::integer as item_count
    from auto_grading.test_items ti
    where ti.test_set_id in (
      select distinct ba.test_set_id
      from base_assignments ba
    )
    group by ti.test_set_id
  ),
  ranked_attempts as (
    select
      at.assignment_id,
      at.total_items as attempt_total_items,
      at.first_correct_count,
      at.final_correct_count,
      at.teacher_final_correct_count,
      at.first_score_percent,
      at.final_score_percent,
      at.teacher_final_score_percent,
      at.started_at,
      at.round1_submitted_at,
      at.round2_submitted_at,
      at.completed_at,
      at.updated_at,
      row_number() over (
        partition by at.assignment_id
        order by coalesce(
          at.completed_at,
          at.round2_submitted_at,
          at.round1_submitted_at,
          at.updated_at,
          at.started_at
        ) desc
      ) as rn
    from auto_grading.attempts at
    where at.student_id = p_student_id
      and (
        v_scope = 'all'
        or (v_scope = 'course' and at.course_id = p_course_id)
        or (v_scope = 'unassigned' and at.course_id is null)
      )
  ),
  latest_attempt as (
    select *
    from ranked_attempts
    where rn = 1
  ),
  history_base as (
    select
      ba.assignment_id,
      ba.test_set_id,
      ts.title as test_title,
      ts.source_type,
      ba.assigned_at,
      coalesce(
        la.completed_at,
        la.round2_submitted_at,
        la.round1_submitted_at,
        la.started_at,
        ba.assigned_at
      )::date as event_date,
      coalesce(
        nullif(tic.item_count, 0),
        la.attempt_total_items,
        ts.total_items,
        0
      ) as total_items,
      la.first_correct_count as round1_correct_count,
      case
        when la.round1_submitted_at is not null
          and la.first_score_percent is not null
          then round(la.first_score_percent::numeric, 1)
        when la.round1_submitted_at is not null
          and la.first_correct_count is not null
          and coalesce(
            nullif(tic.item_count, 0),
            la.attempt_total_items,
            ts.total_items,
            0
          ) > 0
          then round(
            (la.first_correct_count::numeric * 100)
            / coalesce(
              nullif(tic.item_count, 0),
              la.attempt_total_items,
              ts.total_items
            ),
            1
          )
        else null
      end as round1_score_percent,
      case
        when ts.source_type = 'manual' then null
        else la.final_correct_count
      end as round2_correct_count,
      case
        when ts.source_type = 'manual' then null
        when la.round2_submitted_at is not null
          and la.final_score_percent is not null
          then round(la.final_score_percent::numeric, 1)
        when la.round2_submitted_at is not null
          and la.final_correct_count is not null
          and coalesce(
            nullif(tic.item_count, 0),
            la.attempt_total_items,
            ts.total_items,
            0
          ) > 0
          then round(
            (la.final_correct_count::numeric * 100)
            / coalesce(
              nullif(tic.item_count, 0),
              la.attempt_total_items,
              ts.total_items
            ),
            1
          )
        else null
      end as round2_score_percent,
      case
        when ts.source_type = 'manual' then la.teacher_final_correct_count
        else coalesce(la.teacher_final_correct_count, la.final_correct_count)
      end as final_correct_count,
      case
        when ts.source_type = 'manual' then
          case
            when la.teacher_final_score_percent is not null
              then round(la.teacher_final_score_percent::numeric, 1)
            else null
          end
        else
          case
            when coalesce(
              la.teacher_final_score_percent,
              la.final_score_percent
            ) is not null
              then round(coalesce(
                la.teacher_final_score_percent,
                la.final_score_percent
              )::numeric, 1)
            when coalesce(
              la.teacher_final_correct_count,
              la.final_correct_count
            ) is not null
              and coalesce(
                nullif(tic.item_count, 0),
                la.attempt_total_items,
                ts.total_items,
                0
              ) > 0
              then round(
                (coalesce(
                  la.teacher_final_correct_count,
                  la.final_correct_count
                )::numeric * 100)
                / coalesce(
                  nullif(tic.item_count, 0),
                  la.attempt_total_items,
                  ts.total_items
                ),
                1
              )
            else null
          end
      end as final_score_percent,
      case
        when la.teacher_final_score_percent is not null
          then round(la.teacher_final_score_percent::numeric, 1)
        when la.teacher_final_correct_count is not null
          and coalesce(
            nullif(tic.item_count, 0),
            la.attempt_total_items,
            ts.total_items,
            0
          ) > 0
          then round(
            (la.teacher_final_correct_count::numeric * 100)
            / coalesce(
              nullif(tic.item_count, 0),
              la.attempt_total_items,
              ts.total_items
            ),
            1
          )
        else null
      end as teacher_final_score_percent,
      coalesce(
        la.completed_at,
        la.round2_submitted_at,
        la.round1_submitted_at,
        la.updated_at,
        ba.assignment_updated_at,
        ba.assigned_at
      ) as last_activity_at
    from base_assignments ba
    left join latest_attempt la on la.assignment_id = ba.assignment_id
    left join auto_grading.test_sets ts on ts.id = ba.test_set_id
    left join test_item_counts tic on tic.test_set_id = ba.test_set_id
  )
  select
    hb.assignment_id,
    hb.test_set_id,
    hb.test_title,
    hb.source_type,
    hb.assigned_at,
    hb.event_date,
    hb.total_items,
    hb.round1_correct_count,
    hb.round1_score_percent,
    hb.round2_correct_count,
    hb.round2_score_percent,
    hb.final_correct_count,
    hb.final_score_percent,
    hb.teacher_final_score_percent,
    hb.last_activity_at
  from history_base hb
  order by hb.last_activity_at desc, hb.assignment_id desc
  limit greatest(coalesce(p_limit, 200), 1);
end;
$function$;

revoke execute on function auto_grading._student_achievement_history_core(
  uuid,
  text,
  uuid,
  integer
) from public, anon, authenticated, service_role;

-- --------------------------------------------------------------------------
-- 3. 학생에게 표시할 성취도 강좌 목록
-- --------------------------------------------------------------------------
create or replace function auto_grading.get_student_achievement_courses_by_code(
  p_student_code text
)
returns jsonb
language plpgsql
security definer
set search_path to 'auto_grading', 'public'
as $function$
declare
  v_student_id uuid;
  v_result jsonb;
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

  with course_keys as (
    select sc.course_id
    from auto_grading.student_courses sc
    where sc.student_id = v_student_id

    union

    select a.course_id
    from auto_grading.assignments a
    where a.student_id = v_student_id

    union

    select at.course_id
    from auto_grading.attempts at
    where at.student_id = v_student_id
  ),
  course_rows as (
    select
      ck.course_id,
      coalesce(c.course_name, '강좌 미지정 과거 기록') as course_name,
      coalesce(c.is_active, false) as course_is_active,
      ck.course_id is null as is_unassigned,
      exists (
        select 1
        from auto_grading.student_courses sc
        where sc.student_id = v_student_id
          and sc.course_id is not distinct from ck.course_id
      ) as has_enrollment_history,
      exists (
        select 1
        from auto_grading.student_courses sc
        where sc.student_id = v_student_id
          and sc.course_id is not distinct from ck.course_id
          and coalesce(sc.is_active, sc.ended_at is null)
      ) as enrollment_is_active,
      (
        select count(*)::integer
        from auto_grading.assignments a
        where a.student_id = v_student_id
          and a.course_id is not distinct from ck.course_id
      ) as assignment_count,
      (
        select count(*)::integer
        from auto_grading.attempts at
        where at.student_id = v_student_id
          and at.course_id is not distinct from ck.course_id
      ) as attempt_count,
      (
        select count(*)::integer
        from auto_grading.attempts at
        where at.student_id = v_student_id
          and at.course_id is not distinct from ck.course_id
          and at.status in ('completed', 'needs_review')
      ) as achievement_attempt_count,
      greatest(
        (
          select max(coalesce(sc.ended_at, sc.joined_at, sc.created_at))
          from auto_grading.student_courses sc
          where sc.student_id = v_student_id
            and sc.course_id is not distinct from ck.course_id
        ),
        (
          select max(coalesce(a.updated_at, a.assigned_at, a.created_at))
          from auto_grading.assignments a
          where a.student_id = v_student_id
            and a.course_id is not distinct from ck.course_id
        ),
        (
          select max(coalesce(
            at.completed_at,
            at.round2_submitted_at,
            at.round1_submitted_at,
            at.updated_at,
            at.started_at
          ))
          from auto_grading.attempts at
          where at.student_id = v_student_id
            and at.course_id is not distinct from ck.course_id
        )
      ) as last_activity_at
    from course_keys ck
    left join auto_grading.courses c on c.id = ck.course_id
  )
  select coalesce(
    jsonb_agg(
      to_jsonb(cr)
      order by
        cr.enrollment_is_active desc,
        cr.course_is_active desc,
        cr.last_activity_at desc nulls last,
        cr.course_name,
        cr.course_id
    ),
    '[]'::jsonb
  )
  into v_result
  from course_rows cr;

  return v_result;
end;
$function$;

revoke execute on function auto_grading.get_student_achievement_courses_by_code(text)
  from public, anon;
grant execute on function auto_grading.get_student_achievement_courses_by_code(text)
  to authenticated, service_role;

-- --------------------------------------------------------------------------
-- 4. 명시된 범위의 성취도 + 시험 기록 번들
-- --------------------------------------------------------------------------
create or replace function auto_grading.get_student_achievement_by_code(
  p_student_code text,
  p_scope text,
  p_course_id uuid,
  p_limit integer default 200
)
returns jsonb
language plpgsql
security definer
set search_path to 'auto_grading', 'public'
as $function$
declare
  v_student_id uuid;
  v_scope text := lower(btrim(coalesce(p_scope, '')));
  v_course_name text;
  v_course_is_active boolean;
  v_is_unassigned boolean := false;
  v_stats_payload jsonb;
  v_history jsonb := '[]'::jsonb;
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

  v_stats_payload := auto_grading._student_achievement_stats_core(
    v_student_id,
    v_scope,
    p_course_id
  );

  select coalesce(
    jsonb_agg(
      to_jsonb(h)
      order by h.last_activity_at desc, h.assignment_id desc
    ),
    '[]'::jsonb
  )
  into v_history
  from auto_grading._student_achievement_history_core(
    v_student_id,
    v_scope,
    p_course_id,
    p_limit
  ) h;

  return jsonb_build_object(
    'scope', v_scope,
    'course', jsonb_build_object(
      'course_id', case when v_scope = 'course' then p_course_id else null end,
      'course_name', v_course_name,
      'course_is_active', v_course_is_active,
      'is_unassigned', v_is_unassigned
    ),
    'stats', v_stats_payload -> 'stats',
    'history', v_history
  );
end;
$function$;

revoke execute on function auto_grading.get_student_achievement_by_code(
  text,
  text,
  uuid,
  integer
) from public, anon;
grant execute on function auto_grading.get_student_achievement_by_code(
  text,
  text,
  uuid,
  integer
) to authenticated, service_role;

-- --------------------------------------------------------------------------
-- 5. 공개 토큰용 강좌 목록/범위 성취도 래퍼
-- --------------------------------------------------------------------------
create or replace function auto_grading.get_student_achievement_courses_by_token(
  p_token uuid
)
returns jsonb
language plpgsql
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
    return '[]'::jsonb;
  end if;

  return auto_grading.get_student_achievement_courses_by_code(v_code);
end;
$function$;

create or replace function auto_grading.get_student_achievement_by_token(
  p_token uuid,
  p_scope text,
  p_course_id uuid,
  p_limit integer default 200
)
returns jsonb
language plpgsql
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

  return auto_grading.get_student_achievement_by_code(
    v_code,
    p_scope,
    p_course_id,
    p_limit
  );
end;
$function$;

revoke execute on function auto_grading.get_student_achievement_courses_by_token(uuid)
  from public;
revoke execute on function auto_grading.get_student_achievement_by_token(
  uuid,
  text,
  uuid,
  integer
) from public;

grant execute on function auto_grading.get_student_achievement_courses_by_token(uuid)
  to anon, authenticated, service_role;
grant execute on function auto_grading.get_student_achievement_by_token(
  uuid,
  text,
  uuid,
  integer
) to anon, authenticated, service_role;

comment on function auto_grading._student_achievement_stats_core(
  uuid,
  text,
  uuid
) is '비공개 공통 코어. all/course/unassigned 범위별 성취도 stats와 감사용 basis를 반환.';

comment on function auto_grading._student_achievement_history_core(
  uuid,
  text,
  uuid,
  integer
) is '비공개 공통 코어. 기존 시험 기록 반환 계약을 유지하며 범위별 기록을 반환.';

comment on function auto_grading.get_student_achievement_courses_by_code(text) is
  '학생의 수강이력·assignment·attempt에 나타나는 성취도 강좌 목록. 관리자 직접 조회용.';

comment on function auto_grading.get_student_achievement_by_code(
  text,
  text,
  uuid,
  integer
) is '학생의 all/course/unassigned 범위별 성취도와 시험 기록을 jsonb로 반환.';

comment on function auto_grading.get_student_achievement_courses_by_token(uuid) is
  '공개 토큰 검증 후 학생의 성취도 강좌 목록을 반환.';

comment on function auto_grading.get_student_achievement_by_token(
  uuid,
  text,
  uuid,
  integer
) is '공개 토큰 검증 후 all/course/unassigned 범위의 성취도와 시험 기록을 반환.';

notify pgrst, 'reload schema';

commit;

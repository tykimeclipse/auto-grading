-- ============================================================================
-- audit_course_enrollment_achievement_stage7_major_unit_preflight.sql
--
-- 7단계 대단원별 누적 성취도 구현 전 운영 DB 사전점검.
-- SELECT만 수행하며 데이터와 스키마를 변경하지 않는다.
--
-- 집계 계약:
--   - 대상 attempt: status in ('completed', 'needs_review')
--   - 대단원 키: grade_level + curriculum_version + subject + major_unit_code
--   - 문항 수: attempts.total_items 스냅샷
--   - 2차 반영: manual 제외, final_correct_count / total_items
--   - 최종: 기존 _student_achievement_stats_core 규칙과 동일
--   - curriculum_units.is_active 여부와 무관하게 과거 성취도를 보존
--
-- 다음 단계 진행 차단 조건:
--   1. schema_contract.missing_or_unvalidated_constraint_count > 0
--   2. achievement_metadata_coverage.invalid_metadata_attempt_count > 0
--   3. major_unit_name_integrity.conflict_group_count > 0
--   4. assignment_attempt_invariant.multiple_attempt_assignment_count > 0
--   5. unit_partition_integrity.mismatch_student_count > 0
--
-- 정보성 항목:
--   - 단원 미지정 완료 평가는 허용하며 향후 소급 분류 대상으로 사용한다.
--   - 비활성 curriculum_units 참조 평가는 집계에서 제외하지 않는다.
--
-- 결과 6행의 section / blocking_issue_count / details를 공유한다.
-- ============================================================================

with
target_constraints as (
  select
    c.conname,
    c.contype,
    c.convalidated,
    pg_get_constraintdef(c.oid) as definition
  from pg_constraint c
  where c.conrelid in (
      'auto_grading.test_sets'::regclass,
      'auto_grading.curriculum_units'::regclass
    )
    and c.conname in (
      'test_sets_unit_code_format_chk',
      'test_sets_curriculum_ref_all_or_none_chk',
      'test_sets_curriculum_units_fk',
      'curriculum_units_unique',
      'curriculum_units_hierarchy_chk'
    )
),
expected_constraints as (
  select unnest(array[
    'test_sets_unit_code_format_chk',
    'test_sets_curriculum_ref_all_or_none_chk',
    'test_sets_curriculum_units_fk',
    'curriculum_units_unique',
    'curriculum_units_hierarchy_chk'
  ])::text as conname
),
constraint_audit as (
  select
    e.conname,
    c.contype,
    coalesce(c.convalidated, false) as convalidated,
    c.definition,
    c.conname is null as is_missing
  from expected_constraints e
  left join target_constraints c on c.conname = e.conname
),
target_columns as (
  select
    c.table_name,
    c.column_name,
    c.data_type,
    c.is_nullable
  from information_schema.columns c
  where c.table_schema = 'auto_grading'
    and (
      (c.table_name = 'test_sets' and c.column_name in (
        'grade_level', 'curriculum_version', 'subject', 'unit_code'
      ))
      or
      (c.table_name = 'curriculum_units' and c.column_name in (
        'grade_level', 'curriculum_version', 'subject', 'unit_code',
        'major_unit_code', 'major_unit_name', 'unit_level', 'is_active'
      ))
    )
),
eligible_attempts as (
  select
    at.id as attempt_id,
    at.assignment_id,
    at.student_id,
    at.test_set_id,
    at.course_id,
    at.status,
    at.total_items,
    at.first_correct_count,
    at.final_correct_count,
    at.teacher_final_correct_count,
    coalesce(
      at.completed_at,
      at.round2_submitted_at,
      at.round1_submitted_at,
      at.updated_at,
      at.started_at
    ) as evaluated_at,
    ts.title as test_title,
    ts.source_type,
    ts.grade_level,
    ts.curriculum_version,
    ts.subject,
    ts.unit_code,
    cu.major_unit_code,
    cu.major_unit_name,
    cu.unit_level,
    cu.is_active as curriculum_unit_is_active,
    case
      when ts.grade_level is null
       and ts.curriculum_version is null
       and ts.subject is null
       and ts.unit_code is null
        then 'unassigned'
      when ts.grade_level is not null
       and ts.curriculum_version is not null
       and ts.subject is not null
       and ts.unit_code is not null
       and cu.unit_code is not null
        then 'classified'
      else 'invalid_metadata'
    end as metadata_state
  from auto_grading.attempts at
  join auto_grading.test_sets ts on ts.id = at.test_set_id
  left join auto_grading.curriculum_units cu
    on cu.grade_level = ts.grade_level
   and cu.curriculum_version = ts.curriculum_version
   and cu.subject = ts.subject
   and cu.unit_code = ts.unit_code
  where at.status in ('completed', 'needs_review')
),
unassigned_test_sets as (
  select
    ea.test_set_id,
    ea.test_title,
    ea.source_type,
    count(*)::integer as achievement_attempt_count,
    count(distinct ea.student_id)::integer as student_count,
    min(ea.evaluated_at) as first_evaluated_at,
    max(ea.evaluated_at) as last_evaluated_at
  from eligible_attempts ea
  where ea.metadata_state = 'unassigned'
  group by ea.test_set_id, ea.test_title, ea.source_type
),
invalid_metadata_test_sets as (
  select
    ea.test_set_id,
    ea.test_title,
    ea.source_type,
    ea.grade_level,
    ea.curriculum_version,
    ea.subject,
    ea.unit_code,
    count(*)::integer as achievement_attempt_count
  from eligible_attempts ea
  where ea.metadata_state = 'invalid_metadata'
  group by
    ea.test_set_id,
    ea.test_title,
    ea.source_type,
    ea.grade_level,
    ea.curriculum_version,
    ea.subject,
    ea.unit_code
),
major_unit_name_groups as (
  select
    cu.grade_level,
    cu.curriculum_version,
    cu.subject,
    cu.major_unit_code,
    count(*)::integer as curriculum_row_count,
    count(distinct btrim(cu.major_unit_name))::integer as distinct_name_count,
    array_agg(distinct btrim(cu.major_unit_name) order by btrim(cu.major_unit_name))
      as major_unit_names,
    count(*) filter (where cu.unit_level = 'major')::integer as major_row_count,
    max(cu.major_unit_name) filter (where cu.unit_level = 'major') as major_row_name
  from auto_grading.curriculum_units cu
  group by
    cu.grade_level,
    cu.curriculum_version,
    cu.subject,
    cu.major_unit_code
),
major_unit_name_conflicts as (
  select *
  from major_unit_name_groups mug
  where mug.distinct_name_count <> 1
),
major_unit_missing_major_rows as (
  select *
  from major_unit_name_groups mug
  where mug.major_row_count = 0
),
assignment_attempt_counts as (
  select
    at.assignment_id,
    count(*)::integer as attempt_count,
    array_agg(at.id order by at.created_at, at.id) as attempt_ids,
    min(at.created_at) as first_attempt_created_at,
    max(at.created_at) as last_attempt_created_at
  from auto_grading.attempts at
  where at.assignment_id is not null
  group by at.assignment_id
),
multiple_attempt_assignments as (
  select *
  from assignment_attempt_counts aac
  where aac.attempt_count > 1
),
all_student_basis as (
  select
    ea.student_id,
    coalesce(sum(ea.first_correct_count), 0)::bigint as round1_correct_count,
    coalesce(sum(ea.total_items), 0)::bigint as round1_item_count,
    coalesce(sum(ea.final_correct_count) filter (
      where ea.source_type is distinct from 'manual'
    ), 0)::bigint as round2_correct_count,
    coalesce(sum(ea.total_items) filter (
      where ea.source_type is distinct from 'manual'
    ), 0)::bigint as round2_item_count,
    coalesce(sum(
      case
        when ea.teacher_final_correct_count is not null
          then ea.teacher_final_correct_count
        when ea.source_type = 'manual' then null
        when ea.status = 'completed' then ea.final_correct_count
        else null
      end
    ), 0)::bigint as final_correct_count,
    coalesce(sum(
      case
        when ea.teacher_final_correct_count is not null then ea.total_items
        when ea.source_type = 'manual' then null
        when ea.status = 'completed' then ea.total_items
        else null
      end
    ), 0)::bigint as final_item_count,
    coalesce(sum(
      case
        when ea.teacher_final_correct_count is not null
          then ea.teacher_final_correct_count
        else null
      end
    ), 0)::bigint as teacher_final_correct_count,
    coalesce(sum(
      case
        when ea.teacher_final_correct_count is not null then ea.total_items
        else null
      end
    ), 0)::bigint as teacher_final_item_count
  from eligible_attempts ea
  group by ea.student_id
),
partition_basis as (
  select
    ea.student_id,
    case
      when ea.metadata_state = 'classified' then concat_ws(
        '|',
        ea.grade_level,
        ea.curriculum_version,
        ea.subject,
        ea.major_unit_code
      )
      when ea.metadata_state = 'unassigned' then '__unassigned__'
      else '__invalid_metadata__'
    end as partition_key,
    ea.metadata_state,
    coalesce(sum(ea.first_correct_count), 0)::bigint as round1_correct_count,
    coalesce(sum(ea.total_items), 0)::bigint as round1_item_count,
    coalesce(sum(ea.final_correct_count) filter (
      where ea.source_type is distinct from 'manual'
    ), 0)::bigint as round2_correct_count,
    coalesce(sum(ea.total_items) filter (
      where ea.source_type is distinct from 'manual'
    ), 0)::bigint as round2_item_count,
    coalesce(sum(
      case
        when ea.teacher_final_correct_count is not null
          then ea.teacher_final_correct_count
        when ea.source_type = 'manual' then null
        when ea.status = 'completed' then ea.final_correct_count
        else null
      end
    ), 0)::bigint as final_correct_count,
    coalesce(sum(
      case
        when ea.teacher_final_correct_count is not null then ea.total_items
        when ea.source_type = 'manual' then null
        when ea.status = 'completed' then ea.total_items
        else null
      end
    ), 0)::bigint as final_item_count,
    coalesce(sum(
      case
        when ea.teacher_final_correct_count is not null
          then ea.teacher_final_correct_count
        else null
      end
    ), 0)::bigint as teacher_final_correct_count,
    coalesce(sum(
      case
        when ea.teacher_final_correct_count is not null then ea.total_items
        else null
      end
    ), 0)::bigint as teacher_final_item_count
  from eligible_attempts ea
  where ea.metadata_state in ('classified', 'unassigned')
  group by ea.student_id, partition_key, ea.metadata_state
),
partition_student_sum as (
  select
    pb.student_id,
    coalesce(sum(pb.round1_correct_count), 0)::bigint as round1_correct_count,
    coalesce(sum(pb.round1_item_count), 0)::bigint as round1_item_count,
    coalesce(sum(pb.round2_correct_count), 0)::bigint as round2_correct_count,
    coalesce(sum(pb.round2_item_count), 0)::bigint as round2_item_count,
    coalesce(sum(pb.final_correct_count), 0)::bigint as final_correct_count,
    coalesce(sum(pb.final_item_count), 0)::bigint as final_item_count,
    coalesce(sum(pb.teacher_final_correct_count), 0)::bigint
      as teacher_final_correct_count,
    coalesce(sum(pb.teacher_final_item_count), 0)::bigint
      as teacher_final_item_count
  from partition_basis pb
  group by pb.student_id
),
partition_mismatches as (
  select
    s.student_code,
    s.name as student_name,
    to_jsonb(ab) - 'student_id' as all_basis,
    to_jsonb(ps) - 'student_id' as partition_basis
  from all_student_basis ab
  join auto_grading.students s on s.id = ab.student_id
  left join partition_student_sum ps on ps.student_id = ab.student_id
  where row(
    ab.round1_correct_count,
    ab.round1_item_count,
    ab.round2_correct_count,
    ab.round2_item_count,
    ab.final_correct_count,
    ab.final_item_count,
    ab.teacher_final_correct_count,
    ab.teacher_final_item_count
  ) is distinct from row(
    coalesce(ps.round1_correct_count, 0),
    coalesce(ps.round1_item_count, 0),
    coalesce(ps.round2_correct_count, 0),
    coalesce(ps.round2_item_count, 0),
    coalesce(ps.final_correct_count, 0),
    coalesce(ps.final_item_count, 0),
    coalesce(ps.teacher_final_correct_count, 0),
    coalesce(ps.teacher_final_item_count, 0)
  )
),
audit_rows as (
  select
    10 as sort_order,
    'schema_contract'::text as section,
    (
      select count(*)::integer
      from constraint_audit ca
      where ca.is_missing or not ca.convalidated
    ) as blocking_issue_count,
    jsonb_build_object(
      'expected_constraint_count', 5,
      'installed_and_validated_constraint_count', (
        select count(*)
        from constraint_audit ca
        where not ca.is_missing and ca.convalidated
      ),
      'missing_or_unvalidated_constraint_count', (
        select count(*)
        from constraint_audit ca
        where ca.is_missing or not ca.convalidated
      ),
      'constraints', coalesce((
        select jsonb_agg(to_jsonb(ca) order by ca.conname)
        from constraint_audit ca
      ), '[]'::jsonb),
      'columns', coalesce((
        select jsonb_agg(to_jsonb(tc) order by tc.table_name, tc.column_name)
        from target_columns tc
      ), '[]'::jsonb)
    ) as details

  union all

  select
    20,
    'achievement_metadata_coverage',
    count(*) filter (where ea.metadata_state = 'invalid_metadata')::integer,
    jsonb_build_object(
      'eligible_attempt_count', count(*),
      'classified_attempt_count', count(*) filter (
        where ea.metadata_state = 'classified'
      ),
      'unassigned_attempt_count', count(*) filter (
        where ea.metadata_state = 'unassigned'
      ),
      'invalid_metadata_attempt_count', count(*) filter (
        where ea.metadata_state = 'invalid_metadata'
      ),
      'unassigned_test_set_count', (select count(*) from unassigned_test_sets),
      'unassigned_manual_attempt_count', count(*) filter (
        where ea.metadata_state = 'unassigned'
          and ea.source_type = 'manual'
      ),
      'unassigned_non_manual_attempt_count', count(*) filter (
        where ea.metadata_state = 'unassigned'
          and ea.source_type is distinct from 'manual'
      ),
      'unassigned_test_sets', coalesce((
        select jsonb_agg(
          to_jsonb(uts)
          order by uts.last_evaluated_at desc nulls last, uts.test_set_id
        )
        from unassigned_test_sets uts
      ), '[]'::jsonb),
      'invalid_metadata_test_sets', coalesce((
        select jsonb_agg(to_jsonb(imt) order by imt.test_set_id)
        from invalid_metadata_test_sets imt
      ), '[]'::jsonb)
    )
  from eligible_attempts ea

  union all

  select
    30,
    'major_unit_name_integrity',
    (select count(*)::integer from major_unit_name_conflicts),
    jsonb_build_object(
      'major_unit_group_count', (select count(*) from major_unit_name_groups),
      'conflict_group_count', (select count(*) from major_unit_name_conflicts),
      'missing_major_row_count', (select count(*) from major_unit_missing_major_rows),
      'conflicts', coalesce((
        select jsonb_agg(
          to_jsonb(x)
          order by x.grade_level, x.curriculum_version, x.subject, x.major_unit_code
        )
        from major_unit_name_conflicts x
      ), '[]'::jsonb),
      'missing_major_rows', coalesce((
        select jsonb_agg(
          to_jsonb(x)
          order by x.grade_level, x.curriculum_version, x.subject, x.major_unit_code
        )
        from major_unit_missing_major_rows x
      ), '[]'::jsonb)
    )

  union all

  select
    40,
    'assignment_attempt_invariant',
    (select count(*)::integer from multiple_attempt_assignments),
    jsonb_build_object(
      'assignment_with_attempt_count', (select count(*) from assignment_attempt_counts),
      'multiple_attempt_assignment_count', (
        select count(*) from multiple_attempt_assignments
      ),
      'attempt_without_assignment_count', (
        select count(*)
        from auto_grading.attempts at
        where at.assignment_id is null
      ),
      'multiple_attempt_assignments', coalesce((
        select jsonb_agg(
          to_jsonb(maa)
          order by maa.attempt_count desc, maa.assignment_id
        )
        from multiple_attempt_assignments maa
      ), '[]'::jsonb)
    )

  union all

  select
    50,
    'unit_partition_integrity',
    (select count(*)::integer from partition_mismatches),
    jsonb_build_object(
      'compared_student_count', (select count(*) from all_student_basis),
      'mismatch_student_count', (select count(*) from partition_mismatches),
      'mismatches', coalesce((
        select jsonb_agg(to_jsonb(pm) order by pm.student_code)
        from partition_mismatches pm
      ), '[]'::jsonb),
      'rule', 'classified major-unit partitions + unassigned partition = all eligible attempts, compared by numerator and denominator'
    )

  union all

  select
    60,
    'inactive_curriculum_unit_history',
    0,
    jsonb_build_object(
      'inactive_unit_achievement_attempt_count', count(*) filter (
        where ea.metadata_state = 'classified'
          and ea.curriculum_unit_is_active = false
      ),
      'inactive_unit_test_set_count', count(distinct ea.test_set_id) filter (
        where ea.metadata_state = 'classified'
          and ea.curriculum_unit_is_active = false
      ),
      'policy', 'informational only: inactive curriculum units must remain in historical achievement aggregation'
    )
  from eligible_attempts ea
)
select
  ar.section,
  ar.blocking_issue_count,
  ar.details
from audit_rows ar
order by ar.sort_order;

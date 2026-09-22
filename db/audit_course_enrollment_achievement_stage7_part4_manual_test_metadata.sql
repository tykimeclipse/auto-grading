-- ============================================================================
-- audit_course_enrollment_achievement_stage7_part4_manual_test_metadata.sql
--
-- 7단계 part 4 배포 후 신규 수동 시험 RPC의 시그니처·권한·메타데이터 계약과
-- 기존 수동 시험 데이터 상태를 검증한다. SELECT만 수행한다.
--
-- 화면 배포 진행 조건:
--   - 모든 행의 blocking_issue_count = 0
--
-- 결과 5행의 section / blocking_issue_count / details를 공유한다.
--
-- 기존의 완전 미분류 수동 시험은 허용한다. 일부 값만 채워진 행 또는
-- curriculum_units에 없는 참조만 차단한다.
-- ============================================================================

with
target_functions as (
  select
    p.oid,
    p.proname as function_name,
    oidvectortypes(p.proargtypes) as identity_arguments,
    pg_get_function_result(p.oid) as result_type,
    p.prosecdef as security_definer,
    p.provolatile = 'v' as is_volatile,
    p.prosrc,
    exists (
      select 1
      from aclexplode(coalesce(p.proacl, acldefault('f', p.proowner))) acl
      where acl.grantee = 0
        and acl.privilege_type = 'EXECUTE'
    ) as public_can_execute,
    has_function_privilege('anon', p.oid, 'EXECUTE') as anon_can_execute,
    has_function_privilege('authenticated', p.oid, 'EXECUTE')
      as authenticated_can_execute,
    has_function_privilege('service_role', p.oid, 'EXECUTE')
      as service_role_can_execute
  from pg_proc p
  join pg_namespace n on n.oid = p.pronamespace
  where n.nspname = 'auto_grading'
    and p.proname = 'teacher_create_manual_test_set'
),
expected_function as (
  select tf.*
  from target_functions tf
  where tf.identity_arguments = 'text, integer, text, text, text, text'
),
function_issues as (
  select 'expected_function_count'::text as issue
  where (select count(*) from expected_function) <> 1

  union all

  select 'unexpected_overload_count'
  where (
    select count(*)
    from target_functions tf
    where tf.identity_arguments <> 'text, integer, text, text, text, text'
  ) > 0

  union all

  select 'function_contract'
  from expected_function ef
  where ef.result_type is distinct from 'jsonb'
     or ef.security_definer is distinct from true
     or ef.is_volatile is distinct from true
     or ef.public_can_execute is distinct from false
     or ef.anon_can_execute is distinct from false
     or ef.authenticated_can_execute is distinct from true
     or ef.service_role_can_execute is distinct from false

  union all

  select 'function_body_contract'
  from expected_function ef
  where position('assert_admin()' in ef.prosrc) = 0
     or position('cu.is_active = true' in ef.prosrc) = 0
     or position('curriculum_version' in ef.prosrc) = 0
     or position('unit_code' in ef.prosrc) = 0
     or position('source_type' in ef.prosrc) = 0
     or position('''manual''' in ef.prosrc) = 0
),
expected_constraints as (
  select *
  from (values
    ('test_sets_unit_code_format_chk'::text),
    ('test_sets_curriculum_ref_all_or_none_chk'::text),
    ('test_sets_curriculum_units_fk'::text),
    ('curriculum_units_unique'::text),
    ('curriculum_units_hierarchy_chk'::text)
  ) x(constraint_name)
),
constraint_state as (
  select
    ec.constraint_name,
    c.contype,
    coalesce(c.convalidated, false) as convalidated,
    pg_get_constraintdef(c.oid) as definition,
    c.oid is null as is_missing
  from expected_constraints ec
  left join pg_constraint c
    on c.conname = ec.constraint_name
   and c.conrelid in (
     'auto_grading.test_sets'::regclass,
     'auto_grading.curriculum_units'::regclass
   )
),
manual_test_sets as (
  select
    ts.id as test_set_id,
    ts.title,
    ts.is_active as test_set_is_active,
    ts.grade_level,
    ts.curriculum_version,
    ts.subject,
    ts.unit_code,
    cu.id as curriculum_unit_id,
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
       and cu.id is not null
        then 'classified'
      else 'invalid_metadata'
    end as metadata_state
  from auto_grading.test_sets ts
  left join auto_grading.curriculum_units cu
    on cu.grade_level = ts.grade_level
   and cu.curriculum_version = ts.curriculum_version
   and cu.subject = ts.subject
   and cu.unit_code = ts.unit_code
  where ts.source_type = 'manual'
),
invalid_manual_test_sets as (
  select *
  from manual_test_sets mts
  where mts.metadata_state = 'invalid_metadata'
),
active_curriculum_summary as (
  select
    count(*)::integer as active_unit_count,
    count(distinct cu.curriculum_version)::integer
      as curriculum_version_count,
    count(distinct cu.grade_level)::integer as grade_count,
    count(distinct row(cu.curriculum_version, cu.grade_level, cu.subject))::integer
      as curriculum_grade_subject_count
  from auto_grading.curriculum_units cu
  where cu.is_active = true
),
curriculum_api_access as (
  select
    n.nspname as schema_name,
    c.relname as table_name,
    c.relrowsecurity as rls_enabled,
    c.relforcerowsecurity as rls_forced,
    has_schema_privilege('authenticated', n.oid, 'USAGE')
      as authenticated_has_schema_usage,
    has_table_privilege('authenticated', c.oid, 'SELECT')
      as authenticated_can_select,
    has_schema_privilege('anon', n.oid, 'USAGE')
      as anon_has_schema_usage,
    has_table_privilege('anon', c.oid, 'SELECT') as anon_can_select,
    (
      select count(*)::integer
      from pg_policy p
      where p.polrelid = c.oid
    ) as policy_count,
    (
      select count(*)::integer
      from pg_policy p
      where p.polrelid = c.oid
        and p.polcmd in ('r', '*')
    ) as select_policy_count,
    (
      select count(*)::integer
      from pg_policy p
      where p.polrelid = c.oid
        and p.polcmd in ('r', '*')
        and p.polpermissive
        and exists (
          select 1
          from unnest(p.polroles) pr(role_oid)
          left join pg_roles r on r.oid = pr.role_oid
          where pr.role_oid = 0
             or r.rolname = 'authenticated'
        )
    ) as authenticated_select_policy_count,
    coalesce((
      select jsonb_agg(
        jsonb_build_object(
          'policy_name', p.polname,
          'command', p.polcmd,
          'permissive', p.polpermissive,
          'roles', coalesce((
            select jsonb_agg(
              case
                when pr.role_oid = 0 then 'PUBLIC'
                else coalesce(r.rolname, pr.role_oid::text)
              end
              order by
                case when pr.role_oid = 0 then 'PUBLIC' else r.rolname end
            )
            from unnest(p.polroles) pr(role_oid)
            left join pg_roles r on r.oid = pr.role_oid
          ), '[]'::jsonb),
          'using_expression', pg_get_expr(p.polqual, p.polrelid)
        )
        order by p.polname
      )
      from pg_policy p
      where p.polrelid = c.oid
    ), '[]'::jsonb) as policies
  from pg_class c
  join pg_namespace n on n.oid = c.relnamespace
  where n.nspname = 'auto_grading'
    and c.relname = 'curriculum_units'
    and c.relkind in ('r', 'p')
),
audit_rows as (
  select
    10 as sort_order,
    'function_contract'::text as section,
    (select count(*)::integer from function_issues) as blocking_issue_count,
    jsonb_build_object(
      'expected_identity_arguments',
        'text, integer, text, text, text, text',
      'installed_overload_count', (select count(*) from target_functions),
      'issues', coalesce((
        select jsonb_agg(fi.issue order by fi.issue)
        from function_issues fi
      ), '[]'::jsonb),
      'functions', coalesce((
        select jsonb_agg(
          jsonb_build_object(
            'function_name', tf.function_name,
            'identity_arguments', tf.identity_arguments,
            'result_type', tf.result_type,
            'security_definer', tf.security_definer,
            'is_volatile', tf.is_volatile,
            'public_can_execute', tf.public_can_execute,
            'anon_can_execute', tf.anon_can_execute,
            'authenticated_can_execute', tf.authenticated_can_execute,
            'service_role_can_execute', tf.service_role_can_execute,
            'source_md5', md5(tf.prosrc)
          )
          order by tf.identity_arguments
        )
        from target_functions tf
      ), '[]'::jsonb)
    ) as details

  union all

  select
    20,
    'schema_contract',
    count(*) filter (where cs.is_missing or not cs.convalidated)::integer,
    jsonb_build_object(
      'expected_constraint_count', count(*),
      'missing_or_unvalidated_constraint_count', count(*) filter (
        where cs.is_missing or not cs.convalidated
      ),
      'constraints', jsonb_agg(to_jsonb(cs) order by cs.constraint_name)
    )
  from constraint_state cs

  union all

  select
    30,
    'manual_metadata_integrity',
    (select count(*)::integer from invalid_manual_test_sets),
    jsonb_build_object(
      'manual_test_set_count', (select count(*) from manual_test_sets),
      'classified_test_set_count', (
        select count(*) from manual_test_sets
        where metadata_state = 'classified'
      ),
      'unassigned_legacy_test_set_count', (
        select count(*) from manual_test_sets
        where metadata_state = 'unassigned'
      ),
      'invalid_metadata_test_set_count', (
        select count(*) from invalid_manual_test_sets
      ),
      'inactive_curriculum_reference_count', (
        select count(*) from manual_test_sets
        where metadata_state = 'classified'
          and curriculum_unit_is_active = false
      ),
      'invalid_test_sets', coalesce((
        select jsonb_agg(
          to_jsonb(x)
          order by x.title, x.test_set_id
        )
        from invalid_manual_test_sets x
      ), '[]'::jsonb),
      'policy',
        'fully unassigned legacy manual tests are allowed; partial or dangling metadata is blocked'
    )

  union all

  select
    40,
    'curriculum_units_api_access',
    case
      when caa.table_name is null then 1
      when not caa.authenticated_has_schema_usage then 1
      when not caa.authenticated_can_select then 1
      when caa.rls_enabled
        and caa.authenticated_select_policy_count = 0 then 1
      else 0
    end,
    jsonb_build_object(
      'schema_name', caa.schema_name,
      'table_name', caa.table_name,
      'rls_enabled', caa.rls_enabled,
      'rls_forced', caa.rls_forced,
      'authenticated_has_schema_usage',
        caa.authenticated_has_schema_usage,
      'authenticated_can_select', caa.authenticated_can_select,
      'anon_has_schema_usage', caa.anon_has_schema_usage,
      'anon_can_select', caa.anon_can_select,
      'policy_count', caa.policy_count,
      'select_policy_count', caa.select_policy_count,
      'authenticated_select_policy_count',
        caa.authenticated_select_policy_count,
      'policies', caa.policies,
      'blocking_rule',
        'authenticated requires schema USAGE and table SELECT; when RLS is enabled, a permissive PUBLIC or authenticated SELECT policy is also required',
      'browser_followup',
        'after deployment, open the manual-test modal as an authenticated teacher and confirm that curriculum options are populated'
    )
  from (
    select * from curriculum_api_access
    union all
    select
      null::text,
      null::text,
      false,
      false,
      false,
      false,
      false,
      false,
      0,
      0,
      0,
      '[]'::jsonb
    where not exists (select 1 from curriculum_api_access)
  ) caa

  union all

  select
    50,
    'active_curriculum_options',
    case when acs.active_unit_count = 0 then 1 else 0 end,
    jsonb_build_object(
      'active_unit_count', acs.active_unit_count,
      'curriculum_version_count', acs.curriculum_version_count,
      'grade_count', acs.grade_count,
      'curriculum_grade_subject_count', acs.curriculum_grade_subject_count,
      'rule', 'the manual-test UI can issue only against active curriculum units'
    )
  from active_curriculum_summary acs
)
select
  ar.section,
  ar.blocking_issue_count,
  ar.details
from audit_rows ar
order by ar.sort_order;

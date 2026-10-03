-- ============================================================================
-- audit_teacher_list_assignments_filters_pagination_preflight.sql
--
-- teacher_list_assignments_v2.sql(필터·페이지네이션·assert_admin 버전) 배포 전 점검.
-- SELECT만 수행한다.
--
-- 배포 진행 조건:
--   - 모든 행의 blocking_issue_count = 0
--
-- 섹션
--   10 deployed_function_state   운영 함수가 repo v2(9개 인자) 또는 계약을 모두 지키는 14개 인자 버전인지
--                                (14개 인자라도 plpgsql·security definer·assert_admin 선행·집계 컬럼·
--                                 필터 본문·권한 계약 중 하나라도 어긋나면 차단)
--                                (repo 와 다르면 details.definition 을 repo 파일과 눈으로 대조)
--   20 schema_contract           대단원·출처 필터가 기대는 제약이 존재하고 검증 완료 상태인지
--   30 major_unit_key_integrity  unit_code 앞자리 = major_unit_code, 발행 시험의 대단원 선택 가능 여부
--   40 source_category_inventory 출처 드롭다운에 나올 값과 건수 (정보용)
--   50 unit_metadata_coverage    단원 정보가 없는 발행 건수 (정보용, '전체'에서만 보임)
--   60 source_categories_rpc     출처 드롭다운이 쓰는 teacher_list_source_categories 존재·authenticated 실행 가능
--                                (없거나 authenticated 가 실행 못 하면 차단. PUBLIC/anon 개방·assert_admin 부재는
--                                 기존부터 있던 보안 공백으로, 후속 teacher_* 조회 함수 일괄 감사 대상 — 정보용)
-- ============================================================================

with
v2_args as (
  select 'uuid, uuid, uuid, boolean, text, text, text, integer, integer'::text as identity_arguments
),
new_args as (
  select 'uuid, uuid, uuid, boolean, text, text, text, integer, integer, text, text, text, text, text'::text
    as identity_arguments
),
target_functions as (
  select
    p.oid,
    oidvectortypes(p.proargtypes) as identity_arguments,
    pg_get_function_result(p.oid) as result_type,
    l.lanname as language,
    p.prosecdef as security_definer,
    p.prosrc,
    md5(replace(p.prosrc, chr(13), '')) as prosrc_md5_lf,
    exists (
      select 1
      from aclexplode(coalesce(p.proacl, acldefault('f', p.proowner))) acl
      where acl.grantee = 0
        and acl.privilege_type = 'EXECUTE'
    ) as public_can_execute,
    has_function_privilege('anon', p.oid, 'EXECUTE') as anon_can_execute,
    has_function_privilege('authenticated', p.oid, 'EXECUTE') as authenticated_can_execute,
    has_function_privilege('service_role', p.oid, 'EXECUTE') as service_role_can_execute
  from pg_proc p
  join pg_namespace n on n.oid = p.pronamespace
  join pg_language l on l.oid = p.prolang
  where n.nspname = 'auto_grading'
    and p.proname = 'teacher_list_assignments'
),
function_state as (
  select
    tf.*,
    case
      -- 재배포: 14개 인자라는 사실만으로 통과시키지 않고 postdeploy 와 같은 계약을 모두 확인한다.
      when tf.identity_arguments = (select identity_arguments from new_args)
       and tf.language = 'plpgsql'
       and tf.security_definer
       and position('perform auto_grading.assert_admin()' in tf.prosrc) > 0
       and position('perform auto_grading.assert_admin()' in tf.prosrc)
           < position('return query' in tf.prosrc)
       and position('filtered_open_count bigint' in tf.result_type) > 0
       and position('filtered_final_confirmed_count bigint' in tf.result_type) > 0
       and position('reset_token uuid' in tf.result_type) > 0
       and position('btrim(ts.source_category) = v_source_category' in tf.prosrc) > 0
       and position('left(ts.unit_code, 1) = v_major_unit_code' in tf.prosrc) > 0
       and not tf.public_can_execute
       and not tf.anon_can_execute
       and not tf.service_role_can_execute
       and tf.authenticated_can_execute
        then 'already_deployed_new_version'
      when tf.identity_arguments = (select identity_arguments from new_args)
        then 'new_signature_but_contract_differs'
      when tf.identity_arguments = (select identity_arguments from v2_args)
       and tf.language = 'sql'
       and tf.security_definer
       and position('reset_token uuid' in tf.result_type) > 0
       and position('round2_submitted_at timestamp with time zone' in tf.result_type) > 0
       and position('to_jsonb(s)->>''grade_level''' in tf.prosrc) > 0
        then 'repo_v2_match'
      when tf.identity_arguments = (select identity_arguments from v2_args)
        then 'v2_signature_but_body_differs'
      else 'unexpected_overload'
    end as state
  from target_functions tf
),
expected_constraints as (
  select *
  from (values
    ('curriculum_units_hierarchy_chk'::text),
    ('test_sets_curriculum_units_fk'::text),
    ('test_sets_curriculum_ref_all_or_none_chk'::text)
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
unit_code_mismatch as (
  select cu.curriculum_version, cu.grade_level, cu.subject, cu.unit_code, cu.major_unit_code
  from auto_grading.curriculum_units cu
  where left(cu.unit_code, 1) is distinct from cu.major_unit_code
),
assigned_test_sets as (
  select distinct
    ts.id as test_set_id,
    ts.title,
    ts.source_type,
    ts.curriculum_version,
    ts.grade_level,
    ts.subject,
    ts.unit_code
  from auto_grading.assignments a
  join auto_grading.test_sets ts on ts.id = a.test_set_id
),
assigned_without_active_major as (
  select ats.*
  from assigned_test_sets ats
  where ats.unit_code is not null
    and not exists (
      select 1
      from auto_grading.curriculum_units cu
      where cu.is_active
        and cu.curriculum_version = ats.curriculum_version
        and cu.grade_level = ats.grade_level
        and cu.subject = ats.subject
        and cu.major_unit_code = left(ats.unit_code, 1)
    )
),
source_inventory as (
  select
    btrim(ts.source_category) as source_category,
    count(distinct ts.id) as test_set_count,
    count(distinct ts.id) filter (where ts.source_category <> btrim(ts.source_category))
      as whitespace_variant_count,
    count(a.id) as assignment_count
  from auto_grading.test_sets ts
  left join auto_grading.assignments a on a.test_set_id = ts.id
  where nullif(btrim(ts.source_category), '') is not null
  group by btrim(ts.source_category)
),
source_null_by_type as (
  select
    ts.source_type,
    count(distinct ts.id) as test_set_count,
    count(a.id) as assignment_count
  from auto_grading.test_sets ts
  left join auto_grading.assignments a on a.test_set_id = ts.id
  where nullif(btrim(ts.source_category), '') is null
  group by ts.source_type
),
source_categories_rpc as (
  select
    oidvectortypes(p.proargtypes) as identity_arguments,
    p.prosecdef as security_definer,
    position('assert_admin()' in p.prosrc) > 0 as has_assert_admin,
    exists (
      select 1
      from aclexplode(coalesce(p.proacl, acldefault('f', p.proowner))) acl
      where acl.grantee = 0
        and acl.privilege_type = 'EXECUTE'
    ) as public_can_execute,
    has_function_privilege('anon', p.oid, 'EXECUTE') as anon_can_execute,
    has_function_privilege('authenticated', p.oid, 'EXECUTE') as authenticated_can_execute,
    has_function_privilege('service_role', p.oid, 'EXECUTE') as service_role_can_execute
  from pg_proc p
  join pg_namespace n on n.oid = p.pronamespace
  where n.nspname = 'auto_grading'
    and p.proname = 'teacher_list_source_categories'
),
audit_rows as (
  select
    10 as sort_order,
    'deployed_function_state'::text as section,
    (
      case when (select count(*) from function_state) <> 1 then 1 else 0 end
      + (select count(*)::integer from function_state fs
         where fs.state in (
           'v2_signature_but_body_differs',
           'new_signature_but_contract_differs',
           'unexpected_overload'
         ))
    )::integer as blocking_issue_count,
    jsonb_build_object(
      'installed_overload_count', (select count(*) from function_state),
      'rule',
        'exactly one overload; state must be repo_v2_match (first deploy) or already_deployed_new_version (redeploy, full contract verified)',
      'functions', coalesce((
        select jsonb_agg(
          jsonb_build_object(
            'state', fs.state,
            'identity_arguments', fs.identity_arguments,
            'language', fs.language,
            'security_definer', fs.security_definer,
            'has_assert_admin', position('perform auto_grading.assert_admin()' in fs.prosrc) > 0,
            'public_can_execute', fs.public_can_execute,
            'anon_can_execute', fs.anon_can_execute,
            'authenticated_can_execute', fs.authenticated_can_execute,
            'service_role_can_execute', fs.service_role_can_execute,
            'result_type', fs.result_type,
            'prosrc_length', length(fs.prosrc),
            'prosrc_md5_lf', fs.prosrc_md5_lf,
            'definition', pg_get_functiondef(fs.oid)
          )
          order by fs.identity_arguments
        )
        from function_state fs
      ), '[]'::jsonb)
    ) as details

  union all

  select
    20,
    'schema_contract',
    count(*) filter (where cs.is_missing or not cs.convalidated)::integer,
    jsonb_build_object(
      'expected_constraint_count', count(*),
      'missing_or_unvalidated_constraint_count',
        count(*) filter (where cs.is_missing or not cs.convalidated),
      'constraints', jsonb_agg(to_jsonb(cs) order by cs.constraint_name)
    )
  from constraint_state cs

  union all

  select
    30,
    'major_unit_key_integrity',
    (select count(*)::integer from unit_code_mismatch),
    jsonb_build_object(
      'unit_code_major_mismatch_count', (select count(*) from unit_code_mismatch),
      'unit_code_major_mismatch_sample', coalesce((
        select jsonb_agg(to_jsonb(x)) from (select * from unit_code_mismatch limit 20) x
      ), '[]'::jsonb),
      'assigned_test_set_count', (select count(*) from assigned_test_sets),
      'assigned_without_active_major_count', (select count(*) from assigned_without_active_major),
      'assigned_without_active_major_sample', coalesce((
        select jsonb_agg(to_jsonb(x)) from (select * from assigned_without_active_major limit 20) x
      ), '[]'::jsonb),
      'note',
        'assigned_without_active_major 는 차단 사유가 아니다. 대단원 드롭다운이 활성 단원표로 만들어지므로 이 시험들은 ''전체''에서만 보인다.'
    )

  union all

  select
    40,
    'source_category_inventory',
    0,
    jsonb_build_object(
      'category_count', (select count(*) from source_inventory),
      'categories', coalesce((
        select jsonb_agg(to_jsonb(si) order by si.source_category) from source_inventory si
      ), '[]'::jsonb),
      'null_category_by_source_type', coalesce((
        select jsonb_agg(to_jsonb(sn) order by sn.source_type) from source_null_by_type sn
      ), '[]'::jsonb),
      'note',
        '옛 이름·오타가 있으면 드롭다운 옵션이 갈라진다. source_category 가 없는 시험(수동 포함)은 특정 출처 선택 시 제외되고 ''전체''에서만 보인다.'
    )

  union all

  select
    50,
    'unit_metadata_coverage',
    0,
    jsonb_build_object(
      'assignment_count', (select count(*) from auto_grading.assignments),
      'assignment_without_unit_code_count', (
        select count(*)
        from auto_grading.assignments a
        join auto_grading.test_sets ts on ts.id = a.test_set_id
        where ts.unit_code is null
      ),
      'note', '단원 정보가 없는 시험은 대단원 필터에 걸리지 않고 ''전체''에서만 보인다 (설계상 허용).'
    )

  union all

  select
    60,
    'source_categories_rpc',
    case
      when not exists (
        select 1 from source_categories_rpc scr
        where scr.identity_arguments = '' and scr.authenticated_can_execute
      ) then 1
      else 0
    end,
    jsonb_build_object(
      'functions', coalesce((select jsonb_agg(to_jsonb(scr)) from source_categories_rpc scr), '[]'::jsonb),
      'blocking_rule',
        'teacher_list_source_categories() 가 있고 authenticated 가 실행할 수 있어야 출처 드롭다운이 채워진다.',
      'follow_up',
        'PUBLIC/anon 실행 가능 또는 assert_admin 부재는 이번 변경 전부터 있던 공백이다. 차단 사유가 아니며 teacher_search_test_sets 와 함께 후속 teacher_* 조회 함수 일괄 보안 감사에서 정리한다.'
    )
)
select
  ar.section,
  ar.blocking_issue_count,
  ar.details
from audit_rows ar
order by ar.sort_order;

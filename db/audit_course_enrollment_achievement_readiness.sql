-- ============================================================================
-- audit_course_enrollment_achievement_readiness.sql
--
-- 1단계-A: 강좌 종료 / 수강 관리 / 강좌별 성취도 개발 전 메타데이터 점검.
--
-- 중요:
--   - 운영 데이터 테이블을 스캔하지 않는다.
--   - pg_catalog / information_schema / 통계 뷰만 조회한다.
--   - SELECT만 수행하며 데이터와 스키마를 변경하지 않는다.
--   - Supabase SQL Editor에서 이 파일 전체를 실행해도 된다.
--   - 결과로 반환되는 8행의 section, details 값을 공유한다.
--
-- 1단계-B 실데이터 점검은 이 결과를 검토한 뒤 별도 파일로 작성한다.
-- ============================================================================

with
target_functions as (
  select
    n.nspname as schema_name,
    p.proname as function_name,
    pg_get_function_identity_arguments(p.oid) as identity_arguments,
    pg_get_userbyid(p.proowner) as owner_name,
    p.prosecdef as security_definer,
    p.proacl,
    md5(p.prosrc) as source_md5,
    pg_get_function_result(p.oid) as result_type,
    case
      when p.proname = 'start_attempt' then
        p.prosrc ~* 'insert\s+into\s+(auto_grading\.)?attempts\s*\([^)]*assignment_id'
      else null
    end as start_attempt_inserts_assignment_id,
    case
      when p.proname = 'start_attempt' then
        p.prosrc ~* 'insert\s+into\s+(auto_grading\.)?attempts\s*\([^)]*total_items'
      else null
    end as start_attempt_inserts_total_items,
    case
      when p.proname = 'start_attempt' then
        p.prosrc ~* 'insert\s+into\s+(auto_grading\.)?attempts\s*\([^)]*course_id'
      else null
    end as start_attempt_inserts_course_id,
    case
      when p.proname = 'teacher_set_course_active' then
        p.prosrc ~* 'update\s+auto_grading\.student_courses'
      else null
    end as course_close_updates_enrollments,
    case
      when p.proname in (
        'teacher_attach_student_to_course',
        'teacher_deactivate_student_course',
        'teacher_set_course_active'
      ) then
        p.prosrc ~* 'assert_admin\s*\('
      else null
    end as has_assert_admin,
    has_function_privilege('anon', p.oid, 'EXECUTE') as anon_can_execute,
    has_function_privilege('authenticated', p.oid, 'EXECUTE') as authenticated_can_execute,
    has_function_privilege('service_role', p.oid, 'EXECUTE') as service_role_can_execute
  from pg_proc p
  join pg_namespace n on n.oid = p.pronamespace
  where n.nspname = 'auto_grading'
    and p.proname in (
      'start_attempt',
      'teacher_set_course_active',
      'teacher_list_course_catalog',
      'teacher_attach_student_to_course',
      'teacher_deactivate_student_course',
      'teacher_issue_assignments',
      'get_student_stats_by_code',
      'get_student_assignment_history_by_code'
    )
),
table_columns as (
  select
    c.table_name,
    c.ordinal_position,
    c.column_name,
    c.data_type,
    c.udt_name,
    c.is_nullable,
    c.column_default
  from information_schema.columns c
  where c.table_schema = 'auto_grading'
    and c.table_name in ('students', 'courses', 'student_courses', 'assignments', 'attempts')
),
target_constraints as (
  select
    cls.relname as table_name,
    con.conname as constraint_name,
    con.contype as constraint_type,
    pg_get_constraintdef(con.oid, true) as definition
  from pg_constraint con
  join pg_class cls on cls.oid = con.conrelid
  join pg_namespace n on n.oid = cls.relnamespace
  where n.nspname = 'auto_grading'
    and cls.relname in ('student_courses', 'assignments', 'attempts')
),
target_indexes as (
  select
    tablename as table_name,
    indexname as index_name,
    indexdef as definition
  from pg_indexes
  where schemaname = 'auto_grading'
    and tablename in ('student_courses', 'assignments', 'attempts')
),
relation_estimates as (
  select
    relname as table_name,
    n_live_tup as estimated_live_rows,
    n_dead_tup as estimated_dead_rows,
    last_analyze,
    last_autoanalyze
  from pg_stat_user_tables
  where schemaname = 'auto_grading'
    and relname in ('students', 'courses', 'student_courses', 'assignments', 'attempts')
),
audit_rows as (
  select
    10 as sort_order,
    'connection'::text as section,
    jsonb_build_object(
      'checked_at', current_timestamp,
      'database', current_database(),
      'user', current_user,
      'statement_timeout', current_setting('statement_timeout'),
      'server_version', current_setting('server_version')
    ) as details

  union all

  select
    20,
    'schema_columns',
    coalesce(
      (select jsonb_agg(to_jsonb(x) order by x.table_name, x.ordinal_position) from table_columns x),
      '[]'::jsonb
    )

  union all

  select
    30,
    'constraints',
    coalesce(
      (select jsonb_agg(to_jsonb(x) order by x.table_name, x.constraint_name) from target_constraints x),
      '[]'::jsonb
    )

  union all

  select
    40,
    'indexes',
    coalesce(
      (select jsonb_agg(to_jsonb(x) order by x.table_name, x.index_name) from target_indexes x),
      '[]'::jsonb
    )

  union all

  select
    50,
    'function_inventory_and_acl',
    coalesce(
      (select jsonb_agg(to_jsonb(x) order by x.function_name, x.identity_arguments) from target_functions x),
      '[]'::jsonb
    )

  union all

  select
    60,
    'normalized_view',
    jsonb_build_object(
      'exists', to_regclass('auto_grading.v_student_courses_normalized') is not null,
      'definition', case
        when to_regclass('auto_grading.v_student_courses_normalized') is null then null
        else pg_get_viewdef(to_regclass('auto_grading.v_student_courses_normalized'), true)
      end
    )

  union all

  select
    70,
    'default_function_privileges',
    coalesce(
      (
        select jsonb_agg(
          jsonb_build_object(
            'owner', pg_get_userbyid(d.defaclrole),
            'schema', n.nspname,
            'object_type', d.defaclobjtype,
            'acl', d.defaclacl
          )
          order by pg_get_userbyid(d.defaclrole), n.nspname nulls first
        )
        from pg_default_acl d
        left join pg_namespace n on n.oid = d.defaclnamespace
        where d.defaclobjtype = 'f'
          and (n.nspname = 'auto_grading' or d.defaclnamespace = 0)
      ),
      '[]'::jsonb
    )

  union all

  select
    80,
    'estimated_row_counts',
    coalesce(
      (select jsonb_agg(to_jsonb(x) order by x.table_name) from relation_estimates x),
      '[]'::jsonb
    )
)
select section, details
from audit_rows
order by sort_order;

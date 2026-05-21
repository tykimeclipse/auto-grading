-- ================================================================
-- student_public_links : 학생 성취도 페이지 공개 링크 토큰
-- ================================================================

-- ----------------------------------------------------------------
-- 1. 테이블 생성
-- ----------------------------------------------------------------
create table if not exists auto_grading.student_public_links (
  id           uuid        primary key default gen_random_uuid(),
  student_id   uuid        not null references auto_grading.students(id) on delete cascade,
  public_token uuid        not null default gen_random_uuid() unique,
  is_active    boolean     not null default true,
  created_at   timestamptz not null default now(),
  revoked_at   timestamptz,
  expires_at   timestamptz
);

-- ----------------------------------------------------------------
-- 2. 컬럼 보장 (테이블이 이미 존재했을 때 대비)
-- ----------------------------------------------------------------

-- public_token 컬럼이 없으면 추가
alter table auto_grading.student_public_links
  add column if not exists public_token uuid not null default gen_random_uuid();

-- public_token 컬럼이 text 타입으로 만들어진 경우 uuid 로 변환
-- (이미 uuid 타입이면 무해하게 통과)
alter table auto_grading.student_public_links
  alter column public_token type uuid using public_token::uuid;

-- DEFAULT 보장
alter table auto_grading.student_public_links
  alter column public_token set default gen_random_uuid();

-- 이전 실행에서 잘못 추가됐을 수 있는 access_token 컬럼 제거
alter table auto_grading.student_public_links
  drop column if exists access_token;

-- 나머지 컬럼
alter table auto_grading.student_public_links
  add column if not exists is_active boolean not null default true;
alter table auto_grading.student_public_links
  add column if not exists created_at timestamptz not null default now();
alter table auto_grading.student_public_links
  add column if not exists revoked_at timestamptz;
alter table auto_grading.student_public_links
  add column if not exists expires_at timestamptz;

comment on table auto_grading.student_public_links
  is '학생 성취도 페이지 공개 링크 토큰. 학생 1명당 활성 토큰 1개 유지.';
comment on column auto_grading.student_public_links.public_token
  is 'UUID v4 랜덤 토큰. 성취도 페이지 URL의 ?token= 값으로 사용.';

-- ----------------------------------------------------------------
-- 3. 인덱스
-- ----------------------------------------------------------------
-- 학생당 활성 토큰 1개 제한
create unique index if not exists student_public_links_student_active_uidx
  on auto_grading.student_public_links (student_id)
  where is_active = true;

-- 토큰 중복 방지 (일반 index → unique index로 교체)
drop index if exists auto_grading.student_public_links_token_idx;
create unique index if not exists student_public_links_token_uidx
  on auto_grading.student_public_links (public_token);


-- ================================================================
-- RPC 함수
-- ================================================================

-- ----------------------------------------------------------------
-- 4. get_student_by_public_token(uuid)
--    학생/학부모용 — 토큰 검증 후 최소 학생 정보 반환
--    · 유효하지 않거나 비활성 토큰이면 null 반환 (에러 raise 금지)
--    · 반환: student_code, name, grade_level 만
-- ----------------------------------------------------------------
drop function if exists auto_grading.get_student_by_public_token(text);
drop function if exists auto_grading.get_student_by_public_token(uuid);

create or replace function auto_grading.get_student_by_public_token(
  p_token uuid
)
returns jsonb
language sql
security definer
set search_path to 'auto_grading', 'public'
as $$
  select jsonb_build_object(
    'student_code', s.student_code,
    'name',         s.name,
    'grade_level',  s.grade_level
  )
  from auto_grading.student_public_links l
  join auto_grading.students s on s.id = l.student_id
  where l.public_token = p_token
    and l.is_active    = true
    and (l.expires_at is null or l.expires_at > now())
    and s.is_active    = true;
$$;

grant execute on function auto_grading.get_student_by_public_token(uuid)
  to anon, authenticated, service_role;

comment on function auto_grading.get_student_by_public_token(uuid)
  is '토큰으로 학생 기본 정보(student_code, name, grade_level) 반환. 유효하지 않으면 null.';


-- ----------------------------------------------------------------
-- 5. get_or_create_student_public_token(uuid)
--    교사용 — 활성 토큰 있으면 반환, 없으면 신규 발급
-- ----------------------------------------------------------------
drop function if exists auto_grading.get_or_create_student_public_token(uuid);

create or replace function auto_grading.get_or_create_student_public_token(
  p_student_id uuid
)
returns jsonb
language plpgsql
security definer
set search_path to 'auto_grading', 'public'
as $$
declare
  v_token uuid;
begin
  if p_student_id is null then
    raise exception 'p_student_id is required';
  end if;

  select public_token
    into v_token
  from auto_grading.student_public_links
  where student_id = p_student_id
    and is_active  = true
  limit 1;

  if v_token is null then
    insert into auto_grading.student_public_links (student_id)
    values (p_student_id)
    returning public_token into v_token;
  end if;

  return jsonb_build_object('ok', true, 'token', v_token);
end;
$$;

-- 교사용 함수 — anon 직접 호출 차단.
-- student_id 만 알면 학생 범용 public token 을 발급할 수 있으므로 anon 노출 금지.
-- PUBLIC 기본 grant 도 함께 revoke (anon 은 PUBLIC 멤버).
-- SECURITY DEFINER 함수(omr_bridge, teacher_get_mistake_note_detail) 의 내부
-- 호출은 정의자 권한으로 실행되므로 이 revoke 의 영향을 받지 않는다.
-- TODO(보안 게이트): 장기적으로 authenticated 직접 grant 도 제거하고
--   teacher_get_or_create_student_public_token wrapper(assert_admin 내장) +
--   public grant 없는 internal helper 로 분리한다.
revoke execute on function auto_grading.get_or_create_student_public_token(uuid)
  from public, anon;
grant  execute on function auto_grading.get_or_create_student_public_token(uuid)
  to authenticated, service_role;

comment on function auto_grading.get_or_create_student_public_token(uuid)
  is '교사용. 학생의 활성 성취도 링크 토큰을 반환하거나 신규 발급한다. anon 호출 금지.';


-- ----------------------------------------------------------------
-- 6. revoke_and_reissue_student_public_token(uuid)
--    교사용 — 기존 토큰 폐기 + 새 토큰 발급 (동일 트랜잭션)
-- ----------------------------------------------------------------
drop function if exists auto_grading.revoke_and_reissue_student_public_token(uuid);

create or replace function auto_grading.revoke_and_reissue_student_public_token(
  p_student_id uuid
)
returns jsonb
language plpgsql
security definer
set search_path to 'auto_grading', 'public'
as $$
declare
  v_new_token uuid;
begin
  if p_student_id is null then
    raise exception 'p_student_id is required';
  end if;

  update auto_grading.student_public_links
  set is_active  = false,
      revoked_at = now()
  where student_id = p_student_id
    and is_active  = true;

  insert into auto_grading.student_public_links (student_id)
  values (p_student_id)
  returning public_token into v_new_token;

  return jsonb_build_object('ok', true, 'token', v_new_token);
end;
$$;

-- 교사용 함수 — anon 직접 호출 차단 (위 get_or_create 와 동일 사유).
revoke execute on function auto_grading.revoke_and_reissue_student_public_token(uuid)
  from public, anon;
grant  execute on function auto_grading.revoke_and_reissue_student_public_token(uuid)
  to authenticated, service_role;

comment on function auto_grading.revoke_and_reissue_student_public_token(uuid)
  is '교사용. 기존 성취도 링크 토큰을 폐기하고 새 토큰을 즉시 발급한다. anon 호출 금지.';


-- ----------------------------------------------------------------
-- 7. PostgREST 스키마 캐시 reload (grant 변경 반영)
-- ----------------------------------------------------------------
notify pgrst, 'reload schema';

-- ================================================================
-- student_public_links : 학생 성취도 페이지 공개 링크 토큰
--
-- 목적: 학생 성취도 URL을 ?code=학생코드 대신 ?token=UUID로 바꿔
--       학생코드 추측에 의한 타인 조회를 차단한다.
--
-- 설계 원칙:
--   · access_token = UUID v4 (gen_random_uuid()) — 122비트 엔트로피
--   · 학생 1명 당 활성 토큰은 항상 1개 (partial unique index 보장)
--   · 재발급 시 기존 토큰을 revoke 하고 새 토큰 insert (동일 트랜잭션)
--   · get_student_by_public_token 은 anon 접근 허용 (학생/학부모용)
--   · get_or_create / revoke 는 교사용 (anon 동일 — 별도 auth 없음)
-- ================================================================

-- ----------------------------------------------------------------
-- 1. 테이블 생성 + 컬럼 보장
--    IF NOT EXISTS는 테이블이 이미 있으면 건너뛰므로,
--    각 컬럼을 ADD COLUMN IF NOT EXISTS로 별도 보장한다.
-- ----------------------------------------------------------------
create table if not exists auto_grading.student_public_links (
  id           uuid        primary key default gen_random_uuid(),
  student_id   uuid        not null references auto_grading.students(id) on delete cascade,
  access_token uuid        not null default gen_random_uuid(),
  is_active    boolean     not null default true,
  created_at   timestamptz not null default now(),
  revoked_at   timestamptz
);

-- 테이블이 이미 존재했을 경우를 위한 컬럼 보장
alter table auto_grading.student_public_links
  add column if not exists access_token uuid not null default gen_random_uuid();
alter table auto_grading.student_public_links
  add column if not exists is_active boolean not null default true;
alter table auto_grading.student_public_links
  add column if not exists created_at timestamptz not null default now();
alter table auto_grading.student_public_links
  add column if not exists revoked_at timestamptz;

comment on table auto_grading.student_public_links
  is '학생 성취도 페이지 공개 링크 토큰. 학생 1명당 활성 토큰 1개 유지.';

comment on column auto_grading.student_public_links.access_token
  is 'UUID v4 랜덤 토큰. 성취도 페이지 URL의 ?token= 값으로 사용.';

-- 학생당 활성 토큰 1개 제한 (partial unique index)
create unique index if not exists student_public_links_student_active_uidx
  on auto_grading.student_public_links (student_id)
  where is_active = true;

-- 토큰 조회 속도를 위한 index
create index if not exists student_public_links_token_idx
  on auto_grading.student_public_links (access_token);


-- ----------------------------------------------------------------
-- 2. get_student_by_public_token
--    학생/학부모용 — token 유효성 검증 후 최소 학생 정보 반환
--    · 유효하지 않거나 비활성 토큰이면 null 반환 (에러 raise 금지)
--      이유: 에러 메시지가 토큰 존재 여부를 힌트로 줄 수 있음
--    · 반환값: student_code, name, grade_level 만 (student_id 미포함)
-- ----------------------------------------------------------------
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
  where l.access_token = p_token
    and l.is_active    = true
    and s.is_active    = true;
$$;

grant execute on function auto_grading.get_student_by_public_token(uuid)
  to anon, authenticated, service_role;

comment on function auto_grading.get_student_by_public_token(uuid)
  is '토큰으로 학생 기본 정보(student_code, name, grade_level) 반환. 유효하지 않으면 null.';


-- ----------------------------------------------------------------
-- 3. get_or_create_student_public_token
--    교사용 — 활성 토큰이 있으면 반환, 없으면 신규 발급
-- ----------------------------------------------------------------
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

  -- 기존 활성 토큰 조회
  select access_token
    into v_token
  from auto_grading.student_public_links
  where student_id = p_student_id
    and is_active  = true
  limit 1;

  -- 없으면 신규 발급
  if v_token is null then
    insert into auto_grading.student_public_links (student_id)
    values (p_student_id)
    returning access_token into v_token;
  end if;

  return jsonb_build_object('ok', true, 'token', v_token);
end;
$$;

grant execute on function auto_grading.get_or_create_student_public_token(uuid)
  to anon, authenticated, service_role;

comment on function auto_grading.get_or_create_student_public_token(uuid)
  is '교사용. 학생의 활성 성취도 링크 토큰을 반환하거나 신규 발급한다.';


-- ----------------------------------------------------------------
-- 4. revoke_and_reissue_student_public_token
--    교사용 — 기존 토큰 폐기 + 새 토큰 발급 (동일 트랜잭션)
-- ----------------------------------------------------------------
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

  -- 기존 활성 토큰 폐기
  update auto_grading.student_public_links
  set is_active  = false,
      revoked_at = now()
  where student_id = p_student_id
    and is_active  = true;

  -- 새 토큰 발급
  insert into auto_grading.student_public_links (student_id)
  values (p_student_id)
  returning access_token into v_new_token;

  return jsonb_build_object('ok', true, 'token', v_new_token);
end;
$$;

grant execute on function auto_grading.revoke_and_reissue_student_public_token(uuid)
  to anon, authenticated, service_role;

comment on function auto_grading.revoke_and_reissue_student_public_token(uuid)
  is '교사용. 기존 성취도 링크 토큰을 폐기하고 새 토큰을 즉시 발급한다.';

-- ================================================================
-- mistake_notes / mistake_images : 학생 오답노트 메타데이터
--
-- ⚠️ 이 파일은 개발 DB 전용입니다.
--    Step 1 에서 mistake_images, mistake_notes 를 drop 하므로
--    운영 데이터가 생긴 이후에는 절대 그대로 실행하지 마십시오.
--    운영 전환 후에는 alter migration 파일로 분리해야 합니다.
--
-- 설계 결정 (개발 기획안 v0.3 + Phase 0~1 합의):
--   · 이미지 원본/썸네일/PDF 는 자체 파일서버에 저장.
--     DB 에는 file_key 등 메타만 보관 (Supabase Storage 미사용).
--   · 학생 접근은 token+RPC, 교사 접근은 assert_admin+RPC.
--     테이블 직접 접근은 anon/authenticated 모두 차단,
--     service_role 만 직접 R/W 허용 (파일서버 백그라운드용).
--   · 단원 매핑은 curriculum_unit_id + unit_code + 한 줄 snapshot 조합.
--     test_sets.curriculum_unit_id FK 추가는 별도 마이그레이션 Phase 에서.
--   · source_type 은 text 로만 두고 CHECK 제약 없음 (별도 앱 분리 대비).
--   · teacher_comments / mistake_note_items / reviewed status / pdf_file_key
--     등은 의도적으로 제외 — 모두 후속 확장 Phase 에서 도입.
--   · cascade delete 유지. 파일서버 orphan 은 Phase 9 reconciliation
--     cron (DB 미존재 파일은 7일 grace 후 삭제) 으로 해소.
--   · ownership integrity 는 composite FK 로 DB 차원에서도 강제.
-- ================================================================


-- ── Step 0: 전제 조건 확인 ───────────────────────────────────────
do $$
begin
  if to_regnamespace('auto_grading') is null then
    raise exception 'schema auto_grading does not exist. run schema.sql first';
  end if;

  if to_regclass('auto_grading.students')    is null
  or to_regclass('auto_grading.assignments') is null
  or to_regclass('auto_grading.attempts')    is null
  or to_regclass('auto_grading.test_sets')   is null then
    raise exception 'required base tables missing. run schema.sql first';
  end if;

  if to_regclass('auto_grading.curriculum_units') is null then
    raise exception 'curriculum_units not found. run curriculum_units.sql first';
  end if;

  if to_regprocedure('auto_grading.set_updated_at()') is null then
    raise exception 'auto_grading.set_updated_at() not found. run schema.sql first';
  end if;
end $$;


-- ── Step 1: 이전 초안 정리 (개발 DB 전용) ────────────────────────
drop table if exists auto_grading.mistake_images cascade;
drop table if exists auto_grading.mistake_notes  cascade;


-- ── Step 2: 부모 테이블 composite unique 보강 ────────────────────
-- mistake_notes 의 composite FK 가 참조할 unique key 를 부모 테이블에 보장.
-- 기존 unique 제약과 조합 자체는 다르지만 같은 row 를 식별하는 키이므로
-- 추가 비용은 인덱스 1개 수준이고, ownership 검증에 결정적인 역할.

do $$
begin
  if not exists (
    select 1 from pg_constraint
    where conname  = 'assignments_id_student_testset_uq'
      and conrelid = 'auto_grading.assignments'::regclass
  ) then
    alter table auto_grading.assignments
      add constraint assignments_id_student_testset_uq
      unique (id, student_id, test_set_id);
  end if;
end $$;

do $$
begin
  if not exists (
    select 1 from pg_constraint
    where conname  = 'attempts_id_assignment_uq'
      and conrelid = 'auto_grading.attempts'::regclass
  ) then
    alter table auto_grading.attempts
      add constraint attempts_id_assignment_uq
      unique (id, assignment_id);
    -- 주의: attempts.assignment_id 는 nullable + on delete set null.
    -- nullable 컬럼이 포함된 unique 는 NULL 다중 허용 (PG 표준).
    -- mistake_note 는 assignment_id NOT NULL 이므로 자연스럽게 NULL attempt 차단.
  end if;
end $$;


-- ================================================================
-- 3. mistake_notes
-- ================================================================
create table auto_grading.mistake_notes (
  id                    uuid        primary key default gen_random_uuid(),

  -- 소유/연결 (RPC 가 토큰 검증 후 채움)
  student_id            uuid        not null references auto_grading.students(id)    on delete cascade,
  assignment_id         uuid        not null references auto_grading.assignments(id) on delete cascade,
  attempt_id            uuid        not null references auto_grading.attempts(id)    on delete cascade,
  test_set_id           uuid        not null references auto_grading.test_sets(id)   on delete cascade,

  -- 단원 매핑 (snapshot 은 한 줄 표시용 1개만)
  curriculum_unit_id    uuid                 references auto_grading.curriculum_units(id),
  unit_code             text,
  unit_title_snapshot   text,
  unit_mapping_status   text        not null default 'unmapped'
    check (unit_mapping_status in ('mapped','unmapped','manual')),

  -- 출처/맥락
  source_type           text        not null default 'auto_grading',
  source_round          text        not null default 'after_round2',
  wrong_count_snapshot  integer     not null default 0
    check (wrong_count_snapshot >= 0),

  -- 내용
  title                 text,
  memo                  text,
  image_count           integer     not null default 0
    check (image_count >= 0),

  -- 상태 (MVP: reviewed 제외)
  status                text        not null default 'draft'
    check (status in ('draft','submitted','archived')),

  created_at            timestamptz not null default now(),
  updated_at            timestamptz not null default now(),

  -- mistake_images 의 composite FK 가 가리킬 unique key
  constraint mistake_notes_id_student_uq unique (id, student_id),

  -- ownership integrity: assignment 가 정말 그 학생/테스트셋의 것인지 DB 차원 강제
  constraint mistake_notes_assignment_owner_fk
    foreign key (assignment_id, student_id, test_set_id)
    references auto_grading.assignments (id, student_id, test_set_id)
    on delete cascade,

  -- ownership integrity: attempt 가 정말 그 assignment 의 것인지 DB 차원 강제
  constraint mistake_notes_attempt_assignment_fk
    foreign key (attempt_id, assignment_id)
    references auto_grading.attempts (id, assignment_id)
    on delete cascade
);

comment on table  auto_grading.mistake_notes
  is '학생 오답노트 메타. 이미지/PDF 원본은 자체 파일서버 보관.';
comment on column auto_grading.mistake_notes.unit_title_snapshot
  is '작성 당시 단원명 한 줄. test_sets 단원명이 사후 교정돼도 노트 표시는 보존.';
comment on column auto_grading.mistake_notes.unit_mapping_status
  is 'mapped=curriculum_unit_id 채움 / unmapped=test_set 미매핑 / manual=수기 입력.';
comment on column auto_grading.mistake_notes.source_type
  is '예: auto_grading, standalone. CHECK 제약 없음 — 코드에서 enum 관리.';
comment on column auto_grading.mistake_notes.wrong_count_snapshot
  is '생성 시점의 2차 오답 수 (attempts.incorrect_count_after_round2 복사).';

-- updated_at 트리거 (schema.sql 의 set_updated_at 재사용)
drop trigger if exists trg_mistake_notes_updated_at on auto_grading.mistake_notes;
create trigger trg_mistake_notes_updated_at
before update on auto_grading.mistake_notes
for each row execute function auto_grading.set_updated_at();

-- 인덱스
create index mistake_notes_student_idx
  on auto_grading.mistake_notes (student_id, created_at desc);
create index mistake_notes_assignment_idx
  on auto_grading.mistake_notes (assignment_id);
create index mistake_notes_attempt_idx
  on auto_grading.mistake_notes (attempt_id);
create index mistake_notes_test_set_idx
  on auto_grading.mistake_notes (test_set_id);
create index mistake_notes_unit_code_idx
  on auto_grading.mistake_notes (unit_code)
  where unit_code is not null;
create index mistake_notes_curriculum_unit_idx
  on auto_grading.mistake_notes (curriculum_unit_id)
  where curriculum_unit_id is not null;

-- attempt 당 활성 노트 1개 제한 (재생성 시 기존을 archived 로 전환 후 생성)
create unique index mistake_notes_attempt_active_uidx
  on auto_grading.mistake_notes (attempt_id)
  where status in ('draft','submitted');


-- ================================================================
-- 4. mistake_images
-- ================================================================
create table auto_grading.mistake_images (
  id              uuid        primary key default gen_random_uuid(),
  -- note_id 단순 FK 는 두지 않고 아래 composite FK 로 일원화
  note_id         uuid        not null,
  -- student_id 중복 보관 — note 거치지 않는 권한/통계 쿼리에서 join 1단계 절약
  student_id      uuid        not null references auto_grading.students(id) on delete cascade,

  file_backend    text        not null default 'local_file_server',

  file_key        text        not null,
  thumbnail_key   text,
  mime_type       text        not null
    check (mime_type in ('image/jpeg','image/png','image/webp')),
  file_size       integer     not null check (file_size > 0),
  width           integer              check (width  is null or width  > 0),
  height          integer              check (height is null or height > 0),
  sort_order      integer     not null default 0 check (sort_order >= 0),

  created_at      timestamptz not null default now(),

  -- composite FK: note 의 student 와 image 의 student 일치를 DB 차원 강제
  constraint mistake_images_note_student_fk
    foreign key (note_id, student_id)
    references auto_grading.mistake_notes (id, student_id)
    on delete cascade
);

comment on table  auto_grading.mistake_images
  is '오답노트 이미지 메타. 실제 파일은 자체 파일서버, DB 에는 논리 file_key 만.';
comment on column auto_grading.mistake_images.file_key
  is '파일서버 내 논리 경로. 실제 물리경로는 파일서버 내부 매핑으로 분리.';
comment on column auto_grading.mistake_images.file_backend
  is '파일 저장 백엔드 식별자. 현재는 local_file_server, 향후 s3 등 추가 대비.';

create index mistake_images_note_sort_idx
  on auto_grading.mistake_images (note_id, sort_order, created_at);
create index mistake_images_student_idx
  on auto_grading.mistake_images (student_id);

-- 동일 note 내 file_key 중복 방지 (재업로드 충돌 차단)
create unique index mistake_images_note_file_key_uidx
  on auto_grading.mistake_images (note_id, file_key);


-- ================================================================
-- 5. image_count 동기화 트리거
--    mistake_images insert/delete 시 mistake_notes.image_count 갱신.
--    RPC 외 경로(파일서버 정리 cron 등) 에서 삭제돼도 카운트 일관성 유지.
--    note_id 변경(update) 은 MVP 정책상 발생 안 함 → update trigger 미설치.
-- ================================================================
create or replace function auto_grading.trg_sync_mistake_image_count_ins()
returns trigger
language plpgsql
as $$
begin
  update auto_grading.mistake_notes n
  set image_count = (
    select count(*) from auto_grading.mistake_images
    where note_id = n.id
  )
  where n.id in (select distinct note_id from new_table);
  return null;
end;
$$;

drop trigger if exists trg_mistake_images_sync_count_ins on auto_grading.mistake_images;
create trigger trg_mistake_images_sync_count_ins
after insert on auto_grading.mistake_images
referencing new table as new_table
for each statement
execute function auto_grading.trg_sync_mistake_image_count_ins();


create or replace function auto_grading.trg_sync_mistake_image_count_del()
returns trigger
language plpgsql
as $$
begin
  -- 부모 note 가 같은 tx 에서 cascade 삭제되는 경우 update 는 0 rows 로 안전.
  update auto_grading.mistake_notes n
  set image_count = (
    select count(*) from auto_grading.mistake_images
    where note_id = n.id
  )
  where n.id in (select distinct note_id from old_table);
  return null;
end;
$$;

drop trigger if exists trg_mistake_images_sync_count_del on auto_grading.mistake_images;
create trigger trg_mistake_images_sync_count_del
after delete on auto_grading.mistake_images
referencing old table as old_table
for each statement
execute function auto_grading.trg_sync_mistake_image_count_del();


-- ================================================================
-- 6. GRANT / REVOKE
--    auto_grading 스키마는 default privileges 로 authenticated 에 EXECUTE
--    자동 부여되지만, 테이블 직접 접근은 모두 차단해야 함.
--    모든 입출은 Phase 2(학생 RPC), Phase 3(교사 RPC) 을 통해서만.
-- ================================================================

-- 테이블: anon/authenticated 직접 접근 차단, service_role 만 허용
revoke all on table auto_grading.mistake_notes  from public, anon, authenticated;
revoke all on table auto_grading.mistake_images from public, anon, authenticated;

grant select, insert, update, delete on table auto_grading.mistake_notes  to service_role;
grant select, insert, update, delete on table auto_grading.mistake_images to service_role;

-- 트리거 helper 함수: 외부 RPC 로 노출될 이유 없음 → 명시적 차단
revoke all on function auto_grading.trg_sync_mistake_image_count_ins() from public, anon, authenticated;
revoke all on function auto_grading.trg_sync_mistake_image_count_del() from public, anon, authenticated;


-- ================================================================
-- 7. PostgREST 스키마 캐시 reload
-- ================================================================
notify pgrst, 'reload schema';


-- ── 검증 쿼리 (실행 후 직접 확인) ───────────────────────────────
--
-- 1) 테이블 / 트리거 / 인덱스 존재 확인
-- select table_name from information_schema.tables
--   where table_schema = 'auto_grading'
--     and table_name in ('mistake_notes','mistake_images');
--
-- 2) anon/authenticated 가 직접 접근 못하는지 확인
-- select grantee, privilege_type
--   from information_schema.role_table_grants
--   where table_schema = 'auto_grading'
--     and table_name in ('mistake_notes','mistake_images')
--     and grantee in ('anon','authenticated','PUBLIC');
-- → 결과 0행이어야 정상.
--
-- 3) composite FK 가 ownership 위반을 막는지 확인
-- -- (학생 A 의 attempt 에 학생 B 의 student_id 를 박은 row 삽입 시도)
-- insert into auto_grading.mistake_notes
--   (student_id, assignment_id, attempt_id, test_set_id)
--   values ('<학생 B uuid>', '<학생 A 의 assignment>', '<학생 A 의 attempt>', '<학생 A 의 test_set>');
-- → mistake_notes_assignment_owner_fk 위반으로 reject 되어야 정상.
--
-- 4) attempt 당 활성 노트 1개 제한 확인
-- -- 같은 attempt_id 로 status='draft' row 두 개 삽입 시 두 번째에서 unique violation 발생.
--
-- 5) image_count 트리거 동작 확인
-- insert into auto_grading.mistake_images (note_id, student_id, ...) values (...);
-- select image_count from auto_grading.mistake_notes where id = '<note_id>';
-- → 삽입된 행 수와 image_count 가 일치해야 정상.

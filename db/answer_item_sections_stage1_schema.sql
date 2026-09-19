-- ============================================================================
-- answer_item_sections_stage1_schema.sql
--
-- 답안 입력폼 문항 섹션 기능 1단계: test_items 표시 메타데이터 확장.
--
-- 배포 순서:
--   1. 이 파일 실행
--   2. audit_answer_item_sections_stage1_schema.sql 실행
--   3. 문제지 생성·조회 RPC 확장 단계 진행
--
-- 안전 원칙:
--   - item_no는 제출·재풀이 API의 내부 식별자이므로 절대 재번호하지 않는다.
--   - 기존 행은 display_item_no/display_order=item_no, section_order=1,
--     section_title=NULL로만 보정한다.
--   - 구버전 INSERT가 새 컬럼을 생략해도 BEFORE INSERT 트리거가 표시값만
--     보충한다. 이 트리거는 item_no를 어떤 경우에도 수정하지 않는다.
--   - 동일 시험지에서 제목 있는 행과 없는 행을 섞지 않는다.
--   - 동일 (test_set_id, section_order)의 section_title은 항상 같아야 한다.
--   - 별도 test_set_sections 테이블 대신 test_items에 표시 메타데이터를
--     비정규화해 저장한다. 현재 읽기 경로의 조인 증가를 피하되, 일관성은
--     제약과 트리거로 강제한다.
--
-- 후속 단계 계약:
--   - CSV 섹션 제목은 btrim 후 문자 수 기준 최대 120자로 검증한다.
--   - 업로드 화면은 이 파일의 관련 트리거 예외 4개를 한국어로 변환한다.
-- ============================================================================

begin;

-- --------------------------------------------------------------------------
-- 1. 표시 메타데이터 컬럼 추가
--    기존 RPC가 컬럼을 생략한 INSERT를 계속 수행할 수 있도록 컬럼 default가
--    아니라 행의 item_no를 참조할 수 있는 BEFORE INSERT 트리거를 사용한다.
-- --------------------------------------------------------------------------
alter table auto_grading.test_items
  add column if not exists display_item_no integer,
  add column if not exists section_order integer,
  add column if not exists section_title text,
  add column if not exists display_order integer;

-- 기존 item_no는 읽기만 하며 절대 갱신하지 않는다.
update auto_grading.test_items
set
  display_item_no = coalesce(display_item_no, item_no),
  section_order = coalesce(section_order, 1),
  section_title = nullif(btrim(section_title), ''),
  display_order = coalesce(display_order, item_no)
where display_item_no is null
   or section_order is null
   or display_order is null
   or section_title is distinct from nullif(btrim(section_title), '');

-- --------------------------------------------------------------------------
-- 2. 백필 결과와 섹션 일관성 선검증
--    부분 적용 또는 수동 선행 변경이 있다면 제약 생성 전에 명확히 중단한다.
-- --------------------------------------------------------------------------
do $block$
begin
  if exists (
    select 1
    from auto_grading.test_items ti
    where ti.display_item_no is null
       or ti.section_order is null
       or ti.display_order is null
  ) then
    raise exception 'TEST_ITEM_DISPLAY_METADATA_BACKFILL_INCOMPLETE';
  end if;

  if exists (
    select 1
    from auto_grading.test_items ti
    where ti.display_item_no < 1
       or ti.section_order < 1
       or ti.display_order < 1
  ) then
    raise exception 'TEST_ITEM_DISPLAY_METADATA_NOT_POSITIVE';
  end if;

  if exists (
    select 1
    from auto_grading.test_items ti
    where ti.section_title is not null
      and (
        ti.section_title <> btrim(ti.section_title)
        or char_length(ti.section_title) not between 1 and 120
      )
  ) then
    raise exception 'TEST_ITEM_SECTION_TITLE_INVALID';
  end if;

  if exists (
    select 1
    from auto_grading.test_items ti
    where ti.section_title is null
      and ti.section_order <> 1
  ) then
    raise exception 'UNTITLED_TEST_ITEM_SECTION_ORDER_INVALID';
  end if;

  if exists (
    select 1
    from auto_grading.test_items ti
    group by ti.test_set_id
    having bool_or(ti.section_title is null)
       and bool_or(ti.section_title is not null)
  ) then
    raise exception 'TEST_ITEM_SECTION_TITLE_MODE_MISMATCH';
  end if;

  if exists (
    select 1
    from auto_grading.test_items ti
    group by ti.test_set_id, ti.section_order
    having count(distinct ti.section_title) > 1
  ) then
    raise exception 'TEST_ITEM_SECTION_TITLE_MISMATCH';
  end if;

  if exists (
    select 1
    from auto_grading.test_items ti
    group by ti.test_set_id, ti.section_order, ti.display_item_no
    having count(*) > 1
  ) then
    raise exception 'TEST_ITEM_SECTION_DISPLAY_ITEM_NO_DUPLICATE';
  end if;

  if exists (
    select 1
    from auto_grading.test_items ti
    group by ti.test_set_id, ti.display_order
    having count(*) > 1
  ) then
    raise exception 'TEST_ITEM_DISPLAY_ORDER_DUPLICATE';
  end if;
end;
$block$;

alter table auto_grading.test_items
  alter column display_item_no set not null,
  alter column section_order set not null,
  alter column display_order set not null;

-- --------------------------------------------------------------------------
-- 3. 행 단위 값과 시험지 내 유일성 제약
-- --------------------------------------------------------------------------
do $block$
begin
  if not exists (
    select 1
    from pg_constraint con
    where con.conrelid = 'auto_grading.test_items'::regclass
      and con.conname = 'chk_test_items_display_item_no'
  ) then
    alter table auto_grading.test_items
      add constraint chk_test_items_display_item_no
      check (display_item_no > 0);
  end if;

  if not exists (
    select 1
    from pg_constraint con
    where con.conrelid = 'auto_grading.test_items'::regclass
      and con.conname = 'chk_test_items_section_order'
  ) then
    alter table auto_grading.test_items
      add constraint chk_test_items_section_order
      check (section_order > 0);
  end if;

  if not exists (
    select 1
    from pg_constraint con
    where con.conrelid = 'auto_grading.test_items'::regclass
      and con.conname = 'chk_test_items_section_title'
  ) then
    alter table auto_grading.test_items
      add constraint chk_test_items_section_title
      check (
        section_title is null
        or (
          section_title = btrim(section_title)
          and char_length(section_title) between 1 and 120
        )
      );
  end if;

  if not exists (
    select 1
    from pg_constraint con
    where con.conrelid = 'auto_grading.test_items'::regclass
      and con.conname = 'chk_test_items_untitled_section_order'
  ) then
    alter table auto_grading.test_items
      add constraint chk_test_items_untitled_section_order
      check (section_title is not null or section_order = 1);
  end if;

  if not exists (
    select 1
    from pg_constraint con
    where con.conrelid = 'auto_grading.test_items'::regclass
      and con.conname = 'chk_test_items_display_order'
  ) then
    alter table auto_grading.test_items
      add constraint chk_test_items_display_order
      check (display_order > 0);
  end if;

  if not exists (
    select 1
    from pg_constraint con
    where con.conrelid = 'auto_grading.test_items'::regclass
      and con.conname = 'uq_test_items_test_set_section_display_item_no'
  ) then
    alter table auto_grading.test_items
      add constraint uq_test_items_test_set_section_display_item_no
      unique (test_set_id, section_order, display_item_no);
  end if;

  if not exists (
    select 1
    from pg_constraint con
    where con.conrelid = 'auto_grading.test_items'::regclass
      and con.conname = 'uq_test_items_test_set_display_order'
  ) then
    alter table auto_grading.test_items
      add constraint uq_test_items_test_set_display_order
      unique (test_set_id, display_order);
  end if;
end;
$block$;

comment on column auto_grading.test_items.item_no is
  '제출·채점·재풀이 API가 사용하는 시험지 내부 불변 문항번호. 표시번호와 구분한다.';
comment on column auto_grading.test_items.display_item_no is
  '학생에게 표시하는 섹션 내부 문항번호. 같은 번호는 다른 섹션에서 반복할 수 있다.';
comment on column auto_grading.test_items.section_order is
  '시험지 안의 섹션 출력 순서. 제목 없는 시험지는 1이다.';
comment on column auto_grading.test_items.section_title is
  'CSV 제목 행에서 가져온 섹션 제목. 제목 없는 시험지는 NULL, 최대 120자.';
comment on column auto_grading.test_items.display_order is
  '시험지 전체 문항 출력 순서. 시험지 안에서 유일하며, item_no가 불변이므로 향후 문항 재배치는 이 값만 변경한다.';

-- --------------------------------------------------------------------------
-- 4. 구버전 쓰기 호환 + 비정규화 섹션 일관성 강제
-- --------------------------------------------------------------------------
create or replace function auto_grading.trg_test_items_prepare_display_metadata()
returns trigger
language plpgsql
security definer
set search_path to 'auto_grading', 'public'
as $function$
begin
  -- item_no는 제출·재풀이 API의 내부 키다. 이 함수는 item_no를 절대
  -- 대입하거나 재번호하지 않고, 표시용 메타데이터만 보충한다.
  if tg_op = 'INSERT' then
    new.display_item_no := coalesce(new.display_item_no, new.item_no);
    new.section_order := coalesce(new.section_order, 1);
    new.display_order := coalesce(new.display_order, new.item_no);
  end if;

  new.section_title := nullif(btrim(new.section_title), '');

  if new.section_title is null and new.section_order <> 1 then
    raise exception 'UNTITLED_TEST_ITEM_SECTION_ORDER_INVALID';
  end if;

  -- 같은 시험지에 대한 동시 INSERT도 섹션 모드 검증을 우회하지 못하도록
  -- 부모 test_sets 행을 잠근다. 존재하지 않는 부모는 기존 FK가 거부한다.
  perform 1
  from auto_grading.test_sets ts
  where ts.id = new.test_set_id
  for update;

  if exists (
    select 1
    from auto_grading.test_items ti
    where ti.test_set_id = new.test_set_id
      and ti.id is distinct from new.id
      and (ti.section_title is null)
        is distinct from (new.section_title is null)
  ) then
    raise exception 'TEST_ITEM_SECTION_TITLE_MODE_MISMATCH';
  end if;

  if exists (
    select 1
    from auto_grading.test_items ti
    where ti.test_set_id = new.test_set_id
      and ti.id is distinct from new.id
      and ti.section_order = new.section_order
      and ti.section_title is distinct from new.section_title
  ) then
    raise exception 'TEST_ITEM_SECTION_TITLE_MISMATCH';
  end if;

  return new;
end;
$function$;

drop trigger if exists trg_test_items_prepare_display_metadata
  on auto_grading.test_items;

create trigger trg_test_items_prepare_display_metadata
before insert or update of
  test_set_id,
  display_item_no,
  section_order,
  section_title,
  display_order
on auto_grading.test_items
for each row
execute function auto_grading.trg_test_items_prepare_display_metadata();

revoke execute on function auto_grading.trg_test_items_prepare_display_metadata()
  from public, anon, authenticated, service_role;

-- --------------------------------------------------------------------------
-- 5. 내부 문항 정체성 보호
--    responses는 UUID를 참조하지만 제출 payload와 재풀이 번호 배열은 item_no를
--    사용한다. test_set_id/item_no는 INSERT 이후 변경할 수 없다.
-- --------------------------------------------------------------------------
create or replace function auto_grading.trg_test_items_protect_identity()
returns trigger
language plpgsql
security definer
set search_path to 'auto_grading', 'public'
as $function$
begin
  if new.test_set_id is distinct from old.test_set_id
     or new.item_no is distinct from old.item_no then
    raise exception 'TEST_ITEM_IDENTITY_IMMUTABLE';
  end if;

  return new;
end;
$function$;

drop trigger if exists trg_test_items_protect_identity
  on auto_grading.test_items;

create trigger trg_test_items_protect_identity
before update of test_set_id, item_no
on auto_grading.test_items
for each row
execute function auto_grading.trg_test_items_protect_identity();

revoke execute on function auto_grading.trg_test_items_protect_identity()
  from public, anon, authenticated, service_role;

commit;

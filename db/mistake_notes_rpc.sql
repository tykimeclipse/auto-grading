-- ================================================================
-- mistake_notes 학생용 RPC (Phase 2)
--
-- 학생은 public_token + RPC 로만 접근. 테이블 직접 접근은 차단된 상태
-- (db/mistake_notes.sql Step 6 참조).
--
-- 토큰 검증은 _resolve_student_id_by_token() helper 로 일원화.
-- helper 는 PostgREST 에 노출되지 않도록 anon/authenticated 에서 revoke.
--
-- 함수 목록:
--   _resolve_student_id_by_token(uuid)             -- internal helper
--   create_mistake_note_by_token(uuid, uuid, uuid)
--   attach_mistake_image_by_token(...)
--   delete_mistake_image_by_token(uuid, uuid)
--   submit_mistake_note_by_token(uuid, uuid)
--   list_mistake_notes_by_token(uuid, int, int)
--   get_mistake_note_detail_by_token(uuid, uuid)
--
-- 모든 함수는 실패 시 raise 대신 jsonb {ok:false, error:'...'} 반환
-- (학생 화면에서 친절한 메시지로 변환하기 위함).
-- ================================================================


-- ── Step 0: 전제 조건 ────────────────────────────────────────────
do $$
begin
  if to_regclass('auto_grading.mistake_notes') is null
  or to_regclass('auto_grading.mistake_images') is null then
    raise exception 'mistake_notes/mistake_images 테이블이 없습니다. db/mistake_notes.sql 먼저 실행하세요.';
  end if;
  if to_regclass('auto_grading.student_public_links') is null then
    raise exception 'student_public_links 테이블이 없습니다. db/student_public_links.sql 먼저 실행하세요.';
  end if;
  -- 단원 매핑에 test_sets.unit_code 를 참조한다 (curriculum_units.sql 에서 추가됨).
  if not exists (
    select 1 from information_schema.columns
    where table_schema = 'auto_grading'
      and table_name   = 'test_sets'
      and column_name  = 'unit_code'
  ) then
    raise exception 'test_sets.unit_code 컬럼이 없습니다. db/curriculum_units.sql 먼저 실행하세요.';
  end if;
end $$;


-- ================================================================
-- 1. internal helper: 토큰 → student_id 변환
--    유효하지 않으면 null 반환 (raise 금지).
--    PostgREST 노출 금지.
-- ================================================================
drop function if exists auto_grading._resolve_student_id_by_token(uuid);

create or replace function auto_grading._resolve_student_id_by_token(
  p_token uuid
)
returns uuid
language sql
stable
security definer
set search_path to 'auto_grading', 'public'
as $$
  select l.student_id
  from auto_grading.student_public_links l
  join auto_grading.students s on s.id = l.student_id
  where l.public_token = p_token
    and l.is_active    = true
    and (l.expires_at is null or l.expires_at > now())
    and s.is_active    = true
  limit 1;
$$;

revoke all on function auto_grading._resolve_student_id_by_token(uuid)
  from public, anon, authenticated;
-- service_role 도 직접 호출할 일 없지만 default privileges 차단을 위해 명시 revoke 만 적용.


-- ================================================================
-- 2. create_mistake_note_by_token
--    · 토큰 → student_id
--    · assignment/attempt 소유 + test_set 일치 검증
--    · 2차 채점 완료 상태(completed/needs_review + round2_submitted_at)면 허용.
--      오답 0(만점)도 허용 — 헷갈렸던 문제를 스스로 남길 수 있도록.
--    · 2차 오답 수는 wrong_count_snapshot 용으로만 계산 (차단 안 함)
--    · 멱등성:
--        - 기존 draft note 있으면 그 note_id 반환 (reused:true)
--        - submitted active note 있으면 error: already_submitted
--    · curriculum_units 매핑이 있으면 unit_code/curriculum_unit_id 복사
-- ================================================================
drop function if exists auto_grading.create_mistake_note_by_token(uuid, uuid, uuid);

create or replace function auto_grading.create_mistake_note_by_token(
  p_token         uuid,
  p_assignment_id uuid,
  p_attempt_id    uuid
)
returns jsonb
language plpgsql
security definer
set search_path to 'auto_grading', 'public'
as $$
declare
  v_student_id          uuid;
  v_assignment          record;
  v_attempt             record;
  v_test_set_id         uuid;
  v_existing            record;
  v_wrong_count         integer;
  v_curriculum_unit_id  uuid;
  v_unit_code           text;
  v_unit_title_snapshot text;
  v_unit_mapping_status text;
  v_note_id             uuid;
  v_constraint          text;
begin
  -- 1. 토큰 검증
  v_student_id := auto_grading._resolve_student_id_by_token(p_token);
  if v_student_id is null then
    return jsonb_build_object('ok', false, 'error', 'unauthorized');
  end if;

  -- 2. assignment 소유 검증
  select a.id, a.student_id, a.test_set_id
    into v_assignment
    from auto_grading.assignments a
   where a.id = p_assignment_id;

  if not found or v_assignment.student_id <> v_student_id then
    return jsonb_build_object('ok', false, 'error', 'assignment_not_owned');
  end if;

  -- 3. attempt 검증: 소유 + assignment 일치 + 2차 채점 완료
  select a.id, a.student_id, a.test_set_id, a.assignment_id, a.status,
         a.round2_submitted_at
    into v_attempt
    from auto_grading.attempts a
   where a.id = p_attempt_id;

  if not found then
    return jsonb_build_object('ok', false, 'error', 'attempt_not_found');
  end if;

  if v_attempt.student_id <> v_student_id
     or v_attempt.assignment_id is distinct from p_assignment_id then
    return jsonb_build_object('ok', false, 'error', 'attempt_not_owned');
  end if;

  -- 2차 채점 완료 상태만 필수 조건. 오답 0(만점)이어도 허용 —
  -- 학생이 헷갈렸거나 다시 보고 싶은 문제를 스스로 오답노트에 남길 수 있도록.
  if v_attempt.status not in ('completed', 'needs_review')
     or v_attempt.round2_submitted_at is null then
    return jsonb_build_object('ok', false, 'error', 'attempt_not_eligible',
                              'status', v_attempt.status);
  end if;

  -- attempt 와 assignment 의 test_set 일치 확인.
  -- composite FK 가 insert 시점에 막아주지만, 친화적 에러를 위해 사전 검증.
  if v_attempt.test_set_id is distinct from v_assignment.test_set_id then
    return jsonb_build_object('ok', false, 'error', 'attempt_assignment_mismatch');
  end if;

  v_test_set_id := v_attempt.test_set_id;

  -- 4. 2차 오답 수 계산 (wrong_count_snapshot 용. submit_round2_v2 와 동일 로직)
  --    attempts.incorrect_count_after_round2 는 현재 미사용이라 신뢰 금지.
  --    오답 0 도 허용하므로 차단하지 않고 snapshot 으로만 저장.
  select count(*)
    into v_wrong_count
    from auto_grading.responses r1
    join auto_grading.test_items ti on ti.id = r1.test_item_id
    left join auto_grading.responses r2
      on r2.attempt_id   = r1.attempt_id
     and r2.test_item_id = r1.test_item_id
     and r2.round_no     = 2
   where r1.attempt_id = p_attempt_id
     and r1.round_no   = 1
     and r1.is_correct = false
     and ti.test_set_id = v_test_set_id
     and coalesce(r2.is_correct, false) = false;

  -- 5. 멱등성: 같은 attempt 에 active note 가 있는지 확인
  select id, status
    into v_existing
    from auto_grading.mistake_notes
   where attempt_id = p_attempt_id
     and status in ('draft','submitted');

  if found then
    if v_existing.status = 'draft' then
      return jsonb_build_object('ok', true,
                                'note_id', v_existing.id,
                                'reused', true);
    else
      return jsonb_build_object('ok', false,
                                'error', 'already_submitted',
                                'note_id', v_existing.id);
    end if;
  end if;

  -- 6. 단원 매핑
  --    test_sets.curriculum_unit_id FK 추가는 후속 마이그레이션이라
  --    현재는 unit_code 매칭만 시도. 없으면 unmapped.
  select ts.unit_code,
         nullif(
           concat_ws(' - ',
             nullif(btrim(ts.subject),    ''),
             nullif(btrim(ts.major_unit), ''),
             nullif(btrim(ts.minor_unit), '')
           ),
           ''
         )
    into v_unit_code, v_unit_title_snapshot
    from auto_grading.test_sets ts
   where ts.id = v_test_set_id;

  -- curriculum_units 의 유니크 키는 (grade_level, curriculum_version, subject,
  -- unit_code) 4개 조합이다. unit_code 단독 조회는 동일 코드가 다른 학년/개정/
  -- 과목에 있을 때 비결정적으로 엉뚱한 단원에 매핑된다 → test_sets 의 4개 컬럼을
  -- 모두 매칭해야 정확하다. 4개 중 하나라도 NULL 이면 매칭 실패 → unmapped.
  if v_unit_code is not null then
    select cu.id
      into v_curriculum_unit_id
      from auto_grading.curriculum_units cu
      join auto_grading.test_sets ts on ts.id = v_test_set_id
     where cu.unit_code          = ts.unit_code
       and cu.grade_level        = ts.grade_level
       and cu.curriculum_version = ts.curriculum_version
       and cu.subject            = ts.subject
       and cu.is_active          = true;
  end if;

  v_unit_mapping_status := case
    when v_curriculum_unit_id is not null then 'mapped'
    else 'unmapped'
  end;

  -- 7. note 생성
  insert into auto_grading.mistake_notes (
    student_id, assignment_id, attempt_id, test_set_id,
    curriculum_unit_id, unit_code, unit_title_snapshot, unit_mapping_status,
    source_type, source_round, wrong_count_snapshot,
    status
  )
  values (
    v_student_id, p_assignment_id, p_attempt_id, v_test_set_id,
    v_curriculum_unit_id, v_unit_code, v_unit_title_snapshot, v_unit_mapping_status,
    'auto_grading', 'after_round2', v_wrong_count,
    'draft'
  )
  returning id into v_note_id;

  return jsonb_build_object('ok', true,
                            'note_id', v_note_id,
                            'reused', false,
                            'wrong_count_snapshot', v_wrong_count);
exception
  when unique_violation then
    -- mistake_notes_attempt_active_uidx 경합(동시 클릭)만 멱등 처리한다.
    -- 다른 unique 제약 위반은 데이터 문제를 숨기지 않도록 그대로 전파.
    get stacked diagnostics v_constraint = constraint_name;
    if v_constraint is distinct from 'mistake_notes_attempt_active_uidx' then
      raise;
    end if;
    select id, status into v_existing
      from auto_grading.mistake_notes
     where attempt_id = p_attempt_id
       and status in ('draft','submitted')
     limit 1;
    if found and v_existing.status = 'draft' then
      return jsonb_build_object('ok', true,
                                'note_id', v_existing.id,
                                'reused', true);
    end if;
    return jsonb_build_object('ok', false, 'error', 'conflict');
end;
$$;

-- [보안 잠금 2026-06-23] 오답노트 기능 중단·미게이트 상태 — 공개(anon)/로그인(authenticated) 노출 금지.
-- grant 비활성화 + default privileges 로 authenticated 에 자동 부여되는 EXECUTE 까지 revoke 한다.
-- (revoke 가 없으면 이 파일 재실행 시 함수 재생성만으로 authenticated 가 다시 뚫린다.)
-- 재개(보안 게이트 통과) 후 anon 이 아니라 note-scoped 토큰 검증 경로로만 재부여할 것. [[mistake-note-deploy-gate]]
-- grant execute on function auto_grading.create_mistake_note_by_token(uuid, uuid, uuid)
--   to anon, authenticated, service_role;
revoke execute on function auto_grading.create_mistake_note_by_token(uuid, uuid, uuid) from anon, authenticated;
-- 내부 헬퍼도 default privilege 자동부여 차단
revoke execute on function auto_grading._resolve_student_id_by_token(uuid) from anon, authenticated;

comment on function auto_grading.create_mistake_note_by_token(uuid, uuid, uuid)
  is '학생용. 토큰 검증 + attempt(needs_review) 소유 검증 + 멱등 생성. 실패 시 jsonb error 반환.';


-- ================================================================
-- 3. attach_mistake_image_by_token
--    · note 소유 + status='draft' 확인
--    · sort_order 는 max+1 자동 부여
--    · 테이블 CHECK 가 mime_type/file_size/width/height 검증
--    · (note_id, file_key) unique violation 시 'duplicate_file_key' 반환
-- ================================================================
drop function if exists auto_grading.attach_mistake_image_by_token(
  uuid, uuid, text, text, text, text, integer, integer, integer
);

create or replace function auto_grading.attach_mistake_image_by_token(
  p_token         uuid,
  p_note_id       uuid,
  p_file_backend  text,
  p_file_key      text,
  p_thumbnail_key text,
  p_mime_type     text,
  p_file_size     integer,
  p_width         integer,
  p_height        integer
)
returns jsonb
language plpgsql
security definer
set search_path to 'auto_grading', 'public'
as $$
declare
  v_student_id    uuid;
  v_note          record;
  v_sort_order    integer;
  v_image_id      uuid;
  v_file_backend  text := coalesce(nullif(btrim(p_file_backend), ''), 'local_file_server');
begin
  v_student_id := auto_grading._resolve_student_id_by_token(p_token);
  if v_student_id is null then
    return jsonb_build_object('ok', false, 'error', 'unauthorized');
  end if;

  if p_file_key is null or btrim(p_file_key) = '' then
    return jsonb_build_object('ok', false, 'error', 'file_key_required');
  end if;

  select id, student_id, status
    into v_note
    from auto_grading.mistake_notes
   where id = p_note_id
   for update;

  if not found or v_note.student_id <> v_student_id then
    return jsonb_build_object('ok', false, 'error', 'note_not_owned');
  end if;

  if v_note.status <> 'draft' then
    return jsonb_build_object('ok', false, 'error', 'note_not_editable',
                              'status', v_note.status);
  end if;

  -- 다음 sort_order = max+1 (현재는 -1 + 1 = 0 부터)
  select coalesce(max(sort_order), -1) + 1
    into v_sort_order
    from auto_grading.mistake_images
   where note_id = p_note_id;

  insert into auto_grading.mistake_images (
    note_id, student_id,
    file_backend, file_key, thumbnail_key,
    mime_type, file_size, width, height, sort_order
  ) values (
    p_note_id, v_student_id,
    v_file_backend, p_file_key, p_thumbnail_key,
    p_mime_type, p_file_size, p_width, p_height, v_sort_order
  )
  returning id into v_image_id;

  return jsonb_build_object('ok', true,
                            'image_id',   v_image_id,
                            'sort_order', v_sort_order);
exception
  when unique_violation then
    return jsonb_build_object('ok', false, 'error', 'duplicate_file_key');
  when check_violation then
    return jsonb_build_object('ok', false, 'error', 'invalid_image_metadata');
  when foreign_key_violation then
    -- composite FK: note↔student 불일치는 위에서 거름. 여기 도달 시 시스템 이슈.
    return jsonb_build_object('ok', false, 'error', 'fk_violation');
end;
$$;

-- [보안 잠금 2026-06-23] anon/authenticated 노출 금지. grant 비활성화 + 자동부여 revoke. [[mistake-note-deploy-gate]]
-- grant execute on function auto_grading.attach_mistake_image_by_token(
--   uuid, uuid, text, text, text, text, integer, integer, integer
-- ) to anon, authenticated, service_role;
revoke execute on function auto_grading.attach_mistake_image_by_token(
  uuid, uuid, text, text, text, text, integer, integer, integer
) from anon, authenticated;

comment on function auto_grading.attach_mistake_image_by_token(
  uuid, uuid, text, text, text, text, integer, integer, integer
) is '학생용. draft 노트에 이미지 메타 1건 추가. file_key 중복/제약 위반은 jsonb error 로 변환.';


-- ================================================================
-- 4. delete_mistake_image_by_token
--    · draft 상태 노트의 이미지만 삭제 가능
--    · 파일서버가 정리할 수 있도록 file_key 들을 응답에 포함
--    · image_count 는 트리거가 자동 갱신
-- ================================================================
drop function if exists auto_grading.delete_mistake_image_by_token(uuid, uuid);

create or replace function auto_grading.delete_mistake_image_by_token(
  p_token    uuid,
  p_image_id uuid
)
returns jsonb
language plpgsql
security definer
set search_path to 'auto_grading', 'public'
as $$
declare
  v_student_id uuid;
  v_image      record;
  v_status     text;
begin
  v_student_id := auto_grading._resolve_student_id_by_token(p_token);
  if v_student_id is null then
    return jsonb_build_object('ok', false, 'error', 'unauthorized');
  end if;

  select i.id, i.note_id, i.student_id,
         i.file_backend, i.file_key, i.thumbnail_key
    into v_image
    from auto_grading.mistake_images i
   where i.id = p_image_id;

  if not found or v_image.student_id <> v_student_id then
    return jsonb_build_object('ok', false, 'error', 'image_not_owned');
  end if;

  -- parent note 를 for update 로 잠근 뒤 상태 확인 + 삭제.
  -- submit_mistake_note_by_token 도 note 를 for update 로 잠그므로,
  -- '전송'과 '이미지 삭제'가 동시에 일어나도 직렬화되어
  -- submitted 노트가 이미지 0장이 되는 race 를 막는다.
  select status into v_status
    from auto_grading.mistake_notes
   where id = v_image.note_id
   for update;

  if v_status <> 'draft' then
    return jsonb_build_object('ok', false, 'error', 'note_not_editable',
                              'status', v_status);
  end if;

  delete from auto_grading.mistake_images where id = p_image_id;

  return jsonb_build_object('ok', true,
                            'image_id',      p_image_id,
                            'file_backend',  v_image.file_backend,
                            'file_key',      v_image.file_key,
                            'thumbnail_key', v_image.thumbnail_key);
end;
$$;

-- [보안 잠금 2026-06-23] anon/authenticated 노출 금지. grant 비활성화 + 자동부여 revoke. [[mistake-note-deploy-gate]]
-- grant execute on function auto_grading.delete_mistake_image_by_token(uuid, uuid)
--   to anon, authenticated, service_role;
revoke execute on function auto_grading.delete_mistake_image_by_token(uuid, uuid) from anon, authenticated;

comment on function auto_grading.delete_mistake_image_by_token(uuid, uuid)
  is '학생용. draft 상태 노트의 이미지 1건 삭제. 응답에 file_key 포함 (파일서버 cleanup 용).';


-- ================================================================
-- 5. submit_mistake_note_by_token
--    · draft → submitted
--    · DB 의 image_count >= 1 일 때만 허용 (프론트 값 신뢰 금지)
-- ================================================================
drop function if exists auto_grading.submit_mistake_note_by_token(uuid, uuid);

create or replace function auto_grading.submit_mistake_note_by_token(
  p_token   uuid,
  p_note_id uuid
)
returns jsonb
language plpgsql
security definer
set search_path to 'auto_grading', 'public'
as $$
declare
  v_student_id uuid;
  v_note       record;
begin
  v_student_id := auto_grading._resolve_student_id_by_token(p_token);
  if v_student_id is null then
    return jsonb_build_object('ok', false, 'error', 'unauthorized');
  end if;

  select id, student_id, status, image_count
    into v_note
    from auto_grading.mistake_notes
   where id = p_note_id
   for update;

  if not found or v_note.student_id <> v_student_id then
    return jsonb_build_object('ok', false, 'error', 'note_not_owned');
  end if;

  if v_note.status <> 'draft' then
    return jsonb_build_object('ok', false, 'error', 'note_not_draft',
                              'status', v_note.status);
  end if;

  if v_note.image_count < 1 then
    return jsonb_build_object('ok', false, 'error', 'no_images',
                              'image_count', v_note.image_count);
  end if;

  update auto_grading.mistake_notes
     set status = 'submitted'
   where id = p_note_id;

  return jsonb_build_object('ok', true,
                            'note_id',     p_note_id,
                            'status',      'submitted',
                            'image_count', v_note.image_count);
end;
$$;

-- [보안 잠금 2026-06-23] anon/authenticated 노출 금지. grant 비활성화 + 자동부여 revoke. [[mistake-note-deploy-gate]]
-- grant execute on function auto_grading.submit_mistake_note_by_token(uuid, uuid)
--   to anon, authenticated, service_role;
revoke execute on function auto_grading.submit_mistake_note_by_token(uuid, uuid) from anon, authenticated;

comment on function auto_grading.submit_mistake_note_by_token(uuid, uuid)
  is '학생용. draft 노트를 submitted 로 전환. DB image_count >= 1 강제.';


-- ================================================================
-- 6. list_mistake_notes_by_token
--    · 본인 노트 (archived 제외) 페이지네이션 리스트
--    · 카드/리스트 화면용 요약 필드만 반환
-- ================================================================
drop function if exists auto_grading.list_mistake_notes_by_token(uuid, integer, integer);

create or replace function auto_grading.list_mistake_notes_by_token(
  p_token  uuid,
  p_limit  integer default 50,
  p_offset integer default 0
)
returns jsonb
language plpgsql
security definer
set search_path to 'auto_grading', 'public'
as $$
declare
  v_student_id uuid;
  v_limit      integer;
  v_offset     integer;
  v_total      integer;
  v_rows       jsonb;
begin
  v_student_id := auto_grading._resolve_student_id_by_token(p_token);
  if v_student_id is null then
    return jsonb_build_object('ok', false, 'error', 'unauthorized');
  end if;

  v_limit  := greatest(1, least(coalesce(p_limit, 50), 200));
  v_offset := greatest(0, coalesce(p_offset, 0));

  select count(*)
    into v_total
    from auto_grading.mistake_notes mn
   where mn.student_id = v_student_id
     and mn.status <> 'archived';

  with paged as (
    select mn.id, mn.test_set_id, mn.unit_title_snapshot, mn.unit_code,
           mn.image_count, mn.wrong_count_snapshot, mn.status,
           mn.created_at, mn.updated_at,
           ts.title as test_title
      from auto_grading.mistake_notes mn
      join auto_grading.test_sets ts on ts.id = mn.test_set_id
     where mn.student_id = v_student_id
       and mn.status <> 'archived'
     order by mn.created_at desc, mn.id desc
     limit v_limit offset v_offset
  )
  select coalesce(
    jsonb_agg(
      jsonb_build_object(
        'note_id',              p.id,
        'test_set_id',          p.test_set_id,
        'test_title',           p.test_title,
        'unit_title_snapshot',  p.unit_title_snapshot,
        'unit_code',            p.unit_code,
        'image_count',          p.image_count,
        'wrong_count_snapshot', p.wrong_count_snapshot,
        'status',               p.status,
        'created_at',           p.created_at,
        'updated_at',           p.updated_at
      )
      order by p.created_at desc, p.id desc
    ),
    '[]'::jsonb
  )
  into v_rows
  from paged p;

  return jsonb_build_object(
    'ok',     true,
    'total',  v_total,
    'limit',  v_limit,
    'offset', v_offset,
    'rows',   v_rows
  );
end;
$$;

-- [보안 잠금 2026-06-23] anon/authenticated 노출 금지. grant 비활성화 + 자동부여 revoke. [[mistake-note-deploy-gate]]
-- grant execute on function auto_grading.list_mistake_notes_by_token(uuid, integer, integer)
--   to anon, authenticated, service_role;
revoke execute on function auto_grading.list_mistake_notes_by_token(uuid, integer, integer) from anon, authenticated;

comment on function auto_grading.list_mistake_notes_by_token(uuid, integer, integer)
  is '학생용. 본인 노트 리스트(페이지네이션). archived 제외, 최신순.';


-- ================================================================
-- 7. get_mistake_note_detail_by_token
--    · 본인 노트 + 이미지 리스트
--    · archived 도 조회 가능 (히스토리 열람용)
-- ================================================================
drop function if exists auto_grading.get_mistake_note_detail_by_token(uuid, uuid);

create or replace function auto_grading.get_mistake_note_detail_by_token(
  p_token   uuid,
  p_note_id uuid
)
returns jsonb
language plpgsql
security definer
set search_path to 'auto_grading', 'public'
as $$
declare
  v_student_id uuid;
  v_note       record;
  v_images     jsonb;
begin
  v_student_id := auto_grading._resolve_student_id_by_token(p_token);
  if v_student_id is null then
    return jsonb_build_object('ok', false, 'error', 'unauthorized');
  end if;

  select mn.id, mn.student_id, mn.assignment_id, mn.attempt_id, mn.test_set_id,
         mn.curriculum_unit_id, mn.unit_code, mn.unit_title_snapshot,
         mn.unit_mapping_status, mn.source_type, mn.source_round,
         mn.wrong_count_snapshot, mn.title, mn.memo, mn.image_count,
         mn.status, mn.created_at, mn.updated_at,
         ts.title       as test_title,
         s.name         as student_name,
         s.student_code as student_code
    into v_note
    from auto_grading.mistake_notes mn
    join auto_grading.test_sets ts on ts.id = mn.test_set_id
    join auto_grading.students  s  on s.id  = mn.student_id
   where mn.id = p_note_id;

  if not found or v_note.student_id <> v_student_id then
    return jsonb_build_object('ok', false, 'error', 'note_not_owned');
  end if;

  select coalesce(
    jsonb_agg(
      jsonb_build_object(
        'image_id',      i.id,
        'file_backend',  i.file_backend,
        'file_key',      i.file_key,
        'thumbnail_key', i.thumbnail_key,
        'mime_type',     i.mime_type,
        'file_size',     i.file_size,
        'width',         i.width,
        'height',        i.height,
        'sort_order',    i.sort_order,
        'created_at',    i.created_at
      )
      order by i.sort_order, i.created_at
    ),
    '[]'::jsonb
  )
  into v_images
  from auto_grading.mistake_images i
  where i.note_id = p_note_id;

  return jsonb_build_object(
    'ok', true,
    'note', jsonb_build_object(
      'note_id',              v_note.id,
      'student_name',         v_note.student_name,
      'student_code',         v_note.student_code,
      'assignment_id',        v_note.assignment_id,
      'attempt_id',           v_note.attempt_id,
      'test_set_id',          v_note.test_set_id,
      'test_title',           v_note.test_title,
      'curriculum_unit_id',   v_note.curriculum_unit_id,
      'unit_code',            v_note.unit_code,
      'unit_title_snapshot',  v_note.unit_title_snapshot,
      'unit_mapping_status',  v_note.unit_mapping_status,
      'source_type',          v_note.source_type,
      'source_round',         v_note.source_round,
      'wrong_count_snapshot', v_note.wrong_count_snapshot,
      'title',                v_note.title,
      'memo',                 v_note.memo,
      'image_count',          v_note.image_count,
      'status',               v_note.status,
      'created_at',           v_note.created_at,
      'updated_at',           v_note.updated_at
    ),
    'images', v_images
  );
end;
$$;

-- [보안 잠금 2026-06-23] anon/authenticated 노출 금지. grant 비활성화 + 자동부여 revoke. [[mistake-note-deploy-gate]]
-- grant execute on function auto_grading.get_mistake_note_detail_by_token(uuid, uuid)
--   to anon, authenticated, service_role;
revoke execute on function auto_grading.get_mistake_note_detail_by_token(uuid, uuid) from anon, authenticated;

comment on function auto_grading.get_mistake_note_detail_by_token(uuid, uuid)
  is '학생용. 본인 노트 1건 + 이미지 리스트 반환. archived 도 조회 가능.';


-- ================================================================
-- 8. PostgREST 스키마 캐시 reload
-- ================================================================
notify pgrst, 'reload schema';


-- ── 검증 시나리오 (실행 후 직접 확인) ───────────────────────────
--
-- 1) 토큰 없이 호출 시 error: unauthorized
-- select auto_grading.create_mistake_note_by_token(
--   'ffffffff-ffff-ffff-ffff-ffffffffffff'::uuid,
--   '<assignment_id>', '<attempt_id>');
--
-- 2) 다른 학생의 attempt 로 호출 시 error: attempt_not_owned
--
-- 3) round2 미응시 attempt → error: attempt_not_eligible
--    예: 1차 100점이라 2차를 안 본 경우 (status=completed, round2_submitted_at IS NULL).
--    허용 조건은 status in ('completed','needs_review') AND round2_submitted_at IS NOT NULL.
-- 3b) 2차 채점 완료 attempt → 정상 생성. 2차 만점(오답 0)도 허용됨.
--
-- 4) 같은 attempt 로 2회 호출:
--    1회차 → ok, reused:false
--    2회차 → ok, reused:true (같은 note_id)
--
-- 4b) [멱등 핵심 경로 검증] 같은 attempt 로 동시 호출하거나 강제로 중복 insert 를
--     일으켜, unique_violation 의 CONSTRAINT_NAME 이 'mistake_notes_attempt_active_uidx'
--     로 들어오는지 확인. 다른 값이면 exception 핸들러의 멱등 처리가 동작하지 않는다.
--
-- 5) submitted note 에 attach 시도 → error: note_not_editable
--
-- 6) image_count=0 인 draft 로 submit 시도 → error: no_images
--
-- 7) _resolve_student_id_by_token 이 외부 RPC 로 노출 안 되는지 확인:
--    PostgREST /rpc/_resolve_student_id_by_token 호출 시 404 또는 권한 거부 정상.

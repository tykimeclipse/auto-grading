-- ================================================================
-- mistake_notes 교사용 RPC (Phase 3)
--
-- 교사 접근은 assert_admin() 으로 일원화.
-- 모든 함수는 첫 줄에서 perform auto_grading.assert_admin();
-- insufficient_privilege 예외는 exception 블록에서 jsonb error 로 변환.
--
-- ⚠️ 운영 배포 전 필수: teacher_get_mistake_note_detail 의 student_view_token
--    반환 기능은 학생 범용 토큰을 노출하는 임시 구조다. 외부 배포 전 반드시
--    제거해야 한다. 상세 — 함수 2 주석 및 db/mistake_notes_omr_bridge.sql
--    헤더의 보안 게이트 참조.
--
-- 함수 목록:
--   teacher_list_mistake_notes_for_assignment(uuid)
--   teacher_get_mistake_note_detail(uuid)
--   teacher_reopen_mistake_note_to_draft(uuid)
--   teacher_list_mistake_note_summaries(uuid[])     -- 발행상황 컬럼용 일괄 요약
--
-- 의도적으로 제외:
--   · teacher_list_mistake_notes_for_student      → 후속 (학생 카드/프로필 화면)
--   · teacher_archive_mistake_note                 → 후속 (운영 cleanup)
--   · teacher_comments / teacher_review_*          → 후속 (코멘트 기능)
-- ================================================================


-- ── Step 0: 전제 조건 ────────────────────────────────────────────
do $$
begin
  if to_regclass('auto_grading.mistake_notes') is null
  or to_regclass('auto_grading.mistake_images') is null then
    raise exception 'mistake_notes/mistake_images 테이블이 없습니다. db/mistake_notes.sql 먼저 실행하세요.';
  end if;
  if to_regprocedure('auto_grading.assert_admin()') is null then
    raise exception 'auto_grading.assert_admin() 함수가 없습니다. db/assert_admin.sql 먼저 실행하세요.';
  end if;
end $$;


-- ================================================================
-- 1. teacher_list_mistake_notes_for_assignment
--    한 assignment 의 모든 노트 (attempt 별, status 별 전부 — archived 포함).
--    발행상황 컬럼은 가장 최신 attempt 의 active note(draft/submitted)를 표시.
--    클라이언트가 attempt_no desc 정렬된 결과 중 status != 'archived' 첫 행을
--    "현재 상태" 로, 나머지를 "이전 회차" history 로 처리.
-- ================================================================
drop function if exists auto_grading.teacher_list_mistake_notes_for_assignment(uuid);

create or replace function auto_grading.teacher_list_mistake_notes_for_assignment(
  p_assignment_id uuid
)
returns jsonb
language plpgsql
security definer
set search_path to 'auto_grading', 'public'
as $$
declare
  v_assignment record;
  v_rows       jsonb;
begin
  perform auto_grading.assert_admin();

  if p_assignment_id is null then
    return jsonb_build_object('ok', false, 'error', 'assignment_id_required');
  end if;

  select a.id, a.student_id, a.test_set_id,
         s.name as student_name, s.student_code, s.grade_level as student_grade,
         ts.title as test_title
    into v_assignment
    from auto_grading.assignments a
    join auto_grading.students  s  on s.id  = a.student_id
    join auto_grading.test_sets ts on ts.id = a.test_set_id
   where a.id = p_assignment_id;

  if not found then
    return jsonb_build_object('ok', false, 'error', 'assignment_not_found');
  end if;

  -- assignment 안의 모든 attempt 에 걸쳐 노트 수집 (archived 포함).
  -- 최신 attempt 의 active note 가 화면 상단으로 오도록 attempt_no desc 정렬.
  with notes as (
    select mn.id, mn.attempt_id, at.attempt_no,
           mn.status, mn.image_count, mn.wrong_count_snapshot,
           mn.unit_title_snapshot, mn.unit_code, mn.unit_mapping_status,
           mn.created_at, mn.updated_at
      from auto_grading.mistake_notes mn
      join auto_grading.attempts at on at.id = mn.attempt_id
     where mn.assignment_id = p_assignment_id
  )
  select coalesce(
    jsonb_agg(
      jsonb_build_object(
        'note_id',              n.id,
        'attempt_id',           n.attempt_id,
        'attempt_no',           n.attempt_no,
        'status',               n.status,
        'image_count',          n.image_count,
        'wrong_count_snapshot', n.wrong_count_snapshot,
        'unit_title_snapshot',  n.unit_title_snapshot,
        'unit_code',            n.unit_code,
        'unit_mapping_status',  n.unit_mapping_status,
        'created_at',           n.created_at,
        'updated_at',           n.updated_at
      )
      order by n.attempt_no desc, n.created_at desc
    ),
    '[]'::jsonb
  )
  into v_rows
  from notes n;

  return jsonb_build_object(
    'ok', true,
    'assignment', jsonb_build_object(
      'assignment_id', v_assignment.id,
      'student_id',    v_assignment.student_id,
      'student_name',  v_assignment.student_name,
      'student_code',  v_assignment.student_code,
      'student_grade', v_assignment.student_grade,
      'test_set_id',   v_assignment.test_set_id,
      'test_title',    v_assignment.test_title
    ),
    'notes', v_rows
  );
exception
  when insufficient_privilege then
    return jsonb_build_object('ok', false, 'error', 'forbidden');
end;
$$;

revoke execute on function auto_grading.teacher_list_mistake_notes_for_assignment(uuid)
  from public, anon;
grant  execute on function auto_grading.teacher_list_mistake_notes_for_assignment(uuid)
  to   authenticated, service_role;

comment on function auto_grading.teacher_list_mistake_notes_for_assignment(uuid)
  is '교사용. 한 assignment 의 모든 attempt 의 노트 리스트 (archived 포함, attempt_no desc).';


-- ================================================================
-- 2. teacher_get_mistake_note_detail
--    교사 상세 화면용. 노트 + 이미지 리스트 + 학생/시험 메타 풀세트.
--    학생 RPC 와 달리 student 소유 검증 대신 admin 권한만 검증.
--
--    ╔════════════════════════════════════════════════════════════╗
--    ║ ⚠️ student_view_token 반환 — 운영 배포 전 제거 필수          ║
--    ╠════════════════════════════════════════════════════════════╣
--    ║ 이미지 표시용으로 학생 public token(student_view_token)을    ║
--    ║ 응답에 포함한다. 이 값이 브라우저 로그·네트워크 캡처·프론트  ║
--    ║ 상태·에러리포팅에 잔류하면 학생 *범용* 토큰이 유출된다.      ║
--    ║ 파일서버 /file 가 학생 token 만 인증하기 때문에 생긴 임시    ║
--    ║ 방편이다. 운영/외부 배포 전 보안 게이트(파일서버 admin JWT   ║
--    ║ 또는 note-scoped signed file URL)를 반드시 완료하고, 그      ║
--    ║ 시점에 student_view_token 반환을 제거한다.                   ║
--    ║ (보안 게이트 전문: db/mistake_notes_omr_bridge.sql 헤더)     ║
--    ╚════════════════════════════════════════════════════════════╝
--    assert_admin() 을 먼저 통과한 경우에만 token 을 발급/반환한다.
-- ================================================================
drop function if exists auto_grading.teacher_get_mistake_note_detail(uuid);

create or replace function auto_grading.teacher_get_mistake_note_detail(
  p_note_id uuid
)
returns jsonb
language plpgsql
security definer
set search_path to 'auto_grading', 'public'
as $$
declare
  v_note        record;
  v_images      jsonb;
  v_token_jsonb jsonb;
  v_view_token  uuid;
begin
  perform auto_grading.assert_admin();

  if p_note_id is null then
    return jsonb_build_object('ok', false, 'error', 'note_id_required');
  end if;

  select mn.id, mn.student_id, mn.assignment_id, mn.attempt_id, mn.test_set_id,
         mn.curriculum_unit_id, mn.unit_code, mn.unit_title_snapshot,
         mn.unit_mapping_status, mn.source_type, mn.source_round,
         mn.wrong_count_snapshot, mn.title, mn.memo, mn.image_count,
         mn.status, mn.created_at, mn.updated_at,
         s.name         as student_name,
         s.student_code as student_code,
         s.grade_level  as student_grade,
         ts.title       as test_title,
         ts.subject     as test_subject,
         ts.major_unit  as test_major_unit,
         ts.minor_unit  as test_minor_unit,
         ts.grade_level as test_grade_level,
         at.attempt_no  as attempt_no,
         at.status      as attempt_status
    into v_note
    from auto_grading.mistake_notes mn
    join auto_grading.students  s  on s.id  = mn.student_id
    join auto_grading.test_sets ts on ts.id = mn.test_set_id
    join auto_grading.attempts  at on at.id = mn.attempt_id
   where mn.id = p_note_id;

  if not found then
    return jsonb_build_object('ok', false, 'error', 'note_not_found');
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

  -- 이미지 표시용 student view token (assert_admin 통과 후에만 발급).
  -- ⚠️ 임시 브리지 — 위 함수 주석 참조.
  v_token_jsonb := auto_grading.get_or_create_student_public_token(v_note.student_id);
  if v_token_jsonb is not null and (v_token_jsonb ->> 'ok')::boolean is true then
    v_view_token := (v_token_jsonb ->> 'token')::uuid;
  end if;

  return jsonb_build_object(
    'ok', true,
    'student_view_token', v_view_token,
    -- token 발급 실패 시에도 노트 메타는 반환하되 warning 으로 알린다.
    -- 프론트는 warning 이 있으면 이미지 영역에 '이미지 토큰 발급 실패' 를 표시.
    'warning', case when v_view_token is null then 'view_token_issue_failed' end,
    'note', jsonb_build_object(
      'note_id',              v_note.id,
      'student_id',           v_note.student_id,
      'student_name',         v_note.student_name,
      'student_code',         v_note.student_code,
      'student_grade',        v_note.student_grade,
      'assignment_id',        v_note.assignment_id,
      'attempt_id',           v_note.attempt_id,
      'attempt_no',           v_note.attempt_no,
      'attempt_status',       v_note.attempt_status,
      'test_set_id',          v_note.test_set_id,
      'test_title',           v_note.test_title,
      'test_subject',         v_note.test_subject,
      'test_major_unit',      v_note.test_major_unit,
      'test_minor_unit',      v_note.test_minor_unit,
      'test_grade_level',     v_note.test_grade_level,
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
exception
  when insufficient_privilege then
    return jsonb_build_object('ok', false, 'error', 'forbidden');
end;
$$;

revoke execute on function auto_grading.teacher_get_mistake_note_detail(uuid)
  from public, anon;
grant  execute on function auto_grading.teacher_get_mistake_note_detail(uuid)
  to   authenticated, service_role;

comment on function auto_grading.teacher_get_mistake_note_detail(uuid)
  is '교사용. 노트 1건 상세 + 이미지 리스트 + 학생/시험 풀세트. student_view_token 도 반환(임시 브리지 — 파일서버 admin 인증 전환 시 제거).';


-- ================================================================
-- 3. teacher_reopen_mistake_note_to_draft
--    학생이 사진을 누락했거나 잘못 제출했을 때 교사가 submitted → draft 로
--    되돌리는 최소한의 현장 대응 함수.
--    archived 는 reopen 불가 (정책상 보존 상태).
--    draft 는 이미 draft 이므로 무동작 — 친화적 응답 반환.
-- ================================================================
drop function if exists auto_grading.teacher_reopen_mistake_note_to_draft(uuid);

create or replace function auto_grading.teacher_reopen_mistake_note_to_draft(
  p_note_id uuid
)
returns jsonb
language plpgsql
security definer
set search_path to 'auto_grading', 'public'
as $$
declare
  v_note record;
begin
  perform auto_grading.assert_admin();

  if p_note_id is null then
    return jsonb_build_object('ok', false, 'error', 'note_id_required');
  end if;

  select id, status, attempt_id
    into v_note
    from auto_grading.mistake_notes
   where id = p_note_id
   for update;

  if not found then
    return jsonb_build_object('ok', false, 'error', 'note_not_found');
  end if;

  if v_note.status = 'draft' then
    return jsonb_build_object('ok', true,
                              'note_id', p_note_id,
                              'status',  'draft',
                              'noop',    true);
  end if;

  if v_note.status <> 'submitted' then
    return jsonb_build_object('ok', false,
                              'error',  'note_not_submitted',
                              'status', v_note.status);
  end if;

  -- (attempt_id) where status in ('draft','submitted') unique partial index 가
  -- 있으나, submitted → draft 전환은 active 노트 1개 상태를 유지하므로 안전.
  update auto_grading.mistake_notes
     set status = 'draft'
   where id = p_note_id;

  return jsonb_build_object('ok', true,
                            'note_id', p_note_id,
                            'status',  'draft',
                            'noop',    false);
exception
  when insufficient_privilege then
    return jsonb_build_object('ok', false, 'error', 'forbidden');
end;
$$;

revoke execute on function auto_grading.teacher_reopen_mistake_note_to_draft(uuid)
  from public, anon;
grant  execute on function auto_grading.teacher_reopen_mistake_note_to_draft(uuid)
  to   authenticated, service_role;

comment on function auto_grading.teacher_reopen_mistake_note_to_draft(uuid)
  is '교사용. submitted 노트를 draft 로 되돌려 학생이 재편집할 수 있게 한다. archived 는 reopen 불가.';


-- ================================================================
-- 4. teacher_list_mistake_note_summaries
--    발행상황 테이블의 '오답노트' 컬럼용 일괄 요약 조회.
--    여러 assignment 의 현재 상태를 1 회 호출로 받아 N+1 을 방지.
--    각 assignment 당 active note(draft/submitted) 중 최신 1건만 반환.
--    archived 만 있거나 노트가 없는 assignment 는 결과에서 빠짐
--    → 프론트에서 '미작성' 으로 표시.
-- ================================================================
drop function if exists auto_grading.teacher_list_mistake_note_summaries(uuid[]);

create or replace function auto_grading.teacher_list_mistake_note_summaries(
  p_assignment_ids uuid[]
)
returns jsonb
language plpgsql
security definer
set search_path to 'auto_grading', 'public'
as $$
declare
  v_rows jsonb;
begin
  perform auto_grading.assert_admin();

  if p_assignment_ids is null or array_length(p_assignment_ids, 1) is null then
    return jsonb_build_object('ok', true, 'rows', '[]'::jsonb);
  end if;

  select coalesce(
    jsonb_agg(
      jsonb_build_object(
        'assignment_id', s.assignment_id,
        'note_id',       s.note_id,
        'status',        s.status,
        'image_count',   s.image_count
      )
    ),
    '[]'::jsonb
  )
  into v_rows
  from (
    -- assignment 당 '현재 상태' = 최신 attempt 의 active note.
    -- 재시험으로 attempt 가 여러 개인 경우 attempt_no 가 큰 쪽을 우선.
    select distinct on (mn.assignment_id)
           mn.assignment_id,
           mn.id          as note_id,
           mn.status,
           mn.image_count
      from auto_grading.mistake_notes mn
      join auto_grading.attempts at on at.id = mn.attempt_id
     where mn.assignment_id = any(p_assignment_ids)
       and mn.status in ('draft', 'submitted')
     order by mn.assignment_id, at.attempt_no desc, mn.created_at desc
  ) s;

  return jsonb_build_object('ok', true, 'rows', v_rows);
exception
  when insufficient_privilege then
    return jsonb_build_object('ok', false, 'error', 'forbidden');
end;
$$;

revoke execute on function auto_grading.teacher_list_mistake_note_summaries(uuid[])
  from public, anon;
grant  execute on function auto_grading.teacher_list_mistake_note_summaries(uuid[])
  to   authenticated, service_role;

comment on function auto_grading.teacher_list_mistake_note_summaries(uuid[])
  is '교사용. 발행상황 테이블 오답노트 컬럼용. 여러 assignment 의 active note 상태를 1회 일괄 조회.';


-- ================================================================
-- 5. PostgREST 스키마 캐시 reload
-- ================================================================
notify pgrst, 'reload schema';


-- ── 검증 시나리오 ────────────────────────────────────────────────
--
-- 1) 비관리자 계정으로 호출 → ok:false, error:'forbidden'
-- 2) 존재하지 않는 assignment_id → ok:false, error:'assignment_not_found'
-- 3) 노트가 0건인 assignment → ok:true, notes:[] (화면에서 "미작성" 표시용)
-- 4) submitted 노트 reopen → ok:true, status:'draft', noop:false
-- 5) 이미 draft 인 노트 reopen → ok:true, status:'draft', noop:true
-- 6) archived 노트 reopen → ok:false, error:'note_not_submitted'
-- 7) teacher_list_mistake_note_summaries: assignment 여러 개 일괄 → rows 매핑 확인
-- 8) revoke_public_anon_from_teacher_rpcs.sql 의 DO 블록을 다시 돌리면
--    이 파일의 teacher_* 함수 4개가 자동으로 PUBLIC/anon 에서 차단 확인 가능.

-- ================================================================
-- mistake_notes OMR 과도기 브리지 RPC
--
-- ╔══════════════════════════════════════════════════════════════╗
-- ║ ⚠️ 운영/외부 배포 금지 — 로컬·테스트 검증 전용                 ║
-- ╠══════════════════════════════════════════════════════════════╣
-- ║ 이 RPC 는 assignment_id + student_code 만으로                 ║
-- ║ student_public_links 의 *범용* 공개 토큰을 반환한다.          ║
-- ║ student_code 는 약한 비밀이고 assignment_id 는 URL 에 노출되며,║
-- ║ 반환 토큰은 성취도 페이지 등 모든 token 기반 페이지 접근권이다.║
-- ║                                                               ║
-- ║ 따라서 이 파일은 함수만 생성하고 anon/authenticated 에         ║
-- ║ grant 하지 않는다(파일 하단 revoke). 로컬에서 호출하려면 별도  ║
-- ║ db/mistake_notes_omr_bridge_grant_dev.sql 을 실행한다.        ║
-- ║ 운영 DB 에는 그 grant 파일을 절대 적용하지 않는다.            ║
-- ╚══════════════════════════════════════════════════════════════╝
--
-- 운영/외부 배포 전 필수 보안 게이트 (모두 충족해야 배포 가능):
--   1. OMR bridge 는 student_public_links.public_token 을 반환하지 않는다.
--   2. 오답노트 작성/조회는 note-scoped short-lived token(또는 edit_token)
--      기반으로 분리한다.
--   3. 교사용 이미지 조회는 student public token 반환 없이
--      admin JWT 또는 signed file URL 로 처리한다.
--   4. 위 1~3 완료 전에는 OMR bridge 및 teacher detail 의 token 반환 기능을
--      외부 환경에 배포하지 않는다.
--
-- 장기적으로는 교사 발행 링크 자체를 token 포함 링크로 변경하고,
-- OMR 도 token 기반 RPC 로 완전히 전환하며, 이 함수는 제거한다.
--
-- 배경:
--   현재 OMR (student-answer-omr-v3) 페이지는 [security-migration] 과도기로
--   (assignment_id + student_code) 인증으로 동작한다. token 인증으로 완전
--   전환되기 전까지, OMR 에서 "오답노트 만들기" 버튼을 누른 학생을 위해
--   서버 측에서 소유권을 검증하고 표준 token 기반 흐름으로 연결해주는
--   다리 역할이 필요하다.
--
-- 흐름:
--   1. OMR 에서 '오답노트 만들기' 클릭
--   2. 이 RPC 호출 → 검증 + note 생성(멱등) + active token 발급
--   3. 학생이 mistake-note-write-v1.html?token=...&note_id=... 로 이동
--   4. 이후는 표준 attach/submit/detail RPC 흐름과 동일
--
-- 보안 모델:
--   · assignment_id 와 student_code 의 매칭, attempt 의 assignment 소속,
--     attempt 와 assignment 의 test_set 일치, 2차 채점 완료 상태까지 서버 검증.
--     오답 0(만점)도 허용 — 학생이 헷갈렸던 문제를 스스로 남길 수 있도록.
--   · 검증 통과 시 학생의 long-lived active token 을 반환한다.
--     이는 OMR 진입 자체가 이미 assignment_id 노출 상태이므로 보안 등급
--     동등하다는 판단에 따른 과도기 결정. short-lived token 으로의 전환은
--     교사 발행 링크가 token 포함 링크로 변경되는 시점에 함께 정리.
--   · 프론트 구현 규칙: 반환된 token 전체를 console / debug log 에 출력하지 말 것.
-- ================================================================


-- ── Step 0: 전제 조건 ────────────────────────────────────────────
do $$
begin
  if to_regclass('auto_grading.mistake_notes') is null then
    raise exception 'mistake_notes 테이블이 없습니다. db/mistake_notes.sql 먼저 실행하세요.';
  end if;
  if to_regprocedure('auto_grading.get_or_create_student_public_token(uuid)') is null then
    raise exception 'get_or_create_student_public_token 가 없습니다. db/student_public_links.sql 먼저 실행하세요.';
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
-- create_mistake_note_via_omr_bridge
-- ================================================================
drop function if exists auto_grading.create_mistake_note_via_omr_bridge(uuid, text, uuid);

create or replace function auto_grading.create_mistake_note_via_omr_bridge(
  p_assignment_id uuid,
  p_student_code  text,
  p_attempt_id    uuid
)
returns jsonb
language plpgsql
security definer
set search_path to 'auto_grading', 'public'
as $$
declare
  v_student_code        text;
  v_student_id          uuid;
  v_assignment          record;
  v_attempt             record;
  v_test_set_id         uuid;
  v_wrong_count         integer;
  v_existing            record;
  v_curriculum_unit_id  uuid;
  v_unit_code           text;
  v_unit_title_snapshot text;
  v_unit_mapping_status text;
  v_note_id             uuid;
  v_token_jsonb         jsonb;
  v_token               uuid;
  v_reused              boolean := false;
  v_constraint          text;
begin
  -- 1. 입력 정규화
  v_student_code := nullif(btrim(coalesce(p_student_code, '')), '');
  if v_student_code is null or p_assignment_id is null or p_attempt_id is null then
    return jsonb_build_object('ok', false, 'error', 'invalid_input');
  end if;

  -- 2. assignment + student_code 매칭으로 학생 식별
  select a.id, a.student_id, a.test_set_id, s.student_code
    into v_assignment
    from auto_grading.assignments a
    join auto_grading.students s on s.id = a.student_id
   where a.id = p_assignment_id
     and s.student_code = v_student_code
     and s.is_active = true;

  if not found then
    -- assignment 가 없거나 student_code 가 일치하지 않거나 비활성 학생
    return jsonb_build_object('ok', false, 'error', 'assignment_not_owned');
  end if;

  v_student_id  := v_assignment.student_id;
  v_test_set_id := v_assignment.test_set_id;

  -- 3. attempt 검증 (소유 + 2차 채점 완료)
  select at.id, at.student_id, at.assignment_id, at.test_set_id, at.status,
         at.round2_submitted_at
    into v_attempt
    from auto_grading.attempts at
   where at.id = p_attempt_id;

  if not found then
    return jsonb_build_object('ok', false, 'error', 'attempt_not_found');
  end if;

  if v_attempt.student_id    <> v_student_id
  or v_attempt.assignment_id is distinct from p_assignment_id then
    return jsonb_build_object('ok', false, 'error', 'attempt_not_owned');
  end if;

  -- 2차 채점 완료 상태만 필수 조건. 오답 0(만점)이어도 허용 —
  -- 학생이 헷갈렸거나 다시 보고 싶은 문제를 스스로 오답노트에 남길 수 있도록.
  if v_attempt.status not in ('completed', 'needs_review')
     or v_attempt.round2_submitted_at is null then
    return jsonb_build_object('ok', false,
                              'error',  'attempt_not_eligible',
                              'status', v_attempt.status);
  end if;

  -- attempt 와 assignment 의 test_set 일치 확인 (v_test_set_id = assignment.test_set_id)
  if v_attempt.test_set_id is distinct from v_test_set_id then
    return jsonb_build_object('ok', false, 'error', 'attempt_assignment_mismatch');
  end if;

  -- 4. 2차 오답 수 직접 계산 (wrong_count_snapshot 용. submit_round2_v2 와 동일 로직)
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
      v_note_id := v_existing.id;
      v_reused  := true;
    else
      -- 이미 제출된 노트가 있으면 작성 페이지로 보내지 않고 차단
      return jsonb_build_object('ok', false,
                                'error',   'already_submitted',
                                'note_id', v_existing.id);
    end if;
  else
    -- 6. 단원 매핑
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

    -- curriculum_units 유니크 키는 (grade_level, curriculum_version, subject,
    -- unit_code) 4개 조합. unit_code 단독 조회는 비결정적이므로 4개 모두 매칭.
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
  end if;

  -- 8. 학생 active token 가져오거나 신규 발급
  v_token_jsonb := auto_grading.get_or_create_student_public_token(v_student_id);
  if v_token_jsonb is null or (v_token_jsonb ->> 'ok')::boolean is not true then
    return jsonb_build_object('ok', false, 'error', 'token_issue_failed');
  end if;
  v_token := (v_token_jsonb ->> 'token')::uuid;

  return jsonb_build_object(
    'ok',                   true,
    'note_id',              v_note_id,
    'token',                v_token,
    'reused',               v_reused,
    'wrong_count_snapshot', v_wrong_count
  );

exception
  when unique_violation then
    -- mistake_notes_attempt_active_uidx 경합(동시 호출)만 멱등 처리.
    -- 다른 unique 제약 위반은 데이터 문제를 숨기지 않도록 그대로 전파.
    get stacked diagnostics v_constraint = constraint_name;
    if v_constraint is distinct from 'mistake_notes_attempt_active_uidx' then
      raise;
    end if;
    -- 한 번 더 active note + token 조회해서 멱등 응답.
    select id, status into v_existing
      from auto_grading.mistake_notes
     where attempt_id = p_attempt_id
       and status in ('draft','submitted')
     limit 1;
    if found and v_existing.status = 'draft' then
      v_token_jsonb := auto_grading.get_or_create_student_public_token(v_student_id);
      return jsonb_build_object(
        'ok',      true,
        'note_id', v_existing.id,
        'token',   (v_token_jsonb ->> 'token')::uuid,
        'reused',  true
      );
    end if;
    return jsonb_build_object('ok', false, 'error', 'conflict');
end;
$$;

-- ⚠️ 운영 안전 기본값: anon/authenticated 에 grant 하지 않는다.
--    auto_grading 스키마는 default privileges 로 EXECUTE 가 자동 부여될 수
--    있으므로 PUBLIC·anon·authenticated 를 명시적으로 revoke 한다.
--    로컬/테스트에서 호출하려면 db/mistake_notes_omr_bridge_grant_dev.sql 실행.
revoke all on function auto_grading.create_mistake_note_via_omr_bridge(uuid, text, uuid)
  from public, anon, authenticated;

comment on function auto_grading.create_mistake_note_via_omr_bridge(uuid, text, uuid)
  is '⚠️ 운영 배포 금지(로컬 전용). OMR(assignment_id+student_code) 인증 환경에서 mistake_note 를 생성하고 student_public_links 범용 토큰을 반환하는 과도기 브리지. anon grant 는 mistake_notes_omr_bridge_grant_dev.sql 에서만. 운영 전 note-scoped token 으로 대체 필수.';


-- ── PostgREST 스키마 캐시 reload ─────────────────────────────────
notify pgrst, 'reload schema';


-- ── 검증 시나리오 ────────────────────────────────────────────────
-- 1) 잘못된 student_code → error: assignment_not_owned
-- 2) 다른 학생의 assignment + 자기 student_code → error: assignment_not_owned
-- 3) 2차 미완료 attempt (round2_submitted_at null) → error: attempt_not_eligible
-- 4) 2차 채점 완료 attempt (오답 0 만점 포함) → {ok:true, note_id, token, reused:false}
-- 5) 같은 attempt 로 재호출 → {ok:true, note_id, token, reused:true}
-- 6) submitted 노트 존재 시 호출 → error: already_submitted

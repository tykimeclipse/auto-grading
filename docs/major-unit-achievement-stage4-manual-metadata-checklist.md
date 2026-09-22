# 대단원별 누적 성취도 4단계 수동시험 단원 필수화 체크리스트

## 구현 대상

1. `db/course_enrollment_achievement_stage7_part4_manual_test_metadata.sql`
2. `db/audit_course_enrollment_achievement_stage7_part4_manual_test_metadata.sql`
3. `db/manual_assessment.sql`의 정본 함수 동기화
4. `frontend/teacher-assignment-management.html`

신규 수동시험은 활성 `curriculum_units`의 교육과정·학년·과목·단원을
반드시 선택한 뒤 발행한다. 기존 미분류 수동시험은 변경하거나 임의 분류하지
않고 계속 `단원 미지정` 이력으로 보존한다.

## 선행 조건

- 3단계 화면 회귀검증을 통과해야 한다.
- `db/audit_course_enrollment_achievement_stage7_major_unit_preflight.sql`의
  `schema_contract`가 계속 `0`이어야 한다.
- `curriculum_units`에 현재 사용하는 활성 교육과정 단원이 등록되어 있어야 한다.
- 구현 SQL은 기존 4-인자 `teacher_create_manual_test_set`을 삭제하고 6-인자
  함수로 재생성한다. `_v2` 함수나 구버전 오버로드를 남기지 않는다.

## 배포 순서

1. 함수 소유자 권한의 Supabase SQL Editor에서
   `course_enrollment_achievement_stage7_part4_manual_test_metadata.sql`을 실행한다.
2. 같은 세션에서
   `audit_course_enrollment_achievement_stage7_part4_manual_test_metadata.sql`을
   실행한다.
3. 감사 결과 다섯 섹션의 `blocking_issue_count`가 모두 `0`인지 확인한다.
4. `frontend/teacher-assignment-management.html`을 배포한다.
5. `Ctrl+F5` 후 수동시험 모달과 기존 발행상황 관리 기능을 회귀검증한다.

> `db/manual_assessment.sql`은 신규 환경과 재실행을 위한 정본이며 part 4와 같은
> 함수 정의를 담는다. 운영 DB에는 위 part 4 파일만 실행한다.

## 데이터·RPC 계약

- 함수 시그니처는
  `teacher_create_manual_test_set(text, integer, text, text, text, text)` 하나뿐이다.
- 인자 순서는 제목, 총 문항수, 학년, 교육과정, 과목, 단원코드다.
- 네 메타데이터 값은 모두 필수이며 단원코드는 3자리 숫자다.
- 선택한 4-튜플이 활성 `curriculum_units` 행과 정확히 일치해야 한다.
- 신규 `test_sets`에는 `source_type='manual'`과 네 메타데이터를 함께 저장한다.
- 공개·익명·서비스 역할 실행 권한은 없고 `authenticated`만 호출할 수 있다.
  실제 관리자 여부는 기존 `assert_admin()`이 최종 확인한다.
- 교사 화면의 단원 목록 직접 조회를 위해 `authenticated` 역할에
  `auto_grading` 스키마 `USAGE`와 `curriculum_units` 테이블 `SELECT`가 있어야
  한다. RLS가 활성화됐다면 `PUBLIC` 또는 `authenticated` 대상 permissive
  SELECT 정책도 존재해야 한다.
- 활성 단원 목록은 `count='exact'`와 범위 조회로 페이지네이션하여 PostgREST
  최대 행 수보다 데이터가 많아도 전량을 불러온다. 일부만 반환되면 발행을
  허용하지 않고 오류로 처리한다. 비정상 서버가 범위 요청을 계속 무시하는
  상황을 대비해 최대 100페이지에서 중단한다.
- 단원 조회 상태는 `idle / loading / ready / empty / error`로 구분한다. 최초
  조회가 실패하면 수동시험 모달을 다시 열 때 재시도하며, 실제 활성 단원 0건과
  네트워크·권한 오류를 서로 다른 문구로 안내한다.
- 기존의 네 값이 모두 `null`인 수동시험은 허용한다. 일부 값만 존재하거나
  단원 참조가 끊긴 수동시험은 감사 차단 대상이다.

## 화면 확인

1. 수동시험 모달을 열면 강좌 선택이 시험 발행 영역에 표시된다.
2. 교육과정 선택 전에는 학년·과목·단원 선택이 비활성 상태다.
3. 교육과정 → 학년 → 과목 순서로 선택하면 활성 단원만 표시된다.
   감사의 `active_unit_count`가 1,000건 이상이어도 마지막 단원까지 조회되는지
   확인한다.
4. 필수값 하나라도 비우면 시험이 생성되지 않고 한국어 안내가 표시된다.
5. 시험 발행 후 제목·문항수·강좌·교육과정 메타데이터가 잠겨 점수 입력 중
   시험 기준이 바뀌지 않는다.
6. 발행된 시험으로 학생을 불러오고 1차/최종 점수를 기존대로 저장할 수 있다.
7. 저장 후 학생·교사 단원별 성취도에서 선택한 대단원에 수동 평가가 포함되고
   2차 반영은 `-`로 표시된다.
8. 기존 미분류 수동시험은 계속 `단원 미지정` 묶음에 남는다.
9. 활성 단원 조회가 실패해도 기존 발행상황 조회·점수 수정·강좌 재귀속 기능은
   계속 동작하고, 수동시험 발행만 차단된다. 모달을 닫았다 다시 열면 단원
   조회를 재시도하고, 성공하면 새로고침 없이 발행 버튼이 활성화된다.

## 감사 결과 5개 섹션

1. `function_contract`
2. `schema_contract`
3. `manual_metadata_integrity`
4. `curriculum_units_api_access`
5. `active_curriculum_options`

`curriculum_units_api_access`가 차단되면 화면을 먼저 배포하지 않는다. 실제 운영
DB의 스키마·테이블 권한과 RLS 정책을 확인해 의도한 관리자 읽기 경로를 복구한
후 감사를 다시 실행한다. 정적 정책 검사는 정책 표현식이 실제 JWT 조건에서
행을 반환하는지까지 보증하지 않으므로, 배포 후 교사 계정 브라우저 확인도
반드시 수행한다.

## 다음 단계

- 과거 `단원 미지정` 시험을 검색해 `teacher_update_test_set_metadata`로 소급
  분류하는 전용 화면을 추가한다.
- 소급 분류 전후에 대단원별 성취도 전체 분자·분모 합계가 변하지 않고 단원
  파티션만 이동하는지 별도 감사로 검증한다.

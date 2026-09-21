# 대단원별 누적 성취도 1단계 사전점검

대상 SQL: `db/audit_course_enrollment_achievement_stage7_major_unit_preflight.sql`

이 단계는 운영 데이터를 변경하지 않는다. Supabase SQL Editor에서 감사 SQL 전체를 실행하고 반환되는 6개 섹션을 확인한다.

## 집계 계약

- 집계 대상은 `completed`, `needs_review` attempt다.
- 대단원 키는 `grade_level + curriculum_version + subject + major_unit_code`다.
- 문항 수와 모든 성취도 분모는 응시 당시 스냅샷인 `attempts.total_items`를 사용한다.
- `2차 반영`은 수동 시험을 제외한 `final_correct_count / total_items`다.
- 최종 성취도는 기존 `_student_achievement_stats_core` 규칙을 유지한다.
- 비활성화된 `curriculum_units`도 과거 성취도에서 제외하지 않는다.

## 다음 단계 진행 조건

다음 섹션의 `blocking_issue_count`가 모두 `0`이어야 한다.

1. `schema_contract`
2. `achievement_metadata_coverage`
3. `major_unit_name_integrity`
4. `assignment_attempt_invariant`
5. `unit_partition_integrity`

`inactive_curriculum_unit_history`는 정보성 결과이므로 건수가 있어도 차단하지 않는다.

## 결과 해석

### schema_contract

운영 DB에 필요한 CHECK, UNIQUE, FK 제약이 설치되고 검증됐는지 확인한다. 저장소에 SQL 파일이 있어도 운영 DB에 적용되지 않았으면 차단된다.

### achievement_metadata_coverage

- `classified_attempt_count`: 단원별 집계에 바로 포함할 평가 수
- `unassigned_attempt_count`: 메타데이터 네 항목이 모두 없는 평가 수
- `invalid_metadata_attempt_count`: 일부만 입력됐거나 대응 교육과정 단원이 없는 비정상 평가 수

`unassigned_attempt_count`는 허용한다. 이 목록은 기존 수동 시험 소급 분류 화면의 작업 목록으로 사용한다. `invalid_metadata_attempt_count`는 반드시 `0`이어야 한다.

### major_unit_name_integrity

같은 학년·교육과정·과목·대단원 코드 안에서 대단원명이 서로 다른지 검사한다. `conflict_group_count`는 반드시 `0`이어야 한다.

`missing_major_row_count`는 정보성이다. 대단원 전용 행이 없어도 시험지가 참조한 교육과정 행의 `major_unit_name`으로 폴백할 수 있다.

### assignment_attempt_invariant

현재 집계는 assignment당 attempt 한 건이라는 운영 불변식을 사용한다. `multiple_attempt_assignment_count`는 반드시 `0`이어야 한다.

`attempt_without_assignment_count`는 레거시 현황을 확인하기 위한 정보성 값이며, 단원 집계 자체는 attempt의 학생·시험·강좌 스냅샷으로 처리할 수 있다.

### unit_partition_integrity

학생별로 다음 여덟 개 분자·분모가 일치하는지 비교한다.

- 1차 정답 수 / 문항 수
- 2차 반영 정답 수 / 문항 수
- 최종 정답 수 / 문항 수
- 교사 확정 정답 수 / 문항 수

`대단원별 합계 + 단원 미지정 합계 = 전체 누적`이어야 하며 `mismatch_student_count`는 반드시 `0`이어야 한다.

### inactive_curriculum_unit_history

비활성 교육과정 단원을 참조하는 과거 평가 수를 보여준다. 이 데이터는 향후 단원별 성취도에서도 계속 포함해야 한다.

## 실행 후 공유할 값

각 행의 다음 세 컬럼을 그대로 공유한다.

```text
section
blocking_issue_count
details
```

# 대단원별 누적 성취도 2단계 RPC 배포 체크리스트

## 대상 파일

1. `db/course_enrollment_achievement_stage7_part2_major_unit_achievement.sql`
2. `db/audit_course_enrollment_achievement_stage7_part2_major_unit_achievement.sql`

## 배포 순서

선행 조건: `db/audit_course_enrollment_achievement_stage7_major_unit_preflight.sql`의 다음 다섯 섹션이 모두 통과해야 한다.

1. `schema_contract`
2. `achievement_metadata_coverage`
3. `major_unit_name_integrity`
4. `assignment_attempt_invariant`
5. `unit_partition_integrity`

특히 제약조건 검증이 통과해야 부분 메타데이터나 교육과정에 존재하지 않는 단원이 별도 묶음으로 잘못 집계되지 않는다.

구현 SQL과 감사 SQL은 함수 소유자 권한으로 접속한 Supabase SQL Editor에서 실행한다. 감사 SQL이 비공개 코어 함수를 직접 호출하므로 `anon`, `authenticated`, `service_role` 세션에서는 실행할 수 없다.

1. Supabase SQL Editor에서 구현 SQL 전체를 실행한다.
2. 이어서 감사 SQL 전체를 실행한다.
3. 감사 결과 5개 행의 `blocking_issue_count`가 모두 `0`인지 확인한다.
4. 검증 전에는 학생·교사 화면을 새 RPC로 연결하지 않는다.

## RPC

### 관리자 직접 조회

```sql
select auto_grading.get_student_major_unit_achievement_by_code(
  '학생코드',
  'all',
  null
);
```

강좌 범위 조회:

```sql
select auto_grading.get_student_major_unit_achievement_by_code(
  '학생코드',
  'course',
  '강좌 UUID'::uuid
);
```

### 공개 토큰 조회

```sql
select auto_grading.get_student_major_unit_achievement_by_token(
  '공개 토큰 UUID'::uuid,
  'all',
  null
);
```

## 반환 계약

- `summary`: 선택한 강좌 범위 전체의 분자·분모와 가중 성취도
- `units`: 대단원별 요약과 테스트 상세
- `units[].test_count`: 시험지 수가 아닌 완료 평가 attempt 수
- `units[].scores.round1`: 1차 성취도
- `units[].scores.round2_reflected`: 수동 시험을 제외한 2차 반영 성취도
- `units[].scores.final`: 기존 누적 성취도와 같은 최종 점수 규칙
- `units[].retention.final_confirmed`: 최종 확정 평가만 사용한 반복 평가 변화
- `units[].retention.round1`: 모든 유효 평가의 1차 점수 변화
- 두 반복 평가 지표의 `days_since_latest`: 한국 날짜 기준 마지막 평가 후 경과일
- `units[].tests`: 응시 당시 `attempts.total_items`를 분모로 사용한 상세 기록
- `units[].tests`는 단원별 최근 200건을 선택한 뒤 오래된 평가부터 표시
- `test_count`: 단원에 포함되는 전체 평가 건수
- `returned_test_count`: `tests` 배열로 실제 반환된 건수
- `tests_truncated`: 200건 상한으로 일부 상세가 생략됐는지 여부

상세 배열이 잘려도 단원 요약, 가중 성취도, 반복 평가 변화는 전체 평가를 기준으로 계산한다.

메타데이터가 없는 기존 수동 시험은 `unit.is_unassigned=true`, `major_unit_name="단원 미지정"`인 하나의 묶음으로 반환된다.

## 화면 구현 시 주의

- 기존 시험 기록은 열린 미제출 발행 건을 포함할 수 있지만 단원별 집계는 완료·검토 필요 attempt만 센다. 단원 화면의 건수 라벨은 `시험 기록`이 아니라 `완료 평가`로 표시한다.
- 여러 학년이 함께 보이는 범위에서는 문자열 정렬을 그대로 쓰지 않고 `M1 → M2 → M3 → H1 → H2 → H3` 순서를 화면에서 적용한다.
- `final_confirmed_test_count`는 최종 분모가 존재하는 평가 수이고, `retention.final_confirmed.test_count`는 백분율 계산까지 가능한 평가 수다. `total_items=0`인 병리적 데이터에서는 두 값이 다를 수 있다.

## 감사 결과

다음 5개 섹션이 모두 `0`이어야 한다.

1. `function_contract`
2. `canonical_basis_equivalence`
3. `scope_summary_integrity`
4. `unit_detail_integrity`
5. `wrapper_equivalence`

감사 SQL은 활성 학생의 전체·강좌별·강좌 미지정 범위를 검사한다. 단원 요약 수치는 기존 `_student_achievement_stats_core`의 여덟 개 분자·분모와 비교한다. 상세이 잘리지 않은 단원은 요약과 테스트 상세 합계도 비교하고, 잘린 단원은 반환 건수·상한·시간순 정렬 계약을 검사한다.

공개 토큰은 유효 링크의 결과 동등성뿐 아니라 존재하지 않는 토큰, 비활성 링크, 만료 링크, 비활성 학생 링크가 `null`을 반환하는지도 검사한다.

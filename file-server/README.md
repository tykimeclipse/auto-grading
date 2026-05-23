# auto-grading file-server

학생 오답노트의 이미지/PDF 원본을 저장하는 자체 파일서버.
Supabase 에는 메타데이터만 저장하고, 실제 파일은 이 서버가 관리한다.

기획 §7, §9 의 자체 파일서버 책임을 구현한다.

## 요구사항

- Node.js 20 LTS 이상
- `sharp` 가 native binary 를 사용하므로 macOS / Linux / Windows 빌드 환경 필요
  (대부분 npm install 시 prebuilt 자동 설치됨)

## 설치 / 실행

```bash
cd file-server
npm install
cp .env.example .env
# .env 에 SUPABASE_URL, SUPABASE_ANON_KEY 채우기

npm run dev     # 개발 (watch mode)
npm start       # 운영
```

## DB 의존성

이 서버는 Supabase RPC `auto_grading.get_student_id_by_token` 을 anon 키로 호출한다.
서비스 시작 전 다음 SQL 을 Supabase 에 적용:

```
file-server/db/file_server_rpcs.sql
```

선행 의존성: `db/mistake_notes.sql`, `db/mistake_notes_rpc.sql` 가 이미 적용되어 있어야 한다.

## API

### `GET /health`
서버 동작 확인.

```
200 { "ok": true, "ts": "2026-..." }
```

### `POST /upload`
이미지 1 장 업로드. EXIF 자동 회전 → WebP 변환 → 썸네일 생성.

**요청**
- `Authorization: Bearer <public_token>` (UUID v4)
- `X-Note-Id: <note_id>` (UUID v4)
- `Content-Type: multipart/form-data`
- form field `file`: 이미지 1 개 (`image/jpeg | image/png | image/webp`, 10 MB 이하 기본값)

**응답 (200)**
```json
{
  "ok": true,
  "file_backend": "local_file_server",
  "file_key":      "students/<student_id>/<note_id>/images/<uuid>.webp",
  "thumbnail_key": "students/<student_id>/<note_id>/thumbs/<uuid>.webp",
  "mime_type":     "image/webp",
  "file_size":     123456,
  "width":         1600,
  "height":        1200
}
```

받은 `file_key` 등을 Supabase RPC `attach_mistake_image_by_token` 에 전달해서 DB 메타 등록.

**에러**
| 상태 | error                        | 의미 |
|------|------------------------------|------|
| 401  | `invalid_token_format`       | Authorization 헤더 형식 잘못 |
| 401  | `unauthorized`               | token 으로 student 못 찾음 |
| 400  | `invalid_note_id`            | X-Note-Id 누락/형식 오류 |
| 400  | `file_required`              | multipart file 없음 |
| 400  | `image_processing_failed`    | sharp 처리 실패 (손상 이미지 등) |
| 413  | `file_too_large`             | 파일 크기 초과 |
| 415  | `unsupported_mime`           | jpeg/png/webp 외 mime |
| 500  | `storage_failed`             | 디스크 쓰기 실패 |

### `GET /file/*`
이미지/썸네일/PDF 서빙. 통합 엔드포인트.

**요청**
- URL path 에 `file_key` 그대로 (예: `/file/students/<student_id>/<note_id>/images/<uuid>.webp`)
- 인증: `?token=<uuid>` query string (HTML `<img src>` 용) 또는 `Authorization: Bearer <uuid>` 헤더
- file_key 에 박힌 student_id 와 token 의 student_id 가 일치해야 200

**응답 (200)** — 파일 바이너리. `Content-Type` 은 확장자 기반 (webp / jpg / png / pdf).
`Cache-Control: private, max-age=600` (학생 token 기반이므로 브라우저 사적 캐시만).

**에러**
| 상태 | error              | 의미 |
|------|--------------------|------|
| 400  | `invalid_file_key` | path 형식 위반 |
| 401  | `token_required` / `unauthorized` | 토큰 없음/무효 |
| 403  | `forbidden`        | 다른 학생 파일 접근 |
| 404  | `not_found`        | 디스크에 파일 없음 |
| 502  | `upstream_error`   | Supabase 호출 실패 |

### `POST /delete`
학생이 잘못 찍은 사진을 삭제할 때 호출. 보통 `delete_mistake_image_by_token` Supabase RPC 응답의 `file_key`/`thumbnail_key` 를 그대로 전달.

**요청**
- `Authorization: Bearer <public_token>`
- `Content-Type: application/json`
- body:
  ```json
  {
    "file_key":      "students/<sid>/<nid>/images/<uuid>.webp",
    "thumbnail_key": "students/<sid>/<nid>/thumbs/<uuid>.webp"
  }
  ```
  `thumbnail_key` 는 optional.

**응답 (200)**
```json
{ "ok": true, "file_deleted": true, "thumbnail_deleted": true }
```
`_deleted=false` 는 디스크에 이미 없었음을 의미 (정상으로 처리).

**에러**
| 상태 | error                              |
|------|------------------------------------|
| 400  | `invalid_body` / `invalid_file_key` / `invalid_thumbnail_key` |
| 401  | `invalid_token_format` / `unauthorized` |
| 403  | `forbidden_file` / `forbidden_thumbnail` |
| 500  | `delete_failed`                    |
| 502  | `upstream_error`                   |

### `POST /pdf`
오답노트를 A4 PDF 로 생성. 학생/교사 공용 (Q28: token 으로만 인증, 구분 없음).

**요청**
- `Authorization: Bearer <view_token>` — 학생 public token 또는 교사 모달의 student_view_token
- `Content-Type: application/json`
- body: `{ "note_id": "<uuid>" }`

권한 검증은 `get_mistake_note_detail_by_token` 이 담당 — note 가 token 학생 소유가
아니면 RPC 가 거부하므로 다른 학생 노트 PDF 는 만들 수 없다.

**응답 (200)** — `application/pdf` 바이너리, `Content-Disposition: attachment`.
- 레이아웃: A4, 페이지당 2 장 (판독성 우선). 헤더에 시험명/학생명/단원/날짜, 하단에 페이지번호.
- `submitted` / `archived` 상태만 생성 가능. `draft` 는 거부.

**에러**
| 상태 | error                  | 의미 |
|------|------------------------|------|
| 400  | `invalid_note_id` / `no_images` | note_id 형식 오류 / 이미지 없음 |
| 401  | `invalid_token_format` | 토큰 형식 오류 |
| 403  | `forbidden`            | 토큰 무효 또는 다른 학생 노트 |
| 409  | `note_not_submitted`   | draft 상태 (제출 전) |
| 500  | `pdf_failed`           | PDF 생성 실패 |
| 502  | `upstream_error`       | Supabase 호출 실패 |

## PDF 한글 폰트

PDF 헤더의 한글(학생명/시험명/단원명)을 표시하려면 한글 TTF 폰트가 필요하다.

```
file-server/assets/fonts/NotoSansKR-Regular.ttf
```

Noto Sans KR (SIL OFL 라이선스) 을 위 경로에 두면 한글 헤더가 포함된다.
폰트 파일이 없으면 PDF 는 정상 생성되되 헤더가 날짜 등 ASCII 로만 표시된다.
경로는 `.env` 의 `FONT_DIR` 로 변경 가능.

## 디렉터리 정책

```
${DATA_DIR}/
  students/
    {student_id}/
      {note_id}/
        images/
          {uuid}.webp
        thumbs/
          {uuid}.webp
        exports/        # PDF 생성 Phase 에서 사용
          note.pdf
```

`file_key` 는 `${DATA_DIR}` 을 **제외**한 논리 경로만 DB 에 저장한다.

## 로컬 테스트

```bash
# health
curl http://localhost:8787/health

# upload (예시 — 실제 token 은 student_public_links 에서 발급된 것 사용)
curl -X POST http://localhost:8787/upload \
  -H "Authorization: Bearer 11111111-1111-1111-1111-111111111111" \
  -H "X-Note-Id: 22222222-2222-2222-2222-222222222222" \
  -F "file=@./sample.jpg"
```

## 학원 PC 배포 (Docker Compose)

학원 우분투 PC에 컨테이너로 띄우는 절차. **LAN 내부 검증용** 단계 — 외부
인터넷 노출(Cloudflare Tunnel)은 보안 게이트 통과 후에만 추가한다.

### 사전 준비
- Docker Engine + Compose v2 (`docker compose version` 으로 확인)
- 학원 PC 의 LAN IP 확인 (`hostname -I` 또는 `ip a`)
- (선택) `assets/fonts/NotoSansKR-Regular.ttf` — PDF 한글 헤더용

### 절차
```bash
# 1) 코드 가져오기 (방법은 자유 — git clone 또는 scp 등)
cd file-server

# 2) 환경변수 — SUPABASE_ANON_KEY 채우기
cp .env.example .env
nano .env

# 3) 빌드 + 백그라운드 기동
docker compose up -d --build

# 4) 로그 확인
docker compose logs -f file-server

# 5) 다른 PC(개발 PC 등)에서 health 체크
curl http://<학원-PC-LAN-IP>:8787/health
```

### 관리 명령
| 동작 | 명령 |
|------|------|
| 중지 | `docker compose down` |
| 재시작 | `docker compose restart file-server` |
| 코드 변경 후 재배포 | `docker compose up -d --build` |
| orphan 정리 1회 (dry-run) | `docker compose exec file-server node scripts/cleanup-orphans.js` |

`data/` 와 `assets/fonts/` 는 host 디렉터리에 마운트되므로, 컨테이너를
재생성해도 업로드된 이미지/PDF 와 폰트는 그대로 유지된다.

### 프론트 (개발 PC) 설정
프론트는 개발 PC 의 `python -m http.server`(또는 임의 정적 서버)에서
계속 서빙하고, 파일서버 주소만 학원 PC 로 가리키도록 바꾼다:

`frontend/app-config.js`:
```js
window.APP_CONFIG = {
  FILE_SERVER_URL: 'http://<학원-PC-LAN-IP>:8787',
  ENABLE_OMR_MISTAKE_NOTE_BRIDGE: true,
};
```

학원 PC `.env` 의 `ALLOWED_ORIGINS` 에 개발 PC 의 프론트 origin
(`http://localhost:3000` 등) 이 들어 있어야 CORS 통과한다 (`.env.example`
기본값에 이미 포함).

### orphan 정리 cron (선택, 운영용)
컨테이너 안에서 cron 을 돌리지 말고 host crontab 에서 `docker compose exec`
로 호출한다. 호스트 crontab 예시 (`crontab -e`):
```
0 3 * * * cd /path/to/file-server && docker compose exec -T file-server node scripts/cleanup-orphans.js --apply >> /var/log/cleanup-orphans.log 2>&1
```

## orphan 파일 정리 (배치)

DB(`mistake_images`)에 등록되지 않은 파일을 7일 grace 후 삭제하는 배치 스크립트.

```bash
cp .env.cleanup.example .env.cleanup
# .env.cleanup 에 SUPABASE_SERVICE_ROLE_KEY 채우기

npm run cleanup:dry      # dry-run — 삭제 대상만 로그 출력
npm run cleanup:apply    # 실제 삭제
```

- ⚠️ `cleanup:*` 스크립트만 `service_role` 키를 사용한다. `.env.cleanup` 은
  git 에 올라가지 않으며(`.gitignore` 등록), 프론트/파일서버 본체와 분리한다.
- 판정: `images/` → `file_key`, `thumbs/` → `thumbnail_key` 미등록 시 후보.
  `exports/`(PDF) 는 재생성 가능하므로 DB 대조 없이 mtime 만 본다.
- **운영 권장**: 먼저 며칠 `cleanup:dry` 로그를 검증한 뒤 `cleanup:apply` 전환.
- **Windows 작업 스케줄러**: 하루 1회 `npm run cleanup:apply` (작업 폴더 = `file-server/`).

## 향후 작업

| 항목  | 내용 |
|-------|------|
| 운영  | Cloudflare Tunnel 연동, 고정 도메인, 자동 시작 등록 |
| 보안  | 파일서버 admin JWT 인증 (교사 이미지 조회의 학생 view token 의존 제거) |
| PDF   | 레이아웃 옵션 다양화 (페이지당 1/2/4 장) |

## 보안 메모

- service_role 키는 이 서버에서 절대 사용하지 않는다. anon 키만 사용.
- `file_key` 는 정규식으로 path traversal 검증. `..` 거부.
- Authorization 헤더의 token 은 로그에 노출되지 않도록 Fastify 기본 설정 유지.
- 운영 시 HOST=0.0.0.0 으로 열되, 외부 노출은 Cloudflare Tunnel 한 경로만.

// ============================================================
// cleanup-orphans.js
//
// DB(auto_grading.mistake_images)에 등록되지 않은 파일서버 파일을
// 7일 grace 경과 후 삭제하는 관리자 배치 스크립트.
// Windows 작업 스케줄러에서 하루 1회 실행한다.
//
// ⚠️ 이 스크립트만 Supabase service_role key 를 사용한다.
//    환경변수는 .env.cleanup (없으면 .env) 에서 로드하며,
//    프론트엔드에는 절대 노출하지 않는다.
//
// 사용법:
//   node scripts/cleanup-orphans.js            # dry-run (기본 — 삭제 안 함)
//   node scripts/cleanup-orphans.js --apply    # 실제 삭제
//
// 판정 규칙:
//   images/  파일 → mistake_images.file_key      에 없으면 orphan 후보
//   thumbs/  파일 → mistake_images.thumbnail_key 에 없으면 orphan 후보
//   exports/ 파일 → DB 추적 안 함(PDF 는 재생성 가능) → 항상 후보
//   위 후보 중 mtime 이 7일 이상 경과한 것만 삭제 대상.
// ============================================================

import dotenv from 'dotenv';
import fs from 'node:fs';
import fsp from 'node:fs/promises';
import path from 'node:path';
import { createClient } from '@supabase/supabase-js';

// .env.cleanup 우선, 없으면 .env
const ENV_FILE = fs.existsSync('.env.cleanup') ? '.env.cleanup' : '.env';
dotenv.config({ path: ENV_FILE });

const APPLY    = process.argv.includes('--apply');
const GRACE_MS = 7 * 24 * 60 * 60 * 1000;
const DB_PAGE  = 1000;

function log(...args) { console.log('[cleanup]', ...args); }

function requireEnv(name) {
  const v = process.env[name];
  if (!v || !v.trim()) {
    console.error(`[cleanup] Missing env: ${name} (check ${ENV_FILE})`);
    process.exit(1);
  }
  return v.trim();
}

const SUPABASE_URL = requireEnv('SUPABASE_URL');
const SERVICE_KEY  = requireEnv('SUPABASE_SERVICE_ROLE_KEY');
const DATA_DIR     = (process.env.DATA_DIR || './data/mistake-notes').trim();

const supabase = createClient(SUPABASE_URL, SERVICE_KEY, {
  auth: { persistSession: false, autoRefreshToken: false },
});

// DB 의 모든 file_key / thumbnail_key 수집
async function loadDbKeys() {
  const fileKeys  = new Set();
  const thumbKeys = new Set();
  let from = 0;
  for (;;) {
    const { data, error } = await supabase
      .schema('auto_grading')
      .from('mistake_images')
      .select('file_key, thumbnail_key')
      .range(from, from + DB_PAGE - 1);
    if (error) {
      console.error('[cleanup] DB query failed:', error.message);
      process.exit(1);
    }
    if (!data || !data.length) break;
    for (const row of data) {
      if (row.file_key)      fileKeys.add(row.file_key);
      if (row.thumbnail_key) thumbKeys.add(row.thumbnail_key);
    }
    if (data.length < DB_PAGE) break;
    from += DB_PAGE;
  }
  return { fileKeys, thumbKeys };
}

// DATA_DIR/students/{sid}/{nid}/{images|thumbs|exports}/* 순회
async function listDiskFiles() {
  const out = [];
  const root = path.resolve(DATA_DIR);
  const studentsDir = path.join(root, 'students');

  let students;
  try {
    students = await fsp.readdir(studentsDir);
  } catch {
    log('students dir not found, nothing to scan:', studentsDir);
    return out;
  }

  for (const sid of students) {
    const sidDir = path.join(studentsDir, sid);
    let notes;
    try { notes = await fsp.readdir(sidDir); } catch { continue; }
    for (const nid of notes) {
      for (const kind of ['images', 'thumbs', 'exports']) {
        const kindDir = path.join(sidDir, nid, kind);
        let files;
        try { files = await fsp.readdir(kindDir); } catch { continue; }
        for (const f of files) {
          const abs = path.join(kindDir, f);
          let st;
          try { st = await fsp.stat(abs); } catch { continue; }
          if (!st.isFile()) continue;
          out.push({
            fileKey: `students/${sid}/${nid}/${kind}/${f}`,
            absPath: abs,
            kind,
            mtimeMs: st.mtimeMs,
          });
        }
      }
    }
  }
  return out;
}

async function main() {
  log(APPLY
    ? 'MODE: APPLY (실제 삭제)'
    : 'MODE: DRY-RUN (삭제 안 함 — 실제 삭제는 --apply)');
  log('env file:', ENV_FILE, '| DATA_DIR:', DATA_DIR);

  const { fileKeys, thumbKeys } = await loadDbKeys();
  log(`DB keys: file=${fileKeys.size}, thumbnail=${thumbKeys.size}`);

  const diskFiles = await listDiskFiles();
  log(`disk files scanned: ${diskFiles.length}`);

  const now = Date.now();
  const orphans = [];
  for (const f of diskFiles) {
    let inDb;
    if (f.kind === 'images')      inDb = fileKeys.has(f.fileKey);
    else if (f.kind === 'thumbs') inDb = thumbKeys.has(f.fileKey);
    else                          inDb = false; // exports: 재생성 가능, DB 추적 안 함
    if (inDb) continue;
    if (now - f.mtimeMs < GRACE_MS) continue; // 7일 grace
    orphans.push(f);
  }

  const byKind = { images: 0, thumbs: 0, exports: 0 };
  for (const o of orphans) byKind[o.kind]++;
  log(`orphan candidates: images=${byKind.images}, thumbs=${byKind.thumbs}, exports=${byKind.exports}`);

  let deleted = 0;
  for (const o of orphans) {
    const ageDays = Math.floor((now - o.mtimeMs) / 86400000);
    if (APPLY) {
      try {
        await fsp.unlink(o.absPath);
        deleted++;
        log(`deleted (${ageDays}d) ${o.fileKey}`);
      } catch (err) {
        log(`delete FAILED ${o.fileKey}: ${err.message}`);
      }
    } else {
      log(`would delete (${ageDays}d) ${o.fileKey}`);
    }
  }

  log(APPLY
    ? `done. deleted ${deleted}/${orphans.length}.`
    : `done. ${orphans.length} candidates (dry-run — nothing deleted).`);
}

main().catch((err) => {
  console.error('[cleanup] fatal:', err);
  process.exit(1);
});

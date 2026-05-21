import fs from 'node:fs/promises';
import path from 'node:path';
import crypto from 'node:crypto';
import { config } from '../config.js';

const UUID_RE = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;

// file_key 화이트리스트: students/{uuid}/{uuid}/{images|thumbs|exports}/{uuid}.(webp|jpg|jpeg|png|pdf)
const FILE_KEY_RE =
  /^students\/[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\/[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\/(images|thumbs|exports)\/[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\.(webp|jpg|jpeg|png|pdf)$/i;

export function isValidFileKey(fileKey) {
  return typeof fileKey === 'string' && FILE_KEY_RE.test(fileKey);
}

const STUDENT_ID_FROM_KEY_RE =
  /^students\/([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})\//i;

// file_key 에 박힌 student_id 를 lowercase 로 추출. 형식 위반이면 null.
export function extractStudentIdFromFileKey(fileKey) {
  if (!isValidFileKey(fileKey)) return null;
  const m = STUDENT_ID_FROM_KEY_RE.exec(fileKey);
  return m ? m[1].toLowerCase() : null;
}

// student_id/note_id 가 검증된 상태에서만 호출.
// 반환: { imageKey, thumbKey } — 동일 uuid 를 사용해 짝 식별 용이.
export function generateImageKey(studentId, noteId) {
  if (!UUID_RE.test(studentId) || !UUID_RE.test(noteId)) {
    throw new Error('invalid_uuid');
  }
  const uuid = crypto.randomUUID();
  return {
    imageKey: `students/${studentId}/${noteId}/images/${uuid}.webp`,
    thumbKey: `students/${studentId}/${noteId}/thumbs/${uuid}.webp`,
  };
}

// 검증된 file_key 를 절대 경로로 변환. path traversal 방어.
function toAbsPath(fileKey) {
  if (!isValidFileKey(fileKey)) {
    throw new Error('invalid_file_key');
  }
  const abs = path.resolve(config.dataDir, fileKey);
  const root = path.resolve(config.dataDir);
  // resolve 후에도 경로가 root 밖이면 차단 (이론상 정규식이 막지만 이중 방어)
  if (!abs.startsWith(root + path.sep) && abs !== root) {
    throw new Error('path_traversal');
  }
  return abs;
}

export async function writeFile(fileKey, buffer) {
  const abs = toAbsPath(fileKey);
  await fs.mkdir(path.dirname(abs), { recursive: true });
  await fs.writeFile(abs, buffer);
}

export async function readFile(fileKey) {
  const abs = toAbsPath(fileKey);
  return fs.readFile(abs);
}

export async function deleteFile(fileKey) {
  const abs = toAbsPath(fileKey);
  try {
    await fs.unlink(abs);
    return true;
  } catch (err) {
    if (err.code === 'ENOENT') return false;
    throw err;
  }
}

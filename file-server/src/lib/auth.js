import { supabase } from '../supabase.js';

const UUID_RE = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;

export function isValidUuid(v) {
  return typeof v === 'string' && UUID_RE.test(v);
}

export function parseBearerToken(req) {
  const auth = req.headers['authorization'];
  if (!auth || typeof auth !== 'string') return null;
  const m = /^Bearer\s+(.+)$/i.exec(auth);
  if (!m) return null;
  const token = m[1].trim();
  if (!UUID_RE.test(token)) return null;
  return token;
}

export function readNoteIdHeader(req) {
  const v = req.headers['x-note-id'];
  if (!v || typeof v !== 'string') return null;
  const trimmed = v.trim();
  return UUID_RE.test(trimmed) ? trimmed : null;
}

// token → student_id 변환.
// 성공 시 { ok:true, studentId }, 실패 시 { ok:false, reason }.
export async function resolveStudentIdByToken(token) {
  const { data, error } = await supabase
    .schema('auto_grading')
    .rpc('get_student_id_by_token', { p_token: token });

  if (error) {
    return { ok: false, reason: 'rpc_error', detail: error.message };
  }
  // RPC 가 null 반환 = 유효하지 않은 토큰
  if (data == null) {
    return { ok: false, reason: 'invalid_token' };
  }
  const studentId = String(data);
  if (!UUID_RE.test(studentId)) {
    return { ok: false, reason: 'invalid_rpc_response' };
  }
  return { ok: true, studentId };
}

// token + note_id 권한 검증 + 노트 메타/이미지 조회.
// get_mistake_note_detail_by_token 은 note.student_id 와 token student_id 가
// 일치하지 않으면 note_not_owned 를 반환하므로, 이 호출 하나로 권한 검증이 끝난다.
export async function fetchNoteDetailByToken(token, noteId) {
  const { data, error } = await supabase
    .schema('auto_grading')
    .rpc('get_mistake_note_detail_by_token', {
      p_token:   token,
      p_note_id: noteId,
    });
  if (error) {
    return { ok: false, reason: 'rpc_error', detail: error.message };
  }
  const result = Array.isArray(data) ? data[0] : data;
  if (!result || result.ok !== true) {
    return { ok: false, reason: (result && result.error) || 'unknown' };
  }
  return {
    ok: true,
    note: result.note,
    images: Array.isArray(result.images) ? result.images : [],
  };
}

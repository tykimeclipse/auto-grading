import { parseBearerToken, resolveStudentIdByToken } from '../lib/auth.js';
import {
  isValidFileKey,
  readFile,
  extractStudentIdFromFileKey,
} from '../lib/storage.js';

const UUID_RE = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;

const MIME_BY_EXT = {
  webp: 'image/webp',
  jpg:  'image/jpeg',
  jpeg: 'image/jpeg',
  png:  'image/png',
  pdf:  'application/pdf',
};

function mimeForFileKey(fileKey) {
  const m = /\.([a-z0-9]+)$/i.exec(fileKey);
  if (!m) return 'application/octet-stream';
  return MIME_BY_EXT[m[1].toLowerCase()] || 'application/octet-stream';
}

function extractToken(req) {
  // query string ?token=... 우선 (HTML <img src> 용)
  const q = req.query?.token;
  if (typeof q === 'string' && UUID_RE.test(q.trim())) {
    return q.trim();
  }
  // 또는 Authorization: Bearer <uuid> (서버 간 호출 등)
  return parseBearerToken(req);
}

export async function fileRoute(app) {
  app.get('/file/*', async (req, reply) => {
    const fileKey = req.params['*'];

    if (!isValidFileKey(fileKey)) {
      return reply.code(400).send({ ok: false, error: 'invalid_file_key' });
    }

    const token = extractToken(req);
    if (!token) {
      return reply.code(401).send({ ok: false, error: 'token_required' });
    }

    const auth = await resolveStudentIdByToken(token);
    if (!auth.ok) {
      if (auth.reason === 'rpc_error') {
        req.log.error({ detail: auth.detail }, 'supabase rpc error');
        return reply.code(502).send({ ok: false, error: 'upstream_error' });
      }
      return reply.code(401).send({ ok: false, error: 'unauthorized' });
    }

    // file_key 의 student_id 와 token 의 student_id 가 일치해야 서빙
    if (
      extractStudentIdFromFileKey(fileKey) !== auth.studentId.toLowerCase()
    ) {
      return reply.code(403).send({ ok: false, error: 'forbidden' });
    }

    let buffer;
    try {
      buffer = await readFile(fileKey);
    } catch (err) {
      if (err?.code === 'ENOENT') {
        return reply.code(404).send({ ok: false, error: 'not_found' });
      }
      req.log.error({ err: err?.message }, 'file read failed');
      return reply.code(500).send({ ok: false, error: 'read_failed' });
    }

    return reply
      .header('Content-Type', mimeForFileKey(fileKey))
      .header('Content-Length', buffer.length)
      // 학생 token 기반 → private 캐시만, 짧은 만료
      .header('Cache-Control', 'private, max-age=600')
      .send(buffer);
  });
}

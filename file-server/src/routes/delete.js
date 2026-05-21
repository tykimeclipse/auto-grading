import { parseBearerToken, resolveStudentIdByToken } from '../lib/auth.js';
import {
  isValidFileKey,
  deleteFile,
  extractStudentIdFromFileKey,
} from '../lib/storage.js';

export async function deleteRoute(app) {
  app.post('/delete', async (req, reply) => {
    const token = parseBearerToken(req);
    if (!token) {
      return reply.code(401).send({ ok: false, error: 'invalid_token_format' });
    }

    const body = req.body;
    if (!body || typeof body !== 'object') {
      return reply.code(400).send({ ok: false, error: 'invalid_body' });
    }
    const fileKey  = body.file_key;
    const thumbKey = body.thumbnail_key ?? null;

    if (!isValidFileKey(fileKey)) {
      return reply.code(400).send({ ok: false, error: 'invalid_file_key' });
    }
    if (thumbKey != null && !isValidFileKey(thumbKey)) {
      return reply.code(400).send({ ok: false, error: 'invalid_thumbnail_key' });
    }

    const auth = await resolveStudentIdByToken(token);
    if (!auth.ok) {
      if (auth.reason === 'rpc_error') {
        req.log.error({ detail: auth.detail }, 'supabase rpc error');
        return reply.code(502).send({ ok: false, error: 'upstream_error' });
      }
      return reply.code(401).send({ ok: false, error: 'unauthorized' });
    }

    const studentLower = auth.studentId.toLowerCase();
    if (extractStudentIdFromFileKey(fileKey) !== studentLower) {
      return reply.code(403).send({ ok: false, error: 'forbidden_file' });
    }
    if (thumbKey && extractStudentIdFromFileKey(thumbKey) !== studentLower) {
      return reply.code(403).send({ ok: false, error: 'forbidden_thumbnail' });
    }

    let deletedFile = false;
    let deletedThumb = false;
    try {
      deletedFile = await deleteFile(fileKey);
      if (thumbKey) {
        deletedThumb = await deleteFile(thumbKey);
      }
    } catch (err) {
      req.log.error({ err: err?.message }, 'delete failed');
      return reply.code(500).send({ ok: false, error: 'delete_failed' });
    }

    return reply.send({
      ok: true,
      file_deleted:      deletedFile,
      thumbnail_deleted: deletedThumb,
    });
  });
}

import {
  parseBearerToken,
  readNoteIdHeader,
  resolveStudentIdByToken,
} from '../lib/auth.js';
import { isAcceptedMime, processImage } from '../lib/image.js';
import { generateImageKey, writeFile } from '../lib/storage.js';

export async function uploadRoute(app) {
  app.post('/upload', async (req, reply) => {
    // 1. token 형식 검증 (UUID Bearer)
    const token = parseBearerToken(req);
    if (!token) {
      return reply.code(401).send({ ok: false, error: 'invalid_token_format' });
    }

    // 2. X-Note-Id 헤더
    const noteId = readNoteIdHeader(req);
    if (!noteId) {
      return reply.code(400).send({ ok: false, error: 'invalid_note_id' });
    }

    // 3. token → student_id (Supabase RPC 호출)
    const auth = await resolveStudentIdByToken(token);
    if (!auth.ok) {
      // 외부에 reason 그대로 노출하지 않음
      if (auth.reason === 'rpc_error') {
        req.log.error({ detail: auth.detail }, 'supabase rpc error');
        return reply.code(502).send({ ok: false, error: 'upstream_error' });
      }
      return reply.code(401).send({ ok: false, error: 'unauthorized' });
    }
    const studentId = auth.studentId;

    // 4. multipart file 수신
    let data;
    try {
      data = await req.file();
    } catch (err) {
      // 크기 초과는 multipart 옵션의 limits.fileSize 위반 시 FST_REQ_FILE_TOO_LARGE
      if (err?.code === 'FST_REQ_FILE_TOO_LARGE') {
        return reply.code(413).send({ ok: false, error: 'file_too_large' });
      }
      req.log.warn({ err: err?.message }, 'multipart parse failed');
      return reply.code(400).send({ ok: false, error: 'multipart_parse_failed' });
    }
    if (!data) {
      return reply.code(400).send({ ok: false, error: 'file_required' });
    }
    if (!isAcceptedMime(data.mimetype)) {
      return reply
        .code(415)
        .send({ ok: false, error: 'unsupported_mime', mime: data.mimetype });
    }

    // toBuffer() 도 fileSize limit 초과 시 truncated 가능 → 사이즈 재확인 생략 가능
    let buffer;
    try {
      buffer = await data.toBuffer();
    } catch (err) {
      if (err?.code === 'FST_REQ_FILE_TOO_LARGE') {
        return reply.code(413).send({ ok: false, error: 'file_too_large' });
      }
      throw err;
    }

    // 5. sharp 처리
    let processed;
    try {
      processed = await processImage(buffer);
    } catch (err) {
      req.log.warn({ err: err?.message }, 'image processing failed');
      return reply
        .code(400)
        .send({ ok: false, error: 'image_processing_failed' });
    }

    // 6. 저장
    const { imageKey, thumbKey } = generateImageKey(studentId, noteId);
    try {
      await writeFile(imageKey, processed.main.buffer);
      await writeFile(thumbKey, processed.thumb.buffer);
    } catch (err) {
      req.log.error({ err: err?.message }, 'storage write failed');
      return reply.code(500).send({ ok: false, error: 'storage_failed' });
    }

    return reply.send({
      ok:             true,
      file_backend:   'local_file_server',
      file_key:       imageKey,
      thumbnail_key:  thumbKey,
      mime_type:      processed.main.mimeType,
      file_size:      processed.main.size,
      width:          processed.main.width,
      height:         processed.main.height,
    });
  });
}

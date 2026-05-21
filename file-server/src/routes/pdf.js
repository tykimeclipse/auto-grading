import { parseBearerToken, isValidUuid, fetchNoteDetailByToken } from '../lib/auth.js';
import { buildMistakeNotePdf } from '../lib/pdf.js';

// 학생/교사 공용 PDF 생성 엔드포인트.
// Q28 결정: 학생/교사 구분 없이 Authorization: Bearer <view_token> + note_id 만 검증.
// 권한 검증은 get_mistake_note_detail_by_token 이 담당 — note 가 token 학생 소유가
// 아니면 RPC 가 note_not_owned 를 반환하므로 다른 학생 노트 PDF 는 만들 수 없다.
export async function pdfRoute(app) {
  app.post('/pdf', async (req, reply) => {
    const token = parseBearerToken(req);
    if (!token) {
      return reply.code(401).send({ ok: false, error: 'invalid_token_format' });
    }

    const noteId = req.body && req.body.note_id;
    if (!isValidUuid(noteId)) {
      return reply.code(400).send({ ok: false, error: 'invalid_note_id' });
    }

    // token + note_id 권한 검증 + 메타/이미지 조회 (한 번의 RPC 로 완결)
    const detail = await fetchNoteDetailByToken(token, noteId);
    if (!detail.ok) {
      if (detail.reason === 'rpc_error') {
        req.log.error({ detail: detail.detail }, 'supabase rpc error');
        return reply.code(502).send({ ok: false, error: 'upstream_error' });
      }
      // unauthorized / note_not_owned 등은 모두 403 으로 수렴
      return reply.code(403).send({ ok: false, error: 'forbidden' });
    }

    const note   = detail.note;
    const images = detail.images;

    // draft 는 PDF 대상 아님 (submitted/archived 만)
    if (note.status === 'draft') {
      return reply.code(409).send({ ok: false, error: 'note_not_submitted' });
    }
    if (!images.length) {
      return reply.code(400).send({ ok: false, error: 'no_images' });
    }

    let pdfBytes;
    try {
      pdfBytes = await buildMistakeNotePdf(note, images);
    } catch (err) {
      req.log.error({ err: err?.message }, 'pdf build failed');
      return reply.code(500).send({ ok: false, error: 'pdf_failed' });
    }

    return reply
      .header('Content-Type', 'application/pdf')
      .header('Content-Disposition', 'attachment; filename="mistake-note.pdf"')
      .header('Content-Length', pdfBytes.length)
      .header('Cache-Control', 'private, no-store')
      .send(Buffer.from(pdfBytes));
  });
}

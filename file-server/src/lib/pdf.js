import fs from 'node:fs/promises';
import path from 'node:path';
import { PDFDocument, StandardFonts, rgb } from 'pdf-lib';
import fontkit from '@pdf-lib/fontkit';
import sharp from 'sharp';
import { config } from '../config.js';
import { readFile } from './storage.js';

// A4 (72dpi pt)
const A4 = { width: 595.28, height: 841.89 };
const MARGIN = 36;
const HEADER_H = 64;
const FOOTER_H = 24;
const IMAGES_PER_PAGE = 2;

// 한글 폰트 캐시: Buffer | false(없음) | null(미확인)
let cachedFontBytes = null;

async function loadKoreanFontBytes() {
  if (cachedFontBytes !== null) return cachedFontBytes;
  const fontPath = path.join(config.fontDir, 'NotoSansKR-Regular.ttf');
  try {
    cachedFontBytes = await fs.readFile(fontPath);
  } catch {
    cachedFontBytes = false;
  }
  return cachedFontBytes;
}

function formatDate(value) {
  if (!value) return '';
  const d = new Date(value);
  if (Number.isNaN(d.getTime())) return '';
  const y = d.getFullYear();
  const m = String(d.getMonth() + 1).padStart(2, '0');
  const day = String(d.getDate()).padStart(2, '0');
  return `${y}-${m}-${day}`;
}

// 헤더 아래 영역을 IMAGES_PER_PAGE 개 슬롯으로 분할
function computeSlots() {
  const contentTop    = A4.height - MARGIN - HEADER_H;
  const contentBottom = MARGIN + FOOTER_H;
  const gap = 12;
  const slotH = (contentTop - contentBottom - gap) / 2;
  const slotW = A4.width - MARGIN * 2;
  return [
    { x: MARGIN, y: contentBottom + slotH + gap, w: slotW, h: slotH }, // 위
    { x: MARGIN, y: contentBottom,               w: slotW, h: slotH }, // 아래
  ];
}

function drawHeader(page, font, note, hasKorean) {
  const top = A4.height - MARGIN;
  if (hasKorean) {
    page.drawText(note.test_title || '오답노트', {
      x: MARGIN, y: top - 14, size: 14, font, color: rgb(0.07, 0.07, 0.1),
    });
    const sub = [note.student_name, note.unit_title_snapshot]
      .filter(Boolean).join('  ·  ');
    if (sub) {
      page.drawText(sub, {
        x: MARGIN, y: top - 32, size: 10, font, color: rgb(0.35, 0.35, 0.4),
      });
    }
    const date = formatDate(note.created_at);
    if (date) {
      page.drawText(date, {
        x: MARGIN, y: top - 48, size: 9, font, color: rgb(0.55, 0.55, 0.6),
      });
    }
  } else {
    // 한글 폰트 없음 — ASCII 로만
    page.drawText(`Mistake Note   ${formatDate(note.created_at)}`, {
      x: MARGIN, y: top - 14, size: 11, font, color: rgb(0.2, 0.2, 0.25),
    });
  }
  // 헤더 구분선
  page.drawLine({
    start: { x: MARGIN, y: top - HEADER_H + 8 },
    end:   { x: A4.width - MARGIN, y: top - HEADER_H + 8 },
    thickness: 0.6, color: rgb(0.8, 0.8, 0.84),
  });
}

function drawPageNumber(page, font, pageNo, totalPages) {
  const txt = `${pageNo} / ${totalPages}`;
  const size = 9;
  const w = font.widthOfTextAtSize(txt, size);
  page.drawText(txt, {
    x: (A4.width - w) / 2, y: MARGIN / 2 + 4,
    size, font, color: rgb(0.55, 0.55, 0.6),
  });
}

async function drawImageInSlot(pdfDoc, page, image, slot) {
  let raw;
  try {
    raw = await readFile(image.file_key);
  } catch {
    return false; // 디스크에 파일 없음 → 스킵
  }
  // pdf-lib 는 webp embed 불가 → sharp 로 JPEG 변환
  const jpg = await sharp(raw).rotate().jpeg({ quality: 85 }).toBuffer();
  const embedded = await pdfDoc.embedJpg(jpg);
  const dim = embedded.scale(1);

  const scale = Math.min(slot.w / dim.width, slot.h / dim.height);
  const w = dim.width * scale;
  const h = dim.height * scale;
  const x = slot.x + (slot.w - w) / 2;
  const y = slot.y + (slot.h - h) / 2;
  page.drawImage(embedded, { x, y, width: w, height: h });
  return true;
}

// note: { test_title, student_name, unit_title_snapshot, created_at, ... }
// images: [{ file_key, ... }]  (sort_order 순으로 정렬되어 있다고 가정)
export async function buildMistakeNotePdf(note, images) {
  const pdfDoc = await PDFDocument.create();

  const fontBytes = await loadKoreanFontBytes();
  let font;
  if (fontBytes) {
    pdfDoc.registerFontkit(fontkit);
    font = await pdfDoc.embedFont(fontBytes, { subset: true });
  } else {
    font = await pdfDoc.embedFont(StandardFonts.Helvetica);
  }
  const hasKorean = Boolean(fontBytes);

  const totalPages = Math.max(1, Math.ceil(images.length / IMAGES_PER_PAGE));
  const slots = computeSlots();

  for (let p = 0; p < totalPages; p++) {
    const page = pdfDoc.addPage([A4.width, A4.height]);
    drawHeader(page, font, note, hasKorean);
    drawPageNumber(page, font, p + 1, totalPages);

    for (let i = 0; i < IMAGES_PER_PAGE; i++) {
      const idx = p * IMAGES_PER_PAGE + i;
      if (idx >= images.length) break;
      await drawImageInSlot(pdfDoc, page, images[idx], slots[i]);
    }
  }

  return pdfDoc.save();
}

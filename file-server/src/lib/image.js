import sharp from 'sharp';
import { config } from '../config.js';

const ACCEPTED_MIME = new Set(['image/jpeg', 'image/png', 'image/webp']);

export function isAcceptedMime(mime) {
  if (!mime || typeof mime !== 'string') return false;
  return ACCEPTED_MIME.has(mime.toLowerCase());
}

// 업로드 buffer 를 받아 EXIF 자동 회전 → WebP 변환 + 썸네일 생성.
// 반환: { main, thumb } 각각 { buffer, mimeType, width, height, size }
export async function processImage(buffer) {
  // .rotate() 인자 없이 호출하면 EXIF orientation 기반 자동 회전 후 metadata 제거.
  const base = sharp(buffer, { failOn: 'truncated' }).rotate();

  const meta = await base.metadata();
  if (!meta.width || !meta.height) {
    throw new Error('invalid_image_dimensions');
  }

  // toBuffer({resolveWithObject:true}) 결과의 info 에는 회전 후 width/height 가 들어감.
  // .toBuffer() 이후 자동으로 metadata strip 됨 (별도 호출 불필요).
  const main = await base
    .clone()
    .resize({
      width:  config.maxImageLongEdge,
      height: config.maxImageLongEdge,
      fit: 'inside',
      withoutEnlargement: true,
    })
    .webp({ quality: config.webpQuality })
    .toBuffer({ resolveWithObject: true });

  const thumb = await base
    .clone()
    .resize({
      width:  config.thumbnailLongEdge,
      height: config.thumbnailLongEdge,
      fit: 'inside',
      withoutEnlargement: true,
    })
    .webp({ quality: config.webpQuality })
    .toBuffer({ resolveWithObject: true });

  return {
    main: {
      buffer:   main.data,
      mimeType: 'image/webp',
      width:    main.info.width,
      height:   main.info.height,
      size:     main.info.size,
    },
    thumb: {
      buffer:   thumb.data,
      mimeType: 'image/webp',
      width:    thumb.info.width,
      height:   thumb.info.height,
      size:     thumb.info.size,
    },
  };
}

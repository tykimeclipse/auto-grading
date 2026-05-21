import 'dotenv/config';

function requireEnv(name) {
  const v = process.env[name];
  if (!v || !v.trim()) {
    throw new Error(`Missing required env: ${name}`);
  }
  return v.trim();
}

function parsePositiveInt(name, fallback) {
  const raw = process.env[name];
  if (!raw) return fallback;
  const n = parseInt(raw, 10);
  if (!Number.isFinite(n) || n <= 0) {
    throw new Error(`Env ${name} must be a positive integer, got: ${raw}`);
  }
  return n;
}

export const config = {
  port: parsePositiveInt('PORT', 8787),
  host: process.env.HOST?.trim() || '127.0.0.1',

  dataDir: process.env.DATA_DIR?.trim() || './data/mistake-notes',
  fontDir: process.env.FONT_DIR?.trim() || './assets/fonts',

  supabaseUrl:     requireEnv('SUPABASE_URL'),
  supabaseAnonKey: requireEnv('SUPABASE_ANON_KEY'),

  allowedOrigins: (process.env.ALLOWED_ORIGINS || '')
    .split(',')
    .map((s) => s.trim())
    .filter(Boolean),

  maxUploadBytes:    parsePositiveInt('MAX_UPLOAD_BYTES',     10 * 1024 * 1024),
  maxImageLongEdge:  parsePositiveInt('MAX_IMAGE_LONG_EDGE',  1600),
  thumbnailLongEdge: parsePositiveInt('THUMBNAIL_LONG_EDGE',  300),
  webpQuality:       parsePositiveInt('WEBP_QUALITY',         80),

  logLevel: process.env.LOG_LEVEL?.trim() || 'info',
};

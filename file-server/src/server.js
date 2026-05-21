import Fastify from 'fastify';
import multipart from '@fastify/multipart';
import cors from '@fastify/cors';

import { config } from './config.js';
import { healthRoute } from './routes/health.js';
import { uploadRoute } from './routes/upload.js';
import { fileRoute }   from './routes/file.js';
import { deleteRoute } from './routes/delete.js';
import { pdfRoute }    from './routes/pdf.js';

const app = Fastify({
  logger: {
    level: config.logLevel,
    // Authorization 헤더는 로그에서 마스킹
    redact: ['req.headers.authorization', 'req.headers["x-note-id"]'],
  },
  // JSON body 제한 (multipart 는 별도 한도)
  bodyLimit: 1024 * 16,
  // Cloudflare Tunnel 뒤에서 동작 시 trust proxy
  trustProxy: true,
});

await app.register(cors, {
  origin: (origin, cb) => {
    // origin 미설정(curl, 서버 간 호출) 허용
    if (!origin) return cb(null, true);
    if (config.allowedOrigins.includes(origin)) return cb(null, true);
    return cb(new Error('cors_blocked'), false);
  },
  credentials: false,
  allowedHeaders: ['Content-Type', 'Authorization', 'X-Note-Id'],
});

await app.register(multipart, {
  limits: {
    fileSize: config.maxUploadBytes,
    files: 1,
    fields: 0, // file 외 다른 필드 받지 않음
  },
});

await app.register(healthRoute);
await app.register(uploadRoute);
await app.register(fileRoute);
await app.register(deleteRoute);
await app.register(pdfRoute);

const start = async () => {
  try {
    await app.listen({ port: config.port, host: config.host });
    app.log.info({
      port: config.port,
      host: config.host,
      dataDir: config.dataDir,
    }, 'file-server started');
  } catch (err) {
    app.log.error(err);
    process.exit(1);
  }
};

// graceful shutdown
for (const sig of ['SIGINT', 'SIGTERM']) {
  process.on(sig, async () => {
    app.log.info({ sig }, 'shutting down');
    try {
      await app.close();
      process.exit(0);
    } catch (err) {
      app.log.error(err);
      process.exit(1);
    }
  });
}

start();

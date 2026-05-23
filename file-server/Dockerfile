# ============================================================
# auto-grading file-server — production container
#
# 베이스: node:20-bookworm-slim
#   · alpine 은 sharp prebuild 가 glibc 기반이라 호환 트러블이 잦아
#     debian slim 으로 간다. 이미지 크기는 약간 크지만 안정성 우선.
#   · node 20 LTS — package.json engines 와 일치.
# ============================================================
FROM node:20-bookworm-slim

WORKDIR /app

# 의존성 먼저 — 코드 변경 시 npm ci 캐시 재사용
COPY package.json package-lock.json* ./
RUN npm ci --omit=dev && npm cache clean --force

# 소스
COPY src ./src
COPY scripts ./scripts

# 데이터/폰트 디렉터리 — 볼륨 마운트 대상
RUN mkdir -p data/mistake-notes assets/fonts

ENV NODE_ENV=production
# 컨테이너 안 listen — 호스트 외부 노출은 docker-compose 의 ports 가 결정
ENV HOST=0.0.0.0

EXPOSE 8787

CMD ["node", "src/server.js"]

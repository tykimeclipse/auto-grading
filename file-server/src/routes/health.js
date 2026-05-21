export async function healthRoute(app) {
  app.get('/health', async () => ({
    ok: true,
    ts: new Date().toISOString(),
  }));
}

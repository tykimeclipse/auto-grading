import { createClient } from '@supabase/supabase-js';
import { config } from './config.js';

// anon 키만 사용 — service_role 사용 금지 (Phase 0 B안 정책).
// auth.persistSession=false: 서버에서 세션 캐싱 안 함.
export const supabase = createClient(config.supabaseUrl, config.supabaseAnonKey, {
  auth: {
    persistSession: false,
    autoRefreshToken: false,
  },
});

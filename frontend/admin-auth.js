// ================================================================
// admin-auth.js
// 관리자 페이지 공통 인증 모듈
//
// 사용법 (각 관리자 페이지 <head> 또는 <body> 최상단):
//   <script src="https://cdn.jsdelivr.net/npm/@supabase/supabase-js@2"></script>
//   <script src="admin-auth.js"></script>
//   <script>
//     // DOM 준비 후 즉시 호출 — 미로그인이면 admin.html로 리다이렉트
//     requireAuth();
//   </script>
// ================================================================

const ADMIN_SUPABASE_URL      = 'https://gspsquuyqkydqphbcuel.supabase.co';
const ADMIN_SUPABASE_ANON_KEY = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImdzcHNxdXV5cWt5ZHFwaGJjdWVsIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NzI0OTg5NDgsImV4cCI6MjA4ODA3NDk0OH0.9oqcUTNQpL9oFQ13hUkM5MN7KA2kW79RC3CPI4WZP7I';

// 모듈 전체에서 공유하는 Supabase 클라이언트
// admin-auth.js 로드 후 다른 스크립트에서 window.adminSupabase 로 접근 가능
window.adminSupabase = window.supabase.createClient(
  ADMIN_SUPABASE_URL,
  ADMIN_SUPABASE_ANON_KEY
);

/**
 * 세션을 확인하고 미로그인이면 admin.html로 리다이렉트합니다.
 * 각 관리자 페이지 로드 직후 호출하세요.
 *
 * @param {string} [adminPath] - admin.html의 상대 경로 (기본값: 'admin.html')
 */
async function requireAuth(adminPath = 'admin.html') {
  try {
    const { data: { session } } = await window.adminSupabase.auth.getSession();
    if (!session) {
      // 현재 페이지를 returnTo 파라미터로 전달해 로그인 후 복귀 가능하게 함
      const returnTo = encodeURIComponent(location.href);
      location.replace(`${adminPath}?returnTo=${returnTo}`);
    }
  } catch (err) {
    console.error('[admin-auth] getSession 실패:', err);
    location.replace(adminPath);
  }
}

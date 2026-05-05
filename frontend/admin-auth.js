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
const ADMIN_SUPABASE_ANON_KEY = 'sb_publishable_osq8Sjog7qDEiZ3A8toJ6Q_TQ3ATAt6';

// 허용된 관리자 이메일 목록
const ALLOWED_ADMIN_EMAILS = [
  'tykimeclipse@gmail.com'
];

// 모듈 전체에서 공유하는 Supabase 클라이언트
// admin-auth.js 로드 후 다른 스크립트에서 window.adminSupabase 로 접근 가능
window.adminSupabase = window.supabase.createClient(
  ADMIN_SUPABASE_URL,
  ADMIN_SUPABASE_ANON_KEY
);

/**
 * 세션을 확인하고, 미로그인이거나 허용되지 않은 계정이면 admin.html로 리다이렉트합니다.
 * 각 관리자 페이지 로드 직후 호출하세요.
 *
 * @param {string} [adminPath] - admin.html의 상대 경로 (기본값: 'admin.html')
 * @returns {boolean} 인증 통과 시 true, 리다이렉트 시 false
 */
async function requireAuth(adminPath = 'admin.html') {
  try {
    const { data: { session } } = await window.adminSupabase.auth.getSession();

    // 1. 로그인 여부 확인
    if (!session) {
      const returnTo = encodeURIComponent(location.href);
      location.replace(`${adminPath}?returnTo=${returnTo}`);
      return false;
    }

    // 2. 허용된 관리자 이메일 확인
    const email = session.user?.email || '';
    if (!ALLOWED_ADMIN_EMAILS.includes(email)) {
      await window.adminSupabase.auth.signOut();
      alert('관리자 권한이 없는 계정입니다.');
      location.replace(adminPath);
      return false;
    }

    return true;
  } catch (err) {
    console.error('[admin-auth] getSession 실패:', err);
    location.replace(adminPath);
    return false;
  }
}

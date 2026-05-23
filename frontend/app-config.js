// ============================================================
// 프론트엔드 환경 설정.
// 운영 배포 시 이 파일의 값만 교체한다 (HTML 은 건드리지 않음).
//
// 각 HTML 은 module 스크립트보다 먼저 일반 <script> 로 이 파일을 로드:
//   <script src="app-config.js"></script>
// ============================================================
window.APP_CONFIG = {
  // 자체 파일서버 (오답노트 이미지 업로드/조회/삭제/PDF).
  // 로컬 검증: http://localhost:8787
  // 운영: Cloudflare Tunnel 고정 도메인으로 교체 (예: https://files.bareunscience.kr)
  // ⚠️ HTTPS 페이지에서는 반드시 https 주소여야 mixed content 차단을 피한다.
  FILE_SERVER_URL: "http://192.168.1.30:8787",

  // OMR '오답노트 만들기' 버튼 노출 여부.
  // 이 버튼은 create_mistake_note_via_omr_bridge RPC 를 호출하는데, 그 RPC 는
  // 운영 DB 에서 revoke 상태다(보안 게이트). 따라서 로컬/테스트에서만 true.
  // 운영 배포 시 false 로 두면 버튼이 렌더링되지 않아 '눌렀는데 실패' UX 를 막는다.
  // note-scoped token 으로 게이트가 해소되면 이 플래그를 제거한다.
  ENABLE_OMR_MISTAKE_NOTE_BRIDGE: true,
};

// 운영(비 localhost) 페이지인데 FILE_SERVER_URL 이 localhost 면 배포 설정 누락이다.
// 조용한 localhost fallback 대신 콘솔에 명시적 경고를 남긴다.
window.APP_CONFIG.warnIfMisconfigured = function () {
  const url = window.APP_CONFIG.FILE_SERVER_URL || "";
  const pageLocal = ["localhost", "127.0.0.1"].includes(location.hostname);
  const urlLocal = /(localhost|127\.0\.0\.1)/.test(url);
  if (!pageLocal && urlLocal) {
    console.error(
      "[app-config] 운영 환경인데 FILE_SERVER_URL 이 localhost 입니다. " +
        "app-config.js 를 운영 도메인으로 교체하세요.",
    );
    return false;
  }
  return true;
};

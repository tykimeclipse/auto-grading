// search-address/index.ts
// 주소기반산업지원서비스 API 프록시
// 환경변수 JUSO_API_KEY 에 승인키 저장 (Supabase Dashboard → Project Settings → Edge Functions → Secrets)
//
// 요청: POST { keyword: string, page?: number }
// 응답: { totalCount, currentPage, countPerPage, list: [{ roadAddr, jibunAddr, bdNm, zipNo }] }
//       에러: { error: string }

import { serve } from 'https://deno.land/std@0.168.0/http/server.ts'

const CORS_HEADERS = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Headers': 'authorization, x-client-info, apikey, content-type',
}

const JUSO_API_URL = 'https://business.juso.go.kr/addrlink/addrLinkApi.do'

serve(async (req: Request) => {
  // CORS preflight
  if (req.method === 'OPTIONS') {
    return new Response('ok', { headers: CORS_HEADERS })
  }

  try {
    const { keyword, page = 1 } = await req.json()

    if (!keyword || !String(keyword).trim()) {
      return json({ error: '키워드를 입력해 주세요.' }, 400)
    }

    const confmKey = Deno.env.get('JUSO_API_KEY')
    if (!confmKey) {
      return json({ error: 'JUSO_API_KEY 환경변수가 설정되지 않았습니다.' }, 500)
    }

    const params = new URLSearchParams({
      confmKey,
      currentPage:  String(page),
      countPerPage: '10',
      keyword:      String(keyword).trim(),
      resultType:   'json',
    })

    const apiRes  = await fetch(`${JUSO_API_URL}?${params}`)
    const apiData = await apiRes.json()

    const common = apiData?.results?.common
    const juso   = apiData?.results?.juso ?? []

    // errorCode "0" = 정상, 나머지는 API 오류
    if (common?.errorCode !== '0') {
      return json({
        error: `주소 API 오류 (code: ${common?.errorCode}, msg: ${common?.errorMessage})`,
      }, 502)
    }

    return json({
      totalCount:   Number(common.totalCount),
      currentPage:  Number(common.currentPage),
      countPerPage: Number(common.countPerPage),
      list: juso.map((j: Record<string, string>) => ({
        roadAddr:  j.roadAddr,   // 도로명 전체주소  예: 경기 성남시 분당구 판교역로 235
        jibunAddr: j.jibunAddr,  // 지번 전체주소
        bdNm:      j.bdNm,       // 건물명
        zipNo:     j.zipNo,      // 우편번호
      })),
    })
  } catch (err) {
    return json({ error: String(err) }, 500)
  }
})

function json(body: unknown, status = 200): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: { ...CORS_HEADERS, 'Content-Type': 'application/json' },
  })
}

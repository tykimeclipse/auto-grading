(function attachMajorUnitAchievementUI(global) {
  "use strict";

  const GRADE_LABELS = {
    E1: "초1", E2: "초2", E3: "초3", E4: "초4", E5: "초5", E6: "초6",
    M1: "중1", M2: "중2", M3: "중3",
    H1: "고1", H2: "고2", H3: "고3",
  };

  const GRADE_ORDER = {
    E1: 0, E2: 1, E3: 2, E4: 3, E5: 4, E6: 5,
    M1: 10, M2: 11, M3: 12,
    H1: 20, H2: 21, H3: 22,
  };

  function escapeHtml(value) {
    return String(value ?? "")
      .replaceAll("&", "&amp;")
      .replaceAll("<", "&lt;")
      .replaceAll(">", "&gt;")
      .replaceAll('"', "&quot;")
      .replaceAll("'", "&#39;");
  }

  function gradeLabel(value) {
    return GRADE_LABELS[value] || value || "-";
  }

  function formatDate(value) {
    if (!value) return "-";
    if (/^\d{4}-\d{2}-\d{2}$/.test(String(value))) return String(value);
    const date = new Date(value);
    if (Number.isNaN(date.getTime())) return String(value);
    const parts = new Intl.DateTimeFormat("en", {
      timeZone: "Asia/Seoul",
      year: "numeric",
      month: "2-digit",
      day: "2-digit",
    }).formatToParts(date);
    const values = Object.fromEntries(parts.map(part => [part.type, part.value]));
    const y = values.year;
    const m = values.month;
    const d = values.day;
    return `${y}-${m}-${d}`;
  }

  function unitKey(row) {
    const unit = row?.unit || {};
    return JSON.stringify([
      Boolean(unit.is_unassigned),
      unit.grade_level || "",
      unit.curriculum_version || "",
      unit.subject || "",
      unit.major_unit_code || "",
    ]);
  }

  function compareText(a, b) {
    return String(a || "").localeCompare(String(b || ""), "ko", {
      numeric: true,
      sensitivity: "base",
    });
  }

  function sortUnits(units) {
    return (Array.isArray(units) ? units.slice() : []).sort((a, b) => {
      const au = a?.unit || {};
      const bu = b?.unit || {};
      if (Boolean(au.is_unassigned) !== Boolean(bu.is_unassigned)) {
        return au.is_unassigned ? 1 : -1;
      }
      const gradeDifference = (GRADE_ORDER[au.grade_level] ?? 999)
        - (GRADE_ORDER[bu.grade_level] ?? 999);
      if (gradeDifference) return gradeDifference;
      return compareText(au.curriculum_version, bu.curriculum_version)
        || compareText(au.subject, bu.subject)
        || compareText(au.major_unit_code, bu.major_unit_code)
        || compareText(au.major_unit_name, bu.major_unit_name);
    });
  }

  function scoreClass(value) {
    const score = Number(value);
    if (score === 100) return "is-perfect";
    if (score >= 80) return "is-good";
    if (score >= 60) return "is-watch";
    return "is-risk";
  }

  function scoreHtml(score, decimals) {
    if (score == null || Number.isNaN(Number(score))) return "-";
    const value = Number(score);
    const digits = Number.isInteger(decimals) ? decimals : 1;
    return `<span class="major-unit-score-pill ${scoreClass(value)}">${value.toFixed(digits)}%</span>`;
  }

  function scoreTitle(score) {
    if (!score || score.item_count == null || Number(score.item_count) === 0) return "";
    return `${Number(score.correct_count || 0)} / ${Number(score.item_count)}문항`;
  }

  function daysAgoText(days) {
    if (days == null || Number.isNaN(Number(days))) return "평가일 미상";
    const value = Number(days);
    if (value <= 0) return "오늘 평가";
    return `${value}일 전 평가`;
  }

  function deltaHtml(value) {
    if (value == null || Number.isNaN(Number(value))) return "";
    const delta = Number(value);
    const className = delta > 0 ? "is-up" : (delta < 0 ? "is-down" : "is-flat");
    const sign = delta > 0 ? "+" : "";
    return `<span class="major-unit-delta ${className}">직전 대비 ${sign}${delta.toFixed(1)}%p</span>`;
  }

  function retentionHtml(row) {
    const retention = row?.retention || {};
    const finalSeries = retention.final_confirmed || {};
    const round1Series = retention.round1 || {};
    const finalCount = Number(finalSeries.test_count || 0);
    const round1Count = Number(round1Series.test_count || 0);
    const useFinal = finalCount >= 2 || (round1Count < 2 && finalCount > 0);
    const series = useFinal ? finalSeries : round1Series;
    const testCount = Number(series.test_count || 0);
    if (!testCount || series.latest_score_percent == null) return "-";

    const basis = useFinal ? "최종" : "1차";
    const latest = Number(series.latest_score_percent).toFixed(1);
    const delta = deltaHtml(series.change_from_previous_percentage_points);
    const firstDelta = series.change_from_first_percentage_points;
    const title = testCount >= 2 && firstDelta != null
      ? `첫 평가 대비 ${Number(firstDelta) > 0 ? "+" : ""}${Number(firstDelta).toFixed(1)}%p`
      : `${basis} 평가 ${testCount}건`;
    return `<div class="major-unit-retention-main" title="${escapeHtml(title)}">`
      + `${basis} 최근 ${latest}%${delta ? ` · ${delta}` : ""}`
      + `</div><span class="major-unit-retention-sub">${daysAgoText(series.days_since_latest)}`
      + `${testCount >= 2 ? ` · 반복 평가 ${testCount}건` : " · 첫 평가"}</span>`;
  }

  function unitTitleHtml(row) {
    const unit = row?.unit || {};
    if (unit.is_unassigned) {
      return {
        name: "단원 미지정",
        context: "기존 평가 · 학년/교육과정/과목 미지정",
      };
    }
    const code = unit.major_unit_code ? `${unit.major_unit_code}. ` : "";
    const curriculum = unit.curriculum_version
      ? `${unit.curriculum_version} 교육과정`
      : "교육과정 미지정";
    return {
      name: `${code}${unit.major_unit_name || "대단원명 미지정"}`,
      context: [gradeLabel(unit.grade_level), curriculum, unit.subject || "과목 미지정"].join(" · "),
    };
  }

  function timelineBadge(index, length, truncated) {
    if (length < 2) return "";
    if (index === 0) {
      return truncated
        ? ""
        : '<span class="major-unit-timeline-badge">첫 평가</span>';
    }
    if (index === length - 1) return '<span class="major-unit-timeline-badge">최근 평가</span>';
    return "";
  }

  function renderTestRows(row, manualBadgeLabel) {
    const tests = Array.isArray(row?.tests) ? row.tests : [];
    if (!tests.length) {
      return '<tr><td colspan="6" class="major-unit-empty">표시할 완료 평가가 없습니다.</td></tr>';
    }
    return tests.map((test, index) => {
      const manualBadge = test?.source_type === "manual"
        ? ` <span class="major-unit-badge">${escapeHtml(manualBadgeLabel)}</span>`
        : "";
      return `<tr>`
        + `<td>${escapeHtml(formatDate(test?.event_date || test?.evaluated_at))}`
        + `${timelineBadge(index, tests.length, Boolean(row?.tests_truncated))}</td>`
        + `<td class="major-unit-test-title">${escapeHtml(test?.test_title || "-")}${manualBadge}</td>`
        + `<td>${test?.total_items != null ? escapeHtml(test.total_items) : "-"}</td>`
        + `<td title="${escapeHtml(scoreTitle(test?.round1))}">${scoreHtml(test?.round1?.score_percent, 1)}</td>`
        + `<td title="${escapeHtml(scoreTitle(test?.round2_reflected))}">${scoreHtml(test?.round2_reflected?.score_percent, 1)}</td>`
        + `<td title="${escapeHtml(scoreTitle(test?.final))}">${scoreHtml(test?.final?.score_percent, 1)}</td>`
        + `</tr>`;
    }).join("");
  }

  function renderDetailRow(row, index, manualBadgeLabel) {
    const returnedCount = Number(row?.returned_test_count ?? row?.tests?.length ?? 0);
    const totalCount = Number(row?.test_count || 0);
    const truncated = Boolean(row?.tests_truncated);
    const note = truncated
      ? `전체 ${totalCount}건 중 최근 ${returnedCount}건을 시간순으로 표시합니다.`
      : `${returnedCount}건을 오래된 평가부터 표시합니다.`;
    return `<tr class="major-unit-detail-row" data-unit-detail-index="${index}">`
      + `<td colspan="6"><div class="major-unit-detail">`
      + `<div class="major-unit-detail-heading"><strong>테스트별 성취도</strong>`
      + `<span class="major-unit-detail-note">${escapeHtml(note)}</span></div>`
      + `<div class="major-unit-detail-scroll"><table class="major-unit-detail-table">`
      + `<thead><tr><th>평가일</th><th>시험명</th><th>문항수</th>`
      + `<th>1차</th><th>2차 반영</th><th>최종</th></tr></thead>`
      + `<tbody>${renderTestRows(row, manualBadgeLabel)}</tbody>`
      + `</table></div></div></td></tr>`;
  }

  function renderUnitRows(units, expandedKeys, options) {
    const rows = sortUnits(units);
    const expanded = expandedKeys instanceof Set ? expandedKeys : new Set();
    const manualBadgeLabel = options?.manualBadgeLabel || "단일 평가";
    if (!rows.length) {
      return {
        rows,
        html: '<tr><td colspan="6" class="major-unit-empty">표시할 단원별 완료 평가가 없습니다.</td></tr>',
      };
    }

    const html = rows.map((row, index) => {
      const key = unitKey(row);
      const open = expanded.has(key);
      const title = unitTitleHtml(row);
      const scores = row?.scores || {};
      const summaryRow = `<tr class="major-unit-row${open ? " is-open" : ""}">`
        + `<td class="major-unit-title-cell"><button type="button" class="major-unit-toggle" `
        + `data-unit-index="${index}" aria-expanded="${open ? "true" : "false"}">`
        + `<span class="major-unit-chevron" aria-hidden="true">▶</span><span>`
        + `<span class="major-unit-name">${escapeHtml(title.name)}</span>`
        + `<span class="major-unit-context">${escapeHtml(title.context)}</span>`
        + `</span></button></td>`
        + `<td><span class="major-unit-count">${Number(row?.test_count || 0)}</span>`
        + `<span class="major-unit-count-label">완료 평가</span></td>`
        + `<td title="${escapeHtml(scoreTitle(scores.round1))}">${scoreHtml(scores.round1?.score_percent, 1)}</td>`
        + `<td title="${escapeHtml(scoreTitle(scores.round2_reflected))}">${scoreHtml(scores.round2_reflected?.score_percent, 1)}</td>`
        + `<td title="${escapeHtml(scoreTitle(scores.final))}">${scoreHtml(scores.final?.score_percent, 1)}</td>`
        + `<td class="major-unit-retention">${retentionHtml(row)}</td>`
        + `</tr>`;
      return summaryRow + (open ? renderDetailRow(row, index, manualBadgeLabel) : "");
    }).join("");
    return { rows, html };
  }

  function summaryText(payload) {
    const summary = payload?.summary || {};
    const testCount = Number(summary.test_count || 0);
    const unitCount = Number(summary.unit_count || 0);
    const dateRange = summary.first_evaluated_at && summary.last_evaluated_at
      ? ` · ${formatDate(summary.first_evaluated_at)} ~ ${formatDate(summary.last_evaluated_at)}`
      : "";
    return `완료 평가 ${testCount}건 · 단원 ${unitCount}개 · 문항 수 가중 평균${dateRange}`;
  }

  global.MajorUnitAchievementUI = Object.freeze({
    formatDate,
    renderUnitRows,
    sortUnits,
    summaryText,
    unitKey,
  });
}(window));

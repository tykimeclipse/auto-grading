const SUPABASE_URL = 'https://gspsquuyqkydqphbcuel.supabase.co';
const SUPABASE_ANON_KEY = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImdzcHNxdXV5cWt5ZHFwaGJjdWVsIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NzI0OTg5NDgsImV4cCI6MjA4ODA3NDk0OH0.9oqcUTNQpL9oFQ13hUkM5MN7KA2kW79RC3CPI4WZP7I';
const SHEET_NAME = 'Students_Input';

function registerStudentsToSupabase() {

    const sheet = SpreadsheetApp.getActiveSpreadsheet().getSheetByName(SHEET_NAME);

    if (!sheet) {
        throw new Error(`시트 '${SHEET_NAME}'를 찾을 수 없습니다.`);
    }

    const values = sheet.getDataRange().getValues();

    if (values.length < 2) {
        Logger.log('등록할 데이터가 없습니다.');
        return;
    }

    const headers = values[0];
    const rows = values.slice(1);

    const colIndex = {};

    headers.forEach((h, i) => {
        colIndex[String(h).trim()] = i;
    });

    const requiredColumns = [
        'name', 'gender', 'birth_year', 'grade_level', 'parent_name',
        'student_phone', 'parent_phone', 'email', 'address',
        'has_advanced_progress', 'attends_other_academy', 'uses_online_lectures',
        'last_semester_score_summary', 'desired_path', 'notes', 'status', 'result'
    ];

    requiredColumns.forEach(col => {
        if (!(col in colIndex)) {
            throw new Error(`필수 컬럼이 없습니다: ${col}`);
        }
    });

    rows.forEach((row, rowOffset) => {

        const sheetRow = rowOffset + 2;

        const status = String(row[colIndex.status] || '').trim().toUpperCase();

        if (status !== 'READY') {
            return;
        }

        const payload = {
            p_name: normalizeText(row[colIndex.name]),
            p_gender: normalizeGender(row[colIndex.gender]),
            p_birth_year: normalizeInteger(row[colIndex.birth_year]),
            p_grade_level: normalizeText(row[colIndex.grade_level]),
            p_parent_name: normalizeText(row[colIndex.parent_name]),
            p_student_phone: normalizeText(row[colIndex.student_phone]),
            p_parent_phone: normalizeText(row[colIndex.parent_phone]),
            p_email: normalizeText(row[colIndex.email]),
            p_address: normalizeText(row[colIndex.address]),
            p_has_advanced_progress: normalizeBoolean(row[colIndex.has_advanced_progress]),
            p_attends_other_academy: normalizeBoolean(row[colIndex.attends_other_academy]),
            p_uses_online_lectures: normalizeBoolean(row[colIndex.uses_online_lectures]),
            p_last_semester_score_summary: normalizeText(row[colIndex.last_semester_score_summary]),
            p_desired_path: normalizeText(row[colIndex.desired_path]),
            p_notes: normalizeText(row[colIndex.notes]),
        };

        try {

            if (!payload.p_name) {
                throw new Error('name 값이 비어 있습니다.');
            }

            const result = callSupabaseCreateStudent(payload);

            sheet.getRange(sheetRow, colIndex.status + 1).setValue('DONE');

            sheet.getRange(sheetRow, colIndex.result + 1).setValue(
                `등록 성공: student_no=${result.student_no}, student_code=${result.student_code}`
            );

        } catch (err) {

            sheet.getRange(sheetRow, colIndex.status + 1).setValue('ERROR');

            sheet.getRange(sheetRow, colIndex.result + 1).setValue(
                `등록 실패: ${err.message || err}`
            );
        }

    });
}



function callSupabaseCreateStudent(payload) {

    const url = `${SUPABASE_URL}/rest/v1/rpc/create_student`;

    const options = {
        method: 'post',
        contentType: 'application/json',
        headers: {
            apikey: SUPABASE_ANON_KEY,
            Authorization: `Bearer ${SUPABASE_ANON_KEY}`,
            Prefer: 'return=representation'
        },
        payload: JSON.stringify(payload),
        muteHttpExceptions: true
    };

    const response = UrlFetchApp.fetch(url, options);

    const statusCode = response.getResponseCode();
    const text = response.getContentText();

    let data;

    try {
        data = text ? JSON.parse(text) : null;
    } catch (e) {
        throw new Error(`응답 JSON 파싱 실패: ${text}`);
    }

    if (statusCode < 200 || statusCode >= 300) {

        const message =
            data?.message ||
            data?.error_description ||
            data?.hint ||
            text ||
            `HTTP ${statusCode}`;

        throw new Error(message);
    }

    return data;
}



function normalizeText(v) {

    if (v === null || v === undefined) return null;

    const s = String(v).trim();

    return s === '' ? null : s;
}


function normalizeGender(v) {

    if (!v) return null;

    const s = String(v).trim().toLowerCase();

    if (s === 'm' || s === '남' || s === '남자' || s === 'male') return 'M';
    if (s === 'f' || s === '여' || s === '여자' || s === 'female') return 'F';

    return null;
}



function normalizeInteger(v) {

    if (v === null || v === undefined || v === '') return null;

    const n = Number(v);

    return Number.isFinite(n) ? Math.trunc(n) : null;
}



function normalizeBoolean(v) {

    if (typeof v === 'boolean') return v;

    if (v === null || v === undefined || v === '') return false;

    const s = String(v).trim().toLowerCase();

    return ['true', '1', 'y', 'yes', '예', 'o'].includes(s);
}

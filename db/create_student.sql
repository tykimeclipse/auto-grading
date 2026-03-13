CREATE OR REPLACE FUNCTION auto_grading.create_student(
    p_name text,
    p_gender text DEFAULT NULL,
    p_birth_year integer DEFAULT NULL,
    p_grade_level text DEFAULT NULL,
    p_parent_name text DEFAULT NULL,
    p_student_phone text DEFAULT NULL,
    p_parent_phone text DEFAULT NULL,
    p_email text DEFAULT NULL,
    p_address text DEFAULT NULL,
    p_has_advanced_progress boolean DEFAULT false,
    p_attends_other_academy boolean DEFAULT false,
    p_uses_online_lectures boolean DEFAULT false,
    p_last_semester_score_summary text DEFAULT NULL,
    p_desired_path text DEFAULT NULL,
    p_notes text DEFAULT NULL
) RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = auto_grading, public
AS $$
DECLARE
    v_student_id uuid;
    v_student_no integer;
    v_student_code text;
    v_name text;
    v_gender text;
BEGIN
    v_name := btrim(p_name);

    IF nullif(v_name, '') IS NULL THEN
        RAISE EXCEPTION 'STUDENT_NAME_REQUIRED' USING errcode = 'P0001';
    END IF;

    IF p_birth_year IS NOT NULL
       AND p_birth_year NOT BETWEEN 1900 AND 2100 THEN
        RAISE EXCEPTION 'INVALID_BIRTH_YEAR'
            USING errcode = 'P0001',
                  hint = 'birth_year must be between 1900 and 2100';
    END IF;

    v_gender := upper(nullif(btrim(p_gender), ''));

    IF v_gender IS NOT NULL
       AND v_gender NOT IN ('M', 'F', 'OTHER', 'UNKNOWN') THEN
        RAISE EXCEPTION 'INVALID_GENDER'
            USING errcode = 'P0001',
                  hint = 'Allowed values: M, F, OTHER, UNKNOWN';
    END IF;

    v_student_no := nextval('auto_grading.student_no_seq');
    v_student_code := v_student_no::text;

    INSERT INTO auto_grading.students (
        student_no,
        student_code,
        name,
        grade_level,
        parent_name,
        parent_phone,
        gender,
        birth_year,
        student_phone,
        email,
        address,
        has_advanced_progress,
        attends_other_academy,
        uses_online_lectures,
        last_semester_score_summary,
        desired_path,
        notes,
        is_active
    )
    VALUES (
        v_student_no,
        v_student_code,
        v_name,
        nullif(btrim(p_grade_level), ''),
        nullif(btrim(p_parent_name), ''),
        nullif(btrim(p_parent_phone), ''),
        v_gender,
        p_birth_year,
        nullif(btrim(p_student_phone), ''),
        nullif(btrim(p_email), ''),
        nullif(btrim(p_address), ''),
        p_has_advanced_progress,
        p_attends_other_academy,
        p_uses_online_lectures,
        nullif(btrim(p_last_semester_score_summary), ''),
        nullif(btrim(p_desired_path), ''),
        nullif(btrim(p_notes), ''),
        true
    )
    RETURNING id INTO v_student_id;

    RETURN jsonb_build_object(
        'student_id', v_student_id,
        'student_no', v_student_no,
        'student_code', v_student_code,
        'name', v_name
    );
END;
$$;

CREATE OR REPLACE FUNCTION public.create_student(
    p_name text,
    p_gender text DEFAULT NULL,
    p_birth_year integer DEFAULT NULL,
    p_grade_level text DEFAULT NULL,
    p_parent_name text DEFAULT NULL,
    p_student_phone text DEFAULT NULL,
    p_parent_phone text DEFAULT NULL,
    p_email text DEFAULT NULL,
    p_address text DEFAULT NULL,
    p_has_advanced_progress boolean DEFAULT false,
    p_attends_other_academy boolean DEFAULT false,
    p_uses_online_lectures boolean DEFAULT false,
    p_last_semester_score_summary text DEFAULT NULL,
    p_desired_path text DEFAULT NULL,
    p_notes text DEFAULT NULL
) RETURNS jsonb
LANGUAGE sql
SECURITY DEFINER
SET search_path = auto_grading, public
AS $$
    SELECT auto_grading.create_student(
        p_name => p_name,
        p_gender => p_gender,
        p_birth_year => p_birth_year,
        p_grade_level => p_grade_level,
        p_parent_name => p_parent_name,
        p_student_phone => p_student_phone,
        p_parent_phone => p_parent_phone,
        p_email => p_email,
        p_address => p_address,
        p_has_advanced_progress => p_has_advanced_progress,
        p_attends_other_academy => p_attends_other_academy,
        p_uses_online_lectures => p_uses_online_lectures,
        p_last_semester_score_summary => p_last_semester_score_summary,
        p_desired_path => p_desired_path,
        p_notes => p_notes
    );
$$;

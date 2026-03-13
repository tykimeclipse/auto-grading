alter table
  auto_grading.students
add
  column if not exists student_no integer,
add
  column if not exists gender text,
add
  column if not exists birth_year integer,
add
  column if not exists student_phone text,
add
  column if not exists email text,
add
  column if not exists address text,
add
  column if not exists has_advanced_progress boolean default false,
add
  column if not exists attends_other_academy boolean default false,
add
  column if not exists uses_online_lectures boolean default false,
add
  column if not exists last_semester_score_summary text,
add
  column if not exists desired_path text,
add
  column if not exists notes text;



  create unique index if not exists uq_students_student_no on auto_grading.students(student_no)
where
  student_no is not null;


create unique index if not exists uq_students_student_code on auto_grading.students(student_code)
where
  student_code is not null;



create sequence if not exists auto_grading.student_no_seq start
with
  101 increment by 1 minvalue 101 no maxvalue cache 1;  


select
  setval(
    'auto_grading.student_no_seq',
    greatest(
      coalesce(
        (
          select
            max(student_no)
          from
            auto_grading.students
        ),
        101
      ),
      101
    ),
    false
  ); 
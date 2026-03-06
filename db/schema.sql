-- =========================================================
-- auto_grading schema - full final version
-- =========================================================

create schema if not exists auto_grading;
set search_path = auto_grading;

create extension if not exists pgcrypto;

-- =========================================================
-- 1. updated_at trigger function
-- =========================================================
create or replace function auto_grading.set_updated_at()
returns trigger
language plpgsql
as $$
begin
  new.updated_at = now();
  return new;
end;
$$;

-- =========================================================
-- 2. normalize answer key
--    내부 표준 구분자는 '|'
--    ',' 입력도 방어적으로 허용
-- =========================================================
create or replace function auto_grading.normalize_answer_key(input_text text)
returns text
language sql
immutable
as $$
  with parts as (
    select trim(x) as part
    from unnest(
      string_to_array(
        replace(coalesce(input_text, ''), ',', '|'),
        '|'
      )
    ) as x
  ),
  cleaned as (
    select distinct part::int as n
    from parts
    where part <> ''
      and part ~ '^\d+$'
  )
  select coalesce(string_agg(n::text, '|' order by n), '')
  from cleaned;
$$;

-- =========================================================
-- 3. students
-- =========================================================
create table if not exists auto_grading.students (
  id uuid primary key default gen_random_uuid(),
  student_code text not null unique,
  name text not null,
  grade_level text,
  parent_name text,
  parent_phone text,
  is_active boolean not null default true,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

drop trigger if exists trg_students_updated_at on auto_grading.students;
create trigger trg_students_updated_at
before update on auto_grading.students
for each row
execute function auto_grading.set_updated_at();

-- =========================================================
-- 4. test_sets
-- =========================================================
create table if not exists auto_grading.test_sets (
  id uuid primary key default gen_random_uuid(),
  title text not null,
  source_type text not null default 'csv_upload',
  source_name text,
  original_filename text,
  subject text,
  grade_level text,
  major_unit text,
  minor_unit text,
  default_choice_count integer not null default 5,
  total_items integer not null default 0,
  is_active boolean not null default true,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  constraint chk_test_sets_source_type
    check (source_type in ('csv_upload', 'printed_sheet', 'published_book', 'problem_bank')),
  constraint chk_test_sets_default_choice_count
    check (default_choice_count >= 2 and default_choice_count <= 20),
  constraint chk_test_sets_total_items
    check (total_items >= 0)
);

drop trigger if exists trg_test_sets_updated_at on auto_grading.test_sets;
create trigger trg_test_sets_updated_at
before update on auto_grading.test_sets
for each row
execute function auto_grading.set_updated_at();

create index if not exists idx_test_sets_title on auto_grading.test_sets(title);
create index if not exists idx_test_sets_subject on auto_grading.test_sets(subject);

-- =========================================================
-- 5. test_items
-- =========================================================
create table if not exists auto_grading.test_items (
  id uuid primary key default gen_random_uuid(),
  test_set_id uuid not null references auto_grading.test_sets(id) on delete cascade,
  item_no integer not null,
  choice_count integer not null default 5,
  answer_key_raw text not null,
  answer_key_normalized text not null,
  topic_tag text,
  note text,
  problem_bank_question_id uuid,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  constraint uq_test_items_test_set_item_no unique (test_set_id, item_no),
  constraint chk_test_items_item_no check (item_no > 0),
  constraint chk_test_items_choice_count check (choice_count >= 2 and choice_count <= 20),
  constraint chk_test_items_answer_key_normalized_not_empty check (answer_key_normalized <> '')
);

drop trigger if exists trg_test_items_updated_at on auto_grading.test_items;
create trigger trg_test_items_updated_at
before update on auto_grading.test_items
for each row
execute function auto_grading.set_updated_at();

create index if not exists idx_test_items_test_set_id on auto_grading.test_items(test_set_id);
create index if not exists idx_test_items_topic_tag on auto_grading.test_items(topic_tag);

-- =========================================================
-- 6. assignments
-- =========================================================
create table if not exists auto_grading.assignments (
  id uuid primary key default gen_random_uuid(),
  student_id uuid not null references auto_grading.students(id) on delete cascade,
  test_set_id uuid not null references auto_grading.test_sets(id) on delete cascade,
  assigned_at timestamptz not null default now(),
  assigned_by text,
  due_at timestamptz,
  status text not null default 'assigned',
  note text,
  is_active boolean not null default true,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  constraint uq_assignments_student_test_set unique (student_id, test_set_id),
  constraint chk_assignments_status
    check (status in ('assigned', 'completed', 'archived'))
);

drop trigger if exists trg_assignments_updated_at on auto_grading.assignments;
create trigger trg_assignments_updated_at
before update on auto_grading.assignments
for each row
execute function auto_grading.set_updated_at();

create index if not exists idx_assignments_student_id on auto_grading.assignments(student_id);
create index if not exists idx_assignments_test_set_id on auto_grading.assignments(test_set_id);
create index if not exists idx_assignments_status on auto_grading.assignments(status);

-- =========================================================
-- 7. attempts
-- =========================================================
create table if not exists auto_grading.attempts (
  id uuid primary key default gen_random_uuid(),
  student_id uuid not null references auto_grading.students(id) on delete cascade,
  test_set_id uuid not null references auto_grading.test_sets(id) on delete cascade,
  assignment_id uuid references auto_grading.assignments(id) on delete set null,
  attempt_no integer not null,
  status text not null default 'in_progress',
  max_rounds integer not null default 2,
  current_round integer not null default 1,
  first_correct_count integer not null default 0,
  second_correct_count integer not null default 0,
  final_correct_count integer not null default 0,
  total_items integer not null default 0,
  first_score_percent numeric(5,2) not null default 0,
  final_score_percent numeric(5,2) not null default 0,
  unanswered_count_round1 integer not null default 0,
  incorrect_count_after_round2 integer not null default 0,
  reopen_count integer not null default 0,
  started_at timestamptz not null default now(),
  round1_submitted_at timestamptz,
  round2_submitted_at timestamptz,
  completed_at timestamptz,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  constraint uq_attempts_student_test_set_attempt_no unique (student_id, test_set_id, attempt_no),
  constraint chk_attempts_status
    check (status in ('in_progress', 'awaiting_retry', 'completed', 'needs_review')),
  constraint chk_attempts_attempt_no check (attempt_no > 0),
  constraint chk_attempts_max_rounds check (max_rounds >= 1 and max_rounds <= 10),
  constraint chk_attempts_current_round check (current_round >= 1 and current_round <= 10),
  constraint chk_attempts_counts_non_negative check (
    first_correct_count >= 0 and
    second_correct_count >= 0 and
    final_correct_count >= 0 and
    total_items >= 0 and
    unanswered_count_round1 >= 0 and
    incorrect_count_after_round2 >= 0 and
    reopen_count >= 0
  ),
  constraint chk_attempts_score_range check (
    first_score_percent >= 0 and first_score_percent <= 100 and
    final_score_percent >= 0 and final_score_percent <= 100
  )
);

drop trigger if exists trg_attempts_updated_at on auto_grading.attempts;
create trigger trg_attempts_updated_at
before update on auto_grading.attempts
for each row
execute function auto_grading.set_updated_at();

create index if not exists idx_attempts_student_id on auto_grading.attempts(student_id);
create index if not exists idx_attempts_test_set_id on auto_grading.attempts(test_set_id);
create index if not exists idx_attempts_assignment_id on auto_grading.attempts(assignment_id);
create index if not exists idx_attempts_status on auto_grading.attempts(status);
create index if not exists idx_attempts_started_at on auto_grading.attempts(started_at desc);
create index if not exists idx_attempts_student_status on auto_grading.attempts(student_id, status);

-- =========================================================
-- 8. responses
-- =========================================================
create table if not exists auto_grading.responses (
  id uuid primary key default gen_random_uuid(),
  attempt_id uuid not null references auto_grading.attempts(id) on delete cascade,
  test_item_id uuid not null references auto_grading.test_items(id) on delete cascade,
  round_no integer not null,
  selected_answer_raw text,
  selected_answer_normalized text,
  is_correct boolean not null default false,
  is_submitted boolean not null default false,
  autosaved boolean not null default false,
  answered_at timestamptz,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  constraint uq_responses_attempt_item_round unique (attempt_id, test_item_id, round_no),
  constraint chk_responses_round_no check (round_no >= 1 and round_no <= 10)
);

drop trigger if exists trg_responses_updated_at on auto_grading.responses;
create trigger trg_responses_updated_at
before update on auto_grading.responses
for each row
execute function auto_grading.set_updated_at();

create index if not exists idx_responses_attempt_id on auto_grading.responses(attempt_id);
create index if not exists idx_responses_test_item_id on auto_grading.responses(test_item_id);
create index if not exists idx_responses_round_no on auto_grading.responses(round_no);

-- =========================================================
-- 9. student_public_links
-- =========================================================
create table if not exists auto_grading.student_public_links (
  id uuid primary key default gen_random_uuid(),
  student_id uuid not null references auto_grading.students(id) on delete cascade,
  public_token text not null unique,
  is_active boolean not null default true,
  expires_at timestamptz,
  created_at timestamptz not null default now()
);

create index if not exists idx_student_public_links_student_id
  on auto_grading.student_public_links(student_id);

create index if not exists idx_student_public_links_token
  on auto_grading.student_public_links(public_token);

create index if not exists idx_student_public_links_student_active
  on auto_grading.student_public_links(student_id, is_active);

-- =========================================================
-- 10. test_items 정답 정규화 trigger
-- =========================================================
create or replace function auto_grading.set_test_item_answer_key_normalized()
returns trigger
language plpgsql
as $$
begin
  new.answer_key_normalized := auto_grading.normalize_answer_key(new.answer_key_raw);
  return new;
end;
$$;

drop trigger if exists trg_test_items_normalize_answer_key on auto_grading.test_items;
create trigger trg_test_items_normalize_answer_key
before insert or update on auto_grading.test_items
for each row
execute function auto_grading.set_test_item_answer_key_normalized();

-- =========================================================
-- 11. responses 응답 정규화 trigger
-- =========================================================
create or replace function auto_grading.set_response_answer_normalized()
returns trigger
language plpgsql
as $$
begin
  if new.selected_answer_raw is null then
    new.selected_answer_normalized := null;
  else
    new.selected_answer_normalized := auto_grading.normalize_answer_key(new.selected_answer_raw);
  end if;
  return new;
end;
$$;

drop trigger if exists trg_responses_normalize_answer on auto_grading.responses;
create trigger trg_responses_normalize_answer
before insert or update on auto_grading.responses
for each row
execute function auto_grading.set_response_answer_normalized();

-- =========================================================
-- 12. attempt_no 계산 보조 함수
-- =========================================================
create or replace function auto_grading.get_next_attempt_no(p_student_id uuid, p_test_set_id uuid)
returns integer
language sql
stable
as $$
  select coalesce(max(attempt_no), 0) + 1
  from auto_grading.attempts
  where student_id = p_student_id
    and test_set_id = p_test_set_id;
$$;

-- =========================================================
-- 13. test_set total_items 재계산 함수
-- =========================================================
create or replace function auto_grading.refresh_test_set_total_items(p_test_set_id uuid)
returns void
language plpgsql
as $$
begin
  update auto_grading.test_sets
  set total_items = (
    select count(*)
    from auto_grading.test_items
    where test_set_id = p_test_set_id
  )
  where id = p_test_set_id;
end;
$$;

-- =========================================================
-- 14. test_items -> test_sets.total_items 갱신
--     이벤트별 statement-level trigger
-- =========================================================

create or replace function auto_grading.trg_refresh_test_set_total_items_ins()
returns trigger
language plpgsql
as $$
begin
  update auto_grading.test_sets ts
  set total_items = sub.cnt
  from (
    select ti.test_set_id, count(*)::int as cnt
    from auto_grading.test_items ti
    where ti.test_set_id in (
      select distinct test_set_id
      from new_table
      where test_set_id is not null
    )
    group by ti.test_set_id
  ) sub
  where ts.id = sub.test_set_id;

  return null;
end;
$$;

drop trigger if exists trg_test_items_refresh_total_items_ins on auto_grading.test_items;
create trigger trg_test_items_refresh_total_items_ins
after insert on auto_grading.test_items
referencing new table as new_table
for each statement
execute function auto_grading.trg_refresh_test_set_total_items_ins();

create or replace function auto_grading.trg_refresh_test_set_total_items_del()
returns trigger
language plpgsql
as $$
begin
  update auto_grading.test_sets ts
  set total_items = coalesce(sub.cnt, 0)
  from (
    select affected.test_set_id, count(ti.id)::int as cnt
    from (
      select distinct test_set_id
      from old_table
      where test_set_id is not null
    ) affected
    left join auto_grading.test_items ti
      on ti.test_set_id = affected.test_set_id
    group by affected.test_set_id
  ) sub
  where ts.id = sub.test_set_id;

  return null;
end;
$$;

drop trigger if exists trg_test_items_refresh_total_items_del on auto_grading.test_items;
create trigger trg_test_items_refresh_total_items_del
after delete on auto_grading.test_items
referencing old table as old_table
for each statement
execute function auto_grading.trg_refresh_test_set_total_items_del();

create or replace function auto_grading.trg_refresh_test_set_total_items_upd()
returns trigger
language plpgsql
as $$
begin
  update auto_grading.test_sets ts
  set total_items = coalesce(sub.cnt, 0)
  from (
    select affected.test_set_id, count(ti.id)::int as cnt
    from (
      select distinct test_set_id
      from (
        select test_set_id from old_table
        union
        select test_set_id from new_table
      ) moved
      where test_set_id is not null
    ) affected
    left join auto_grading.test_items ti
      on ti.test_set_id = affected.test_set_id
    group by affected.test_set_id
  ) sub
  where ts.id = sub.test_set_id;

  return null;
end;
$$;

drop trigger if exists trg_test_items_refresh_total_items_upd on auto_grading.test_items;
create trigger trg_test_items_refresh_total_items_upd
after update on auto_grading.test_items
referencing old table as old_table new table as new_table
for each statement
execute function auto_grading.trg_refresh_test_set_total_items_upd();

-- =========================================================
-- 15. 응답 채점 보조 view
-- =========================================================
create or replace view auto_grading.v_response_grading as
select
  r.id as response_id,
  r.attempt_id,
  a.student_id,
  a.test_set_id,
  r.test_item_id,
  ti.item_no,
  r.round_no,
  r.selected_answer_raw,
  r.selected_answer_normalized,
  ti.answer_key_normalized,
  (r.selected_answer_normalized = ti.answer_key_normalized) as computed_is_correct,
  r.is_correct,
  r.is_submitted,
  r.autosaved,
  r.answered_at,
  r.created_at,
  r.updated_at
from auto_grading.responses r
join auto_grading.test_items ti on ti.id = r.test_item_id
join auto_grading.attempts a on a.id = r.attempt_id;

-- =========================================================
-- 16. 학생 대시보드 집계 view
-- =========================================================
create or replace view auto_grading.v_student_summary as
select
  s.id as student_id,
  s.student_code,
  s.name,
  s.grade_level,
  count(a.id) filter (where a.status in ('completed', 'needs_review')) as total_attempts_finished,
  coalesce(sum(a.total_items) filter (where a.status in ('completed', 'needs_review')), 0) as total_items_solved,
  coalesce(round(avg(a.first_score_percent) filter (where a.status in ('completed', 'needs_review')), 2), 0) as avg_first_score,
  coalesce(round(avg(a.final_score_percent) filter (where a.status in ('completed', 'needs_review')), 2), 0) as avg_final_score,
  count(a.id) filter (where a.status = 'needs_review') as attempts_needing_review
from auto_grading.students s
left join auto_grading.attempts a on a.student_id = s.id
group by s.id, s.student_code, s.name, s.grade_level;

-- =========================================================
-- 17. 최근 시험 기록 view
-- =========================================================
create or replace view auto_grading.v_student_attempt_history as
select
  a.id as attempt_id,
  a.student_id,
  s.student_code,
  s.name as student_name,
  a.test_set_id,
  ts.title as test_title,
  ts.subject,
  ts.major_unit,
  ts.minor_unit,
  a.attempt_no,
  a.status,
  a.first_correct_count,
  a.second_correct_count,
  a.final_correct_count,
  a.total_items,
  a.first_score_percent,
  a.final_score_percent,
  a.started_at,
  a.round1_submitted_at,
  a.round2_submitted_at,
  a.completed_at
from auto_grading.attempts a
join auto_grading.students s on s.id = a.student_id
join auto_grading.test_sets ts on ts.id = a.test_set_id;
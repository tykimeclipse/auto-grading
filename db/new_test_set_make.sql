-- test set id 생성하는 sql

insert into auto_grading.test_sets (
  title,
  source_type,
  source_name,
  original_filename,
  subject,
  grade_level,
  major_unit,
  minor_unit,
  default_choice_count,
  total_items,
  is_active
)
values (
  'TEST_auto_close_round1_1q',
  'csv_upload',
  'manual upload',
  'test_auto_close_round1_1q.csv',
  '과학',
  '중3',
  '테스트',
  '자동닫힘검증',
  5,
  1,
  true
)
returning *;
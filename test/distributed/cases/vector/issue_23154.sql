-- @suit
-- @case
-- @desc: regression test for issue #23154
-- @label:bvt
drop table if exists issue_23154;
create table issue_23154 (
    md5_id varchar(255) primary key,
    question_vector vecf64(3)
);

insert into issue_23154 values
    ('ref',   '[1,0,0]'),
    ('same1', '[1,0,0]'),
    ('same2', '[1,0,0]'),
    ('orth',  '[0,1,0]'),
    ('null',  null);

select count(*) as count,
       avg(cosine_similarity(
           question_vector,
           (select question_vector from issue_23154 where md5_id = 'ref'))) as avg_similarity,
       max(cosine_similarity(
           question_vector,
           (select question_vector from issue_23154 where md5_id = 'ref'))) as max_similarity,
       min(cosine_similarity(
           question_vector,
           (select question_vector from issue_23154 where md5_id = 'ref'))) as min_similarity
  from issue_23154
 where question_vector is not null
   and md5_id != 'ref'
   and cosine_similarity(
           question_vector,
           (select question_vector from issue_23154 where md5_id = 'ref')) >= 0.9;

drop table issue_23154;

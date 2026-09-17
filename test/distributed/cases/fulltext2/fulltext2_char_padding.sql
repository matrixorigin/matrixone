-- #29062: a predicate on a CHAR INCLUDE column must keep SQL trailing-space (pad-space) comparison
-- semantics. The index stores the actual bytes; the in-index evaluator now pad-space-compares CHAR
-- columns ('a' = 'a ' = 'a  '), while VARCHAR stays byte-exact. Every FULLTEXT2 result below must
-- equal the base-table result. Also covered: a CHAR primary-key predicate.
set experimental_fulltext2_index = 1;
drop database if exists ft2_char_padding;
create database ft2_char_padding;
use ft2_char_padding;

create table t(
  id bigint primary key,
  body text,
  ch char(4),
  vc varchar(4),
  note text
);
insert into t values
  (1, 'alpha document', 'a',    'a',   'n1'),
  (2, 'alpha document', 'a ',   'a ',  'n2'),
  (3, 'alpha document', 'a  ',  'a  ', 'n3'),
  (4, 'alpha document', 'abc',  'abc', 'n4'),
  (5, 'alpha document', 'abc ', 'abc ','n5'),
  (6, 'alpha document', '',     '',    'n6');

create fulltext2 index ft on t(body) include(ch, vc);
alter table t alter reindex ft fulltext2 force_sync;

-- base-table CHAR semantics: 'a' matches 'a','a ','a  ' -> 1,2,3
select id from t where body like '%alpha%' and ch = 'a' order by id;

-- FULLTEXT2 must match the base table: ch = 'a' -> 1,2,3
select id from t where match(body) against('+alpha' in boolean mode) and ch = 'a' order by id;
-- trailing spaces in the literal are also ignored -> still 1,2,3
select id from t where match(body) against('+alpha' in boolean mode) and ch = 'a ' order by id;
select id from t where match(body) against('+alpha' in boolean mode) and ch = 'a  ' order by id;
-- <> is the complement -> 4,5,6 (not 2,3,4,5,6)
select id from t where match(body) against('+alpha' in boolean mode) and ch <> 'a' order by id;
-- single-value IN -> 1,2,3
select id from t where match(body) against('+alpha' in boolean mode) and ch in ('a') order by id;
-- non-covered projection (base-table join) keeps the same row set -> 1,2,3
select id, ch, note from t where match(body) against('+alpha' in boolean mode) and ch = 'a' order by id;
-- control: RTRIM stays residual and returns the same -> 1,2,3
select id from t where match(body) against('+alpha' in boolean mode) and rtrim(ch) = 'a' order by id;

-- VARCHAR is byte-exact: 'a' matches only 'a' -> 1
select id from t where match(body) against('+alpha' in boolean mode) and vc = 'a' order by id;

-- the fulltext2 index is used (predicate pushed into the search), not a full scan.
-- @separator:table
-- @regex("fulltext2_search", true)
explain select id from t where match(body) against('+alpha' in boolean mode) and ch = 'a' order by id;

drop database ft2_char_padding;

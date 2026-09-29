-- #29062: a predicate on a CHAR INCLUDE column must match MO's ordinary CHAR comparison exactly
-- (func_compare.go / operator_between.go: bytes.Compare after TrimRight of ASCII space ' '). The
-- index stores the actual bytes; the in-index evaluator trims trailing ASCII spaces so
-- 'a' = 'a ' = 'a  ', while a trailing byte BELOW space (tab 0x09, backspace 0x08) is KEPT --
-- NOT the pad-space model where it would sort below the padding space. VARCHAR stays byte-exact.
-- Every FULLTEXT2 result below must equal the base-table result. Also covered: a CHAR primary key.
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
  (6, 'alpha document', '',     '',    'n6'),
  (7, 'alpha document', char(97,9), char(97,9), 'n7'),   -- ch = 'a' + TAB (0x09)
  (8, 'alpha document', char(97,8), char(97,8), 'n8');   -- ch = 'a' + BACKSPACE (0x08)

create fulltext2 index ft on t(body) include(ch, vc);
alter table t alter reindex ft fulltext2 force_sync;

-- base-table CHAR semantics: 'a' matches 'a','a ','a  ' -> 1,2,3
select id from t where body like '%alpha%' and ch = 'a' order by id;

-- FULLTEXT2 must match the base table: ch = 'a' -> 1,2,3
select id from t where match(body) against('+alpha' in boolean mode) and ch = 'a' order by id;
-- trailing spaces in the literal are also ignored -> still 1,2,3
select id from t where match(body) against('+alpha' in boolean mode) and ch = 'a ' order by id;
select id from t where match(body) against('+alpha' in boolean mode) and ch = 'a  ' order by id;
-- <> is the complement of '=' (tab/backspace rows differ from 'a') -> 4,5,6,7,8
select id from t where match(body) against('+alpha' in boolean mode) and ch <> 'a' order by id;
-- single-value IN -> 1,2,3
select id from t where match(body) against('+alpha' in boolean mode) and ch in ('a') order by id;
-- non-covered projection (base-table join) keeps the same row set -> 1,2,3
select id, ch, note from t where match(body) against('+alpha' in boolean mode) and ch = 'a' order by id;
-- control: RTRIM stays residual and returns the same -> 1,2,3
select id from t where match(body) against('+alpha' in boolean mode) and rtrim(ch) = 'a' order by id;

-- VARCHAR is byte-exact: 'a' matches only 'a' -> 1
select id from t where match(body) against('+alpha' in boolean mode) and vc = 'a' order by id;

-- #29062 P1 differential: a trailing byte BELOW ASCII space (tab 0x09, backspace 0x08) is KEPT,
-- exactly like MO's ordinary CHAR path. Each FULLTEXT2 query below is paired with its base-table
-- twin; the two must return the same rows. A pad-space model would diverge (it sorts 0x09/0x08
-- below the padding space), so these guard against a regression back to padding.

-- ch < 'a': trimmed 'a\t','a\b' are > 'a' (byte kept) -> only '' (id 6) qualifies. base:
select id from t where body like '%alpha%' and ch < 'a' order by id;
-- ...and FULLTEXT2 must agree -> 6 (a pad model would wrongly add 7,8)
select id from t where match(body) against('+alpha' in boolean mode) and ch < 'a' order by id;

-- ch between 'a' and 'b': every non-empty ch trims to something in [a,b] -- incl. 'abc'/'abc '
-- (< 'b') and the tab/backspace rows -> 1,2,3,4,5,7,8. base:
select id from t where body like '%alpha%' and ch between 'a' and 'b' order by id;
-- ...and FULLTEXT2 must agree -> 1,2,3,4,5,7,8 (a pad model would wrongly drop 7,8)
select id from t where match(body) against('+alpha' in boolean mode) and ch between 'a' and 'b' order by id;

-- ch = 'a': the tab/backspace rows are NOT equal to 'a' (byte kept) -> 1,2,3
select id from t where match(body) against('+alpha' in boolean mode) and ch = 'a' order by id;

-- the fulltext2 index is used (predicate pushed into the search), not a full scan.
-- @separator:table
-- @regex("fulltext2_search", true)
explain select id from t where match(body) against('+alpha' in boolean mode) and ch = 'a' order by id;

drop database ft2_char_padding;

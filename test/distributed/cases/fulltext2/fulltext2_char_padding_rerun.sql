-- #29062 P2: same-instance repeatability + teardown proof for the CHAR INCLUDE case.
-- The fulltext2_char_padding case opens with `drop database if exists`, whose entry-guard could
-- silently mask residue left by a failed teardown -- so a single run does not prove cleanup. This
-- case runs the SAME scenario twice on one instance: PASS 1 ends with an explicit `drop database`,
-- then PASS 2 does a BARE `create database` (no `if exists`). If PASS 1's teardown left the schema
-- behind, that bare create fails with "database already exists" -- so PASS 2 succeeding IS the
-- proof that the drop cleaned up, and both passes returning identical rows is the repeatability.
set experimental_fulltext2_index = 1;
drop database if exists ft2_char_padding_rerun;

-- ===== PASS 1 =====
create database ft2_char_padding_rerun;
use ft2_char_padding_rerun;

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
  (7, 'alpha document', char(97,9), char(97,9), 'n7'),
  (8, 'alpha document', char(97,8), char(97,8), 'n8');

create fulltext2 index ft on t(body) include(ch, vc);
alter table t alter reindex ft fulltext2 force_sync;

select id from t where body like '%alpha%' and ch = 'a' order by id;
select id from t where match(body) against('+alpha' in boolean mode) and ch = 'a' order by id;
select id from t where match(body) against('+alpha' in boolean mode) and ch <> 'a' order by id;
select id from t where match(body) against('+alpha' in boolean mode) and vc = 'a' order by id;
select id from t where match(body) against('+alpha' in boolean mode) and ch < 'a' order by id;
select id from t where match(body) against('+alpha' in boolean mode) and ch between 'a' and 'b' order by id;

drop database ft2_char_padding_rerun;

-- ===== PASS 2 (same instance) -- BARE create: fails if PASS 1 did not clean up =====
create database ft2_char_padding_rerun;
use ft2_char_padding_rerun;

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
  (7, 'alpha document', char(97,9), char(97,9), 'n7'),
  (8, 'alpha document', char(97,8), char(97,8), 'n8');

create fulltext2 index ft on t(body) include(ch, vc);
alter table t alter reindex ft fulltext2 force_sync;

select id from t where body like '%alpha%' and ch = 'a' order by id;
select id from t where match(body) against('+alpha' in boolean mode) and ch = 'a' order by id;
select id from t where match(body) against('+alpha' in boolean mode) and ch <> 'a' order by id;
select id from t where match(body) against('+alpha' in boolean mode) and vc = 'a' order by id;
select id from t where match(body) against('+alpha' in boolean mode) and ch < 'a' order by id;
select id from t where match(body) against('+alpha' in boolean mode) and ch between 'a' and 'b' order by id;

drop database ft2_char_padding_rerun;

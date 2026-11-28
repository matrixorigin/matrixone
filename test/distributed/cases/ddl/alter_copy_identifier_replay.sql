-- @suit
-- @case
-- @desc: ALGORITHM=COPY preserves exact CHECK names and apostrophe INCLUDE identities for IVFFLAT and FULLTEXT2; every apostrophe-bearing statement is PREPARE-assembled so each stays one logical command
-- @label:bvt

drop database if exists alter_copy_identifier_replay;
create database alter_copy_identifier_replay;
use alter_copy_identifier_replay;

-- An apostrophe CHECK name keeps its exact catalog identity through COPY.
set @s = concat('create table ck(id bigint primary key, v int, constraint ', char(96), 'c', char(39), 'k', char(96), ' check (v >= 0))');
prepare p1 from @s;
execute p1;
deallocate prepare p1;
insert into ck (id, v) values (1, 5);
select constraint_name, HEX(constraint_name) from information_schema.table_constraints where constraint_schema = database() and table_name = 'ck' and constraint_type = 'CHECK';
alter table ck algorithm=copy, add column marker int default 7 first;
select id, marker, v from ck order by id;
select constraint_name, HEX(constraint_name) from information_schema.table_constraints where constraint_schema = database() and table_name = 'ck' and constraint_type = 'CHECK';
show create table ck;
insert into ck (id, v) values (2, -1);
set @d = concat('alter table ck drop constraint ', char(96), 'c', char(39), 'k', char(96));
prepare p2 from @d;
execute p2;
deallocate prepare p2;
select count(*) from information_schema.table_constraints where constraint_schema = database() and table_name = 'ck' and constraint_type = 'CHECK';

-- Ordinary CHECK control keeps its name through the same COPY.
create table ck_ord(id bigint primary key, v int, constraint ck check (v >= 0));
insert into ck_ord (id, v) values (1, 5);
alter table ck_ord algorithm=copy, add column marker int default 7 first;
select constraint_name, HEX(constraint_name) from information_schema.table_constraints where constraint_schema = database() and table_name = 'ck_ord' and constraint_type = 'CHECK';

-- Embedded-backtick CHECK control: the identifier escaper still doubles backticks.
set @sbt = concat('create table ck_bt(id bigint primary key, v int, constraint ', char(96), 'w', char(96), char(96), 'q', char(96), ' check (v >= 0))');
prepare p3 from @sbt;
execute p3;
deallocate prepare p3;
insert into ck_bt (id, v) values (1, 5);
alter table ck_bt algorithm=copy, add column marker int default 7 first;
select constraint_name, HEX(constraint_name) from information_schema.table_constraints where constraint_schema = database() and table_name = 'ck_bt' and constraint_type = 'CHECK';
show create table ck_bt;

-- IVFFLAT INCLUDE with an apostrophe column survives COPY with exact identity.
-- The l2_distance BY-RANK covered route mirrors the maintained
-- vector_ivfflat_include_alter_table suite.
set @sv = concat('create table ivf_bad(id bigint primary key, v vecf32(3), ', char(96), 'pay', char(39), 'load', char(96), ' int)');
prepare p4 from @sv;
execute p4;
deallocate prepare p4;
insert into ivf_bad values (1, '[1,2,3]', 100), (2, '[1,2,4]', 200);
set @si = concat('create index iv using ivfflat on ivf_bad(v) lists=1 op_type ', char(39), 'vector_l2_ops', char(39), ' include(', char(96), 'pay', char(39), 'load', char(96), ')');
prepare p5 from @si;
execute p5;
deallocate prepare p5;
alter table ivf_bad algorithm=copy, add column marker int default 7 first;
set @sp = concat('select id, marker, ', char(96), 'pay', char(39), 'load', char(96), ' from ivf_bad order by id');
prepare p6 from @sp;
execute p6;
deallocate prepare p6;
show create table ivf_bad;
-- Real covering search after COPY: explicit mode=post keeps the rebuilt
-- vector index active; the projection selects the apostrophe INCLUDE column.
set @sp_post = concat('select id, ', char(96), 'pay', char(39), 'load', char(96), ' from ivf_bad order by l2_distance(v, ', char(39), '[1,2,3]', char(39), ') limit 2 by rank with option ', char(39), 'mode=post', char(39));
prepare s_post from @sp_post;
execute s_post;
deallocate prepare s_post;
-- Plan witness for the same covered query: the post plan must run the rebuilt
-- index and join back to the base table. Regex discriminators keep the golden
-- free of scheduling text and relation IDs.
set @se_post = concat('explain select id, ', char(96), 'pay', char(39), 'load', char(96), ' from ivf_bad order by l2_distance(v, ', char(39), '[1,2,3]', char(39), ') limit 2 by rank with option ', char(39), 'mode=post', char(39));
prepare e_post from @se_post;
-- @regex("Vector Index Scan on iv .l2_distance.", true)
-- @regex("Metric: l2_distance", true)
execute e_post;
deallocate prepare e_post;
-- Covering runtime on the index-carried payload: the covered filter is served
-- from the INCLUDE column in the rebuilt index (pre-family mode).
set @sp_cov = concat('select id, ', char(96), 'pay', char(39), 'load', char(96), ' from ivf_bad where ', char(96), 'pay', char(39), 'load', char(96), ' >= 150 order by l2_distance(v, ', char(39), '[1,2,3]', char(39), ') limit 2 by rank with option ', char(39), 'mode=iterative', char(39));
prepare s_cov from @sp_cov;
execute s_cov;
deallocate prepare s_cov;
-- Payload-mapping witness: the analyze plan shows the apostrophe INCLUDE
-- column carried by the rebuilt index and the filter bound to that payload.
set @se_map = concat('explain analyze verbose select id, ', char(96), 'pay', char(39), 'load', char(96), ' from ivf_bad where ', char(96), 'pay', char(39), 'load', char(96), ' >= 100 order by l2_distance(v, ', char(39), '[1,2,3]', char(39), ') limit 2 by rank with option ', char(39), 'mode=iterative', char(39));
prepare e_map from @se_map;
-- @regex("__mo_index_include_pay.load", true)
-- @regex("__mo_index_include_pay.load >= 100", true)
execute e_map;
deallocate prepare e_map;
-- Exact-comparison control only: mode=force explicitly disables the vector
-- index and reads the base table; it must return the same ordered rows.
set @sp_force = concat('select id, ', char(96), 'pay', char(39), 'load', char(96), ' from ivf_bad order by l2_distance(v, ', char(39), '[1,2,3]', char(39), ') limit 2 by rank with option ', char(39), 'mode=force', char(39));
prepare s_force from @sp_force;
execute s_force;
deallocate prepare s_force;

-- Ordinary INCLUDE control through the same COPY.
create table ivf_ord(id bigint primary key, v vecf32(3), payload int);
insert into ivf_ord values (1, '[1,2,3]', 100), (2, '[1,2,4]', 200);
create index ivg using ivfflat on ivf_ord(v) lists=1 op_type 'vector_l2_ops' include(payload);
alter table ivf_ord algorithm=copy, add column marker int default 7 first;
select id, marker, payload from ivf_ord order by id;

-- FULLTEXT2 INCLUDE with an apostrophe column survives COPY. MATCH runs
-- through the rebuilt index once the replacement index's durable tag=0 base
-- exists (the maintained fulltext2 copy-alter readiness route). The projected
-- marker column is a base-table column outside the INCLUDE set, so this is a
-- readback-plus-readiness check, not an index-only claim.
set experimental_fulltext2_index = 1;
set @sf = concat('create table ft2_bad(id bigint primary key, body text, ', char(96), 'sta', char(39), 'tus', char(96), ' int)');
prepare p8 from @sf;
execute p8;
deallocate prepare p8;
insert into ft2_bad values (1, 'alpha beta word', 100), (2, 'gamma word delta', 200);
set @sft = concat('create fulltext2 index ftx on ft2_bad(body) include(', char(96), 'sta', char(39), 'tus', char(96), ') with parser gojieba');
prepare p9 from @sft;
execute p9;
deallocate prepare p9;
alter table ft2_bad algorithm=copy, add column marker int default 7 first;
select id, marker from ft2_bad order by id;
set @ft2_index = (select index_table_name from mo_catalog.mo_indexes where name = 'ftx' and algo = 'fulltext2' and algo_table_type = 'ftv2_index' and table_id in (select rel_id from mo_catalog.mo_tables where reldatabase = database() and relname = 'ft2_bad') limit 1);
set @wait_base_sql = concat('select count(*) > 0 as ready from `', database(), '`.`', @ft2_index, '` where tag = 0');
prepare wait_base from @wait_base_sql;
-- @wait_expect(1, 120)
execute wait_base;
deallocate prepare wait_base;
set @sm = concat('select id, marker, ', char(96), 'sta', char(39), 'tus', char(96), ' from ft2_bad where match(body) against(', char(39), 'word', char(39), ') order by id');
prepare p10 from @sm;
execute p10;
deallocate prepare p10;
show create table ft2_bad;

-- Ordinary FULLTEXT2 INCLUDE control through the same COPY.
create table ft2_ord(id bigint primary key, body text, status int);
insert into ft2_ord values (1, 'alpha beta word', 100), (2, 'gamma word delta', 200);
create fulltext2 index fto on ft2_ord(body) include(status) with parser gojieba;
alter table ft2_ord algorithm=copy, add column marker int default 7 first;
select id, marker, status from ft2_ord order by id;

drop database if exists alter_copy_identifier_replay;

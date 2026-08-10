drop table if exists prefix_idx_dml_sk;
create table prefix_idx_dml_sk(id int primary key, a varchar(32), b int, index idx_a(a(4)));
insert into prefix_idx_dml_sk values (1, 'abcdef-long', 10);
update prefix_idx_dml_sk set a = 'uvwxyz-long' where id = 1;
select id, a, b from prefix_idx_dml_sk where a = 'uvwxyz-long';
delete from prefix_idx_dml_sk where id = 1;
select count(*) from prefix_idx_dml_sk where a = 'uvwxyz-long';
drop table prefix_idx_dml_sk;

drop table if exists prefix_idx_dml_uk;
create table prefix_idx_dml_uk(id int primary key, a varchar(32), b int, unique index uq_a(a(4)));
insert into prefix_idx_dml_uk values (1, 'abcdef-long', 10);
-- @pattern
insert into prefix_idx_dml_uk values (2, 'abcdzz-conflict', 20);
update prefix_idx_dml_uk set a = 'uvwxyz-long' where id = 1;
insert into prefix_idx_dml_uk values (2, 'abcdef-new', 20);
delete from prefix_idx_dml_uk where id = 1;
insert into prefix_idx_dml_uk values (3, 'uvwxyz-new', 30);
select id, a, b from prefix_idx_dml_uk order by id;
select a from prefix_idx_dml_uk force index(uq_a) where a = 'abcdef-new';
drop table prefix_idx_dml_uk;

drop table if exists prefix_idx_dml_create_backfill;
create table prefix_idx_dml_create_backfill(id int primary key, a varchar(32), b int);
insert into prefix_idx_dml_create_backfill values (1, 'abcdef-long', 10), (2, 'lmnopq-long', 20);
create index idx_a on prefix_idx_dml_create_backfill(a(4));
update prefix_idx_dml_create_backfill set a = 'uvwxyz-long' where id = 1;
select id, a, b from prefix_idx_dml_create_backfill where a = 'uvwxyz-long';
delete from prefix_idx_dml_create_backfill where id = 2;
select count(*) from prefix_idx_dml_create_backfill where a = 'lmnopq-long';
drop table prefix_idx_dml_create_backfill;

drop table if exists prefix_idx_dml_alter_add;
create table prefix_idx_dml_alter_add(id int primary key, a varchar(32), b int);
insert into prefix_idx_dml_alter_add values (1, 'abcdef-long', 10), (2, 'lmnopq-long', 20);
alter table prefix_idx_dml_alter_add add index idx_a(a(4));
update prefix_idx_dml_alter_add set a = 'uvwxyz-long' where id = 1;
select id, a, b from prefix_idx_dml_alter_add where a = 'uvwxyz-long';
delete from prefix_idx_dml_alter_add where id = 2;
select count(*) from prefix_idx_dml_alter_add where a = 'lmnopq-long';
drop table prefix_idx_dml_alter_add;

-- Regression for #26813: a prefix index cannot reconstruct a full value for
-- an index-only scan. Exercise the prefix boundary and keep a lookup control.
drop table if exists prefix_idx_covering_scan;
create table prefix_idx_covering_scan(id int primary key, a varchar(32), b int, index idx_a(a(4)));
insert into prefix_idx_covering_scan values
    (1, 'abc', 10),
    (2, 'abcd', 20),
    (3, 'abcdx', 30),
    (4, 'abcdy', 40),
    (5, '中中中中甲', 50),
    (6, '中中中中乙', 60);
select count(*) from prefix_idx_covering_scan force index(idx_a) where a = 'abc';
select count(*) from prefix_idx_covering_scan ignore index(idx_a) where a = 'abc';
select count(*) from prefix_idx_covering_scan force index(idx_a) where a = 'abcd';
select count(*) from prefix_idx_covering_scan ignore index(idx_a) where a = 'abcd';
select count(*) from prefix_idx_covering_scan force index(idx_a) where a = 'abcdx';
select count(*) from prefix_idx_covering_scan ignore index(idx_a) where a = 'abcdx';
select a from prefix_idx_covering_scan force index(idx_a) where a = 'abcdx';
select a from prefix_idx_covering_scan ignore index(idx_a) where a = 'abcdx';
select a from prefix_idx_covering_scan force index(idx_a) where a = '中中中中甲';
select a from prefix_idx_covering_scan ignore index(idx_a) where a = '中中中中甲';
select id, a, b from prefix_idx_covering_scan force index(idx_a) where a = 'abcdx';
select mo_ctl('dn', 'flush', 'prefix_index_dml.prefix_idx_covering_scan');
select a from prefix_idx_covering_scan force index(idx_a) where a = 'abcdx';
select a from prefix_idx_covering_scan force index(idx_a) where a = '中中中中甲';
drop table prefix_idx_covering_scan;

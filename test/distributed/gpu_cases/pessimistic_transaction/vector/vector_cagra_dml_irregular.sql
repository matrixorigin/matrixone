-- =====================================================================
-- vector_cagra_dml_irregular.sql — DML planned by the new DML planner on a
-- table with a CAGRA index (#21790 / #21791 / #33108)
--
-- GPU REQUIRED. CAGRA is maintained by ISCP CDC, so DELETE and PK UPDATE only
-- write the source table; the sync FULLTEXT index on the same table is
-- maintained inside the statement. After each statement:
--   * the FULLTEXT hidden table has no orphan and no missing primary key;
--   * after CDC catch-up, an exact-match probe returns the expected id.
--
-- Data: id=i -> [10*2^(i-1)]*8, so a deleted row has a unique nearest
-- survivor (its lower neighbour). Only top-1 probes are asserted.
-- =====================================================================

SET experimental_cagra_index = 1;
SET experimental_fulltext_index = 1;
SET cagra_threads_build = 7;
SET cagra_max_index_capacity = 99999;
select mo_ctl('cn', 'SetVectorIndexFreshnessInterval', '1s') not like '%failed=[%' as all_cn_updated;

drop database if exists cagra_dml_irregular;
create database cagra_dml_irregular;
use cagra_dml_irregular;

create table t (id bigint primary key, body text, v vecf32(8));
insert into t values
    (1, 'apple one', '[10,10,10,10,10,10,10,10]'),
    (2, 'banana two', '[20,20,20,20,20,20,20,20]'),
    (3, 'apple three', '[40,40,40,40,40,40,40,40]'),
    (4, 'cherry four', '[80,80,80,80,80,80,80,80]'),
    (5, 'apple five', '[160,160,160,160,160,160,160,160]'),
    (6, 'banana six', '[320,320,320,320,320,320,320,320]'),
    (7, 'apple seven', '[640,640,640,640,640,640,640,640]'),
    (8, 'cherry eight', '[1280,1280,1280,1280,1280,1280,1280,1280]'),
    (9, 'durian nine', '[2560,2560,2560,2560,2560,2560,2560,2560]'),
    (10, 'apple ten', '[5120,5120,5120,5120,5120,5120,5120,5120]');
create fulltext index ft_body on t(body);
create index ix using cagra on t (v)
    op_type 'vector_l2_ops' intermediate_graph_degree=8 graph_degree=4;

set @t_ft = (select index_table_name from mo_catalog.mo_indexes
    where name = 'ft_body' and algo = 'fulltext'
      and table_id in (select rel_id from mo_catalog.mo_tables where reldatabase = database() and relname = 't')
    limit 1);
set @check_sql = concat(
    'select ',
    '(select count(distinct doc_id) from `', database(), '`.`', @t_ft, '` where doc_id not in (select id from t)) as ft_orphan, ',
    '(select count(*) from t where id not in (select doc_id from `', database(), '`.`', @t_ft, '`)) as ft_missing'
);
prepare check_t from @check_sql;

-- the base sub-index is built
set @t_store = (select index_table_name from mo_catalog.mo_indexes
    where name = 'ix' and algo_table_type = 'cagra_index'
      and table_id in (select rel_id from mo_catalog.mo_tables where reldatabase = database() and relname = 't')
    limit 1);
set @base_sql = concat('select count(*) > 0 as has_base from `', database(), '`.`', @t_store, '` where tag = 0');
prepare base_t from @base_sql;
execute base_t;

-- @metacmp(false)
-- @wait_expect(1, 120)
select id from t order by l2_distance(v, '[160,160,160,160,160,160,160,160]') limit 1;
execute check_t;

-- point delete on the primary key: id 5 -> nearest survivor id 4
delete from t where id = 5;
execute check_t;
-- @metacmp(false)
-- @wait_expect(1, 120)
select id from t order by l2_distance(v, '[160,160,160,160,160,160,160,160]') limit 1;

-- non-key predicate: id 9 -> nearest survivor id 8
delete from t where body like '%durian%';
execute check_t;
-- @metacmp(false)
-- @wait_expect(1, 120)
select id from t order by l2_distance(v, '[2560,2560,2560,2560,2560,2560,2560,2560]') limit 1;

-- single-target join: id 7 -> nearest survivor id 6
create table doomed (id bigint primary key);
insert into doomed values (7), (42);
delete t from t join doomed d on t.id = d.id;
execute check_t;
-- @metacmp(false)
-- @wait_expect(1, 120)
select id from t order by l2_distance(v, '[640,640,640,640,640,640,640,640]') limit 1;

-- multi-table delete: id 2 -> nearest survivor id 1
create table m (id bigint primary key);
insert into m values (2);
delete t, m from t join m on t.id = m.id;
execute check_t;
select count(*) from m;
-- @metacmp(false)
-- @wait_expect(1, 120)
select id from t order by l2_distance(v, '[20,20,20,20,20,20,20,20]') limit 1;

-- primary-key update: the vector of id 4 is now under id 40
update t set id = 40 where id = 4;
execute check_t;
select id, body from t where match(body) against('cherry') order by id;
-- @metacmp(false)
-- @wait_expect(1, 120)
select id from t order by l2_distance(v, '[80,80,80,80,80,80,80,80]') limit 1;

-- a rolled-back DELETE leaves the index untouched
begin;
delete from t where id = 10;
rollback;
execute check_t;
-- @metacmp(false)
-- @wait_expect(1, 120)
select id from t order by l2_distance(v, '[5120,5120,5120,5120,5120,5120,5120,5120]') limit 1;

-- delete and reinsert the same key with a new vector in one transaction
begin;
delete from t where id = 8;
insert into t values (8, 'grape eight', '[7000,7000,7000,7000,7000,7000,7000,7000]');
commit;
execute check_t;
select id from t where match(body) against('grape');
-- @metacmp(false)
-- @wait_expect(1, 120)
select id from t order by l2_distance(v, '[7000,7000,7000,7000,7000,7000,7000,7000]') limit 1;
-- @metacmp(false)
-- @wait_expect(1, 120)
select id from t order by l2_distance(v, '[1280,1280,1280,1280,1280,1280,1280,1280]') limit 1;

select id from t order by id;

-- in-place delete of every row, then the index serves new rows
delete from t where true;
execute check_t;
select count(*) from t;
insert into t values (11, 'apple again', '[33,33,33,33,33,33,33,33]');
execute check_t;
-- @metacmp(false)
-- @wait_expect(1, 120)
select id from t order by l2_distance(v, '[33,33,33,33,33,33,33,33]') limit 1;

deallocate prepare check_t;
deallocate prepare base_t;

-- foreign key actions on a parent whose child has a CAGRA index
create table p (id int primary key);
insert into p values (1),(2),(3),(4),(5),(6),(7),(8),(9),(10);
create table c (
    id bigint primary key,
    pid int,
    v vecf32(8),
    foreign key (pid) references p(id) on delete cascade on update cascade
);
insert into c values
    (1, 1, '[10,10,10,10,10,10,10,10]'),
    (2, 2, '[20,20,20,20,20,20,20,20]'),
    (3, 3, '[40,40,40,40,40,40,40,40]'),
    (4, 4, '[80,80,80,80,80,80,80,80]'),
    (5, 5, '[160,160,160,160,160,160,160,160]'),
    (6, 6, '[320,320,320,320,320,320,320,320]'),
    (7, 7, '[640,640,640,640,640,640,640,640]'),
    (8, 8, '[1280,1280,1280,1280,1280,1280,1280,1280]'),
    (9, 9, '[2560,2560,2560,2560,2560,2560,2560,2560]'),
    (10, 10, '[5120,5120,5120,5120,5120,5120,5120,5120]');
create index cx using cagra on c (v)
    op_type 'vector_l2_ops' intermediate_graph_degree=8 graph_degree=4;

-- DELETE on the parent cascades: c id 5 -> nearest survivor id 4
delete from p where id = 5;
select count(*) from c;
-- @metacmp(false)
-- @wait_expect(1, 120)
select id from c order by l2_distance(v, '[160,160,160,160,160,160,160,160]') limit 1;

-- UPDATE on the parent cascades to the child's foreign key column
update p set id = 100 where id = 3;
select id, pid from c where id = 3;
-- @metacmp(false)
-- @wait_expect(1, 120)
select id from c order by l2_distance(v, '[40,40,40,40,40,40,40,40]') limit 1;

-- REPLACE on the parent deletes the old parent row and cascades: c id 7 -> id 6
replace into p values (7);
select count(*) from c;
-- @metacmp(false)
-- @wait_expect(1, 120)
select id from c order by l2_distance(v, '[640,640,640,640,640,640,640,640]') limit 1;

set @freshness_restore = mo_ctl('cn', 'SetVectorIndexFreshnessInterval', '0');
SET experimental_fulltext_index = 0;
drop database cagra_dml_irregular;

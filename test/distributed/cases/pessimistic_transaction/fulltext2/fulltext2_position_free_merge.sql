-- fulltext2 POSITION_FREE through the CDC lifecycle: a position-free index (gojieba,
-- bag-of-words only) must STAY position-free across post-create INSERT/DELETE/UPDATE
-- (the tag=1 CdcTail is built position-free — slice 2b-tail), a REINDEX MERGE that folds
-- the tail into the tag=0 base (reconstructs from tf, no positions — slice 2b-compact),
-- and a REINDEX REBUILD from source. IN BM25 MODE reflects the mutations at each step;
-- BOOLEAN phrase stays rejected (no positions). Durable tail advancement gates CDC checks.
set experimental_fulltext2_index = 1;
drop database if exists ft2_posfree_cdc;
create database ft2_posfree_cdc;
use ft2_posfree_cdc;

create table t (id bigint primary key, body text);
insert into t values (1,'我家有三个人'),(2,'三个人都住在我家'),(3,'我家的花园很漂亮');
create fulltext2 index ft on t(body) with parser gojieba position_free = true;
set @t_ft2 = (select index_table_name from mo_catalog.mo_indexes where name = 'ft' and algo_table_type = 'ftv2_index' and table_id in (select rel_id from mo_catalog.mo_tables where reldatabase = database() and relname = 't') limit 1);
set @capture_t_tail_sql = concat(
    'set @t_tail_before_mutation = (select coalesce(max(chunk_id), -1) from `', database(), '`.`', @t_ft2,
    '` where index_id = ''cdc_tail'' and tag = 1)'
);
prepare capture_t_tail from @capture_t_tail_sql;
execute capture_t_tail;
deallocate prepare capture_t_tail;

-- post-create INSERT/DELETE flow through the CDC tail (built position-free)
insert into t values (4,'教室里有三个人'),(5,'昨天我家有三个人来吃饭');
set @wait_t_mutation_sql = concat(
    'select coalesce(max(chunk_id), -1) > ', @t_tail_before_mutation,
    ' as t_insert_ready from `', database(), '`.`', @t_ft2,
    '` where index_id = ''cdc_tail'' and tag = 1'
);
prepare wait_t_mutation from @wait_t_mutation_sql;
-- @wait_expect(2, 120)
execute wait_t_mutation;
deallocate prepare wait_t_mutation;
set @capture_t_tail_sql = concat(
    'set @t_tail_before_mutation = (select coalesce(max(chunk_id), -1) from `', database(), '`.`', @t_ft2,
    '` where index_id = ''cdc_tail'' and tag = 1)'
);
prepare capture_t_tail from @capture_t_tail_sql;
execute capture_t_tail;
deallocate prepare capture_t_tail;
delete from t where id = 3;
set @wait_t_mutation_sql = concat(
    'select coalesce(max(chunk_id), -1) > ', @t_tail_before_mutation,
    ' as t_delete_ready from `', database(), '`.`', @t_ft2,
    '` where index_id = ''cdc_tail'' and tag = 1'
);
prepare wait_t_mutation from @wait_t_mutation_sql;
-- @wait_expect(2, 120)
execute wait_t_mutation;
deallocate prepare wait_t_mutation;

-- IN BM25 MODE reflects the mutations: 我家 now in docs 1,2,5 (doc 3 deleted); 教室 in doc 4
select id from t where match(body) against('我家' in bm25 mode) order by id;
select id from t where match(body) against('教室' in bm25 mode) order by id;

-- REINDEX MERGE: fold the tag=1 tail into the tag=0 base, staying position-free
alter table t alter reindex ft fulltext2 merge;
select algo_params from mo_catalog.mo_indexes where name = 'ft' and algo_params like '%parser%' limit 1;
select id from t where match(body) against('我家' in bm25 mode) order by id;
-- still position-free: BOOLEAN phrase mode is rejected
select id from t where match(body) against('我家' in boolean mode) order by id;

-- more mutations, then REINDEX REBUILD (rebuild the base from source, stays position-free)
set @capture_t_tail_sql = concat(
    'set @t_tail_before_mutation = (select coalesce(max(chunk_id), -1) from `', database(), '`.`', @t_ft2,
    '` where index_id = ''cdc_tail'' and tag = 1)'
);
prepare capture_t_tail from @capture_t_tail_sql;
execute capture_t_tail;
deallocate prepare capture_t_tail;
insert into t values (6,'我家门口有三个人');
set @wait_t_mutation_sql = concat(
    'select coalesce(max(chunk_id), -1) > ', @t_tail_before_mutation,
    ' as t_insert_ready from `', database(), '`.`', @t_ft2,
    '` where index_id = ''cdc_tail'' and tag = 1'
);
prepare wait_t_mutation from @wait_t_mutation_sql;
-- @wait_expect(2, 120)
execute wait_t_mutation;
deallocate prepare wait_t_mutation;
set @capture_t_tail_sql = concat(
    'set @t_tail_before_mutation = (select coalesce(max(chunk_id), -1) from `', database(), '`.`', @t_ft2,
    '` where index_id = ''cdc_tail'' and tag = 1)'
);
prepare capture_t_tail from @capture_t_tail_sql;
execute capture_t_tail;
deallocate prepare capture_t_tail;
update t set body = '教室里没有人' where id = 4;
set @wait_t_mutation_sql = concat(
    'select coalesce(max(chunk_id), -1) > ', @t_tail_before_mutation,
    ' as t_update_ready from `', database(), '`.`', @t_ft2,
    '` where index_id = ''cdc_tail'' and tag = 1'
);
prepare wait_t_mutation from @wait_t_mutation_sql;
-- @wait_expect(2, 120)
execute wait_t_mutation;
deallocate prepare wait_t_mutation;
alter table t alter reindex ft fulltext2;
select algo_params from mo_catalog.mo_indexes where name = 'ft' and algo_params like '%parser%' limit 1;
select id from t where match(body) against('我家' in bm25 mode) order by id;
select id from t where match(body) against('三个人' in bm25 mode) order by id;
select id from t where match(body) against('我家' in boolean mode) order by id;

drop database ft2_posfree_cdc;

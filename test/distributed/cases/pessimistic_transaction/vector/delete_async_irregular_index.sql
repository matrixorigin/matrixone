-- #21790 / #21791: DELETE on tables whose irregular indexes are maintained
-- asynchronously by ISCP CDC (async IVF_FLAT, FULLTEXT2, HNSW). DML only writes
-- the source table; after CDC catches up, every index must reflect the live rows.

set experimental_fulltext2_index = 1;
set experimental_ivf_index = 1;
set experimental_hnsw_index = 1;
set hnsw_threads_build = 1;
select mo_ctl('cn', 'SetVectorIndexFreshnessInterval', '1s') not like '%failed=[%' as all_cn_updated;

drop database if exists dml_irregular_async;
create database dml_irregular_async;
use dml_irregular_async;

create table t (
    id bigint primary key,
    grp int,
    body text,
    v vecf32(3),
    w vecf32(3)
);
create fulltext2 index ft2_body on t(body);
create index hv using hnsw on t(v) op_type 'vector_l2_ops';
create index ivf_w using ivfflat on t(w) lists = 2 op_type 'vector_l2_ops' ASYNC;
insert into t values
    (1, 1, 'apple banana', '[1,0,0]', '[1,0,0]'),
    (2, 1, 'apple cherry', '[2,0,0]', '[2,0,0]'),
    (3, 1, 'banana cherry', '[3,0,0]', '[3,0,0]'),
    (4, 2, 'apple durian', '[0,1,0]', '[0,1,0]'),
    (5, 2, 'cherry durian', '[0,2,0]', '[0,2,0]'),
    (6, 2, 'apple elder', '[0,3,0]', '[0,3,0]'),
    (7, 3, 'banana elder', '[0,0,1]', '[0,0,1]'),
    (8, 3, 'cherry fig', '[0,0,2]', '[0,0,2]'),
    (9, 3, 'apple fig', '[0,0,3]', '[0,0,3]'),
    (10, 4, 'durian fig', '[5,5,5]', '[5,5,5]');

set @t_ft2 = (select index_table_name from mo_catalog.mo_indexes
    where name = 'ft2_body' and algo = 'fulltext2' and algo_table_type = 'ftv2_index'
      and table_id in (select rel_id from mo_catalog.mo_tables where reldatabase = database() and relname = 't')
    limit 1);
set @t_hm = (select index_table_name from mo_catalog.mo_indexes
    where name = 'hv' and algo = 'hnsw' and algo_table_type = 'hnsw_meta'
      and table_id in (select rel_id from mo_catalog.mo_tables where reldatabase = database() and relname = 't')
    limit 1);
set @t_ivf = (select index_table_name from mo_catalog.mo_indexes
    where name = 'ivf_w' and algo = 'ivfflat' and algo_table_type = 'entries'
      and table_id in (select rel_id from mo_catalog.mo_tables where reldatabase = database() and relname = 't')
    limit 1);

-- Ready once all three CDC consumers reflect the live rows:
--   fulltext2: a cdc_tail chunk newer than @ft2_before exists;
--   hnsw: the published model counts exactly the live rows;
--   ivfflat: the entries table has no orphan and no missing primary key.
set @ft2_before = -1;
set @wait_sql = concat(
    'select ',
    '(select coalesce(max(chunk_id), -1) from `', database(), '`.`', @t_ft2, '` where index_id = ''cdc_tail'' and tag = 1) > @ft2_before as ft2_ready, ',
    '(select coalesce(sum(nrow), 0) from `', database(), '`.`', @t_hm, '`) = (select count(*) from t) as hnsw_ready, ',
    '(select count(*) from `', database(), '`.`', @t_ivf, '` e where e.__mo_index_centroid_fk_version = ',
    '(select max(__mo_index_centroid_fk_version) from `', database(), '`.`', @t_ivf, '`) and e.__mo_index_pri_col not in (select id from t)) = 0 ',
    'and (select count(*) from t where w is not null and id not in (select __mo_index_pri_col from `', database(), '`.`', @t_ivf, '`)) = 0 as ivf_ready'
);
prepare wait_t from @wait_sql;
set @capture_sql = concat('select coalesce(max(chunk_id), -1) from `', database(), '`.`', @t_ft2, '` where index_id = ''cdc_tail'' and tag = 1 into @ft2_before');
prepare capture_t from @capture_sql;

-- index-served answers compared with the source table
set @serve_sql = concat(
    'select ',
    '(select count(*) from t where match(body) against(''apple'')) = (select count(*) from t where body like ''%apple%'') as ft2_ok, ',
    '(select count(*) from (select id from t order by l2_distance(v, ''[0,0,3]'') limit 100) q) = (select count(*) from t) as hnsw_ok, ',
    '(select count(*) from (select id from t order by l2_distance(w, ''[0,0,3]'') limit 100) q) = (select count(*) from t) as ivf_ok'
);
prepare serve_t from @serve_sql;

set @saved_probe_limit = @@probe_limit;
set probe_limit = 2;

-- @metacmp(false)
-- @wait_expect(1, 120)
execute wait_t;
-- @metacmp(false)
-- @wait_expect(1, 120)
execute serve_t;

-- point delete on the primary key, non-key predicate, and a single-target join
execute capture_t;
delete from t where id = 3;
delete from t where grp = 2 and body like '%durian%';
create table doomed (id bigint primary key);
insert into doomed values (1), (42);
delete t from t join doomed d on t.id = d.id;
-- @metacmp(false)
-- @wait_expect(1, 120)
execute wait_t;
-- @metacmp(false)
-- @wait_expect(1, 120)
execute serve_t;
select id from t order by id;

-- a rolled-back DELETE leaves the indexes untouched
begin;
delete from t where id = 7;
rollback;
select count(*) from t;

-- delete and reinsert the same key in one transaction
execute capture_t;
begin;
delete from t where id = 8;
insert into t values (8, 3, 'grape reborn', '[0,0,9]', '[0,0,9]');
commit;
-- @metacmp(false)
-- @wait_expect(1, 120)
execute wait_t;
-- @metacmp(false)
-- @wait_expect(1, 120)
select count(*) from t where match(body) against('grape');

-- in-place delete of every row (WHERE TRUE is not rewritten to TRUNCATE)
execute capture_t;
delete from t where true;
-- @metacmp(false)
-- @wait_expect(1, 120)
execute wait_t;
select count(*) from t;

-- the indexes keep working after the table is emptied
execute capture_t;
insert into t values (11, 1, 'apple again', '[1,1,1]', '[1,1,1]');
-- @metacmp(false)
-- @wait_expect(1, 120)
execute wait_t;
-- @metacmp(false)
-- @wait_expect(1, 120)
execute serve_t;

deallocate prepare wait_t;
deallocate prepare capture_t;
deallocate prepare serve_t;

set probe_limit = @saved_probe_limit;
set @freshness_restore = mo_ctl('cn', 'SetVectorIndexFreshnessInterval', '0');
set experimental_fulltext2_index = 0;
set experimental_ivf_index = 0;
set experimental_hnsw_index = 0;
drop database dml_irregular_async;

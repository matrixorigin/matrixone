-- #21790 / #21791: DELETE and PK UPDATE on tables whose irregular indexes are
-- maintained inside the statement (sync IVF_FLAT, classic FULLTEXT, MASTER).
-- After every statement, each hidden index table must reference exactly the
-- live source primary keys: no orphan entries and no missing entries.

set experimental_fulltext_index = 1;
set experimental_ivf_index = 1;

drop database if exists dml_irregular_sync;
create database dml_irregular_sync;
use dml_irregular_sync;

create table t (
    id bigint primary key,
    grp int,
    body text,
    a varchar(30),
    b varchar(30),
    v vecf32(3)
);
insert into t values
    (1, 1, 'apple banana', 'a1', 'b1', '[1,0,0]'),
    (2, 1, 'apple cherry', 'a2', 'b2', '[2,0,0]'),
    (3, 1, 'banana cherry', 'a3', 'b3', '[3,0,0]'),
    (4, 2, 'apple durian', 'a4', 'b4', '[0,1,0]'),
    (5, 2, 'cherry durian', 'a5', 'b5', '[0,2,0]'),
    (6, 2, 'apple elder', 'a6', 'b6', '[0,3,0]'),
    (7, 3, 'banana elder', 'a7', 'b7', '[0,0,1]'),
    (8, 3, 'cherry fig', 'a8', 'b8', '[0,0,2]'),
    (9, 3, 'apple fig', 'a9', 'b9', '[0,0,3]'),
    (10, 4, 'durian fig', 'a10', 'b10', '[5,5,5]');
create fulltext index ft_body on t(body);
create index ivf_v using ivfflat on t(v) lists = 2 op_type 'vector_l2_ops';
create index ms_ab using master on t(a, b);

set @t_ft = (select index_table_name from mo_catalog.mo_indexes
    where name = 'ft_body' and algo = 'fulltext'
      and table_id in (select rel_id from mo_catalog.mo_tables where reldatabase = database() and relname = 't')
    limit 1);
set @t_ivf = (select index_table_name from mo_catalog.mo_indexes
    where name = 'ivf_v' and algo = 'ivfflat' and algo_table_type = 'entries'
      and table_id in (select rel_id from mo_catalog.mo_tables where reldatabase = database() and relname = 't')
    limit 1);
set @t_ms = (select index_table_name from mo_catalog.mo_indexes
    where name = 'ms_ab' and algo = 'master'
      and table_id in (select rel_id from mo_catalog.mo_tables where reldatabase = database() and relname = 't')
    limit 1);

-- One row per index: index name, orphan entries, missing entries.
set @check_sql = concat(
    'select ''ft'' as idx, ',
    '(select count(distinct doc_id) from `', database(), '`.`', @t_ft, '` where doc_id not in (select id from t)) as orphan, ',
    '(select count(*) from t where id not in (select doc_id from `', database(), '`.`', @t_ft, '`)) as missing ',
    'union all select ''ivf'', ',
    '(select count(*) from `', database(), '`.`', @t_ivf, '` e where e.__mo_index_centroid_fk_version = ',
    '(select max(__mo_index_centroid_fk_version) from `', database(), '`.`', @t_ivf, '`) and e.__mo_index_pri_col not in (select id from t)), ',
    '(select count(*) from t where v is not null and id not in (select __mo_index_pri_col from `', database(), '`.`', @t_ivf, '`)) ',
    'union all select ''ms'', ',
    '(select count(*) from `', database(), '`.`', @t_ms, '` where __mo_index_pri_col not in (select id from t)), ',
    '(select count(*) from t where id not in (select __mo_index_pri_col from `', database(), '`.`', @t_ms, '`)) ',
    'order by idx'
);
prepare check_t from @check_sql;

set @saved_probe_limit = @@probe_limit;
set probe_limit = 2;

execute check_t;

-- point delete on the primary key
delete from t where id = 3;
execute check_t;

-- non-key predicate
delete from t where grp = 2 and body like '%durian%';
execute check_t;

-- uncorrelated IN subquery
create table doomed (id bigint primary key);
insert into doomed values (1), (42);
delete from t where id in (select id from doomed);
execute check_t;

-- ORDER BY ... LIMIT
delete from t order by id desc limit 1;
execute check_t;

-- single-target DELETE with a join
delete t from t join doomed d on t.id = d.id + 1;
execute check_t;

-- index-backed reads see only live rows
select count(*) from t where match(body) against('apple');
select id from t order by l2_distance(v, '[0,0,3]') limit 2;
select id from t where a = 'a7' and b = 'b7';

-- explicit transaction: workspace insert + delete, then rollback
begin;
insert into t values (100, 9, 'apple workspace', 'a100', 'b100', '[9,9,9]');
delete from t where id = 100;
delete from t where id = 7;
rollback;
execute check_t;
select id from t order by id;

-- explicit transaction: delete and reinsert the same key, then commit
begin;
delete from t where id = 8;
insert into t values (8, 3, 'grape reborn', 'a8x', 'b8x', '[0,0,9]');
commit;
execute check_t;
select count(*) from t where match(body) against('grape');
select id from t where a = 'a8x' and b = 'b8x';

-- prepared DELETE
prepare del_t from 'delete from t where id = ?';
set @del_id = 9;
execute del_t using @del_id;
deallocate prepare del_t;
execute check_t;

-- in-place delete of every row (WHERE TRUE is not rewritten to TRUNCATE)
delete from t where true;
execute check_t;
select count(*) from t;

-- the indexes keep working after the table is emptied
insert into t values (11, 1, 'apple again', 'a11', 'b11', '[1,1,1]');
execute check_t;
select count(*) from t where match(body) against('apple');
select id from t order by l2_distance(v, '[1,1,1]') limit 1;

deallocate prepare check_t;

-- PK UPDATE: entries are deleted by the old key and rebuilt under the new key.
create table pk (id bigint primary key, body text, a varchar(10), b varchar(10), v vecf32(3));
insert into pk values (1, 'alpha one', 'x1', 'y1', '[1,0,0]'), (2, 'beta two', 'x2', 'y2', '[0,1,0]');
create fulltext index ft on pk(body);
create index ix using ivfflat on pk(v) lists = 1 op_type 'vector_l2_ops';
create index ms using master on pk(a, b);
set @pk_ft = (select index_table_name from mo_catalog.mo_indexes
    where name = 'ft' and algo = 'fulltext'
      and table_id in (select rel_id from mo_catalog.mo_tables where reldatabase = database() and relname = 'pk')
    limit 1);
set @pk_ivf = (select index_table_name from mo_catalog.mo_indexes
    where name = 'ix' and algo = 'ivfflat' and algo_table_type = 'entries'
      and table_id in (select rel_id from mo_catalog.mo_tables where reldatabase = database() and relname = 'pk')
    limit 1);
set @pk_ms = (select index_table_name from mo_catalog.mo_indexes
    where name = 'ms' and algo = 'master'
      and table_id in (select rel_id from mo_catalog.mo_tables where reldatabase = database() and relname = 'pk')
    limit 1);
set @check_pk_sql = concat(
    'select ''ft'' as idx, ',
    '(select count(distinct doc_id) from `', database(), '`.`', @pk_ft, '` where doc_id not in (select id from pk)) as orphan, ',
    '(select count(*) from pk where id not in (select doc_id from `', database(), '`.`', @pk_ft, '`)) as missing ',
    'union all select ''ivf'', ',
    '(select count(*) from `', database(), '`.`', @pk_ivf, '` where __mo_index_pri_col not in (select id from pk)), ',
    '(select count(*) from pk where id not in (select __mo_index_pri_col from `', database(), '`.`', @pk_ivf, '`)) ',
    'union all select ''ms'', ',
    '(select count(*) from `', database(), '`.`', @pk_ms, '` where __mo_index_pri_col not in (select id from pk)), ',
    '(select count(*) from pk where id not in (select __mo_index_pri_col from `', database(), '`.`', @pk_ms, '`)) ',
    'order by idx'
);
prepare check_pk from @check_pk_sql;
update pk set id = 5 where id = 1;
execute check_pk;
-- swap keys inside one statement
update pk set id = 7 - id;
execute check_pk;
select id from pk where match(body) against('alpha');
select id from pk order by l2_distance(v, '[1,0,0]') limit 1;
select id from pk where a = 'x1' and b = 'y1';
-- a rolled-back PK UPDATE leaves the indexes untouched
begin;
update pk set id = 100 where id = 2;
rollback;
execute check_pk;
-- PK and indexed values change together
update pk set v = if(id = 2, '[0,0,1]', '[0,0,9]'), id = id + 10, body = concat(body, ' gamma');
execute check_pk;
select id from pk where match(body) against('gamma') order by id;
select id from pk order by l2_distance(v, '[0,0,1]') limit 1;
deallocate prepare check_pk;

-- composite primary key: hidden tables store the serialized key
create table ck (k1 varchar(10), k2 int, body text, a varchar(10), b varchar(10), primary key (k1, k2));
insert into ck values ('p', 1, 'apple one', 'x1', 'y1'), ('p', 2, 'apple two', 'x2', 'y2'), ('q', 1, 'banana', 'x3', 'y3');
create fulltext index ft_ck on ck(body);
create index ms_ck using master on ck(a, b);
set @ck_ft = (select index_table_name from mo_catalog.mo_indexes
    where name = 'ft_ck' and algo = 'fulltext'
      and table_id in (select rel_id from mo_catalog.mo_tables where reldatabase = database() and relname = 'ck')
    limit 1);
set @ck_ms = (select index_table_name from mo_catalog.mo_indexes
    where name = 'ms_ck' and algo = 'master'
      and table_id in (select rel_id from mo_catalog.mo_tables where reldatabase = database() and relname = 'ck')
    limit 1);
set @check_ck_sql = concat(
    'select (select count(distinct doc_id) from `', database(), '`.`', @ck_ft, '` where doc_id not in (select serial(k1, k2) from ck)) as ft_orphan, ',
    '(select count(*) from `', database(), '`.`', @ck_ms, '` where __mo_index_pri_col not in (select serial(k1, k2) from ck)) as ms_orphan, ',
    '(select count(*) from ck) as live'
);
prepare check_ck from @check_ck_sql;
execute check_ck;
delete from ck where k1 = 'p' and k2 = 1;
execute check_ck;
delete from ck where k1 = 'q';
execute check_ck;
select count(*) from ck where match(body) against('apple');
deallocate prepare check_ck;

set probe_limit = @saved_probe_limit;
set experimental_fulltext_index = 0;
set experimental_ivf_index = 0;
drop database dml_irregular_sync;

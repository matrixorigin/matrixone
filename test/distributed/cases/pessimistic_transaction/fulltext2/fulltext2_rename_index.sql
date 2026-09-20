-- #23392: a logical rename reconstructs the whole plugin group. Poll durable
-- replacement readiness before the first MATCH, avoiding multi-CN cache races.
set experimental_fulltext2_index = 1;
drop database if exists ft2_rename_index;
create database ft2_rename_index;
use ft2_rename_index;
create table docs(id bigint primary key, body text, state int);
insert into docs values(1,'quantum physics',10),(2,'classical mechanics',20);
create fulltext2 index ft_old on docs(body) include(state);
select id, state from docs where match(body) against('quantum') order by id;
begin;
alter table docs rename index ft_old to discarded_name;
rollback;
select distinct name from mo_catalog.mo_indexes
where table_id = (select rel_id from mo_catalog.mo_tables where reldatabase=database() and relname='docs')
order by name;
select id, state from docs where match(body) against('quantum') order by id;
alter table docs rename index ft_old to ft_new;
show create table docs;
select distinct name, algo, algo_table_type from mo_catalog.mo_indexes
where table_id = (select rel_id from mo_catalog.mo_tables where reldatabase = database() and relname = 'docs')
order by name, algo_table_type;
set @idx = (select index_table_name from mo_catalog.mo_indexes
where name='ft_new' and algo_table_type='ftv2_index' and table_id =
(select rel_id from mo_catalog.mo_tables where reldatabase=database() and relname='docs') limit 1);
set @ready_sql = concat('select count(*) > 0 as ready from `', database(), '`.`', @idx, '` where tag=0');
prepare ready from @ready_sql;
-- @wait_expect(1, 120)
execute ready;
deallocate prepare ready;
insert into docs values(3,'quantum computing',30);
update docs set state=11 where id=1;
delete from docs where id=2;
-- Explicit rebuild uses the NEW logical name. FORCE_SYNC is an intentional
-- barrier, not a sleep or an assumption of immediate cross-CN CDC freshness.
-- The replacement has not been searched yet, so no CN holds its old cache.
alter table docs alter reindex ft_new fulltext2 force_sync;
select id, state from docs where match(body) against('quantum') order by id;
alter table docs alter reindex ft_old fulltext2 force_sync;
select id, body, state from docs order by id;
-- Autonomous CDC continuation uses an unsearched replacement, so a remote
-- CN cannot hold a stale warm cache. Wait for a durable tail advance.
create table cdc_docs(id bigint primary key, body text);
insert into cdc_docs values(1,'quantum source');
create fulltext2 index old_cdc on cdc_docs(body);
alter table cdc_docs rename index old_cdc to new_cdc;
set @cdc_idx = (select index_table_name from mo_catalog.mo_indexes
where name='new_cdc' and algo_table_type='ftv2_index' and table_id =
(select rel_id from mo_catalog.mo_tables where reldatabase=database() and relname='cdc_docs') limit 1);
set @cdc_ready_sql = concat('select count(*) > 0 as ready from `', database(), '`.`', @cdc_idx, '` where tag=0');
prepare cdc_ready from @cdc_ready_sql;
-- @wait_expect(1, 120)
execute cdc_ready;
deallocate prepare cdc_ready;
set @baseline_sql = concat('select coalesce(max(chunk_id),-1) into @baseline from `', database(), '`.`', @cdc_idx, '` where tag=1 and index_id=''cdc_tail''');
prepare baseline from @baseline_sql;
execute baseline;
deallocate prepare baseline;
insert into cdc_docs values(2,'quantum incremental');
set @tail_sql = concat('select coalesce(max(chunk_id),-1) > @baseline as ready from `', database(), '`.`', @cdc_idx, '` where tag=1 and index_id=''cdc_tail''');
prepare tail_ready from @tail_sql;
-- @wait_expect(1, 120)
execute tail_ready;
deallocate prepare tail_ready;
select id from cdc_docs where match(body) against('quantum') order by id;
drop database ft2_rename_index;
set experimental_fulltext2_index = 0;

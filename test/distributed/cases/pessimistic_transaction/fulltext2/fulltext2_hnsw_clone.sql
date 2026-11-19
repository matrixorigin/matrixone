-- Regression for #27971: all three clone entry points must restore both
-- CDC-backed indexes and keep their incremental maintenance independent.
-- Share source initialization; every destination owns its tables,
-- CDC generations and SQL assertions. Snapshot clone must read the old image.
set @clone_old_ft = @@experimental_fulltext2_index;
set @clone_old_hnsw = @@experimental_hnsw_index;
set experimental_fulltext2_index = 1;
set experimental_hnsw_index = 1;

drop database if exists ft2_hnsw_clone_source;

drop database if exists ft2_hnsw_clone_table;

drop database if exists ft2_hnsw_clone_database;

drop database if exists ft2_hnsw_clone_snapshot;

drop snapshot if exists ft2_hnsw_clone_shared_sp;

create database ft2_hnsw_clone_source;
use ft2_hnsw_clone_source;
create table src(id bigint primary key, body text, v vecf32(3));
create fulltext2 index ft on src(body);
create index hnsw_idx using hnsw on src(v) op_type 'vector_l2_ops' max_index_capacity 1000;
insert into src values (1, 'alpha copied', '[0,0,0]'), (2, 'beta copied', '[10,10,10]');

set @src_ft = (select index_table_name from mo_catalog.mo_indexes where name = 'ft' and algo = 'fulltext2' and algo_table_type = 'ftv2_index' and table_id = (select rel_id from mo_catalog.mo_tables where reldatabase = 'ft2_hnsw_clone_source' and relname = 'src') limit 1);

set @src_hm = (select index_table_name from mo_catalog.mo_indexes where name = 'hnsw_idx' and algo = 'hnsw' and algo_table_type = 'hnsw_meta' and table_id = (select rel_id from mo_catalog.mo_tables where reldatabase = 'ft2_hnsw_clone_source' and relname = 'src') limit 1);

set @wait_source_ready_sql = concat('select (select coalesce(max(chunk_id), -1) from `ft2_hnsw_clone_source`.`',
    @src_ft,
    '` where index_id = ''cdc_tail'' and tag = 1) >= 0 and (select count(*) = 1 and min(filesize) > 0 from `ft2_hnsw_clone_source`.`',
    @src_hm,
    '`) as ready');
prepare wait_source_ready from @wait_source_ready_sql;

-- @wait_expect(2, 60)
execute wait_source_ready;
deallocate prepare wait_source_ready;

set @capture_source_sql = concat('select (select coalesce(max(chunk_id), -1) from `ft2_hnsw_clone_source`.`',
    @src_ft,
    '` where index_id = ''cdc_tail'' and tag = 1), (select checksum from `ft2_hnsw_clone_source`.`',
    @src_hm,
    '` limit 1) into @src_tail_before, @src_h_before');
prepare capture_source from @capture_source_sql;

execute capture_source;
deallocate prepare capture_source;

create snapshot ft2_hnsw_clone_shared_sp for table ft2_hnsw_clone_source src;
set experimental_fulltext2_index = 0;
set experimental_hnsw_index = 0;
-- The ordinary table and database entry points see the live baseline.
create database ft2_hnsw_clone_table;
create table ft2_hnsw_clone_table.dst clone ft2_hnsw_clone_source.src;
create database ft2_hnsw_clone_database clone ft2_hnsw_clone_source;
-- The snapshot entry point must exclude this subsequently committed row.
insert into src values (4, 'alpha source incremental', '[0.1,0.1,0.1]');

set @wait_source_advanced_sql = concat('select (select coalesce(max(chunk_id), -1) from `ft2_hnsw_clone_source`.`',
    @src_ft,
    '` where index_id = ''cdc_tail'' and tag = 1) > @src_tail_before and (select checksum from `ft2_hnsw_clone_source`.`',
    @src_hm,
    '` limit 1) <> @src_h_before as ready');
prepare wait_source_advanced from @wait_source_advanced_sql;

-- @wait_expect(2, 60)
execute wait_source_advanced;
deallocate prepare wait_source_advanced;

create database ft2_hnsw_clone_snapshot;
create table ft2_hnsw_clone_snapshot.dst clone ft2_hnsw_clone_source.src {snapshot = 'ft2_hnsw_clone_shared_sp'};

set @t_ft = (select index_table_name from mo_catalog.mo_indexes where name = 'ft' and algo = 'fulltext2' and algo_table_type = 'ftv2_index' and table_id = (select rel_id from mo_catalog.mo_tables where reldatabase = 'ft2_hnsw_clone_table' and relname = 'dst') limit 1);

set @t_hm = (select index_table_name from mo_catalog.mo_indexes where name = 'hnsw_idx' and algo = 'hnsw' and algo_table_type = 'hnsw_meta' and table_id = (select rel_id from mo_catalog.mo_tables where reldatabase = 'ft2_hnsw_clone_table' and relname = 'dst') limit 1);

set @d_ft = (select index_table_name from mo_catalog.mo_indexes where name = 'ft' and algo = 'fulltext2' and algo_table_type = 'ftv2_index' and table_id = (select rel_id from mo_catalog.mo_tables where reldatabase = 'ft2_hnsw_clone_database' and relname = 'src') limit 1);

set @d_hm = (select index_table_name from mo_catalog.mo_indexes where name = 'hnsw_idx' and algo = 'hnsw' and algo_table_type = 'hnsw_meta' and table_id = (select rel_id from mo_catalog.mo_tables where reldatabase = 'ft2_hnsw_clone_database' and relname = 'src') limit 1);

set @s_ft = (select index_table_name from mo_catalog.mo_indexes where name = 'ft' and algo = 'fulltext2' and algo_table_type = 'ftv2_index' and table_id = (select rel_id from mo_catalog.mo_tables where reldatabase = 'ft2_hnsw_clone_snapshot' and relname = 'dst') limit 1);

set @s_hm = (select index_table_name from mo_catalog.mo_indexes where name = 'hnsw_idx' and algo = 'hnsw' and algo_table_type = 'hnsw_meta' and table_id = (select rel_id from mo_catalog.mo_tables where reldatabase = 'ft2_hnsw_clone_snapshot' and relname = 'dst') limit 1);

set @wait_destinations_ready_sql = concat('select (select count(*) = 1 and min(filesize) > 0 from `ft2_hnsw_clone_table`.`',
    @t_hm,
    '`) and (select count(*) = 1 and min(filesize) > 0 from `ft2_hnsw_clone_database`.`',
    @d_hm,
    '`) and (select count(*) = 1 and min(filesize) > 0 from `ft2_hnsw_clone_snapshot`.`',
    @s_hm,
    '`) as ready');
prepare wait_destinations_ready from @wait_destinations_ready_sql;
-- Three independent destinations retain their original 60-second budgets.
-- @wait_expect(2, 180)
execute wait_destinations_ready;
deallocate prepare wait_destinations_ready;

set @capture_destinations_sql = concat('select (select coalesce(max(chunk_id), -1) from `ft2_hnsw_clone_table`.`',
    @t_ft,
    '` where index_id = ''cdc_tail'' and tag = 1), (select checksum from `ft2_hnsw_clone_table`.`',
    @t_hm,
    '` limit 1), (select coalesce(max(chunk_id), -1) from `ft2_hnsw_clone_database`.`',
    @d_ft,
    '` where index_id = ''cdc_tail'' and tag = 1), (select checksum from `ft2_hnsw_clone_database`.`',
    @d_hm,
    '` limit 1), (select coalesce(max(chunk_id), -1) from `ft2_hnsw_clone_snapshot`.`',
    @s_ft,
    '` where index_id = ''cdc_tail'' and tag = 1), (select checksum from `ft2_hnsw_clone_snapshot`.`',
    @s_hm,
    '` limit 1) into @t_tail_before, @t_h_before, @d_tail_before, @d_h_before, @s_tail_before, @s_h_before');
prepare capture_destinations from @capture_destinations_sql;

execute capture_destinations;
deallocate prepare capture_destinations;

insert into ft2_hnsw_clone_table.dst values (3, 'alpha destination incremental', '[0.1,0.1,0.1]');

insert into ft2_hnsw_clone_database.src values (5, 'alpha destination incremental', '[0.1,0.1,0.1]');

insert into ft2_hnsw_clone_snapshot.dst values (6, 'alpha destination incremental', '[0.1,0.1,0.1]');

set @wait_destinations_advanced_sql = concat('select (select coalesce(max(chunk_id), -1) from `ft2_hnsw_clone_table`.`',
    @t_ft,
    '` where index_id = ''cdc_tail'' and tag = 1) > @t_tail_before and (select checksum from `ft2_hnsw_clone_table`.`',
    @t_hm,
    '` limit 1) <> @t_h_before and (select coalesce(max(chunk_id), -1) from `ft2_hnsw_clone_database`.`',
    @d_ft,
    '` where index_id = ''cdc_tail'' and tag = 1) > @d_tail_before and (select checksum from `ft2_hnsw_clone_database`.`',
    @d_hm,
    '` limit 1) <> @d_h_before and (select coalesce(max(chunk_id), -1) from `ft2_hnsw_clone_snapshot`.`',
    @s_ft,
    '` where index_id = ''cdc_tail'' and tag = 1) > @s_tail_before and (select checksum from `ft2_hnsw_clone_snapshot`.`',
    @s_hm,
    '` limit 1) <> @s_h_before as ready');
prepare wait_destinations_advanced from @wait_destinations_advanced_sql;
-- @wait_expect(2, 180)
execute wait_destinations_advanced;
deallocate prepare wait_destinations_advanced;

select id from src where match(body) against('alpha' in boolean mode) order by id;
select id from src order by l2_distance(v, '[0.1,0.1,0.1]') limit 2;
select count(*) from src where id in (3,5,6);

-- Ordinary table clone: copied contents, incremental contents, Top-K, and no source alias.
select id from ft2_hnsw_clone_table.dst where match(body) against('alpha' in boolean mode) order by id;
select id from ft2_hnsw_clone_table.dst where match(body) against('beta' in boolean mode) order by id;
select id from ft2_hnsw_clone_table.dst order by l2_distance(v, '[0.1,0.1,0.1]') limit 2;
select count(*) from ft2_hnsw_clone_table.dst where id = 4;

create table ft2_hnsw_clone_table.dst clone ft2_hnsw_clone_source.src;

select count(*) from ft2_hnsw_clone_table.dst;

-- Database clone: copied contents, incremental contents, Top-K, and no source alias.
select id from ft2_hnsw_clone_database.src where match(body) against('alpha' in boolean mode) order by id;
select id from ft2_hnsw_clone_database.src where match(body) against('beta' in boolean mode) order by id;
select id from ft2_hnsw_clone_database.src order by l2_distance(v, '[0.1,0.1,0.1]') limit 2;
select count(*) from ft2_hnsw_clone_database.src where id = 4;

create database ft2_hnsw_clone_database clone ft2_hnsw_clone_source;

select count(*) from ft2_hnsw_clone_database.src;

-- Snapshot table clone: copied contents, incremental contents, Top-K, and no source alias.
select id from ft2_hnsw_clone_snapshot.dst where match(body) against('alpha' in boolean mode) order by id;
select id from ft2_hnsw_clone_snapshot.dst where match(body) against('beta' in boolean mode) order by id;
select id from ft2_hnsw_clone_snapshot.dst order by l2_distance(v, '[0.1,0.1,0.1]') limit 2;
select count(*) from ft2_hnsw_clone_snapshot.dst where id = 4;

create table ft2_hnsw_clone_snapshot.dst clone ft2_hnsw_clone_source.src {snapshot = 'ft2_hnsw_clone_shared_sp'};

select count(*) from ft2_hnsw_clone_snapshot.dst;

drop snapshot ft2_hnsw_clone_shared_sp;

drop database ft2_hnsw_clone_table;

drop database ft2_hnsw_clone_database;

drop database ft2_hnsw_clone_snapshot;

drop database ft2_hnsw_clone_source;

set experimental_fulltext2_index = @clone_old_ft;
set experimental_hnsw_index = @clone_old_hnsw;

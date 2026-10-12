drop database if exists vector_ivfflat_include_cdc_values;
create database vector_ivfflat_include_cdc_values;
use vector_ivfflat_include_cdc_values;
set @saved_time_zone = @@session.time_zone;
set time_zone = '+00:00';

-- Populate the async index while the synchronous generated-INCLUDE controls
-- run. Two rows distinguish fractional temporal values from NULL without a
-- larger training fixture. Affine quantization must still target the vector.
create table temporal_values(
    id int primary key, v vecf32(2),
    ts6 timestamp(6), ts3 timestamp(3), dt6 datetime(6), tm3 time(3)
);
insert into temporal_values values
    (1, '[0,0]', '2026-01-02 03:04:05.123456', '2026-01-02 03:04:05.123',
        '2026-01-02 03:04:05.123456', '03:04:05.123'),
    (2, '[1,0]', null, null, null, null);
create index temporal_idx using ivfflat on temporal_values(v)
lists=1 op_type 'vector_l2_ops' quantization 'int8' include(ts6, ts3, dt6, tm3) async;

-- Shared generated dependencies must retain the same UPDATE/ODKU dependency
-- semantics while an unrelated ODKU assignment leaves the covering values
-- unchanged. The scale/cost boundary is covered by the package benchmark.
create table generated_values(
    id int primary key, src int, payload int, v vecf32(2),
    gmid int as (src + src) stored,
    ginclude int as (gmid + gmid) stored
);
insert into generated_values(id, src, payload, v) values
    (1, 1, 10, '[0,0]'), (2, 2, 20, '[1,0]');
create index generated_idx using ivfflat on generated_values(v)
lists=1 op_type 'vector_l2_ops' include(ginclude);
select id, ginclude from generated_values
order by l2_distance(v, '[0,0]') limit 2 by rank with option 'mode=include';
update generated_values set src = 3 where id = 1;
select id, ginclude from generated_values
order by l2_distance(v, '[0,0]') limit 2 by rank with option 'mode=include';
insert into generated_values(id, src, payload, v) values (1, 9, 30, '[0,0]')
on duplicate key update payload = values(payload);
select id, ginclude from generated_values
order by l2_distance(v, '[0,0]') limit 2 by rank with option 'mode=include';
insert into generated_values(id, src, payload, v) values (1, 4, 40, '[0,0]')
on duplicate key update src = values(src);
select id, ginclude from generated_values
order by l2_distance(v, '[0,0]') limit 2 by rank with option 'mode=include';
select id, ginclude from generated_values order by id;
drop table generated_values;

set @entries = (select index_table_name from mo_catalog.mo_indexes
    where name = 'temporal_idx' and algo_table_type = 'entries'
    and table_id in (select rel_id from mo_catalog.mo_tables
        where reldatabase = database() and relname = 'temporal_values'));
set @metadata = (select index_table_name from mo_catalog.mo_indexes
    where name = 'temporal_idx' and algo_table_type = 'metadata'
    and table_id in (select rel_id from mo_catalog.mo_tables
        where reldatabase = database() and relname = 'temporal_values'));
set @entry_query = concat(
    'select __mo_index_pri_col as id, cast(__mo_index_include_ts6 as varchar) as ts6, ',
    'cast(__mo_index_include_ts3 as varchar) as ts3, ',
    'cast(__mo_index_include_dt6 as varchar) as dt6, ',
    'cast(__mo_index_include_tm3 as varchar) as tm3 ',
    'from `', database(), '`.`', @entries, '` ',
    'where __mo_index_centroid_fk_version = (select cast(__mo_index_val as bigint) ',
    'from `', database(), '`.`', @metadata, '` where __mo_index_key = ''version'') ',
    'order by __mo_index_pri_col');
prepare temporal_entries from @entry_query;
-- @wait_expect(2, 60)
execute temporal_entries;

-- Only mutate after initial convergence to prove CDC replay, not just initial
-- synchronous population. Fractions near the next second catch scale-0 casts.
update temporal_values set ts6 = '2026-01-02 03:04:05.987654',
    ts3 = '2026-01-02 03:04:05.987', dt6 = '2026-01-02 03:04:05.987654',
    tm3 = '03:04:05.987' where id = 1;
insert into temporal_values values (3, '[2,0]', null, null, null, null);
delete from temporal_values where id = 2;
-- @wait_expect(2, 60)
execute temporal_entries;
select id, cast(ts6 as varchar) as ts6, cast(ts3 as varchar) as ts3,
    cast(dt6 as varchar) as dt6, cast(tm3 as varchar) as tm3
from temporal_values order by id;

-- Reading the same stored instants in another timezone must agree with the
-- source; the ISCP session's timezone must not affect the persisted values.
set time_zone = '+08:00';
execute temporal_entries;
select id, cast(ts6 as varchar) as ts6, cast(ts3 as varchar) as ts3,
    cast(dt6 as varchar) as dt6, cast(tm3 as varchar) as tm3
from temporal_values order by id;
deallocate prepare temporal_entries;
drop table temporal_values;
set time_zone = @saved_time_zone;
drop database vector_ivfflat_include_cdc_values;

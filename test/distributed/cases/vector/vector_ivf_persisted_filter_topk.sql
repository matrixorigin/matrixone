-- #30509: all-matching persisted residual filters retain the identity row map.
-- Empty-index creation is public DDL, not hidden-entry injection. Eight rows
-- have distinct distances and physical PK order different from rank order.
drop database if exists ivf_persisted_filter_topk;
create database ivf_persisted_filter_topk;
use ivf_persisted_filter_topk;

set @saved_ivf_index = @@experimental_ivf_index;
set @saved_probe_limit = @@probe_limit;
set experimental_ivf_index = 1;
set probe_limit = 8;

create table docs(
    id varchar(8) primary key,
    v vecf32(4),
    delete_flag int,
    content_type varchar(16),
    payload varchar(128),
    selected int
);
create index idx using ivfflat on docs(v) lists=1 op_type 'vector_l2_ops';
insert into docs values
    ('r1','[9,0,0,0]',null,null,'payload-1',1),
    ('r2','[5,0,0,0]',null,null,'payload-2',0),
    ('r3','[7,0,0,0]',null,null,'payload-3',1),
    ('r4','[2,0,0,0]',null,null,'payload-4',0),
    ('r5','[8,0,0,0]',null,null,'payload-5',1),
    ('r6','[1,0,0,0]',null,null,'payload-6',0),
    ('r7','[6,0,0,0]',null,null,'payload-7',1),
    ('r8','[3,0,0,0]',null,null,'payload-8',0);

-- The failing boundary is persisted-block Top-K, not an in-memory scan.
-- @ignore:0
select mo_ctl('dn','flush','ivf_persisted_filter_topk.docs');
set @entries = (select distinct i.index_table_name from mo_catalog.mo_indexes i join mo_catalog.mo_tables t on i.table_id=t.rel_id where t.reldatabase=database() and t.relname='docs' and i.name='idx' and i.algo_table_type='entries');
-- @ignore:0
select mo_ctl('dn','flush',concat(database(),'.',@entries));
set @entry_stats = concat('select table_cnt, accurate_object_number > 0 as persisted from table_stats("',database(),'.',@entries,'","refresh","full") g');
prepare persisted_stats from @entry_stats;
execute persisted_stats;
deallocate prepare persisted_stats;

-- Keep index use and exact full-scan results as separate oracles.
-- @regex("Vector Index Scan",true)
explain select id,payload,l2_distance(v,'[0,0,0,0]') as dist from docs
where delete_flag is null and content_type is null
order by dist limit 3 by rank with option 'mode=pre';
-- @regex("Vector Index Scan",false)
explain select id,payload,l2_distance(v,'[0,0,0,0]') as dist from docs
where delete_flag is null and content_type is null
order by dist limit 3 by rank with option 'mode=force';
select id,payload,l2_distance(v,'[0,0,0,0]') as dist from docs
where delete_flag is null and content_type is null
order by dist limit 3 by rank with option 'mode=pre';
select id,payload,l2_distance(v,'[0,0,0,0]') as dist from docs
where delete_flag is null and content_type is null
order by dist limit 3 by rank with option 'mode=force';

-- A proper subset requires an explicit, nonidentity row map.
select id,payload,l2_distance(v,'[0,0,0,0]') as dist from docs
where selected=1 order by dist limit 3 by rank with option 'mode=pre';
select id,payload,l2_distance(v,'[0,0,0,0]') as dist from docs
where selected=1 order by dist limit 3 by rank with option 'mode=force';

-- Empty is not the absent-selection identity sentinel.
select id,payload,l2_distance(v,'[0,0,0,0]') as dist from docs
where content_type='missing' order by dist limit 3 by rank with option 'mode=pre';

-- Tombstones shift logical rows relative to persisted physical offsets.
delete from docs where id='r6';
select id,payload,l2_distance(v,'[0,0,0,0]') as dist from docs
where delete_flag is null and content_type is null
order by dist limit 3 by rank with option 'mode=pre';
select id,payload,l2_distance(v,'[0,0,0,0]') as dist from docs
where delete_flag is null and content_type is null
order by dist limit 3 by rank with option 'mode=force';

-- Reusing a primary key must pair the new vector with the new payload.
insert into docs values ('r6','[0,0,0,0]',null,null,'payload-reinserted',0);
select id,payload,l2_distance(v,'[0,0,0,0]') as dist from docs
where delete_flag is null and content_type is null
order by dist limit 3 by rank with option 'mode=pre';
select id,payload,l2_distance(v,'[0,0,0,0]') as dist from docs
where delete_flag is null and content_type is null
order by dist limit 3 by rank with option 'mode=force';

set experimental_ivf_index = @saved_ivf_index;
set probe_limit = @saved_probe_limit;
select @@experimental_ivf_index = @saved_ivf_index as ivf_index_restored,
       @@probe_limit = @saved_probe_limit as probe_limit_restored;
drop database ivf_persisted_filter_topk;

-- The vector access path must use the same historical snapshot as the source
-- table.  A post-snapshot insert is the counterexample: the current IVF entry
-- is the nearest candidate, but the source table at the snapshot cannot join it.
set experimental_ivf_index = 1;
set probe_limit = 5;

drop snapshot if exists vector_ivf_scan_snapshot;
drop database if exists vector_ivf_snapshot;
create database vector_ivf_snapshot;
use vector_ivf_snapshot;

create table t (id bigint primary key, v vecf32(2));
insert into t values (1, '[1,1]'), (2, '[10,10]');
create index idx_snapshot using ivfflat on t(v) lists=1 op_type 'vector_l2_ops';
create snapshot vector_ivf_scan_snapshot for account;

insert into t values (3, '[0.1,0.1]');

-- The current view includes the post-snapshot row.
select id from t order by l2_distance(v, '[0,0]') limit 1;

-- The historical view must ignore that row in both the source and IVF tables.
-- @separator:table
-- @regex("Vector Index Scan", true)
explain select id from t {snapshot = 'vector_ivf_scan_snapshot'}
order by l2_distance(v, '[0,0]') limit 1;

select id from t {snapshot = 'vector_ivf_scan_snapshot'}
order by l2_distance(v, '[0,0]') limit 1;

drop snapshot vector_ivf_scan_snapshot;
drop database vector_ivf_snapshot;

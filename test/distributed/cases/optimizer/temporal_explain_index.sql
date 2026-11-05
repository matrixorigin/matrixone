-- Plain EXPLAIN must show the temporal covering-index access used by execution.
drop database if exists temporal_explain_bvt;
create database temporal_explain_bvt;
use temporal_explain_bvt;
set @temporal_explain_saved_tz = @@time_zone;
set time_zone='+00:00';
create table events(id int primary key, d datetime(6), key idx_d(d));
insert into events
select result, case when result < 99900 then date_add('2020-01-01 00:00:00', interval result second) else date_add(now(6), interval 1 day) end
from generate_series(0,99999) g;
select mo_ctl('dn','flush','temporal_explain_bvt.events');
set @temporal_explain_index = (select index_table_name from mo_catalog.mo_indexes where name='idx_d' and table_id=(select rel_id from mo_catalog.mo_tables where reldatabase='temporal_explain_bvt' and relname='events') limit 1);
select mo_ctl('dn','flush',concat('temporal_explain_bvt.',@temporal_explain_index));
-- @ignore:0,1,2,3,4,5,6,7,8,9,10
analyze table events(d) fullscan;
-- @regex("Index Table Scan on events.idx_d",true)
explain select id,d from events where d > now(6);
select count(*) from events where d > now(6);
select count(*) from events ignore index(idx_d) where d > now(6);
set time_zone=@temporal_explain_saved_tz;
drop database temporal_explain_bvt;

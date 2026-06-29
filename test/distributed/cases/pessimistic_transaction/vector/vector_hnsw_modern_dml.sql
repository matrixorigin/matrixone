-- HNSW indexes are maintained asynchronously by cron (off the base-table CDC),
-- so INSERT / ON DUPLICATE KEY UPDATE / REPLACE on an HNSW table ride the modern
-- DML path with no synchronous index maintenance and no legacy fallback. This
-- verifies the base-table outcome of each DML kind; the HNSW index itself is
-- reconciled by cron and is not asserted here.
SET experimental_hnsw_index = 1;
drop table if exists hnsw_dml;
create table hnsw_dml(a bigint primary key, b vecf32(3), c int, KEY idx using hnsw(b) op_type 'vector_l2_ops');
insert into hnsw_dml values(1,"[1,2,3]",10),(2,"[4,5,6]",20),(3,"[7,8,9]",30);
select a,b,c from hnsw_dml order by a;
insert into hnsw_dml values(1,"[1,1,1]",11) on duplicate key update b="[9,9,9]", c=c+100;
select a,b,c from hnsw_dml order by a;
insert into hnsw_dml values(4,"[2,2,2]",40) on duplicate key update c=c+1;
select a,b,c from hnsw_dml order by a;
replace into hnsw_dml values(2,"[0,0,0]",22);
replace into hnsw_dml values(5,"[3,3,3]",50);
select a,b,c from hnsw_dml order by a;
drop table if exists hnsw_dml;
SET experimental_hnsw_index = 0;

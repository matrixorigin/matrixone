
SET experimental_hnsw_index = 1;
create database if not exists hnsw_cdc;
use hnsw_cdc;

create table vector_index_01(a bigint primary key, b vecf32(3),c int,key c_k(c));

-- empty data
create index idx01 using hnsw on vector_index_01(b) op_type "vector_l2_ops" M 48 EF_CONSTRUCTION 64 EF_SEARCH 64;

drop pitr if exists `__mo_table_pitr_hnsw`;
create pitr `__mo_table_pitr_hnsw` for table hnsw_cdc vector_index_01 range 2 'h';

create cdc `__mo_cdc_hnsw_idx01` 'mysql://root:111@127.0.0.1:6001' 'hnswsync' 'mysql://root:111@127.0.0.1:6001' 'hnsw_cdc.vector_index_01' {'Level'='table'};

-- show cdc all;
select sleep(30);

insert into vector_index_01 values (0, "[1,2,3]", 1);

select sleep(1);

select * from vector_index_01 order by  L2_DISTANCE(b,"[1,2,3]") ASC LIMIT 10;


-- select hnsw_cdc_update('hnsw_cdc', 'vector_index_01', 3, '{"start":"", "end":"", "cdc":[{"t":"U", "pk":0, "v":[1,2,3]}]}');

select * from vector_index_01 order by  L2_DISTANCE(b,"[1,2,3]") ASC LIMIT 10;

DELETE FROM vector_index_01 WHERE a=0;
-- select hnsw_cdc_update('hnsw_cdc', 'vector_index_01', 3, '{"start":"", "end":"", "cdc":[{"t":"D", "pk":0}]}');
select sleep(1);

select * from vector_index_01 order by  L2_DISTANCE(b,"[1,2,3]") ASC LIMIT 10;

insert into vector_index_01 values (1, "[2,3,4]", 1);
-- select hnsw_cdc_update('hnsw_cdc', 'vector_index_01', 3, '{"start":"", "end":"", "cdc":[{"t":"I", "pk":1, "v":[2,3,4]}]}');

select sleep(1);

select * from vector_index_01 order by  L2_DISTANCE(b,"[2,3,4]") ASC LIMIT 10;


drop cdc task `__mo_cdc_hnsw_idx01`;
drop pitr if exists `__mo_table_pitr_hnsw`;
drop table vector_index_01;

drop database hnsw_cdc;

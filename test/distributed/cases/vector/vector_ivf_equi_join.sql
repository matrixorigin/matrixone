-- 普通等值 INNER JOIN 的资格域与最终重复行必须分别保留。
drop database if exists vector_ivf_equi_join;
create database vector_ivf_equi_join;
use vector_ivf_equi_join;
set @saved_experimental_ivf_index = @@experimental_ivf_index;
set experimental_ivf_index = 1;
set @probe_limit = 1;

create table chunks (
    id varchar(32) primary key,
    v vecf32(2) not null,
    document_id varchar(32),
    category varchar(32)
);
insert into chunks values
    ('c0', '[0,0]', 'u', 'x'),
    ('c1', '[0.01,0]', 'u', 'x'),
    ('c2', '[0.1,0]', 'd1', 'x'),
    ('c3', '[0.2,0]', 'd2', 'z'),
    ('c4', '[0.3,0]', 'd1', 'y'),
    ('c5', '[0.6,0]', 'd3', 'outside'),
    ('c6', '[0.15,0]', null, 'null'),
    ('c7', '[0.4,0]', 'd4', 'new');
create index chunks_ivf using ivfflat on chunks(v) lists = 1 op_type 'vector_l2_ops';
create table documents (row_id int primary key, document_id varchar(32), label varchar(32));
insert into documents values (1,'d1','x'),(2,'d1','y'),(3,'d2','z'),(4,'d3','outside'),(5,null,'null');
create table unique_documents (document_id varchar(32) primary key, label varchar(32));
insert into unique_documents values ('d1','x'),('d2','z'),('d3','outside');

-- 默认与显式 PRE 都必须出现索引；FORCE 保持精确关系扫描。
-- @regex("Vector Index Scan",true)
explain select a.id from chunks a join documents b on a.document_id=b.document_id
    order by l2_distance(a.v,'[0,0]') limit 3;
-- @regex("Vector Index Scan",true)
explain select a.id from chunks a join documents b on a.document_id=b.document_id
    order by l2_distance(a.v,'[0,0]') limit 3 by rank with option 'mode=pre';
-- @regex("Vector Index Scan",false)
explain select a.id from chunks a join documents b on a.document_id=b.document_id
    order by l2_distance(a.v,'[0,0]') limit 3 by rank with option 'mode=force';

-- 最近的不合格行不能占掉候选预算；重复 JOIN 与 IN 不能等价替换。
select a.id from chunks a join documents b on a.document_id=b.document_id
    order by l2_distance(a.v,'[0,0]') limit 3;
select a.id from chunks a join documents b on a.document_id=b.document_id
    order by l2_distance(a.v,'[0,0]') limit 3 by rank with option 'mode=pre';
select a.id from chunks a join documents b on a.document_id=b.document_id
    order by l2_distance(a.v,'[0,0]') limit 3 by rank with option 'mode=force';
select a.id from chunks a where a.document_id in (select document_id from documents)
    order by l2_distance(a.v,'[0,0]') limit 3;
select a.id from chunks a join unique_documents b on a.document_id=b.document_id
    order by l2_distance(a.v,'[0,0]') limit 3;

-- 右侧投影、过滤、反向 JOIN 和多列等值条件。
-- @sortkey:0,1
select a.id,b.label from chunks a join documents b on a.document_id=b.document_id
    order by l2_distance(a.v,'[0,0]') limit 3;
-- @sortkey:0,1
select a.id,b.label,l2_distance(a.v,'[0,0]') as distance from chunks a
    join documents b on a.document_id=b.document_id order by distance limit 3;
select a.id,b.label from chunks a join documents b on a.document_id=b.document_id
    where b.label='x' order by l2_distance(a.v,'[0,0]') limit 3;
select a.id from documents b join chunks a on b.document_id=a.document_id
    order by l2_distance(a.v,'[0,0]') limit 3;
select a.id,b.label from chunks a join documents b
    on a.document_id=b.document_id and a.category=b.label
    order by l2_distance(a.v,'[0,0]') limit 3;

-- 距离阈值必须作用于候选，OFFSET 必须作用于重复展开后的最终行。
select a.id from chunks a join documents b on a.document_id=b.document_id
    where l2_distance(a.v,'[0,0]')<=0.15 order by l2_distance(a.v,'[0,0]') limit 10;
select a.id from chunks a join documents b on a.document_id=b.document_id
    where l2_distance(a.v,'[0,0]')<=0.5 order by l2_distance(a.v,'[0,0]') limit 10;
select a.id from chunks a join documents b on a.document_id=b.document_id
    where l2_distance(a.v,'[0,0]')<=0.5 order by l2_distance(a.v,'[0,0]') limit 3 offset 1;
select a.id from chunks a join documents b on a.document_id=b.document_id
    where l2_distance(a.v,'[0,0]')<=0.5 order by l2_distance(a.v,'[0,0]') limit 3 offset 1
    by rank with option 'mode=force';
select a.id from chunks a join documents b on a.document_id=b.document_id
    order by l2_distance(a.v,'[0,0]') limit 2 offset 20;
select a.id from chunks a join documents b on a.document_id=b.document_id
    order by l2_distance(a.v,'[0,0]') limit 0;
select a.id from chunks a join documents b on a.document_id=b.document_id
    where b.label='absent' order by l2_distance(a.v,'[0,0]') limit 3;

-- 重复执行必须重新建立资格域，零 LIMIT 不应等待未启动的生产者。
prepare p from 'select a.id from chunks a join documents b on a.document_id=b.document_id
    order by l2_distance(a.v,"[0,0]") limit ?';
set @k = 0;
execute p using @k;
set @k = 3;
execute p using @k;
update documents set document_id='u' where document_id='d1';
execute p using @k;
update documents set document_id='d1' where document_id='u';
execute p using @k;
set @k = 0;
execute p using @k;
deallocate prepare p;

-- 查询向量未知或 NULL 时保守回退，不能把全 NULL 排序误当成空资格域。
prepare null_vector from 'select count(*) from (select a.id from chunks a join documents b
    on a.document_id=b.document_id order by l2_distance(a.v,?) limit 3) q';
set @query_vector = null;
execute null_vector using @query_vector;
set @query_vector = '[0,0]';
execute null_vector using @query_vector;
deallocate prepare null_vector;

-- NULL 向量行不应被新路径遗漏；显式排除 NULL 后才允许下推。
create table nullable_chunks (id varchar(32) primary key, v vecf32(2), document_id varchar(32));
insert into nullable_chunks values ('n0',null,'d1'),('n1','[0.1,0]','d1');
create index nullable_ivf using ivfflat on nullable_chunks(v) lists=1 op_type 'vector_l2_ops';
-- @regex("Vector Index Scan",false)
explain select a.id from nullable_chunks a join documents b on a.document_id=b.document_id
    order by l2_distance(a.v,'[0,0]') limit 2;
select a.id from nullable_chunks a join documents b on a.document_id=b.document_id
    order by l2_distance(a.v,'[0,0]') limit 2;
select a.id from nullable_chunks a join documents b on a.document_id=b.document_id
    order by l2_distance(a.v,'[0,0]') limit 2 by rank with option 'mode=force';
-- @regex("Vector Index Scan",true)
explain select a.id from nullable_chunks a join documents b on a.document_id=b.document_id
    where a.v is not null order by l2_distance(a.v,'[0,0]') limit 3;
select a.id from nullable_chunks a join documents b on a.document_id=b.document_id
    where a.v is not null order by l2_distance(a.v,'[0,0]') limit 3;
create table empty_documents (document_id varchar(32));
select a.id from chunks a join empty_documents b on a.document_id=b.document_id
    order by l2_distance(a.v,'[0,0]') limit 3;

-- 不支持的形态不能意外下推。
-- @regex("Vector Index Scan",false)
explain select a.id from chunks a left join documents b on a.document_id=b.document_id
    order by l2_distance(a.v,'[0,0]') limit 3;
-- @regex("Vector Index Scan",false)
explain select a.id from chunks a join documents b on a.document_id<b.document_id
    order by l2_distance(a.v,'[0,0]') limit 3;

drop database vector_ivf_equi_join;
set experimental_ivf_index = @saved_experimental_ivf_index;

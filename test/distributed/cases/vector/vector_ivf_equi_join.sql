-- 普通等值 INNER JOIN：资格域不能替代最终重复行关系。
drop database if exists vector_ivf_equi_join;
create database vector_ivf_equi_join;
use vector_ivf_equi_join;
set @saved_experimental_ivf_index = @@experimental_ivf_index;
set experimental_ivf_index = 1;
set probe_limit = 1;
create table chunks (id varchar(32) primary key, v vecf32(2) not null, document_id varchar(32), category varchar(32));
insert into chunks values ('c0','[0,0]','u','x'),('c1','[0.01,0]','u','x'),('c2','[0.1,0]','d1','x'),('c3','[0.2,0]','d2','z'),('c4','[0.3,0]','d1','y'),('c5','[0.6,0]','d3','outside'),('c6','[0.15,0]',null,'null');
create index chunks_ivf using ivfflat on chunks(v) lists=1 op_type 'vector_l2_ops';
create table documents (row_id int primary key, document_id varchar(32), label varchar(32));
insert into documents values (1,'d1','x'),(2,'d1','y'),(3,'d2','z'),(4,'d3','outside'),(5,null,'null');
create table unique_documents (document_id varchar(32) primary key, label varchar(32));
insert into unique_documents values ('d1','x'),('d2','z'),('d3','outside');
-- @regex("ivf_search",true)
explain select a.id from chunks a join documents b on a.document_id=b.document_id order by l2_distance(a.v,'[0,0]') limit 3;
-- @regex("ivf_search",true)
explain select a.id from chunks a join documents b on a.document_id=b.document_id order by l2_distance(a.v,'[0,0]') limit 3 by rank with option 'mode=pre';
-- @regex("ivf_search",false)
explain select a.id from chunks a join documents b on a.document_id=b.document_id order by l2_distance(a.v,'[0,0]') limit 3 by rank with option 'mode=force';
-- 最近的两行不合格；重复展开后前三行应为 c2,c2,c3。
select a.id from chunks a join documents b on a.document_id=b.document_id order by l2_distance(a.v,'[0,0]') limit 3;
select a.id from chunks a join documents b on a.document_id=b.document_id order by l2_distance(a.v,'[0,0]') limit 3 by rank with option 'mode=pre';
select a.id from chunks a join documents b on a.document_id=b.document_id order by l2_distance(a.v,'[0,0]') limit 3 by rank with option 'mode=force';
select a.id from chunks a where a.document_id in (select document_id from documents) order by l2_distance(a.v,'[0,0]') limit 3;
select a.id from chunks a join unique_documents b on a.document_id=b.document_id order by l2_distance(a.v,'[0,0]') limit 3;
-- @sortkey:0,1
select a.id,b.label from chunks a join documents b on a.document_id=b.document_id order by l2_distance(a.v,'[0,0]') limit 3;
-- @sortkey:0,1
select a.id,b.label,l2_distance(a.v,'[0,0]') as distance from chunks a join documents b on a.document_id=b.document_id order by distance limit 3;
select a.id,b.label from chunks a join documents b on a.document_id=b.document_id where b.label='x' order by l2_distance(a.v,'[0,0]') limit 3;
select a.id from documents b join chunks a on b.document_id=a.document_id order by l2_distance(a.v,'[0,0]') limit 3;
select a.id,b.label from chunks a join documents b on a.document_id=b.document_id and a.category=b.label order by l2_distance(a.v,'[0,0]') limit 3;
-- 阈值、重复展开后的 OFFSET、空资格域及零 LIMIT。
select a.id from chunks a join documents b on a.document_id=b.document_id where l2_distance(a.v,'[0,0]')<=0.15 order by l2_distance(a.v,'[0,0]') limit 10;
select a.id from chunks a join documents b on a.document_id=b.document_id where l2_distance(a.v,'[0,0]')<=0.15 order by l2_distance(a.v,'[0,0]') limit 10 by rank with option 'mode=force';
-- 下界不能只在 Top-K 后重检，否则近距离的不合格候选会耗尽预算。
select a.id from chunks a join documents b on a.document_id=b.document_id where l2_distance(a.v,'[0,0]')>0.15 order by l2_distance(a.v,'[0,0]') limit 3;
select a.id from chunks a join documents b on a.document_id=b.document_id where l2_distance(a.v,'[0,0]')>0.15 order by l2_distance(a.v,'[0,0]') limit 3 by rank with option 'mode=force';
select a.id from chunks a join documents b on a.document_id=b.document_id where l2_distance(a.v,'[0,0]')<=0.5 order by l2_distance(a.v,'[0,0]') limit 3 offset 1;
select a.id from chunks a join documents b on a.document_id=b.document_id where l2_distance(a.v,'[0,0]')<=0.5 order by l2_distance(a.v,'[0,0]') limit 3 offset 1 by rank with option 'mode=force';
select a.id from chunks a join documents b on a.document_id=b.document_id order by l2_distance(a.v,'[0,0]') limit 2 offset 20;
select a.id from chunks a join documents b on a.document_id=b.document_id order by l2_distance(a.v,'[0,0]') limit 0;
select a.id from chunks a join documents b on a.document_id=b.document_id where b.label='absent' order by l2_distance(a.v,'[0,0]') limit 3;
prepare p from 'select a.id from chunks a join documents b on a.document_id=b.document_id order by l2_distance(a.v,"[0,0]") limit ?';
set @k=0;
execute p using @k;
set @k=3;
execute p using @k;
update documents set document_id='u' where document_id='d1';
execute p using @k;
update documents set document_id='d1' where document_id='u';
execute p using @k;
set @k=0;
execute p using @k;
deallocate prepare p;
-- NULL 参数不能变成空资格域。
prepare null_vector from 'select count(*) from (select a.id from chunks a join documents b on a.document_id=b.document_id order by l2_distance(a.v,?) limit 3) q';
set @query_vector=null;
execute null_vector using @query_vector;
set @query_vector='[0,0]';
execute null_vector using @query_vector;
deallocate prepare null_vector;
create table nullable_chunks (id varchar(32) primary key,v vecf32(2),document_id varchar(32));
insert into nullable_chunks values ('n0',null,'d1'),('n1','[0.1,0]','d1');
create index nullable_ivf using ivfflat on nullable_chunks(v) lists=1 op_type 'vector_l2_ops';
-- @regex("ivf_search",false)
explain select a.id from nullable_chunks a join documents b on a.document_id=b.document_id order by l2_distance(a.v,'[0,0]') limit 2;
select a.id from nullable_chunks a join documents b on a.document_id=b.document_id order by l2_distance(a.v,'[0,0]') limit 2;
select a.id from nullable_chunks a join documents b on a.document_id=b.document_id order by l2_distance(a.v,'[0,0]') limit 2 by rank with option 'mode=force';
-- @regex("ivf_search",true)
explain select a.id from nullable_chunks a join documents b on a.document_id=b.document_id where a.v is not null order by l2_distance(a.v,'[0,0]') limit 3;
select a.id from nullable_chunks a join documents b on a.document_id=b.document_id where a.v is not null order by l2_distance(a.v,'[0,0]') limit 3;
-- 整数主键走精确 membership 协议，重复重数与字符串主键保持一致。
create table int_chunks (id bigint primary key,v vecf32(2) not null,document_id varchar(32));
insert into int_chunks values (1,'[0,0]','u'),(2,'[0.1,0]','d1'),(3,'[0.2,0]','d2');
create index int_ivf using ivfflat on int_chunks(v) lists=1 op_type 'vector_l2_ops';
-- @regex("ivf_search",true)
explain select a.id from int_chunks a join documents b on a.document_id=b.document_id order by l2_distance(a.v,'[0,0]') limit 3;
select a.id from int_chunks a join documents b on a.document_id=b.document_id order by l2_distance(a.v,'[0,0]') limit 3;
select a.id from int_chunks a join documents b on a.document_id=b.document_id order by l2_distance(a.v,'[0,0]') limit 3 by rank with option 'mode=force';
create table empty_documents (document_id varchar(32));
select a.id from chunks a join empty_documents b on a.document_id=b.document_id order by l2_distance(a.v,'[0,0]') limit 3;
-- @regex("ivf_search",false)
explain select a.id from chunks a left join documents b on a.document_id=b.document_id order by l2_distance(a.v,'[0,0]') limit 3;
-- @regex("ivf_search",false)
explain select a.id from chunks a join documents b on a.document_id<b.document_id order by l2_distance(a.v,'[0,0]') limit 3;
drop database vector_ivf_equi_join;
set experimental_ivf_index=@saved_experimental_ivf_index;

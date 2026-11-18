-- COPY ALTER must rebuild every secondary index that stores a changed primary key.
drop database if exists alter_copy_primary_index;
create database alter_copy_primary_index;
use alter_copy_primary_index;
create table t (a int, b int, u int, v int, payload int, primary key(a,b), unique key ux(u), key ix(v));
insert into t values (1,1,101,201,11),(2,2,102,202,22);
prepare p from 'select a,b,u,v,payload from t force index(ux) where u = ?';
set @u = 101;
execute p using @u;
alter table t modify column a bigint, add column extra int default 9;
execute p using @u;
insert into t values (3,3,103,203,33,9);
select a,b,u,v,payload from t force index(ux) where u in (101,102,103) order by a;
select a,b,u,v,payload from t force index(ix) where v in (201,202,203) order by a;
select a,b,u,v,payload from t ignore index(ux,ix) order by a;
update t set payload=44 where u=101;
delete from t where u=102;
select a,b,u,v,payload from t force index(ux) where u in (101,102,103) order by a;
select a,b,u,v,payload from t ignore index(ux,ix) order by a;
deallocate prepare p;
create table d (a int, b int, u int, v int, primary key(a,b), unique key ux(u), key ix(v));
insert into d values (1,1,101,201),(2,2,102,202);
alter table d drop column b;
select * from d force index(ux) where u in (101,102) order by a;
select * from d ignore index(ux,ix) order by a;
create table c (a decimal(5,2), b int, u int, primary key(a,b), unique key ux(u), key ix(b));
insert into c values (1.21,1,101),(1.24,1,102);
alter table c modify column a decimal(5,1);
show create table c;
select * from c force index(ux) where u in (101,102) order by u;
insert into c values (2.50,1,103);
select * from c force index(ux) where u in (101,102,103) order by u;
create table renamed_key (a int, b int, u int, v int, payload int, primary key(a,b), unique key ux(u), key ix(v));
insert into renamed_key values (1,11,101,201,31),(2,22,102,202,32);
alter table renamed_key change column b renamed bigint;
insert into renamed_key values (3,33,103,203,33);
select a,renamed,u,v,payload from renamed_key where u in (101,102,103) order by a;
select a,renamed,u,v,payload from renamed_key force index(ux) where u in (101,102,103) order by a;
select a,renamed,u,v,payload from renamed_key force index(ix) where v in (201,202,203) order by a;
select a,renamed,u,v,payload from renamed_key ignore index(ux,ix) order by a;
create table cow_pk (id int primary key, u int, v int, payload varchar(10), unique key ux(u), key ix(v));
insert into cow_pk values (1,101,201,'one'),(2,102,202,'two');
alter table cow_pk modify column payload varchar(20), algorithm=copy;
select id,u,v,payload from cow_pk force index(ux) where u in (101,102) order by id;
select id,u,v,payload from cow_pk force index(ix) where v in (201,202) order by id;
select id,u,v,payload from cow_pk ignore index(ux,ix) order by id;
create table cow_fake (id int, u int, v int, payload varchar(10), unique key ux(u), key ix(v));
insert into cow_fake values (1,101,201,'one'),(2,102,202,'two');
alter table cow_fake modify column payload varchar(20), algorithm=copy;
select id,u,v,payload from cow_fake force index(ux) where u in (101,102) order by id;
select id,u,v,payload from cow_fake force index(ix) where v in (201,202) order by id;
select id,u,v,payload from cow_fake ignore index(ux,ix) order by id;
-- An unchanged generated primary key retains consistent secondary indexes.
create table generated_pk (a int, id int generated always as (a*2) stored, v int, primary key(id), key ix(v));
insert into generated_pk(a,v) values (1,101),(2,102);
alter table generated_pk algorithm=copy, add column extra int default 9;
select a,id,v,extra from generated_pk force index(ix) where v in (101,102) order by a;
select a,id,v,extra from generated_pk ignore index(ix) where v in (101,102) order by a;
update generated_pk set v=103 where a=1;
delete from generated_pk where a=2;
select a,id,v,extra from generated_pk force index(ix) where v in (101,102,103) order by a;
select a,id,v,extra from generated_pk ignore index(ix) where v in (101,102,103) order by a;

-- A changed input can change a generated primary key without changing ix(v).
create table generated_input (a decimal(5,2), id decimal(6,2) generated always as (a*2) stored, v int, primary key(id), key ix(v));
insert into generated_input(a,v) values (1.21,101),(2.21,102);
alter table generated_input algorithm=copy, change a a decimal(5,1);
select a,id,v from generated_input force index(ix) where v in (101,102) order by v;
select a,id,v from generated_input ignore index(ix) where v in (101,102) order by v;

-- Preserve stored generated values when only the session time zone changed.
set @saved_time_zone = @@time_zone;
set time_zone = '+00:00';
create table generated_timezone (a bigint, id datetime generated always as (from_unixtime(a)) stored, v int, primary key(id), key ix(v));
insert into generated_timezone(a,v) values (1700000000,10),(1700003600,20);
set time_zone = '+08:00';
alter table generated_timezone algorithm=copy, add column extra int default 9;
select id,v from generated_timezone force index(ix) where v in (10,20) order by v;
select id,v from generated_timezone ignore index(ix) where v in (10,20) order by v;
set time_zone = @saved_time_zone;

-- Preserve a referenced stored generated key; reject changes that need to recompute it.
set time_zone = '+00:00';
create table generated_parent (a bigint, id datetime generated always as (from_unixtime(a)) stored, primary key(id));
create table generated_child (id int primary key, pid datetime, constraint fk_generated_parent foreign key(pid) references generated_parent(id));
insert into generated_parent(a) values (1700000000);
insert into generated_child select 1,id from generated_parent;
set time_zone = '+08:00';
alter table generated_parent algorithm=copy, add column extra int default 9;
select c.id,c.pid,p.id from generated_child c left join generated_parent p on c.pid=p.id;
set time_zone = @saved_time_zone;
create table generated_parent_changed (a decimal(5,2), id decimal(6,2) generated always as (a*2) stored, primary key(id));
create table generated_child_changed (id int primary key, pid decimal(6,2), constraint fk_generated_changed foreign key(pid) references generated_parent_changed(id));
insert into generated_parent_changed(a) values (1.21);
insert into generated_child_changed select 1,id from generated_parent_changed;
alter table generated_parent_changed algorithm=copy, change a a decimal(5,1);
select c.id,c.pid,p.id from generated_child_changed c left join generated_parent_changed p on c.pid=p.id;

-- Child-side recomputation uses the ordinary foreign-key check on COPY INSERT.
create table generated_ref_parent (id decimal(6,2) primary key);
insert into generated_ref_parent values (2.42),(2.40);
create table generated_ref_child (a decimal(5,2), g decimal(6,2) generated always as (a*2) stored, constraint fk_generated_ref foreign key(g) references generated_ref_parent(id));
insert into generated_ref_child(a) values (1.21);
alter table generated_ref_child algorithm=copy, change a a decimal(5,1);
select a,g from generated_ref_child;
create table generated_ref_parent_bad (id decimal(6,2) primary key);
insert into generated_ref_parent_bad values (2.42);
create table generated_ref_child_bad (a decimal(5,2), g decimal(6,2) generated always as (a*2) stored, constraint fk_generated_ref_bad foreign key(g) references generated_ref_parent_bad(id));
insert into generated_ref_child_bad(a) values (1.21);
alter table generated_ref_child_bad algorithm=copy, change a a decimal(5,1);
select a,g from generated_ref_child_bad;

-- A child remains referenced until its last foreign key to the parent is dropped.
create table multiple_fk_parent (id int primary key);
insert into multiple_fk_parent values (1);
create table multiple_fk_child (id int primary key, a int, b int, constraint mfk1 foreign key(a) references multiple_fk_parent(id), constraint mfk2 foreign key(b) references multiple_fk_parent(id));
alter table multiple_fk_child drop foreign key mfk1;
alter table multiple_fk_parent algorithm=copy, add column extra int;
insert into multiple_fk_child values (1,null,1);
insert into multiple_fk_child values (2,null,9);
select * from multiple_fk_child;

create table multiple_fk_self (id int primary key, a int, b int, constraint sfk1 foreign key(a) references multiple_fk_self(id), constraint sfk2 foreign key(b) references multiple_fk_self(id));
insert into multiple_fk_self values (1,null,null);
alter table multiple_fk_self drop foreign key sfk1;
alter table multiple_fk_self algorithm=copy, add column extra int;
insert into multiple_fk_self(id,b) values (2,9);
alter table multiple_fk_self drop foreign key sfk2;
alter table multiple_fk_self algorithm=copy, modify id bigint;
select id,a,b from multiple_fk_self;

-- A self-reference must remain valid and cannot target a recomputed key.
create table generated_self (a decimal(5,2), g decimal(6,2) generated always as (a*2) stored, ref decimal(6,2), primary key(g), constraint fk_generated_self foreign key(ref) references generated_self(g));
insert into generated_self(a,ref) values (1.21,null);
update generated_self set ref=2.42;
alter table generated_self algorithm=copy, add column extra int;
select a,g,ref from generated_self;
alter table generated_self algorithm=copy, change a a decimal(5,1);
select a,g,ref from generated_self;

-- MatrixOne materializes virtual generated columns; preserve them in COPY too.
create table virtual_parent (id int primary key);
insert into virtual_parent values (2);
create table virtual_child (a int, g int generated always as (a+1) virtual, constraint fk_virtual_child foreign key(g) references virtual_parent(id));
insert into virtual_child(a) values (1);
alter table virtual_child algorithm=copy, add column extra int;
select c.a,c.g,p.id from virtual_child c join virtual_parent p on c.g=p.id;
set time_zone = '+00:00';
create table generated_chain (a bigint, v datetime generated always as (from_unixtime(a)) virtual, g datetime generated always as (v) stored, primary key(g));
insert into generated_chain(a) values (1700000000);
set time_zone = '+08:00';
alter table generated_chain algorithm=copy, add column extra int;
select g from generated_chain;
set time_zone = @saved_time_zone;

-- An explicitly stored auto-increment zero is a value, not a request to allocate.
set @saved_sql_mode = @@sql_mode;
create table auto_zero (id bigint auto_increment primary key, v int, key ix(v));
set sql_mode = 'NO_AUTO_VALUE_ON_ZERO';
insert into auto_zero(id,v) values (0,10),(1,20);
set sql_mode = '';
alter table auto_zero algorithm=copy, add column extra int default 9;
select id,v from auto_zero force index(ix) where v in (10,20) order by v;
select id,v from auto_zero ignore index(ix) where v in (10,20) order by v;
insert into auto_zero(id,v) values (0,30);
select id > 1 from auto_zero where v=30;

-- The COPY marker must also survive when no dedup or index option is enabled.
create table auto_plain (id bigint auto_increment primary key, v int);
set sql_mode = 'NO_AUTO_VALUE_ON_ZERO';
insert into auto_plain(id,v) values (0,10),(1,20);
set sql_mode = '';
alter table auto_plain algorithm=copy, modify column id bigint unsigned auto_increment;
select id,v from auto_plain order by v;

-- A newly added auto-increment column still needs allocation for existing rows.
create table auto_added (v int);
insert into auto_added values (10),(20);
alter table auto_added algorithm=copy, add column id bigint not null auto_increment, add primary key(id);
insert into auto_added(v) values (30);
select count(*),count(distinct id),min(id)>0 from auto_added;
set sql_mode = @saved_sql_mode;

-- Equal ENUM/SET definitions must copy ordinals and bitmaps, not display text.
create table enum_copy (id int primary key, e enum('a','b',''), unique key ux(e));
insert ignore into enum_copy values (1,'bad'),(2,'');
select id,e+0 from enum_copy order by id;
alter table enum_copy algorithm=copy, add column extra int;
select id,e+0 from enum_copy force index(ux) where e in ('','a') order by id;
select id,e+0 from enum_copy ignore index(ux) where e in ('','a') order by id;
select id,e+0 from enum_copy order by id;
insert into enum_copy(id,e) values (3,'');
create table set_copy (id int primary key, s set('','a'), key ix(s));
insert into set_copy values (1,0),(2,1);
select id,cast(s as unsigned) from set_copy order by id;
alter table set_copy algorithm=copy, add column extra int;
select id,cast(s as unsigned) from set_copy order by id;

-- PK changes and a vector-column rename must both reach the index rebuild.
set @saved_ivf = @@experimental_ivf_index;
set @saved_probe_limit = @@probe_limit;
set experimental_ivf_index = 1;
set probe_limit = 10;
create table vector_pk (a int, b int, v vecf32(3), primary key(a,b));
insert into vector_pk values (1,1,'[1,1,1]'),(2,2,'[2,2,2]'),(10,10,'[100,100,100]'),(11,11,'[101,101,101]'),(20,20,'[500,500,500]'),(21,21,'[501,501,501]');
create index vector_ix using ivfflat on vector_pk(v) lists=3 op_type 'vector_l2_ops';
alter table vector_pk algorithm=copy, modify column a bigint, rename column v to v2;
set @centroid_sql = concat('select count(*),sum(case when __mo_index_centroid is null then 1 else 0 end) from `', (select index_table_name from mo_catalog.mo_indexes where name='vector_ix' and algo_table_type='centroids' and table_id=(select rel_id from mo_catalog.mo_tables where reldatabase=database() and relname='vector_pk')), '`');
prepare centroid_query from @centroid_sql;
execute centroid_query;
deallocate prepare centroid_query;
select a from vector_pk order by l2_distance(v2,'[1,1,1]') limit 1;
select a from vector_pk order by l2_distance(v2,'[100,100,100]') limit 1;
select a from vector_pk order by l2_distance(v2,'[500,500,500]') limit 1;
set experimental_ivf_index = @saved_ivf;
set probe_limit = @saved_probe_limit;
drop database alter_copy_primary_index;

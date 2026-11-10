-- A replacement column remains a new value source across later ALTER clauses.
drop database if exists alter_copy_source_42;
create database alter_copy_source_42;
use alter_copy_source_42;
create table plain (id int primary key, v int);
insert into plain values (1,10),(2,20);
alter table plain drop column v, add column v int default 7;
select * from plain order by id;
create table u (id int primary key, v int, unique key uv(v));
insert into u values (1,10),(2,20);
alter table u drop column v, add column v int default 7 unique, modify column v int default 7;
show create table u;
select * from u force index(uv) order by id;
insert into u values (3,30);
select * from u ignore index(uv) order by id;
create table renamed (id int primary key, v int);
insert into renamed values (1,10),(2,20);
alter table renamed drop column v, add column v int default 7, change column v w int default 7;
select * from renamed order by id;
create table p (id int primary key, v int);
insert into p values (1,10),(2,20);
alter table p drop primary key, drop column id, add column id int default 8, add primary key(id);
show create table p;
select * from p order by id;
insert into p values (3,30);
select * from p order by id;

-- A reused name must not cause the old primary key to validate a different source.
create table chain_bad (id decimal(10,0) primary key, src decimal(10,0) not null, payload int, key ix(payload));
insert into chain_bad values (1,8,10),(2,8,20);
alter table chain_bad algorithm=copy, drop primary key, add primary key(src), drop column id, rename column src to mid, rename column mid to id;
show create table chain_bad;
select id,src,payload from chain_bad force index(ix) where payload in (10,20) order by id;
insert into chain_bad values (3,9,30);
select id,src,payload from chain_bad ignore index(ix) order by id;

create table chain_ok (id decimal(10,0) primary key, src decimal(10,0) not null, payload int);
insert into chain_ok values (1,8,10),(2,9,20);
alter table chain_ok algorithm=copy, drop primary key, add primary key(src), drop column id, rename column src to mid, rename column mid to id;
select id,payload from chain_ok order by id;
drop database alter_copy_source_42;

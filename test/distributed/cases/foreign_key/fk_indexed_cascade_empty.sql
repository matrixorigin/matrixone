-- Indexed foreign-key CASCADE must delete only the action row set.

drop database if exists fk_indexed_cascade_empty;
create database fk_indexed_cascade_empty;
use fk_indexed_cascade_empty;

-- single: unrelated children, child-delete-first rollback, and matching CASCADE.
create table p_single(id varchar(255) primary key);
create table c_single(id int primary key, pid varchar(255), note int, key idx_pid(pid),
    foreign key(pid) references p_single(id) on delete cascade);
insert into p_single values ('empty'),('match'),('other');
insert into c_single values (1,'match',10),(2,'other',20);
begin;
delete from c_single where pid='match';
prepare clear_parent from 'delete from p_single where id=?';
set @target_parent='match';
execute clear_parent using @target_parent;
deallocate prepare clear_parent;
select id from p_single order by id;
select id,pid,note from c_single order by id;
rollback;
select id from p_single order by id;
select id,pid,note from c_single order by id;
delete from p_single where id='empty';
select id from p_single order by id;
select id,pid,note from c_single where pid='other' order by id;
prepare delete_parent from 'delete from p_single where id=?';
set @target_parent='match';
execute delete_parent using @target_parent;
deallocate prepare delete_parent;
select id from p_single order by id;
select id,pid,note from c_single order by id;
drop table c_single;
drop table p_single;

-- composite: unrelated children, child-delete-first rollback, and matching CASCADE.
create table p_composite(id varchar(255) primary key);
create table c_composite(id int primary key, pid varchar(255), note int, key idx_pid(pid,note),
    foreign key(pid) references p_composite(id) on delete cascade);
insert into p_composite values ('empty'),('match'),('other');
insert into c_composite values (1,'match',10),(2,'other',20);
begin;
delete from c_composite where pid='match';
prepare clear_parent from 'delete from p_composite where id=?';
set @target_parent='match';
execute clear_parent using @target_parent;
deallocate prepare clear_parent;
select id from p_composite order by id;
select id,pid,note from c_composite order by id;
rollback;
select id from p_composite order by id;
select id,pid,note from c_composite order by id;
delete from p_composite where id='empty';
select id from p_composite order by id;
select id,pid,note from c_composite where pid='other' order by id;
prepare delete_parent from 'delete from p_composite where id=?';
set @target_parent='match';
execute delete_parent using @target_parent;
deallocate prepare delete_parent;
select id from p_composite order by id;
select id,pid,note from c_composite order by id;
drop table c_composite;
drop table p_composite;

-- no_index: unrelated children, child-delete-first rollback, and matching CASCADE.
create table p_no_index(id varchar(255) primary key);
create table c_no_index(id int primary key, pid varchar(255), note int,
    foreign key(pid) references p_no_index(id) on delete cascade);
insert into p_no_index values ('empty'),('match'),('other');
insert into c_no_index values (1,'match',10),(2,'other',20);
begin;
delete from c_no_index where pid='match';
prepare clear_parent from 'delete from p_no_index where id=?';
set @target_parent='match';
execute clear_parent using @target_parent;
deallocate prepare clear_parent;
select id from p_no_index order by id;
select id,pid,note from c_no_index order by id;
rollback;
select id from p_no_index order by id;
select id,pid,note from c_no_index order by id;
delete from p_no_index where id='empty';
select id from p_no_index order by id;
select id,pid,note from c_no_index where pid='other' order by id;
prepare delete_parent from 'delete from p_no_index where id=?';
set @target_parent='match';
execute delete_parent using @target_parent;
deallocate prepare delete_parent;
select id from p_no_index order by id;
select id,pid,note from c_no_index order by id;
drop table c_no_index;
drop table p_no_index;

-- UNIQUE lookup has a separate delete filter and omits NULL keys.
create table p_unique(id varchar(255) primary key);
create table c_unique(id int primary key,pid varchar(255),unique key idx_pid(pid),
    foreign key(pid) references p_unique(id) on delete cascade);
insert into p_unique values ('empty'),('match'),('other');
insert into c_unique values (1,'match'),(2,null);
delete from p_unique where id='empty';
select id from p_unique order by id;
select id,pid from c_unique order by id;
delete from p_unique where id='match';
select id from p_unique order by id;
select id,pid from c_unique order by id;
drop table c_unique;
drop table p_unique;

-- Self-referencing secondary index: REPLACE cascades, empty actions and key reuse.
create table self_secondary(id int primary key,parent_id int,key idx_parent(parent_id),
    foreign key(parent_id) references self_secondary(id) on delete cascade);
insert into self_secondary values (1,null),(2,1),(3,2),(4,null),(5,4);
replace into self_secondary values (1,null);
select id,parent_id from self_secondary order by id;
select id,parent_id from self_secondary force index(idx_parent) where parent_id=4 order by id;
replace into self_secondary values (1,null);
select id,parent_id from self_secondary order by id;
insert into self_secondary values (2,1);
select id,parent_id from self_secondary force index(idx_parent) where parent_id=1 order by id;
delete from self_secondary where id=1;
select id,parent_id from self_secondary order by id;
select id,parent_id from self_secondary force index(idx_parent) where parent_id=4 order by id;
drop table self_secondary;

-- Self-referencing unique index: REPLACE cascades, empty actions and key reuse.
create table self_unique(id int primary key,parent_id int,unique key idx_parent(parent_id),
    foreign key(parent_id) references self_unique(id) on delete cascade);
insert into self_unique values (1,null),(2,1),(3,2),(4,null),(5,4);
replace into self_unique values (1,null);
select id,parent_id from self_unique order by id;
select id,parent_id from self_unique force index(idx_parent) where parent_id=4 order by id;
replace into self_unique values (1,null);
select id,parent_id from self_unique order by id;
insert into self_unique values (2,1);
select id,parent_id from self_unique force index(idx_parent) where parent_id=1 order by id;
delete from self_unique where id=1;
select id,parent_id from self_unique order by id;
select id,parent_id from self_unique force index(idx_parent) where parent_id=4 order by id;
drop table self_unique;

-- Indexed RESTRICT permits an empty match and still rejects a live child.
create table p_restrict(id varchar(255) primary key);
create table c_restrict(id int primary key,pid varchar(255),key idx_pid(pid),
    foreign key(pid) references p_restrict(id) on delete restrict);
insert into p_restrict values ('empty'),('other');
insert into c_restrict values (2,'other');
delete from p_restrict where id='empty';
delete from p_restrict where id='other';
select id from p_restrict order by id;
select id,pid from c_restrict order by id;
drop database fk_indexed_cascade_empty;

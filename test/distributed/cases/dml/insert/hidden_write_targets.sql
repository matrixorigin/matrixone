-- @suit
-- @case
-- @desc: Hidden storage columns are never explicit SQL write targets.
-- @label:bvt
drop database if exists hidden_write_targets;
create database hidden_write_targets;
use hidden_write_targets;
create table composite_key(a int,b int,v int,primary key(a,b),key idx_v(v));
insert into composite_key values(1,10,7);

-- Reject input and assignment targets before any visible or hidden write.
insert into composite_key(__mo_cpkey_col,a,b,v) values(unhex('0001'),1,99,8);
insert into composite_key(a,b,v,__mo_cpkey_col) select 1,99,8,unhex('0001');
replace into composite_key(a,b,v,__mo_cpkey_col) values(1,99,8,unhex('0001'));
insert ignore into composite_key(a,b,__mo_cpkey_col) values(1,99,default);
update composite_key set v=999,__mo_cpkey_col=unhex('0001') where a=1 and b=10;
insert into composite_key values(1,10,999) on duplicate key update __mo_cpkey_col=default;
prepare hidden_target from 'insert into composite_key(a,b,v,__mo_cpkey_col) values(?,?,?,?)';
select a,b,v,__mo_cpkey_col=serial(a,b) as key_matches from composite_key order by a,b;

-- Ordinary writes derive the key and maintain both incremental and new indexes.
insert into composite_key(a,b,v) select 2,20,8;
update composite_key set b=11 where a=1 and b=10;
insert into composite_key values(1,11,9) on duplicate key update v=values(v);
replace into composite_key values(2,20,10);
create index idx_b on composite_key(b);
select a,b,v,__mo_cpkey_col=serial(a,b) as key_matches from composite_key order by a,b;
select a,b,v from composite_key force index(idx_v) where v=10;
select a,b,v from composite_key force index(idx_b) where b=11;
insert into composite_key values(1,11,100);
select count(*) as rows_after_duplicate from composite_key;

create table no_key(a int);
insert into no_key values(1);
insert into no_key(a,__mo_fake_pk_col) values(2,777);
update no_key set a=999,__mo_fake_pk_col=777 where a=1;
insert into no_key values(2) on duplicate key update __mo_fake_pk_col=default;
load data inline format='csv',data='2,777\n' into table no_key fields terminated by ',' (a,__mo_fake_pk_col);
load data inline format='csv',data='2,777\n' into table no_key fields terminated by ',' (a,@v) set a=nullif(a,0),__mo_fake_pk_col=nullif(@v,0);
select a from no_key order by a;
insert into no_key values(2) on duplicate key update a=values(a);
load data inline format='csv',data='3\n' into table no_key;
select a from no_key order by a;
select count(distinct __mo_fake_pk_col)=count(*) as unique_generated_keys from no_key;
-- Internal COPY preserves fake keys used by shallow-cloned indexes, including tombstones.
create table copy_keys(a int,v int,key idx_a(a));
insert into copy_keys values(1,10),(2,20),(3,30);
delete from copy_keys where a=2;
create table saved_keys as select a,__mo_fake_pk_col as saved_key from copy_keys;
alter table copy_keys add column extra int, algorithm=copy;
select count(*)=2 and sum(c.__mo_fake_pk_col=s.saved_key)=2 as identities_preserved from copy_keys c join saved_keys s on c.a=s.a;
select a,v from copy_keys force index(idx_a) where a=3;
delete from copy_keys where a=3;
select a,v from copy_keys order by a;
insert into copy_keys(a,v) values(4,40);
select count(distinct __mo_fake_pk_col)=count(*) as unique_copy_keys from copy_keys;
insert into copy_keys(a,__mo_fake_pk_col) values(9,777);
select count(*) as rows_after_rejected_key from copy_keys;
create table copy_generated(a int,g int generated always as(a+1) stored);
insert into copy_generated(a) values(5);
create table saved_generated as select __mo_fake_pk_col as saved_key from copy_generated;
alter table copy_generated add column extra int, algorithm=copy;
select a,g,c.__mo_fake_pk_col=s.saved_key as generated_identity_preserved from copy_generated c cross join saved_generated s;
drop database hidden_write_targets;

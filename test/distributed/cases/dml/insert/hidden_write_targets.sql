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
drop database hidden_write_targets;

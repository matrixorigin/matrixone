drop table if exists dis_table_02;
create table dis_table_02(a int not null auto_increment,b varchar(25) not null,c datetime,primary key(a),key bstr (b),key cdate (c) );
insert into dis_table_02(b,c) values ('aaaa','2020-09-08');
insert into dis_table_02(b,c) values ('aaaa','2020-09-08');

begin;
update dis_table_02 set b='tittttt' where a>1;

-- @session:id=1{
use ww_conflict;
begin;
-- @wait:0:commit
update dis_table_02 set b='dpqweoe' where a>1;
-- @session}

commit;

-- @session:id=1{
start transaction;
-- @session}

drop table if exists dis_table_02;

-- transcation: w-w conflict
drop table if exists dis_table_02;
create table dis_table_02(a int not null auto_increment,b varchar(25) not null,c datetime,primary key(a),key bstr (b),key cdate (c) );
insert into dis_table_02(b,c) values ('aaaa','2020-09-08');
insert into dis_table_02(b,c) values ('aaaa','2020-09-08');
begin;
alter table dis_table_02 rename column a to newA;
-- @session:id=1{
use ww_conflict;
begin;
-- @wait:0:commit
update dis_table_02 set b='dpqweoe' where a>1;
update dis_table_02 set b='dpqweoe' where newA>1;
commit;
select * from dis_table_02;
-- @session}
select * from dis_table_02;
drop table dis_table_02;

---------------------------------------------------
-- alter table add primary key
drop table if exists dis_table_02;
create table dis_table_02(a int not null default 10,b varchar(25) not null,c datetime);
insert into dis_table_02(b,c) values ('aaaa','2020-09-08');
insert into dis_table_02(b,c) values ('bbbb','2020-09-08');
begin;
alter table dis_table_02 add constraint primary key (b);
-- @session:id=1{
use ww_conflict;
begin;
-- @wait:0:commit
update dis_table_02 set b='dpqweoe' where a>1;
commit;
select * from dis_table_02;
-- @session}
select * from dis_table_02;
drop table dis_table_02;

-------------------------------------------------------------
-- alter table drop primary key
drop table if exists dis_table_02;
create table dis_table_02(a int not null default 10,b varchar(25) not null,c datetime,primary key (b));
insert into dis_table_02(b,c) values ('aaaa','2020-09-08');
insert into dis_table_02(b,c) values ('bbbb','2020-09-08');
begin;
alter table dis_table_02 drop primary key;
-- @session:id=1{
use ww_conflict;
begin;
-- @wait:0:commit
insert into dis_table_02 values(10, 'aaaa', '1998-09-12');
commit;
select * from dis_table_02;
-- @session}
select * from dis_table_02;
drop table dis_table_02;


create table t1 (a int);
insert into t1 values (1), (2), (3);
begin;
delete from t1 where a = 1; -- dels in workspace
drop table t1;  -- the previous delete should be canceled
show tables;    -- no error in transfer deletes
commit;

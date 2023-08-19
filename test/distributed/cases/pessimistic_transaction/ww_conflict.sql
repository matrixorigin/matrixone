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
-- @bvt:issue#11268
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
-- @bvt:issue

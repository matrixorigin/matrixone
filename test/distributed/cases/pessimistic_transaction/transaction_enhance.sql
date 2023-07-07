-- truncate table
drop table if exists atomic_table_10;
create table atomic_table_10(c1 int,c2 varchar(25));
insert into atomic_table_10 values (3,"a"),(4,"b"),(5,"c");
start transaction ;
truncate table atomic_table_10;
-- @session:id=2&user=sys:dump&password=111
use transaction_enhance;
select * from atomic_table_10;
-- @session
select * from atomic_table_10;
commit;
select * from atomic_table_10;

drop table if exists atomic_table_10;
create table atomic_table_10(c1 int,c2 varchar(25));
insert into atomic_table_10 values (3,"a"),(4,"b"),(5,"c");
start transaction ;
truncate table atomic_table_10;
-- @session:id=2&user=sys:dump&password=111
use transaction_enhance;
select * from atomic_table_10;
-- @session
select * from atomic_table_10;
rollback;
select * from atomic_table_10;

drop table if exists atomic_table_10;
create table atomic_table_10(c1 int,c2 varchar(25));
insert into atomic_table_10 values (3,"a"),(4,"b"),(5,"c");
begin ;
truncate table atomic_table_10;
-- @bvt:issue#8848
-- @session:id=2&user=sys:dump&password=111
use transaction_enhance;
insert into atomic_table_10 values (6,"a"),(7,"b"),(8,"c");
select * from atomic_table_10;
-- @session
-- @bvt:issue
select * from atomic_table_10;
commit;
select * from atomic_table_10;

-- drop table

drop table if exists atomic_table_11;
create table atomic_table_11(c1 int,c2 varchar(25));
insert into atomic_table_11 values (3,"a"),(4,"b"),(5,"c");
begin;
drop table atomic_table_11;

-- @session:id=2&user=sys:dump&password=111
-- @wait:0:commit
use transaction_enhance;
insert into atomic_table_11 values (6,"a");
select * from atomic_table_11;
-- @session

commit;
select * from atomic_table_11;

drop table if exists atomic_table_11;
create table atomic_table_11(c1 int,c2 varchar(25));
insert into atomic_table_11 values (3,"a"),(4,"b"),(5,"c");
begin;
drop table atomic_table_11;

-- @session:id=2&user=sys:dump&password=111
-- @wait:0:rollback
use transaction_enhance;
insert into atomic_table_11 values (6,"a");
select * from atomic_table_11;
-- @session
rollback ;
select * from atomic_table_11;

drop table if exists atomic_table_11;
create table atomic_table_11(c1 int,c2 varchar(25));
insert into atomic_table_11 values (3,"a"),(4,"b"),(5,"c");
begin;
-- @session:id=2&user=sys:dump&password=111
use transaction_enhance;
drop table atomic_table_11;
-- @session
drop table atomic_table_11;
commit;

--alter table
drop table if exists atomic_table_12;
create table atomic_table_12(c1 int,c2 varchar(25));
insert into atomic_table_12 values (3,"a"),(4,"b"),(5,"c");
begin;
alter table atomic_table_12 add index key1(c1);
alter table atomic_table_12 alter index key1 visible;
-- @session:id=2&user=sys:dump&password=111
use transaction_enhance;
show create table atomic_table_12;
-- @session
commit;
show create table atomic_table_12;
show index from atomic_table_12;

use transaction_enhance;
drop table if exists atomic_table_12_1;
create table atomic_table_12_1(c1 int,c2 varchar(25));
insert into atomic_table_12_1 values (3,"a"),(4,"b"),(5,"c");
begin;
alter table atomic_table_12_1 add index key1(c1);
alter table atomic_table_12_1 alter index key1 visible;
-- @session:id=2&user=sys:dump&password=111
use transaction_enhance;
show create table atomic_table_12_1;
-- @session
rollback;
show create table atomic_table_12_1;
show index from atomic_table_12_1;

drop table if exists atomic_table_12_2;
drop table if exists atomic_table_13;
create table atomic_table_12_2(c1 int primary key,c2 varchar(25));
insert into atomic_table_12_2 values (3,"a"),(4,"b"),(5,"c");
create table atomic_table_13(c1 int primary key,c2 varchar(25));
insert into atomic_table_13 values (3,"d"),(4,"e"),(5,"f");
begin;
alter table atomic_table_13 add constraint ffa foreign key f_a(c1) references atomic_table_12_2(c1);
-- @session:id=2&user=sys:dump&password=111
use transaction_enhance;
show create table atomic_table_12_2;
insert into atomic_table_13 values (8,"h");
select * from atomic_table_13;
-- @session
insert into atomic_table_13 values (6,"h");
commit;
show create table atomic_table_13;

drop table if exists atomic_table_12_3;
drop table if exists atomic_table_13;
create table atomic_table_12_3(c1 int primary key,c2 varchar(25));
insert into atomic_table_12_3 values (3,"a"),(4,"b"),(5,"c");
create table atomic_table_13(c1 int primary key,c2 varchar(25));
insert into atomic_table_13 values (3,"d"),(4,"e"),(5,"f");
alter table atomic_table_13 add constraint ffa foreign key f_a(c1) references atomic_table_12_3(c1);
begin;
alter table atomic_table_13 drop foreign key ffa;
-- @session:id=2&user=sys:dump&password=111
use transaction_enhance;
insert into atomic_table_13 values (8,"h");
select * from atomic_table_13;
-- @session
commit;
show create table atomic_table_13;

drop table if exists atomic_table_12_4;
drop table if exists atomic_table_13;
create table atomic_table_12_4(c1 int primary key,c2 varchar(25));
insert into atomic_table_12_4 values (3,"a"),(4,"b"),(5,"c");
create table atomic_table_13(c1 int primary key,c2 varchar(25));
insert into atomic_table_13 values (3,"d"),(4,"e"),(5,"f");
alter table atomic_table_13 add constraint ffa foreign key f_a(c1) references atomic_table_12_4(c1);
begin;
alter table atomic_table_13 drop foreign key ffa;
-- @session:id=2&user=sys:dump&password=111
use transaction_enhance;
insert into atomic_table_13 values (8,"h");
select * from atomic_table_13;
-- @session
rollback ;
show create table atomic_table_13;

drop table if exists atomic_table_12_5;
drop table if exists atomic_table_13;
create table atomic_table_12_5(c1 int,c2 varchar(25));
insert into atomic_table_12_5 values (3,"a"),(4,"b"),(5,"c");
alter table atomic_table_12_5 add index key1(c1);
begin;
alter table atomic_table_12_5 drop index key1;
-- @session:id=2&user=sys:dump&password=111
use transaction_enhance;
show create table atomic_table_12_5;
select * from atomic_table_12_5;
-- @session
commit;
show index from atomic_table_12_5;

-- w-w conflict
drop table if exists atomic_table_14;
create table atomic_table_14(c1 int,c2 varchar(25));
insert into atomic_table_14 values (3,"a"),(4,"b"),(5,"c");
start transaction ;
alter table atomic_table_14 add  index key1(c1);
-- @session:id=2&user=sys:dump&password=111
use transaction_enhance;
drop table atomic_table_14;
-- @session
insert into atomic_table_14 values (6,"a"),(7,"b");
select * from atomic_table_14;
commit;
select * from atomic_table_14;

drop table if exists atomic_table_15;
create table atomic_table_15(c1 int,c2 varchar(25));
begin;
insert into atomic_table_15 values (6,"a"),(7,"b");
truncate table atomic_table_15;
-- @session:id=2&user=sys:dump&password=111
-- @wait:0:commit
use transaction_enhance;
drop table atomic_table_15;
-- @session
select * from atomic_table_15;
commit;
select * from atomic_table_15;

drop table if exists atomic_table_16;
create table atomic_table_16(c1 int,c2 varchar(25));
begin;
insert into atomic_table_16 values (6,"a"),(7,"b");
drop table atomic_table_16;
-- @session:id=2&user=sys:dump&password=111
-- @wait:0:commit
use transaction_enhance;
drop table atomic_table_16;
-- @session
commit;
select * from atomic_table_16;

drop table if exists atomic_table_17;
create table atomic_table_17(c1 int,c2 varchar(25));
begin;
insert into atomic_table_17 values (6,"a"),(7,"b");
drop table atomic_table_17;
-- @session:id=2&user=sys:dump&password=111
use transaction_enhance;
alter table atomic_table_17 add constraint unique key (c1);
update atomic_table_17 set c1=8 where c2="b";
-- @session
commit;
select * from atomic_table_17;

-- create/drop database，sequence ,create/drop account
start transaction ;
create database tdb;
-- @session:id=2&user=sys:dump&password=111
use tdb;
-- @session
drop database tdb;
commit;

begin;
create sequence seq_01 as int start 30;
-- @session:id=2&user=sys:dump&password=111
use transaction_enhance;
select nextval('seq_01');
-- @session
commit;
select nextval('seq_01');

drop table if exists atomic_table_11;
drop account if exists trans_acc1;
create account trans_acc1 admin_name "admin" identified by "111";
begin;
drop account trans_acc1;
-- @session:id=2&user=trans_acc1:admin&password=111
use transaction_enhance;
select count(*) from mo_catalog.mo_account where account_name='trans_acc1';
-- @session
commit;
select count(*) from mo_catalog.mo_account where account_name='trans_acc1';

-- autocommit
drop table if exists atomic_table_18;
create table atomic_table_18(c1 int,c2 varchar(25));
insert into atomic_table_18 values (6,"a"),(7,"b");
set autocommit=0;
alter table atomic_table_18 add index key1(c1);
alter table atomic_table_18 alter index key1 visible;
-- @session:id=2&user=sys:dump&password=111
use transaction_enhance;
show create table atomic_table_18;
-- @session
rollback;
show create table atomic_table_18;
show index from atomic_table_18;

truncate table atomic_table_18;
-- @session:id=2&user=sys:dump&password=111
-- @wait:0:commit
use transaction_enhance;
drop table atomic_table_18;
-- @session
select * from atomic_table_18;
commit;
select * from atomic_table_18;

set autocommit=1;
drop table if exists atomic_table_18;
create table atomic_table_18(c1 int,c2 varchar(25));
insert into atomic_table_18 values (6,"a"),(7,"b");
set autocommit=0;
drop table atomic_table_18;
-- @session:id=2&user=sys:dump&password=111
-- @wait:0:commit
use transaction_enhance;
drop table atomic_table_18;
-- @session
select * from atomic_table_18;
commit;
select * from atomic_table_18;
set autocommit=1;
drop account if exists trans_acc1;

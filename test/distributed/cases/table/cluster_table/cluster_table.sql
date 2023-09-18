
-- test sys tenement non-system database, create cluster table.
drop database if exists test_db1;
create database test_db1;
use test_db1;
drop table if exists t1;
create cluster table t1(a int, b int);
drop database test_db1;


-- test sys tenement system database, create cluster table.(only mo_catalog support)
use mo_task;
drop table if exists t2;
create cluster table t2(a int, b int);

use information_schema;
drop table if exists t3;
create cluster table t3(a int, b int);
desc t3;
drop table t3;

use mysql;
drop table if exists t4;
create cluster table t4(a int, b int);
desc t4;
drop table t4;

use system_metrics;
drop table if exists t5;
create cluster table t5(a int, b int);
desc t5;
drop table t5;

use system;
drop table if exists t6;
create cluster table t6(a int, b int);
desc t6;
drop table t6;

use mo_catalog;
drop table if exists t7;
create cluster table t7(a int, b int);
desc t7;
drop table t7;

-- test system tenant inserts data into the cluster table
use mo_catalog;
drop table if exists cluster_table_1;
create cluster table cluster_table_1(a int, b int);

drop account if exists test_account1;
create account test_account1 admin_name = 'test_user' identified by '111';

drop account if exists test_account2;
create account test_account2 admin_name = 'test_user' identified by '111';

insert into cluster_table_1 values(0,0,0),(1,1,0);
insert into cluster_table_1 values(0,0,1),(1,1,1);
insert into cluster_table_1 values(0,0,2),(1,1,2) on duplicate key update b=b;
update cluster_table_1 set account_id=(select account_id from mo_account where account_name="test_account1") where account_id=1;
update cluster_table_1 set account_id=(select account_id from mo_account where account_name="test_account2") where account_id=2;
select a,b from cluster_table_1;

-- @session:id=2&user=test_account1:test_user&password=111
use mo_catalog;
select * from cluster_table_1;
-- @session

-- @session:id=3&user=test_account2:test_user&password=111
use mo_catalog;
select * from cluster_table_1;
-- @session

insert into cluster_table_1 values(200,200, 0);
insert into cluster_table_1 values(100,100, 0);
insert into cluster_table_1 values(50,50, 0);
select a,b from cluster_table_1;

-- @session:id=2&user=test_account1:test_user&password=111
use mo_catalog;
select * from cluster_table_1;
-- @session

-- @session:id=3&user=test_account2:test_user&password=111
use mo_catalog;
select * from cluster_table_1;
-- @session

drop table cluster_table_1;


-- test system tenant load data into the cluster table
drop table if exists cluster_table_2;
create cluster table cluster_table_2(
col1 int,
col2 float,
col3 decimal,
col4 date,
col5 bool,
col6 json,
col7 blob,
col8 text,
col9 varchar
);

load data infile '$resources/load_data/cluster_table.csv' into table cluster_table_2;
update cluster_table_2 set account_id=(select account_id from mo_account where account_name="test_account1") where account_id=1;
update cluster_table_2 set account_id=(select account_id from mo_account where account_name="test_account2") where account_id=2;
select col1,col2,col3,col4,col5,col6,col7,col8,col9 from cluster_table_2;

-- @session:id=2&user=test_account1:test_user&password=111
use mo_catalog;
select * from cluster_table_2;
-- @session

-- @session:id=3&user=test_account2:test_user&password=111
use mo_catalog;
select * from cluster_table_2;
-- @session

drop table cluster_table_2;


-- test system tenement, operation cluster table (update,delete,truncate)
drop table if exists cluster_table_3;
create cluster table cluster_table_3(
col1 int,
col2 float,
col3 decimal,
col4 date,
col5 bool,
col6 json,
col7 blob,
col8 text,
col9 varchar
);

insert into cluster_table_3 values (1,1.09,1.345,"2022-10-02",0,'{"a":1}',"你好","text","varchar", 0);
insert into cluster_table_3 values (1,1.09,1.345,"2022-10-02",0,'{"a":1}',"你好","text","varchar", 1);
insert into cluster_table_3 values (1,1.09,1.345,"2022-10-02",0,'{"a":1}',"你好","text","varchar", 2);
insert into cluster_table_3 values (2,10.9,13.45,"2022-10-02",1,'{"b":2}',"nihao","文本","字符", 0);
insert into cluster_table_3 values (2,10.9,13.45,"2022-10-02",1,'{"b":2}',"nihao","文本","字符", 1);
insert into cluster_table_3 values (2,10.9,13.45,"2022-10-02",1,'{"b":2}',"nihao","文本","字符", 2);
update cluster_table_3 set account_id=(select account_id from mo_account where account_name="test_account1") where account_id=1;
update cluster_table_3 set account_id=(select account_id from mo_account where account_name="test_account2") where account_id=2;
select col1,col2,col3,col4,col5,col6,col7,col8,col9 from cluster_table_3;

update cluster_table_3 set col1=100 where account_id=0 and col1=1;
select col1,col2,col3,col4,col5,col6,col7,col8,col9 from cluster_table_3;

-- @session:id=2&user=test_account1:test_user&password=111
use mo_catalog;
select * from cluster_table_3;
-- @session

-- @session:id=3&user=test_account2:test_user&password=111
use mo_catalog;
select * from cluster_table_3;
-- @session

update cluster_table_3 set col1=100 where account_id=(select account_id from mo_account where account_name="test_account1") and col1=1;
select col1,col2,col3,col4,col5,col6,col7,col8,col9 from cluster_table_3;

-- @session:id=2&user=test_account1:test_user&password=111
use mo_catalog;
select * from cluster_table_3;
-- @session

-- @session:id=3&user=test_account2:test_user&password=111
use mo_catalog;
select * from cluster_table_3;
-- @session

update cluster_table_3 set col1=100 where account_id=(select account_id from mo_account where account_name="test_account2") and col1=1;
select col1,col2,col3,col4,col5,col6,col7,col8,col9 from cluster_table_3;

-- @session:id=2&user=test_account1:test_user&password=111
use mo_catalog;
select * from cluster_table_3;
-- @session

-- @session:id=3&user=test_account2:test_user&password=111
use mo_catalog;
select * from cluster_table_3;
-- @session


delete from cluster_table_3 where account_id=0;
select col1,col2,col3,col4,col5,col6,col7,col8,col9 from cluster_table_3;

-- @session:id=2&user=test_account1:test_user&password=111
use mo_catalog;
select * from cluster_table_3;
-- @session

-- @session:id=3&user=test_account2:test_user&password=111
use mo_catalog;
select * from cluster_table_3;
-- @session


delete from cluster_table_3 where account_id=(select account_id from mo_account where account_name="test_account1");
select col1,col2,col3,col4,col5,col6,col7,col8,col9 from cluster_table_3;

-- @session:id=2&user=test_account1:test_user&password=111
use mo_catalog;
select * from cluster_table_3;
-- @session

-- @session:id=3&user=test_account2:test_user&password=111
use mo_catalog;
select * from cluster_table_3;
-- @session


delete from cluster_table_3 where account_id=(select account_id from mo_account where account_name="test_account2");
select col1,col2,col3,col4,col5,col6,col7,col8,col9 from cluster_table_3;

-- @session:id=2&user=test_account1:test_user&password=111
use mo_catalog;
select * from cluster_table_3;
-- @session

-- @session:id=3&user=test_account2:test_user&password=111
use mo_catalog;
select * from cluster_table_3;
-- @session


truncate table cluster_table_3;
select col1,col2,col3,col4,col5,col6,col7,col8,col9 from cluster_table_3;

-- @session:id=2&user=test_account1:test_user&password=111
use mo_catalog;
select * from cluster_table_3;
-- @session

-- @session:id=3&user=test_account2:test_user&password=111
use mo_catalog;
select * from cluster_table_3;
-- @session

drop table cluster_table_3;


-- test create cluster table include account_id columns
create cluster table cluster_table_xx(account_id int);

-- test common tenement operation(desc table,show create table,drop table)
drop table if exists cluster_table_4;
create cluster table cluster_table_4(
col1 int,
col2 varchar
);

insert into cluster_table_4 values (1,'a',0),(2,'b',0);
insert into cluster_table_4 values (1,'a',1),(2,'b',1);
insert into cluster_table_4 values (1,'a',2),(2,'b',2);
update cluster_table_4 set account_id=(select account_id from mo_account where account_name="test_account1") where account_id=1;
update cluster_table_4 set account_id=(select account_id from mo_account where account_name="test_account2") where account_id=2;
select col1,col2 from cluster_table_4;

-- @session:id=2&user=test_account1:test_user&password=111
use mo_catalog;
desc cluster_table_4;
show create table cluster_table_4;
drop table cluster_table_4;
-- @session

-- test common tenement operation table include (insert,update,delete,truncate)
-- @session:id=2&user=test_account1:test_user&password=111
use mo_catalog;
insert into cluster_table_4 values (3, 'c');
update cluster_table_4 set col1=10 where col2='a';
delete from cluster_table_4 where col1=2;
truncate table cluster_table_4;
-- @session

drop table cluster_table_4;


-- test cluster table relevance query(join,union)
drop table if exists cluster_table_5;
create cluster table cluster_table_5(
col1 int,
col2 varchar
);

insert into cluster_table_5  values (1,'a',0),(2,'b',0),(3,'c',0),(4,'d',0),(5,'f',0),(6,'g',0);
insert into cluster_table_5  values (1,'a',1),(2,'b',1),(3,'c',1),(4,'d',1),(5,'f',1),(6,'g',1);
insert into cluster_table_5  values (1,'a',2),(2,'b',2),(3,'c',2),(4,'d',2),(5,'f',2),(6,'g',2);
update cluster_table_5 set account_id=(select account_id from mo_account where account_name="test_account1") where account_id=1;
update cluster_table_5 set account_id=(select account_id from mo_account where account_name="test_account2") where account_id=2;
drop table if exists cluster_table_6;
create cluster table cluster_table_6(
a int,
b varchar
);

insert into cluster_table_6 values (100,'a',0),(200,'a',0),(300,'a',0);
insert into cluster_table_6 values (100,'a',1),(200,'a',1),(300,'a',1);
insert into cluster_table_6 values (100,'a',2),(200,'a',2),(300,'a',2);
update cluster_table_6 set account_id=(select account_id from mo_account where account_name="test_account1") where account_id=1;
update cluster_table_6 set account_id=(select account_id from mo_account where account_name="test_account2") where account_id=2;

select a1.col1,a1.col2,a2.a,a2.b from cluster_table_5 a1 left join cluster_table_6 a2 on a1.col2=a2.b;
select a1.col1,a1.col2,a2.a,a2.b from cluster_table_5 a1 right join cluster_table_6 a2 on a1.col2=a2.b;
select a1.col1,a1.col2,a2.a,a2.b from cluster_table_5 a1 inner join cluster_table_6 a2 on a1.col2=a2.b;

select col1,col2 from cluster_table_5 union select a,b from cluster_table_6;
select col1,col2 from cluster_table_5 union all select a,b from cluster_table_6;

select col1,col2 from cluster_table_5 intersect select a,b from cluster_table_6;

SELECT col1,col2 FROM cluster_table_5 MINUS SELECT a,b FROM cluster_table_6;
SELECT a,b FROM cluster_table_6 MINUS SELECT col1,col2 FROM cluster_table_5;


-- @session:id=2&user=test_account1:test_user&password=111
use mo_catalog;
select * from cluster_table_5  left join cluster_table_6 on cluster_table_5.col2=cluster_table_6.b;
select * from cluster_table_5  right join cluster_table_6 on cluster_table_5.col2=cluster_table_6.b;
select * from cluster_table_5  inner join cluster_table_6 on cluster_table_5.col2=cluster_table_6.b;

select * from cluster_table_5 union select * from cluster_table_6;
select * from cluster_table_5 union all select * from cluster_table_6;

select * from cluster_table_5 intersect select * from cluster_table_6;

SELECT * FROM cluster_table_5 MINUS SELECT * FROM cluster_table_6;
SELECT * FROM cluster_table_6 MINUS SELECT * FROM cluster_table_5;
-- @session


-- @session:id=3&user=test_account2:test_user&password=111
use mo_catalog;
select * from cluster_table_5  left join cluster_table_6 on cluster_table_5.col2=cluster_table_6.b;
select * from cluster_table_5  right join cluster_table_6 on cluster_table_5.col2=cluster_table_6.b;
select * from cluster_table_5  inner join cluster_table_6 on cluster_table_5.col2=cluster_table_6.b;

select * from cluster_table_5 union select * from cluster_table_6;
select * from cluster_table_5 union all select * from cluster_table_6;

select * from cluster_table_5 intersect select * from cluster_table_6;

SELECT * FROM cluster_table_5 MINUS SELECT * FROM cluster_table_6;
SELECT * FROM cluster_table_6 MINUS SELECT * FROM cluster_table_5;
-- @session


drop table cluster_table_5;
drop table cluster_table_6;


-- test when delete a tenant, the data of the tenant in the cluster table is deleted
drop table if exists cluster_table_7;
create cluster table cluster_table_7(
col1 int,
col2 varchar
);

insert into cluster_table_7 values (1,'a',0),(2,'b',0);
insert into cluster_table_7 values (1,'a',1),(2,'b',1);
insert into cluster_table_7 values (1,'a',2),(2,'b',2);
update cluster_table_7 set account_id=(select account_id from mo_account where account_name="test_account1") where account_id=1;
update cluster_table_7 set account_id=(select account_id from mo_account where account_name="test_account2") where account_id=2;
select col1,col2 from cluster_table_7;

drop account test_account1;
select col1,col2 from cluster_table_7;

drop account test_account2;
select col1,col2 from cluster_table_7;

drop table cluster_table_7;

use mo_catalog;
CREATE CLUSTER TABLE mo_instance (id varchar(128) NOT NULL,name VARCHAR(255) NOT NULL,account_name varchar(128) NOT NULL,provider longtext NOT NULL,provider_id longtext,region longtext NOT NULL,plan_type longtext NOT NULL,version longtext,status longtext,quota longtext,network_policy longtext,created_by longtext,created_at datetime(3) NULL,PRIMARY KEY (id, account_id),UNIQUE INDEX uniq_acc (account_name));
create account acc_idx ADMIN_NAME 'root' IDENTIFIED BY '123456';
drop account acc_idx;
drop table mo_instance;
CREATE TABLE shirts (
                        name VARCHAR(40),
                        size ENUM('x-small', 'small', 'medium', 'large', 'x-large')
);
INSERT INTO shirts (name, size) VALUES ('dress shirt','large'), ('t-shirt','medium'), ('polo shirt','small');
SELECT name, size FROM shirts;
SELECT name, size FROM shirts WHERE size = 'medium';
DELETE FROM shirts where size = 'large';
SELECT name, size FROM shirts;
DROP TABLE shirts;

create table t_enum(a int,b enum('1','2','3','4','5'));
insert into t_enum values(1,1);
select * from t_enum;
drop table t_enum;

-- @suit
-- @case
-- @desc: datatype:enum
-- @label:bvt

-- abnormal create table: non-string type
drop table if exists enum01;
create table enum01(col1 enum(132142,'*&*',6278131));
drop table enum02;


-- abnormal test: insert a column that does not exist in an enumeration type
drop table if exists enum02;
create table enum02(col1 enum('123214','*&*(234','database数据库'));
insert into enum02 values('1232145');
insert into enum02 values('*&*(2344');
drop table enum02;


-- test insert with enum
drop table if exists enum01;
create table enum01 (col1 enum('red','blue','green'));
insert into enum01 values ('red'),('blue'),('green');
desc enum01;
select * from enum01;
update enum01 set col1 ='blue' where col1 = 'green';
delete from enum01 where col1 = 'blue';
show create table enum01;
select table_name, COLUMN_NAME, data_type, is_nullable from information_schema.columns where table_name like 'enum01' and COLUMN_NAME not like '__mo%';
select * from enum01;


-- insert null
drop table if exists enum02;
create table enum02 (col1 enum('red','blue','green'));
insert into enum02 values ('red'),('blue'),('green');
insert into enum02 values (null);
insert into enum02 values ('');
select * from enum02;
show columns from enum03;
select table_name,COLUMN_NAME, data_type,is_nullable from information_schema.columns where table_name like 'enum02' and COLUMN_NAME not like '__mo%';
drop table enum02;


-- enum column constraint not null
drop table if exists enum03;
create table enum03 (col1 enum('ejwuijfew', 'ewrwrewretr', 'werwre') not null);
insert into enum03 values ('ejwuijfew');
insert into enum03 values (null);
select * from enum03;
show create table enum03;
show columns from enum03;
select table_name, COLUMN_NAME, data_type, is_nullable from information_schema.columns where table_name like 'enum02' and COLUMN_NAME not like '__mo%';
drop table enum03;


-- insert into select
drop table if exists enum02;
drop table if exists enum03;
create table enum02(col1 enum('数据库','数据库管理','数据库管理软件'));
insert into enum02 values('数据库');
insert into enum02 values('数据库管理');
insert into enum02 values('数据库管理软件');
insert into enum02 values(null);
select * from enum02;
create table enum03(col1 enum('数据库','数据库管理','数据库管理软件'));
insert into enum03 select * from enum02;
select * from enum03;
drop table enum02;
drop table enum03;


-- abnormal test: insert non-enumerated value, error
drop table if exists enum04;
create table enum04 (col1 enum ('2133212312hsay899323--__', 'euijn2fde324', 'e32rew'));
insert into enum04 values ('2133212312hsay899323--__');
insert into enum04 values ('euijn2fde324');
insert into enum04 (col1) values ('welll');
select * from enum04;
drop table enum04;


-- if there are numbers stored as strings in the enumeration type, the corresponding column still represents the sequence number as a number rather than a specific value, for example
drop table if exists enum05;
create table enum05 (a int,b enum('4','3','2','1'));
insert into enum05 values(1,1);
select * from enum05;
insert into enum05 values(2,'1');
select * from enum05;
drop table enum05;


-- create table enum column
drop table if exists pri01;
create table pri01 (col1 enum('qy4iujd3wi4fu4h3f', '323242r34df432432', '32e3ewfdewrew'));
show create table pri01;
insert into pri01 values ('qy4iujd3wi4fu4h3f');
insert into pri01 values ('qy4iujd3wi4fu4h3f');
insert into pri01 (col1) values ('323242r34df432432');
insert into pri01 (col1) values (null);
select * from pri01;
show create table pri01;
show columns from pri01;
drop table pri01;


drop table if exists pri02;
create table pri02 (col1 int, col2 enum('数据库', '数据库系统', '数据库管理系统'));
insert into pri02 values (1, '数据库');
insert into pri02 values (2, '数据库');
select * from pri02;
alter table pri02 add primary key (col2);
show create table pri02;
show columns from pri02;
select table_name, COLUMN_NAME, data_type, is_nullable from information_schema.columns where table_name like 'pri02' and COLUMN_NAME not like '__mo%';
drop table pri02;


drop table if exists pri03;
create table pri03 (col1 int, col2 enum('数据库', '数据库系统', '数据库管理系统'));
insert into pri03 values (1, '数据库');
insert into pri03 values (2, '数据库系统');
select * from pri03;
alter table pri03 add primary key (col2);
show create table pri03;
show columns from pri03;
select table_name, COLUMN_NAME, data_type, is_nullable from information_schema.columns where table_name like 'pri03' and COLUMN_NAME not like '__mo%';
drop table pri03;


drop table if exists pri04;
create table pri04 (col1 int, col2 enum('database', 'database management', 'database management system'));
insert into pri04 (col1, col2) values (1, 'database');
insert into pri04 values (2, 'database management system');
show create table pri04;
show create table pri04;
show columns from pri04;
select * from pri04 where col2 = 'database';
select table_name, COLUMN_NAME, data_type, is_nullable from information_schema.columns where table_name like 'pri04' and COLUMN_NAME not like '__mo%';
drop table pri04;


-- insert into table,  either use a number to represent a number or insert a specific value
-- query, update, or delete data, can also use numeric numbers or specific values
drop table if exists inert01;
create table insert01 (id int primary key,
                       order_number VARCHAR(20),
                       status enum('Pending', 'Processing', 'Completed', 'Cancelled')
);
insert into insert01 values(1,'111',1),(2,'222',2),(3,'333',3),(4,'444','Cancelled');
select * from insert01;
show create table insert01;
show columns from insert01;
delete from insert01 where status=3;
update insert01 set status='Pending' where status=2;
select * from insert01;
select * from insert01 where status=4;
select * from insert01 where status in ('Pending',4);
drop table insert01;


-- default
drop table if exists default01;
create table default01 (`col1` enum('T', 'E') not null default 'T');
desc default01;
insert into default01 values(default);
select * from default01;
drop table default01;

drop table if exists default02;
create table default02 (`col1` enum('T', 'E') not null default '1');
desc default02;
insert into default02 values(default);
select * from default02;
drop table default02;

drop table if exists default03;
create table default03 (`col1` enum('T', 'E') not null default 1);
desc default03;
insert into default03 values(default);
select * from default03;
drop table default03;

drop table if exists default04;
create table default04 (`col1` enum('T', 'E') not null default 2);
desc default04;
insert into default04 values(default);
select * from default04;
drop table default04;

drop table if exists default05;
create table default05 (`col1` enum('T', 'E') not null default '2');
desc default05;
insert into default05 values(default);
select * from default05;
drop table default05;


-- enumeration types support operators
drop table if exists enum04;
create table enum04(col1 int,col2 enum('38921384','abc','','MOMOMO','矩阵起源'));
insert into enum04 values(1,'38921384');
insert into enum04 values(2,'');
insert into enum04 values(3,'矩阵起源');
select * from enum04;

-- =,!=,>,>=,<,<=,between and,not between and,in,not in,like,COALESCE
select * from enum04 where col2 = '';
select * from enum04 where col2 != '';

select * from enum04 where col2 > '38921384';
select * from enum04 where col2 >= '38921384';

select * from enum04 where col2 < '矩阵起源';
select * from enum04 where col2 <= '矩阵起源';

select * from enum04 where col2 between '38921384' and '矩阵起源';
select * from enum04 where col2 not between '38921384' and '矩阵起源';

select * from enum04 where col2 in('38921384','');
select * from enum04 where col2 not in('38921384','');

select * from enum04 where col2 like '%921384';
select coalesce(null,null,col2) from enum04;
drop table enum04;


-- builtin function
drop table if exists builtin01;
create table builtin01(col1 enum('  云原生数据库  ','存储引擎 TAE', 'database system') not null,col2 enum(' database','engine ','index meta data'));
insert into builtin01 values('  云原生数据库  ',' database');
insert into builtin01 values('存储引擎 TAE','engine ');
insert into builtin01 values('database system','engine ');
select * from builtin01;
select concat_ws(',,,',col1,col2) from builtin01;
select find_in_set('  云原生数据库  ',col1) from builtin01;
select length(col1) as length_col1, length(col2) as length_col2 from builtin01;
select char_length(col1),char_length(col2) from builtin01;
select ltrim(col1) from builtin01;
select rtrim(col2) from builtin01;
select lpad(col1,20,'-') from builtin01;
select rpad(col2,10,'****') from builtin01;
select startswith(col2,'eng') from builtin01;
select endswith(col1,'数据表') from builtin01;
select reverse(col1),reverse(col2) from builtin01;
select substring(col1,4,6),substring(col2,1,6) from builtin01;
select * from builtin01 where col1 = space(5);
select bit_length(col2) from builtin01;
select empty(col2) from builtin01;


-- aggregate:count, max, min, any_value, group_concat
select count(col1) as count_col1 from builtin01;
-- @bvt:issue#11348
select max(col1), max(col2) from builtin01;
select min(col1), min(col2) from builtin01;
select group_concat(col1,col2) from builtin01;
drop table builtin01;
-- @bvt:issue

-- aggregate: max, min
drop table if exists agg01;
create table agg01 (col1 int, col2 enum('egwjqebwq', 'qwewqewqeqewq', 'weueiwqeowqehwgqjhenw'));
insert into agg01 values (1, 'egwjqebwq');
insert into agg01 values (2, 'weueiwqeowqehwgqjhenw');
insert into agg01 values (3, 'qwewqewqeqewq');
insert into agg01 values (4, null);
-- @bvt:issue#11348
select max(col2) from agg01;
select min(col2) from agg01;
-- @bvt:issue
select * from agg01;
drop table agg01;


-- cte
drop table if exists cte01;
create table cte01(col1 int, col2 enum('hfjsa','123214321','&**())_'));
insert into cte01 VALUES(1, 'hfjsa');
insert into cte01 VALUES(2, '123214321');
insert into cte01 VALUES(3, '&**())_');
select * from cte01;
with cte_1 as(select * from cte01 where col2 = 'hfjsa') select col2 from cte_1 where col2 = 'hfjsa';
with cte_2 as(select col1,col2 from cte01 where col1 = 3) select col2 from cte_2 where col2 = '&**())_';
drop table cte01;

drop table if exists agg01;
create table agg01 (col1 int, col2 enum('egwjqebwq', 'qwewqewqeqewq', 'weueiwqeowqehwgqjhenw') primary key);
drop table if exists agg01;
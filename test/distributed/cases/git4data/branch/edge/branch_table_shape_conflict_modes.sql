-- DATA BRANCH diff/merge across table shapes: no primary key (duplicate rows),
-- AUTO_INCREMENT keys, STORED generated columns and composite primary keys,
-- with every conflict mode (fail / skip / accept).
drop database if exists branch_shape_modes;
create database branch_shape_modes;
use branch_shape_modes;

-- no primary key: duplicate rows are tracked as separate rows
create table nopk(a int, b varchar(10));
insert into nopk values (1,'x'),(1,'x'),(2,'y');
data branch create table nopk_b from nopk;
select a, b, count(*) from nopk_b group by a, b order by a;
insert into nopk_b values (1,'x');
delete from nopk_b where a = 2;
data branch diff nopk_b against nopk output count;
data branch merge nopk_b into nopk when conflict accept;
select a, b, count(*) from nopk group by a, b order by a;

-- AUTO_INCREMENT: both sides allocate the same next id
create table ai(id int auto_increment primary key, v int);
insert into ai(v) values (1),(2);
data branch create table ai_b from ai;
insert into ai_b(v) values (3);
select * from ai_b order by id;
insert into ai(v) values (4);
data branch diff ai_b against ai output count;
data branch merge ai_b into ai when conflict fail;
data branch merge ai_b into ai when conflict skip;
select * from ai order by id;
insert into ai(v) values (5);
select * from ai order by id;

-- STORED generated column is carried by diff and merge
create table g(id int primary key, a int, b int as (a*2) stored);
insert into g(id,a) values (1,1),(2,2);
data branch create table g_b from g;
update g_b set a = 5 where id = 1;
insert into g_b(id,a) values (3,7);
data branch diff g_b against g output count;
data branch merge g_b into g when conflict accept;
select * from g order by id;

-- composite primary key with a real conflict on (1,'a')
create table cp(a int, b varchar(5), c int, primary key(a,b));
insert into cp values (1,'a',1),(1,'b',2),(2,'a',3);
data branch create table cp_b from cp;
update cp_b set c = 9 where a = 1 and b = 'a';
update cp set c = 8 where a = 1 and b = 'a';
insert into cp_b values (3,'z',4);
data branch diff cp_b against cp output count;
data branch merge cp_b into cp when conflict fail;
select * from cp order by a, b;
data branch merge cp_b into cp when conflict skip;
select * from cp order by a, b;
data branch merge cp_b into cp when conflict accept;
select * from cp order by a, b;

-- error paths
data branch create table cp_b from cp;
data branch create table cp_c from nosuch;

drop database branch_shape_modes;

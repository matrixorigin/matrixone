-- @separator:table
drop account if exists pub_ft_20210;
drop account if exists sub_ft_20210_a;
drop account if exists sub_ft_20210_b;
create account pub_ft_20210 admin_name = 'admin' identified by '111';
create account sub_ft_20210_a admin_name = 'admin' identified by '111';
create account sub_ft_20210_b admin_name = 'admin' identified by '111';

-- @session:id=1&user=pub_ft_20210:admin&password=111
create database `pub-ft-db`;
create table `pub-ft-db`.`articles-quoted` (id int primary key, body text);
insert into `pub-ft-db`.`articles-quoted` values (1, 'hello matrixone'), (2, 'unrelated text');
create fulltext index ft_articles on `pub-ft-db`.`articles-quoted` (body);
create table `pub-ft-db`.secret_table (id int primary key);
insert into `pub-ft-db`.secret_table values (99);
create publication pub_ft_table database `pub-ft-db` table `articles-quoted` account sub_ft_20210_a, sub_ft_20210_b;

create database pub_ft_all_db;
create table pub_ft_all_db.all_articles (id int primary key, body text);
insert into pub_ft_all_db.all_articles values (10, 'database publication');
create fulltext index ft_all_articles on pub_ft_all_db.all_articles (body);
create publication pub_ft_database database pub_ft_all_db account sub_ft_20210_a;
-- @session

-- @session:id=2&user=sub_ft_20210_a:admin&password=111
create database subscriber_table_alias from pub_ft_20210 publication pub_ft_table;
select id, body from subscriber_table_alias.`articles-quoted` order by id;
select id from subscriber_table_alias.`articles-quoted` where match(body) against('hello') order by id;
create database subscriber_database_alias from pub_ft_20210 publication pub_ft_database;
select id from subscriber_database_alias.all_articles where match(body) against('publication') order by id;
select * from subscriber_table_alias.secret_table;
select count(*) from mo_catalog.mo_indexes where algo = 'fulltext';
prepare subscriber_match_stmt from 'select id from subscriber_table_alias.`articles-quoted` where match(body) against(\'hello\') order by id';
-- @session

-- @session:id=3&user=sub_ft_20210_b:admin&password=111
create database second_subscriber_alias from pub_ft_20210 publication pub_ft_table;
select id from second_subscriber_alias.`articles-quoted` where match(body) against('hello') order by id;
-- @session

-- @session:id=1&user=pub_ft_20210:admin&password=111
insert into `pub-ft-db`.`articles-quoted` values (3, 'hello subscriber');
update `pub-ft-db`.`articles-quoted` set body = 'hello updated' where id = 2;
delete from `pub-ft-db`.`articles-quoted` where id = 1;
-- @session

-- @session:id=2&user=sub_ft_20210_a:admin&password=111
select id from subscriber_table_alias.`articles-quoted` where match(body) against('hello') order by id;
execute subscriber_match_stmt;
-- @session

-- @session:id=1&user=pub_ft_20210:admin&password=111
alter publication pub_ft_table account sub_ft_20210_b;
-- @session

-- @session:id=2&user=sub_ft_20210_a:admin&password=111
execute subscriber_match_stmt;
select id from subscriber_table_alias.`articles-quoted` where match(body) against('hello') order by id;
deallocate prepare subscriber_match_stmt;
-- @session

-- @session:id=1&user=pub_ft_20210:admin&password=111
alter publication pub_ft_table account sub_ft_20210_a, sub_ft_20210_b;
-- @session

-- @session:id=2&user=sub_ft_20210_a:admin&password=111
prepare restored_subscriber_stmt from 'select id from subscriber_table_alias.`articles-quoted` where match(body) against(\'hello\') order by id';
execute restored_subscriber_stmt;
deallocate prepare restored_subscriber_stmt;
-- @session

-- @session:id=3&user=sub_ft_20210_b:admin&password=111
prepare second_subscriber_stmt from 'select id from second_subscriber_alias.`articles-quoted` where match(body) against(\'hello\') order by id';
execute second_subscriber_stmt;
-- @session

-- @session:id=1&user=pub_ft_20210:admin&password=111
drop publication pub_ft_table;
-- @session

-- @session:id=3&user=sub_ft_20210_b:admin&password=111
execute second_subscriber_stmt;
deallocate prepare second_subscriber_stmt;
drop database second_subscriber_alias;
-- @session

-- @session:id=2&user=sub_ft_20210_a:admin&password=111
drop database subscriber_table_alias;
drop database subscriber_database_alias;
-- @session

drop account sub_ft_20210_a;
drop account sub_ft_20210_b;
drop account pub_ft_20210;

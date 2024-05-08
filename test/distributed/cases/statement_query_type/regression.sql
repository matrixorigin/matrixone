-- prepare account
drop account if exists bvt_query_type_reg;
create account if not exists `bvt_query_type_reg` ADMIN_NAME 'admin' IDENTIFIED BY '123456';

-- Login account
-- @session:id=1&user=bvt_query_type_reg:admin:accountadmin&password=123456

create database statement_query_type;

begin;
use statement_query_type;
create table test_table(col1 int,col2 varchar);
create view test_view as select * from test_table;
-- issue #7789
show create view test_view;
show collation like 'utf8mb4_general_ci';
show collation like 'utf8mb4_general_ci%';
-- issue end
load data infile '$resources/load_data/test.csv' into table test_table fields terminated by ',';
insert into test_table values (1,'a'),(2,'b'),(3,'c');
-- issue #7789
update test_table set col2='xxx' where col1=1;
delete from test_table where col1=3;
-- issue end
rollback ;

-- @session
-- END

-- cleanup
drop account if exists bvt_query_type_reg;

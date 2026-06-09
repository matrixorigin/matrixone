drop database if exists test_diff_save_decimal;
create database test_diff_save_decimal;
use test_diff_save_decimal;

create table decimal_base(
id int primary key,
d64 decimal(18,2),
d128 decimal(19,3),
d256 decimal(39,4)
);

insert into decimal_base values
(1, 1234567890123456.78, 1234567890123456.789, 12345678901234567890123456789012345.6789);

data branch create table decimal_branch from decimal_base;
insert into decimal_branch values
(2, 15.00, 1234567890123456.125, 12345678901234567890123456789012344.1234);

set save_query_result = on;
data branch diff decimal_branch against decimal_base;
set save_query_result = off;

drop table decimal_branch;
drop table decimal_base;
drop database test_diff_save_decimal;

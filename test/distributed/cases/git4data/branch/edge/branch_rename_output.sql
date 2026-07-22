-- DATA BRANCH DIFF OUTPUT AS must materialize a one-sided renamed column.
drop database if exists br_rename_output;
create database br_rename_output;
use br_rename_output;

create table base(a int primary key, b int);
insert into base values (1, 10), (2, 20);

data branch create table rename_left from base;
data branch create table rename_right from base;
alter table rename_left rename column b to bb;
update rename_left set bb = 11 where a = 1;

data branch diff rename_left against rename_right output as rename_diff_output;
show create table rename_diff_output;
select __mo_diff_source, __mo_diff_flag, a, bb from rename_diff_output;

drop database br_rename_output;

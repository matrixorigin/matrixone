-- @bvt:issue#7357
create table f1(a int primary key, b int);
create table c1 (a int, b int, foreign key f_a(a) references f1(c));
create table c1 (a int, b int, foreign key f_a(a) references f1(b));
create table c1 (a int, b int, foreign key f_a(a) references f1(a));
select * from f1;
select * from c1;
drop table f1;
-- @bvt:issue

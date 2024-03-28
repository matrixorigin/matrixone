create table tt(aa int, bb int, cc int);
insert into tt values (1, 1, 1);
select * from tt;
select aa from tt;
select aA from tt;
select count(*) from tt;
select COUNT(*)  from tt;
show variables like 'keep_user_target%';
set global keep_user_target_list_in_result = 1;
select aa from tt;
select aA from tt;
select AA from tt;
select Aa from tt;
select count(*) from tt;
select COUNT(*)  from tt;
select cOuNt(*)  from tt;
drop table tt;



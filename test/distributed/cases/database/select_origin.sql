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
set global keep_user_target_list_in_result = default;
drop table tt;

drop table if exists t1;
create table t1(
aa int,
bb varchar(25)
);
insert into  t1 values (0, 'a');
insert into  t1 values (1, NULL);
insert into  t1 values (NULL, NULL);
insert into  t1 values (null, 'b');
set global keep_user_target_list_in_result = 1;
select * from t1;
select coalesCE(Aa, 1) from t1;
select coalEsCE(aA, 1) from t1;
select cOAlesce(bB, '1') from t1;
set global keep_user_target_list_in_result = default;
drop table t1;

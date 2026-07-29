drop database if exists check_constraint_test;
create database check_constraint_test;
use check_constraint_test;

create table t(
    a int,
    b varchar(10),
    constraint positive_a check(a > 0),
    constraint short_b check(length(b) < 5)
);

insert into t values (1, 'ok'), (null, null);
insert into t values (-1, 'bad');
insert into t values (2, 'too long');
select * from t order by a, b;

insert ignore into t values (-1, 'bad'), (3, 'yes');
select * from t order by a, b;

insert into t select * from (values row(4, 'four'), row(-2, 'bad')) as src;
select * from t order by a, b;

drop database check_constraint_test;

-- issue #31751: APPROX_COUNT takes exactly one argument (or *)
drop database if exists approx_count_arity;
create database approx_count_arity;
use approx_count_arity;
create table t (a int, b int);
insert into t values (1, 1), (2, 2), (null, 3);
select approx_count(a, b) from t;
select approx_count(a, b, a) from t;
select approx_count(a) from t;
select approx_count(*) from t;
select approx_count(b) from t where a is null;
drop database approx_count_arity;

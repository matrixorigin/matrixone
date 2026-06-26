-- test case for rand(n) compatibility (issue #24813)

drop database if exists rand_n_func;
create database rand_n_func;
use rand_n_func;

-- same seed in the same statement
select rand(3) as r1, rand(3) as r2;

create table t_rand(id int, a int);
insert into t_rand values (1, 1), (2, 2), (3, 3), (4, 1);

-- constant seed: one sequence per statement, advanced row by row
select id, a, rand(7) as r from t_rand order by a, id;

-- non-constant seed: reseeded per row, same seed => same value
select id, a, rand(a) as r from t_rand order by a, id;

-- null seed behaves the same as seed 0 in MySQL/MariaDB
select rand(null) as rand_null_1, rand(0) as rand_zero_1;
select rand(null) as rand_null_2, rand(0) as rand_zero_2;

drop table t_rand;
drop database rand_n_func;

-- issue #24550: public aggregate contract and rejected arguments
drop table if exists t_approx_percentile_boundary;
create table t_approx_percentile_boundary(grp int, v int, p double, s varchar(10));
insert into t_approx_percentile_boundary values
    (1, 1, 0.5, '1'),
    (1, 3, 0.5, '3'),
    (1, 5, 0.5, '5'),
    (2, null, 0.5, null);

-- exact endpoints and an interior percentile on a small ordered set
select cast(approx_percentile(v, 0) as decimal(10, 2)),
       cast(approx_percentile(v, 0.5) as decimal(10, 2)),
       cast(approx_percentile(v, 1) as decimal(10, 2))
from t_approx_percentile_boundary where grp = 1;

-- all-null and empty input both produce SQL NULL
select approx_percentile(v, 0.5) is null from t_approx_percentile_boundary where grp = 2;
create table t_approx_percentile_empty(v int);
select approx_percentile(v, 0.5) is null from t_approx_percentile_empty;
drop table t_approx_percentile_empty;

-- percentile must be a non-null constant in the closed [0, 1] interval
select approx_percentile(v, p) from t_approx_percentile_boundary where grp = 1;
select approx_percentile(v, null) from t_approx_percentile_boundary where grp = 1;

-- unsupported value types are rejected without corrupting the connection
select approx_percentile(s, 0.5) from t_approx_percentile_boundary where grp = 1;
select 'AFTER_APPROX_PERCENTILE_ERRORS';

drop table t_approx_percentile_boundary;

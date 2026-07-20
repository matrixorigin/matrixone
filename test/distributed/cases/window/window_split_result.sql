-- Regression for issue #25813. AggFuncExec splits physical result vectors at
-- 8192 rows; Window must expose one complete logical result across that
-- internal boundary.
drop database if exists window_split_result;
create database window_split_result;
use window_split_result;

create table t_win_split (
    id bigint,
    g int,
    v decimal(20, 2)
);
insert into t_win_split
select result, 1, cast(result as decimal(20, 2))
from generate_series(1, 9000, 1) g;

-- No PARTITION BY: Window receives and materializes all input batches. The
-- CURRENT ROW frame keeps the regression O(n) while producing 9000 groups.
select current_sum
from (
    select sum(v) over (rows between current row and current row) as current_sum
    from t_win_split
) w
where current_sum in (1, 8192, 8193, 9000)
order by current_sum;

-- One 9000-row partition exercises the receive-per-partition path and the
-- dedicated ROW_NUMBER executor. Filtering on window results prevents the
-- boundary predicate from being pushed below the Window operators.
select id, current_sum, rn
from (
    select id,
           sum(v) over (partition by g rows between current row and current row) as current_sum,
           row_number() over (partition by g order by id) as rn
    from t_win_split
) w
where rn in (1, 8192, 8193, 9000)
order by rn;

-- Verify that no row is lost, duplicated, or reordered while chunks merge.
select count(*) as row_count,
       min(rn) as min_rn,
       max(rn) as max_rn,
       sum(rn) as rn_checksum,
       sum(current_sum) as value_checksum,
       sum(abs(id - rn)) as row_alignment_checksum,
       sum(abs(cast(id as decimal(20, 2)) - current_sum)) as value_alignment_checksum
from (
    select id,
           sum(v) over (partition by g rows between current row and current row) as current_sum,
           row_number() over (partition by g order by id) as rn
    from t_win_split
) w;

drop database window_split_result;

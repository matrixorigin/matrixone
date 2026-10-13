-- @suite

-- @case
-- @desc: BIT_XOR supports window specifications like BIT_AND and BIT_OR
-- @label:bvt

drop database if exists mysql_compat_window_bit_xor;
create database mysql_compat_window_bit_xor;
use mysql_compat_window_bit_xor;

create table t (
  id int primary key,
  grp int,
  v int
);

insert into t values
  (1, 1, 10),
  (2, 1, 20),
  (3, 1, 30),
  (4, 2, 40),
  (5, 2, null),
  (6, 2, 5);

-- Cumulative BIT_XOR and controls for the other bitwise window aggregates.
select id,
       bit_and(v) over (order by id rows between unbounded preceding and current row) as cumulative_and,
       bit_or(v) over (order by id rows between unbounded preceding and current row) as cumulative_or,
       bit_xor(v) over (order by id rows between unbounded preceding and current row) as cumulative_xor
from t
order by id;

-- Partitioned and explicit sliding-frame BIT_XOR windows.
select id, grp,
       bit_xor(v) over (partition by grp) as partition_xor,
       bit_xor(v) over (order by id rows between 1 preceding and current row) as sliding_xor
from t
order by id;

-- Empty and all-NULL frames use BIT_XOR's aggregate identity value.
create table t_null (
  id int primary key,
  grp int,
  v int
);
insert into t_null values (1, 1, null), (2, 1, null), (3, 2, 7);

select id,
       bit_xor(v) over (partition by grp) as partition_xor,
       bit_xor(v) over (order by id rows between 1 preceding and 1 preceding) as previous_xor
from t_null
order by id;

drop database mysql_compat_window_bit_xor;

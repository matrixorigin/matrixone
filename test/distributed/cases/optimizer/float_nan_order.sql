-- @suite

-- @case
-- @desc: scalar FLOAT/DOUBLE ORDER BY keeps NaNs last and as peers
-- @label:bvt

drop database if exists float_nan_order;
create database float_nan_order;
use float_nan_order;

-- A secondary index stores the DOUBLE primary key inside its encoded hidden
-- key. The index remains a valid access path, but that encoding must not
-- replace the logical float sort or drive an early LIMIT.
create table t_double_order (
  k double primary key,
  tenant int,
  id int,
  key idx_tenant(tenant)
);
insert into t_double_order values
  (bit_cast(unhex('000000000000f87f') as double), 1, 70),
  (bit_cast(unhex('010000000000f87f') as double), 1, 71),
  (cast('Inf' as double), 1, 60),
  (cast('-Inf' as double), 1, 10),
  (-1.0, 1, 20),
  (bit_cast(unhex('0000000000000080') as double), 1, 30),
  (0.0, 1, 31),
  (1.0, 1, 40);

select id from t_double_order force index(idx_tenant)
where tenant = 1 order by k asc limit 2;
select id from t_double_order force index(idx_tenant)
where tenant = 1 order by k desc limit 2;

select id from t_double_order where tenant = 1 order by k asc, id asc;
select id from t_double_order where tenant = 1 order by k desc, id asc;

select id, rank() over (order by k) as r, dense_rank() over (order by k) as dr
from t_double_order order by id;

select id, rank() over (partition by tenant order by k) as r
from t_double_order order by id;

select id, row_number() over (partition by tenant order by k, id) as rn
from t_double_order order by rn;

create table t_float_order(id int primary key, k float);
insert into t_float_order values
  (70, bit_cast(unhex('0000c07f') as float)),
  (71, bit_cast(unhex('0100c07f') as float)),
  (60, cast('Inf' as float)),
  (10, cast('-Inf' as float)),
  (20, -1.0),
  (30, bit_cast(unhex('00000080') as float)),
  (31, 0.0),
  (40, 1.0);

select id from t_float_order order by k asc, id asc;
select id from t_float_order order by k desc, id asc;

drop database float_nan_order;

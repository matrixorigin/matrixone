-- @suite

-- @case
-- @desc: VARIANCE is a VAR_POP alias for window aggregates
-- @label:bvt

drop database if exists mysql_compat_window_variance;
create database mysql_compat_window_variance;
use mysql_compat_window_variance;

create table orders (
  id int primary key,
  customer_id int,
  amount decimal(10,2)
);

insert into orders values
  (1, 10, 100.00), (2, 10, 200.00), (3, 10, 200.00),
  (4, 20, null), (5, 20, 150.50), (6, 20, 150.50),
  (7, 30, 300.00), (8, 30, 100.00);

select id, customer_id,
       cast(variance(amount) over (partition by customer_id) as decimal(16,4)) as variance_v,
       cast(var_pop(amount) over (partition by customer_id) as decimal(16,4)) as var_pop_v
from orders
order by id;

drop database mysql_compat_window_variance;

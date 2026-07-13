-- @suite

-- @case
-- @desc: MySQL compatibility cases for JSON_ARRAYAGG / JSON_OBJECTAGG as window functions
-- @label:bvt

drop database if exists mysql_compat_window_json_arrayagg;
create database mysql_compat_window_json_arrayagg;
use mysql_compat_window_json_arrayagg;

create table orders (
  id int primary key,
  customer_id int,
  status varchar(16)
);

insert into orders values
(1,10,'paid'),(2,10,'paid'),(3,10,'refund'),
(4,20,'paid'),(5,20,'paid'),(6,20,'pending'),
(7,30,'paid'),(8,30,'refund');

-- json_arrayagg as a cumulative window aggregate
select id, customer_id,
       json_arrayagg(status) over (partition by customer_id order by id) v
from orders order by id;

-- json_arrayagg over the whole partition (no order by)
select customer_id,
       json_arrayagg(status) over (partition by customer_id) v
from orders order by customer_id, id;

-- json_arrayagg with an explicit frame
select id, customer_id,
       json_arrayagg(status) over (partition by customer_id order by id rows between 1 preceding and current row) v
from orders order by id;

-- json_objectagg as a cumulative window aggregate (key must be a string type in MO)
select id, customer_id,
       json_objectagg(cast(id as char), status) over (partition by customer_id order by id) v
from orders order by id;

-- plain aggregate still works
select customer_id, json_arrayagg(status) v
from orders group by customer_id order by customer_id;

select customer_id, json_objectagg(cast(id as char), status) v
from orders group by customer_id order by customer_id;

-- json_arrayagg / json_objectagg remain usable as identifiers (non-reserved)
create table t_ident (json_arrayagg int, json_objectagg int);
insert into t_ident values (1, 2);
select json_arrayagg, json_objectagg from t_ident;
drop table t_ident;

drop database mysql_compat_window_json_arrayagg;

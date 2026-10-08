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

-- json_arrayagg as a cumulative window aggregate.
-- id is the (unique) primary key, so ORDER BY id makes the aggregation order deterministic.
select id, customer_id,
       json_arrayagg(status) over (partition by customer_id order by id) v
from orders order by id;

-- json_arrayagg over the whole partition. ORDER BY id + a full ROWS frame keeps the
-- element order deterministic while still aggregating every row of the partition.
select id, customer_id,
       json_arrayagg(status) over (partition by customer_id order by id
                                   rows between unbounded preceding and unbounded following) v
from orders order by id;

-- json_arrayagg with an explicit rows frame.
select id, customer_id,
       json_arrayagg(status) over (partition by customer_id order by id rows between 1 preceding and current row) v
from orders order by id;

-- json_objectagg accepts numeric keys and applies the same VARCHAR cast as a scalar aggregate.
select id, customer_id,
       json_objectagg(id, status) over (partition by customer_id order by id) v
from orders order by id;

-- Duplicate numeric keys keep the last value in each ordered frame.
select id, customer_id,
       json_objectagg(customer_id, status) over (partition by customer_id order by id
                                                 rows between unbounded preceding and current row) v
from orders order by id;

-- plain aggregates still work; use single-row groups so the output is order-independent.
select customer_id, json_arrayagg(status) v
from orders where id in (1, 4, 7) group by customer_id order by customer_id;

select customer_id, json_objectagg(id, status) v
from orders where id in (1, 4, 7) group by customer_id order by customer_id;

-- json_arrayagg / json_objectagg remain usable as identifiers (non-reserved)
create table t_ident (json_arrayagg int, json_objectagg int);
insert into t_ident values (1, 2);
select json_arrayagg, json_objectagg from t_ident;
drop table t_ident;

-- Opaque aggregate values retain their subtype tags through window frames.
create table opaque_orders (
  id int primary key,
  customer_id int,
  bit_value bit(8),
  binary_value binary(3),
  varbinary_value varbinary(3),
  blob_value blob
);
insert into opaque_orders values
(1,10,b'10101010',X'00FF41',X'00FF41',X'00'),
(2,10,NULL,NULL,UNHEX(''),NULL),
(3,10,b'00000111',X'000102',X'000102',X'80');
select id,
       json_arrayagg(bit_value) over (partition by customer_id order by id) bit_array,
       json_arrayagg(binary_value) over (partition by customer_id order by id) binary_array,
       json_arrayagg(varbinary_value) over (partition by customer_id order by id) varbinary_array,
       json_arrayagg(blob_value) over (partition by customer_id order by id) blob_array
from opaque_orders order by id;
select id,
       json_objectagg(id, bit_value) over (partition by customer_id order by id) bit_object,
       json_objectagg(id, binary_value) over (partition by customer_id order by id) binary_object,
       json_objectagg(id, varbinary_value) over (partition by customer_id order by id) varbinary_object,
       json_objectagg(id, blob_value) over (partition by customer_id order by id) blob_object
from opaque_orders order by id;
drop table opaque_orders;

-- TIMESTAMP JSON window aggregates follow the session time zone (#28890).
set @json_window_saved_time_zone = @@session.time_zone;
set time_zone = '+00:00';
create table timestamp_orders (
  id int primary key,
  customer_id int,
  k varchar(16),
  ts timestamp(6)
);
insert into timestamp_orders values
(1,10,'a','2024-02-29 04:34:56.123456'),
(2,10,'b','2024-02-29 05:34:56.123456');
set time_zone = '+08:00';
select id,
       json_unquote(json_extract(
           json_arrayagg(ts) over (partition by customer_id order by id),
           '$[0]')) array_agg,
       json_unquote(json_extract(
           json_objectagg(k, ts) over (partition by customer_id order by id),
           '$.a')) object_agg
from timestamp_orders order by id;
drop table timestamp_orders;
set time_zone = @json_window_saved_time_zone;

drop database mysql_compat_window_json_arrayagg;

-- #31530: reuse full frames and cumulative aggregate states without changing
-- NULL, peer, partition, decimal, or grouped-pagination semantics.
drop database if exists window_unbounded_aggregate;
create database window_unbounded_aggregate;
use window_unbounded_aggregate;
create table t (id int, p int, k int, v decimal(20,2), s varchar(64));
insert into t values
(1,1,NULL,NULL,'long string beyond inline storage'),
(2,1,1,2,'b'), (3,1,1,4,'c'), (4,1,2,-1,'d'),
(5,1,2,NULL,'e'), (6,1,3,7,'f'),
(7,2,NULL,NULL,'g'), (8,2,1,10,'h');

select id, count(*) over () as n, count(v) over () as nn,
sum(v) over () as total, avg(v) over () as mean,
min(s) over () as first_s
from t order by id;

select id,
sum(v) over (partition by p order by k) as range_sum,
sum(v) over (partition by p order by k,id rows unbounded preceding) as rows_sum,
avg(v) over (partition by p order by k) as range_avg,
count(v) over (partition by p order by k) as range_count
from t order by id;

select id,
sum(v) over (partition by p order by k desc,id desc) as multi_key_sum,
sum(v) over (partition by p order by k desc rows between unbounded preceding and unbounded following) as full_sum,
sum(v) over (partition by p order by id rows between 1 preceding and current row) as bounded_sum
from t order by id;

-- COUNT observes the GROUP BY output, and LIMIT does not change the count.
select p, sum(v) as total, count(*) over () as groups_seen
from t group by p order by p limit 1;

select id, sum(v) over () as total, count(v) over () as nn
from t where id in (1,7) order by id;
select count(*) over () as n from t where id < 0;
select id, count(1) over () as n, sum(2) over () as total from t where id = 2;

-- Finalized varlen results survive release of aggregate state.
select id,
json_arrayagg(id) over (order by id rows between unbounded preceding and unbounded following) as ids
from t where p = 2 order by id;

prepare q from 'select count(*) over () as n, sum(v) over () as total from t order by id limit 1';
execute q;
insert into t values (9,2,2,20,'i');
execute q;
deallocate prepare q;

-- Cross the physical 8192-group boundary with a peer spanning that boundary.
select id, running_sum, running_count
from (
select result as id,
sum(cast(result as decimal(20,2))) over (order by result rows unbounded preceding) as running_sum,
count(*) over (order by result div 3) as running_count
from generate_series(1,9000,1) g
) w where id in (1,8191,8192,8193,9000) order by id;

drop database window_unbounded_aggregate;

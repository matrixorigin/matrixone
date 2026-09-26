drop database if exists cte_outer_reference;
create database cte_outer_reference;
use cte_outer_reference;
create table pages(id int primary key, parent_id int, active tinyint);
insert into pages values (1,null,1),(2,1,0),(3,2,1),(4,3,1),(5,1,1);

-- Local projection preserves NULL and repeated outer parameter values.
select p.id,
       (with q(n) as (select p.parent_id) select n from q) as parent
from pages p order by p.id;

-- A NULL seed is one row, not an empty recursive relation.
select p.id,
       (with recursive r(n) as (
          select p.parent_id
          union all
          select n-1 from r where n>1
        ) select count(*) from r) as depth
from pages p order by p.id;

-- Each row checks only its own ancestors.
select p.id from pages p where not exists (
  with recursive ancestors as (
    select a.* from pages a where a.id=p.parent_id
    union all
    select a.* from pages a join ancestors on ancestors.parent_id=a.id
  ) select * from ancestors where active=0
) and active=1 order by p.id;

-- Empty anchors must produce zero, not a fabricated seed row.
select p.id,
       (with recursive r(n) as (
          select a.id from pages a where a.id=p.parent_id
          union all
          select n-1 from r where n>1
        ) select count(*) from r) as depth
from pages p order by p.id;

-- Outer parameters used in a recursive member stay fixed across iterations.
select p.id,
       (with recursive r(n) as (
          select 1
          union all
          select n+1 from r where n<p.parent_id
        ) select count(*) from r) as depth
from pages p order by p.id;

-- DISTINCT is local to an outer row, including repeated parameter values.
select p.id,
       (with recursive r(n) as (
          select p.parent_id
          union distinct
          select n from r where n is not null
        ) select count(*) from r) as depth
from pages p order by p.id;

-- Multiple references retain only the declared output columns.
select p.id,
       (with q(n) as (select p.parent_id)
        select a.n+b.n from q a join q b on a.n=b.n) as doubled
from pages p order by p.id;

-- Scalar cardinality violations must not become an ordinary LEFT JOIN.
select (with q(n) as (select p.parent_id from pages a where a.id<=2)
        select n from q) from pages p;

-- The recursion limit applies to the longest nonempty parameter partition.
set @saved_cte_depth=@@cte_max_recursion_depth;
set cte_max_recursion_depth=2;
select p.id,
       (with recursive r(n) as (
          select p.parent_id union all select n-1 from r where n>1
        ) select count(*) from r) as depth
from pages p order by p.id;
set cte_max_recursion_depth=1;
select (with recursive r(n) as (
          select p.parent_id union all select n-1 from r where n>1
        ) select count(*) from r) from pages p;
set cte_max_recursion_depth=@saved_cte_depth;

-- Hidden parameters remain subject to the existing recursive memory quota.
set @saved_cte_memory=@@cte_max_memory_bytes;
set cte_max_memory_bytes=1;
-- @regex("recursive CTE memory quota exceeded",true)
select (with recursive r(n) as (
          select p.parent_id union all select n-1 from r where n>1
        ) select count(*) from r) from pages p;
set cte_max_memory_bytes=@saved_cte_memory;

-- Reexecution observes the current parameter, without retaining prior seeds.
prepare local_cte_stmt from 'select p.id, (with recursive r(n) as (select p.parent_id union all select n-1 from r where n>1) select count(*) from r) as depth from pages p where p.id=? order by p.id';
set @page_id=4;
execute local_cte_stmt using @page_id;
set @page_id=1;
execute local_cte_stmt using @page_id;
deallocate prepare local_cte_stmt;

-- A window must restart for each outer identity, even for repeated parameters.
select p.id,
       (with q(n) as (select p.parent_id)
        select row_number() over (order by n) from q) as rn
from pages p where p.id in (2, 5) order by p.id;
select p.id,
       (with q(n) as (select p.parent_id from pages a where a.id in (2, 5))
        select max(rn) from (select row_number() over (order by n) as rn from q) w) as max_rn
from pages p where p.id in (2, 5) order by p.id;

-- HAVING drops a nonempty scalar aggregate row; missing input still yields COUNT=0.
select p.id,
       (with q(n) as (select p.parent_id)
        select count(*) from q having count(*)=0) as filtered_count
from pages p where p.id in (2, 5) order by p.id;
select p.id,
       (with q(n) as (select p.parent_id from pages a where a.id=p.id and a.active=0)
        select count(*) from q having count(*)=0) as filtered_count
from pages p where p.id in (2, 5) order by p.id;

-- Both UNION ALL arms export the same hidden identity column, not a visible column.
select p.id from pages p where exists (
  with q(n) as (select p.parent_id)
  select n from q union all select n from q
) and p.id in (2, 5) order by p.id;

-- A UNION ALL scalar still observes both rows, not an extra visible identity column.
select (with q(n) as (select p.parent_id)
        select n from q union all select n from q)
from pages p where p.id=2;

-- A branch without a replay identity is rejected rather than mismatching schemas.
select p.id from pages p where exists (
  with q(n) as (select p.parent_id)
  select n from q union all select 7
) and p.id=2;

-- HAVING cannot move past pagination or split across set-operation branches.
select p.id, (with q(n) as (select p.parent_id)
              select count(*) from q having count(*)=0 limit 0) as c
from pages p where p.id=2;
select p.id, (with q(n) as (select p.parent_id)
              select count(*) from q having count(*)=0 limit 1 offset 1) as c
from pages p where p.id=2;
select p.id from pages p where exists (
  with q(n) as (select p.parent_id from pages a where a.id=-1)
  select count(*) from q having count(*)=0
  union all select count(*) from q having count(*)=0
) and p.id=2;

-- A consumer LEFT JOIN must retain its unmatched left rows.
select p.id, (with q(n) as (select p.parent_id)
              select count(*) from pages a left join q on a.id=q.n) as c
from pages p where p.id=2;

-- The projected window slot cannot be substituted with the COUNT HAVING result.
select p.id, (with q(n) as (select p.parent_id)
              select row_number() over (order by count(*)) from q having count(*)=1) as rn
from pages p where p.id=2;

-- Non-equality WHERE cannot lose HAVING in the non-equality aggregate path.
select p.id, (with q(n) as (select p.parent_id)
              select count(n) from q where n<=p.id having count(n)=0) as c
from pages p where p.id=2;

-- An ungrouped aggregate on empty input still returns a row to EXISTS and IN.
select p.id from pages p where exists (
  with q(n) as (select p.parent_id from pages a where a.id=-1)
  select count(*) from q
) and p.id=2;
select p.id from pages p where 0 in (
  with q(n) as (select p.parent_id from pages a where a.id=-1)
  select count(*) from q
) and p.id=2;
select p.id from pages p where exists (
  with q(n) as (select p.parent_id from pages a where a.id=-1)
  select count(*) from q union all select count(*) from q
) and p.id=2;
select p.id, (with q(n) as (select p.parent_id from pages a where a.id=-1)
              select sum(c) from (select count(*) as c from q) s) as total
from pages p where p.id=2;

-- User WHERE filters before window numbering, not after.
select p.id, (with q(n) as (select a.id+p.parent_id-1 from pages a where a.id in (2, 5))
              select row_number() over (order by n desc) from q where n<=p.id) as rn
from pages p where p.id=2;

-- Pagination can delete an aggregate result row without HAVING.
select p.id, (with q(n) as (select p.parent_id)
              select count(*) from q limit 0) as c from pages p where p.id=2;
select p.id, (with q(n) as (select p.parent_id)
              select count(*) from q limit 1 offset 1) as c from pages p where p.id=2;

-- A non-COUNT window projection cannot inherit COUNT empty-group fallback.
select p.id, (with q(n) as (select p.parent_id)
              select row_number() over (order by count(*)) from q) as rn
from pages p where p.id=2;

-- Explicit grouping on empty input produces no COUNT result row, not zero.
select p.id, (with q(n) as (select p.parent_id from pages a where a.id=-1)
              select count(*) from q group by n) as c
from pages p where p.id=2;
select p.id, (with q(n) as (select p.parent_id from pages a where a.id=-1)
              select count(*) from q group by n limit 1 offset 1) as c
from pages p where p.id=2;
select p.id, (with q(n) as (select p.parent_id)
              select count(*) from q group by n) as c
from pages p where p.id in (2, 5) order by p.id;

-- An aggregate window must not inherit COUNT's empty-input fallback.
select p.id, (with q(n) as (select p.parent_id)
              select row_number() over (order by count(*)) from q group by n) as rn
from pages p where p.id=2;

-- Consumer JOIN ON references to the outer row must not reach the executor.
select p.id, (with q(n) as (select p.parent_id)
              select count(*) from q join pages b on n=b.id and n=p.id) as c
from pages p where p.id=2;

-- Empty outer input starts no parameter partitions.
select p.id,
       (with recursive r(n) as (
          select p.parent_id union all select n-1 from r where n>1
        ) select count(*) from r) as depth
from pages p where p.id=99;

drop database cte_outer_reference;

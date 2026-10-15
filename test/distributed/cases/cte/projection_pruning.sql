drop database if exists cte_projection_pruning;
create database cte_projection_pruning;
use cte_projection_pruning;
create table small(g int,h int,v int);
insert into small values (1,null,10);

-- #30141: shared readers must use the producer's compacted column positions.
with r as (select g,v total,h n,v mx from small where v>0)
select a.g,a.mx,a.total,b.g,b.mx,b.total
from r a join r b on a.g<=>b.g and a.mx=b.mx;

-- Keeping the intermediate column is the nearest non-pruning control.
with r as (select g,v total,h n,v mx from small where v>0)
select a.*,b.* from r a join r b on a.g<=>b.g and a.mx=b.mx;

with r as (select g,sum(v) total,count(*) n,max(v) mx from small group by g)
select a.g,a.mx,a.total,b.g,b.mx,b.total
from r a join r b on a.g<=>b.g and a.mx=b.mx;

-- Distinct totals, counts, minima and maxima expose in-range wrong-column reads.
insert into small values (1,5,4),(2,8,7),(2,9,20),(null,3,6),(null,4,null);
with r as (select g,sum(v) total,count(*) n,max(v) mx,min(v) mn from small group by g)
select a.g,a.total,a.mx,a.mn,b.mx
from r a join r b on a.g<=>b.g and a.mx=b.mx order by a.g;

-- Readers have different demands; their output positions are independent.
with r as (select g,sum(v) total,count(*) n,max(v) mx from small group by g)
select a.g,a.total,b.g,b.mx
from r a join r b on a.g<=>b.g and a.total=b.total order by a.g;

-- Prune the leading columns and keep only late numeric outputs.
with r as (select g,sum(v) total,count(*) n,max(v) mx from small group by g)
select a.total,a.mx,b.total,b.mx
from r a join r b on a.mx=b.mx order by a.mx;

-- One reader preserving the middle column pins that producer output.
with r as (select g,sum(v) total,count(*) n,max(v) mx from small group by g)
select a.g,a.mx,a.total,b.n
from r a join r b on a.g<=>b.g and a.mx=b.mx order by a.g;

-- A count-only fallback must publish all the columns it still reads.
with r as (select g,sum(v) total,count(*) n,max(v) mx from small group by g)
select count(*) x from r union all select max(mx) from r order by x;

-- Count-only fallback retains variable-width columns for storage admission.
with r as (select g,max(cast(h as varchar(10))) payload from small group by g)
select count(*) x from r union all select max(g) from r order by x;

-- HAVING and LIMIT reach the same materialization boundary without ROLLUP.
with r as (select g,sum(v) total,count(*) n,max(v) mx from small group by g having total>6)
select a.g,a.total,a.mx,b.mx
from r a join r b on a.g<=>b.g and a.mx=b.mx order by a.g;

with r as (select g,v total,h n,v+1 mx from small limit 10)
select a.g,a.total,a.mx,b.mx
from r a join r b on a.g<=>b.g and a.mx=b.mx order by a.g,a.mx;

-- Pruning above UNION preserves the shared producer's semantic columns.
with r as (select g,sum(v) total,count(*) n,max(v) mx from small group by g)
select g,sum(total) total,max(mx) mx
from (select * from r union all select * from r) u group by g order by g;

-- A reused CTE containing another CTE preserves its projected values.
with r as (select g,sum(v) total,count(*) n,max(v) mx from small group by g),
s as (select a.g,a.total,a.mx,b.total btotal from r a join r b on a.g<=>b.g and a.mx=b.mx)
select x.g,x.mx,y.total from s x join s y on x.g<=>y.g and x.mx=y.mx order by x.g;

-- Grouping markers distinguish the real NULL group from the grand total.
with r as (select g,sum(v) total,count(*) n,max(v) mx,grouping(g) gm from small group by g with rollup)
select a.g,a.gm,a.total,a.mx,b.mx
from r a join r b on a.g<=>b.g and a.gm=b.gm and a.mx=b.mx order by a.gm,a.g;

with r as (select g,sum(v) total,count(*) n,max(v) mx,grouping(g) gm from small group by cube(g))
select a.g,a.gm,a.total,a.mx,b.mx
from r a join r b on a.g<=>b.g and a.gm=b.gm and a.mx=b.mx order by a.gm,a.g;

with r as (select g,sum(v) total,count(*) n,max(v) mx,grouping(g) gm from small group by grouping sets ((g),()))
select a.g,a.gm,a.total,a.mx,b.mx
from r a join r b on a.g<=>b.g and a.gm=b.gm and a.mx=b.mx order by a.gm,a.g;

with r as (select g,sum(v) total,count(*) n,max(v) mx,grouping(g) gm from small group by g with rollup)
select g,gm,sum(total) total,max(mx) mx
from (select * from r union all select * from r) u group by g,gm order by gm,g;

-- Preparing and executing twice must not apply the positional map twice.
prepare cte_stmt from 'with r as (select g,sum(v) total,count(*) n,max(v) mx from small group by g) select a.g,a.mx,a.total,b.g,b.mx,b.total from r a join r b on a.g<=>b.g and a.mx=b.mx order by a.g';
execute cte_stmt;
execute cte_stmt;
deallocate prepare cte_stmt;

-- DML retains the existing positional-scan contract and the sentinel row.
create table output_rows(g int,mx int,total int,gg int,bmx int,btotal int);
insert into output_rows values (-1,999,999,-1,999,999);
insert into output_rows
with r as (select g,sum(v) total,count(*) n,max(v) mx from small group by g)
select a.g,a.mx,a.total,b.g,b.mx,b.total
from r a join r b on a.g<=>b.g and a.mx=b.mx;
select * from output_rows order by g;

-- Empty producer and all-NULL scalar aggregates preserve cardinality.
delete from small;
with r as (select g,v total,h n,v mx from small where v>0)
select a.g,a.mx,a.total,b.g,b.mx,b.total
from r a join r b on a.g<=>b.g and a.mx=b.mx;

with r as (select g,sum(v) total,count(*) n,max(v) mx from small group by g)
select count(*) x from r union all select max(mx) from r order by x;

with r as (select 1 g,sum(v) total,count(*) n,max(v) mx from small)
select a.g,a.total,a.mx,b.mx from r a join r b on a.g=b.g and a.mx<=>b.mx;

drop database cte_projection_pruning;

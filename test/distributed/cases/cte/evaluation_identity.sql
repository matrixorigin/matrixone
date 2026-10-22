-- Each reference must read the same produced rows; compare identities, not random snapshots.
drop database if exists cte_evaluation_identity;
create database cte_evaluation_identity;
use cte_evaluation_identity;
create table t(k int);
insert into t values(1),(2),(2),(null);
with r as(select rand() x) select count(*) from r a join r b on a.x=b.x;
with r as(select k,uuid() x from t) select count(*) from r a join r b on a.k<=>b.k and a.x=b.x;
with r as(select 1 k,rand() x union all select 2,rand()) select count(*) from r a join r b on a.k=b.k and a.x=b.x;
with recursive r(n,x) as(select 1,uuid() union all select n+1,x from r where n<5 limit 2 offset 1) select count(*) from r a join r b on a.n=b.n and a.x=b.x;
with r as(select uuid() x),s as(select x from r) select count(*) from s a join s b on a.x=b.x join r c on a.x=c.x;
with recursive base as(select uuid() x),r(n,x) as(select 1,x from base union all select n+1,base.x from r join base on r.x=base.x where n<3) select count(*) from r;
drop database cte_evaluation_identity;

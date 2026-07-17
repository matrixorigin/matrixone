-- @case
-- @desc: aggregate functions over correlated scalar aggregate subqueries, issues #23073 and #23076
-- @label:bvt
drop table if exists nested_agg_outer;
drop table if exists nested_agg_inner;
create table nested_agg_outer (id int, grp int, name varchar(10));
create table nested_agg_inner (outer_id int);
insert into nested_agg_outer values (1, 1, 'c'), (2, 1, 'a'), (3, 2, 'b');
insert into nested_agg_inner values (1), (1), (2);

select o.grp, cast(avg((select count(*) from nested_agg_inner i where i.outer_id = o.id)) as decimal(10, 2)) as avg_matches from nested_agg_outer o group by o.grp order by o.grp;

select o.grp, group_concat(o.name order by o.name) as names, cast(avg((select count(*) from nested_agg_inner i where i.outer_id = o.id)) as decimal(10, 2)) as avg_matches from nested_agg_outer o group by o.grp order by o.grp;

with stats as (select o.grp, cast(sum((select count(*) from nested_agg_inner i where i.outer_id = o.id)) as decimal(10, 0)) as total_matches from nested_agg_outer o group by o.grp) select * from stats order by grp;

drop table nested_agg_inner;
drop table nested_agg_outer;

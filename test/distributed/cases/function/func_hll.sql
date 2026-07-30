drop table if exists hll_01;
drop table if exists hll_daily;

create table hll_01(dt date, user_id int);
insert into hll_01 values ('2026-05-01', 1), ('2026-05-01', 1), ('2026-05-01', 2), ('2026-05-02', 2), ('2026-05-02', 3), ('2026-05-02', null);

select hll_cardinality(hll_add_agg(user_id)) from hll_01;
select dt, hll_cardinality(hll_add_agg(user_id)) from hll_01 group by dt order by dt;

create table hll_daily(dt date, sketch blob);
insert into hll_daily select dt, hll_add_agg(user_id) from hll_01 group by dt;
select hll_cardinality(hll_merge_agg(sketch)) from hll_daily;
select hll_cardinality(hll_merge_agg(null));
select hll_cardinality(null);
select hll_cardinality(cast('bad' as varbinary));

drop table hll_daily;
drop table hll_01;

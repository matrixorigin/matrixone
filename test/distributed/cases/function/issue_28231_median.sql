-- issue #28231: MEDIAN must not lose the half unit between BIGINT extremes.
drop table if exists issue_28231_median;
create table issue_28231_median(g int, v bigint);
insert into issue_28231_median values
    (1, -9223372036854775808),
    (1,  9223372036854775807),
    (2, -1),
    (2,  0),
    (2,  1),
    (2,  2);

-- Global, filtered, grouped, and DISTINCT paths share the same midpoint rule.
select median(v) from issue_28231_median;
select median(v) from issue_28231_median where g = 1;
select g, median(v) from issue_28231_median group by g order by g;
select median(distinct v) from issue_28231_median where g = 1;

drop table issue_28231_median;

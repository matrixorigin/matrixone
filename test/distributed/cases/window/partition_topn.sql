-- Regression coverage for issue #27067. A small literal ROW_NUMBER upper
-- bound should prune each logical partition before the Window operator while
-- preserving the original filter and complete-row semantics.
drop database if exists partition_topn;
create database partition_topn;
use partition_topn;

create table t_partition_topn (
    g int,
    score int,
    tie_key int,
    payload varchar(20)
);
insert into t_partition_topn values
    (1, 10, 1, 'a'),
    (1, 10, 2, 'b'),
    (1, 20, 3, 'c'),
    (2, null, 1, 'null-score'),
    (2, 5, 2, 'e'),
    (2, 7, 3, 'f'),
    (null, 3, 1, 'x'),
    (null, 1, 2, 'y');

select g, score, tie_key, payload, rn
from (
    select g, score, tie_key, payload,
           row_number() over (partition by g order by score desc, tie_key desc) rn
    from t_partition_topn
) ranked
where rn = 1
order by g is null, g;

select g, score, tie_key, payload, rn
from (
    select g, score, tie_key, payload,
           row_number() over (partition by g order by score desc, tie_key desc) rn
    from t_partition_topn
) ranked
where rn <= 2
order by g is null, g, rn;

select g, score, tie_key, payload, rn
from (
    select g, score, tie_key, payload,
           row_number() over (partition by g order by score desc, tie_key desc) rn
    from t_partition_topn
) ranked
where 2 >= rn and rn >= 2
order by g is null, g;

select g, score, tie_key, payload, rn
from (
    select g, score, tie_key, payload,
           row_number() over (
               partition by g
               order by score desc, tie_key desc
               rows between unbounded preceding and current row
           ) rn
    from t_partition_topn
) ranked
where rn < 3
order by g is null, g, rn;

-- RANK is a control case and must keep the generic Window path. The tie
-- proves that ROW_NUMBER-style truncation would be semantically wrong here.
select g, score, tie_key, payload, rnk
from (
    select g, score, tie_key, payload,
           rank() over (partition by g order by score asc) rnk
    from t_partition_topn
) ranked
where rnk <= 1
order by g is null, g, tie_key;

create table t_partition_topn_empty like t_partition_topn;
select count(*)
from (
    select row_number() over (partition by g order by score) rn
    from t_partition_topn_empty
) ranked
where rn <= 2;

drop database partition_topn;

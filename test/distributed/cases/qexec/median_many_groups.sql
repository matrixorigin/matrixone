drop database if exists qa_median_many_groups_32141;
create database qa_median_many_groups_32141;
use qa_median_many_groups_32141;
create table t (id bigint, v bigint);
insert into t select result, if(result % 17 = 0, null, result) from generate_series(1, 8193) g;
insert into t select result, if(result % 17 = 0, null, result + 2) from generate_series(1, 8193) g;

set @saved_agg_spill_mem_32141 = @@agg_spill_mem;
set @@agg_spill_mem = 1000000000;
-- Check every group, including all-NULL groups, across the 8192-row boundary.
select count(*),
       sum(if((id % 17 = 0 and n is null) or (id % 17 <> 0 and n = id + 1), 0, 1)),
       sum(if((id % 17 = 0 and d64 is null) or (id % 17 <> 0 and d64 = id + 1), 0, 1)),
       sum(if((id % 17 = 0 and d128 is null) or (id % 17 <> 0 and d128 = id + 1), 0, 1))
from (select id, median(v) n, median(cast(v as decimal(10,2))) d64,
             median(cast(v as decimal(30,2))) d128 from t group by id) grouped;

-- Below 10000 the variable selects the test-only group-count spill threshold.
set @@agg_spill_mem = 8000;
select count(*),
       sum(if((id % 17 = 0 and n is null) or (id % 17 <> 0 and n = id + 1), 0, 1)),
       sum(if((id % 17 = 0 and d64 is null) or (id % 17 <> 0 and d64 = id + 1), 0, 1)),
       sum(if((id % 17 = 0 and d128 is null) or (id % 17 <> 0 and d128 = id + 1), 0, 1))
from (select id, median(v) n, median(cast(v as decimal(10,2))) d64,
             median(cast(v as decimal(30,2))) d128 from t where id > 0 group by id) grouped;
set @@agg_spill_mem = @saved_agg_spill_mem_32141;

select median(v) from t where id < 0;
drop database qa_median_many_groups_32141;

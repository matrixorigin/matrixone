drop database if exists qa_median_spill_32141;
create database qa_median_spill_32141;
use qa_median_spill_32141;
create table t (id bigint, v bigint);
insert into t select result, if(result % 17 = 0, null, result) from generate_series(1, 16385) g;
insert into t select result, if(result % 17 = 0, null, result + 2) from generate_series(1, 16385) g;
insert into t select result, if(result % 17 = 0, null, result + 2) from generate_series(1, 49) g;
set @saved_agg_spill_mem_32141 = @@agg_spill_mem;
-- The high threshold is a no-spill test control.
set @@agg_spill_mem = 1000000000;
select count(*),
       sum(if((id % 17 = 0 and n is null) or (id % 17 <> 0 and n = id + if(id <= 49, 2, 1)), 0, 1)),
       sum(if((id % 17 = 0 and d is null) or (id % 17 <> 0 and d = id + if(id <= 49, 2, 1)), 0, 1))
from (select id, median(v) n, median(cast(v as decimal(20,2))) d from t group by id) grouped;
-- DISTINCT disables group spill on this branch; check duplicate and all-NULL
-- groups separately so the spill query below actually exercises spill.
select id, median(v), median(cast(v as decimal(20,2))), median(distinct v)
from t where id in (1, 17) group by id order by id;
select median(v) from t where id < 0;
-- Values below 10000 select the test-only group-count spill threshold.
-- Use a different SQL text so a previously cached plan cannot retain the high threshold.
set @@agg_spill_mem = 9000;
select count(*),
       sum(if((id % 17 = 0 and n is null) or (id % 17 <> 0 and n = id + if(id <= 49, 2, 1)), 0, 1)),
       sum(if((id % 17 = 0 and d is null) or (id % 17 <> 0 and d = id + if(id <= 49, 2, 1)), 0, 1))
from (select id, median(v) n, median(cast(v as decimal(20,2))) d from t where id > 0 group by id) grouped;
set @@agg_spill_mem = @saved_agg_spill_mem_32141;
drop database qa_median_spill_32141;

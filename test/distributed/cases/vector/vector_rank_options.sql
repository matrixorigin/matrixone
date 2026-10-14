-- Query-level IVF probe options: independent small fixture, no global changes.
drop database if exists vector_rank_options;
create database vector_rank_options;
use vector_rank_options;
set @saved_probe_limit = @@probe_limit;
set @saved_ivf = @@experimental_ivf_index;
set @saved_auto = @@enable_vector_auto_mode_by_default;
set @saved_pre = @@enable_vector_prefilter_by_default;
set experimental_ivf_index = 1;
set enable_vector_auto_mode_by_default = 0;
set enable_vector_prefilter_by_default = 0;
create table t(id bigint primary key, v vecf32(2));
insert into t values (1, '[0,0]'), (2, '[1,1]'), (3, '[2,2]');
create index ix using ivfflat on t(v) lists=1 op_type 'vector_l2_ops';
set probe_limit = 5;

-- Mode-less options retain default mode; override affects planning, not session state.
-- @regex("NProbe: 1",true)
explain verbose select id from t order by l2_distance(v, '[0,0]') limit 2 by rank with option 'nprobe=1';
-- @regex("NProbe: 9",true)
explain verbose select id from t order by l2_distance(v, '[0,0]') limit 2 by rank with option 'mode=post', 'nprobe=9';
-- @regex("NProbe: 5",true)
explain verbose select id from t order by l2_distance(v, '[0,0]') limit 2 by rank with option 'mode=post';
select @@probe_limit;
select id from t order by l2_distance(v, '[0,0]') limit 2 by rank with option 'nprobe=1';
select id from t order by l2_distance(v, '[0,0]') limit 2 by rank with option 'mode=post', 'nprobe=9';
prepare rank_stmt from "select id from t order by l2_distance(v, '[0,0]') limit 2 by rank with option 'nprobe=1'";
execute rank_stmt;
deallocate prepare rank_stmt;

-- Rejected options must be visible errors, including without a mode.
select id from t limit 1 by rank with option 'nprobe=0';
select id from t limit 1 by rank with option 'mode=post', 'nprobe=80001';
select id from t limit 1 by rank with option 'nprobe=not-a-number';
select id from t limit 1 by rank with option 'fudge_factor=100';
select id from t limit 1 by rank with option 'mode=post', 'nproeb=99';
select id from t limit 1 by rank with option 'nprobe=1=2';
select id from t limit 1 by rank with option 1;

set probe_limit = @saved_probe_limit;
set experimental_ivf_index = @saved_ivf;
set enable_vector_auto_mode_by_default = @saved_auto;
set enable_vector_prefilter_by_default = @saved_pre;
drop database vector_rank_options;

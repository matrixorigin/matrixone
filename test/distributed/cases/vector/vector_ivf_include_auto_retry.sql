-- Regression for explicit IVF POST fallback with included columns.
-- The initial POST path sees only the nearby category=0 cluster. An empty
-- INCLUDE-covered page must retry the same subtree exactly, while AUTO keeps
-- its existing POST/PRE/FORCE behavior.
drop database if exists vector_ivf_include_auto_retry;
create database vector_ivf_include_auto_retry;
use vector_ivf_include_auto_retry;

create table t(
    id int primary key,
    vec vecf32(3),
    category int,
    payload varchar(32)
);

insert into t values
    (1, '[0.10,0.10,0.10]', 0, 'near-1'),
    (2, '[0.11,0.11,0.11]', 0, 'near-2'),
    (3, '[0.12,0.12,0.12]', 0, 'near-3'),
    (4, '[0.13,0.13,0.13]', 0, 'near-4'),
    (5, '[0.14,0.14,0.14]', 0, 'near-5'),
    (6, '[0.15,0.15,0.15]', 0, 'near-6'),
    (7, '[0.16,0.16,0.16]', 0, 'near-7'),
    (8, '[0.17,0.17,0.17]', 0, 'near-8'),
    (9, '[0.18,0.18,0.18]', 0, 'near-9'),
    (10, '[0.19,0.19,0.19]', 0, 'near-10'),
    (11, '[0.20,0.20,0.20]', 0, 'near-11'),
    (12, '[0.21,0.21,0.21]', 0, 'near-12'),
    (13, '[0.22,0.22,0.22]', 0, 'near-13'),
    (14, '[0.23,0.23,0.23]', 0, 'near-14'),
    (15, '[0.24,0.24,0.24]', 0, 'near-15'),
    (16, '[0.25,0.25,0.25]', 0, 'near-16'),
    (17, '[0.26,0.26,0.26]', 0, 'near-17'),
    (18, '[0.27,0.27,0.27]', 0, 'near-18'),
    (19, '[0.28,0.28,0.28]', 0, 'near-19'),
    (20, '[0.29,0.29,0.29]', 0, 'near-20'),
    (999, '[10,10,10]', 1, 'hit');

create index idx using ivfflat on t(vec)
    lists=5 op_type 'vector_l2_ops' include(category, payload);
set experimental_ivf_index = 1;
set probe_limit = 1;

-- Check the executed empty POST and non-empty exact fallback, not only final rows.
-- Match each operator's own Analyze line without pinning IDs, timings or costs.
-- @separator:table
-- @ignore:0
-- @regex("Adaptive Top[^\n]*\n[^\n]*Output:[^\n]*\n[^\n]*Analyze:[^\n]*inputRows=1 outputRows=1", true)
-- @regex("->  Project[^\n]*\n[^\n]*Output:[^\n]*\n[^\n]*Analyze:[^\n]*inputRows=0 outputRows=0", true)
-- @regex("Vector Index Scan[^\n]*\n[^\n]*Output:[^\n]*\n[^\n]*Analyze:[^\n]*outputRows=[1-9][0-9]*", true)
-- @regex("->  Sort[^\n]*\n[^\n]*Output:[^\n]*\n[^\n]*Analyze:[^\n]*inputRows=1 outputRows=1", true)
explain analyze verbose select id, category, payload
from t
where category = 1
order by l2_distance(vec, '[0,0,0]')
limit 1 by rank with option 'mode=post';

prepare p from 'select id, category, payload from t where category = 1 order by l2_distance(vec, ''[0,0,0]'') limit 1 by rank with option ''mode=post''';
execute p;
execute p;
deallocate prepare p;

select id, category, payload
from t
where category = 1
order by l2_distance(vec, '[0,0,0]')
limit 1 by rank with option 'mode=auto';

prepare s from 'select id, category, payload from t where category = 1 order by l2_distance(vec, ''[0,0,0]'') limit 1 by rank with option ''mode=auto''';
execute s;
execute s;
deallocate prepare s;

drop table t;
drop database vector_ivf_include_auto_retry;
-- @regex("vector_ivf_include_auto_retry", false)
show databases;

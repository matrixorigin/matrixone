drop database if exists vector_ivf_projection;
create database vector_ivf_projection;
use vector_ivf_projection;

create table t(id bigint, v vecf32(4));
insert into t values
    (1, '[1,1,1,1]'),
    (2, '[2,2,2,2]'),
    (3, '[3,3,3,3]'),
    (4, '[10,10,10,10]');
create index idx_v using ivfflat on t(v) lists=2 op_type 'vector_l2_ops';

-- @separator:table
-- @regex("Table Function on ivf_search", true)
explain select l2_distance(v, '[1,1,1,1]') as distance
from t order by distance limit 3;

select id, l2_distance(v, '[1,1,1,1]') as distance
from t order by distance limit 3;

select l2_distance(v, '[1,1,1,1]') as distance
from t order by distance limit 3;

drop database vector_ivf_projection;

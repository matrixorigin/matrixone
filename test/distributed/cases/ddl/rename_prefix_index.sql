drop database if exists rename_prefix_index;
create database rename_prefix_index;
use rename_prefix_index;

create table t (
    id int primary key,
    a varchar(32),
    b varchar(32),
    payload varchar(32),
    unique key uq_a(a(4)),
    key idx_ab(a(3), b(2))
);
insert into t values
    (1, 'abcd-one', 'xy-one', 'p1');

alter table t rename column a to headline;
show create table t;

-- Both ordinary indexes must carry the renamed prefix key in catalog metadata.
select distinct name, column_name, algo_params
from mo_catalog.mo_indexes
where database_id = (
    select dat_id from mo_catalog.mo_database where datname = 'rename_prefix_index'
) and name in ('uq_a', 'idx_ab')
order by name, column_name;

-- The renamed unique prefix still rejects an equal prefix, while a different
-- prefix remains valid and must be materialized with the same metadata.
insert into t values (2, 'abcd-two', 'xy-two', 'dup');
insert into t values (3, 'abce-two', 'xz-two', 'p3');

select id, headline, b, payload from t order by id;
select mo_ctl('dn', 'flush', 'rename_prefix_index.t');
select id, headline, b, payload
from t force index(uq_a) where headline = 'abcd-one';
select id, headline, b, payload
from t ignore index(uq_a) where headline = 'abcd-one';
select id, headline, b, payload
from t force index(idx_ab) where headline = 'abce-two' and b = 'xz-two';
select id, headline, b, payload
from t ignore index(idx_ab) where headline = 'abce-two' and b = 'xz-two';

drop database rename_prefix_index;

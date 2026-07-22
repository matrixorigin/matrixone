-- Regression for issue #24927: merge must preserve BLOB values while applying
-- changes to other columns in the same rows.
drop database if exists data_branch_blob_merge;
create database data_branch_blob_merge;
use data_branch_blob_merge;

create table t_src (
    id bigint primary key,
    v int,
    bin binary(5),
    vbin varbinary(5),
    b blob
);

insert into t_src values
    (1, 10, x'61005c27ff', x'61005c27ff', x'61005c27ff'),
    (2, 20, x'000102', x'000102', x'000102'),
    (3, 30, null, null, null),
    (4, 40, x'', x'', x'');

data branch create table t_branch from t_src;
update t_branch set v = v + 1 where id in (1, 2, 4);

data branch diff t_branch against t_src output count;
data branch merge t_branch into t_src when conflict accept;

select id, v, hex(bin), hex(vbin), hex(b), ifnull(length(b), -1) as b_len from t_src order by id;
data branch diff t_branch against t_src output count;

drop database data_branch_blob_merge;

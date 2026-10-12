-- issue #29842: strict JSONLINE LOAD must fail (not silently drop) an unfinished final record
set @saved_sql_mode = @@sql_mode;
set sql_mode = 'STRICT_TRANS_TABLES';
drop table if exists issue_29842_t;
create table issue_29842_t(id int primary key, v json);

-- one complete record followed by an unfinished final record (no trailing newline)
load data infile {'filepath'='$resources/load_data/issue_29842_jsonline_unfinished_tail.jl','format'='jsonline','jsondata'='object'} into table issue_29842_t;
select count(*) from issue_29842_t;

-- same with a trailing newline
load data infile {'filepath'='$resources/load_data/issue_29842_jsonline_unfinished_tail_nl.jl','format'='jsonline','jsondata'='object'} into table issue_29842_t;
select count(*) from issue_29842_t;

-- a subsequent valid statement still succeeds
load data infile {'filepath'='$resources/load_data/issue_29842_jsonline_complete.jl','format'='jsonline','jsondata'='object'} into table issue_29842_t;
select id, v from issue_29842_t order by id;
insert into issue_29842_t values (3, '{"a":3}');
select count(*) from issue_29842_t;

drop table issue_29842_t;
set sql_mode = @saved_sql_mode;

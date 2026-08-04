-- Regression for issue #26128.
-- Public DATA BRANCH output paths preserve difficult strings and input tables.
-- The batch.Dup failure injection itself remains in frontend Go tests.
drop database if exists bvt_issue_26128;
create database bvt_issue_26128;
use bvt_issue_26128;

create table base(
    id int primary key,
    payload varchar(200)
);
insert into base values
    (1, 'comma,value'),
    (2, 'quote"value'),
    (3, 'back\\slash'),
    (4, '中文原值'),
    (5, 'line1\nline2');
data branch create table branch_t from base;
update branch_t set payload = 'comma,updated' where id = 1;
delete from branch_t where id = 2;
update branch_t set payload = '中文"更新"' where id = 4;
insert into branch_t values (6, 'new\nline');

select id, hex(payload) as payload_hex from base order by id;
select id, hex(payload) as payload_hex from branch_t order by id;

-- The path and hint are generated dynamically; success and both columns are
-- still required, while their text is excluded from baseline comparison.
-- @ignore:0,1
data branch diff branch_t against base output file '/tmp/';

data branch diff branch_t against base output as diff_rows;
select __mo_diff_source,
       __mo_diff_flag,
       id,
       hex(payload) as payload_hex
from diff_rows
order by id;
select count(*) as output_as_branch_metadata
from mo_catalog.mo_branch_metadata b
join mo_catalog.mo_tables t on t.rel_id = b.table_id
where t.account_id = 0
  and t.reldatabase = 'bvt_issue_26128'
  and t.relname = 'diff_rows';

-- Output generation must not mutate either input.
select id, hex(payload) as payload_hex from base order by id;
select id, hex(payload) as payload_hex from branch_t order by id;

drop database bvt_issue_26128;

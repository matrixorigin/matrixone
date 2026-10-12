-- Regression for sequence function access to a legal identifier containing a backtick.
drop database if exists issue_28245;
create database issue_28245;
use issue_28245;

create sequence `s``q` increment by 2 minvalue 1 maxvalue 99 start with 7 no cycle;
select nextval('s`q') as v;
select setval('s`q', 21, true) as v;
select nextval('s`q') as v;

drop database issue_28245;

-- #31909 nextval/currval/setval must resolve names under the session's
-- lower_case_table_names policy exactly like CREATE SEQUENCE does.
drop database if exists issue31909;
create database issue31909;
use issue31909;

create sequence MySeq;
select nextval('MySeq');
select currval('MYSEQ');
select setval('MySeq', 10);
select nextval('myseq');
create table t2 (id int default nextval('MySeq'), v int);
insert into t2(v) values (1);
select * from t2;

-- existing rejection contracts keep firing and the session recovers
select nextval('missing_seq');
select nextval(null);
create table ordinary(a int);
select nextval('ordinary');
select lastval();

drop database if exists issue31909;

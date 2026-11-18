drop database if exists odku_nullable_defaults;
create database odku_nullable_defaults;
use odku_nullable_defaults;

create table t (id int primary key, s varchar(128), d decimal(10,2), n int);
insert into t values (1, 'old', 12.34, 0);
insert into t (id, n) values (1, 1), (1, 2), (2, 3)
  on duplicate key update n = n + values(n), d = d + 1;
select id, s, d, n from t order by id;
insert into t (id, n) values (1, 4) on duplicate key update s = values(s), d = values(d);
select id, s, d, n from t order by id;

drop database odku_nullable_defaults;

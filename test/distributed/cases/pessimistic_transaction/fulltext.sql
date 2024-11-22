drop table if exists t0;
create table t0 (
  a int,
  b int,
  c varchar(20),
  primary key(a, b)
);
insert into t0 select result, result, result || 'test' from generate_series(1, 100) g;
set experimental_fulltext_index=1;
create fulltext index ftidx on t0 (c);
drop table if exists t1;
create table t1 (
  a int,
  b int,
  c varchar(20),
  primary key(a, b)
);

create fulltext index ftidx on t1 (c);

insert into t1 select * from t0 where a = 1 and b = 1;
insert into t1 select * from t0 where a = 1 and b = 1;
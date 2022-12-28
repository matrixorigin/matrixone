select json_quote('a');
select json_quote('a"');
select json_quote('a"b');
select json_quote(null);
create table t1 (a varchar(10));
insert into t1 values ('a'), ('a"'), ('a"b'), (null);
select json_quote(a) from t1;
select json_quote('xax') from t1;
select json_quote(null) from t1;
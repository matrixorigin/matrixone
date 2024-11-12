drop table if exists t;
create table t (a int, b int, j json);
insert into t values(1,3,'{"foo":1,"bar":2}'),(2,-2,'{"foo":11,"bar":22}');
select * from t;
select t.a,t.b,tf.* from t cross apply generate_series(t.a, t.b) tf;
select t.a,t.b,tf.* from t cross apply generate_series(t.a, t.b) tf where t.a>1;
select t.a,tmp.* from t cross apply unnest(t.j,'$') tmp;
drop table t;

drop table if exists jt;
create table jt (id int, tags json, metrics json);

insert into jt values (1, '{"tag1": "v1", "tag2": "v2", "tag3": "v3"}', '{"m1": 1, "m2": 2, "m3": 3}');
insert into jt values (2, '{"tag1": "v1", "tag2": "v22", "tag3": "v23"}', '{"m1": 12, "m2": 22, "m3": 32}');
insert into jt values (3, '{"tag13": "v13", "tag23": "v23", "tag33": "v33"}', '{"m1": 13, "m2": 23, "m3": 33}');
insert into jt values (4, '{"tag1": "v1", "tag2": "v2", "tag3": "v3"}', '{"m1": 1, "m2": 2, "m3": 3}');
insert into jt values (5, '{"tag1": "v1", "tag2": "v2", "tag3": "v3"}', '{"m1": 1, "m2": 2, "m3": 3}');
insert into jt values (6, '{"tag1": "v1", "tag2": "v2", "tag3": "v3"}', '{"m1": 1, "m2": 2, "m3": 3}');
insert into jt values (7, '{"tag1": "v1", "tag2": "v2", "tag3": "v3"}', '{"m1": 1, "m2": 2, "m3": 3}');
insert into jt values (8, '{"tag1": "v1", "tag2": "v2", "tag3": "v3"}', '{"m1": 1, "m2": 2, "m3": 3}');
insert into jt values (9, '{"tag1": "v1", "tag2": "v2", "tag3": "v3"}', '{"m1": 1, "m2": 2, "m3": 3}');
insert into jt values (10, '{"tag1": "v1", "tag2": "v2", "tag3", "v3"}', '{"m1": 1, "m2": 2, "m3": 3}');
insert into jt values (11, '{"tag1": "v1", "tag2": "v2", "tag3": "v3"}', '{"m1": 1, "m2": 2, "m3": 3}');
insert into jt values (12, '{"tag1": "v1", "tag2": "v22", "tag3": "v23"}', '{"m1": 12, "m2": 22, "m3": 32}');
insert into jt values (13, '{"tag13": "v13", "tag23": "v23", "tag33": "v33"}', '{"m1": 13, "m2": 23, "m3": 33}');
insert into jt values (14, '[1, 2, 3]', '{"m1": 1, "m2": 2, "m3": 3}');
insert into jt values (15, '["v1", "v2", "v3"]', '{"m1": 1, "m2": 2, "m3": 3}');
insert into jt values (16, 'null', '{"m1": 1, "m2": 2, "m3": 3}');
insert into jt values (17, '"string"', '{"m1": 1, "m2": 2, "m3": 3}');
insert into jt values (18, '1', '{"m1": 1, "m2": 2, "m3": 3}');
insert into jt values (20, null, '{"m1": 1, "m2": 2, "m3": 3}');

select id, u.`key`, u.`index`, u.value from jt cross apply unnest(tags, '$') u where id < 5;
select id, u.`key`, u.`index`, u.value from jt cross apply unnest(tags, '$') u;
select id, u.`key`, u.`index`, u.value from jt cross apply unnest(metrics, '$') u;
select id, u.`key`, u.`index`, u.value from jt cross apply unnest(tags, '$') u where id = 19;
select id, u.`key`, u.`index`, u.value from jt cross apply unnest(tags, '$') u where id = 20;

select id, u.`key`, u.`index`, u.value from jt cross apply unnest(metrics, '$') u where u.`key` = 'm2';
select id, u.`key`, u.`index`, u.value from jt cross apply unnest(metrics, '$') u where u.`key` = 'm2' and u.value > 2;
select id, u.`key`, u.`index`, u.value from jt cross apply unnest(metrics, '$') u where u.`key` = 'm2' and json_extract_float64(u.value, '$') > 2;

select count(*) from jt cross apply unnest(tags, '$') u where id < 10;
select count(*) from jt cross apply unnest(tags, '$') u where id = 2;
select count(*) from jt cross apply unnest(tags, '$') u;
select count(*) from jt cross apply unnest(tags, '$') u where id = 19;
select count(*) from jt cross apply unnest(tags, '$') u where id = 20;

drop table jt;

drop table if exists t1;
create table t1(a int, b int);
insert into t1 values(1,3),(1,-1);
select * from t1 cross apply generate_series(t1.a,t1.b,1)g;
select * from t1 outer apply generate_series(t1.a,t1.b,1)g;
select * from t1 cross apply generate_series(t1.a,t1.b,-1)g;
select * from t1 outer apply generate_series(t1.a,t1.b,-1)g;
drop table t1;

drop table if exists t2;
create table t2(id int,j json);
insert into t2 values(1,'{"tag1": "v1", "tag2": "v2", "tag3": "v3"}');
insert into t2 values(2,null);
select t2.id,tf.* from t2 cross apply unnest(t2.j,'$')tf;
select t2.id,tf.* from t2 outer apply unnest(t2.j,'$')tf;

drop table if exists t1;
create table t1(a int, b int, c int);
insert into t1 values(1, 1, NULL);
select * from t1 cross apply generate_series(t1.a,t1.b,t1.c)g;
drop table t1;
create table t1(a int, b int, c int);
insert into t1 values(1, NULL, 1);
select * from t1 cross apply generate_series(t1.a,t1.b,t1.c)g;
drop table t1;
create table t1(a int, b int, c int);
insert into t1 values(NULL, 1, 1);
select * from t1 cross apply generate_series(t1.a,t1.b,t1.c)g;
drop table t1;
create table t1(a bigint, b bigint, c bigint);
insert into t1 values(1, 1, NULL);
select * from t1 cross apply generate_series(t1.a,t1.b,t1.c)g;
drop table t1;
create table t1(a bigint, b bigint, c bigint);
insert into t1 values(1, NULL, 1);
select * from t1 cross apply generate_series(t1.a,t1.b,t1.c)g;
drop table t1;
create table t1(a bigint, b bigint, c bigint);
insert into t1 values(NULL, 1, 1);
select * from t1 cross apply generate_series(t1.a,t1.b,t1.c)g;
drop table t1;
create table t1(a datetime,b datetime,c varchar);
insert into t1 values('2020-02-28 00:00:00','2021-03-01 00:01:00', NULL);
select * from t1 cross apply generate_series(t1.a,t1.b,t1.c)g;

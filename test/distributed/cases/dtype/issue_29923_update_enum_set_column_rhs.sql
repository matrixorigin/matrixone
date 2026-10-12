-- Column-valued ENUM/SET UPDATE and JOIN UPDATE must preserve stored ENUM
-- error member (ordinal 0 from INSERT IGNORE) on inactive IF branches.

drop table if exists t29923_src;
drop table if exists t29923_dst;
create table t29923_dst(id int primary key, e enum('red','blue','green'), s set('a','b','c'), note varchar(10));
create table t29923_src(id int primary key, e enum('red','blue','green'), s set('a','b','c'));

insert into t29923_dst values (1, 'red', 'a', 'keep');
insert into t29923_dst values (2, 'blue', 'b', 'upd');
insert ignore into t29923_dst values (3, 'nope', 'x', 'err');
insert into t29923_src values (1, 'green', 'c'), (2, 'red', 'a'), (3, 'blue', 'b');

-- single-table column-valued assignment via correlated subquery
update t29923_dst set e = (select e from t29923_src where t29923_src.id = t29923_dst.id),
                      s = (select s from t29923_src where t29923_src.id = t29923_dst.id)
 where id = 1;
select id, e, cast(e as unsigned), s, note from t29923_dst order by id;

-- JOIN UPDATE of a normal row
update t29923_dst d join t29923_src s on d.id = s.id
   set d.e = s.e, d.s = s.s
 where d.id = 2;
select id, e, cast(e as unsigned), s, note from t29923_dst order by id;

-- JOIN UPDATE that assigns a valid value onto the stored error-member row
update t29923_dst d join t29923_src s on d.id = s.id
   set d.e = s.e, d.s = s.s
 where d.id = 3;
select id, e, cast(e as unsigned), s, note from t29923_dst order by id;

-- Re-seed error member, then verify filtered-out rows remain unchanged.
delete from t29923_dst where id = 3;
insert ignore into t29923_dst values (3, 'nope', 'x', 'err');
update t29923_dst d join t29923_src s on d.id = s.id
   set d.e = s.e
 where d.note = 'upd';
select id, e, cast(e as unsigned), note from t29923_dst order by id;

-- Duplicate target matches reach the inactive selector; WHERE filtering does not.
drop table if exists t29923_dup;
create table t29923_dup(id int primary key, owner int, note int);
insert into t29923_dup values(1,3,0),(2,3,0);
update t29923_dst d join t29923_dup s on d.id=s.owner
   set d.e=d.e, s.note=s.note+1;
select id,e,cast(e as unsigned),note from t29923_dst order by id;
select id,note from t29923_dup order by id;
drop table t29923_dup;

drop table t29923_src;
drop table t29923_dst;

-- @suite
-- @case
-- @desc: regression test for #23105 — IN subquery must not trigger REGEXP parsing errors

drop table if exists company;
create table company (id int primary key, name varchar(100));
insert into company values (1, 'Alpha'), (2, 'Beta'), (3, 'Gamma');

-- issue repro: IN subquery with empty result
select * from company where id in (select id from company where 1=0);

-- IN subquery with non-empty result
select * from company where id in (select id from company where id < 3) order by id;

-- data containing regex metacharacters
drop table if exists company2;
create table company2 (id varchar(50) primary key, name varchar(100));
insert into company2 values ('abc[1]', 'Alpha'), ('def[2]', 'Beta'), ('g+h', 'Gamma');

select * from company2 where id in (select id from company2 where 1=0);
select * from company2 where id in (select id from company2 where name = 'Alpha');
select * from company2 where id in ('abc[1]', 'g+h') order by id;

-- LIKE with bracket (fixed by #23630)
select * from company2 where id like 'abc[%';

drop table if exists company;
drop table if exists company2;

drop database if exists prefix_index_non_equality;
create database prefix_index_non_equality;
use prefix_index_non_equality;

-- Prefix IN and range probes used to under-fetch persisted hidden-index blocks.
-- FORCE/IGNORE pairs are correctness oracles; FORCE may safely fall back to the
-- base table when a lossy prefix index is not eligible for the predicate.
create table n (
    id int primary key,
    a varchar(64),
    key idx_a(a(4))
);
insert into n values
    (1, 'abbz'),
    (2, 'abcd'),
    (3, 'abcdx'),
    (4, 'abcdy'),
    (5, 'abce'),
    (6, 'abcex'),
    (7, 'abcf'),
    (8, '中中中中甲'),
    (9, '中中中中乙');

select id from n where a in ('abcdx', 'abcex') order by id;
select id from n force index(idx_a) where a in ('abcdx', 'abcex') order by id;
select id from n ignore index(idx_a) where a in ('abcdx', 'abcex') order by id;
select id from n where a between 'abcdx' and 'abcex' order by id;
select id from n force index(idx_a) where a between 'abcdx' and 'abcex' order by id;
select id from n ignore index(idx_a) where a between 'abcdx' and 'abcex' order by id;
select id from n where a >= 'abcdx' and a < 'abcf' order by id;
select id from n force index(idx_a) where a >= 'abcdx' and a < 'abcf' order by id;
select id from n ignore index(idx_a) where a >= 'abcdx' and a < 'abcf' order by id;
prepare prefix_in_stmt from 'select id from n force index(idx_a) where a in (?, ?) order by id';
set @left_value = 'abcdx', @right_value = 'abcex';
execute prefix_in_stmt using @left_value, @right_value;

select mo_ctl('dn', 'flush', 'prefix_index_non_equality.n');
select id from n force index(idx_a) where a in ('abcdx') order by id;
select id from n ignore index(idx_a) where a in ('abcdx') order by id;
select id from n force index(idx_a) where a in ('abcdx', 'abcex') order by id;
select id from n ignore index(idx_a) where a in ('abcdx', 'abcex') order by id;
select id from n where a in ('abcdx', 'abcex') order by id;
select id from n force index(idx_a) where a between 'abcdx' and 'abcex' order by id;
select id from n ignore index(idx_a) where a between 'abcdx' and 'abcex' order by id;
select id from n where a between 'abcdx' and 'abcex' order by id;
select id from n force index(idx_a) where a >= 'abcdx' and a < 'abcf' order by id;
select id from n ignore index(idx_a) where a >= 'abcdx' and a < 'abcf' order by id;
select id from n where a >= 'abcdx' and a < 'abcf' order by id;
execute prefix_in_stmt using @left_value, @right_value;
deallocate prepare prefix_in_stmt;
select id from n force index(idx_a) where a in ('中中中中甲') order by id;
select id from n ignore index(idx_a) where a in ('中中中中甲') order by id;
select id from n where a in ('中中中中甲') order by id;

create table rhs (a varchar(64));
insert into rhs values ('abcdx'), ('abcex');
select n.id from n force index for join(idx_a) join rhs on n.a = rhs.a order by n.id;
select n.id from n ignore index for join(idx_a) join rhs on n.a = rhs.a order by n.id;

create table ordered_prefix (
    id int primary key,
    a varchar(64),
    key idx_a(a(4))
);
insert into ordered_prefix values (1, 'abcdz'), (2, 'abcda'), (3, 'abcdy');
select id, a from ordered_prefix force index for order by(idx_a) order by a limit 2;
select id, a from ordered_prefix ignore index for order by(idx_a) order by a limit 2;

create table u (
    id int primary key,
    a varchar(64),
    unique key uq_a(a(4))
);
insert into u values (1, 'abbz'), (2, 'abcdx'), (3, 'abcex'), (4, 'abcf');
select mo_ctl('dn', 'flush', 'prefix_index_non_equality.u');
select id from u where a in ('abcdx', 'abcex') order by id;
select id from u force index(uq_a) where a in ('abcdx', 'abcex') order by id;
select id from u ignore index(uq_a) where a in ('abcdx', 'abcex') order by id;
select id from u where a between 'abcdw' and 'abcex' order by id;
select id from u force index(uq_a) where a between 'abcdw' and 'abcex' order by id;
select id from u ignore index(uq_a) where a between 'abcdw' and 'abcex' order by id;

drop database prefix_index_non_equality;

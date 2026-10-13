-- Regression for #26841: range bounds derived from a lossy prefix index must
-- never exclude rows before the original predicate reaches the base table.

drop database if exists prefix_index_range;
create database prefix_index_range;
use prefix_index_range;

create table prefix_range_nonunique (
    id int primary key,
    s varchar(32),
    key idx_s(s(3))
);
insert into prefix_range_nonunique values
    (1, 'abbZ'),
    (2, 'abc'),
    (3, 'abcX'),
    (4, 'abcY'),
    (5, 'abd'),
    (6, 'abdA'),
    (7, 'abe'),
    (8, null);

-- Scoped ORDER/GROUP hints run before ordinary filter index selection. A
-- prefix key cannot reconstruct full values or order same-prefix neighbors.
-- @regex("Index Table Scan",false)
explain select id, s from prefix_range_nonunique force index for order by(idx_s)
where s between 'abcX' and 'abdA' order by s, id;
select id, s from prefix_range_nonunique force index for order by(idx_s)
where s between 'abcX' and 'abdA' order by s, id;
select s, count(*) from prefix_range_nonunique force index for group by(idx_s)
where s between 'abcX' and 'abdA' group by s order by s;

-- FORCE INDEX previously returned only ids 5 and 6. A prefix index is not a
-- safe range candidate, so the forced query must fall back to a table scan.
-- @regex("Index Table Scan",false)
explain select id from prefix_range_nonunique force index(idx_s)
where s between 'abcX' and 'abdA';
select id from prefix_range_nonunique force index(idx_s)
where s between 'abcX' and 'abdA' order by id;
select id from prefix_range_nonunique ignore index(idx_s)
where s between 'abcX' and 'abdA' order by id;

-- Single and paired range predicates exercise separate planner paths.
select id from prefix_range_nonunique force index(idx_s)
where s >= 'abcX' order by id;
select id from prefix_range_nonunique ignore index(idx_s)
where s >= 'abcX' order by id;
select id from prefix_range_nonunique force index(idx_s)
where s > 'abcX' and s < 'abdA' order by id;
select id from prefix_range_nonunique ignore index(idx_s)
where s > 'abcX' and s < 'abdA' order by id;

-- OR must be rejected if any arm contains a range predicate.
select id from prefix_range_nonunique force index(idx_s)
where s between 'abcX' and 'abdA' or s in ('abe') order by id;
select id from prefix_range_nonunique ignore index(idx_s)
where s between 'abcX' and 'abdA' or s in ('abe') order by id;

-- IN follows the same conservative fallback as other lossy non-equality paths.
-- @regex("Index Table Scan",false)
explain select id from prefix_range_nonunique force index(idx_s)
where s in ('abcX', 'abdA');
select id from prefix_range_nonunique force index(idx_s)
where s in ('abcX', 'abdA') order by id;
select id from prefix_range_nonunique ignore index(idx_s)
where s in ('abcX', 'abdA') order by id;

-- Runtime bounds follow the same conservative range path.
prepare prefix_between_stmt from
'select id from prefix_range_nonunique force index(idx_s) where s between ? and ? order by id';
set @prefix_lower = 'abcX';
set @prefix_upper = 'abdA';
execute prefix_between_stmt using @prefix_lower, @prefix_upper;
deallocate prepare prefix_between_stmt;

-- Unique prefix indexes store a different hidden-key shape but have the same
-- lossy ordering property for full column values.
create table prefix_range_unique (
    id int primary key,
    s varchar(32),
    unique key uq_s(s(3))
);
insert into prefix_range_unique values
    (1, 'abbZ'),
    (2, 'abcY'),
    (3, 'abdA'),
    (4, 'abe');
-- @regex("Index Table Scan",false)
explain select id from prefix_range_unique force index(uq_s)
where s between 'abcX' and 'abdA';
select id from prefix_range_unique force index(uq_s)
where s between 'abcX' and 'abdA' order by id;
select id from prefix_range_unique ignore index(uq_s)
where s between 'abcX' and 'abdA' order by id;

-- Recheck after persisted blocks replace the in-memory data path.
-- @separator:table
select mo_ctl('dn', 'flush', 'prefix_index_range.prefix_range_nonunique');
-- @separator:table
select mo_ctl('dn', 'flush', 'prefix_index_range.prefix_range_unique');
select id from prefix_range_nonunique force index(idx_s)
where s between 'abcX' and 'abdA' order by id;
select id from prefix_range_unique force index(uq_s)
where s between 'abcX' and 'abdA' order by id;

-- Complete secondary indexes keep their range optimization.
create index idx_s_complete on prefix_range_nonunique(s);
-- @regex("Index Table Scan",true)
explain select id from prefix_range_nonunique force index(idx_s_complete)
where s between 'abcX' and 'abdA';
select id from prefix_range_nonunique force index(idx_s_complete)
where s between 'abcX' and 'abdA' order by id;

-- RENAME must update both logical parts and persisted prefix metadata. Verify
-- range fallback before/after flush and all hidden-key DML maintenance forms.
create table prefix_range_rename (
    id int primary key,
    s varchar(32),
    key idx_s(s(3))
);
insert into prefix_range_rename values
    (1, 'abcX'),
    (2, 'abcY'),
    (3, 'abd'),
    (4, 'abdA');
alter table prefix_range_rename rename column s to renamed;
-- @regex("Index Table Scan",false)
explain select id from prefix_range_rename force index(idx_s)
where renamed between 'abcX' and 'abdA';
select id from prefix_range_rename force index(idx_s)
where renamed between 'abcX' and 'abdA' order by id;
insert into prefix_range_rename values (9, 'abcZ');
select id from prefix_range_rename force index(idx_s) where renamed = 'abcZ';
update prefix_range_rename set renamed = 'abdB' where id = 9;
select id from prefix_range_rename force index(idx_s) where renamed = 'abdB';
delete from prefix_range_rename where id = 9;
select count(*) from prefix_range_rename where id = 9;
-- @separator:table
select mo_ctl('dn', 'flush', 'prefix_index_range.prefix_range_rename');
select id from prefix_range_rename force index(idx_s)
where renamed between 'abcX' and 'abdA' order by id;

drop database prefix_index_range;

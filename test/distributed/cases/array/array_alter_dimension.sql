-- #28917: ALTER TABLE ... MODIFY/CHANGE COLUMN copies rows without re-encoding vectors, so
-- changing a vector column's declared dimension would leave existing rows at the old dimension --
-- a mixed-dimension column that breaks distance queries and HNSW index construction. The DDL must
-- be REJECTED. A dimension-mismatched vector CAST is likewise rejected.
drop database if exists array_alter_dim;
create database array_alter_dim;
use array_alter_dim;

create table t(id bigint primary key, v vecf32(3) not null);
insert into t values (1,'[1,2,3]'),(2,'[4,5,6]');

-- MODIFY / CHANGE to a different dimension is rejected (both grow and shrink).
alter table t modify column v vecf32(4) not null;
alter table t change column v v vecf32(2) not null;

-- vecf64 dimension change is rejected too.
create table t2(id bigint primary key, w vecf64(3));
insert into t2 values (1,'[1,2,3]');
alter table t2 modify column w vecf64(5);

-- Same dimension is allowed (this is a nullability change, not a dimension change).
alter table t modify column v vecf32(3) null;

-- A dimension change that ALSO changes the element type is rejected at DDL too (not left to the
-- per-row cast to fail mid-copy) -- v is vecf32(3) here.
alter table t modify column v vecf64(4);

-- The rows are untouched and still 3-dimensional and queryable.
select id, vector_dims(v), v from t order by id;
select id, l2_distance(v,'[0,0,0]') from t order by id;

-- A dimension-mismatched vector CAST fails; a same-dimension element-type cast is fine.
select cast(cast('[1,2,3]' as vecf32(3)) as vecf32(4));
select cast(cast('[1,2,3]' as vecf32(3)) as vecf64(3));

-- Every width-bearing vector type is covered, not just VECF32/VECF64.
create table t3(id bigint primary key, a vecf16(3), b vecbf16(3), c vecint8(3), d vecuint8(3));
alter table t3 modify column a vecf16(4);
alter table t3 modify column b vecbf16(5);
alter table t3 modify column c vecint8(6);
alter table t3 modify column d vecuint8(7);

-- A STORED generated column + CHECK depending on the vector: the dimension ALTER must be rejected
-- so the table never reaches the broken mixed-dimension state where a valid 4-d write is refused by
-- the old CHECK and a 3-d write by the new column type. Rejecting keeps the table fully usable.
create table tg(id bigint primary key, v vecf32(3), dims int as (vector_dims(v)) stored, check (dims = 3));
insert into tg(id, v) values (1, '[1,2,3]');
alter table tg modify column v vecf32(4);
insert into tg(id, v) values (2, '[4,5,6]');
select id, v, dims from tg order by id;

drop database array_alter_dim;

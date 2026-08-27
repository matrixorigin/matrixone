-- ivfflat: synchronous DML (INSERT/UPDATE/DELETE) against a QUANTIZATION index.
--
-- The entries table is declared with the quantization element type while the base
-- column stays wide, so a DML-written entry must be narrowed and scaled by the same
-- quantizer the build path uses. It was previously projected verbatim, storing the
-- raw base bytes in the narrow column: a vecf32(4) read back as a 16-element vecint8,
-- failing the next search with "vector dimension not matched" (#27732).
--
-- Every DML row below sits INSIDE the range trained at CREATE INDEX time, so the
-- expected neighbour is fixed by the data and not by how the quantizer clips an
-- out-of-range value. float32 and no-quantization are controls: their entry width
-- already equals the base width, so they passed before this fix too.
drop database if exists ivfqdml;
create database ivfqdml;
use ivfqdml;

-- ---------- int8 ----------
create table q_i8(a int primary key, v vecf32(4));
insert into q_i8 values (1,'[1,1,1,1]'),(2,'[9,9,9,9]'),(4,'[3,3,3,3]'),(5,'[7,7,7,7]');
create index i_i8 using ivfflat on q_i8(v) lists=1 op_type 'vector_l2_ops' quantization 'int8';
insert into q_i8 values (3,'[5,5,5,5]'),(6,'[2,2,2,2]');
update q_i8 set v='[8,8,8,8]' where a=6;
delete from q_i8 where a=4;
select a from q_i8 order by l2_distance(v,'[5,5,5,5]') limit 1;
select a from q_i8 order by l2_distance(v,'[8,8,8,8]') limit 1;
select count(*) from q_i8 where a=4;

-- ---------- uint8 ----------
create table q_u8(a int primary key, v vecf32(4));
insert into q_u8 values (1,'[1,1,1,1]'),(2,'[9,9,9,9]'),(4,'[3,3,3,3]'),(5,'[7,7,7,7]');
create index i_u8 using ivfflat on q_u8(v) lists=1 op_type 'vector_l2_ops' quantization 'uint8';
insert into q_u8 values (3,'[5,5,5,5]'),(6,'[2,2,2,2]');
update q_u8 set v='[8,8,8,8]' where a=6;
delete from q_u8 where a=4;
select a from q_u8 order by l2_distance(v,'[5,5,5,5]') limit 1;
select a from q_u8 order by l2_distance(v,'[8,8,8,8]') limit 1;
select count(*) from q_u8 where a=4;

-- ---------- float16 ----------
create table q_f16(a int primary key, v vecf32(4));
insert into q_f16 values (1,'[1,1,1,1]'),(2,'[9,9,9,9]'),(4,'[3,3,3,3]'),(5,'[7,7,7,7]');
create index i_f16 using ivfflat on q_f16(v) lists=1 op_type 'vector_l2_ops' quantization 'float16';
insert into q_f16 values (3,'[5,5,5,5]'),(6,'[2,2,2,2]');
update q_f16 set v='[8,8,8,8]' where a=6;
delete from q_f16 where a=4;
select a from q_f16 order by l2_distance(v,'[5,5,5,5]') limit 1;
select a from q_f16 order by l2_distance(v,'[8,8,8,8]') limit 1;
select count(*) from q_f16 where a=4;

-- ---------- bf16 ----------
create table q_bf16(a int primary key, v vecf32(4));
insert into q_bf16 values (1,'[1,1,1,1]'),(2,'[9,9,9,9]'),(4,'[3,3,3,3]'),(5,'[7,7,7,7]');
create index i_bf16 using ivfflat on q_bf16(v) lists=1 op_type 'vector_l2_ops' quantization 'bf16';
insert into q_bf16 values (3,'[5,5,5,5]'),(6,'[2,2,2,2]');
update q_bf16 set v='[8,8,8,8]' where a=6;
delete from q_bf16 where a=4;
select a from q_bf16 order by l2_distance(v,'[5,5,5,5]') limit 1;
select a from q_bf16 order by l2_distance(v,'[8,8,8,8]') limit 1;
select count(*) from q_bf16 where a=4;

-- ---------- vecf64 base, int8 entries (the base is narrowed to f32 first) ----------
create table q64_i8(a int primary key, v vecf64(4));
insert into q64_i8 values (1,'[1,1,1,1]'),(2,'[9,9,9,9]'),(4,'[3,3,3,3]'),(5,'[7,7,7,7]');
create index i64_i8 using ivfflat on q64_i8(v) lists=1 op_type 'vector_l2_ops' quantization 'int8';
insert into q64_i8 values (3,'[5,5,5,5]'),(6,'[2,2,2,2]');
update q64_i8 set v='[8,8,8,8]' where a=6;
delete from q64_i8 where a=4;
select a from q64_i8 order by l2_distance(v,'[5,5,5,5]') limit 1;
select a from q64_i8 order by l2_distance(v,'[8,8,8,8]') limit 1;
select count(*) from q64_i8 where a=4;

-- ---------- controls: entry width already equals base width ----------
create table q_f32(a int primary key, v vecf32(4));
insert into q_f32 values (1,'[1,1,1,1]'),(2,'[9,9,9,9]'),(4,'[3,3,3,3]'),(5,'[7,7,7,7]');
create index i_f32 using ivfflat on q_f32(v) lists=1 op_type 'vector_l2_ops' quantization 'float32';
insert into q_f32 values (3,'[5,5,5,5]'),(6,'[2,2,2,2]');
update q_f32 set v='[8,8,8,8]' where a=6;
delete from q_f32 where a=4;
select a from q_f32 order by l2_distance(v,'[5,5,5,5]') limit 1;
select a from q_f32 order by l2_distance(v,'[8,8,8,8]') limit 1;
select count(*) from q_f32 where a=4;

create table q_none(a int primary key, v vecf32(4));
insert into q_none values (1,'[1,1,1,1]'),(2,'[9,9,9,9]'),(4,'[3,3,3,3]'),(5,'[7,7,7,7]');
create index i_none using ivfflat on q_none(v) lists=1 op_type 'vector_l2_ops';
insert into q_none values (3,'[5,5,5,5]'),(6,'[2,2,2,2]');
update q_none set v='[8,8,8,8]' where a=6;
delete from q_none where a=4;
select a from q_none order by l2_distance(v,'[5,5,5,5]') limit 1;
select a from q_none order by l2_distance(v,'[8,8,8,8]') limit 1;
select count(*) from q_none where a=4;

-- ---------- narrow BASE columns ----------
-- A narrow base still has to be scaled: quantization is not just a width change.
create table q_f16_i8(a int primary key, v vecf16(4));
insert into q_f16_i8 values (1,'[1,1,1,1]'),(2,'[9,9,9,9]'),(4,'[3,3,3,3]'),(5,'[7,7,7,7]');
create index i_f16_i8 using ivfflat on q_f16_i8(v) lists=1 op_type 'vector_l2_ops' quantization 'int8';
insert into q_f16_i8 values (3,'[5,5,5,5]'),(6,'[2,2,2,2]');
update q_f16_i8 set v='[8,8,8,8]' where a=6;
delete from q_f16_i8 where a=4;
select a from q_f16_i8 order by l2_distance(v,'[5,5,5,5]') limit 1;
select a from q_f16_i8 order by l2_distance(v,'[8,8,8,8]') limit 1;
select count(*) from q_f16_i8 where a=4;

create table q_bf16_i8(a int primary key, v vecbf16(4));
insert into q_bf16_i8 values (1,'[1,1,1,1]'),(2,'[9,9,9,9]'),(4,'[3,3,3,3]'),(5,'[7,7,7,7]');
create index i_bf16_i8 using ivfflat on q_bf16_i8(v) lists=1 op_type 'vector_l2_ops' quantization 'int8';
insert into q_bf16_i8 values (3,'[5,5,5,5]'),(6,'[2,2,2,2]');
update q_bf16_i8 set v='[8,8,8,8]' where a=6;
delete from q_bf16_i8 where a=4;
select a from q_bf16_i8 order by l2_distance(v,'[5,5,5,5]') limit 1;
select a from q_bf16_i8 order by l2_distance(v,'[8,8,8,8]') limit 1;
select count(*) from q_bf16_i8 where a=4;

-- ---------- entry type EQUAL to the base type, but still affine ----------
-- int8/uint8 quantization rescales by the trained q(x)=x*mul+add even when the base
-- is already that type, so "same type" is NOT a no-op here. Skipping the transform
-- mixes raw and scaled entries in one index and silently corrupts the ranking.
create table q_i8_i8(a int primary key, v vecint8(4));
insert into q_i8_i8 values (1,'[1,1,1,1]'),(2,'[9,9,9,9]'),(4,'[3,3,3,3]'),(5,'[7,7,7,7]');
create index i_i8_i8 using ivfflat on q_i8_i8(v) lists=1 op_type 'vector_l2_ops' quantization 'int8';
insert into q_i8_i8 values (3,'[5,5,5,5]'),(6,'[2,2,2,2]');
update q_i8_i8 set v='[8,8,8,8]' where a=6;
delete from q_i8_i8 where a=4;
select a from q_i8_i8 order by l2_distance(v,'[5,5,5,5]') limit 1;
select a from q_i8_i8 order by l2_distance(v,'[8,8,8,8]') limit 1;
select count(*) from q_i8_i8 where a=4;

create table q_u8_u8(a int primary key, v vecuint8(4));
insert into q_u8_u8 values (1,'[1,1,1,1]'),(2,'[9,9,9,9]'),(4,'[3,3,3,3]'),(5,'[7,7,7,7]');
create index i_u8_u8 using ivfflat on q_u8_u8(v) lists=1 op_type 'vector_l2_ops' quantization 'uint8';
insert into q_u8_u8 values (3,'[5,5,5,5]'),(6,'[2,2,2,2]');
update q_u8_u8 set v='[8,8,8,8]' where a=6;
delete from q_u8_u8 where a=4;
select a from q_u8_u8 order by l2_distance(v,'[5,5,5,5]') limit 1;
select a from q_u8_u8 order by l2_distance(v,'[8,8,8,8]') limit 1;
select count(*) from q_u8_u8 where a=4;

-- ---------- the exact shapes reported in #27732 ----------
create table t4(id int primary key, embedding vecf32(4));
insert into t4 values (1,'[1,1,1,1]'),(2,'[9,9,9,9]');
create index t4idx using ivfflat on t4(embedding) lists=1 op_type 'vector_l2_ops' quantization 'int8';
select id from t4 order by l2_distance(embedding,'[0,0,0,0]') limit 1;
insert into t4 values (3,'[0,0,0,0]');
-- must not raise "vector dimension not matched"; 0 clips into the same int8 bucket
-- as 1 under the bounds trained on [1,9], so the tie resolves to the lower pk.
select id from t4 order by l2_distance(embedding,'[0,0,0,0]') limit 1;
-- retraining the bounds over the new data separates them again
alter table t4 alter reindex t4idx ivfflat lists=1;
select id from t4 order by l2_distance(embedding,'[0,0,0,0]') limit 1;

create table t2(id int primary key, embedding vecf32(2));
insert into t2 values (1,'[1,1]'),(2,'[9,9]');
create index t2idx using ivfflat on t2(embedding) lists=1 op_type 'vector_l2_ops' quantization 'float16';
select id from t2 order by l2_distance(embedding,'[0,0]') limit 1;
insert into t2 values (3,'[0,0]');
select id from t2 order by l2_distance(embedding,'[0,0]') limit 1;

drop database ivfqdml;

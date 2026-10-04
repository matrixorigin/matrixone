-- 向量表达式的宽度、共同类型与赋值边界。
drop database if exists vector_result_dimensions;
create database vector_result_dimensions;
use vector_result_dimensions;

create table src(id int primary key, f32 vecf32(2), f64 vecf64(2), f16 vecf16(2), bf16 vecbf16(2), i8 vecint8(2), u8 vecuint8(2));
insert into src values (1,'[1,4]','[1,4]','[1,4]','[1,4]','[1,4]','[1,4]'),(2,null,null,null,null,null,null);

-- 六类型保留固定维度，包括无类型 NULL、typed NULL 和字符串分支。
create table same_dim as select id,coalesce(null,f32,'[0,0]') f32,coalesce(f64,cast(null as vecf64(2)),'[0,0]') f64,coalesce(f16,'[0,0]') f16,coalesce(bf16,'[0,0]') bf16,coalesce(i8,'[0,0]') i8,coalesce(u8,'[0,0]') u8 from src;
show create table same_dim;
insert into same_dim values(3,'[3,4]','[3,4]','[3,4]','[3,4]','[3,4]','[3,4]');
select id,vector_dims(f32),vector_dims(f64),vector_dims(f16),vector_dims(bf16),vector_dims(i8),vector_dims(u8) from same_dim order by id;
select id,least(f32,cast('[2,3]' as vecf32(2))),greatest(f32,cast('[2,3]' as vecf32(2))) from src order by id;

-- 固定宽度冲突必须在规划期拒绝，且不依赖参数顺序。
select coalesce(f32,cast('[1,2,3]' as vecf32(3))) from src;
select coalesce(cast('[1,2,3]' as vecf64(3)),f64) from src;
select least(f16,cast('[1,2,3]' as vecf16(3))) from src;
select greatest(cast('[1,2,3]' as vecbf16(3)),bf16) from src;
select coalesce(i8,cast('[1,2,3]' as vecint8(3))) from src;
select greatest(u8,cast('[1,2,3]' as vecuint8(3))) from src;

-- 长度保持函数传播输入宽度，类型转换仍然为 F64。
create table sqrt_dim as select sqrt(f32) a,sqrt(f64) b from src where id=1;
show create table sqrt_dim;
select a,b,vector_dims(a),vector_dims(b) from sqrt_dim;
insert into sqrt_dim values('[3,4]','[5,6]');
select count(*) from sqrt_dim;

-- 两种参数顺序都提升到 F64，保留有限大数和双精度尾数。
create table promoted as select coalesce(cast(null as vecf32(2)),cast('[1e300,1.0000000000000002]' as vecf64(2))) a,coalesce(cast('[1e300,1.0000000000000002]' as vecf64(2)),cast(null as vecf32(2))) b;
show create table promoted;
select a,b,a=b from promoted;

create view dim_view as select coalesce(f32,'[0,0]') v from src;
select v,vector_dims(v) from dim_view order by v;
prepare dim_stmt from 'select coalesce(cast(? as vecf32(2)),cast(? as vecf64(2))) v';
set @a=null;
set @b='[1e300,1.0000000000000002]';
execute dim_stmt using @a,@b;
set @a='[1,2]';
execute dim_stmt using @a,@b;
deallocate prepare dim_stmt;

-- 动态宽度标记在 CTAS、普通插入和参数赋值中保持一致。
create table dynamic_dim as select subvector(f32,1,1) f32,vecf64_from_base64(to_base64(f64)) f64,cast(f16 as vecf16) f16,cast(bf16 as vecbf16) bf16,cast(i8 as vecint8) i8,cast(u8 as vecuint8) u8 from src where id=1;
show create table dynamic_dim;
insert into dynamic_dim values('[3]','[3,4]','[3,4]','[3,4]','[3,4]','[3,4]');
prepare dynamic_stmt from 'insert into dynamic_dim(f32,f64) values(?,?)';
set @a='[5]';
set @b='[5,6]';
execute dynamic_stmt using @a,@b;
deallocate prepare dynamic_stmt;
create table blob_payload(f32 blob,f64 blob);
insert into blob_payload values(unhex('0000e040'),unhex('0000000000001c400000000000002040'));
insert into dynamic_dim(f32,f64) select f32,f64 from blob_payload;
update blob_payload set f32=unhex('ff');
insert into dynamic_dim(f32) select f32 from blob_payload;
select f32,f64,vector_dims(f32),vector_dims(f64),vector_dims(f16),vector_dims(bf16),vector_dims(i8),vector_dims(u8) from dynamic_dim order by f32;

-- 显式目标的实际长度检查，错误 INSERT/UPDATE 不留下错误行。
create table fixed_target(id int primary key,v vecf32(2));
insert into fixed_target select 1,greatest(cast('[1,2]' as vecf32(2)),cast('[3,4,5]' as vecf32(3)));
insert into fixed_target select 2,subvector(f32,1,1) from src where id=1;
select count(*) from fixed_target;
insert into fixed_target select id,coalesce(f32,'[0,0]') from src;
update fixed_target set v=subvector(v,1,1) where id=1;
select id,v,vector_dims(v) from fixed_target order by id;

-- STORED 生成列在生成值后检查固定维度，成功和失败路径共享同一 schema。
create table generated_dim(id int primary key,a vecf32(2),n int,g vecf32(2) as (subvector(a,1,n)) stored);
insert into generated_dim(id,a,n) values(1,'[1,2]',2);
insert into generated_dim(id,a,n) values(2,'[3,4]',2),(3,'[5,6]',1);
update generated_dim set n=1 where id=1;
replace into generated_dim(id,a,n) values(1,'[3,4]',1);
insert into generated_dim(id,a,n) values(1,'[5,6]',2) on duplicate key update n=1;
select id,g,vector_dims(g),l2_distance(g,cast('[1,2]' as vecf32(2))) from generated_dim order by id;
create index dim_ivf using ivfflat on generated_dim(g) lists=1 op_type 'vector_l2_ops';
select id,g from generated_dim order by l2_distance(g,cast('[1,2]' as vecf32(2))) limit 1;
drop index dim_ivf on generated_dim;

drop database vector_result_dimensions;

-- @suite
-- @case

drop database if exists mixed_unsigned_arithmetic_28581;
create database mixed_unsigned_arithmetic_28581;
use mixed_unsigned_arithmetic_28581;

set @mixed_unsigned_saved_sql_mode = @@sql_mode;
set session sql_mode = '';

create table mixed_values (id int primary key, u bigint unsigned, s bigint);
insert into mixed_values values
    (1, 18446744073709551615, 0),
    (2, 18446744073709551615, 1),
    (3, 0, 1);

select u + s from mixed_values where id = 1;
select u + s from mixed_values where id = 2;
select u - s from mixed_values where id = 3;
select u * s from mixed_values where id = 2;
select u * 2 from mixed_values where id = 2;

select (cast(0 as unsigned) + cast(0 as signed)) - 1;
create table typed_result as select u + s as q from mixed_values where id = 1;
select column_type from information_schema.columns
where table_schema = 'mixed_unsigned_arithmetic_28581' and table_name = 'typed_result' and column_name = 'q';
create table rejected_result as select u + s as q from mixed_values where id = 2;
select count(*) from information_schema.tables
where table_schema = 'mixed_unsigned_arithmetic_28581' and table_name = 'rejected_result';

prepare mixed_subtract_stmt from 'select u - s from mixed_values where id = ?';
set @mixed_subtract_id = 3;
execute mixed_subtract_stmt using @mixed_subtract_id;
set session sql_mode = 'NO_UNSIGNED_SUBTRACTION';
select u - s from mixed_values where id = 3;
execute mixed_subtract_stmt using @mixed_subtract_id;
select u - s from mixed_values where id = 1;
select cast(0 as unsigned) - cast(1 as unsigned);
set session sql_mode = '';
execute mixed_subtract_stmt using @mixed_subtract_id;
deallocate prepare mixed_subtract_stmt;

prepare mixed_operand_stmt from 'select cast(0 as unsigned) - ? as q';
set @mixed_operand = 1;
execute mixed_operand_stmt using @mixed_operand;
set session sql_mode = 'NO_UNSIGNED_SUBTRACTION';
execute mixed_operand_stmt using @mixed_operand;
set @mixed_operand = cast(18446744073709551615 as unsigned);
execute mixed_operand_stmt using @mixed_operand;
deallocate prepare mixed_operand_stmt;
set session sql_mode = '';

prepare mixed_unsigned_stmt from 'select u + s from mixed_values where id = ?';
set @mixed_unsigned_id = 1;
execute mixed_unsigned_stmt using @mixed_unsigned_id;
deallocate prepare mixed_unsigned_stmt;

prepare explicit_unsigned_stmt from 'select cast(? as unsigned) + ? as q';
set @explicit_unsigned_a = 0, @explicit_unsigned_b = -1;
execute explicit_unsigned_stmt using @explicit_unsigned_a, @explicit_unsigned_b;
set @explicit_unsigned_a = 2, @explicit_unsigned_b = 1;
execute explicit_unsigned_stmt using @explicit_unsigned_a, @explicit_unsigned_b;
deallocate prepare explicit_unsigned_stmt;

create table narrow_values(id int primary key,b tinyint,s smallint,i int,u tinyint unsigned);
insert into narrow_values values(1,100,30000,2147483647,200),(2,-128,NULL,NULL,NULL);
select id,b+b as bp,b*b as bs,s*s as ss,i*i as ii,u*u as uu from narrow_values order by id;
select sum(b*b) as total from narrow_values;
create table narrow_result as select b*b as bb,i*i as ii from narrow_values;
select column_name,column_type from information_schema.columns where table_schema='mixed_unsigned_arithmetic_28581' and table_name='narrow_result' order by ordinal_position;
select bb,ii from narrow_result order by bb;
prepare narrow_stmt from 'select b*b as square from narrow_values where id=?';
set @narrow_id=1;
execute narrow_stmt using @narrow_id;
deallocate prepare narrow_stmt;

-- Non-NULL narrow vectors preserve signed minima and unsigned high bits on widening.
create table narrow_cast_values(id int primary key,b tinyint not null,s smallint not null,i int not null,ub tinyint unsigned not null,us smallint unsigned not null,ui int unsigned not null);
insert into narrow_cast_values values
    (1,-128,-32768,-2147483648,0,0,0),
    (2,0,0,0,128,32768,2147483648),
    (3,127,32767,2147483647,255,65535,4294967295);
-- @metacmp(true)
select id,cast(b as signed) as b,cast(s as signed) as s,cast(i as signed) as i,cast(ub as unsigned) as ub,cast(us as unsigned) as us,cast(ui as unsigned) as ui from narrow_cast_values order by id;
create table wide_cast_values(id int primary key,b bigint,s bigint,i bigint,ub bigint unsigned,us bigint unsigned,ui bigint unsigned);
insert into wide_cast_values select * from narrow_cast_values;
-- @metacmp(true)
select * from wide_cast_values order by id;

-- Lexical integer precision must not change numeric-to-text assignment.
create table literal_texts(id int primary key,b binary(4),v varchar(4),j json);
insert into literal_texts values(1,0002,0002,0002),(2,'0002','0002','"0002"');
select id,hex(b) as b,v,j from literal_texts order by id;

set session sql_mode = @mixed_unsigned_saved_sql_mode;
drop database mixed_unsigned_arithmetic_28581;

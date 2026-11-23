-- @suite

-- @case
-- @desc: integer dividends with a DECIMAL256 divisor bind the division operator; reverse,
-- narrower decimal, typed-cast, DIV-by-DECIMAL256 and division-by-zero neighbors stay on their
-- existing contracts (issue 31637)
-- @label:bvt

drop database if exists int_d256_div;
create database int_d256_div;
use int_d256_div;

-- unsigned and signed extremes, NULL dividends and NULL divisors
create table src(id int primary key, u bigint unsigned, s bigint, d decimal(65, 30));
insert into src values (1,10,10,2),(2,18446744073709551615,-9223372036854775808,1),(3,null,null,2),(4,1,1,null);

select id, u / d from src order by id;
select id, s / d from src order by id;

-- every integer storage width binds, exact cast values through the scale-0 widening
create table iw(t8 tinyint, t16 smallint, t32 int, t64 bigint, u8 tinyint unsigned, u16 smallint unsigned, u32 int unsigned, u64 bigint unsigned, d decimal(65, 30));
insert into iw values (127,32767,2147483647,-9223372036854775808,255,65535,4294967295,18446744073709551615,2);

select t8 / d, t16 / d, t32 / d, t64 / d from iw;
select u8 / d, u16 / d, u32 / d, u64 / d from iw;

-- reverse rows keep the DECIMAL256 dividend side unchanged
select id, d / u from src where id = 1;

-- a cast into narrow decimal keeps the DECIMAL128-domain typing
select id, u / cast(d as decimal(38, 10)) as q from src where id <= 2 order by id;

-- explicitly typed same-physical-type operands stay on decimal256 division
select id, cast(u as decimal(65, 0)) / d as q from src order by id;

-- unsigned DIV with a DECIMAL256 divisor keeps the unsigned-mixed quotient
select id, u div d from src order by id;

-- a zero divisor stays NULL on the SELECT path
insert into src values (5, 7, 7, 0);
select id, u / d from src where id = 5;

-- INSERT ... SELECT consumer widens the scale-4 quotient into the destination scale
create table dst(id int primary key, v decimal(65, 30));
insert into dst values (99, 9);
insert into dst select id, u / d from src where id <= 2;
select id, v from dst order by id;

drop database int_d256_div;

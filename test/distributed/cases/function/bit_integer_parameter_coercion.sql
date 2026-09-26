drop database if exists bit_integer_parameter_coercion;
create database bit_integer_parameter_coercion;
use bit_integer_parameter_coercion;

-- Exact scales, approximate rounding, booleans and NULL share the parameter contract.
create table sources(id int primary key,d1 decimal(4,1),d2 decimal(5,2),f double,b boolean);
insert into sources values (1,1.4,1.40,1.4,true),(2,1.5,1.50,1.5,false),(3,1.9,1.90,1.9,null),(4,-1.5,-1.50,-1.5,null),(5,2.5,2.50,2.5,true),(6,null,null,null,null);
select export_set(cast(1.4 as decimal(4,1)),'Y','N','',4),export_set(cast(1.40 as decimal(4,2)),'Y','N','',4),export_set(cast(1.400 as decimal(5,3)),'Y','N','',4);
select id,export_set(d1,'Y','N','',4),export_set(d2,'Y','N','',4),export_set(f,'Y','N','',4),export_set(b,'Y','N','',4) from sources order by id;
select id,hex(d1),hex(f),hex(cast(f as double)),make_set(f,'a','b','c'),hex(char(f)),export_set(cast(f as double),'Y','N','',4) from sources order by id;
select id,export_set(if(id=5,d1,f),'Y','N','',4),export_set(case when id=4 then d2 else f end,'Y','N','',4) from sources order by id;

-- Signed values, unsigned/BIT and text bit patterns retain separate source ranges.
create table domains(id int,u bigint unsigned,s bigint,bits bit(64),e enum('one','two'),v varchar(32));
insert into domains values (1,18446744073709551615,-2,0xffffffffffffffff,'two','-2tail'),(2,9223372036854775808,-9223372036854775808,0x8000000000000000,'one','18446744073709551615'),(3,null,null,null,null,null);
select id,hex(u),hex(s),hex(bits),hex(e),hex(v),hex(char(u)),hex(char(e)),make_set(e,'a','b'),export_set(e,'Y','N','',4),export_set(v,'Y','N','',4) from domains order by id;
select id,export_set(u,'Y','N','',4),export_set(bits,'Y','N','',4),make_set(s,'a','b','c'),export_set(if(id=1,s,u),'Y','N','',4) from domains order by id;
select hex(cast('9007199254740993' as decimal(20,0))),export_set(cast('9007199254740993' as decimal(20,0)),'Y','N','',4);
select export_set(if(false,cast('9223372036854775808' as decimal(20,0)),1),'Y','N','',4);
select export_set(cast('9223372036854775808' as decimal(20,0)),'Y','N','',4);
select make_set('18446744073709551616','a','b');
select hex(char('-9223372036854775809'));
select hex(cast('9223372036854775807.5' as decimal(38,1)));

-- CONV coerces only its base parameters; HEX strings and BIN/OCT keep their domains.
select conv('ff',15.5,9.5),conv('1.9',10,16),conv('ffffffffffffffff',16,-10),hex('1.5'),hex(0x41),bin(-1),oct(-1);
select hex(char(65.5e0,'67.9',true)),hex(char(cast(65.5 as double))),export_set(null,'Y','N','',4);

-- Repeated SQL EXECUTE switches source types without poisoning the prepared template.
prepare bit_source from 'select hex(?),hex(char(?)),make_set(?,"a","b","c"),export_set(?,"Y","N","",4)';
set @v=null;
execute bit_source using @v,@v,@v,@v;
set @v=1.5e0;
execute bit_source using @v,@v,@v,@v;
set @v=2.5;
execute bit_source using @v,@v,@v,@v;
set @v='1.5';
execute bit_source using @v,@v,@v,@v;
set @v=true;
execute bit_source using @v,@v,@v,@v;
set @v=cast('18446744073709551615' as unsigned);
execute bit_source using @v,@v,@v,@v;
set @v=cast('9223372036854775808' as decimal(20,0));
execute bit_source using @v,@v,@v,@v;
set @v=-1.5e0;
execute bit_source using @v,@v,@v,@v;
deallocate prepare bit_source;
prepare radix_source from 'select conv("ff",?,?)';
set @a=15.5,@b=9.5;
execute radix_source using @a,@b;
deallocate prepare radix_source;

-- Catalog owners persist the shared identities, not executor-local conversion rules.
create table generated_bits(f double,g varchar(4) generated always as (export_set(f,'Y','N','',4)) stored,h varchar(16) default (hex(cast('9007199254740993' as decimal(20,0)))),check(length(export_set(f,'Y','N','',4))=4));
insert into generated_bits(f) values (1.5),(2.5),(null);
select * from generated_bits order by f;
drop database bit_integer_parameter_coercion;

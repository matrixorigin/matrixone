drop database if exists integer_parameter_coercion;
create database integer_parameter_coercion;
use integer_parameter_coercion;

-- 同一数值的整数求值取决于表达式来源，而不仅是最终类型。
create table sources(id int, d decimal(38,18), v double, s varchar(32), bits bit(8));
insert into sources values (1,2.5,2.5,'1.9tail',b'10'),(2,1.5,1.5,'-1.9tail',b'1'),(3,null,null,null,null);
select id,substring_index('a.b.c.d','.',d),substring_index('a.b.c.d','.',v),substring_index('a.b.c.d','.',cast(v as double)),substring_index('a.b.c.d','.',s),substring_index('a.b.c.d','.',bits) from sources order by id;
select substring_index('a.b.c.d','.',2.5),substring_index('a.b.c.d','.',2.5e0),substring_index('a.b.c.d','.',cast(1.5 as double));
select id,substring_index('a.b.c.d','.',if(id=1,d,v)),substring_index('a.b.c.d','.',case when id=1 then d else v end),substring_index('a.b.c.d','.',if(id=2,cast(v as double),v)) from sources order by id;
select id,substring_index('a.b.c.d','.',cast(v as double)+0e0),substring_index('a.b.c.d','.',abs(cast(v as double))),substring_index('a.b.c.d','.',coalesce(cast(v as double),0e0)),substring_index('a.b.c.d','.',ifnull(cast(v as double),0e0)),substring_index('a.b.c.d','.',nullif(cast(v as double),-1e0)) from sources order by id;
select substring_index('a.b.c','.',if(true,1,cast('9223372036854775808' as decimal(20,0))));
select substring_index('a.b.c','.',case when false then cast('9223372036854775808' as decimal(20,0)) else 1 end);

-- 共享转换覆盖所有首批消费者及别名。
select left('abcd',2.5),right('abcd',2.5),substring('abcd',2.5),substr('abcd',1,2.5),mid('abcd',1,2.5);
select lpad('x',2.5,'0'),rpad('x',2.5,'0'),insert('abcd',2.5,1.5,'X'),locate('b','abcabc',2.5);
select repeat('x',2.5),length(space(2.5)),elt(2.5,'a','b','c'),period_add(202401,2.5),period_diff(202403.5,202401.5);
select left('abcd',2.5e0),right('abcd',2.5e0),substring('abcd',2.5e0),lpad('x',2.5e0,'0'),repeat('x',2.5e0),elt(2.5e0,'a','b','c');
select substring_index('a.b.c','.',true),substring_index('a.b.c','.',false),substring_index('a.b.c','.', '  +2tail'),substring_index('a.b.c','.', '1e2'),substring_index('a.b.c','.', 'bad');
select substring_index('a.b.c','.',cast('9007199254740993.5' as decimal(38,1)));
select substring_index('a.b.c','.',cast('9223372036854775807.4' as decimal(38,1)));
select substring_index('a.b.c','.',cast('9223372036854775807.5' as decimal(38,1)));
select substring_index('a.b.c','.',18446744073709551615);
select substring_index('a.b.c','.', '-9223372036854775809');
select substring_index('a.b.c','.',null);

-- DEFAULT 的目标列类型不能预先把 count 的 DECIMAL 变成文本。
create table defaults_t(a varchar(32) default (substring_index('a.b.c.d','.',2.5)),b varchar(32) default (substring_index('a.b.c.d','.',cast(1.5 as double))));
insert into defaults_t values (default,default);
select * from defaults_t;
create table generated_t(v double,g varchar(32) generated always as (substring_index('a.b.c.d','.',v)) stored,h varchar(32) generated always as (substring_index('a.b.c.d','.',cast(v as double))) stored,check(length(substring_index('a.b.c.d','.',v))>=0));
insert into generated_t(v) values (1.5),(2.5),(null);
select * from generated_t order by v;
update generated_t set v=3.5 where v=1.5;
select * from generated_t order by v;

-- SQL EXECUTE 在同一个 statement 上切换来源，并在错误后复用。
prepare integer_source from 'select substring_index("a.b.c.d",".",?),substring_index("a.b.c.d",".",cast(? as double)),period_add(202401,?),left("abcd",?)';
set @v=1.5e0;
execute integer_source using @v,@v,@v,@v;
set @v=2.5;
execute integer_source using @v,@v,@v,@v;
set @v='1.9tail';
execute integer_source using @v,@v,@v,@v;
set @v=null;
execute integer_source using @v,@v,@v,@v;
set @v='9223372036854775808';
execute integer_source using @v,@v,@v,@v;
set @v=1.5e0;
execute integer_source using @v,@v,@v,@v;
deallocate prepare integer_source;

-- 精度参数只整数化第二个参数，数值本身保留小数与返回域。
select round(1234,-1.5e0),round(1234,cast(-1.5 as double)),round(1234,-2.5);
select round(1.2345e0,1.5e0),round(1.2345e0,cast(1.5 as double)),truncate(1.2399e0,2.5);
select ceil(12345,-1.5e0),ceiling(12345,cast(-1.5 as double)),floor(12345,-1.5e0),floor(12345,cast(-1.5 as double));
select case when true then ceiling(12345,cast(-1.5 as double)) else round(1,1e40) end;
select case when false then ceiling(12345,cast(-1.5 as double)) else round(1,1e40) end;
create table more_sources(id int,n double,d decimal(5,1));
insert into more_sources values (1,1.5,1.5),(2,2.5,2.5),(3,null,null);
select id,round(1.2345e0,n),round(1.2345e0,d),truncate(1.2345e0,n),truncate(1.2345e0,d) from more_sources order by id;
select ceil(12345,n) from more_sources;
select floor(12345,n) from more_sources;
select id,regexp_instr('a.b.c','[.]',1,n),regexp_substr('a1b2c3','[0-9]',1,n),regexp_substr('a1b2c3','[0-9]',1,d),regexp_replace('a.b.c','[.]','X',1,n) from more_sources order by id;
select regexp_instr('a.b.c','[.]',1,1,0.5e0),regexp_instr('a.b.c','[.]',1,1,0.5);
select regexp_instr('a.b.c','[.]',1,1,127.5e0);
select from_days(738886.5),from_days(738886.5e0),from_days(null);
select week('2021-01-03',2.5),week('2021-01-03',2.5e0),yearweek('2021-01-03',2.5),yearweek('2021-01-03',2.5e0);
select length(random_bytes(1.5e0)),length(random_bytes(cast(1.5 as double))),length(random_bytes(null));
select length(random_bytes(0));
select length(random_bytes(1025));
select length(sha2('abc','256tail')),length(sha2('abc',255.5e0)),length(sha2('abc',cast(255.5 as double))),sha2('abc',null);
select sha2('abc','9223372036854775808');
select l1_norm(subvector(cast('[1,2,3,4]' as vecf32),2.5,1.5e0));
create table year_source(y year);
insert into year_source values (0),(2024),(null);
select cast(y as unsigned),substring_index('a.b.c','.',y) from year_source order by y;
select length(random_bytes(y)) from year_source where y=2024;

prepare additional_integer_source from 'select round(1.2345e0,?),regexp_instr("a.b.c","[.]",1,?),length(random_bytes(?)),week("2021-01-03",?)';
set @v=1.5e0;
execute additional_integer_source using @v,@v,@v,@v;
set @v='1.9tail';
execute additional_integer_source using @v,@v,@v,@v;
set @v=null;
execute additional_integer_source using @v,@v,@v,@v;
set @v=1.5e0;
execute additional_integer_source using @v,@v,@v,@v;
deallocate prepare additional_integer_source;

-- Text bit parameters preserve the full range without changing numeric source domains.
create table bit_sources(id int,s varchar(32),n double,u bigint unsigned,i bigint);
insert into bit_sources values (1,'65.5',65.5,18446744073709551615,-2),(2,'18446744073709551615',66.5,9223372036854775808,-1),(3,'-1',null,0,-2),(4,null,null,null,null);
select id,hex(char(s)),hex(char(n)),hex(char(cast(n as double))),hex(char(if(id=1,u,i))) from bit_sources order by id;
select id,make_set(s,'a','b'),export_set(s,'Y','N',',',2),hex(s),hex(n) from bit_sources order by id;
select hex(char('9223372036854775808')),hex(char(-1,18446744073709551615)),hex(0x0001);
select hex(char(if(false,cast('9223372036854775808' as decimal(20,0)),18446744073709551615)));
select char(if(true,cast('9223372036854775808' as decimal(20,0)),18446744073709551615));
select char('18446744073709551616');
select char('-9223372036854775809');
select substring_index('a.b','.', '18446744073709551615');
select conv('10',2.5,10),conv('10',2.5e0,10),conv(1.5,10,8);
select split_part('a.b.c','.',1.5e0),split_part('a.b.c','.',cast(1.5 as double));
select split_part('a.b.c','.',4294967296);
prepare bit_integer_source from 'select hex(char(?)),make_set(?,"a","b"),export_set(?,"Y","N",",",2)';
set @v=65.5e0;
execute bit_integer_source using @v,@v,@v;
set @v='65.5';
execute bit_integer_source using @v,@v,@v;
set @v='18446744073709551615';
execute bit_integer_source using @v,@v,@v;
set @v=-1;
execute bit_integer_source using @v,@v,@v;
set @v=null;
execute bit_integer_source using @v,@v,@v;
deallocate prepare bit_integer_source;

-- TIMESTAMPADD converts only the count, preserving unit and temporal return metadata.
select timestampadd(day,2.5,'2024-01-01'),timestampadd(day,2.5e0,'2024-01-01'),timestampadd(day,cast(1.5 as double),'2024-01-01');
select id,timestampadd(day,n,'2024-01-01'),timestampadd(day,d,'2024-01-01'),timestampadd(day,cast(n as double),'2024-01-01') from more_sources order by id;
select timestampadd(day,1,cast('2024-01-01' as date)),timestampadd(day,1.5e0,cast('2024-01-01' as date)),timestampadd(microsecond,1.5e0,'2024-01-01');
select timestampadd(day,if(true,1,1e40),'2024-01-01');
select timestampadd(day,18446744073709551615,'2024-01-01');
select timestampadd(day,'9223372036854775808','2024-01-01');

-- Extended sources retain role-specific acceptance and native value semantics.
create table enum_sources (v enum('20.5','10.5'));
insert into enum_sources values ('20.5'),('10.5');
select cast(v as unsigned),left('abcd',v),hex(char(v)),substring_index('a.b.c','.',v) from enum_sources order by v;
select octet_length(random_bytes(v)),conv('10',v,10) from enum_sources order by v;
select left('abcd',v),left('abcd',ifnull(v,'')) from (select v from enum_sources) derived order by v;
create table temporal_sources(t time(6),d datetime(6),day_value date,ts timestamp(6));
insert into temporal_sources values ('00:02:55.5','2023-12-31 23:59:59.5','2024-01-01','2024-01-01 00:00:00.5');
select sha2('abc',t),sha2('abc',d),sha2('abc',day_value),sha2('abc',ts) from temporal_sources;
select sha2('abc',cast('838:59:59.999999' as time(6))),sha2('abc',cast('9999-12-31 23:59:59.999999' as datetime(6)));
select sha2('abc',if(true,cast('00:02:55.5' as time(6)),1e40));
select sha2('abc',cast('00000256-0000-0000-0000-000000000000' as uuid));
select left('abc',cast('00:00:01.5' as time(6)));

-- Time construction integerizes hour/minute but preserves fractional seconds.
select maketime(12.5,58.5,30.5),maketime(12.5e0,58.5e0,30.5e0),maketime(cast(12.5 as double),cast(58.5 as double),cast(30.5 as double)),maketime('12.9tail','58.9tail','30.5tail');
select id,maketime(n,d,30.5),maketime(d,n,30.5e0),maketime(cast(n as double),cast(d as double),30.5) from more_sources order by id;
select maketime(cast('00:00:12.5' as time(6)),1,2),maketime(if(true,12.5,12.5e0),if(false,58.5,58.5e0),30.5),maketime(coalesce(cast(12.5 as double),0e0),1,2);
select maketime(v,1,2) from enum_sources order by v;
select maketime(NULL,1,2),maketime(12,NULL,2),maketime(12,1,NULL),maketime(cast('00000012-0000-0000-0000-000000000000' as uuid),1,2);
select maketime(cast('9223372036854775808' as decimal(20,0)),0,0);
select maketime('9223372036854775808',0,0);
-- Formatting precision keeps source-specific integer evaluation while number domains stay distinct.
select format(1234.5,1.5),format(1234.5,1.5e0),format(1234.5,cast(1.5 as double)),format(1234.5,'1.9tail');
select format(2.5,0),format(2.5e0,0),format('2.5',0),format(cast(2.5 as double),0);
select id,format(n,d),format(d,n),format(cast(n as double),n) from more_sources order by id;
select format(1234.5,cast('00:00:01.5' as time(6))),format(1234.5,if(true,2.5,2.5e0)),format(1234.5,coalesce(cast(1.5 as double),0e0));
select format(1234.5,v) from enum_sources order by v;
select format(1234.5,NULL),format(1234.5,1,'de_DE'),format(1234.5,cast('00000002-0000-0000-0000-000000000000' as uuid));
select format(1,cast('9223372036854775808' as decimal(20,0)));
select format(1,'9223372036854775808');
-- Calendar counts keep integer precision through range validation.
select makedate(2024.5,2.5),makedate(2024.5e0,2.5e0),makedate(2024,cast(1.5 as double)),makedate('2024tail','2.9tail');
select id,makedate(2024,n),makedate(2024,d),makedate(2024,cast(n as double)) from more_sources order by id;
select makedate(0,1),makedate(69,1),makedate(70,1),makedate(9999,365),makedate(9999,366),makedate(2024,4294967297),makedate(2024,9223372036854775807);
select makedate(2024,cast('00:00:01.5' as time(6))),makedate(2024,if(true,2.5,2.5e0)),makedate(2024,coalesce(cast(1.5 as double),0e0));
select makedate(2024,v) from enum_sources order by v;
select makedate(NULL,1),makedate(2024,NULL),makedate(cast('00002024-0000-0000-0000-000000000000' as uuid),1);
select makedate(2024,cast('9223372036854775808' as decimal(20,0)));
select makedate(2024,'9223372036854775808');
drop database integer_parameter_coercion;

drop database if exists integer_parameter_coercion;
create database integer_parameter_coercion;
use integer_parameter_coercion;

-- These cases preserve MySQL's permissive integer-parameter behavior. Opt in
-- explicitly because the server default now rejects numeric prefixes.
SET @integer_parameter_saved_sql_mode = @@session.sql_mode;
SET SESSION sql_mode = CONCAT_WS(',', NULLIF(@integer_parameter_saved_sql_mode, ''), 'MYSQL_NUMERIC_COMPATIBILITY');

-- Source-domain behavior is owned by the integer parameter, not only its final type.
create table sources(id int, d decimal(38,18), v double, s varchar(32), bits bit(8));
insert into sources values (1,2.5,2.5,'1.9tail',b'10'),(2,1.5,1.5,'-1.9tail',b'1'),(3,null,null,null,null);
select id,substring_index('a.b.c.d','.',d),substring_index('a.b.c.d','.',v),substring_index('a.b.c.d','.',cast(v as double)),substring_index('a.b.c.d','.',s),substring_index('a.b.c.d','.',bits) from sources order by id;
select substring_index('a.b.c.d','.',2.5),substring_index('a.b.c.d','.',2.5e0),substring_index('a.b.c.d','.',cast(1.5 as double));
select id,substring_index('a.b.c.d','.',if(id=1,d,v)),substring_index('a.b.c.d','.',case when id=1 then d else v end),substring_index('a.b.c.d','.',if(id=2,cast(v as double),v)) from sources order by id;
select id,substring_index('a.b.c.d','.',cast(v as double)+0e0),substring_index('a.b.c.d','.',abs(cast(v as double))),substring_index('a.b.c.d','.',coalesce(cast(v as double),0e0)),substring_index('a.b.c.d','.',ifnull(cast(v as double),0e0)),substring_index('a.b.c.d','.',nullif(cast(v as double),-1e0)) from sources order by id;
select substring_index('a.b.c','.',if(true,1,cast('9223372036854775808' as decimal(20,0))));
select substring_index('a.b.c','.',case when false then cast('9223372036854775808' as decimal(20,0)) else 1 end);
select substring_index('a.b.c','.',true),substring_index('a.b.c','.',false),substring_index('a.b.c','.', '  +2tail'),substring_index('a.b.c','.', '1e2'),substring_index('a.b.c','.', 'bad');
select substring_index('a.b.c','.',cast('9007199254740993.5' as decimal(38,1)));
select substring_index('a.b.c','.',cast('9223372036854775807.4' as decimal(38,1)));
select substring_index('a.b.c','.',cast('9223372036854775807.5' as decimal(38,1)));
select substring_index('a.b.c','.',18446744073709551615);
select substring_index('a.b.c','.', '-9223372036854775809');
select substring_index('a.b.c','.',null);

-- Persisted expressions preserve their original SQL and recover after restart.
create table defaults_t(a varchar(32) default (substring_index('a.b.c.d','.',2.5)),b varchar(32) default (substring_index('a.b.c.d','.',cast(1.5 as double))));
insert into defaults_t values (default,default);
select * from defaults_t;
create table generated_t(v double,g varchar(32) generated always as (substring_index('a.b.c.d','.',v)) stored,h varchar(32) generated always as (substring_index('a.b.c.d','.',cast(v as double))) stored,check(length(substring_index('a.b.c.d','.',v))>=0));
insert into generated_t(v) values (1.5),(2.5),(null);
select * from generated_t order by v;
update generated_t set v=3.5 where v=1.5;
select * from generated_t order by v;

-- One prepared statement switches runtime source domains and remains reusable after errors.
prepare integer_source from 'select substring_index("a.b.c.d",".",?),substring_index("a.b.c.d",".",cast(? as double))';
set @v=1.5e0;
execute integer_source using @v,@v;
set @v=2.5;
execute integer_source using @v,@v;
set @v='1.9tail';
execute integer_source using @v,@v;
set @v=null;
execute integer_source using @v,@v;
set @v='9223372036854775808';
execute integer_source using @v,@v;
set @v=1.5e0;
execute integer_source using @v,@v;
deallocate prepare integer_source;

-- Domainless NULL stays nullable across physical projections and cached executions.
prepare nullable_source from 'select substring_index("a.b.c.d",".",(select ? group by 1)),substring_index("a.b.c.d",".",(select ? union all select null limit 1)),substring_index("a.b.c.d",".",coalesce(?,null))';
set @v=null;
execute nullable_source using @v,@v,@v;
set @v=1.5e0;
execute nullable_source using @v,@v,@v;
set @v='1.5';
execute nullable_source using @v,@v,@v;
set @v=null;
execute nullable_source using @v,@v,@v;
deallocate prepare nullable_source;

-- Nested bit aggregates own their source conversion, including protocol text.
prepare bit_source from 'select substring_index("a.b.c.d",".",(select bit_or(?) from sources where id=1)),substring_index("a.b.c.d",".",(select bit_and(?) from sources where id=1)),substring_index("a.b.c.d",".",(select bit_xor(?) from sources where id=1))';
set @v='1.5';
execute bit_source using @v,@v,@v;
set @v=1.5e0;
execute bit_source using @v,@v,@v;
deallocate prepare bit_source;

-- Source lineage crosses scalar subqueries, derived UNION branches and recursive CTE source steps.
prepare lineage_source from 'select substring_index("a.b.c.d",".",coalesce((select ? where true),2.5e0)),substring_index("a.b.c.d",".",(select 0e0 where false union all select x from (select ? x) d)),substring_index("a.b.c.d",".",(with recursive r(n) as (select ? union all select n from r where false) select n from r)),substring_index("a.b.c.d",".",bit_count(?))';
set @v=1.5e0,@bits=2;
execute lineage_source using @v,@v,@v,@bits;
set @v='1.5',@bits='2';
execute lineage_source using @v,@v,@v,@bits;
set @v=null,@bits=null;
execute lineage_source using @v,@v,@v,@bits;
deallocate prepare lineage_source;

-- String position/count/length consumers share the same conversion for columns.
select id,left('abcdef',d),right('abcdef',v),substring('abcdef',d),substr('abcdef',1,v),mid('abcdef',1,cast(v as double)),lpad('x',d,'.'),rpad('x',v,'.') from sources order by id;
select id,insert('abcdef',d,1,'X'),insert('abcdef',1,v,'X'),locate('a','baaa',d),repeat('x',v),length(space(d)),elt(d,'a','b','c'),elt(s,'a','b','c') from sources order by id;
select left('abcdef',2.5),left('abcdef',2.5e0),left('abcdef',cast(1.5 as double)),left('abcdef','1.9tail');
select id,left('abcdef',if(id=1,d,v)),substring('abcdef',1,case when id=1 then d else v end),elt(if(id=1,d,v),'a','b','c') from sources order by id;
select left('abc',-1),right('abc',0),substring('abc',-2),substring('abc',1,0),lpad('x',0,'.'),rpad('x',0,'.'),insert('abc',0,1,'X'),locate('a','abc',0),repeat('x',-1),length(space(-1)),elt(0,'a','b');
select locate('a','baaa'),substring('abcdef',2),substr('abcdef',2),mid('abcdef',2);
select left('abc',cast('9223372036854775807.4' as decimal(38,1))),elt(9223372036854775807,'a','b'),length(space(-9223372036854775808));
select left('abc',cast('9223372036854775807.5' as decimal(38,1)));
select substring('abc',1,18446744073709551615);
select elt(18446744073709551615,'a','b');
select space('-9223372036854775809');
select repeat('x',if(true,2,18446744073709551615)),elt(case when false then 18446744073709551615 else 1 end,'a','b');

-- Every migrated parameter participates in cached SQL EXECUTE specialization.
prepare string_source from 'select left("abcdef",?),right("abcdef",?),substring("abcdef",?),substr("abcdef",1,?),mid("abcdef",1,?),lpad("x",?,"."),rpad("x",?,"."),insert("abcdef",?,1,"X"),insert("abcdef",1,?,"X"),locate("a","baaa",?),repeat("x",?),length(space(?)),elt(?,"a","b","c")';
set @v=null;
execute string_source using @v,@v,@v,@v,@v,@v,@v,@v,@v,@v,@v,@v,@v;
set @v=2.5;
execute string_source using @v,@v,@v,@v,@v,@v,@v,@v,@v,@v,@v,@v,@v;
set @v=2.5e0;
execute string_source using @v,@v,@v,@v,@v,@v,@v,@v,@v,@v,@v,@v,@v;
set @v='1.9tail';
execute string_source using @v,@v,@v,@v,@v,@v,@v,@v,@v,@v,@v,@v,@v;
set @v='9223372036854775808';
execute string_source using @v,@v,@v,@v,@v,@v,@v,@v,@v,@v,@v,@v,@v;
set @v=2;
execute string_source using @v,@v,@v,@v,@v,@v,@v,@v,@v,@v,@v,@v,@v;
deallocate prepare string_source;

-- Utility consumers use private integer coercion; math precision retains strict INT64.
select period_add(202401,d),period_diff(202402+d,202401) from sources order by id;
select ceil(12.345,2.5),floor(12.345,2.5),round(12.345,2.5),truncate(12.345,2.5);
-- Invalid text precision stays strict even in MYSQL_NUMERIC_COMPATIBILITY.
select ceil(12.345,'2.5tail');
select ceiling(12.345,'2.5tail');
select floor(12.345,'2.5tail');
select round(12.345,'2.5tail');
select truncate(12.345,'2.5tail');
select from_days(d),week(cast('2026-09-20' as date),d),yearweek(cast('2026-09-20' as date),d),timestampadd(day,d,cast('2026-09-20' as date)) from sources order by id;
select subvector(cast('[1,2,3,4]' as vecf32(4)),d),subvector(cast('[1,2,3,4]' as vecf32(4)),1,d) from sources order by id;
select split_part('a.b.c','.',d),sha2('matrixone',d),regexp_instr('abcabc','b',d),regexp_replace('abcabc','b','X',d,d),regexp_substr('abcabc','b',d,d) from sources order by id;
select length(random_bytes(2.5));
select last_query_id(-1) is not null;
select split_part('a.b.c','.',cast('4294967296' as decimal(20,0)));
select sha2('matrixone',cast('9223372036854775807.5' as decimal(38,1)));

prepare utility_source from 'select period_add(202401,?),round(12.345,?),from_days(?),week(cast("2026-09-20" as date),?),timestampadd(day,?,cast("2026-09-20" as date)),split_part("a.b.c",".",?),sha2("matrixone",?),regexp_instr("abcabc","b",?)';
set @v=1.5;
execute utility_source using @v,@v,@v,@v,@v,@v,@v,@v;
set @v='2.5tail';
set @integer_math_precision=2;
execute utility_source using @v,@integer_math_precision,@v,@v,@v,@v,@v,@v;
-- Preserve the other utilities' prefix oracle, then reject ROUND's own control.
execute utility_source using @v,@v,@v,@v,@v,@v,@v,@v;
set @integer_math_precision=null;
set @v=null;
execute utility_source using @v,@v,@v,@v,@v,@v,@v,@v;
deallocate prepare utility_source;

drop database integer_parameter_coercion;
SET SESSION sql_mode = @integer_parameter_saved_sql_mode;
SET @integer_parameter_saved_sql_mode = NULL;

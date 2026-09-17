drop database if exists integer_parameter_coercion;
create database integer_parameter_coercion;
use integer_parameter_coercion;

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

drop database integer_parameter_coercion;

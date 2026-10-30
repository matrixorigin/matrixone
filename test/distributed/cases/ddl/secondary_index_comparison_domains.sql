-- Issue #31619: encoded candidates must preserve SQL comparison domains.
-- Fixed statement time makes NOW and precision boundaries deterministic.
drop database if exists index_comparison_domains;
create database index_comparison_domains;
use index_comparison_domains;
set @saved_zone=@@time_zone;
set time_zone='+00:00';
set timestamp=1704888000.123;
create table t(id int primary key, tenant int, d datetime(6) not null, ts timestamp(3), payload varchar(8), key kd(d), key kts(ts), key ktd(tenant,d));
insert into t values
(1,1,'2024-01-10 11:59:59.999999','2024-01-10 11:59:59.999','a'),
(2,1,'2024-01-10 12:00:00.123000','2024-01-10 12:00:00.123','b'),
(3,1,'2024-01-10 12:00:00.123456','2024-01-10 12:00:00.123','c'),
(4,1,'2024-01-10 12:00:00.124000','2024-01-10 12:00:00.124','d'),
(5,2,'2025-01-10 12:00:00.123000','2025-01-10 12:00:00.123','e');

-- Non-nullable leading keys exercise the original false-positive report.
select id from t ignore index(kd,kts,ktd) where d<now(3) order by id;
select id from t where d<now(3) order by id;
select id from t force index(kd) where d<now(3) order by id;
-- Mixed comparison truncates to TIMESTAMP(3): both 123000 and 123456 match.
select id from t where d=now(3) order by id;
select id,payload from t where d=now(3) order by id;
select id from t force index(kd) where d>=now(3) order by id;
select id from t where now(3)<d order by id;
select id from t where d<=now(3) order by id;
select id from t where d>=cast('2024-01-10 12:00:00.123' as timestamp(3)) and d<cast('2024-01-10 12:00:00.124' as timestamp(3)) order by id;
select id from t where d between cast('2024-01-10 12:00:00.123' as timestamp(3)) and cast('2024-01-10 12:00:00.124' as timestamp(3)) order by id;
select id from t where d in (now(3),cast('2025-01-10 12:00:00.123' as timestamp(3)),null) order by id;
select id from t where d<now(3) or d>=cast('2025-01-10 00:00:00' as timestamp(3)) order by id;
select id from t where tenant=1 and d=now(3) order by id;
select id from t where ts>=cast('2024-01-10 12:00:00.123456' as datetime(6)) order by id;
select id from t force index(kd) where d<now(3) order by id desc limit 1;
-- Nearest same-domain controls retain different, exact DATETIME semantics.
select id from t force index(kd) where d=cast(now(3) as datetime(6)) order by id;
select id from t where d<cast(now(3) as datetime(6)) order by id;

-- Repeated EXECUTE must use the current parameter domain and session timezone.
prepare peq from 'select id from t where d=? order by id';
set @p=cast('2024-01-10 12:00:00.123' as datetime(6));
execute peq using @p;
set @p=now(3);
execute peq using @p;
set @p=null;
execute peq using @p;
set time_zone='+08:00';
set @p=now(3);
execute peq using @p;
set time_zone='+00:00';

-- Flush base and all index tables: byte-oriented pruning must also be safe.
select mo_ctl('dn','flush','index_comparison_domains.t');
set @idx=(select distinct index_table_name from mo_catalog.mo_indexes where name='kd' and table_id=(select rel_id from mo_catalog.mo_tables where reldatabase='index_comparison_domains' and relname='t'));
select mo_ctl('dn','flush',concat('index_comparison_domains.',@idx));
set @idx=(select distinct index_table_name from mo_catalog.mo_indexes where name='kts' and table_id=(select rel_id from mo_catalog.mo_tables where reldatabase='index_comparison_domains' and relname='t'));
select mo_ctl('dn','flush',concat('index_comparison_domains.',@idx));
set @idx=(select distinct index_table_name from mo_catalog.mo_indexes where name='ktd' and table_id=(select rel_id from mo_catalog.mo_tables where reldatabase='index_comparison_domains' and relname='t'));
select mo_ctl('dn','flush',concat('index_comparison_domains.',@idx));
select id from t where d<now(3) order by id;
select id from t where d=now(3) order by id;
select id,payload from t force index(kd) where d=now(3) order by id;
select id from t where d>=now(3) order by id;
select id from t where tenant=1 and d=now(3) order by id;
set @p=now(3);
execute peq using @p;
deallocate prepare peq;

-- Late composite-key lowering must preserve unsafe temporal SQL residuals.
create table cp(tenant int,d datetime(6),id int,primary key(tenant,d));
insert into cp select tenant,d,id from t;
select id from cp where tenant=1 and d=now(3) order by id;
select id from cp where tenant=1 and d>=cast('2024-01-10 12:00:00.123200' as timestamp(6)) and d<=cast('2024-01-10 12:00:00.123' as timestamp(3)) order by id;
-- Direct unique typed comparisons remain valid, but paired mixed scales differ
-- from a newly synthesized BETWEEN precision contract.
create table uq(id int primary key,d datetime(6),unique key ud(d));
insert into uq select id,d from t;
select id from uq where d>=cast('2024-01-10 12:00:00.123200' as timestamp(6)) and d<=cast('2024-01-10 12:00:00.123' as timestamp(3)) order by id;
select id from uq force index(ud) where cast('2024-01-10 12:00:00.123' as datetime(6))>d order by id;

-- Prefix equality and open bounds on escaped NULs need exact SQL rechecks.
create table bytes_key(tenant int,a varbinary(8),id int,primary key(tenant,a,id));
insert into bytes_key values(1,x'61',1),(1,x'6100',2),(1,x'62',3);
select id from bytes_key where tenant=1 and a=cast(x'61' as varbinary(8)) order by id;
select id from bytes_key where tenant=1 and a>cast(x'61' as varbinary(8)) order by id limit 2;
create table nullable_cluster(id int,a int,b int) cluster by(a,b);
insert into nullable_cluster values(1,1,null),(2,1,3),(3,1,7);
select id from nullable_cluster where a=1 and b<5 order by id;


select id from nullable_cluster where a=1 and b<=5 order by id limit 1;
create table bytes_or(a varbinary(8),id int,primary key(a,id));
insert into bytes_or values(x'61',1),(x'6100',2),(x'62',3);
select id from bytes_or where a=cast(x'61' as varbinary(8)) or a=cast(x'62' as varbinary(8)) order by id;
create table bytes_full(tenant int,a varbinary(8),id int,primary key(tenant,a));
insert into bytes_full select tenant,a,id from bytes_key;
select id from bytes_full where tenant=1 and a>cast(x'61' as varbinary(8)) and a<cast(x'62' as varbinary(8)) order by id;
create table text_cluster(a text,id int) cluster by(a,id);
insert into text_cluster values('a',1),(concat('a',char(0)),2),('b',3);
select id from text_cluster where a=cast('a' as text) order by id;
select id from text_cluster where a>cast('a' as text) order by id;
create table blob_cluster(a blob,id int) cluster by(a,id);
insert into blob_cluster values(x'61',1),(x'6100',2),(x'62',3);
select id from blob_cluster where a=cast(x'61' as blob) order by id;
select id from blob_cluster where a>cast(x'61' as blob) order by id;


-- CHAR PAD SPACE equality can match distinct persisted byte strings.
-- VARCHAR is the adjacent byte-preserving functional-index control.
create table json_char(id int primary key,doc json,index kj((cast(doc ->> '$.kind' as char(20)))));
insert into json_char values(1,'{"kind":"alpha"}'),(2,'{"kind":"alpha "}');
select id from json_char where cast(doc ->> '$.kind' as char(20))='alpha' order by id;
select id from json_char ignore index(kj) where cast(doc ->> '$.kind' as char(20))='alpha' order by id;
create table json_varchar(id int primary key,doc json,index kj((cast(doc ->> '$.kind' as varchar(20)))));
insert into json_varchar select id,doc from json_char;
select id from json_varchar where cast(doc ->> '$.kind' as varchar(20))='alpha' order by id;

-- A repeated local time is not an invertible TIMESTAMP-to-DATETIME endpoint.
create table dst(id int primary key,d datetime(6),key dd(d));
insert into dst values(1,'2024-11-03 01:15:00'),(2,'2024-11-03 01:30:00'),(3,'2024-11-03 02:00:00');
set time_zone='America/New_York';
set timestamp=1730614500;
select id from dst ignore index(dd) where d<now(6) order by id;
select id from dst where d<now(6) order by id;
select id from dst force index(dd) where d<now(6) order by id;
-- Nonexistent local times use the engine's existing scalar gap normalization.
delete from dst;
insert into dst values(1,'2024-03-10 01:30:00'),(2,'2024-03-10 02:30:00'),(3,'2024-03-10 03:30:00');
set timestamp=1710054900;
select id from dst ignore index(dd) where d<now(6) order by id;
select id from dst where d<now(6) order by id;
select id from dst force index(dd) where d<now(6) order by id;
set time_zone=@saved_zone;
set timestamp=0;
set @p=null,@idx=null,@saved_zone=null;
drop database index_comparison_domains;

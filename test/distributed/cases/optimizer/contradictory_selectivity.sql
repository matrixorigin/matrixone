drop database if exists contradictory_selectivity;
create database contradictory_selectivity;
use contradictory_selectivity;
create table points(id int primary key, v int, d decimal(18,2), s varchar(20));
insert into points values (1,0,0.00,'a'), (2,19,19.00,'b'), (3,81,81.00,'c'), (4,100,100.00,null);
-- @regex("returnStr.*OK",true)
select mo_ctl('dn', 'flush', 'contradictory_selectivity.points');
-- 采样块数与字节数不是本用例契约，保留行数、覆盖方式和完成状态的比较。
-- @ignore:7,8
analyze table points(v,d,s) fullscan;

-- 同列矛盾点谓词必须折叠为常量假：估算近似零行并返回零行，而不是全表。
-- @regex("Table Scan.*selectivity=0[.]0000.*",true)
explain verbose select id from points where v = 19 and v = 81;
select count(*) as matches from points where v = 19 and v = 81;
-- @regex("Table Scan.*selectivity=0[.]0000.*",true)
explain verbose select id from points where d = 19.00 and d = 81.00;
select count(*) as matches from points where d = 19.00 and d = 81.00;
-- @regex("Table Scan.*selectivity=0[.]0000.*",true)
explain verbose select id from points where v = 19 and v = 19 and v = 81;
select count(*) as matches from points where v = 19 and v = 19 and v = 81;

-- 点域与互斥谓词、与 NULL 域的交集同样折叠为常量假。
-- @regex("Table Scan.*selectivity=0[.]0000.*",true)
explain verbose select id from points where v = 19 and v <> 19;
select count(*) as matches from points where v = 19 and v <> 19;
-- @regex("Table Scan.*selectivity=0[.]0000.*",true)
explain verbose select id from points where s = 'a' and s is null;
select count(*) as matches from points where s = 'a' and s is null;

-- 空区间（下界越过上界）维持近似零行估算。
-- @regex("Table Scan.*selectivity=0[.]0000.*",true)
explain verbose select id from points where v < 20 and v > 40;
select count(*) as matches from points where v < 20 and v > 40;

-- 兼容的同列 BETWEEN 不得顺延矛盾点域的折叠：矛盾等值对与 =x AND <>x
-- 在带区间谓词时同样折叠为常量假，返回零行并保持近似零行估算。
-- @regex("Table Scan.*selectivity=0[.]0000.*",true)
explain verbose select id from points where v = 19 and v = 81 and v between 0 and 100;
select count(*) as matches from points where v = 19 and v = 81 and v between 0 and 100;
-- @regex("Table Scan.*selectivity=0[.]0000.*",true)
explain verbose select id from points where v = 19 and v <> 19 and v between 0 and 100;
select count(*) as matches from points where v = 19 and v <> 19 and v between 0 and 100;

-- 可满足控制：单值等值加包含该值的 BETWEEN 保持 1/ndv 点估算（0.2500）
-- 并返回该值的行，证明折叠没有过度剪枝。
-- @regex("Table Scan.*selectivity=0[.]2500.*",true)
explain verbose select id from points where v = 19 and v between 0 and 100;
select count(*) as matches from points where v = 19 and v between 0 and 100;

-- 重复谓词折叠为一个点值，估算保持 1/ndv（ndv(v)=4 → 0.2500），不得回归为全表或零。
-- @regex("Table Scan.*selectivity=0[.]2500.*",true)
explain verbose select id from points where v = 19 and v = 19;
select count(*) as matches from points where v = 19 and v = 19;

-- 合并入块的 #32059 控制（互斥同列等值）：单值等值保持 1/ndv 点估算，
-- 互斥 OR 与等价 IN 既不做零行估算也不做全表估算，行数是两值并集。
-- @regex("Table Scan.*selectivity=0[.]2500.*",true)
explain verbose select id from points where v = 19;
select count(*) as matches from points where v = 19;
-- @regex("Table Scan.*selectivity=0[.]2500.*",true)
explain verbose select id from points where v = 81;
select count(*) as matches from points where v = 81;
-- @regex("Table Scan.*selectivity=0[.]5000.*",true)
explain verbose select id from points where v = 19 or v = 81;
select count(*) as matches from points where v = 19 or v = 81;
-- @regex("Table Scan.*selectivity=0[.]5000.*",true)
explain verbose select id from points where v in (19, 81);
select count(*) as matches from points where v in (19, 81);

-- 复用既有高基数口径（10000 行、k2 均匀循环 1000 个值、带 idx_k2 二级索引）：
-- 单值等值保持 1/ndv=0.0010；互斥 OR 与等价 IN 打印的 selectivity
-- 不得低于单值下界（四位小数在 0.0010 与 1.0000 之间），行数是 10/10/20/20。
create table dig(n int);
insert into dig values (0),(1),(2),(3),(4),(5),(6),(7),(8),(9),(10),(11),(12),(13),(14),(15),(16),(17),(18),(19),(20),(21),(22),(23),(24),(25),(26),(27),(28),(29),(30),(31),(32),(33),(34),(35),(36),(37),(38),(39),(40),(41),(42),(43),(44),(45),(46),(47),(48),(49),(50),(51),(52),(53),(54),(55),(56),(57),(58),(59),(60),(61),(62),(63),(64),(65),(66),(67),(68),(69),(70),(71),(72),(73),(74),(75),(76),(77),(78),(79),(80),(81),(82),(83),(84),(85),(86),(87),(88),(89),(90),(91),(92),(93),(94),(95),(96),(97),(98),(99);
create table plain_or_bound(id int primary key, k2 int, key idx_k2(k2));
insert into plain_or_bound select d1.n * 100 + d2.n, (d1.n * 100 + d2.n) % 1000 from dig d1, dig d2;
-- @regex("returnStr.*OK",true)
select mo_ctl('dn', 'flush', 'contradictory_selectivity.plain_or_bound');
-- @ignore:7,8
analyze table plain_or_bound(k2) fullscan;
-- @regex("Table Scan.*selectivity=0[.]0010.*",true)
explain verbose select id from plain_or_bound where k2 = 5;
select count(*) as matches from plain_or_bound where k2 = 5;
-- @regex("Table Scan.*selectivity=0[.]0010.*",true)
explain verbose select id from plain_or_bound where k2 = 6;
select count(*) as matches from plain_or_bound where k2 = 6;
-- @regex("Table Scan.*selectivity=(0[.]001[0-9]|0[.]00[2-9][0-9]|0[.]0[1-9][0-9][0-9]|0[.][1-9][0-9][0-9][0-9]|1[.]0000).*",true)
explain verbose select id from plain_or_bound where k2 = 5 or k2 = 6;
select count(*) as matches from plain_or_bound where k2 = 5 or k2 = 6;
-- @regex("Table Scan.*selectivity=(0[.]001[0-9]|0[.]00[2-9][0-9]|0[.]0[1-9][0-9][0-9]|0[.][1-9][0-9][0-9][0-9]|1[.]0000).*",true)
explain verbose select id from plain_or_bound where k2 in (5, 6);
select count(*) as matches from plain_or_bound where k2 in (5, 6);

-- 双精度有符号零按 SQL 相等取值：0e0 与 -0e0 匹配同一行。该可满足点域不得
-- 因编码位不同折叠为常量假（保持正估算并返回该行）；不带区间时与重复等值
-- 一样合并为单个等值条件，而真正不同的双精度值叠加 BETWEEN 后仍然折叠。
create table zpoints(id int primary key, f double);
insert into zpoints values (1, 0e0), (2, -1.5e0);
-- @regex("returnStr.*OK",true)
select mo_ctl('dn', 'flush', 'contradictory_selectivity.zpoints');
-- @ignore:7,8
analyze table zpoints(f) fullscan;
-- @regex("Table Scan.*selectivity=0[.]2500.*",true)
explain verbose select id from zpoints where f = 0e0 and f = -0e0 and f between -1e0 and 1e0;
select count(*) as matches from zpoints where f = 0e0 and f = -0e0 and f between -1e0 and 1e0;
-- @regex("Table Scan.*selectivity=0[.]5000.*",true)
explain verbose select id from zpoints where f = 0e0 and f = -0e0;
select count(*) as matches from zpoints where f = 0e0 and f = -0e0;
-- @regex("Table Scan.*selectivity=0[.]0000.*",true)
explain verbose select id from zpoints where f = 0e0 and f = -1.5e0 and f between -1e0 and 1e0;
select count(*) as matches from zpoints where f = 0e0 and f = -1.5e0 and f between -1e0 and 1e0;

-- 原生 Unicode 排序规则按 SQL 值相等取值：utf8mb4_unicode_ci 之下 'a' 与 'A'、
-- 'e' 与 'é' 折叠大小写/重音，属于可满足点域，不得判为矛盾常量假；真正不同的
-- 值仍然折叠。二进制排序规则按字节比较，'a' 与 'A' 是真实矛盾，依旧折叠。
create table cpoints(id int primary key, s varchar(20) collate utf8mb4_unicode_ci);
insert into cpoints values (1, 'a'), (2, 'z'), (3, 'é');
-- @regex("returnStr.*OK",true)
select mo_ctl('dn', 'flush', 'contradictory_selectivity.cpoints');
-- @ignore:7,8
analyze table cpoints(s) fullscan;
-- @regex("Table Scan.*selectivity=0[.]0111.*",true)
explain verbose select id from cpoints where s = 'a' and s = 'A' and s between 'a' and 'z';
select count(*) as matches from cpoints where s = 'a' and s = 'A' and s between 'a' and 'z';
-- @regex("Table Scan.*selectivity=0[.]3333.*",true)
explain verbose select id from cpoints where s = 'a' and s = 'A';
select count(*) as matches from cpoints where s = 'a' and s = 'A';
-- @regex("Table Scan.*selectivity=0[.]0111.*",true)
explain verbose select id from cpoints where s = 'e' and s = 'é' and s between 'a' and 'z';
select count(*) as matches from cpoints where s = 'e' and s = 'é' and s between 'a' and 'z';
-- @regex("Table Scan.*selectivity=0[.]0000.*",true)
explain verbose select id from cpoints where s = 'a' and s = 'z' and s between 'a' and 'z';
select count(*) as matches from cpoints where s = 'a' and s = 'z' and s between 'a' and 'z';
create table bpts(id int primary key, b varchar(20) collate utf8mb4_bin);
insert into bpts values (1, 'a'), (2, 'C');
-- @regex("returnStr.*OK",true)
select mo_ctl('dn', 'flush', 'contradictory_selectivity.bpts');
-- @ignore:7,8
analyze table bpts(b) fullscan;
-- @regex("Table Scan.*selectivity=0[.]0000.*",true)
explain verbose select id from bpts where b = 'a' and b = 'A' and b between 'a' and 'z';
select count(*) as matches from bpts where b = 'a' and b = 'A' and b between 'a' and 'z';

-- CHAR 列比较忽略尾部空格：'a' 与 'a ' 是 SQL 相等值，可满足而不折叠；
-- 真正不同的 CHAR 值仍然折叠。
create table hpts(id int primary key, c char(10));
insert into hpts values (1, 'a'), (2, 'zz');
-- @regex("returnStr.*OK",true)
select mo_ctl('dn', 'flush', 'contradictory_selectivity.hpts');
-- @ignore:7,8
analyze table hpts(c) fullscan;
-- @regex("Table Scan.*selectivity=0[.]0250.*",true)
explain verbose select id from hpts where c = 'a' and c = 'a ' and c between 'a' and 'z';
select count(*) as matches from hpts where c = 'a' and c = 'a ' and c between 'a' and 'z';
-- @regex("Table Scan.*selectivity=0[.]0000.*",true)
explain verbose select id from hpts where c = 'a' and c = 'zz' and c between 'a' and 'z';
select count(*) as matches from hpts where c = 'a' and c = 'zz' and c between 'a' and 'z';

-- 时间字面量的声明精度（round 5）只是显示元数据：运行期比较以物化值为准，
-- DATETIME(0) 与 DATETIME(6) 拼写的同一时刻值是 SQL 相等值，不得判为矛盾常量假；
-- 真正不同（含按精度舍入后物化值不同）的时间值仍然折叠。
create table dpts(id int primary key, d datetime(6));
insert into dpts values (1, '2024-01-01 00:00:00'), (2, '2024-01-01 00:00:01'), (3, '2023-06-15 12:30:00');
-- @regex("returnStr.*OK",true)
select mo_ctl('dn', 'flush', 'contradictory_selectivity.dpts');
-- @ignore:7,8
analyze table dpts(d) fullscan;
-- @regex("Table Scan.*selectivity=0[.]1111.*",true)
explain verbose select id from dpts where d = cast('2024-01-01 00:00:00' as datetime(0)) and d = cast('2024-01-01 00:00:00' as datetime(6)) and d between '2023-01-01 00:00:00' and '2025-01-01 00:00:00';
select count(*) as matches from dpts where d = cast('2024-01-01 00:00:00' as datetime(0)) and d = cast('2024-01-01 00:00:00' as datetime(6)) and d between '2023-01-01 00:00:00' and '2025-01-01 00:00:00';
-- @regex("Table Scan.*selectivity=0[.]3333.*",true)
explain verbose select id from dpts where d = cast('2024-01-01 00:00:00' as datetime(0)) and d = cast('2024-01-01 00:00:00' as datetime(6));
select count(*) as matches from dpts where d = cast('2024-01-01 00:00:00' as datetime(0)) and d = cast('2024-01-01 00:00:00' as datetime(6));
-- @regex("Table Scan.*selectivity=0[.]3333.*",true)
explain verbose select id from dpts where d = cast('2024-01-01 00:00:00' as datetime(0));
select count(*) as matches from dpts where d = cast('2024-01-01 00:00:00' as datetime(0));
-- 真正不同的时间值仍然折叠为常量假。
-- @regex("Table Scan.*selectivity=0[.]0000.*",true)
explain verbose select id from dpts where d = cast('2024-01-01 00:00:00' as datetime(0)) and d = cast('2024-01-03 00:00:00' as datetime(6)) and d between '2023-01-01 00:00:00' and '2025-01-01 00:00:00';
select count(*) as matches from dpts where d = cast('2024-01-01 00:00:00' as datetime(0)) and d = cast('2024-01-03 00:00:00' as datetime(6)) and d between '2023-01-01 00:00:00' and '2025-01-01 00:00:00';
-- @regex("Table Scan.*selectivity=0[.]0000.*",true)
explain verbose select id from dpts where d = cast('2024-01-01 00:00:00.5' as datetime(0)) and d = cast('2024-01-01 00:00:00.5' as datetime(6));
select count(*) as matches from dpts where d = cast('2024-01-01 00:00:00.5' as datetime(0)) and d = cast('2024-01-01 00:00:00.5' as datetime(6));
create table tpts(id int primary key, ts timestamp(6));
insert into tpts values (1, '2024-01-01 00:00:00'), (2, '2024-02-01 00:00:00');
-- @regex("returnStr.*OK",true)
select mo_ctl('dn', 'flush', 'contradictory_selectivity.tpts');
-- @ignore:7,8
analyze table tpts(ts) fullscan;
-- @regex("Table Scan.*selectivity=0[.]2500.*",true)
explain verbose select id from tpts where ts = cast('2024-01-01 00:00:00' as timestamp(0)) and ts = cast('2024-01-01 00:00:00' as timestamp(6)) and ts between '2023-01-01 00:00:00' and '2025-01-01 00:00:00';
select count(*) as matches from tpts where ts = cast('2024-01-01 00:00:00' as timestamp(0)) and ts = cast('2024-01-01 00:00:00' as timestamp(6)) and ts between '2023-01-01 00:00:00' and '2025-01-01 00:00:00';
-- @regex("Table Scan.*selectivity=0[.]0000.*",true)
explain verbose select id from tpts where ts = cast('2024-01-01 00:00:00' as timestamp(0)) and ts = cast('2024-02-01 00:00:00' as timestamp(6)) and ts between '2023-01-01 00:00:00' and '2025-01-01 00:00:00';
select count(*) as matches from tpts where ts = cast('2024-01-01 00:00:00' as timestamp(0)) and ts = cast('2024-02-01 00:00:00' as timestamp(6)) and ts between '2023-01-01 00:00:00' and '2025-01-01 00:00:00';

drop table if exists tpts;
drop table if exists dpts;

-- DECIMAL 宽度元数据（round 6）：运行期十进制比较只按系数与标度对齐取值，
-- DECIMAL(M,D) 的 M（宽度）是声明元数据：同一值按不同宽度拼写是 SQL 相等值，
-- 不得判为矛盾常量假；真正不同的值仍然折叠。
create table wpts(id int primary key, d decimal(16,2));
insert into wpts values (1, 19.00), (2, 81.25), (3, 7.50);
-- @regex("returnStr.*OK",true)
select mo_ctl('dn', 'flush', 'contradictory_selectivity.wpts');
-- @ignore:7,8
analyze table wpts(d) fullscan;
-- @regex("Table Scan.*selectivity=0[.]1111.*",true)
explain verbose select id from wpts where d = cast('19.00' as decimal(8,2)) and d = cast('19.00' as decimal(12,2)) and d between cast('0.00' as decimal(16,2)) and cast('100.00' as decimal(16,2));
select count(*) as matches from wpts where d = cast('19.00' as decimal(8,2)) and d = cast('19.00' as decimal(12,2)) and d between cast('0.00' as decimal(16,2)) and cast('100.00' as decimal(16,2));
-- @regex("Table Scan.*selectivity=0[.]3333.*",true)
explain verbose select id from wpts where d = cast('19.00' as decimal(8,2)) and d = cast('19.00' as decimal(12,2));
select count(*) as matches from wpts where d = cast('19.00' as decimal(8,2)) and d = cast('19.00' as decimal(12,2));
-- @regex("Table Scan.*selectivity=0[.]3333.*",true)
explain verbose select id from wpts where d = cast('19.00' as decimal(8,2));
select count(*) as matches from wpts where d = cast('19.00' as decimal(8,2));
-- 真正不同的 DECIMAL 值（同为标度 2，系数不同）仍然折叠为常量假。
-- @regex("Table Scan.*selectivity=0[.]0000.*",true)
explain verbose select id from wpts where d = cast('19.00' as decimal(8,2)) and d = cast('81.25' as decimal(12,2)) and d between cast('0.00' as decimal(16,2)) and cast('100.00' as decimal(16,2));
select count(*) as matches from wpts where d = cast('19.00' as decimal(8,2)) and d = cast('81.25' as decimal(12,2)) and d between cast('0.00' as decimal(16,2)) and cast('100.00' as decimal(16,2));
-- DECIMAL128 宽度类比：跨宽度同值可满足；真不同值折叠。
-- DECIMAL 归一化拼写来源（round 7）：绑定器只对拼写与规范形不同的十进制字面量
-- （如 019.00 规范化为 19.00）打上 DecimalLiteralRequiresV82 协议标记，该标记
-- 属执行表达式身份，不参与 SQL 值相等：同一值的规范化拼写与普通拼写仍是
-- 可满足的等值对，不得判为矛盾常量假；真正不同的值仍然折叠。
-- @regex("Table Scan.*selectivity=0[.]1111.*",true)
explain verbose select id from wpts where d = 019.00 and d = 19.00 and d between cast('0.00' as decimal(16,2)) and cast('100.00' as decimal(16,2));
select count(*) as matches from wpts where d = 019.00 and d = 19.00 and d between cast('0.00' as decimal(16,2)) and cast('100.00' as decimal(16,2));
-- @regex("Table Scan.*selectivity=0[.]1111.*",true)
explain verbose select id from wpts where d = 19.00 and d = 019.00 and d between cast('0.00' as decimal(16,2)) and cast('100.00' as decimal(16,2));
select count(*) as matches from wpts where d = 19.00 and d = 019.00 and d between cast('0.00' as decimal(16,2)) and cast('100.00' as decimal(16,2));
-- @regex("Table Scan.*selectivity=0[.]3333.*",true)
explain verbose select id from wpts where d = 019.00 and d = 19.00;
select count(*) as matches from wpts where d = 019.00 and d = 19.00;
-- @regex("Table Scan.*selectivity=0[.]3333.*",true)
explain verbose select id from wpts where d = 19.00 and d = 019.00;
select count(*) as matches from wpts where d = 19.00 and d = 019.00;
-- 真正不同的 DECIMAL 值（同一列另一值）仍然折叠为常量假。
-- @regex("Table Scan.*selectivity=0[.]0000.*",true)
explain verbose select id from wpts where d = 019.00 and d = 81.25 and d between cast('0.00' as decimal(16,2)) and cast('100.00' as decimal(16,2));
select count(*) as matches from wpts where d = 019.00 and d = 81.25 and d between cast('0.00' as decimal(16,2)) and cast('100.00' as decimal(16,2));

-- 整数 CAST 声明元数据（round 8）：运行期整数等值比较只按物化载荷逐值比较，
-- 整数 Type.Eq 忽略 Width/Scale：普通字面量 19（Width/Scale 0/0）与折叠后的
-- CAST('19' AS SIGNED)（Width 64/Scale -1）是同一 SQL 值，不得判为矛盾常量假；
-- 真正不同的值仍然折叠。
create table ipts(id int primary key, v bigint);
insert into ipts values (1, 19), (2, 81), (3, 7);
-- @regex("returnStr.*OK",true)
select mo_ctl('dn', 'flush', 'contradictory_selectivity.ipts');
-- @ignore:7,8
analyze table ipts(v) fullscan;
-- @regex("Table Scan.*selectivity=0[.]1111.*",true)
explain verbose select id from ipts where v = 19 and v = cast('19' as signed) and v between 0 and 100;
select count(*) as matches from ipts where v = 19 and v = cast('19' as signed) and v between 0 and 100;
-- @regex("Table Scan.*selectivity=0[.]1111.*",true)
explain verbose select id from ipts where v = cast('19' as signed) and v = 19 and v between 0 and 100;
select count(*) as matches from ipts where v = cast('19' as signed) and v = 19 and v between 0 and 100;
-- @regex("Table Scan.*selectivity=0[.]3333.*",true)
explain verbose select id from ipts where v = 19 and v = cast('19' as signed);
select count(*) as matches from ipts where v = 19 and v = cast('19' as signed);
-- @regex("Table Scan.*selectivity=0[.]3333.*",true)
explain verbose select id from ipts where v = cast('19' as signed) and v = 19;
select count(*) as matches from ipts where v = cast('19' as signed) and v = 19;
-- 真正不同的整数（普通 19 对 SIGNED 81）仍然折叠为常量假。
-- @regex("Table Scan.*selectivity=0[.]0000.*",true)
explain verbose select id from ipts where v = 19 and v = cast('81' as signed) and v between 0 and 100;
select count(*) as matches from ipts where v = 19 and v = cast('81' as signed) and v between 0 and 100;
-- DIV 折叠来源（round 9）：绑定期常量折叠把
-- CAST('19.00' AS DECIMAL(8,2)) DIV 1 物化为 INT64 字面量并在 Literal.Src
-- 保留原表达式；域键必须按物化载荷判定，同一值的两种拼写不得判为矛盾
-- 常量假；真正不同的 DIV 折叠值仍然折叠。
-- @regex("Table Scan.*selectivity=0[.]1111.*",true)
explain verbose select id from ipts where v = 19 and v = (cast('19.00' as decimal(8,2)) div 1) and v between 0 and 100;
select count(*) as matches from ipts where v = 19 and v = (cast('19.00' as decimal(8,2)) div 1) and v between 0 and 100;
-- @regex("Table Scan.*selectivity=0[.]1111.*",true)
explain verbose select id from ipts where (cast('19.00' as decimal(8,2)) div 1) = v and v = 19 and v between 0 and 100;
select count(*) as matches from ipts where (cast('19.00' as decimal(8,2)) div 1) = v and v = 19 and v between 0 and 100;
-- @regex("Table Scan.*selectivity=0[.]3333.*",true)
explain verbose select id from ipts where v = 19 and v = (cast('19.00' as decimal(8,2)) div 1);
select count(*) as matches from ipts where v = 19 and v = (cast('19.00' as decimal(8,2)) div 1);
-- @regex("Table Scan.*selectivity=0[.]3333.*",true)
explain verbose select id from ipts where (cast('19.00' as decimal(8,2)) div 1) = v and v = 19;
select count(*) as matches from ipts where (cast('19.00' as decimal(8,2)) div 1) = v and v = 19;
-- 真正不同的 DIV 折叠值（19 对 81）仍然折叠为常量假。
-- @regex("Table Scan.*selectivity=0[.]0000.*",true)
explain verbose select id from ipts where v = 19 and v = (cast('81.00' as decimal(8,2)) div 1) and v between 0 and 100;
select count(*) as matches from ipts where v = 19 and v = (cast('81.00' as decimal(8,2)) div 1) and v between 0 and 100;

drop table if exists ipts;

-- 浮点 CAST 声明元数据（round 9）：运行期 DOUBLE 等值比较只比较物化载荷
-- （func_compare.go float64 按位比较），FLOAT 等值比较按各操作数自己的声明
-- scale 归一化（Float32ScaleNormalizer）。显式 double(M,D)/float(M,D) 转换携带
-- 的宽度与精度是声明元数据：同一值的跨宽度/跨精度拼写不得判为矛盾常量假；
-- 载荷不同或归一化后不同的拼写仍然是真矛盾，必须折叠。
create table fpts(id int primary key, f double, g float);
insert into fpts values (1, 19, 19.00), (2, 81.5, 81.50), (3, 7.25, 7.25);
-- @regex("returnStr.*OK",true)
select mo_ctl('dn', 'flush', 'contradictory_selectivity.fpts');
-- @ignore:7,8
analyze table fpts(f) fullscan;
-- @ignore:7,8
analyze table fpts(g) fullscan;
-- DOUBLE 跨宽度拼写 19（double(8,2) 对 double(12,2)），两种顺序加覆盖区间。
-- @regex("Table Scan.*selectivity=0[.]1111.*",true)
explain verbose select id from fpts where f = cast('19' as double(8,2)) and f = cast('19' as double(12,2)) and f between 0e0 and 100e0;
select count(*) as matches from fpts where f = cast('19' as double(8,2)) and f = cast('19' as double(12,2)) and f between 0e0 and 100e0;
-- @regex("Table Scan.*selectivity=0[.]1111.*",true)
explain verbose select id from fpts where cast('19' as double(12,2)) = f and f = cast('19' as double(8,2)) and f between 0e0 and 100e0;
select count(*) as matches from fpts where cast('19' as double(12,2)) = f and f = cast('19' as double(8,2)) and f between 0e0 and 100e0;
-- DOUBLE 跨精度拼写 81.5（double(8,1) 对 double(12,2)）加覆盖区间。
-- @regex("Table Scan.*selectivity=0[.]1111.*",true)
explain verbose select id from fpts where f = cast('81.5' as double(8,1)) and f = cast('81.50' as double(12,2)) and f between 0e0 and 100e0;
select count(*) as matches from fpts where f = cast('81.5' as double(8,1)) and f = cast('81.50' as double(12,2)) and f between 0e0 and 100e0;
-- DOUBLE 跨宽度拼写无区间时合并为单一等值。
-- @regex("Table Scan.*selectivity=0[.]3333.*",true)
explain verbose select id from fpts where f = cast('19' as double(8,2)) and f = cast('19' as double(12,2));
select count(*) as matches from fpts where f = cast('19' as double(8,2)) and f = cast('19' as double(12,2));
-- FLOAT 跨宽度拼写 19.00（float(8,2) 对 float(12,2)），两种顺序加覆盖区间。
-- @regex("Table Scan.*selectivity=0[.]1111.*",true)
explain verbose select id from fpts where g = cast('19.00' as float(8,2)) and g = cast('19.00' as float(12,2)) and g between 0e0 and 100e0;
select count(*) as matches from fpts where g = cast('19.00' as float(8,2)) and g = cast('19.00' as float(12,2)) and g between 0e0 and 100e0;
-- @regex("Table Scan.*selectivity=0[.]1111.*",true)
explain verbose select id from fpts where cast('19.00' as float(12,2)) = g and g = cast('19.00' as float(8,2)) and g between 0e0 and 100e0;
select count(*) as matches from fpts where cast('19.00' as float(12,2)) = g and g = cast('19.00' as float(8,2)) and g between 0e0 and 100e0;
-- FLOAT 归一化后同一值的跨 scale 拼写（81.5：float(8,2) 对 float(16,4)）。
-- @regex("Table Scan.*selectivity=0[.]1111.*",true)
explain verbose select id from fpts where g = cast('81.5' as float(8,2)) and g = cast('81.5000' as float(16,4)) and g between 0e0 and 100e0;
select count(*) as matches from fpts where g = cast('81.5' as float(8,2)) and g = cast('81.5000' as float(16,4)) and g between 0e0 and 100e0;
-- FLOAT 跨宽度拼写无区间时合并为单一等值。
-- @regex("Table Scan.*selectivity=0[.]3333.*",true)
explain verbose select id from fpts where g = cast('19.00' as float(8,2)) and g = cast('19.00' as float(12,2));
select count(*) as matches from fpts where g = cast('19.00' as float(8,2)) and g = cast('19.00' as float(12,2));
-- 载荷不同的 DOUBLE 拼写（19 对 81）仍然折叠为常量假。
-- @regex("Table Scan.*selectivity=0[.]0000.*",true)
explain verbose select id from fpts where f = cast('19' as double(8,2)) and f = cast('81' as double(12,2)) and f between 0e0 and 100e0;
select count(*) as matches from fpts where f = cast('19' as double(8,2)) and f = cast('81' as double(12,2)) and f between 0e0 and 100e0;
-- 归一化后不同的 FLOAT 拼写（19.004 在 scale 2 下比较 19.00，scale 3 下比较
-- 19.004）是真矛盾，仍然折叠为常量假。
-- @regex("Table Scan.*selectivity=0[.]0000.*",true)
explain verbose select id from fpts where g = cast('19.004' as float(8,2)) and g = cast('19.004' as float(12,3)) and g between 0e0 and 100e0;
select count(*) as matches from fpts where g = cast('19.004' as float(8,2)) and g = cast('19.004' as float(12,3)) and g between 0e0 and 100e0;
drop table if exists fpts;

create table qpts(id int primary key, e decimal(38,2));
insert into qpts values (1, '19.00'), (2, '9999999999999999999999999999.99');
-- @regex("returnStr.*OK",true)
select mo_ctl('dn', 'flush', 'contradictory_selectivity.qpts');
-- @ignore:7,8
analyze table qpts(e) fullscan;
-- @regex("Table Scan.*selectivity=0[.]2500.*",true)
explain verbose select id from qpts where e = cast('19.00' as decimal(20,2)) and e = cast('19.00' as decimal(30,2)) and e between cast('0.00' as decimal(38,2)) and cast('9999999999999999999999999999.99' as decimal(38,2));
select count(*) as matches from qpts where e = cast('19.00' as decimal(20,2)) and e = cast('19.00' as decimal(30,2)) and e between cast('0.00' as decimal(38,2)) and cast('9999999999999999999999999999.99' as decimal(38,2));
-- @regex("Table Scan.*selectivity=0[.]0000.*",true)
explain verbose select id from qpts where e = cast('19.00' as decimal(20,2)) and e = cast('9999999999999999999999999999.99' as decimal(30,2)) and e between cast('0.00' as decimal(38,2)) and cast('9999999999999999999999999999.99' as decimal(38,2));
select count(*) as matches from qpts where e = cast('19.00' as decimal(20,2)) and e = cast('9999999999999999999999999999.99' as decimal(30,2)) and e between cast('0.00' as decimal(38,2)) and cast('9999999999999999999999999999.99' as decimal(38,2));
drop table if exists qpts;
drop table if exists wpts;

drop table if exists hpts;
drop table if exists bpts;
drop table if exists cpoints;
drop table if exists zpoints;
drop table if exists plain_or_bound;
drop table if exists dig;

drop database if exists contradictory_selectivity;

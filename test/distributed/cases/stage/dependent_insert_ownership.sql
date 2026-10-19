-- @suite
-- @setup
-- 独立 stage、文件前缀与 schema，不依赖其它外部表 case 的状态。
drop database if exists dependent_external_dml;
create database dependent_external_dml;
use dependent_external_dml;
drop stage if exists dependent_ownership_stage;
create stage dependent_ownership_stage url='file:///$resources/into_outfile/stage';
-- DROP DATABASE/STAGE 不删除外部文件；入口恢复此前中断留下的独占前缀。
remove files from stage if exists 'stage://dependent_ownership_stage/dependent_ownership_*.csv';
create table src(id int primary key, label varchar(20));
insert into src values (1, 'one'), (2, 'two');
create external table dst(id int, label varchar(20))
infile{'filepath'='stage://dependent_ownership_stage/dependent_ownership_*.csv', 'format'='csv', 'write_file_pattern'='stage://dependent_ownership_stage/dependent_ownership_%U.csv'}
fields terminated by ',';

-- 主输出必须是 INSERT，而非原 source 或任何 dependent program root。
insert into dst select (select (select s.id)), s.label from src s;
select id, label from dst order by id;
select count(*) from dst;
-- 空 source 不能产生重复写入。
insert into dst select (select (select s.id)), s.label from src s where s.id=3;
select count(*) from dst;
-- 同一 prepared 模板的两代外部写入必须保留 writer 模式。
prepare stmt from 'insert into dst select (select (select s.id+?)), s.label from src s';
set @offset=10;
execute stmt using @offset;
set @offset=20;
execute stmt using @offset;
deallocate prepare stmt;
select id, label from dst order by id;
select count(*) from dst;
-- 成功路径也必须自行清理，不能依赖测试驱动在两轮之间移动文件。
remove files from stage if exists 'stage://dependent_ownership_stage/dependent_ownership_*.csv';
drop database dependent_external_dml;
drop stage dependent_ownership_stage;

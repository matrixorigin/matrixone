#SELECT，多种语言
SELECT CHAR_LENGTH("你好");
SELECT CHAR_LENGTH("français");
SELECT CHAR_LENGTH("にほんご");
SELECT CHAR_LENGTH("Español");

#NULL
SELECT CHAR_LENGTH(NULL);


create table t1 (a char(20));
insert into t1 values ('123456'),('андрей');
select char_length(a), length(a), a from t1 order by a;
drop table t1;

#EXTRME VALUE
select char_length('\n\t\r\b\0\_\%\\');

#嵌套

SELECT CHAR_LENGTH(concat_ws(" ",121,83,81,'76')) as my_column;



create table t (c varchar(20));
insert into t values ('\\');
select char_length(c) from t;
insert into t values ('0C');
select sum(char_length(c)) from t;
insert into t values ('"');
select sum(char_length(c)) from t;
insert into t values ('\a');
select sum(char_length(c)) from t;
insert into t values ('\b');
select sum(char_length(c)) from t;
insert into t values ('\t');
select sum(char_length(c)) from t;
insert into t values ('\n');
select sum(char_length(c)) from t;
insert into t values ('\r');
select sum(char_length(c)) from t;
insert into t values ('10');
select sum(char_length(c)) from t;
drop table t;



#DATATYPE
create table t1(a tinyint, b SMALLINT, c BIGINT, d INT, e BIGINT, f FLOAT, g DOUBLE, h decimal(38,19), i DATE, k datetime, l TIMESTAMP, m char(255), n varchar(255));
insert into t1 values(1, 1, 2, 43, 5, 35.5, 31.133, 14.314, "2012-03-10",  "2012-03-12 10:03:12", "2022-03-12 13:03:12", "ab23c", "d5cf");
insert into t1 values(71, 1, 2, 34, 5, 5.5, 341.13, 15.314, "2012-03-22",  "2013-03-12 10:03:12", "2032-03-12 13:04:12", "abr23c", "3dcf");
insert into t1 values(1, 1, 21, 4, 54, 53.5, 431.13, 14.394, "2011-03-12",  "2015-03-12 10:03:12", "2002-03-12 13:03:12", "afbc", "dct5f");
insert into t1 values(1, 71, 2, 34, 5, 5.5, 31.313, 124.314, "2012-01-12",  "2019-03-12 10:03:12", "2013-03-12 13:03:12", "3abd1c", "dcvf");
select char_length(a),char_length(b),char_length(c),char_length(d),char_length(e),char_length(f),char_length(g),char_length(h),char_length(i),char_length(k),char_length(l),char_length(m),char_length(n) from t1;
drop table t1;

#0.5暂不支持time类型
#create table t1(a time)
#insert into t1 values("10:03:12");
#select char_length(a) from t1;
#drop table t1;


#insert into, distinct
create table t1(a int, b varchar(255));
insert into t1 select char_length("你好"), "你好";
insert into t1 select char_length("再见"), "再见";
select distinct a, char_length(b) from t1;
drop table t1;

#WHERE
drop table if exists t1;
create table t1(a INT,  b varchar(255));
insert into t1 select char_length("你好"), "你好";
insert into t1 select char_length("再见"), "再见";
select * from t1 where char_length(b)=2;
drop table t1;


#ON CONDITION
drop table if exists t1;
drop table if exists t2;
create table t1(a INT,  b varchar(255));
create table t2(a INT,  b varchar(255));
insert into t1 select char_length("你好"), "你好";
insert into t1 select char_length("再见"), "再见";
insert into t2 select char_length("今天"), "日期时间";
insert into t2 select char_length("明天"), "明天";
SELECT t1.a, t2.a FROM t1 JOIN t2 ON (char_length(t1.b) = char_length(t2.b));
drop table t1;
drop table t2;

#HAVING，比较操作
drop table if exists t1;
create table t1(a INT,  b varchar(255));
insert into t1 select char_length("你好"), "你好";
insert into t1 select char_length("再见"), "再见";
select b from t1 group by b having char_length(b)<3;
drop table t1;


#算术操作
SELECT char_length("你好")+char_length("再见");


drop database if exists char_length_test;
create database char_length_test;
use char_length_test;
drop table if exists kb_portal_article_order;
CREATE TABLE kb_portal_article_order (
    id INT AUTO_INCREMENT PRIMARY KEY,
    uniqueCode VARCHAR(50) NOT NULL UNIQUE,
    esId VARCHAR(50),
    originalName VARCHAR(255),
    title VARCHAR(255) NOT NULL,
    spaceId INT NOT NULL,
    folderFullPath VARCHAR(500)
);

drop table if exists kb_portal_article_item;
CREATE TABLE kb_portal_article_item (
    itemId INT AUTO_INCREMENT PRIMARY KEY,
    uniqueCode VARCHAR(50) NOT NULL UNIQUE,
    rtfContent TEXT,
    FOREIGN KEY (uniqueCode) REFERENCES kb_portal_article_order(uniqueCode)
);

INSERT INTO kb_portal_article_order (uniqueCode, esId, originalName, title, spaceId, folderFullPath) VALUES
 ('UC001', 'ES_123', '原始文档1', 'SQL基础教程', 12, '技术/SQL'),
 ('UC002', 'ES_456', '原始文档2', '数据库设计', 12, '技术/数据库');

INSERT INTO kb_portal_article_item (uniqueCode, rtfContent) VALUES
 ('UC001', '本文介绍SQL语句的创建表、插入数据等操作...'), -- 富文本内容示例
 ('UC002', '数据库设计的三范式与ER图详解...');

SELECT
    a.title AS question,
    a.id,
    a.uniqueCode,
    a.esId,
    a.originalName,
    CHAR_LENGTH(i.rtfContent) AS article_length,
    a.spaceId,
    a.folderFullPath AS category,
    '标题' AS data_source
FROM kb_portal_article_order a
LEFT JOIN kb_portal_article_item i ON i.uniqueCode = a.uniqueCode
WHERE a.spaceId = 12;

drop database char_length_test;


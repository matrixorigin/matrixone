-- @suit
-- @cASe
-- @test function group_concat(str,delim,count)
-- @label:bvt


-- @suite
-- @setup
DROP TABLE IF EXISTS group_concat_01;

CREATE TABLE group_concat_01 (grp int,
                              a bigint unsigned,
                              c char(10) NOT NULL,
                              d char(10) NOT NULL);

INSERT INTO group_concat_01 VALUES (1,1,'a','a');
INSERT INTO group_concat_01 VALUES (2,2,'b','a');
INSERT INTO group_concat_01 VALUES (2,3,'c','b');
INSERT INTO group_concat_01 VALUES (3,4,'E','a');
INSERT INTO group_concat_01 VALUES (3,5,'C','b');
INSERT INTO group_concat_01 VALUES (3,6,'D','b');
INSERT INTO group_concat_01 VALUES (3,7,'d','d');
INSERT INTO group_concat_01 VALUES (3,8,'d','d');
INSERT INTO group_concat_01 VALUES (3,9,'D','c');


-- Test of MO simple request
SELECT grp,group_concat(c) FROM group_concat_01 GROUP BY grp;
SELECT grp,group_concat(c) FROM group_concat_01 GROUP BY grp;
SELECT grp,group_concat(a,c) FROM group_concat_01 GROUP BY grp;
SELECT grp,group_concat("(",a,":",c,")") FROM group_concat_01 GROUP BY grp;
SELECT grp,group_concat(NULL) FROM group_concat_01 GROUP BY grp;
SELECT grp,group_concat(a,NULL) FROM group_concat_01 GROUP BY grp;
SELECT group_concat(NULL) FROM group_concat_01;
SELECT group_concat(a,c,NULL) FROM group_concat_01;
SELECT group_concat(a,NULL) FROM group_concat_01;


-- Test of MO with options
SELECT grp,group_concat(c separator ",") FROM group_concat_01 GROUP BY grp;
SELECT grp,group_concat(c separator "---->") FROM group_concat_01 GROUP BY grp;
SELECT grp,group_concat(c ORDER BY c) FROM group_concat_01 GROUP BY grp;
SELECT grp,group_concat(c ORDER BY c DESC) FROM group_concat_01 GROUP BY grp;
SELECT grp,group_concat(d ORDER BY a) FROM group_concat_01 GROUP BY grp;
SELECT grp,group_concat(d ORDER BY a DESC) FROM group_concat_01 GROUP BY grp;
SELECT grp,group_concat(c ORDER BY 1) FROM group_concat_01 GROUP BY grp;
SELECT grp,group_concat(c ORDER BY c separator ",") FROM group_concat_01 GROUP BY grp;
SELECT grp,group_concat(c ORDER BY c DESC separator ",") FROM group_concat_01 GROUP BY grp;
SELECT grp,group_concat(c ORDER BY grp DESC) FROM group_concat_01 GROUP BY grp ORDER BY grp;


-- Test transfer to real values
SELECT grp, group_concat(a separator "")+0 FROM group_concat_01 GROUP BY grp;
SELECT grp, group_concat(a separator "")+0.0 FROM group_concat_01 GROUP BY grp;
SELECT grp, ROUND(group_concat(a separator "")) FROM group_concat_01 GROUP BY grp;


-- Test errors
SELECT group_concat(sum(c)) FROM group_concat_02 group by grp;
SELECT grp,group_concat(c order by 2) FROM group_concat_02 group by grp;


-- @suite
-- @setup
DROP TABLE IF EXISTS group_concat_03;
DROP TABLE IF EXISTS group_concat_04;
CREATE TABLE group_concat_03 ( URL_ID int(11), URL varchar(80));
CREATE TABLE group_concat_04 ( REQ_ID int(11), URL_ID int(11));

INSERT INTO group_concat_03 values (4,'www.host.com');
INSERT INTO group_concat_03 values (5,'www.google.com');
INSERT INTO group_concat_03 values (5,'www.help.com');
INSERT INTO group_concat_04 values (1,4);
INSERT INTO group_concat_04 values (5,4);
INSERT INTO group_concat_04 values (5,5);

SELECT REQ_ID, group_concat(URL) AS URL FROM group_concat_03, group_concat_04 WHERE group_concat_04.URL_ID = group_concat_03.URL_ID group by REQ_ID;


-- @suite
-- @setup
DROP TABLE IF EXISTS group_concat_05;
DROP TABLE IF EXISTS group_concat_06;
CREATE TABLE group_concat_05(id int);
CREATE TABLE group_concat_06(id int);
INSERT INTO group_concat_05 values(0),(1);

-- check zero rows
SELECT group_concat(group_concat_05.id) FROM group_concat_05,group_concat_06;


-- @suite
-- @setup
DROP TABLE IF EXISTS group_concat_07;
CREATE TABLE group_concat_07(bar varchar(32));
INSERT INTO group_concat_07 values('tesgroup_concat_08');
INSERT INTO group_concat_07 values('tesgroup_concat_09');
SELECT group_concat(bar order by concat(bar,bar)) FROM group_concat_07;
SELECT group_concat(bar order by concat(bar,bar) ASC) FROM group_concat_07;

-- Abnormal test
SELECT bar FROM group_concat_07 HAVING group_concat(bar)='';
SELECT bar FROM group_concat_07 HAVING instr(group_concat(bar), "test") > 0;
SELECT bar FROM group_concat_07 HAVING instr(group_concat(bar order by concat(bar,bar) desc), "tesgroup_concat_09,tesgroup_concat_08") > 0;


-- @suite
-- @setup
DROP TABLE IF EXISTS group_concat_08;
DROP TABLE IF EXISTS group_concat_09;

CREATE TABLE group_concat_08 (id1 tinyint(4) NOT NULL, id2 tinyint(4) NOT NULL);
INSERT INTO group_concat_08 VALUES (1, 1);
INSERT INTO group_concat_08 VALUES (1, 2);
INSERT INTO group_concat_08 VALUES (1, 3);
INSERT INTO group_concat_08 VALUES (1, 4);
INSERT INTO group_concat_08 VALUES (1, 5);
INSERT INTO group_concat_08 VALUES (2, 1);
INSERT INTO group_concat_08 VALUES (2, 2);
INSERT INTO group_concat_08 VALUES (2, 3);

CREATE TABLE group_concat_09 (id1 tinyint(4) NOT NULL);
INSERT INTO group_concat_09 VALUES (1);
INSERT INTO group_concat_09 VALUES (2);
INSERT INTO group_concat_09 VALUES (3);
INSERT INTO group_concat_09 VALUES (4);
INSERT INTO group_concat_09 VALUES (5);

SELECT group_concat_08.id1, GROUP_CONCAT(group_concat_08.id2 ORDER BY group_concat_08.id2 ASC) AS concat_id FROM group_concat_08, group_concat_09 WHERE group_concat_08.id1 = group_concat_09.id1 AND group_concat_08.id1=1 GROUP BY group_concat_08.id1;
SELECT group_concat_08.id1, GROUP_CONCAT(group_concat_08.id2 ORDER BY group_concat_08.id2 ASC) AS concat_id FROM group_concat_08, group_concat_09 WHERE group_concat_08.id1 = group_concat_09.id1 GROUP BY group_concat_08.id1;


-- @suite
-- @setup
DROP TABLE IF EXISTS group_concat_10;
CREATE TABLE group_concat_10 (s1 char(10), s2 int not null);
INSERT INTO group_concat_10 values ('a',2);
INSERT INTO group_concat_10 values ('b',2);
INSERT INTO group_concat_10 values ('c',1);
INSERT INTO group_concat_10 values ('a',3);
INSERT INTO group_concat_10 values ('b',4);
INSERT INTO group_concat_10 values ('c',4);

-- distinct
SELECT group_concat(distinct s1) FROM group_concat_10;


-- @suite
-- @setup
DROP TABLE IF EXISTS group_concat_11;
DROP TABLE IF EXISTS group_concat_12;
CREATE TABLE group_concat_11 (a int, c int);
INSERT INTO group_concat_11 values (1, 2);
INSERT INTO group_concat_11 values (2, 3);
INSERT INTO group_concat_11 values (2, 4);
INSERT INTO group_concat_11 values (3, 5);

CREATE TABLE group_concat_12 (a int, c int);
INSERT INTO group_concat_12 values (1, 5);
INSERT INTO group_concat_12 values (2, 4);
INSERT INTO group_concat_12 values (3, 3);
INSERT INTO group_concat_12 values (3, 3);

-- subqueris
SELECT group_concat(c) FROM group_concat_11;
SELECT group_concat_12.a,group_concat_12.c FROM group_concat_12,group_concat_11 where group_concat_12.a=group_concat_11.a;
SELECT group_concat(c order by (SELECT mid(group_concat(c order by a),1,5) FROM group_concat_12 where group_concat_12.a=group_concat_11.a) desc) as grp FROM group_concat_11;


-- @suite
-- @setup
DROP TABLE IF EXISTS group_concat_13;
DROP TABLE IF EXISTS group_concat_14;
CREATE TABLE group_concat_13 ( a int );
CREATE TABLE group_concat_14 ( a int );
INSERT INTO group_concat_13 VALUES (1), (2);
INSERT INTO group_concat_14 VALUES (1), (2);

-- union
SELECT GROUP_CONCAT(group_concat_13.a*group_concat_14.a ORDER BY group_concat_14.a) FROM group_concat_13, group_concat_14 GROUP BY group_concat_13.a;


-- @suite
-- @setup
DROP TABLE IF EXISTS group_concat_15;
CREATE TABLE group_concat_15 (a int, b text);
INSERT INTO group_concat_15 values (1, 'bb');
INSERT INTO group_concat_15 values (1, 'ccc');
INSERT INTO group_concat_15 values (1, 'a');
INSERT INTO group_concat_15 values (1, 'bb');
INSERT INTO group_concat_15 values (1, 'ccc');
INSERT INTO group_concat_15 values (2, 'BB');
INSERT INTO group_concat_15 values (2, 'CCC');
INSERT INTO group_concat_15 values (2, 'A');
INSERT INTO group_concat_15 values (2, 'BB');
INSERT INTO group_concat_15 values (2, 'CCC');

-- join test
SELECT group_concat(b) FROM group_concat_15 group by a;
SELECT group_concat(distinct b) FROM group_concat_15 group by a;
SELECT group_concat(b) FROM group_concat_15 group by a;
SELECT group_concat(distinct b) FROM group_concat_15 group by a;


-- @suite
-- @setup
DROP TABLE IF EXISTS group_concat_16;
DROP TABLE IF EXISTS group_concat_17;
CREATE TABLE group_concat_16 (
                                 aID smallint(5) unsigned NOT NULL auto_increment,
                                 sometitle varchar(255) NOT NULL default '',
                                 bID smallint(5) unsigned NOT NULL,
                                 PRIMARY KEY  (aID),
                                 UNIQUE KEY sometitle (sometitle)
);
INSERT INTO group_concat_16 SET sometitle = 'title1', bID = 1;
INSERT INTO group_concat_16 SET sometitle = 'title2', bID = 1;

CREATE TABLE group_concat_17 (
                                 bID smallint(5) unsigned NOT NULL auto_increment,
                                 somename varchar(255) NOT NULL default '',
                                 PRIMARY KEY  (bID),
                                 UNIQUE KEY somename (somename)
);
INSERT INTO group_concat_17 SET somename = 'test';

-- join
SELECT COUNT(*), GROUP_CONCAT(DISTINCT group_concat_17.somename SEPARATOR ' |')
FROM group_concat_16 JOIN group_concat_17 ON group_concat_16.bID = group_concat_17.bID;
INSERT INTO group_concat_17 SET somename = 'tesgroup_concat_17';
SELECT COUNT(*), GROUP_CONCAT(DISTINCT group_concat_17.somename SEPARATOR ' |')
FROM group_concat_16 JOIN group_concat_17 ON group_concat_16.bID = group_concat_17.bID;
DELETE FROM group_concat_17 WHERE somename = 'tesgroup_concat_17';
SELECT COUNT(*), GROUP_CONCAT(DISTINCT group_concat_17.somename SEPARATOR ' |')
FROM group_concat_16 JOIN group_concat_17 ON group_concat_16.bID = group_concat_17.bID;

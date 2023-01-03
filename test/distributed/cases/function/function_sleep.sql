-- @suit
-- @case
-- @desc:test for sleep() function
-- @label:bvt

SELECT sleep(0);
SELECT sleep(3);
SELECT sleep(4.33);

-- parameter error
SELECT sleep(-2);
SELECT sleep(-23.1);

-- @suite
-- @setup
DROP TABLE IF EXISTS sleep_01;
CREATE TABLE sleep_01 (id int);

-- Empty table
SELECT * ,sleep(2) from sleep_01;

-- One record
INSERT INTO sleep_01 VALUES(273);
SELECT *, sleep(2) FROM sleep_01;
SELECT *, sleep(0) FROM sleep_01;

-- two records
INSERT INTO sleep_01 VALUES(-32783);

-- @bvt:issue#7367
SELECT *,sleep(2) FROM sleep_01;
SELECT *,sleep(0) FROM sleep_01;
-- @bvt:issue

-- three records
INSERT INTO sleep_01 VALUES(0);

-- @bvt:issue#7367
SELECT * ,sleep(2) FROM sleep_01;
-- @bvt:issue

SELECT *, sleep(0) FROM sleep_01;

-- @suite
-- @setup
DROP TABLE IF EXISTS sleep_02;
CREATE TABLE sleep_02 (id int, name VARCHAR(10), province VARCHAR(100) NOT NULL, address VARCHAR(100));
INSERT INTO sleep_02 VALUES(1, '张三', '陕西', '大头村二组');
INSERT INTO sleep_02 VALUES(2, '李四', '北京', '快乐村三组');
INSERT INTO sleep_02 VALUES(3, '王五', '陕西', '美丽村四组');


SELECT province, address, sleep(2) FROM sleep_02 WHERE name = '张三';
SELECT name, sleep(3) FROM sleep_02 WHERE address = '大头村二组' AND province = '陕西';
SELECT *, sleep(2) FROM sleep_02 WHERE id = COS(0) + TAN(45);

-- @bvt:issue#7367
SELECT name, province, sleep(2) FROM sleep_02;
-- @bvt:issue


-- @suite
-- @setup
DROP TABLE IF EXISTS sleep_03;
DROP TABLE IF EXISTS sleep_04;
CREATE TABLE sleep_03(d INT,d1 VARCHAR(20), d2 BIGINT,PRIMARY KEY (d));
CREATE TABLE sleep_04( d INT,d1 CHAR(20),d2 DATE,PRIMARY KEY (d));
INSERT INTO sleep_03 VALUES (1,'lijklnfdsalj',19290988);
INSERT INTO sleep_03 VALUES (2,'xlziblkfdi',1949100132);
INSERT INTO sleep_03 VALUES (3,'ixioklakmaria',69456486);
INSERT INTO sleep_03 VALUES (4,'brzilaiusd',6448781575);

INSERT INTO sleep_04 VALUES (1,'usaisagoodnat','1970-01-02');
INSERT INTO sleep_04 VALUES (2,'chanialfakbjap','1971-11-12');
INSERT INTO sleep_04 VALUES (3,'indiaisashit','1972-09-09');
INSERT INTO sleep_04 VALUES (4,'xingoporelka','1973-12-07');

SELECT sleep_03.d, sleep(0) FROM sleep_03,sleep_04 WHERE sleep_03.d = sleep_04.d;

-- @bvt:issue#7367
SELECT sleep_03.d, sleep(3.1) FROM sleep_03;
SELECT sleep_03.d, sleep_03.d1, sleep(4) FROM sleep_03 JOIN sleep_04 ON sleep_03.d = sleep_04.d;
SELECT sleep_03.d, sleep(1) FROM sleep_03 join sleep_04 on sleep_03.d=sleep_04.d;
SELECT sleep_03.d, sleep(1.432)FROM sleep_03 right join sleep_04 on sleep_03.d=sleep_04.d;
SELECT d, sleep(5.0) FROM sleep_03 ORDER BY d2 desc;
-- @bvt:issue

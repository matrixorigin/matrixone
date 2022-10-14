-- @suit
-- @case
-- @desc:test for SHOW TABLE STATUS [{FROM | IN }]...
-- @label:bvt

-- Empty Set if the databases has no table
SHOW TABLE STATUS;
DROP TABLE IF EXISTS t;
DROP TABLE IF EXISTS t1;
CREATE TABLE t(
	id INT NOT NULL DEFAULT 0,
	name CHAR(20) NOT NULL,
	PRIMARY KEY (id)
);
CREATE TABLE t1(
	cid FLOAT NOT NULL,
	cls VARCHAR(20) NOT NULL,
	PRIMARY KEY(cid, cls)
);
SHOW TABLE STATUS LIKE 't' \G;
SHOW TABLE STATUS LIKE 't%' \G;

-- for item 'Create_time', check it whether time_zone related.
SHOW VARIABLES LIKE '%time_zone%';
--SET time_zone = '+07:00';
DROP TABLE IF EXISTS t;
DROP TABLE IF EXISTS t1;
CREATE TABLE t(
	id CHAR(200) NOT NULL DEFAULT 'FLJDIAOJFDKLA',
	name CHAR(200) NOT NULL,
	PRIMARY KEY (id)
);
CREATE TABLE t1(
	cid FLOAT NOT NULL,
	cls VARCHAR(200) NOT NULL,
	PRIMARY KEY(cid, cls)
);
INSERT INTO t VALUES ('lijljidlsajfdklsafjd', 'fijiofjdizvckxzjvico');
INSERT INTO t VALUES ('&(*^&*T&YD(SF*(DUS(*F', '0');
INSERT INTO t1 VALUES (1.101, 'I have a dream that one day this nation will rise up');
INSERT INTO t1 VALUES (0.1, 'wo will never surender dukerke is a');
-- according to return value, item 'Create_time' is time_zone related.
SHOW TABLE STATUS LIKE 't_' \G;
SHOW TABLE STATUS FROM test \G;

-- @bvt:issue#5548
SHOW TABLE STATUS FROM test WHERE Name = 't' \G;
-- @bvt:issue

DROP TABLE IF EXISTS t;
DROP TABLE IF EXISTS t1;
CREATE TABLE t(
	tiny TINYINT NOT NULL,
	small SMALLINT NOT NULL,
	int_t INT NOT NULL,
	big BIGINT NOT NULL,
	str1 VARCHAR(200),
	str2 CHAR(200),
	float_32 FLOAT,
	float_64 DOUBLE,
	d1 DATE,
	d2 DATETIME,
	d3 TIMESTAMP,
	PRIMARY KEY (tiny, str1, d1)
);
SHOW TABLE STATUS LIKE 't_' \G;

DROP TABLE IF EXISTS t;
CREATE TABLE t(
	tiny TINYINT,
	small SMALLINT,
	int_t INT,
	big BIGINT,
	str1 VARCHAR(200),
	str2 CHAR(200),
	float_32 FLOAT,
	float_64 DOUBLE,
	d1 DATE,
	d2 DATETIME,
	d3 TIMESTAMP
);
INSERT INTO t(str1,str2) VALUES ('jixlzvcxfiowurioewmnklrn', 'abc');
SHOW TABLE STATUS LIKE NULL \G;
SHOW TABLE STATUS LIKE 'NULL' \G;
SHOW TABLE STATUS LIKE '_' \G;
SHOW TABLE STATUS LIKE 't*' \G;

-- @bvt:issue#5557
SHOW TABLE STATUS LIKE 't' \G;
-- @bvt:issue

-- @bvt:issue#5556
SHOW TABLE STATUS LIKE '*' \G;
-- @bvt:issue

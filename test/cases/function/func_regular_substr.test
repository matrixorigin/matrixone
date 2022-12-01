SELECT REGEXP_SUBSTR('Thailand or Cambodia', 'l.nd') Result;

SELECT REGEXP_SUBSTR('Lend for land', '^C') Result;

SELECT REGEXP_SUBSTR('Cat Cut Cot', 'C.t', 2) Result;

SELECT REGEXP_SUBSTR('Cat Cut Cot', 'C.t', 1, 1) 'Occurrence1';
SELECT REGEXP_SUBSTR('Cat Cut Cot', 'C.t', 1, 2) 'Occurrence2';
SELECT REGEXP_SUBSTR('Cat Cut Cot', 'C.t', 1, 3) 'Occurrence3';

SELECT REGEXP_SUBSTR('Cat Cut Cot', 'C.t', 2, 1) 'Occurrence1';
SELECT REGEXP_SUBSTR('Cat Cut Cot', 'C.t', 2, 2) 'Occurrence2';
SELECT REGEXP_SUBSTR('Cat Cut Cot', 'C.t', 2, 3) 'Occurrence3';

SELECT REGEXP_SUBSTR(NULL, 'C.t', 2, 1);
SELECT REGEXP_SUBSTR('Cat Cut Cot', NULL, 2, 1);
SELECT REGEXP_SUBSTR(NULL, NULL, 2, 1);
SELECT REGEXP_SUBSTR('Cat Cut Cot', "", 2, 1);

create table t1(a int, b varchar(100));
insert into t1 values(1 , "PowerSlave");
insert into t1 values(2 , "Powerage");
insert into t1 values( 3 , "Singing Down the Lane" );
insert into t1 values(4 , "Ziltoid the Omniscient");
insert into t1 values(5 , "Casualties of Cool");
insert into t1 values( 6 , "Epicloud");
insert into t1 values(7 , "Somewhere in Time");
insert into t1 values(8 , "Piece of Mind");
SELECT a, REGEXP_SUBSTR(b, '.i') from t1;
drop table t1;

create table t1(a int, b varchar(100));
insert into t1 values(1 , "PowerSlave");
insert into t1 values(2 , "Powerage");
insert into t1 values( 3 , "Singing Down the Lane" );
insert into t1 values(4 , "Ziltoid the Omniscient");
insert into t1 values(5 , "Casualties of Cool");
insert into t1 values( 6 , "Epicloud");
insert into t1 values(7 , "Somewhere in Time");
insert into t1 values(8 , "Piece of Mind");
SELECT a, REGEXP_SUBSTR(b, '.i', 3) from t1;
drop table t1;

create table t1(a int, b varchar(100));
insert into t1 values(1 , "PowerSlave");
insert into t1 values(2 , "Powerage");
insert into t1 values( 3 , "Singing Down the Lane" );
insert into t1 values(4 , "Ziltoid the Omniscient");
insert into t1 values(5 , "Casualties of Cool");
insert into t1 values( 6 , "Epicloud");
insert into t1 values(7 , "Somewhere in Time");
insert into t1 values(8 , "Piece of Mind");
SELECT a, REGEXP_SUBSTR(b, '.i', 3, 2) from t1;
drop table t1;

DROP TABLE IF EXISTS t;
CREATE TABLE t(str1 VARCHAR(20), str2 CHAR(20));
INSERT INTO t VALUES ('W * P', 'W + Z - O'), ('have has having', 'do does doing');
INSERT INTO t VALUES ('XV*XZ', 'PP-ZZ-DXA'), ('aa bbb cc ddd', 'k ii lll oooo');
SELECT REGEXP_SUBSTR(str1, '*'), REGEXP_SUBSTR(str2,'hav','hiv') FROM t;
drop table t;
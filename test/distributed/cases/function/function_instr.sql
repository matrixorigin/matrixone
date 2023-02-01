-- @suit
-- @case
-- @test function INSTR(str,SUBSTR)
-- @label:bvt

SELECT INSTR('ejwnqke','wn');
SELECT INSTR('wn','ejwnqke');
SELECT INSTR('hvjdke3qj','a');
SELECT INSTR('edhjw 38902&A**',' ');
SELECT INSTR('reuwYHWJMQ781///-+++','fe3232');
SELECT INSTR('','');
SELECT INSTR('','ehwj32');
SELECT INSTR('251625%$#@*(ejf2f32f','');
SELECT INSTR(NULL,'ewqe');
SELECT INSTR('24e8w7/*37289',NULL);
SELECT INSTR(NULL,NULL);


-- @suite
-- @setup 
DROP TABLE IF EXISTS instr_01;
CREATE TABLE instr_01(id int,
					str1 CHAR,
					str2 VARCHAR(30),
					PRIMARY KEY(id));


INSERT INTO instr_01 VALUES(1, 'a', 'dhehw');
INSERT INTO instr_01 VALUES(2, '2', '372890932');
INSERT INTO instr_01 VALUES(3, '*', 'fejkj4kl332342');
INSERT INTO instr_01 VALUES(4, '-', ' 4ej348324*&&^*&*--++');
INSERT INTO instr_01 VALUES(5, '.', '3h2h3kj2*()____  ))() ');
INSERT INTO instr_01 VALUES(6, '', NULL);
INSERT INTO instr_01 VALUES(7, NULL, '');
INSERT INTO instr_01 VALUES(8, NULL, NULL);


SELECT INSTR(str2,str1) FROM instr_01;
SELECT INSTR(str2,'') FROM instr_01 WHERE str2 IS NOT NULL;
SELECT INSTR('',str1) FROM instr_01;
SELECT INSTR(NULL, str2) FROM instr_01;
SELECT INSTR(str1, NULL) FROM instr_01;


-- -- Nested with string functions
SELECT concat_ws('-',INSTR(str2,str1),INSTR('abc','a')) FROM instr_01;
SELECT find_in_set(INSTR(str2,str1),'hdkewqjfew-') FROM instr_01 WHERE id = 4;
SELECT oct(INSTR(str2,str1)) FROM instr_01 WHERE id = 4;
SELECT empty(instr(str2,str1)) FROM instr_01;
SELECT LENGTH(INSTR(str2,str1)) FROM instr_01;
SELECT INSTR(LTRIM(str2),'*&&^*&') FROM instr_01 WHERE id = 4;
SELECT INSTR(RTRIM(str2),'()____  )') FROM instr_01 WHERE str1 = '.'; 
SELECT LPAD(INSTR(str2, str1), 10, '-') FROM instr_01;
SELECT RPAD(INSTR(str2, 3), 5, '***') FROM instr_01;
SELECT substring(instr(str1,'a'),'321421') FROM instr_01;
SELECT INSTR(REVERSE(str2), ' ') FROM instr_01;
SELECT bin(INSTR(str1, 'a')) FROM instr_01 WHERE id = 1;
SELECT hex(INSTR(str2, 'w')) FROM instr_01 WHERE id = 1;


-- @suite
-- @setup 
DROP TABLE IF EXISTS instr_02;
CREATE TABLE instr_02(id int,
					str1 mediumtext,
					str2 VARCHAR(30) NOT NULL);
	
	
INSERT INTO instr_02 VALUES(1, '今天是很美好的一天 Today is a wonderful day!!!', '美好');
INSERT INTO instr_02 VALUES(2, '4**-1+83982j4mfkerwvuh43oij3f42j4iuu32oi4ejf32j432YUDINWKJ<DJ>>A>欢迎使用mo','');
INSERT INTO instr_02 VALUES(3, '', 'gchjewqhedjw');
INSERT INTO instr_02 VALUES(4, '', '');
INSERT INTO instr_02 VALUES(5, NULL,'abcd');
INSERT INTO instr_02 VALUES(6, '   ewfew3324   ed_+_+  ', 'ew');


SELECT * FROM instr_02 WHERE INSTR(str1,str2) = 5;		
SELECT INSTR(str1, str2) FROM instr_02; 	
SELECT INSTR(str2, str1) FROM instr_02;	


-- Nested with string functions
SELECT concat_ws('-',INSTR(str2,str1),INSTR('abc','a')) FROM instr_02;
SELECT find_in_set(INSTR(str2,str1),'hdkewqjfew-') FROM instr_02 WHERE id = 4;
SELECT oct(INSTR(str2,str1)) FROM instr_02 WHERE id = 4;
SELECT empty(instr(str2,str1)) FROM instr_02;
SELECT LENGTH(INSTR(str2,str1)) FROM instr_02;
SELECT INSTR(LTRIM(str1),'ed_+_+') FROM instr_02 WHERE id = 6;
SELECT INSTR(RTRIM(str1),'3324') FROM instr_02 WHERE id = 6; 
SELECT LPAD(INSTR(str2, str1), 6, 'abc') FROM instr_02;
SELECT RPAD(INSTR(str2, 3), 5, '') FROM instr_02;
SELECT INSTR(substring(str2, 1, 6), 'cd') FROM instr_02 WHERE id = 5;
SELECT INSTR(REVERSE(str1), '用使') FROM instr_02 WHERE id = 2;
SELECT bin(INSTR(str2, 'd')) FROM instr_02 WHERE id = 3;
SELECT hex(INSTR(str1, 'ed')) FROM instr_02 WHERE id = 6;

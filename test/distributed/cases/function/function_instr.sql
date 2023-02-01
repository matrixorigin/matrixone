-- @suit
-- @case
-- @test function INSTR(str,SUBSTR)
-- @label:bvt

SELECT INSTR('ejwnqke','wn');
SELECT INSTR('wn','ejwnqke');
SELECT INSTR('hvjdke3qj','a');
SELECT INSTR('今天是晴天ok.are yioeore;wmv','晴天');
SELECT INSTR('ewhihjreiwhvrejw8344332￥#……@#@￥#@￥DSCSVRERGEWvefw','');
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
SELECT * FROM instr_01 WHERE instr(str2,'qke') = 5;
SELECT id,str1,str2 FROM instr_01 WHERE instr(str2, '32') = NULL;


-- subquries
SELECT * FROM instr_01 WHERE id = (SELECT id FROM instr_01 WHERE instr(str2,'qke') = 5);
SELECT(SELECT str2 FROM instr_01 WHERE instr(str1,'a') = 1),id FROM instr_01;
SELECT id ,str1, str2 FROM instr_01 WHERE id = (SELECT id FROM instr_01 WHERE instr(str2,'aaa') = NULL);


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
SELECT * FROM instr_02 WHERE id = (SELECT id FROM instr_02 WHERE INSTR(str1,'+8') = 6);
SELECT(SELECT str2 FROM instr_02 WHERE instr(str1,'a') = 1),id FROM instr_02;


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



-- @suite
-- @setup
DROP TABLE IF EXISTS instr_03;
DROP TABLE IF EXISTS instr_04;
CREATE TABLE instr_03(
	id int,
    d1 CHAR,
    str1 VARCHAR(50),
    primary key (id));
	
CREATE TABLE instr_04(
    id int,
	str1 mediumtext NOT NULL,
    primary key (id));


INSERT INTO instr_03 VALUES(1, 'a', 'zheshimeihaodeyitian,这是美好的一天');
INSERT INTO instr_03 VALUES(2, '*', '明天更美好ehwqknjcw*^*qk67329&&*');
INSERT INTO instr_03 VALUES(3, NULL, 'ewgu278wd-+ABNJDSK');
INSERT INTO instr_03 VALUES(4, '', NULL);

INSERT INTO instr_04 VALUES(1, '盼望着,盼望着,东风来了,春天的脚步近了。 一切都像刚睡醒的样子,欣欣然张开了眼。山朗润起来了,水涨 起来了,太阳的脸红起来了。 小草偷偷地从土里钻出来，Choose to Be Alone on Purpose Here we are, all by ourselves, all 22 million of us by recent count, alone in our rooms');
INSERT INTO instr_04 VALUES(2, 'zheshimeihaodeyitian,这是美好的一天');
INSERT INTO instr_04 VALUES(3, 'ewgu278wd-+ABNJDSK');
INSERT INTO instr_04 VALUES(4, 'hey32983..........,,');


SELECT * FROM instr_03 WHERE id = (SELECT id FROM instr_04 WHERE INSTR(str1,'u27'));
SELECT instr_03.id AS id_3,instr_03.id AS id_4 FROM instr_03,instr_04 WHERE INSTR(instr_03.str1,'shi') = INSTR(instr_04.str1,'美好');
SELECT instr_03.str1 AS str1_3,instr_03.str1 FROM instr_03,instr_04 WHERE INSTR(instr_03.str1,'meihao') = INSTR(instr_04.str1,'meihao');
SELECT INSTR(instr_03.str1, 'ABNJDSK') FROM instr_03,instr_04 WHERE instr_03.str1 = instr_04.str1;
SELECT * FROM instr_03 WHERE str1 = (SELECT str1 FROM instr_04 WHERE INSTR(str1,'ABNJDSK') = 12);


-- join 
SELECT INSTR(instr_03.str1, 'zheshi')AS tmp, instr_04.str1 AS temp FROM instr_03 join instr_04 ON instr_03.str1 = instr_04.str1;
SELECT instr_03.id AS id_3,instr_04.id AS id_4 FROM instr_03 left join instr_04 ON instr_03;

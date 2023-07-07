-- @suit
-- @case
-- @desc:test for REPLACE() function
-- @label:bvt

select replace("XYZ FGH XYZ","X","M");

select replace(".*.*.*",".*","1");

select replace("11","","1");

drop table if exists t1;
create table t1(a varchar,b varchar);
insert into t1 values("testtest","case case");

select a,replace(b,"case","11") from t1;

select replace(a,"test","22") from t1;

select replace(a,"test",22),replace(b,"case","11") from t1;


-- character
SELECT REPLACE('aaaa','a','b');
SELECT REPLACE('aaaa','aa','b');
SELECT REPLACE('aaaa','a','bb');
SELECT REPLACE('aaaa','','b');
SELECT REPLACE('bbbb','a','c');
SELECT REPLACE(' hhdjs','','C');
SELECT REPLACE('absdefg','b',' ');
SELECT REPLACE('rhjewnjfnkljvmkrewrjj','j','');
SELECT REPLACE('http://www.google.com.cn/','/','');
SELECT REPLACE('aaa.mysql.com','a','w');
SELECT REPLACE('abchfjwabcABCvshfjdrvoiewjvkmdns mndsabcAbcueiuiwqjemkvwme000','abc','def');
SELECT REPLACE('dhnnnnjjkwkmoHskdhwjnejwqeHsk382983ndjhsemcsHskuwin mndsHsk','Hsk','t12vdjeke');
SELECT REPLACE('jewTWUHWJWwwwwww', 'w', 'Q');
SELECT REPLACE(' woshincjwne hjw ',' ','C');
SELECT REPLACE(' 2617hejwnc 3920kdsm 3289uj ',' ','ssh');


-- number
SELECT REPLACE(123456,6,1);
SELECT REPLACE(NULL,'', '');
SELECT REPLACE('', NULL, 3);
SELECT REPLACE(378273,2,NULL);


-- Chinese
SELECT REPLACE('我是谁我在哪里', '我', '你');
SELECT REPLACE('老师说我是好孩子', '你', '我');
SELECT REPLACE(NULL, '哈', '哈');
SELECT REPLACE('我是老大', '', '');
SELECT REPLACE('你好，见到你很高兴',NULL,'你');
SELECT REPLACE('哈哈哈哈哈', '哈', NULL);
SELECT REPLACE('今天是个好日子，明天也是个好日子，后天也是个好日子，大后天也是个好日子','后天','前天');
SELECT REPLACE(' 老师说明天带我们去春游，后天带我们去植树   ,五一带我们去博物馆参观  ',' ','Alice');
SELECT REPLACE('  abc老师说我们班的语文成绩是全年级第1数学成绩是全年级第2', '全年级', '金台区');


-- null
SELECT REPLACE( 'a', 'b', NULL );
SELECT REPLACE( 'a', '', NULL );
SELECT REPLACE('','',NULL);
SELECT REPLACE( NULL, 'b', 'bravo' );
SELECT REPLACE( NULL, '', 'bravo' );
SELECT REPLACE( 'a', NULL, 'bravo' );
SELECT REPLACE( NULL, NULL, 'bravo' );


-- Special characters
SELECT REPLACE('37829((&^&8', '(', ')');
SELECT REPLACE('dnu2@#$%^&(*()____**&^%', '*', '');
SELECT REPLACE('7j^$&**JWI*@(@', '@', '@@@@@@')
SELECT REPLACE(NULL, 'a', '37*&');
SELECT REPLACE('45e&^%$', NULL, 'abc');
SELECT REPLACE('782hfe4nijned,.,/.', '78', NULL);
SELECT REPLACE(NULL, NULL,'') / 2;


-- @suite
-- @setup
DROP TABLE IF EXISTS replace_01;
CREATE TABLE replace_01(
                           s_id int(20) NOT NULL AUTO_INCREMENT,
                           s_name CHAR(5) DEFAULT NULL COMMENT '姓名',
                           phone VARCHAR(11) DEFAULT NULL COMMENT '电话',
                           PRIMARY KEY (s_id)
);

INSERT INTO replace_01 VALUES (1, 'Tom', '13603735566');
INSERT INTO replace_01 VALUES (2, 'Lee', '13603735533');
INSERT INTO replace_01 VALUES (3, 'Harry', '13603735544');
INSERT INTO replace_01 VALUES (4, 'Odin', '13603735577');
INSERT INTO replace_01 VALUES (5, 'Jack', '13603735587');

SELECT s_id, s_name, REPLACE(phone,'136','158') FROM replace_01;
SELECT * FROM replace_01 WHERE REPLACE(s_name, 'Tom', 'Bob') = 'Bob';
SELECT REPLACE(s_name,'a','ahfjse') FROM replace_01;
SELECT s_id, s_name FROM replace_01 WHERE s_name = REPLACE('s_name','Jack','Bob');
SELECT REPLACE(phone, '136', NULL) FROM replace_01;
SELECT REPLACE(NULL, 'q','p') FROM replace_01;
SELECT REPLACE(s_name,'Tom','newqjndwdewmwve')FROM replace_01;
SELECT REPLACE(s_name, s_name, 'zhang') FROM replace_01;


-- Abnormal test:The length after replacement exceeds the original defined size
INSERT INTO replace_01 VALUES(6, 'HHANjdncd','445478855');
INSERT INTO replace_01 VALUES(7, 'Wl', '11111124841550');
UPDATE replace_01 set s_name = REPLACE(s_name,'Tom','efjhhoiwuwnvnjwiewori');


-- like, not like
SELECT s_name, phone FROM replace_01 WHERE REPLACE(s_name, 'Tom', 'Bo') LIKE 'B%';
SELECT * FROM replace_01 WHERE REPLACE(s_name, 'Tom', 'Bo') NOT LIKE 'B%';


-- between and, not between and
SELECT s_id, s_name FROM replace_01 WHERE REPLACE(s_id, 1, 6) BETWEEN 4 AND 7;
SELECT * FROM replace_01 WHERE REPLACE(s_id, 2, 8) NOT BETWEEN 3 AND 6;


-- is null, is not null
SELECT * FROM replace_01 WHERE REPLACE('s_name','Lee',NULL) IS NULL;
SELECT * FROM replace_01 WHERE REPLACE('s_name','Lee','Vicky') IS NOT NULL;

-- >,<,>=,<=,!=
SELECT * FROM replace_01 WHERE REPLACE(s_id, 1, 10) <4;
SELECT s_id, s_name FROM replace_01 WHERE REPLACE(s_id, 2, 9) > 2;
SELECT * FROM replace_01 WHERE REPLACE(s_id, 5, 9) >=5;
SELECT s_id, s_name FROM replace_01 WHERE REPLACE(s_id, 2, 9) != 6;
SELECT * FROM replace_01 WHERE REPLACE(s_id, 5, 9) <=5;


-- @suite
-- @setup
DROP TABLE IF EXISTS replace_02;
CREATE TABLE replace_02(
                           id int,
                           name VARCHAR(10),
                           password varchar(32),
                           mm bigint,
                           PRIMARY KEY(id)
);

INSERT INTO replace_02 VALUES(1, ' 张三', '3672*(*^&hu32', 100);
INSERT INTO replace_02 VALUES(2, '李四 ', '4545dwjkekwe&&&&', 200);
INSERT INTO replace_02 VALUES(3, ' 王五 ', '&&%$%^&^*()))', 300);
INSERT INTO replace_02 VALUES(4, '赵六', 'ewu8qu39821dijoq', 400);
INSERT INTO replace_02 VALUES(5, '钱七', '--2102-9328', 500);
INSERT INTO replace_02 VALUES(6, ' ', ' ', 900);


-- data update
UPDATE replace_02 set name = REPLACE(name,'张三','大哥');
UPDATE replace_02 set password = REPLACE(password, '&&&&', 'AAAA'), name = REPLACE(name,'李四','小栗子');
SELECT REPLACE(mm,mm,600) FROM replace_02;


-- cases:String
SELECT REPLACE(ltrim(name), '张三', '三张') FROM replace_02;
SELECT REPLACE(rtrim(name), '李', '栗') FROM replace_02;
SELECT LENGTH(REPLACE(password, 'a', 'b')) FROM replace_02;
SELECT ltrim(REPLACE('nhfej', 'fe', ' fe'));
SELECT rtrim(REPLACE('ejdwj 3782 ', '37', '2222'));
SELECT find_in_set('b',(REPLACE('a,b,c','a','b')));
SELECT substring(REPLACE(name,'钱七','钱八'),0) FROM replace_02;
SELECT bin(REPLACE(id,1,4)) FROM replace_02;
SELECT hex(REPLACE(id, 2, 8)) FROM replace_02;
SELECT RPAD(REPLACE(password,'a','b'),40,'+-') FROM replace_02;
SELECT REPLACE(id, 2, 10) AS newid FROM replace_02 ORDER BY newid DESC;


-- @suite
-- @setup
-- date function
DROP TABLE IF EXISTS replace_04;
CREATE TABLE replace_04(
                           id INT,
                           dd1 DATE,
                           dd2 DATETIME NOT NULL,
                           dd3 TIMESTAMP,
                           PRIMARY KEY (id)
);

INSERT INTO replace_04 VALUES (1, '2020-01-01', '2020-01-01 12:12:12', '2020-02-02 06:06:06.163');
INSERT INTO replace_04 VALUES (2, '2021-11-11', '2021-01-11 23:23:23', '2021-12-12 16:16:16.843');
INSERT INTO replace_04 VALUES (3, '2002-11-11', '2002-01-11 23:23:23', '2002-12-12 16:16:16.843');
INSERT INTO replace_04 VALUES (4, '2023-01-04', '1998-01-09 11:10:56', NULL);


-- date function cases
SELECT * FROM replace_04 WHERE REPLACE(dd1,'20','21') < '2020-01-01';
SELECT dd3 FROM replace_04 WHERE REPLACE(dd1, '20', '21') > '2020-01-01';
SELECT MONTH(REPLACE(dd1, '20', '21')) FROM replace_04;
SELECT year(REPLACE(dd2, 12, 20)) FROM replace_04;
SELECT day(REPLACE(dd2, '23:23', '10:10')) , dd3 FROM replace_04;
SELECT DATE_FORMAT(REPLACE(dd1, '20', '21'),'%m-%d-%Y') FROM replace_04;
SELECT DATE(REPLACE(dd2, 12, 20)) FROM replace_04;
SELECT to_date(REPLACE(dd2, '23:23', '10:10'),'%Y-%m-%d %H:%i:%s') FROM replace_04;
SELECT weekday(REPLACE(dd2, 12, 20)) FROM replace_04;
SELECT dayofyear(REPLACE(dd1, 20, 21)) FROM replace_04;
SELECT id, extract(year FROM REPLACE(dd3, 20, 18)) FROM replace_04;
SELECT id, extract(MONTH FROM REPLACE(dd3, 20, 18)) FROM replace_04;

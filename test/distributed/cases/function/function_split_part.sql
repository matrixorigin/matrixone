-- @suit
-- @case
-- @test function split_part(str,delim,count)
-- @label:bvt

SELECT split_part('abc^123^ioo','^',1);
SELECT split_part('efjq34','4r832r432094-3',2);
SELECT split_part('床前明月光，疑是地上霜，举头望明月，低头思故乡','，',4);
SELECT split_part('jewkrje的jdksvfe32893**(的——++））（）（  的', '的', 3);
SELECT split_part('fhew嗯嗯圣诞节chejwk嗯嗯__++__w嗯嗯','嗯嗯',2);
SELECT split_part('v23dnnr###ewjrfkjewm#vrewnvrenjvnewmvrdjvrnjerewmvrjenjwvewmvrrnenjvrenjvrejnvewvrevrjewvrnew','ewmvr',8);
SELECT split_part('www.baidu.com','.',1);
SELECT split_part('43728943902493-24fjk43nmfjkwek432','3',3);
SELECT split_part('ABC*123*()(','*',2);
SELECT split_part('12345*&+789*&dhejwfew2','*&',2);
SELECT split_part('.+0___=+. ','.',1);
SELECT split_part('..','.',1);
SELECT split_part('  ewfere..  ',' ',4);
SELECT split_part('','327832',1);
SELECT split_part(NULL,'.',6);
SELECT split_part('-+0988   &^88?/7!@~~~~',NULL,3);
SELECT split_part('efwjkfe&*&**(*))))','*',NULL);


-- Abnormal test
SELECT split_part('dfjwkfrewfr','r',0);
SELECT split_part('ejwkvr&&(()))___hf真假ejw真假)','真假',-1);


-- @suite
-- @setup
DROP TABLE IF EXISTS split_part_01;
CREATE TABLE split_part_01(id int,
                           s1 VARCHAR(100),
                           delim VARCHAR(20),
                           count1 smallint,
                           PRIMARY KEY(id));


INSERT INTO split_part_01 VALUES(1, 'abc.com.cn','.',2);
INSERT INTO split_part_01 VALUES(2, '新年快乐，身体健康，万事如意', ',',3);
INSERT INTO split_part_01 VALUES(3, 'ehjwkvnrkew哈哈哈&9832哈哈哈,84321093,','哈哈',6);
INSERT INTO split_part_01 VALUES(4, '123abc&*.jjkmm&*.73290302','&*.',3);
INSERT INTO split_part_01 VALUES(5, '  78829,.327hjfew.;,32oi  cekw', ',',22);
INSERT INTO split_part_01 VALUES(6, 'efwu3nkjr3w3;;  9099032c45dc3s// *  ',' ', 3242);
INSERT INTO split_part_01 VALUES(7, '83092i3f2o.dkwec<>dhwkjv<>789392-3<>', NULL, 3);
INSERT INTO split_part_01 VALUES(8, NULL, '.',11);
INSERT INTO split_part_01 VALUES(9, '442+562++++——----吃饭了',',',NULL);

-- Abnormal insert
INSERT INTO split_part_01 VALUES(1, 'ewjj32..3,l43/.43', 0);
INSERT INTO split_part_01 VALUES(11, 'vhjdwewj3902i302o302($#$%^&*()_POJHFTY&(*UIOPL:<DQ87*q8JIFWJLWKMDXKLSMDXKSLMKCw54545484154444489897897o8u8&92)(','few',4);
INSERT INTO split_part_01 VALUES(12, '', 'vjdkelwvrew', 32769);


SELECT split_part(s1,NULL,count1) FROM split_part_01;
SELECT split_part(s1,delim,NULL) FROM split_part_01;
SELECT split_part(s1,delim,count1) FROM split_part_01;
SELECT split_part(s1,delim,count1) FROM split_part_01 WHERE count1 >= 2;
SELECT split_part(s1,delim,count1 + 3) FROM split_part_01 WHERE count1 < 0;
SELECT split_part(s1,delim,count1) FROM split_part_01 WHERE count1 = 3242;
SELECT * FROM split_part_01 WHERE split_part(s1,'.',2) = 'com';
SELECT * FROM split_part_01 WHERE split_part(s1,'.',2) LIKE '%com%';
SELECT * FROM split_part_01 WHERE split_part(s1,' ',3) = '78829,.327hjfew.;,32oi';
SELECT split_part(s1,' ',1) FROM split_part_01 WHERE id = 6;
SELECT split_part(s1,'*.',ABS(-2)) FROM split_part_01 WHERE id = 4;
SELECT * FROM split_part_01 WHERE split_part(s1, '.', 1 + 6) = 'com.cn';
SELECT split_part(split_part(s1,'.',22),'.',1) FROM split_part_01 WHERE id = 1;


-- -- Nested with string functions
SELECT * FROM split_part_01 WHERE LENGTH(split_part(s1,'*.',2)) = 6;
SELECT * FROM split_part_01 WHERE split_part(LTRIM(s1),'.',2) = '.327hjfew.;';
SELECT delim,count1 FROM split_part_01 WHERE split_part(RTRIM(s1),'<>',1) = '83092i3f2o.dkwec';
SELECT * FROM split_part_01 WHERE LPAD(split_part(LTRIM(s1),'.',2),20,'*') = '************327hjfew';
SELECT * FROM split_part_01 WHERE RPAD(split_part(s1,'*.',3),20,'*') = '73290302************';
SELECT startswith(split_part(s1,'*.',3),'123') FROM split_part_01;
SELECT endswith(split_part(s1,'+',2),'62') FROM split_part_01;
SELECT * FROM split_part_01 WHERE find_in_set(split_part(s1,delim,count1),NULL) = NULL;
SELECT CONCAT_WS(split_part(s1,delim,count1),'hehaha32789','ABCNSLK') FROM split_part_01 WHERE id = 2;
SELECT empty(split_part(s1,delim,count1)) FROM split_part_01;
SELECT substring(split_part(s1,delim,count1),1,5) FROM split_part_01;
SELECT REVERSE(split_part(s1,delim,3)) FROM split_part_01;


-- subquries
SELECT * FROM split_part_01 WHERE s1 = (SELECT s1 FROM split_part_01 WHERE split_part(LTRIM(s1),'.',2) = '327hjfew');
SELECT(SELECT s1 FROM split_part_01 WHERE split_part(RTRIM(s1),'<>',1) = '83092i3f2o.dkwec');
SELECT id ,s1, delim FROM split_part_01 WHERE s1 = (SELECT s1 FROM split_part_01 WHERE split_part(LTRIM(s1),'*.',1) = '123abc&');


-- @suite
-- @setup
DROP TABLE IF EXISTS split_part_02;
CREATE TABLE split_part_02(id int,
                           s1 longtext,
                           delim CHAR,
                           count1 int NOT NULL,
                           count2 bigint,
                           PRIMARY KEY(id));


INSERT INTO split_part_02 VALUES(1, 'SUBSTRING函数的功能:用于从字符串的指定位置开始截取指定长度的字符串substring语法:SUBSTRING(string, start, length)','a',1231,548494515);
INSERT INTO split_part_02 VALUES(2, 'dvuewinviecfjds439432094ie3jiHHDIUWH*&*(UIJCSijfje3iu2j9032^&(*&()(*)I)A&^%^*&','j',3,123);
INSERT INTO split_part_02 VALUES(3, '', NULL, 1, 45);
INSERT INTO split_part_02 VALUES(4, NULL, '*', 5, NULL);
INSERT INTO split_part_02 VALUES(5, '  dhewjvrew  er&&***&&n e89__+&&**+=--=*(&&***&(&^*)(  ','*', 6, 83092302);


SELECT split_part(s1, NULL, count1) FROM split_part_02;
SELECT split_part(s1, delim, NULL) FROM split_part_02;
SELECT split_part(s1,delim,count1) FROM split_part_02;
SELECT split_part(s1,delim,3),split_part(s1,delim,4) FROM split_part_02;
SELECT split_part(s1,delim,count1) FROM split_part_02 WHERE count2 IS NOT NULL;
SELECT * FROM split_part_02 WHERE split_part(s1,'的',2) = '功能:用于从字符串';
SELECT * FROM split_part_02 WHERE split_part(s1,'的',2)spilt( LIKE '功能%';
SELECT * FROM split_part_02 WHERE split_part(s1, 'iii', 3-1) = 'cn';


-- -- Nested with string functions
SELECT * FROM split_part_02 WHERE LENGTH(split_part(s1,delim,2)) = 14;
SELECT split_part(LTRIM(s1),delim,6) FROM split_part_02 WHERE id = 5;
SELECT * FROM split_part_02 WHERE split_part(LTRIM(s1),delim,6) = '+=--=';
SELECT delim,count1 FROM split_part_02 WHERE split_part(RTRIM(s1),delim,3) = NULL;
SELECT * FROM split_part_02 WHERE LPAD(split_part(LTRIM(s1),'ew',2),20,'*') = '*****************jvr';
SELECT startswith(split_part(s1,delim,3),'SUB') FROM split_part_02;
SELECT endswith(split_part(s1,delim,6),'h)') FROM split_part_02;
SELECT find_in_set(split_part(s1,delim,count1),'SUBSTRING函数的功能:用于从字符串的指定位置开始截取指定长度的字符串substring语法:SUBSTRING(string, start, length),dvuewinviecfjds439432094ie3jiHHDIUWH*&*(UIJCSijfje3iu2j9032^&(*&()(*)I)A&^%^*&') FROM split_part_02;
SELECT CONCAT_WS(split_part(s1,delim,count1),'hehaha32789','ABCNSLK') FROM split_part_02 WHERE id = 2;
SELECT REVERSE(split_part(s1,delim,3)) FROM split_part_02;



-- @suite
-- @setup
DROP TABLE IF EXISTS split_part_03;
DROP TABLE IF EXISTS split_part_04;
CREATE TABLE split_part_03(
	id int,
    d1 CHAR,
    str1 VARCHAR(50),
    primary key (id));

CREATE TABLE split_part_04(
    id int,
    d2 smallint,
	str1 mediumtext NOT NULL,
    primary key (id));


INSERT INTO split_part_03 VALUES(1, ')', '78213)jji)JIJSC_)dhej');
INSERT INTO split_part_03 VALUES(2, '', '***((((()))');
INSERT INTO split_part_03 VALUES(3, ' ', NULL);
INSERT INTO split_part_03 VALUES(4, NULL, '  hciuwejw^&*((*&*^GGHJjqm');
INSERT INTO split_part_03 VALUES(5, '*',' fjewlk*(&^de jw*(&^wuio*(&^,,,, ');

INSERT INTO split_part_04 VALUES(1, 1, '78213)jji)JIJSC_  )dhej   ');
INSERT INTO split_part_04 VALUES(2, 90, 'jewjeioqjeio3j4729u3ewqiu(U)(JOIWJ***((((()))');
INSERT INTO split_part_04 VALUES(3, NULL,'  hciuwejw^&*((*&');
INSERT INTO split_part_04 VALUES(4, 6, '  hciuwejw^&*(*^GGHJjqmmqjJHGG');


-- join
SELECT split_part_03.id AS id_3,split_part_04.str1 AS str1_4 FROM split_part_03,split_part_04 WHERE split_part(split_part_03.str1, ')', 2) = split_part(split_part_04.str1, ')', 2);
SELECT split_part_03.str1 AS tmp,split_part_04.str1 AS temp FROM split_part_03 left join split_part_04 ON split_part(split_part_03.str1, '2', 1) = split_part(split_part_04.str1, '2', 1);
SELECT split_part_03.d1 AS d1_3,split_part_04.d2 AS d2_4 FROM split_part_03 right join split_part_04 ON split_part(split_part_03.str1, '2', 1) = split_part(split_part_04.str1, '2', 1);
-- @suit
-- @case
-- @test function SUBSTRING_INDEX(str,delim,count)
-- @label:bvt

SELECT substring_index('abc^123^ioo','^',1);
SELECT substring_index('efjq34','4r832r432094-3',2);
SELECT substring_index('床前明月光，疑是地上霜，举头望明月，低头思故乡','，',4);
SELECT substring_index('jewkrje的jdksvfe32893**(的——++））（）（  的', '的', -3);
SELECT substring_index('fhew嗯嗯圣诞节chejwk嗯嗯__++__w嗯嗯','嗯嗯',2);
SELECT substring_index('v23dnnr###ewjrfkjewm#vrewnvrenjvnewmvrdjvrnjerewmvrjenjwvewmvrrnenjvrenjvrejnvewvrevrjewvrnew','ewmvr',8);
SELECT substring_index('www.baidu.com','.',-1);
SELECT substring_index('hdjwkrfew*(dehw382*(vnejw4','*(',-5);
SELECT substring_index('43728943902493-24fjk43nmfjkwek432','3',3);
SELECT substring_index('dfjwkfrewfr','r',0);
SELECT substring_index('ABC*123*()(','*',2);
SELECT substring_index('12345*&+789*&dhejwfew2','*&',2);
SELECT substring_index('.+0___=+. ','.',1);
SELECT substring_index('..','.',1);
SELECT substring_index('','327832',1);
SELECT substring_index(NULL,'.',6);
SELECT substring_index('-+0988   &^88?/7!@~~~~',NULL,3);
SELECT substring_index('efwjkfe&*&**(*))))','*',NULL);


-- @suite
-- @setup
DROP TABLE IF EXISTS substring_index_01;
CREATE TABLE substring_index_01(id int,
                                s1 VARCHAR(100),
                                delim VARCHAR(20),
                                count1 smallint,
                                PRIMARY KEY(id));


INSERT INTO substring_index_01 VALUES(1, 'abc.com.cn','.',2);
INSERT INTO substring_index_01 VALUES(2, '新年快乐，身体健康，万事如意', ',',3);
INSERT INTO substring_index_01 VALUES(3, 'ehjwkvnrkew哈哈哈&9832哈哈哈,84321093,','哈哈',-6);
INSERT INTO substring_index_01 VALUES(4, '123abc&*.jjkmm&*.73290302','&*.',3);
INSERT INTO substring_index_01 VALUES(5, '  78829,.327hjfew.;,32oi  cekw', ',',-2);
INSERT INTO substring_index_01 VALUES(6, 'efwu3nkjr3w3;;  9099032c45dc3s// *  ',' ', -4);
INSERT INTO substring_index_01 VALUES(7, '','',0);
INSERT INTO substring_index_01 VALUES(8, '83092i3f2o.dkwec<>dhwkjv<>789392-3<>', NULL, 3);
INSERT INTO substring_index_01 VALUES(9, NULL, '.',11);
INSERT INTO substring_index_01 VALUES(10, '442+562++++——----吃饭了',',',NULL);


-- Abnormal insert
INSERT INTO substring_index_01 VALUES(1, 'ewjj32..3,l43/.43', 0);
INSERT INTO substring_index_01 VALUES(11, 'vhjdwewj3902i302o302($#$%^&*()_POJHFTY&(*UIOPL:<DQ87*q8JIFWJLWKMDXKLSMDXKSLMKCw54545484154444489897897o8u8&92)(','few',4);
INSERT INTO substring_index_01 VALUES(12, '', 'vjdkelwvrew', 32769);


SELECT substring_index(s1,delim,count1) FROM substring_index_01;
SELECT substring_index(s1,delim,count1) FROM substring_index_01 WHERE count1 >= 2;
SELECT substring_index(s1,delim,count1 + 3) FROM substring_index_01 WHERE count1 < 0;
SELECT substring_index(s1,delim,count1 % 2) FROM substring_index_01 WHERE count1 % 2 = 0;
SELECT * FROM substring_index_01 WHERE substring_index(s1,'.',2) = 'abc.com';
SELECT * FROM substring_index_01 WHERE substring_index(s1,' ',-3) = '*  ';
SELECT substring_index(s1,' ',-10) FROM substring_index_01 WHERE id = 6;
SELECT substring_index(s1,'*.',ABS(-2)) FROM substring_index_01 WHERE id = 4;
SELECT * FROM substring_index_01 WHERE substring_index(s1, '.', 1 - 3) = 'com.cn';
SELECT substring_index(substring_index(s1,'.',-2),'.',1) FROM substring_index_01 WHERE id = 1;


-- -- Nested with string functions
SELECT * FROM substring_index_01 WHERE LENGTH(substring_index(s1,'*.',2)) = 15;
SELECT * FROM substring_index_01 WHERE substring_index(LTRIM(s1),'.',2) = '78829,.327hjfew';
SELECT delim,count1 FROM substring_index_01 WHERE substring_index(RTRIM(s1),'<>',1) = '83092i3f2o.dkwec';
SELECT * FROM substring_index_01 WHERE LPAD(substring_index(LTRIM(s1),'.',2),20,'*') = '83092i3f2o.dkwec****';
SELECT RPAD(substring_index(s1,'*.',3),20,'*') FROM substring_index_01 WHERE id = 4;
SELECT startswith(substring_index(s1,'*.',3),'123') FROM substring_index_01;
SELECT endswith(substring_index(s1,'+',2),'62') FROM substring_index_01;
SELECT * FROM substring_index_01 WHERE find_in_set(substring_index(s1,delim,count1),NULL) = NULL;
SELECT CONCAT_WS(substring_index(s1,delim,count1),'hehaha32789','ABCNSLK') FROM substring_index_01 WHERE id = 2;
SELECT empty(substring_index(s1,delim,count1)) FROM substring_index_01;
SELECT substring(substring_index(s1,delim,count1),1,5) FROM substring_index_01;
SELECT REVERSE(substring_index(s1,delim,3)) FROM substring_index_01;


-- subquries
SELECT * FROM substring_index_01 WHERE s1 = (SELECT s1 FROM substring_index_01 WHERE substring_index(LTRIM(s1),'.',2) = '78829,.327hjfew');
SELECT(SELECT s1 FROM substring_index_01 WHERE substring_index(RTRIM(s1),'<>',1) = '83092i3f2o.dkwec');
SELECT id ,s1, delim FROM substring_index_01 WHERE s1 = (SELECT s1 FROM substring_index_01 WHERE substring_index(LTRIM(s1),'.',0) = NULL);


-- @suite
-- @setup
DROP TABLE IF EXISTS substring_index_02;
CREATE TABLE substring_index_02(id int,
                                s1 longtext,
                                delim CHAR,
                                count1 int NOT NULL,
                                count2 bigint unsigned,
                                PRIMARY KEY(id));


INSERT INTO substring_index_02 VALUES(1, 'SUBSTRING函数的功能:用于从字符串的指定位置开始截取指定长度的字符串substring语法:SUBSTRING(string, start, length)','a',-1231,548494515);
INSERT INTO substring_index_02 VALUES(2, 'dvuewinviecfjds439432094ie3jiHHDIUWH*&*(UIJCSijfje3iu2j9032^&(*&()(*)I)A&^%^*&','j',0,123);
INSERT INTO substring_index_02 VALUES(3, '', NULL, -3, 45);
INSERT INTO substring_index_02 VALUES(4, NULL, '*', 5, NULL);
INSERT INTO substring_index_02 VALUES(5, '  dhewjvrew  er&&***&&n e89__+&&**+=--=*(&&***&(&^*)(  ','*', 6, 83092302);


SELECT substring_index(s1,delim,count1) FROM substring_index_02;
SELECT substring_index(s1,delim,count1 + 3),substring_index(s1,delim,count2) FROM substring_index_02 WHERE count1 < 0;
SELECT substring_index(s1,delim,count2 % 2) FROM substring_index_02 WHERE count2 IS NOT NULL;
SELECT * FROM substring_index_02 WHERE substring_index(s1,'的',2) = 'SUBSTRING函数的功能:用于从字符串';
SELECT * FROM substring_index_02 WHERE substring_index(s1, 'iii', 1 - 3) = 'com.cn';


-- -- Nested with string functions
SELECT * FROM substring_index_02 WHERE LENGTH(substring_index(s1,delim,2)) = 27;
SELECT * FROM substring_index_02 WHERE substring_index(LTRIM(s1),delim,count1) = 'dhewjvrew  er&&***&&n e89__+&&**+=--=';
SELECT delim,count1 FROM substring_index_02 WHERE substring_index(RTRIM(s1),delim,3) = '&&***&(&^*)(  ';
SELECT * FROM substring_index_02 WHERE LPAD(substring_index(LTRIM(s1),'e',3),20,'*') = 'dhewjvrew  *********';
SELECT startswith(substring_index(s1,delim,3),'SUB') FROM substring_index_02;
SELECT endswith(substring_index(s1,delim,-2),'h)') FROM substring_index_02;
SELECT find_in_set(substring_index(s1,delim,count1),'SUBSTRING函数的功能:用于从字符串的指定位置开始截取指定长度的字符串substring语法:SUBSTRING(string, start, length),dvuewinviecfjds439432094ie3jiHHDIUWH*&*(UIJCSijfje3iu2j9032^&(*&()(*)I)A&^%^*&') FROM substring_index_02;
SELECT CONCAT_WS(substring_index(s1,delim,count1),'hehaha32789','ABCNSLK') FROM substring_index_02 WHERE id = 2;
SELECT substring(substring_index(s1,delim,count2),0,10) FROM substring_index_02;
SELECT REVERSE(substring_index(s1,delim,3)) FROM substring_index_02;


-- @suite
-- @setup
DROP TABLE IF EXISTS substring_index_03;
DROP TABLE IF EXISTS substring_index_04;
CREATE TABLE substring_index_03(
                                   id int,
                                   d1 CHAR,
                                   str1 VARCHAR(50),
                                   primary key (id));

CREATE TABLE substring_index_04(
                                   id int,
                                   d2 smallint,
                                   str1 mediumtext NOT NULL,
                                   primary key (id));


INSERT INTO substring_index_03 VALUES(1, ')', '78213)jji)JIJSC_)dhej');
INSERT INTO substring_index_03 VALUES(2, '', '***((((()))');
INSERT INTO substring_index_03 VALUES(3, ' ', NULL);
INSERT INTO substring_index_03 VALUES(4, NULL, '  hciuwejw^&*((*&*^GGHJjqm');
INSERT INTO substring_index_03 VALUES(5, '*',' fjewlk*(&^de jw*(&^wuio*(&^,,,, ');

INSERT INTO substring_index_04 VALUES(1, 0, '78213)jji)JIJSC_  )dhej   ');
INSERT INTO substring_index_04 VALUES(2, 90, 'jewjeioqjeio3j4729u3ewqiu(U)(JOIWJ***((((()))');
INSERT INTO substring_index_04 VALUES(3, NULL,'  hciuwejw^&*((*&');
INSERT INTO substring_index_04 VALUES(4, -6, '  hciuwejw^&*(*^GGHJjqmmqjJHGG');


-- join
SELECT * FROM substring_index_03 WHERE str1 = (SELECT str1 FROM substring_index_04 WHERE substring_index(substring_index_04.str1, '(', 4) = '***(((');
SELECT * FROM substring_index_03,substring_index_04 WHERE substring_index(substring_index_03.str1, ')', 2) = substring_index(substring_index_04.str1, ')', 2);
SELECT substring_index_03.str1 AS tmp,substring_index_04.str1 AS temp FROM substring_index_03 left join substring_index_04 ON substring_index(substring_index_03.str1, '2', 1) = substring_index(substring_index_04.str1, '2', 1);
SELECT substring_index_03.d1 AS d1_3,substring_index_04.d2 AS d2_4 FROM substring_index_03 right join substring_index_04 ON substring_index(substring_index_03.str1, '2', 1) = substring_index(substring_index_04.str1, '2', 1);



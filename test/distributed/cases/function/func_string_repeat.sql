-- @suit
-- @case
-- @desc:test for function repeat()
-- @label:bvt


SELECT repeat('abc', -1);
SELECT repeat('abc', 0);
SELECT repeat('abc', 1), repeat('abc', 2), repeat('abc', 3);
SELECT repeat('abc', null);
select repeat('abc', 100000000000);


-- test for function repeat(str,count)  count < 0, return ''
select repeat('372891uhj4r23uj4r3fv()(*)W@', -10);
select repeat('efhwuh4ejkwn433qwieu^%^&&(*(%$%^',-100);


-- test for function repeat(str,count) count = 0, return ''
select repeat('database数据库',0);
select repeat('qyf783y82y489u32y49u39204i032i9589ijfiekwfjkw432r435g34f',0);


-- test for function repeat(str,count) count > 0
select repeat('cjwijejnvrewvew',100);
select repeat('23324{}{}|}{}{}{}|{%$#%$^&*()*("""',10000000);


-- test for function repeat(str,count) str = null，return null
select repeat('',213);
select repeat(null,21893);


-- test for function repeat(str,count) count = null，return null
select repeat('dwhjkvwe3w4r3wfrrw',null);
select repeat('euu4832ur48ei3jfojcjncj*(*())',null);
select repeat(null,null);


-- test for function repeat(str,count)
select repeat('MO',3);
select repeat('MO ',30);
select repeat('MO MO MO ', 100);
select repeat('ewrfewvrefr',100.23123321);


-- test for function repeat(str,count)
select repeat('1223wer33fggfeew',1048577);
select repeat('12321412132134321321423ewdfqwf4wevfrevdcsIOPKMKMq29389c323e32ndy',1048000);


-- test for function repeat(str,count),count is non-int type
select repeat('hej32h4rk23vrw3g534',1.3);
select repeat('few84uiwjvwev rwb',213.3328948309240324);
select repeat('38290ri432vr',0.3892432324);
select repeat('erwr43wgvrwebvrtbregterb   324423*()*^&%^$%$#@',-390213.242342);


-- test for function repeat(str,count) with string function
select repeat(oct(100),1000);
select repeat(ltrim(' hrekjwrfv'), 10);
select repeat(rtrim('momomvdjehwe   '), 20);
select repeat(trim(' ewvre  refrew wrew   wrfewre45324r32r43f22g34r3ew      '),100);
select repeat(lpad('48932493289020-f4o3fikioewjivrev',200,'11132343423232'),20);
select repeat(rpad('  wqdyeuqdejwhqjfehuqhfueh432484u324i3u2jr432fr230-03-24324 rvv',80,'1234567890***'),1);
select repeat(substring('dqeijfqknvwerijrewf',4,10),2);
select repeat(reverse('today is a good day!'),10);
select repeat(group_concat('ehiuqh3214234r32432fcewvre','euiqjned3f4'),21);
select repeat(split_part('equheqwjnvcjenjrXXXXkewjefwvrwe','XXXX',2),334);


-- abnoraml test
select repeat('wefui2n3f2',eee);
select repeat(3729743928r423r43r,10);
select repeat(e28eeiowieofpwipefwerfwe32,24382048922w);


-- table column:repeat(varchar,count)
drop table if exists repeat01;
create table repeat01(col1 int, col2 varchar(100) not null);
insert into repeat01 values(1, '37829743920819432fjnjk23n4j324i3f34ekkmfjk24nk32j4ei3k2fode3m24r');
insert into repeat01 values(2, '3289u4392u4r4943i249ri32oikdi2349i3');
insert into repeat01 values(null, '379281738942uj1oi423g4{}{}}|{|}"WWU*UFE');
select * from repeat01;
select repeat(col2, col1) from repeat01;
drop table repeat01;


-- table column:repeat(tinytext,count)
drop table if exists repeat02;
create table repeat02(col1 tinytext not null, col2 tinyint);
insert into repeat02 values('!@@@#########################$%^&*(!@#$%^&*(IOIUYGFDRTFYGUHJNBHGVFRDE#W@Q#$%6t7yuihbgtfre43we5r6t7yuhihygtfrde43w4e5r6t7yuihnygtfrde45r6t7yhujhygtr54e5rt6ygh','20');
insert into repeat02 values('3t976dfy2hg4g43yh28r4u2of43]f\e3yuih4r328uehdw```ui21`ju`h32ih21iojhui1rh4ufhui4hfehwnjcfehrhvbweuyreihbfcreuhwuiwhuvrnehvbrenhvjebwnhrbuefhrneuwfnrejwnfrwehjbvhrebwruehwruewnvrew,',100);
insert into repeat02 values('1',378);
select * from repeat02;
select repeat(col1,col2) from repeat02;
drop table repeat02;


-- table column:repeat(mediumtext,count)
drop table if exists repeat03;
create table repeat03(col1 mediumtext,col2 int);
insert into repeat03 values ('MatrixOne 是一款超融合异构分布式数据库，通过云原生化和存储、计算、事务分离的架构构建 HSTAP 超融合数据引擎，实现单一数据库系统支持 OLTP、OLAP、流计算等多种业务负载，并且支持公有云、私有云、边缘云部署和使用，实现异构基础设施的兼容。MatrixOne 具备实时 HTAP，多租户，流式计算，极致扩展性，高性价比，企业级高可用及 MySQL 高度兼容等重要特性，通过为用户提供一站式超融合数据解决方案，可以将过去由多个数据库完成的工作合并到一个数据库里，从而简化开发运维，消减数据碎片，提高开发敏捷度。', null);
insert into repeat03 values (null, 1000000000);
insert into repeat03 values ('通过 Compute Node 和 Data Node 的灵活配合兼顾点查询与批处理，对于 OLTP 和 OLAP 都具备极致性能。',2147483647);
select * from repeat03;
select repeat(col1,col2) from repeat03;
drop table repeat03;


-- repeat with string function in table column, float cast to int
drop table if exists repeat04;
create table repeat04(col1 float, col2 varchar(200));
insert into repeat04 values (1232.21312, 'YT^&Y*UJIJH* ( U ( HJHFDTYUIHJFYTGUHIJueiyw8uhfwjh483i2verwe4r3f2432432');
insert into repeat04 values (0.3243214, '4324213421v dsr3\\}{}{}Ew4rfebte');
insert into repeat04 values (-32.321342, '  321432fvewvegre5 g5 ');
insert into repeat04 values (null, 'jh   i w o  IIIO ejwqd');
insert into repeat04 values (0,'');
select * from repeat04;
select repeat(col2,col1) from repeat04 where repeat(col2,1) = '  321432fvewvegre5 g5 ';
select repeat(ltrim(col2), col1 % 10) from repeat04;
select repeat(rtrim(col2), col1 * 0.2) from repeat04 ;
select repeat(trim(col2),10) from repeat04;
select repeat(lpad(col2,50,'11132343423232'),20) from repeat04;
select repeat(rpad(col2,300,'1234567890***'),col1) from repeat04;
select repeat(reverse(col2),10) from repeat04;
drop table repeat04;


-- repeat: double cast to int64
drop table if exists repeat05;
create table repeat05(col1 double, col2 text);
insert into repeat05 values (-10.321,324242332);
insert into repeat05 values (0,'3214243232532323afewevc432');
insert into repeat05 values (null,'423432f234{}{{}DEIUIOEKoiu9iofkle');
insert into repeat05 values (10.93243342, '123');
select * from repeat05;
select repeat(col2,col1) from repeat05;
drop table repeat05;


-- repeat: decimal cast to int64
drop table if exists repeat06;
create table repeat06(col1 decimal(10,8), col2 mediumtext);
insert into repeat06 values (-10.321232,'ehwqjcnjewn4f3236348921734uf32ryd8329uf432ih4uiwedofoqw3[2]34-=+++++++++fe[powp4l3]2v[r32]3p4l3po2kvfcir4j2i4j3f2vc23432f3f2vre2');
insert into repeat06 values (1.23412342,'3214243232532323afewevc432');
insert into repeat06 values (null,'423432f234{}{{}DEIUIOEKoiu9iofkle');
insert into repeat06 values (0, '123');
select * from repeat06;
select repeat(col2,col1) from repeat06;
drop table repeat06;


-- repeat with insert into select
drop table if exists repeat07;
drop table if exists repeat08;
create table repeat07(col1 int, col2 longtext);
insert into repeat07 values (null, '324r314e2343232fcewvfewrfew{}{}|{}FDWE');
insert into repeat07 values (2114,'32421rd3e4][as][rew[e4]\32');
select * from repeat07;
create table repeat08(col1 int, col2 longtext);
insert into repeat08 select col1,repeat(col2,col1) from repeat08;
select * from repeat08;
drop table repeat07;
drop table repeat08;

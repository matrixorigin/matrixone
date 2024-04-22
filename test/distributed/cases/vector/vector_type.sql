create table vtab1(id int primary key auto_increment,`vecf32` vecf32(0));
create table vtab1(id int primary key auto_increment,`vecf32` vecf32(-1));
create table vtab2(a int primary key,b vecf32(3) primary key,c vecf64(3));
create table vtab32(id int primary key auto_increment,`vecf32_3` vecf32(3),`vecf32_5` vecf32(5));
create table vtab64(id int primary key auto_increment,`vecf64_3` vecf64(3),`vecf64_5` vecf64(5));

insert into vtab32(vecf32_3,vecf32_5) values("[0.8166459,NULL,0.4886152]",NULL);
insert into vtab32(vecf32_3,vecf32_5) values(NULL,NULL);
insert into vtab32(vecf32_3,vecf32_5) values("[0.8166459,0.66616553,0.4886152]",NULL);
insert into vtab32(vecf32_3,vecf32_5) values("0.1726299,3.29088557,30.4330937","0.1726299,3.29088557,30.4330937");
insert into vtab32(vecf32_3,vecf32_5) values ("[0.1726299,3.29088557,30.4330937]","[0.45052445,2.19845265,9.579752,123.48039162,4635.89423394]");
insert into vtab32(vecf32_3,vecf32_5) values ("[8.5606893,6.7903588,821.977768]","[0.46323407,23.49801546,563.9229458,56.07673508,8732.9583881]");
insert into vtab64(vecf64_3,vecf64_5) values("[0.8166459,NULL,0.4886152]",NULL);
insert into vtab64(vecf64_3,vecf64_5) values(NULL,NULL);
insert into vtab64(vecf64_3,vecf64_5) values("[0.8166459,0.66616553,0.4886152]",NULL);
insert into vtab64(vecf64_3,vecf64_5) values ("[8.5606893,6.7903588,821.977768]","[0.46323407,23.49801546,563.9229458,56.07673508,8732.9583881]");
insert into vtab64(vecf64_3,vecf64_5) values ("[0.9260021,0.26637346,0.06567037]","[0.45756745,65.2996871,321.623636,3.60082066,87.58445764]");

select * from vtab32;
select * from vtab32 where vecf32_3 > "[1,1,1]";
select * from vtab32 where vecf32_3 >= "[0.8166459, 0.66616553, 0.4886152]";
select * from vtab32 where vecf32_5 <= "[0.45052445,2.19845265,9.579752,123.48039162,4635.89423394]";
select * from vtab32 where vecf32_5 <= "[0.4505244500,002.19845265,9.579752,123.48039162,4635.89423394]";
select * from vtab32 where vecf32_5 < "[0.45052445,02.19845265,9.579752,123.48039162,4635.89423394]";
select * from vtab32 order by vecf32_3;
select * from vtab64 order by vecf64_5 desc;
select vecf32_3,count(*) from vtab32 group by vecf32_3 order by vecf32_3;
select * from vtab64;
select * from vtab32 where id = 1;
select * from vtab32 where vecf32_3 = "[0.1726299,3.29088557,30.4330937]";
select * from vtab32 where vecf32_3 is null;
select * from vtab32 where vecf32_3 is not null;

select * from vtab64 where id = 2;
select * from vtab64 where vecf64_5 = "[0.46323407,23.49801546,563.9229458,56.07673508,8732.9583881]";
select * from vtab64 where vecf64_5 is null;
select * from vtab64 where vecf64_5 is not null;

select a.id,a.vecf32_3, a.vecf32_5, b.id, b.vecf64_3, b.vecf64_5 from vtab32 a, vtab64 b where a.id = b.id;

select * from vtab64 join vtab32 on vtab64.vecf32_3 = vtab32.vecf32_3;

select * from vtab32 left join vtab64 on vtab64.vecf32_5 = vtab32.vecf32_5;
select count(*), SUMMATION(vecf32_3) from vtab32 group by SUMMATION(vecf32_3) order by SUMMATION(vecf32_3);



select id,vecf32_3 + vecf32_3, vecf32_5 + vecf32_5 from vtab32;
select id,vecf32_3 + "[1,1,1]", vecf32_5 + vecf32_5 from vtab32;
select id,vecf32_3 - vecf32_3, vecf32_5 - vecf32_5 from vtab32;
select id,vecf32_3 * vecf32_3, vecf32_5 * vecf32_5 from vtab32;
select id,vecf32_3 / vecf32_3, vecf32_5 / vecf32_5 from vtab32;

select id,vecf64_3 + vecf64_3, vecf64_5 + vecf64_5 from vtab64;
select id,vecf64_3 + "[1,1,1]", vecf64_5 + vecf64_5 from vtab64;
select id,vecf64_3 - vecf64_3, vecf64_5 - vecf64_5 from vtab64;
select id,vecf64_3 * vecf64_3, vecf64_5 * vecf64_5 from vtab64;
select id,vecf64_3 / vecf64_3, vecf64_5 / vecf64_5 from vtab64;

select a.id,a.vecf32_3 + b.vecf64_3, a.vecf32_5 + b.vecf64_5 from vtab32 a, vtab64 b where a.id = b.id;

select a.id,a.vecf32_3 + b.vecf64_3, a.vecf32_5 + b.vecf64_5 from vtab32 a, vtab64 b where a.vecf32_3 is not null;

select a.id,a.vecf32_3 + b.vecf64_3, a.vecf32_5 + b.vecf64_5 from vtab32 a, vtab64 b where a.vecf32_3 is null;

update vtab32 set vecf32_3 = '0.752072,0.41402978,0.058849692' where id = 1;
select * from vtab32;

update vtab64 set vecf64_3 = NULL where id = 1;
select * from vtab32;

update vtab32 set vecf32_3 = '[5.33816308, 78.35970277, 982.7183621]' where id = 1;
select vecf32_3 from vtab32;

update vtab64 set vecf64_5 = '[43234.87576795,32.8673464,12312.5788535,0.30837893,22.21907276]';
select vecf64_5 from vtab64;


delete from vtab32 where vecf32_3 = '[5.33816308, 78.35970277, 982.7183621]';

create table vtab64_1(id int primary key auto_increment,`vecf64_3` vecf64(3),`vecf64_5` vecf64(5));
load data infile '$resources/vector/vector.csv' into table vtab64_1;
select * from vtab64_1;
(select * from vtab64) except (select * from vtab64_1);



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

select SUMMATION(vecf32_3),SUMMATION(vecf32_5) from vtab32;
select SUMMATION(vecf64_3),SUMMATION(vecf64_5) from vtab64;

select SUMMATION("[0.45052445,2.19845265,9.579752]");
select SUMMATION(1);
select SUMMATION(NULL);
select id, SUMMATION(vecf32_3) from vtab32 group by id order by id;
select id, count(vecf32_3) from vtab32 group by id having id > 1 order by id ;
select count(*), SUMMATION(vecf32_3) from vtab32 group by SUMMATION(vecf32_3) Having SUMMATION(vecf32_3) > 33.89660960435867 order by SUMMATION(vecf32_3) desc ;
select * from vtab32 where SUMMATION(vecf32_3) is null;
select * from vtab32 where SUMMATION(vecf32_3) = 33.89660960435867;
select * from vtab32 where SUMMATION(vecf32_3) > 33.89660960435867;
select distinct(SUMMATION(vecf32_3)) from vtab32;
select sum(SUMMATION(vecf32_3)) from vtab32;
select min(SUMMATION(vecf32_3)) from vtab32;
select max(SUMMATION(vecf32_3)) from vtab32;
select avg(SUMMATION(vecf32_3)) from vtab32;
select count(SUMMATION(vecf32_3)) from vtab32;
select abs(SUMMATION(vecf64_3)) from vtab64;
select atan(SUMMATION(vecf64_3)) from vtab64;
select SUMMATION(vecf32_3) - SUMMATION(vecf32_5) from vtab32;
select SUMMATION(vecf32_3) * SUMMATION(vecf32_5) from vtab32;
select SUMMATION(vecf64_3) + SUMMATION(vecf64_5) from vtab64;
select SUMMATION(vecf64_3) / SUMMATION(vecf64_5) from vtab64;
select * from (select SUMMATION(vecf32_3),SUMMATION(vecf32_5) from vtab32);
select SUMMATION(vecf64_3),SUMMATION(vecf64_5) from (select * from vtab64);
WITH qn AS (select SUMMATION(vecf32_3),SUMMATION(vecf32_5) from vtab32) SELECT * FROM qn;



select inner_product(vecf32_3,"[1,1,1]") from vtab32;
select inner_product(vecf32_3,"[0,0,-1]") from vtab32;
select inner_product(vecf32_3,vecf32_3), inner_product(vecf32_5,vecf32_5) from vtab32;
select inner_product(vecf64_3,vecf64_3), inner_product(vecf64_5,vecf64_5) from vtab64;
select inner_product("[0.45052445,2.19845265,9.579752]","[1,1,1]");
select inner_product(1,1);
select inner_product(NULL,NULL);
select id, inner_product(vecf32_3,vecf32_3) from vtab32 group by id order by id;
select count(*), inner_product(vecf32_3,vecf32_3) from vtab32 group by inner_product(vecf32_3,vecf32_3) Having inner_product(vecf32_3,vecf32_3) > 937.0329415976587 order by inner_product(vecf32_3,vecf32_3) desc ;
select * from vtab32 where inner_product(vecf32_3,vecf32_3) is null;
select * from vtab32 where inner_product(vecf32_3,vecf32_3) = 675766.8704508307;
select * from vtab32 where inner_product(vecf32_3,vecf32_3) <= 1.3494319015309593;
select distinct(inner_product(vecf32_3,vecf32_3)) from vtab32;
select sum(inner_product(vecf32_3,vecf32_3)) from vtab32;
select min(inner_product(vecf32_5,vecf32_5)) from vtab32;
select max(inner_product(vecf32_3,vecf32_3)) from vtab32;
select avg(inner_product(vecf32_5,vecf32_5)) from vtab32;
select count(inner_product(vecf32_5,vecf32_5)) from vtab32;
select sin(inner_product(vecf64_3,vecf64_3)) from vtab64;
select cos(inner_product(vecf64_5,vecf64_5)) from vtab64;
select inner_product(vecf32_3,vecf32_3) - inner_product(vecf32_5,vecf32_5) from vtab32;
select inner_product(vecf32_3,vecf32_3) * inner_product(vecf32_5,vecf32_5) from vtab32;
select inner_product(vecf64_3,vecf64_3) + inner_product(vecf64_5,vecf64_5) from vtab64;
select inner_product(vecf64_3,vecf64_3) / inner_product(vecf64_5,vecf64_5) from vtab64;
select * from (select inner_product(vecf32_3,vecf32_3),inner_product(vecf32_5,vecf32_5) from vtab32);
select inner_product(vecf64_3,vecf64_3), inner_product(vecf64_5,vecf64_5) from (select * from vtab64);
WITH qn AS (select inner_product(vecf32_3,vecf32_3),inner_product(vecf32_5,vecf32_5) from vtab32) SELECT * FROM qn;



select l1_norm(vecf32_3), l1_norm(vecf32_5) from vtab32;
select l1_norm(vecf64_3), l1_norm(vecf64_5) from vtab64;
select l1_norm(vecf64_3 - vecf32_3),l1_norm(vecf64_5 - vecf32_5) from vtab32 a, vtab64 b where a.id = b.id;
select l1_norm(NULL);
select l1_norm(1);
select l1_norm("[1,2,3]");
select count(*), l1_norm(vecf32_3) from vtab32 group by l1_norm(vecf32_3) Having l1_norm(vecf32_3) > 1.9714266657829285 order by l1_norm(vecf32_3) desc ;
select * from vtab32 where l1_norm(vecf32_3) is null;
select * from vtab32 where l1_norm(vecf32_3) = 1.9714266657829285;
select * from vtab32 where l1_norm(vecf32_3) > 1.9714266657829285;
select distinct(l1_norm(vecf32_3)) from vtab32;
select sum(l1_norm(vecf32_3)) from vtab32;
select min(l1_norm(vecf32_3)) from vtab32;
select max(l1_norm(vecf32_3)) from vtab32;
select avg(l1_norm(vecf32_3)) from vtab32;
select count(l1_norm(vecf32_3)) from vtab32;
select abs(l1_norm(vecf64_3)) from vtab64;
select atan(l1_norm(vecf64_3)) from vtab64;
select l1_norm(vecf32_3) - l1_norm(vecf32_5) from vtab32;
select l1_norm(vecf32_3) * l1_norm(vecf32_5) from vtab32;
select l1_norm(vecf64_3) + l1_norm(vecf64_5) from vtab64;
select l1_norm(vecf64_3) / l1_norm(vecf64_5) from vtab64;
select * from (select l1_norm(vecf32_3),l1_norm(vecf32_5) from vtab32);
select l1_norm(vecf64_3),l1_norm(vecf64_5) from (select * from vtab64);
WITH qn AS (select l1_norm(vecf32_3),l1_norm(vecf32_5) from vtab32) SELECT * FROM qn;




select l2_norm(vecf32_3), l2_norm(vecf32_5) from vtab32;
select l2_norm(vecf64_3), l2_norm(vecf64_5) from vtab64;
select l2_norm(vecf64_3 - vecf32_3),l1_norm(vecf64_5 - vecf32_5) from vtab32 a, vtab64 b where a.id = b.id;
select l2_norm(NULL);
select l2_norm(1);
select l2_norm("[1,2,3]");
select count(*), l2_norm(vecf32_3) from vtab32 group by l2_norm(vecf32_3) Having l2_norm(vecf32_3) > 1.1616505074810406 order by l2_norm(vecf32_3) desc ;
select * from vtab32 where l2_norm(vecf32_3) is null;
select * from vtab32 where l2_norm(vecf32_3) = 1.1616505074810406;
select * from vtab32 where l2_norm(vecf32_3) > 1.1616505074810406;
select distinct(l2_norm(vecf32_3)) from vtab32;
select sum(l2_norm(vecf32_3)) from vtab32;
select min(l2_norm(vecf32_3)) from vtab32;
select max(l2_norm(vecf32_3)) from vtab32;
select avg(l2_norm(vecf32_3)) from vtab32;
select count(l2_norm(vecf32_3)) from vtab32;
select abs(l2_norm(vecf64_3)) from vtab64;
select atan(l2_norm(vecf64_3)) from vtab64;
select l2_norm(vecf32_3) - l2_norm(vecf32_5) from vtab32;
select l2_norm(vecf32_3) * l2_norm(vecf32_5) from vtab32;
select l2_norm(vecf64_3) + l2_norm(vecf64_5) from vtab64;
select l2_norm(vecf64_3) / l2_norm(vecf64_5) from vtab64;
select * from (select l2_norm(vecf32_3),l2_norm(vecf32_5) from vtab32);
select l2_norm(vecf64_3),l2_norm(vecf64_5) from (select * from vtab64);
WITH qn AS (select l2_norm(vecf32_3),l2_norm(vecf32_5) from vtab32) SELECT * FROM qn;

select vector_dims(vecf32_5),vector_dims(vecf32_3) from vtab32;
select vector_dims(vecf64_5),vector_dims(vecf64_3) from vtab64;
select vector_dims(vecf64_3 - vecf32_3),vector_dims(vecf64_5 - vecf32_5) from vtab32 a, vtab64 b where a.id = b.id;
select vector_dims(NULL);
select vector_dims(1);
select vector_dims("[1,2,3]");
select count(*), vector_dims(vecf32_3) from vtab32 group by vector_dims(vecf32_3) Having vector_dims(vecf32_3) > 1  order by vector_dims(vecf32_3) desc ;
select * from vtab32 where vector_dims(vecf32_3) is null;
select * from vtab32 where vector_dims(vecf32_3) = 3;
select * from vtab32 where vector_dims(vecf32_3) > 3;
select distinct(vector_dims(vecf32_3)) from vtab32;
select sum(vector_dims(vecf32_3)) from vtab32;
select min(vector_dims(vecf32_3)) from vtab32;
select max(vector_dims(vecf32_3)) from vtab32;
select avg(vector_dims(vecf32_3)) from vtab32;
select count(vector_dims(vecf32_3)) from vtab32;
select abs(vector_dims(vecf64_3)) from vtab64;
select atan(vector_dims(vecf64_3)) from vtab64;
select vector_dims(vecf32_3) - vector_dims(vecf32_5) from vtab32;
select vector_dims(vecf32_3) * vector_dims(vecf32_5) from vtab32;
select vector_dims(vecf64_3) + vector_dims(vecf64_5) from vtab64;
select vector_dims(vecf64_3) / vector_dims(vecf64_5) from vtab64;
select * from (select vector_dims(vecf32_3),vector_dims(vecf32_5) from vtab32);
select vector_dims(vecf64_3),vector_dims(vecf64_5) from (select * from vtab64);
WITH qn AS (select vector_dims(vecf32_3),vector_dims(vecf32_5) from vtab32) SELECT * FROM qn;



create table vtab32_1(id int primary key auto_increment,`vecf32_3` vecf32(3),`vecf32_3_1` vecf32(3),`vecf32_5` vecf32(5),`vecf32_5_1` vecf32(5));
create table vtab64_1(id int primary key auto_increment,`vecf64_3` vecf64(3),`vecf64_3_1` vecf64(3),`vecf64_5` vecf64(5),`vecf64_5_1` vecf64(5));

insert into vtab32_1(vecf32_3,vecf32_5) values("[0.8166459,NULL,0.4886152]",NULL);
insert into vtab32_1(vecf32_3,vecf32_5) values(NULL,NULL);
insert into vtab32_1(vecf32_3,vecf32_5) values("[0.8166459,0.66616553,0.4886152]",NULL);
insert into vtab32_1(vecf32_3,vecf32_5) values("0.1726299,3.29088557,30.4330937","0.1726299,3.29088557,30.4330937");
insert into vtab32_1(vecf32_3,vecf32_5) values ("[0.1726299,3.29088557,30.4330937]","[0.45052445,2.19845265,9.579752,123.48039162,4635.89423394]");
insert into vtab32_1(vecf32_3,vecf32_5) values ("[8.5606893,6.7903588,821.977768]","[0.46323407,23.49801546,563.9229458,56.07673508,8732.9583881]");
insert into vtab64_1(vecf64_3,vecf64_5) values("[0.8166459,NULL,0.4886152]",NULL);
insert into vtab64_1(vecf64_3,vecf64_5) values(NULL,NULL);
insert into vtab64_1(vecf64_3,vecf64_5) values("[0.8166459,0.66616553,0.4886152]",NULL);
insert into vtab64_1(vecf64_3,vecf64_5) values ("[8.5606893,6.7903588,821.977768]","[0.46323407,23.49801546,563.9229458,56.07673508,8732.9583881]");
insert into vtab64_1(vecf64_3,vecf64_5) values ("[0.9260021,0.26637346,0.06567037]","[0.45756745,65.2996871,321.623636,3.60082066,87.58445764]");
update vtab32_1 set `vecf32_3_1` = `vecf32_3` + "[7.326893,10.787382173,21.32132143]";
update vtab32_1 set `vecf32_5_1` = `vecf32_5` + "[7.326893,10.787382173,21.32132143,4.32132,98.321321]";
update vtab64_1 set `vecf64_3_1` = `vecf64_3` + "[32.89849324,1.98392832,192.095843]";
update vtab64_1 set `vecf64_5_1` = `vecf64_5` + "[32.89849324,1.98392832,192.095843,90.321321,23.12312321]";

select cosine_similarity(vecf32_3,"[1,1,1]") from vtab32;
select cosine_similarity(vecf32_3,"[0,0,-1]") from vtab32;
select cosine_similarity(a.vecf32_3,b.vecf64_3), cosine_similarity(a.vecf32_5,b.vecf64_5) from vtab32 a , vtab64 b where a.id = b.id;
select cosine_similarity(b.vecf64_3, a.vecf32_3), cosine_similarity(b.vecf64_5, a.vecf32_5) from vtab32 a , vtab64 b where a.id = b.id;
select cosine_similarity("[0.45052445,2.19845265,9.579752]","[1,1,1]");
select cosine_similarity(1,1);
select cosine_similarity(NULL,NULL);
select count(*), cosine_similarity(a.vecf32_3,b.vecf64_3) from vtab32 a , vtab64 b where a.id = b.id group by cosine_similarity(a.vecf32_3,b.vecf64_3) HAVING cosine_similarity(a.vecf32_3,b.vecf64_3) > 0.3 order by cosine_similarity(a.vecf32_3,b.vecf64_3) desc ;

select cosine_similarity(vecf32_3,vecf32_3_1) from vtab32_1;
select cosine_similarity(vecf32_5,vecf32_5_1) from vtab32_1;
select cosine_similarity(vecf64_3,vecf64_3_1) from vtab64_1;
select cosine_similarity(vecf64_5,vecf64_5_1) from vtab64_1;

select * from vtab32_1 where cosine_similarity(vecf32_3_1,vecf32_3) is null;
select * from vtab32_1 where cosine_similarity(vecf32_3,vecf32_3_1) = 0.9788139235276682;
select * from vtab32_1 where cosine_similarity(vecf32_3,vecf32_3) <= 0.9788139235276682;
select distinct(cosine_similarity(vecf32_3,vecf32_3_1)) from vtab32_1;
select sum(cosine_similarity(vecf32_3,vecf32_3_1)) from vtab32_1;
select min(cosine_similarity(vecf32_5,vecf32_5_1)) from vtab32_1;
select max(cosine_similarity(vecf32_3,vecf32_3_1)) from vtab32_1;
select avg(cosine_similarity(vecf32_5,vecf32_5_1)) from vtab32_1;
select count(cosine_similarity(vecf32_5,vecf32_5_1)) from vtab32_1;
select sin(cosine_similarity(vecf64_3,vecf64_3_1)) from vtab64_1;
select cos(cosine_similarity(vecf64_5,vecf64_5_1)) from vtab64_1;
select cosine_similarity(vecf32_3,vecf32_3_1) - cosine_similarity(vecf32_5,vecf32_5_1) from vtab32_1;
select cosine_similarity(vecf32_3,vecf32_3_1) * cosine_similarity(vecf32_5,vecf32_5_1) from vtab32_1;
select cosine_similarity(vecf64_3,vecf64_3_1) + cosine_similarity(vecf64_5,vecf64_5_1) from vtab64_1;
select cosine_similarity(vecf64_3,vecf64_3_1) / cosine_similarity(vecf64_5,vecf64_5_1) from vtab64_1;
select * from (select cosine_similarity(vecf32_3,vecf32_3_1),cosine_similarity(vecf32_5,vecf32_5_1) from vtab32_1);
select cosine_similarity(vecf64_3,vecf64_3_1), cosine_similarity(vecf64_5,vecf64_5_1) from (select * from vtab64_1);
WITH qn AS (select cosine_similarity(vecf32_3,vecf32_3_1),cosine_similarity(vecf32_5,vecf32_5_1) from vtab32_1) SELECT * FROM qn;
select cosine_similarity(vecf32_3,vecf32_3), cosine_similarity(vecf32_5,vecf32_5) from vtab32;
select cosine_similarity(vecf64_3,vecf64_3), cosine_similarity(vecf64_5,vecf64_5) from vtab64;

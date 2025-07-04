values row(1,1), row(2,2), row(3,3);
values row(1,1), row(2,2), row(3,3) order by column_0 desc;
select * from (values row(1,1), row(2,2), row(3,3)) a;
select * from (values row(1,1), row(2,2), row(3,3)) a(a, b);
select * from (values row(1,"1",1.0), row(abs(-2),"2",2.0)) a;
select column_1 from (values row(0, 1, cast('[3, 4, 5]' as vecf32(3))));
select column_2 from (values row(0, 1, cast('[3, 4, 5]' as vecf32(3))));
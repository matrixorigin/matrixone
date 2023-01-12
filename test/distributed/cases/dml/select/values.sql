values row(1,1), row(2,2), row(3,3);
values row(1,1), row(2,2), row(3,3) order by column_0 desc;
select * from (values row(1,1), row(2,2), row(3,3)) a;
select * from (values row(1,1), row(2,2), row(3,3)) a(a, b);
select * from (values row(1,"1",1.0), row(abs(-2),"2",2.0)) a;
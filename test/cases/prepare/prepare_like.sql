drop table if exists t1;
create table t1 (str1 varchar(25),str2 char(25));

insert into t1 values('a1','b1'),('a2', 'b2'),('%str1%', '%str2%'),('?','???');

prepare p1 from 'select * from t1 where str1 like "%\%%"';
execute p1;
deallocate prepare p1;

prepare p2 from 'select * from t1 where str1 like "%\%"';
execute p2;
deallocate prepare p2;

prepare p3 from 'select * from t1 where str1 like "\%%"';
execute p3;
deallocate prepare p3;

prepare p4 from 'select * from t1 where str1 like "%"';
execute p4;
deallocate prepare p4;

prepare p5 from 'select * from t1 where str1 like "\%str1\%"';
execute p5;
deallocate prepare p5;

prepare p6 from 'select * from t1 where str1 like "%?%"';
execute p6;
deallocate prepare p6;

prepare p7 from 'select * from t1 where str1 like ?';
execute p7;
deallocate prepare p7;

prepare p8 from 'select * from t1 where str1 like "?"';
execute p8;
deallocate prepare p8;

prepare s2 from 'select * from t1 where str1 like ?';

set @b1='%1%';
execute s2 using @b1;

set @b2='%1';
execute s2 using @b2;

set @b3='1%';
execute s2 using @b3;

set @b4='%str%';
execute s2 using @b4;

set @b5='%str';
execute s2 using @b5;

set @b6='str%';
execute s2 using @b6;

set @b7='\%str%';
execute s2 using @b7;

set @b8='%str1\%';
execute s2 using @b8;

set @b9='%\%str%';
execute s2 using @b9;

set @b10='%str1\%%';
execute s2 using @b10;

set @b11='%\%str%\%';
execute s2 using @b11;

deallocate prepare s2;

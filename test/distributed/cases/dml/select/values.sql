values row(1,1), row(2,2), row(3,3);
values row(1,1), row(2,2), row(3,3) order by column_0 desc;
select * from (values row(1,1), row(2,2), row(3,3)) a;
select * from (values row(1,1), row(2,2), row(3,3)) a(a, b);
select * from (values row(1,"1",1.0), row(abs(-2),"2",2.0)) a;
select column_1 from (values row(0, 1, cast('[3, 4, 5]' as vecf32(3)))) as v;
select column_2 from (values row(0, 1, cast('[3, 4, 5]' as vecf32(3)))) as v;

-- issue #29214: VALUES columns use an order-independent common type.
select ord, score from (values row(1, 26.27946), row(2, 15.2667265)) as v(ord, score) order by ord;
select ord, score from (values row(1, 15.2667265), row(2, 26.27946)) as v(ord, score) order by ord;
select ord, score from (values row(1, null), row(2, 26.27946)) as v(ord, score) order by ord;
select ord, score from (values row(1, 26.27946), row(2, null)) as v(ord, score) order by ord;
select score from (values row(null), row(null)) as v(score);
select ord, dt from (values row(1, cast('2024-01-02 12:34:56.123' as datetime(3))), row(2, cast('2024-01-02 12:34:56.123456' as datetime(6)))) as v(ord, dt) order by ord;
select ord, dt from (values row(1, cast('2024-01-02 12:34:56.123456' as datetime(6))), row(2, cast('2024-01-02 12:34:56.123' as datetime(3)))) as v(ord, dt) order by ord;
select ord, label from (values row(1, cast('a' as char(4))), row(2, cast('abcdefgh' as char(8)))) as v(ord, label) order by ord;
select ord, label from (values row(1, cast('abcdefgh' as char(8))), row(2, cast('a' as char(4)))) as v(ord, label) order by ord;
select x = 'a' as eq, hex(x) as bytes from (values row(coalesce(cast('a   ' as char(4)), cast('x' as varchar(8))))) v(x);
select count(*) as n from (select distinct x from (values row(coalesce(cast('a   ' as char(4)), cast('x' as varchar(8)))), row(cast('a' as varchar(8)))) v(x)) d;
select count(*) as n from (select distinct x from (values row(cast('a' as varchar(8))), row(coalesce(cast('a   ' as char(4)), cast('x' as varchar(8))))) v(x)) d;
select ord, x = 'a' as eq from (values row(1, null), row(2, coalesce(cast('a   ' as char(4)), cast('x' as varchar(8)))), row(3, null)) v(ord, x) order by ord;
select count(*) as n from (select distinct x from (values row(null), row(coalesce(cast('a   ' as char(4)), cast('x' as varchar(8)))), row(cast('a' as varchar(8))), row(null)) v(x)) d;
select count(*) as n from (select distinct x from (values row(cast('a   ' as varchar(8))), row(cast('a' as varchar(8)))) v(x)) d;

drop table if exists bitmappanic;
create table bitmappanic (col2 int);

-- group 0 : 0
-- group 1 : gap
-- group 2 : 65536
-- group 3 : gap
-- group 4 : 131072
insert into bitmappanic values(0),(65536),(131072);

--
select 
    bitmap_bucket_number(col2) bucket, 
    BITMAP_COUNT(bitmap_construct_agg(bitmap_bit_position(col2))) bmp 
from bitmappanic
group by bucket;

-- 
select
BITMAP_COUNT(bitmap_construct_agg(BITMAP_BUCKET_NUMBER(col2))) bmp
from bitmappanic;

-- The middle bucket has three different bits; the third row must lose the first bit.
insert into bitmappanic values (65537), (65538);
select col2, bitmap_count(bitmap_construct_agg(bitmap_bit_position(col2)) over (
    partition by bitmap_bucket_number(col2) order by col2 rows between 1 preceding and current row)) as cnt
from bitmappanic order by col2;

drop table if EXISTS bitmappanic;
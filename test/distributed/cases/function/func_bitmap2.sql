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

drop table if EXISTS bitmappanic;
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
-- Materialize overlapping non-empty bitmaps, then compare the exact window unions.
drop table if exists bitmap_window_union_input;
drop table if exists bitmap_window_union_seed;
create table bitmap_window_union_seed (p int, id int, bit int);
insert into bitmap_window_union_seed values (1,1,1),(1,1,2),(1,2,2),(1,2,3),(1,3,4),(1,3,5),(1,3,6),(2,4,8),(2,5,8),(2,5,9);
create table bitmap_window_union_input as select p, id, bitmap_construct_agg(cast(bit as unsigned)) as bm from bitmap_window_union_seed group by p, id;
select p, id,
    hex(bitmap_or_agg(bm) over ()) as whole_union,
    hex(bitmap_or_agg(bm) over (partition by p)) as partition_union,
    hex(bitmap_or_agg(bm) over (partition by p order by id rows between 1 preceding and current row)) as sliding_union
from bitmap_window_union_input order by p, id;
drop table bitmap_window_union_input;
drop table bitmap_window_union_seed;

CREATE TABLE my_table (
    d_1 int,
    d_2 int,
    v_1 int
);
insert into my_table values (0, 0, 0), (0, 0, 1), (0, 0, 2), (0, 0, 32768), (0, 0, 65537);
insert into my_table values (0, 1, 0), (0, 1, 32769);
insert into my_table values (1, 0, 3), (1, 0, 65540);

-- <0, 0, 0>: 0x07; <0, 0, 1>: 0x01; <0, 0, 2>: 0x02;
-- <0, 1, 0>: 0x01; <0, 0, 1>: 0x02;
-- <1, 0, 0>: 0x08; <1, 0, 2>: 0x10;
CREATE TABLE precompute AS
SELECT
    d_1,
    d_2,
    BITMAP_BUCKET_NUMBER(v_1) bucket,
    BITMAP_CONSTRUCT_AGG(BITMAP_BIT_POSITION(v_1)) bmp
FROM my_table
GROUP BY d_1, d_2, bucket;

-- <0, 0>: 5
-- <0, 1>: 2
-- <1, 0>: 2
SELECT
    d_1,
    d_2,
    SUM(BITMAP_COUNT(bmp)) sum_cnt
FROM precompute
GROUP BY d_1, d_2;

-- <0>: 6
-- <1>: 2
SELECT d_1, SUM(cnt) FROM (
    SELECT
        d_1,
        BITMAP_COUNT(BITMAP_OR_AGG(bmp)) cnt
    FROM precompute
    GROUP BY d_1, bucket
)
GROUP BY d_1;

drop table my_table;
drop table precompute;

drop table if exists bitmap01;
create table bitmap01(col1 bigint);
insert into bitmap01 values (1844674407370955161);
insert into bitmap01 values (9223372036854775807);
insert into bitmap01 values (0);
select bitmap_bit_position(col1) from bitmap01;
select bitmap_bucket_number(col1) from bitmap01;
select bitmap_bucket_number(col1) bucket,
       bitmap_construct_agg(bitmap_bit_position(col1)) bmp from bitmap01 group by col1;
select bitmap_count(bitmap_construct_agg(bitmap_bit_position(col1))) from bitmap01 group by col1;
drop table bitmap01;

drop table if exists bitmap02;
create table bitmap02 (val int);
insert into bitmap02 values (1), (32769);
select bitmap_bucket_number(val) as bitmap_id,
    bitmap_construct_agg(bitmap_bit_position(val)) as bitmap
    from bitmap02
    group by bitmap_id;
insert into bitmap02 values (32769), (32769), (1);
select bitmap_bucket_number(val) as bitmap_id,
    bitmap_construct_agg(bitmap_bit_position(val)) as bitmap
    from bitmap02
    group by bitmap_id;
insert into bitmap02 values (2), (3), (4);
select bitmap_bucket_number(val) as bitmap_id,
    bitmap_construct_agg(bitmap_bit_position(val)) as bitmap
    from bitmap02
    group by bitmap_id;
select bitmap_bucket_number(val) as bitmap_id,
    bitmap_count(bitmap_construct_agg(bitmap_bit_position(val))) as distinct_values
    from bitmap02
    group by bitmap_id;
select sum(distinct_values) from (
select bitmap_bucket_number(val) as bitmap_id,
    bitmap_count(bitmap_construct_agg(bitmap_bit_position(val))) as distinct_values
    from bitmap02
    group by bitmap_id
);
drop table bitmap02;

drop table if exists bitmap03;
create table bitmap03 (val tinyint unsigned);
insert into bitmap03 values (1), (254), (127);
select bitmap_bucket_number(val) as bitmap_id,
    bitmap_construct_agg(bitmap_bit_position(val)) as bitmap
    from bitmap03
    group by bitmap_id;
insert into bitmap03 values (10), (100), (1);
select bitmap_bucket_number(val) as bitmap_id,
    bitmap_construct_agg(bitmap_bit_position(val)) as bitmap
    from bitmap03
    group by bitmap_id;
insert into bitmap03 values (2), (3), (4);
select bitmap_bucket_number(val) as bitmap_id,
    bitmap_construct_agg(bitmap_bit_position(val)) as bitmap
    from bitmap03
    group by bitmap_id;
select bitmap_bucket_number(val) as bitmap_id,
    bitmap_count(bitmap_construct_agg(bitmap_bit_position(val))) as distinct_values
    from bitmap03
    group by bitmap_id;
select sum(distinct_values) from (
select bitmap_bucket_number(val) as bitmap_id,
    bitmap_count(bitmap_construct_agg(bitmap_bit_position(val))) as distinct_values
    from bitmap03
    group by bitmap_id
);
drop table bitmap03;
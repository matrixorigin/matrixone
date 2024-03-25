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
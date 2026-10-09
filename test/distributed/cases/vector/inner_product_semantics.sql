-- 数学点积及其排序语义：正值、负值、自点积与零。
drop database if exists vec_inner_product_semantics;
create database vec_inner_product_semantics;
use vec_inner_product_semantics;

select inner_product('[1,2,3]', '[4,5,6]') as dot,
       inner_product('[1,2,3]', '[1,2,3]') as self_dot,
       inner_product('[1,2,3]', '[-4,-5,-6]') as negative_dot,
       inner_product('[1,2,3]', '[0,0,0]') as zero_dot;
select inner_product(cast('[1,2,3]' as vecf64(3)), cast('[4,5,6]' as vecf64(3))) as dot64,
       inner_product(cast('[1,2,3]' as vecbf16(3)), cast('[4,5,6]' as vecbf16(3))) as dotbf,
       inner_product(cast('[1,2,3]' as vecf16(3)), cast('[4,5,6]' as vecf16(3))) as dot16,
       inner_product(cast('[1,2,3]' as vecint8(3)), cast('[4,5,6]' as vecint8(3))) as doti8,
       inner_product(cast('[1,2,3]' as vecuint8(3)), cast('[4,5,6]' as vecuint8(3))) as dotu8;

create table ip_values(id int primary key, v vecf32(3), v64 vecf64(3));
insert into ip_values values (1,'[4,5,6]','[4,5,6]'), (2,'[1,2,3]','[1,2,3]'),
                            (3,'[-4,-5,-6]','[-4,-5,-6]'), (4,'[0,0,0]','[0,0,0]');
-- 无 NULL 的常量/列查询覆盖批处理；列/列、自点积和 float64 覆盖标量路径。
select id, inner_product(v,'[1,2,3]') as dot, inner_product('[1,2,3]',v) as reversed_dot,
       inner_product(v64,cast('[1,2,3]' as vecf64(3))) as dot64 from ip_values order by id;
select a.id, inner_product(a.v,b.v) as dot from ip_values a join ip_values b on b.id=2 order by a.id;
select id, inner_product(v,v) as self_dot from ip_values order by id;
select id, inner_product(v,'[1,2,3]') as dot from ip_values order by inner_product(v,'[1,2,3]') desc limit 2;
select id, inner_product(v,'[1,2,3]') as dot from ip_values order by inner_product(v,'[1,2,3]') asc limit 2;
-- 其他距离的符号和数值不变。
select l2_distance('[1,2,3]','[4,5,6]') > 0 as l2_positive,
       cosine_similarity('[1,2,3]','[4,5,6]') > 0 as cosine_positive;
insert into ip_values values (5,null,null);
select id, inner_product(v,'[1,2,3]') as dot from ip_values order by id;
select inner_product(null,'[1,2,3]') as null_dot;
select inner_product('[1,2,3]','[4,5]');

drop database vec_inner_product_semantics;

-- @case
-- @desc: Correlated pagination applies all predicates before selecting rows per outer occurrence
-- @label:bvt
drop database if exists test_correlated_pagination;
create database test_correlated_pagination;
use test_correlated_pagination;
create table parent (seq int primary key, id int, ceiling bigint);
create table child (seq int primary key, parent_id int, event_idx bigint, value varchar(20));
insert into parent values (1,0,2),(2,2,2),(3,2,1),(4,2,1),(5,3,5),(6,4,null),(7,null,2);
insert into child values (11,0,1,'a'),(21,2,1,'b'),(22,2,2,'c'),(23,2,null,'n'),(31,3,null,'v'),(41,null,5,'z');

-- Outer-only predicates retain the existing limit and expression behavior.
create table outer_only_parent(seq int primary key);
create table outer_only_child(seq int primary key, value int not null, text_value varchar(20));
insert into outer_only_parent values (0),(1),(2);
insert into outer_only_child values (1,0,'0'),(2,7,'7');
select p.seq, (select abs(c.value) from outer_only_child c where p.seq>0 order by c.seq desc limit 1) as abs_value, (select c.value+1 from outer_only_child c where p.seq>0 order by c.seq limit 1) as incremented, (select c.value from outer_only_child c where p.seq>0 and abs(c.value)>0 order by c.seq limit 1) as filtered, (select abs(c.value) from outer_only_child c where p.seq>0 order by c.seq desc limit 1 offset 1) as offset_value from outer_only_parent p order by p.seq;
select p.seq,(select coalesce(abs(c.value),99) from outer_only_child c where p.seq>0 order by c.seq desc limit 1) as coalesced,(select case when abs(c.value)>0 then 42 else 99 end from outer_only_child c where p.seq>0 order by c.seq desc limit 1) as chosen,(select coalesce(abs(c.value),99) from outer_only_child c where p.seq>0 order by c.seq desc limit 1 offset 2) as empty_offset from outer_only_parent p order by p.seq;
select p.seq,(select cast(c.text_value as signed) from outer_only_child c where p.seq>0 order by c.seq desc limit 1) as cast_value,(select c.value from outer_only_child c where p.seq>0 and cast(c.text_value as signed)>0 order by c.seq limit 1) as cast_filter,(select c.value from outer_only_child c where p.seq>0 order by cast(c.text_value as signed) desc limit 1) as cast_order from outer_only_parent p order by p.seq;
select p.seq,(select min(abs(c.value)) from outer_only_child c where p.seq>0 limit 1) as min_value from outer_only_parent p order by p.seq;
select p.seq,(select max(cast(c.text_value as signed)) from outer_only_child c where p.seq=1 limit 1) as max_value from outer_only_parent p order by p.seq;
select p.seq,(select sum(c.value) from outer_only_child c where p.seq>0 limit 1) as sum_value from outer_only_parent p order by p.seq;
select p.seq,(select max(cast(c.text_value as signed)) from outer_only_child c where p.seq>0 limit 1) as max_non_eq from outer_only_parent p order by p.seq;
select p.seq,(select count(*) from outer_only_child c where p.seq>0 and cast(c.text_value as signed)>0 limit 1 offset 0) as matched_count from outer_only_parent p order by p.seq;
select p.seq,(select count(abs(c.value)) from outer_only_child c where p.seq=1 limit 1) as equal_count from outer_only_parent p order by p.seq;
select p.seq,(select avg(cast(c.text_value as signed)) from outer_only_child c where p.seq>0 limit 1) as avg_value from outer_only_parent p order by p.seq;
create table outer_only_empty(value int);
select p.seq,(select min(abs(c.value)) from outer_only_empty c where p.seq>0 limit 1) as empty_min from outer_only_parent p order by p.seq;
select p.seq,(select max(abs(c.value)) from outer_only_empty c where p.seq>0 limit 1) as empty_max from outer_only_parent p order by p.seq;
select p.seq,(select count(abs(c.value)) from outer_only_empty c where p.seq>0 limit 1) as empty_count from outer_only_parent p order by p.seq;
select p.seq,(select avg(abs(c.value)) from outer_only_empty c where p.seq>0 limit 1) as empty_avg from outer_only_parent p order by p.seq;
-- A positive LIMIT cannot remove the implicit aggregate row, even for a range predicate.
select p.seq,(select count(*) from outer_only_child c where c.value<p.seq limit 1 offset 0) as range_count from outer_only_parent p order by p.seq;
select p.seq,(select avg(abs(c.value)) from outer_only_child c where c.value<p.seq limit 1) as range_avg from outer_only_parent p order by p.seq;
select p.seq,(select count(*) from outer_only_child c where c.value<p.seq limit 2) as range_count from outer_only_parent p order by p.seq;
select p.seq,(select count(*) from outer_only_child c where c.value=p.seq and c.value<=p.seq limit 1) as mixed_count from outer_only_parent p order by p.seq;
select p.seq,(select count(*) from outer_only_child c where c.value<p.seq and cast(c.text_value as signed)>=0 limit 1) as filtered_count from outer_only_parent p order by p.seq;
select p.seq,(select count(*) from outer_only_child c where c.value<p.seq or c.seq=p.seq limit 1) as disjunction_count from outer_only_parent p order by p.seq;
select (select count(*) from outer_only_child c where c.value=p.seq or c.value>1 limit 1) from outer_only_parent p;
create table aggregate_distinct(k int, value int);
insert into aggregate_distinct values (1,3),(2,3),(3,null),(4,5);
select p.seq,(select count(distinct c.value) from aggregate_distinct c where c.k>p.seq limit 1) as distinct_count from outer_only_parent p order by p.seq;
select p.seq,(select sum(distinct c.value) from aggregate_distinct c where c.k>p.seq limit 1) as distinct_sum from outer_only_parent p order by p.seq;
select p.seq,(select avg(distinct c.value) from aggregate_distinct c where c.k>p.seq limit 1) as distinct_avg from outer_only_parent p order by p.seq;
select p.seq,(select count(distinct c.value) from aggregate_distinct c where p.seq>0 limit 1) as outer_distinct_count from outer_only_parent p order by p.seq;
select p.seq,(select count(distinct c.value+c.k) from aggregate_distinct c where c.k>p.seq limit 1) as expression_count from outer_only_parent p order by p.seq;
select p.seq,(select sum(distinct c.value+c.k) from aggregate_distinct c where c.k>p.seq limit 1) as expression_sum from outer_only_parent p order by p.seq;
select p.seq,(select count(distinct c.value/1) from aggregate_distinct c where c.k>p.seq limit 1) as division_count from outer_only_parent p order by p.seq;
select p.seq,(select count(distinct c.value div 2) from aggregate_distinct c where c.k>p.seq limit 1) as integer_division_count from outer_only_parent p order by p.seq;
select p.seq,(select count(distinct c.value%2) from aggregate_distinct c where c.k>p.seq limit 1) as remainder_count from outer_only_parent p order by p.seq;
select p.seq,(select count(distinct c.value/0) from aggregate_distinct c where c.k>p.seq limit 1) as zero_division_count from outer_only_parent p order by p.seq;
select p.seq,(select count(distinct c.value,c.k) from aggregate_distinct c where c.k>p.seq limit 1) as tuple_count from outer_only_parent p order by p.seq;
select p.seq,(select count(distinct c.value,1) from aggregate_distinct c where c.k>p.seq limit 1) as constant_count from outer_only_parent p order by p.seq;
select p.seq,(select count(distinct c.value,c.k) from aggregate_distinct c where p.seq>0 limit 1) as outer_tuple_count from outer_only_parent p order by p.seq;
select p.seq,(select count(distinct c.value,coalesce(c.k,9)) from aggregate_distinct c where c.k>p.seq limit 1) as coalesced_tuple from outer_only_parent p order by p.seq;
select p.seq,(select count(distinct ifnull(c.value,9),c.k) from aggregate_distinct c where c.k>p.seq limit 1) as ifnull_tuple from outer_only_parent p order by p.seq;
select p.seq,(select count(coalesce(c.value,null)) from aggregate_distinct c where c.k>p.seq limit 1) as nullable_count from outer_only_parent p order by p.seq;
select (select count(distinct coalesce(c.value,9)) from aggregate_distinct c where c.k>p.seq limit 1) from outer_only_parent p;
select (select count(distinct c.value) from aggregate_distinct c where c.k=p.seq limit 1) from outer_only_parent p;
create table outer_range_overflow(k int, value bigint);
insert into outer_range_overflow values (1,-9223372036854775808);
select (select min(abs(c.value)) from outer_range_overflow c where c.k>p.seq limit 1) as unmatched from outer_only_parent p where p.seq=2;
select (select min(abs(c.value)) from outer_range_overflow c where c.k>p.seq limit 1) from outer_only_parent p where p.seq=0;
select (select sum(distinct c.value+c.value) from outer_range_overflow c where c.k>p.seq limit 1) as unmatched from outer_only_parent p where p.seq=2;
select (select sum(distinct c.value+c.value) from outer_range_overflow c where c.k>p.seq limit 1) from outer_only_parent p where p.seq=0;
select (select count(distinct c.k,cast(coalesce(abs(c.value),9) as signed)) from outer_range_overflow c where c.k>p.seq limit 1) as unmatched from outer_only_parent p where p.seq=2;
select (select count(distinct c.k,cast(coalesce(abs(c.value),9) as signed)) from outer_range_overflow c where c.k>p.seq limit 1) from outer_only_parent p where p.seq=0;
-- The non-equality aggregate owner must not discard an OFFSET above its AGG.
select p.seq,(select count(*) from outer_only_child c where p.seq>0 limit 1 offset 1) from outer_only_parent p order by p.seq;
select p.seq,(select count(*) from outer_only_child c where c.value<=p.seq limit 1 offset 1) from outer_only_parent p order by p.seq;
select p.seq,(select count(*) from outer_only_child c where p.seq=1 limit 1 offset 1) as equal_offset from outer_only_parent p order by p.seq;
-- Non-strict aggregates need separate semantics for unmatched rows.
select (select count(coalesce(abs(c.value),9)) from outer_only_child c where p.seq>100 limit 1) from outer_only_parent p;
select (select count(coalesce(c.value,9)) from outer_only_child c where p.seq>100 limit 1) from outer_only_parent p;
insert into outer_only_child values (3,-2147483648,'-2147483648');
select p.seq,(select abs(c.value) from outer_only_child c where p.seq>0 order by c.seq desc limit 1) as abs_min_int,(select c.value+1 from outer_only_child c where p.seq>0 order by c.seq desc limit 1) as incremented from outer_only_parent p order by p.seq;
-- Multiple inner rows only violate scalar cardinality for an outer row that matches.
select p.seq,(select c.value from outer_only_child c where p.seq>100 order by c.seq limit 2) as unmatched from outer_only_parent p order by p.seq;
select (select abs(c.value) from outer_only_child c where p.seq>0 order by c.seq limit 2) from outer_only_parent p where p.seq=1;
select (select min(abs(c.value)) from outer_only_child c where p.seq>0 limit 1 offset 1) from outer_only_parent p;
-- Volatile expressions remain guarded.
select (select uuid() from child c where p.seq>100 order by c.seq limit 1) from outer_only_parent p;
select (select random_bytes(4) from outer_only_child c where p.seq>0 order by c.seq limit 1) from outer_only_parent p;
set @outer_var=1;
select (select c.value+@outer_var from outer_only_child c where p.seq>0 order by c.seq limit 1) from outer_only_parent p;

-- Prepared bounds retain parameter order and NULL extension across reuse.
prepare p_outer from 'select p.seq,(select c.value from outer_only_child c where p.seq>? order by c.seq limit ? offset ?) as value from outer_only_parent p order by p.seq';
set @threshold=0; set @count=0; set @offset=0;
execute p_outer using @threshold,@count,@offset;
set @count=1;
execute p_outer using @threshold,@count,@offset;
set @count=0;
execute p_outer using @threshold,@count,@offset;
set @count=2;
execute p_outer using @threshold,@count,@offset;
deallocate prepare p_outer;
select p.seq,(select coalesce(c.value,99) from outer_only_child c where p.seq>0 order by c.seq limit 1 offset 9223372036854775807) as value from outer_only_parent p order by p.seq;

-- Different bounds and duplicate visible parent values must not share one Top-N.
select p.seq, (select c.event_idx from child c where c.parent_id=p.id and c.event_idx>0 and c.event_idx<=p.ceiling order by c.event_idx desc limit 1) as latest from parent p order by p.seq;
select p.seq, (select c.value from child c where c.parent_id=p.id and c.event_idx>0 and c.event_idx<=p.ceiling order by c.event_idx asc, c.seq asc limit 1) as first_value from parent p order by p.seq;
select p.seq, (select c.value from child c where c.parent_id=p.id and c.event_idx>0 and c.event_idx<=p.ceiling order by c.event_idx desc, c.seq desc limit 1 offset 1) as second_value from parent p order by p.seq;

-- Integer extrema share the existing aggregate owners; nullable ASC still sorts NULL first.
select p.seq,(select c.seq from child c where c.parent_id=p.id order by c.seq limit 1) as first_id from parent p order by p.seq;
select p.seq,(select c.event_idx from child c where c.parent_id=p.id order by c.event_idx limit 1) as first_value from parent p order by p.seq;
select p.seq,case when (select c.seq from child c where c.parent_id=p.id order by c.seq desc limit 1) is null then 1 else 0 end as missing from parent p order by p.seq;
select p.seq,(select c.event_idx from child c where c.parent_id=p.id and c.event_idx<=p.ceiling order by c.event_idx desc limit 1) as latest,(select c.seq from child c where c.parent_id=p.id order by c.seq desc limit 1) as last_id from parent p order by p.seq;

-- Equal-key pagination, NULL order and the distinction between NULL and no row.
select p.seq, (select c.seq from child c where c.parent_id=p.id order by c.event_idx asc,c.seq asc limit 1) as first_seq from parent p order by p.seq;
select p.seq, (select coalesce(c.event_idx,99) from child c where c.parent_id=p.id order by c.seq desc limit 1) as value_or_99 from parent p order by p.seq;
select p.seq, (select 123 from child c where c.parent_id=p.id and p.ceiling>=0 order by c.seq limit 1) as marker from parent p order by p.seq;

-- NULL-safe equality and a mixed OR predicate select the right per-outer set.
select p.seq, (select c.value from child c where c.parent_id <=> p.id order by c.seq desc limit 1) as value from parent p order by p.seq;
select p.seq, (select c.value from child c where c.parent_id=p.id or c.event_idx=p.ceiling order by c.seq desc limit 1) as value from parent p order by p.seq;

-- Pagination can remove the aggregate row; do not replace its NULL with COUNT's empty-group zero.
select p.seq, (select count(*) from child c where c.parent_id=p.id limit 0) as empty_count, (select count(*) from child c where c.parent_id=p.id limit 1 offset 1) as offset_count, (select count(*) from child c where c.parent_id=p.id limit 1) as row_count from parent p order by p.seq;
select p.seq, (select case when count(*)=0 then 99 else count(*) end from child c where c.parent_id=p.id limit 1) as empty_safe_count, (select count(*) from child c where c.parent_id=p.id group by c.event_idx order by c.event_idx limit 1) as grouped_count from parent p order by p.seq;
select p.seq, (select count(c.event_idx) from child c where c.parent_id=p.id group by c.event_idx limit 1) as nonnull_count, (select coalesce(count(c.event_idx),99) from child c where c.parent_id=p.id group by c.event_idx limit 1) as counted, (select count(c.event_idx) from child c where c.parent_id=p.id group by c.event_idx limit 1) is null as no_group from parent p where p.seq in (5,6) order by p.seq;
select p.seq, (select case when count(*)=0 then 99 else count(*) end from child c where p.seq=p.id limit 1) as outer_only_count from parent p where p.seq<=2 order by p.seq;
select p.seq, (select count(*) from child c where c.parent_id=p.id or c.parent_id=p.seq limit 1) from parent p where p.seq=2;

-- A zero LIMIT stays above a correlated non-equality aggregate.
select p.seq, (select count(*) from child c where c.event_idx<=p.ceiling limit 0) as empty_count, (select max(c.event_idx) from child c where c.event_idx<=p.ceiling limit 0) as empty_max from parent p order by p.seq;
select p.seq, (select coalesce(c.event_idx,99) from child c where c.parent_id=p.id limit 0) as correlated_empty, (select coalesce(c.event_idx,99) from child c limit 0) as uncorrelated_empty from parent p order by p.seq;
select p.seq, (select coalesce(sum(c.event_idx),99) from child c where c.parent_id=p.id limit 0) as empty_sum from parent p where p.seq<=2 order by p.seq;
select (select sum(c.event_idx) from child c limit 0) as empty_sum, (select abs(c.event_idx) from child c limit 0) as empty_abs, (select c.event_idx from child c join parent p on c.parent_id=p.id limit 0) as empty_join;

-- Nonempty quantified inputs retain SQL three-valued logic after per-key pagination.
select p.seq, p.ceiling in (select c.event_idx from child c where c.parent_id=p.id order by c.seq limit 2) as in_value, p.ceiling not in (select c.event_idx from child c where c.parent_id=p.id order by c.seq limit 2) as not_in_value from parent p order by p.seq;

-- Empty no-FROM subqueries have scalar NULL and the correct empty-set truth values.
select p.seq, (select p.id limit 0) as empty_value, exists(select p.id limit 0) as exists_empty, p.id in (select 1 limit 0) as in_empty, p.id not in (select 1 limit 0) as not_in_empty from parent p order by p.seq;
select p.seq, exists(select 1 from child c where c.parent_id=p.id and c.event_idx<=p.ceiling limit 1) as present, not exists(select 1 from child c where c.parent_id=p.id and c.event_idx<=p.ceiling limit 1) as absent from parent p order by p.seq;

-- Tied leading rows still leave the same row at the requested offset.
create table tie_child(parent_id int, event_idx bigint, value varchar(20));
insert into tie_child values (2,2,'x'),(2,2,'y'),(2,1,'z');
select (select c.value from tie_child c where c.parent_id=p.id and c.event_idx<=p.ceiling order by c.event_idx desc limit 1 offset 2) as third_value from parent p where p.seq=2;

-- A group key must not be evaluated for unrelated inner rows.
create table group_child(parent_id int, value varchar(20));
insert into group_child values (0,'10'),(2,'bad');
select (select count(*) from group_child c where c.parent_id=p.id group by cast(c.value as signed) limit 1) from parent p where p.seq=1;
select (select c.parent_id from group_child c join group_child d on c.parent_id=d.parent_id and cast(d.value as signed)>0 where c.parent_id=p.id order by c.parent_id limit 1) from parent p where p.seq=1;

-- Scalar LIMIT > 1 retains the normal cardinality error.
select (select c.event_idx from child c where c.parent_id=p.id and c.event_idx>0 order by c.event_idx desc limit 2) from parent p where p.seq=2;

-- Duplicate rows without a primary key must remain separate occurrences.
create table parent_dup(id int, ceiling bigint);
insert into parent_dup values (2,1),(2,1),(2,2);
select p.id,p.ceiling,(select c.event_idx from child c where c.parent_id=p.id and c.event_idx<=p.ceiling order by c.event_idx desc limit 1) as latest from parent_dup p order by p.id,p.ceiling;

-- Workspace Row_IDs must remain unique before transaction commit.
start transaction;
insert into parent_dup values (2,1);
select count(*) as row_count, sum(latest) as latest_sum from (select (select c.event_idx from child c where c.parent_id=p.id and c.event_idx<=p.ceiling order by c.event_idx desc limit 1) as latest from parent_dup p) q;
rollback;

-- Equality keys whose SQL identity is not proven for partitioning use outer identity.
create table str_parent(seq int primary key, k varchar(20));
create table str_child(k varchar(20), v int);
insert into str_parent values (1,'a'),(2,'b'),(3,'c'),(4,null);
insert into str_child values ('a',10),('a',20),('b',30),(null,40);
select p.seq,(select c.v from str_child c where c.k=p.k order by c.v desc limit 1) as value from str_parent p order by p.seq;

-- A UNION ALL outer input has no unique row identity, but equal byte keys can share inner ranks.
insert into str_parent values (5,'d');
insert into str_child values ('d',null);
select p.seq, (select coalesce(c.v,99) from str_child c where c.k<=>p.k order by c.v desc limit 1) as null_safe, (select c.v from str_child c where c.k=p.k order by c.v desc limit 1) as equal_value, (select c.v from str_child c where c.k<=>p.k order by c.v desc limit 1 offset 1) as second_value from (select * from str_parent union all select * from str_parent) p order by p.seq;

-- Fixed-width character keys follow the same equality rule.
create table str_fixed(k char(3), v int);
insert into str_fixed values ('a',10),('a',20),('b',30),(null,40);
select p.v,(select c.v from str_fixed c where c.k<=>p.k order by c.v desc limit 1) as value from (select * from str_fixed union all select * from str_fixed) p order by p.v;
insert into str_fixed values ('a ',50);
select p.v,(select c.v from str_fixed c where c.k<=>p.k order by c.v desc limit 1) as value,g.value as grouped_value from (select * from str_fixed union all select * from str_fixed) p left join (select k,max(v) as value from str_fixed group by k) g on g.k<=>p.k order by p.v;

-- Grouped pagination preserves distinct multibyte keys on the rebased group operator.
create table grouped_parent(id tinyint);
create table grouped_child(parent_id tinyint, label varchar(3) not null);
insert into grouped_parent values (1),(2),(3);
insert into grouped_child values (1,'未付一'),(1,'未付一'),(1,'未付丁'),(1,'未付丁'),(1,'未付丁'),(2,'未付一');
select p.id,(select count(*) from grouped_child c where c.parent_id=p.id group by c.label order by c.label limit 1 offset 1) as grouped_count from grouped_parent p order by p.id;

-- Dynamic pagination of an uncorrelated no-FROM subquery remains executable.
prepare p_limit from 'select (select 42 limit ?) as value';
set @lim=0;
execute p_limit using @lim;
set @lim=1;
execute p_limit using @lim;
deallocate prepare p_limit;
prepare p_empty from 'select (select ? from child limit 0) as value';
set @param=42;
execute p_empty using @param;
deallocate prepare p_empty;

-- UPDATE consumes the same per-row result.
create table update_target(seq int primary key, id int, ceiling bigint, result bigint);
insert into update_target values (1,0,2,null),(2,2,2,null),(3,2,1,null),(4,4,1,null);
update update_target p set result=(select c.event_idx from child c where c.parent_id=p.id and c.event_idx<=p.ceiling order by c.event_idx desc limit 1);
select seq,result from update_target order by seq;
drop database test_correlated_pagination;

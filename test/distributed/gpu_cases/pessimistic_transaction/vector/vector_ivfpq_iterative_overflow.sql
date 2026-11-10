-- IVF-PQ iterative search on a generation that only has the CDC overflow: no IVF list is
-- built, so the first round searches every row and is the last; rows below LIMIT are
-- returned.
--
-- GPU REQUIRED. Lives under pessimistic_transaction/ because it depends on the ISCP CDC
-- consumer appending the rows into the index's overflow.

drop database if exists vector_ivfpq_iterative_overflow;
create database vector_ivfpq_iterative_overflow;
use vector_ivfpq_iterative_overflow;

set experimental_ivfpq_index = 1;

-- an inline index without options is refused like CREATE INDEX: IVF-PQ requires lists
create table t_nolists(id bigint primary key, v vecf32(8), index i_nolists using ivfpq(v));

create table t_tiny(id bigint primary key, note int, v vecf32(8), index i_tiny using ivfpq(v) lists=8);
insert into t_tiny select result, result % 2, cast(concat('[', result, ',0,0,0,0,0,0,0]') as vecf32(8)) from generate_series(1, 5, 1) g;
-- @wait_expect(1, 180)
select group_concat(id order by id) from (select id from t_tiny order by l2_distance(v, '[0,0,0,0,0,0,0,0]') limit 10 by rank with option 'mode=iterative') x;
select group_concat(id order by id) from (select id from t_tiny where id > 2 order by l2_distance(v, '[0,0,0,0,0,0,0,0]') limit 10 by rank with option 'mode=iterative') x;
select group_concat(id order by id) from (select id from t_tiny where note = 1 order by l2_distance(v, '[0,0,0,0,0,0,0,0]') limit 10 by rank with option 'mode=iterative') x;

drop database vector_ivfpq_iterative_overflow;

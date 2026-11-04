-- IndexSearchScan contract (docs/design/20261008-index-search-scan.md):
-- the search table functions are removed, also behind a view; a prepared index search
-- returns, at every execution, the rows of the same search without the index.
drop database if exists index_search_contract;
create database index_search_contract;
use index_search_contract;
set experimental_ivf_index = 1;
set experimental_hnsw_index = 1;
set experimental_fulltext_index = 1;
set experimental_fulltext2_index = 1;

create table src(id bigint primary key, body text, v vecf32(3) not null);
insert into src select result, if(result % 3 = 0, 'needle text', 'plain text'), concat('[', result, ',', result + 1, ',', result + 2, ']') from generate_series(1, 200) g;
create table t_ivf like src;
insert into t_ivf select * from src;
create index vi using ivfflat on t_ivf(v) lists = 4 op_type 'vector_l2_ops';
create table t_hnsw like src;
insert into t_hnsw select * from src;
create index vh using hnsw on t_hnsw(v) op_type 'vector_l2_ops';
create table t_ft like src;
insert into t_ft select * from src;
create fulltext index f on t_ft(body);
create table t_ft2 like src;
insert into t_ft2 select * from src;
create fulltext2 index f on t_ft2(body) with parser ngram;

-- removed search table functions, called directly and through a view
select * from hnsw_search('{}', '{}', '[0,0,0]');
select * from fulltext_index_scan('{}', 'src', 'src', 'needle', 0);
create view v_removed as select * from fulltext2_search('{}', '{}', 'needle', 0);
create view v_removed2 as select * from ivfpq_search('{}', '{}', '[0,0,0]');

-- prepared index searches: each execution equals the unindexed reference
prepare p_ivf from 'select id from t_ivf order by l2_distance(v, ?) limit 3';
prepare p_hnsw from 'select id from t_hnsw order by l2_distance(v, ?) limit 3';
prepare p_ref from 'select id from src order by l2_distance(v, ?) limit 3';
set @q = '[0,0,0]';
execute p_ivf using @q;
execute p_hnsw using @q;
execute p_ref using @q;
set @q = '[100.2,101.2,102.2]';
execute p_ivf using @q;
execute p_hnsw using @q;
execute p_ref using @q;
set @q = '[300,301,302]';
execute p_ivf using @q;
execute p_hnsw using @q;
execute p_ref using @q;

prepare p_ft from 'select id from t_ft where match(body) against(?) order by id limit 4';
prepare p_ft2 from 'select id from t_ft2 where match(body) against(?) order by id limit 4';
prepare p_ft_ref from 'select id from src where body like concat("%", ?, "%") order by id limit 4';
set @w = 'needle';
execute p_ft using @w;
execute p_ft2 using @w;
execute p_ft_ref using @w;
set @w = 'plain';
execute p_ft using @w;
execute p_ft2 using @w;
execute p_ft_ref using @w;
set @w = 'absent';
execute p_ft using @w;
execute p_ft2 using @w;
execute p_ft_ref using @w;

deallocate prepare p_ivf;
deallocate prepare p_hnsw;
deallocate prepare p_ref;
deallocate prepare p_ft;
deallocate prepare p_ft2;
deallocate prepare p_ft_ref;
drop database index_search_contract;
set experimental_ivf_index = 0;
set experimental_hnsw_index = 0;
set experimental_fulltext_index = 0;
set experimental_fulltext2_index = 0;

-- token_filter on the CDC tail: rows inserted after CREATE INDEX are indexed by the async
-- tail build, which must strip stopwords like the CREATE build. A document's indexed length
-- feeds BM25, so 'fox' and 'the the the the fox' score equally only when the stopwords were
-- stripped; the unfiltered index on p scores them differently. The index is created empty
-- and first queried after the tail is written, so no CN serves an earlier cached version.
set experimental_fulltext2_index = 1;
drop database if exists ft2_tokfilter_cdc;
create database ft2_tokfilter_cdc;
use ft2_tokfilter_cdc;

create table s (id bigint primary key, body text);
create fulltext2 index sx on s(body) with parser gojieba position_free = true token_filter = 'stop';
create table p (id bigint primary key, body text);
create fulltext2 index px on p(body) with parser gojieba position_free = true;
insert into s values (1, 'fox'), (2, 'the the the the fox'), (3, 'bear');
insert into p values (1, 'fox'), (2, 'the the the the fox'), (3, 'bear');

set @s_storage = (select index_table_name from mo_catalog.mo_indexes where name = 'sx' and algo = 'fulltext2' and algo_table_type = 'ftv2_index' and table_id in (select rel_id from mo_catalog.mo_tables where reldatabase = database() and relname = 's') limit 1);
set @p_storage = (select index_table_name from mo_catalog.mo_indexes where name = 'px' and algo = 'fulltext2' and algo_table_type = 'ftv2_index' and table_id in (select rel_id from mo_catalog.mo_tables where reldatabase = database() and relname = 'p') limit 1);
set @tail_sql = concat('select (select count(*) > 0 from `', database(), '`.`', @s_storage, '` where index_id = ''cdc_tail'' and tag = 1) and (select count(*) > 0 from `', database(), '`.`', @p_storage, '` where index_id = ''cdc_tail'' and tag = 1) as tails_ready');
prepare wait_tail from @tail_sql;
-- @wait_expect(2, 120)
execute wait_tail;
deallocate prepare wait_tail;

select count(*) as matches, count(distinct match(body) against('fox' in bm25 mode)) as scores from s where match(body) against('fox' in bm25 mode);
select count(*) as matches, count(distinct match(body) against('fox' in bm25 mode)) as scores from p where match(body) against('fox' in bm25 mode);

drop database ft2_tokfilter_cdc;

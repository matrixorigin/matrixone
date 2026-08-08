-- fulltext2 PREPARED-STATEMENT test: MATCH(col) AGAINST(? IN <mode>) with a runtime
-- parameter pattern. The fulltext2 rewrite passes the pattern as a bound expression
-- (not a literal), so a prepared '?' pattern flows into the fulltext2_search TVF and is
-- evaluated per execution. Covers BOOLEAN / NATURAL LANGUAGE (positional index) and
-- BM25 (POSITION_FREE index), plus valid -> empty -> valid -> null -> valid reuse so a
-- failed/empty execution does not wedge the prepared statement.

drop database if exists test_fulltext2_prepare;
create database test_fulltext2_prepare;
use test_fulltext2_prepare;

-- CREATE FULLTEXT2 INDEX is gated behind experimental_fulltext2_index (default off).
set experimental_fulltext2_index = 1;

drop table if exists docs;
create table docs(id bigint primary key, body text);
insert into docs values
 (0,'the quick brown fox jumps'),
 (1,'a quick brown dog'),
 (2,'the lazy fox sleeps'),
 (3,'brown bear and lazy cat'),
 (4,'quick quick quick fox fox');
create fulltext2 index ft on docs(body);

-- ============ prepared BOOLEAN mode ============
prepare ft_bool from 'select id from docs where match(body) against(? in boolean mode) order by id';
set @q = '+quick +fox';
execute ft_bool using @q;
set @q = '';
execute ft_bool using @q;
set @q = '+lazy';
execute ft_bool using @q;
set @q = null;
execute ft_bool using @q;
set @q = '+quick +fox';
execute ft_bool using @q;
deallocate prepare ft_bool;

-- ============ prepared NATURAL LANGUAGE mode (exact phrase) ============
prepare ft_nl from 'select id from docs where match(body) against(? in natural language mode) order by id';
set @q = 'quick brown fox';
execute ft_nl using @q;
set @q = 'brown fox';
execute ft_nl using @q;
set @q = '';
execute ft_nl using @q;
set @q = 'quick brown fox';
execute ft_nl using @q;
deallocate prepare ft_nl;

-- ============ prepared BM25 mode on a POSITION_FREE index (bag-of-words) ============
drop table if exists bw;
create table bw(id bigint primary key, body text);
insert into bw values
 (0,'the quick brown fox jumps'),
 (1,'a quick brown dog'),
 (2,'the lazy fox sleeps'),
 (3,'brown bear and lazy cat'),
 (4,'quick quick quick fox fox');
create fulltext2 index ft on bw(body) with parser gojieba position_free = true;
prepare ft_bm25 from 'select id from bw where match(body) against(? in bm25 mode) order by id';
set @q = 'quick fox';
execute ft_bm25 using @q;
set @q = 'lazy';
execute ft_bm25 using @q;
set @q = 'quick fox';
execute ft_bm25 using @q;
deallocate prepare ft_bm25;

drop database test_fulltext2_prepare;

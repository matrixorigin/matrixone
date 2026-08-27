-- Prepared MATCH() score threshold: MATCH(col) AGAINST(? IN <mode>) <op> ?
--
-- A MATCH can only be answered from the index, so the planner refuses the rewrite --
-- error 20105 -- whenever a document the index never returns (relevance 0) could
-- satisfy the predicate. With a literal threshold it tests that at plan time. A '?'
-- has no value yet, so the same test is carried to the table function as `0 <op> ?`
-- and raised there instead.
--
-- The contract this pins: a prepared threshold behaves EXACTLY like the same literal
-- at every value. Both halves matter -- `> ?` and `>= ?` must work where the literal
-- works (#27400), and must still raise 20105 where the literal does.
--
-- Binary-protocol COM_STMT_PREPARE is not reachable from mo-tester; it shares this
-- plan path, and the issue reports the same failure for both.
drop database if exists ft_score_param;
create database ft_score_param;
use ft_score_param;

create table docs(id bigint primary key, body text not null);
insert into docs values
  (1,'fox zeta zeta'),
  (2,'fox fox zeta'),
  (3,'fox fox fox'),
  (4,'alpha alpha');
create fulltext index ft on docs(body);

-- ============ literal controls: what a prepared threshold must reproduce ============
select id from docs where match(body) against('fox' in natural language mode) > 0 order by id;
select id from docs where match(body) against('fox' in natural language mode) >= 0.001 order by id;
-- relevance 0 satisfies `>= 0`, so the index cannot answer it
select id from docs where match(body) against('fox' in natural language mode) >= 0 order by id;
-- ...nor `> -1`
select id from docs where match(body) against('fox' in natural language mode) > -1 order by id;
-- ...nor a MATCH on the lesser side
select id from docs where 0 > match(body) against('fox' in natural language mode) order by id;

-- ============ both operands parameterized ============
prepare both_stmt from 'select id from docs where match(body) against(? in natural language mode) > ? order by id';
set @term='fox';
set @score=0;
execute both_stmt using @term,@score;
-- repeated execution must reuse the prepared plan and give the same answer
execute both_stmt using @term,@score;
-- a positive threshold selects the higher-scoring subset
set @hi=0.04;
execute both_stmt using @term,@hi;
-- a negative threshold is the literal `> -1` case: 20105, not a silently smaller answer
set @neg=-1;
execute both_stmt using @term,@neg;
-- reuse after the refusal still works
execute both_stmt using @term,@score;
deallocate prepare both_stmt;

-- ============ threshold-only parameterized (literal search term) ============
prepare score_stmt from 'select id from docs where match(body) against(''fox'' in natural language mode) > ? order by id';
set @score=0;
execute score_stmt using @score;
deallocate prepare score_stmt;

-- ============ term-only parameterized (literal threshold) ============
prepare term_stmt from 'select id from docs where match(body) against(? in natural language mode) > 0 order by id';
set @term='fox';
execute term_stmt using @term;
deallocate prepare term_stmt;

-- ============ >= with a runtime threshold ============
prepare ge_stmt from 'select id from docs where match(body) against(? in natural language mode) >= ? order by id';
set @term='fox';
-- strictly positive: relevance 0 fails it, so the index can drive
set @pos=0.001;
execute ge_stmt using @term,@pos;
-- zero: relevance 0 satisfies `>= 0`, so 20105 -- the same answer as the literal
set @zero=0;
execute ge_stmt using @term,@zero;
deallocate prepare ge_stmt;

-- ============ CAST(? AS DOUBLE) threshold ============
prepare cast_stmt from 'select id from docs where match(body) against(? in natural language mode) > cast(? as double) order by id';
set @term='fox';
set @score=0;
execute cast_stmt using @term,@score;
deallocate prepare cast_stmt;

-- ============ reversed operand order ============
-- `? < MATCH(...)` is `MATCH(...) > ?`, still membership-implying
prepare rev_stmt from 'select id from docs where ? < match(body) against(? in natural language mode) order by id';
set @score=0;
set @term='fox';
execute rev_stmt using @score,@term;
deallocate prepare rev_stmt;
-- `? > MATCH(...)` is `MATCH(...) < ?`, which relevance 0 can satisfy: 20105
prepare revbad_stmt from 'select id from docs where ? > match(body) against(? in natural language mode) order by id';
set @score=0;
set @term='fox';
execute revbad_stmt using @score,@term;
deallocate prepare revbad_stmt;

-- ============ AND with an ordinary predicate ============
prepare and_stmt from 'select id from docs where match(body) against(? in natural language mode) > ? and id > 1 order by id';
set @term='fox';
set @score=0;
execute and_stmt using @term,@score;
deallocate prepare and_stmt;

-- ============ operators the literal form cannot use, the parameter form cannot either ============
-- `MATCH(...) < c` is not membership-implying for ANY c: a relevance-0 document
-- satisfies it whenever c > 0, and nothing satisfies it when c <= 0. The literal form
-- therefore raises 20105 at every value, and the parameter form must do the same --
-- otherwise binding 0 would quietly gain an evaluation path the literal never has.
select id from docs where match(body) against('fox' in natural language mode) < 5 order by id;
select id from docs where match(body) against('fox' in natural language mode) < 0 order by id;

prepare lt_stmt from 'select id from docs where match(body) against(? in natural language mode) < ? order by id';
set @term='fox';
set @s=0;
execute lt_stmt using @term,@s;
set @s=5;
execute lt_stmt using @term,@s;
deallocate prepare lt_stmt;

prepare le_stmt from 'select id from docs where match(body) against(? in natural language mode) <= ? order by id';
set @term='fox';
set @s=0;
execute le_stmt using @term,@s;
deallocate prepare le_stmt;

-- ============ two thresholds on the same MATCH ============
-- A relevance-0 document satisfies a CONJUNCTION only when it satisfies every part, so
-- these must be ANDed. Combining them the other way refuses `> ? AND < ?` at (0,5),
-- which the identical literals are accepted for.
--
-- The search term is a literal on both halves ON PURPOSE: prepared parameters are
-- positional, so writing `against(?)` twice makes two structurally different MATCHes
-- rather than two thresholds on one, and the `<` half then has no membership-implying
-- predicate of its own to be served by.
select id from docs where match(body) against('fox' in natural language mode) > 0 and match(body) against('fox' in natural language mode) < 5 order by id;

prepare band_stmt from 'select id from docs where match(body) against(''fox'' in natural language mode) > ? and match(body) against(''fox'' in natural language mode) < ? order by id';
set @lo=0;
set @hi=5;
execute band_stmt using @lo,@hi;
-- relevance 0 satisfies BOTH halves here (0 > -1 and 0 < 5), so the index cannot answer it
set @neg=-1;
execute band_stmt using @neg,@hi;
deallocate prepare band_stmt;

-- A LITERAL half that already excludes relevance 0 makes the rewrite safe whatever the
-- runtime half is, so no guard is emitted at all.
prepare mixed_stmt from 'select id from docs where match(body) against(''fox'' in natural language mode) > 0 and match(body) against(''fox'' in natural language mode) < ? order by id';
set @hi=5;
execute mixed_stmt using @hi;
deallocate prepare mixed_stmt;

-- Two DIFFERENT MATCHes, one of which has only a non-membership-implying predicate:
-- unserved, so 20105 -- the behaviour a literal pair has too.
prepare twomatch_stmt from 'select id from docs where match(body) against(? in natural language mode) > ? and match(body) against(? in natural language mode) < ? order by id';
set @term='fox';
set @lo=0;
set @hi=5;
execute twomatch_stmt using @term,@lo,@term,@hi;
deallocate prepare twomatch_stmt;

-- ============ FULLTEXT2 / BM25 ============
set experimental_fulltext2_index = 1;
create table docs2(id bigint primary key, body text not null);
insert into docs2 select * from docs;
create fulltext2 index ft2 on docs2(body);

select id from docs2 where match(body) against('fox' in bm25 mode) > 0 order by id;

prepare ft2_stmt from 'select id from docs2 where match(body) against(? in bm25 mode) > ? order by id';
set @term='fox';
set @score=0;
execute ft2_stmt using @term,@score;
execute ft2_stmt using @term,@score;
set @neg=-1;
execute ft2_stmt using @term,@neg;
execute ft2_stmt using @term,@score;
deallocate prepare ft2_stmt;

prepare ft2_ge from 'select id from docs2 where match(body) against(? in bm25 mode) >= ? order by id';
set @term='fox';
set @zero=0;
execute ft2_ge using @term,@zero;
deallocate prepare ft2_ge;

drop database ft_score_param;

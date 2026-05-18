-- LIKE ... ESCAPE syntax (#23104)

drop table if exists t_like_escape;
create table t_like_escape (a varchar(100));
insert into t_like_escape values ('a_b'), ('a%b'), ('a_c'), ('A_B');

-- 1. LIKE with default escape \ to match literal _
select * from t_like_escape where a like 'a\_b' escape '\\';

-- 2. LIKE with escape to match literal %
select * from t_like_escape where a like 'a\%b' escape '\\';

-- 3. LIKE with custom escape char #
select * from t_like_escape where a like 'a#_b' escape '#';

-- 4. ILIKE with escape (case insensitive)
select * from t_like_escape where a ilike 'a\_b' escape '\\';

-- 5. NOT LIKE with escape
select * from t_like_escape where a not like 'a\_b' escape '\\';

-- 6. NOT ILIKE with escape
select * from t_like_escape where a not ilike 'a\_b' escape '\\';

-- 7. ESCAPE '' disables escaping: _ is wildcard, matches any single char
select * from t_like_escape where a like 'a_b' escape '';

-- 8. ESCAPE '' with literal % wildcard
select * from t_like_escape where a like 'a%b' escape '';

-- 9. Multi-char escape should report error
select * from t_like_escape where a like 'a\_b' escape 'ab';

drop table t_like_escape;

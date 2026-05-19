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

-- 10. Row-dependent ESCAPE from a column
drop table if exists t_like_esc_col;
create table t_like_esc_col (val varchar(100), esc varchar(1));
insert into t_like_esc_col values ('a_b', '\\'), ('a_c', '\\'), ('A_B', '\\');
select * from t_like_esc_col where val like 'a\_b' escape esc;

-- 11. Row-dependent ESCAPE: different escape per row
insert into t_like_esc_col values ('a_b', '#');
select * from t_like_esc_col where val like 'a#_b' escape esc;

-- 12. ESCAPE NULL returns NULL for all rows
select * from t_like_escape where a like 'a_b' escape null;

-- 13. NOT LIKE ESCAPE NULL also returns no rows
select * from t_like_escape where a not like 'a_b' escape null;

drop table t_like_esc_col;
drop table t_like_escape;

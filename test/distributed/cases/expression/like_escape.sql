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

-- 14. ILIKE ESCAPE NULL returns no rows
select * from t_like_escape where a ilike 'a_b' escape null;

-- 15. NOT ILIKE ESCAPE NULL also returns no rows
select * from t_like_escape where a not ilike 'a_b' escape null;

-- 16. ILIKE row-dependent ESCAPE from column (case insensitive)
select * from t_like_esc_col where val ilike 'a\_b' escape esc;

-- 17. ILIKE row-dependent ESCAPE: different escape per row
select * from t_like_esc_col where val ilike 'a#_b' escape esc;

-- 18. LIKE row-dependent ESCAPE: NULL escape in a row returns NULL for that row
drop table if exists t_esc_null;
create table t_esc_null (val varchar(100), esc varchar(10));
insert into t_esc_null values ('a_b', '\\'), ('a_b', null);
select * from t_esc_null where val like 'a\_b' escape esc;

-- 19. ILIKE row-dependent ESCAPE: NULL escape in a row
select * from t_esc_null where val ilike 'a\_b' escape esc;

-- 20. LIKE row-dependent ESCAPE: multi-char escape in a row reports error
drop table if exists t_esc_multi;
create table t_esc_multi (val varchar(100), esc varchar(10));
insert into t_esc_multi values ('a_b', '\\'), ('a_b', 'ab');
select * from t_esc_multi where val like 'a\_b' escape esc;

-- 21. ILIKE row-dependent ESCAPE: multi-char escape in a row reports error
select * from t_esc_multi where val ilike 'a\_b' escape esc;

drop table t_esc_null;
drop table t_esc_multi;
drop table t_like_esc_col;
drop table t_like_escape;

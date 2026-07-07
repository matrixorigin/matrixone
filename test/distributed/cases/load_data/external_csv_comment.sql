-- CSV reader COMMENT option for external tables. The same fixture
-- (external_table_file/csv_comment.csv) is read three ways:
--   #c1,c2          <- '#'-prefixed, also valid 2-column data
--   1,alice
--   REMx,REMy       <- 'REM'-prefixed, also valid 2-column data
--   2,bob
--   "#quoted",enc   <- enclosed value beginning with '#': raw prefix is the
--                      quote, so it is data under any comment marker
--   3,#midhash      <- '#' is mid-line (second field): never a comment
-- A comment line is matched on the line's RAW prefix before unquoting.

drop database if exists csvcmt;
create database csvcmt;
use csvcmt;

-- ---------- default: no comment marker, every line is data ----------
drop table if exists t_default;
create external table t_default(a varchar(20), b varchar(20))
infile{'filepath'='$resources/external_table_file/csv_comment.csv', 'format'='csv'}
fields terminated by ',';
select a, b from t_default order by a;
select count(*) from t_default;

-- ---------- comment = '#': lines whose raw prefix is '#' are skipped ----------
-- The '#c1,c2' line is skipped; the enclosed "#quoted" and the mid-line
-- '#midhash' are data.
drop table if exists t_hash;
create external table t_hash(a varchar(20), b varchar(20))
infile{'filepath'='$resources/external_table_file/csv_comment.csv', 'format'='csv', 'comment'='#'}
fields terminated by ',';
select a, b from t_hash order by a;
select count(*) from t_hash;

-- ---------- comment = 'REM': lines whose raw prefix is 'REM' are skipped ----------
-- The 'REMx,REMy' line is skipped; '#'-prefixed lines are data here.
drop table if exists t_rem;
create external table t_rem(a varchar(20), b varchar(20))
infile{'filepath'='$resources/external_table_file/csv_comment.csv', 'format'='csv', 'comment'='REM'}
fields terminated by ',';
select a, b from t_rem order by a;
select count(*) from t_rem;

drop table if exists t_default;
drop table if exists t_hash;
drop table if exists t_rem;
drop database if exists csvcmt;

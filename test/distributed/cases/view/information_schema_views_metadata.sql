-- @label:bvt
drop database if exists information_schema_views_metadata;
create database information_schema_views_metadata;
use information_schema_views_metadata;

create table t(a int, b int);
insert into t values (1, 10), (1, 20), (2, 30);
create view direct_v as select a, b from t;
create view agg_v as select a, count(*) cnt from t group by a;
/* migration */ create view leading_block_comment_v as select a from t;
/*!50001 CREATE DEFINER = `root`@`%` VIEW dump_v AS select a from t */;
/*!50001 CREATE ALGORITHM=UNDEFINED */
/*!50013 DEFINER=`user view fake as select 0`@`%` SQL SECURITY DEFINER */
/*!50001 VIEW split_dump_v AS select a from t */;
create view line_comment_v -- migration-generated view
as select a from t;
create view hash_comment_v # migration-generated view
as select a from t;
create view slash_comment_v // migration-generated view
as select a from t;
create view block_comment_v /* migration-generated view */ as select a from t;
create view adjacent_block_comment_v/* migration-generated view */as select a from t;
create /* migration view fake as */ view block_before_view_v as select a from t;
create view repeated_star_comment_v /***/ as select a from t;
create view long_repeated_star_comment_v /*****/ as select a from t;
/*! CREATE VIEW executable_without_version_v AS select a from t */;
/*!50001 CREATE VIEW executable_trailing_comment_v AS select a from t */ /* application */;
/*!50001 CREATE VIEW executable_string_terminator_v AS select 'x*/y' as s */;
CREATE DEFINER=' view fake as select 0'@'%' VIEW quoted_definer_v AS select a from t;
CREATE DEFINER=' view fake \' VIEW fake AS select 0'@'%' VIEW escaped_quoted_definer_v AS select a from t;

select table_name, view_definition, is_updatable
from information_schema.views
where table_schema = 'information_schema_views_metadata'
order by table_name;

update agg_v set cnt = 1;
update direct_v set b = 1;

drop database information_schema_views_metadata;

-- The stored VIEWS definition must remain executable when a system database is cloned.
drop database if exists information_schema_views_clone;
create database information_schema_views_clone clone information_schema;
drop database information_schema_views_clone;

-- SHOW CREATE VIEW must return the stored definition unchanged, backslashes included (#31070).
drop database if exists show_create_view_escape;
create database show_create_view_escape;
use show_create_view_escape;
create view v1 as select 'C:\\new' as p, 'it''s' as q, "dq""x" as r;
select hex(p), q, r from v1;
show create view v1;
drop database show_create_view_escape;

-- Regression coverage for MatrixOne issue #31663.
-- mo_ctl flush must classify bad targets as 20301 invalid input with a usage
-- hint and a missing database as the typed unknown-database error instead of
-- leaking the internal OkExpectedEOB sentinel; valid flushes are unchanged.
-- Errors must not poison the session: SELECT 1 and the next valid flush on
-- the same connection keep working.
drop database if exists issue_31663_flush_ctl;
create database issue_31663_flush_ctl;
create table issue_31663_flush_ctl.t(a int);
insert into issue_31663_flush_ctl.t values (1);
select mo_ctl('dn', 'flush', 'issue_31663_flush_ctl.t');
select mo_ctl('dn', 'flush', 'a');
select 1;
select mo_ctl('dn', 'flush', 'issue_31663_flush_ctl.t');
select mo_ctl('dn', 'flush', 'issue_31663_flush_ctl.nosuch');
select mo_ctl('dn', 'flush', 'nosuch_db_31663.t');
select mo_ctl('dn', 'flush', '');
select mo_ctl('dn', 'flush', 'a.b.c');
select mo_ctl('dn', 'flush', 'a.b.c.d');
select mo_ctl('dn', 'flush', 'issue_31663_flush_ctl.');
drop database issue_31663_flush_ctl;

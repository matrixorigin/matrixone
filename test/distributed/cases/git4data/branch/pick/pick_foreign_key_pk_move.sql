-- PICK of a source primary-key move must replay as one native UPDATE with
-- declared ON UPDATE actions when the picked key is the move's OLD key
-- (issue #28026). Picking the NEW key alone selects the moved image as a
-- plain insert: the old destination row was never selected and must stay.
drop database if exists branch_pick_fk_pk_move;
create database branch_pick_fk_pk_move;
use branch_pick_fk_pk_move;

-- Fault-point arming discipline (D185(B.1) fencing certification): every
-- data branch statement in this file runs against a DISABLED fault
-- framework, so each arm lives inside a tight
-- enable->add->...->remove->disable window that never encloses a fencing
-- statement. The single recorded exception is the cov1 probe-budget cell
-- below: its pick executes while armed and witnesses exactly the
-- certification envelope rejection plus failure atomicity.

create table parent_t (id int primary key, payload varchar(32));
create table child_t (
    id int primary key,
    parent_id int,
    constraint fk_child_parent foreign key (parent_id) references parent_t(id) on update cascade
);
insert into parent_t values (2, 'two'), (8, 'eight'), (9, 'nine');
insert into child_t values (1, 2), (2, 8);

data branch create table move_src from parent_t;
update move_src set id = 3 where id = 2;
update move_src set id = 10 where id = 9;
update move_src set payload = 'eight-edited' where id = 8;

-- The issue case: picking the OLD key of a moved row cascades.
data branch pick move_src into parent_t keys(2) when conflict accept;
select id, payload from parent_t order by id;
select id, parent_id from child_t order by id;

-- Picking the NEW key of a moved row installs the moved image as a plain
-- insert; the old key was not selected, so the destination row at the old
-- key stays untouched (no native UPDATE is produced).
data branch pick move_src into parent_t keys(10) when conflict accept;
select id, payload from parent_t order by id;
select id, parent_id from child_t order by id;

-- A stable-key picked update still lands without disturbing the moved rows.
data branch pick move_src into parent_t keys(8) when conflict accept;
select id, payload from parent_t order by id;
select id, parent_id from child_t order by id;

-- Counterexample: an independent delete+insert pair keeps its meaning, so
-- picking its old key fails the foreign-key check atomically.
data branch create table delins_src from parent_t;
delete from delins_src where id = 3;
insert into delins_src values (4, 'four');
-- @regex("Cannot delete or update a parent row: a foreign key constraint fails", true)
data branch pick delins_src into parent_t keys(3) when conflict accept;
select id, payload from parent_t order by id;
select id, parent_id from child_t order by id;

-- Both branches moved the picked row to different keys: ACCEPT applies the
-- source move, and the child re-parents to the source key.
create table bmacc_p (id int primary key, v varchar(32));
create table bmacc_c (
    id int primary key,
    pid int,
    constraint fk_bmacc foreign key (pid) references bmacc_p(id) on update cascade
);
insert into bmacc_p values (2, 'two');
insert into bmacc_c values (1, 2);
data branch create table bmacc_src from bmacc_p;
update bmacc_src set id = 3, v = 'three' where id = 2;
update bmacc_p set id = 4, v = 'four' where id = 2;
data branch pick bmacc_src into bmacc_p keys(2) when conflict accept;
select id, v from bmacc_p order by id;
select id, pid from bmacc_c order by id;

-- SKIP keeps the destination move.
create table bmskip_p (id int primary key, v varchar(32));
create table bmskip_c (
    id int primary key,
    pid int,
    constraint fk_bmskip foreign key (pid) references bmskip_p(id) on update cascade
);
insert into bmskip_p values (2, 'two');
insert into bmskip_c values (1, 2);
data branch create table bmskip_src from bmskip_p;
update bmskip_src set id = 3, v = 'three' where id = 2;
update bmskip_p set id = 4, v = 'four' where id = 2;
data branch pick bmskip_src into bmskip_p keys(2) when conflict skip;
select id, v from bmskip_p order by id;
select id, pid from bmskip_c order by id;

-- FAIL aborts atomically.
create table bmfail_p (id int primary key, v varchar(32));
insert into bmfail_p values (2, 'two');
data branch create table bmfail_src from bmfail_p;
update bmfail_src set id = 3, v = 'three' where id = 2;
update bmfail_p set id = 4, v = 'four' where id = 2;
-- @regex("with different values", true)
data branch pick bmfail_src into bmfail_p keys(2);
select id, v from bmfail_p order by id;

-- Counterexample: a delete and an insert committed as one transaction are two
-- independent operations, never an update; picking the deleted key keeps
-- delete semantics and fails the foreign-key check atomically.
create table tx_p (id int primary key, v varchar(32));
create table tx_c (
    id int primary key,
    pid int,
    constraint fk_tx foreign key (pid) references tx_p(id) on update cascade
);
insert into tx_p values (2, 'two');
insert into tx_c values (1, 2);
data branch create table tx_src from tx_p;
begin;
delete from tx_src where id = 2;
insert into tx_src values (5, 'five');
commit;
-- @regex("Cannot delete or update a parent row: a foreign key constraint fails", true)
data branch pick tx_src into tx_p keys(2) when conflict accept;
select id, v from tx_p order by id;
select id, pid from tx_c order by id;

-- A row moved before the branch fork leaves no picked difference for its old
-- key; the destination is untouched.
create table prefork_p (id int primary key, v varchar(32));
insert into prefork_p values (2, 'two');
update prefork_p set id = 3 where id = 2;
data branch create table prefork_src from prefork_p;
create table prefork_dst (id int primary key, v varchar(32));
insert into prefork_dst values (7, 'seven');
data branch pick prefork_src into prefork_dst keys(2) when conflict accept;
select id, v from prefork_dst order by id;

-- Destination deleted the picked row while the source moved it: ACCEPT
-- wins the lineage back through the recorded provenance, restoring the
-- deleted row at its landed key with the source's new values (the
-- destination's delete is the losing side, so nothing is ambiguous).
create table delpick_p (id int primary key, v varchar(32));
insert into delpick_p values (2, 'two'), (9, 'nine');
data branch create table delpick_src from delpick_p;
update delpick_src set id = 3, v = 'three' where id = 2;
delete from delpick_p where id = 2;
data branch pick delpick_src into delpick_p keys(2) when conflict accept;
select id, v from delpick_p order by id;

-- A destination row independently occupying the picked move's NEW key is a
-- conflict; a bare PICK resolves it fail-like and aborts atomically, like a
-- bare same-kind overlap.
create table nkpfail_p (id int primary key, v varchar(32));
create table nkpfail_c (
    id int primary key,
    pid int,
    constraint fk_nkpfail foreign key (pid) references nkpfail_p(id) on update cascade
);
insert into nkpfail_p values (2, 'two');
insert into nkpfail_c values (1, 2);
data branch create table nkpfail_src from nkpfail_p;
insert into nkpfail_p values (3, 'dst-three');
insert into nkpfail_c values (2, 3);
update nkpfail_src set id = 3, v = 'three-src' where id = 2;
-- @regex("conflict: nkpfail_src", true)
data branch pick nkpfail_src into nkpfail_p keys(2);
select id, v from nkpfail_p order by id;
select id, pid from nkpfail_c order by id;

-- SKIP keeps the destination occupant and drops the move as a whole.
create table nkpskip_p (id int primary key, v varchar(32));
create table nkpskip_c (
    id int primary key,
    pid int,
    constraint fk_nkpskip foreign key (pid) references nkpskip_p(id) on update cascade
);
insert into nkpskip_p values (2, 'two');
insert into nkpskip_c values (1, 2);
data branch create table nkpskip_src from nkpskip_p;
insert into nkpskip_p values (3, 'dst-three');
insert into nkpskip_c values (2, 3);
update nkpskip_src set id = 3, v = 'three-src' where id = 2;
data branch pick nkpskip_src into nkpskip_p keys(2) when conflict skip;
select id, v from nkpskip_p order by id;
select id, pid from nkpskip_c order by id;

-- ACCEPT routes the destination occupant through the conflict pipeline: it
-- leaves with its ON DELETE CASCADE child, and the picked move lands at the
-- contested key with its own child re-parented there.
create table nkpacc_p (id int primary key, v varchar(32));
create table nkpacc_c (
    id int primary key,
    pid int,
    constraint fk_nkpacc foreign key (pid) references nkpacc_p(id)
        on update cascade on delete cascade
);
insert into nkpacc_p values (2, 'two');
insert into nkpacc_c values (1, 2);
data branch create table nkpacc_src from nkpacc_p;
insert into nkpacc_p values (3, 'dst-three');
insert into nkpacc_c values (2, 3);
update nkpacc_src set id = 3, v = 'three-src' where id = 2;
data branch pick nkpacc_src into nkpacc_p keys(2) when conflict accept;
select id, v from nkpacc_p order by id;
select id, pid from nkpacc_c order by id;

-- Reused intermediate keys admit only the picked lineage: lineage A moves
-- 2->3->5 while lineage B reuses the vacated 3 with 4->3. Picking key 2
-- applies lineage A alone and leaves lineage B at 4 with its child;
-- picking key 4 afterwards applies lineage B.
create table rk_p (id int primary key, v varchar(32));
create table rk_c (
    id int primary key,
    pid int,
    constraint fk_rk foreign key (pid) references rk_p(id) on update cascade
);
insert into rk_p values (2, 'two'), (4, 'four');
insert into rk_c values (1, 2), (2, 4);
data branch create table rk_src from rk_p;
update rk_src set id = 3 where id = 2;
update rk_src set id = 5 where id = 3;
update rk_src set id = 3 where id = 4;
data branch pick rk_src into rk_p keys(2) when conflict accept;
select id, v from rk_p order by id;
select id, pid from rk_c order by id;
data branch pick rk_src into rk_p keys(4) when conflict accept;
select id, v from rk_p order by id;
select id, pid from rk_c order by id;

-- A multi-generation branch folds moves recorded on every physical table on
-- the branch path: picking the leaf's move cascades through both recorded
-- hops as one native destination update.
create table mgp_p (id int primary key, v varchar(32));
create table mgp_c (
    id int primary key,
    pid int,
    constraint fk_mgp foreign key (pid) references mgp_p(id) on update cascade
);
insert into mgp_p values (2, 'two');
insert into mgp_c values (1, 2);
data branch create table mgp1 from mgp_p;
update mgp1 set id = 3 where id = 2;
data branch create table mgp2 from mgp1;
update mgp2 set id = 4 where id = 3;
data branch pick mgp2 into mgp_p keys(2) when conflict accept;
select id, v from mgp_p order by id;
select id, pid from mgp_c order by id;

-- PICK of an unrelated lineage reborn at the move's OLD key fails closed:
-- separately committed DELETE 2; INSERT (2, 'unrelated'); UPDATE 2->3 is
-- delete+insert semantics, and folding would re-parent destination children
-- onto the unrelated lineage, so the foreign key rejects it atomically.
create table rip_p (id int primary key, v varchar(32));
create table rip_c (
    id int primary key,
    pid int,
    constraint fk_rip foreign key (pid) references rip_p(id) on update cascade
);
insert into rip_p values (2, 'two');
insert into rip_c values (1, 2);
data branch create table rip_src from rip_p;
delete from rip_src where id = 2;
insert into rip_src values (2, 'unrelated');
update rip_src set id = 3 where id = 2;
-- @regex("Cannot delete or update a parent row: a foreign key constraint fails", true)
data branch pick rip_src into rip_p keys(2) when conflict accept;
select id, v from rip_p order by id;
select id, pid from rip_c order by id;

-- Interacting moves apply in dependency order when both lineages are
-- picked: moving 3->4 first vacates 3 for the 2->3 move, so both
-- destination rows and both children survive.
create table imp_p (id int primary key, v varchar(32));
create table imp_c (
    id int primary key,
    pid int,
    constraint fk_imp foreign key (pid) references imp_p(id) on update cascade
);
insert into imp_p values (2, 'two'), (3, 'three');
insert into imp_c values (1, 2), (2, 3);
data branch create table imp_src from imp_p;
update imp_src set id = 4 where id = 3;
update imp_src set id = 3 where id = 2;
data branch pick imp_src into imp_p keys(2, 3) when conflict accept;
select id, v from imp_p order by id;
select id, pid from imp_c order by id;

-- A destination occupant unchanged since the fork is never silently
-- deleted by a picked move, whichever conflict mode applies: SKIP drops
-- the move whole, bare and FAIL report the conflict, ACCEPT meets the raw
-- duplicate-key guard atomically, and every attempt leaves row 3 and its
-- child alive.
create table uo_p (id int primary key, v varchar(32));
create table uo_c (
    id int primary key,
    pid int,
    constraint fk_uo foreign key (pid) references uo_p(id)
        on update cascade on delete cascade
);
insert into uo_p values (2, 'two'), (3, 'three');
insert into uo_c values (1, 2), (2, 3);
data branch create table uo_src from uo_p;
delete from uo_src where id = 3;
update uo_src set id = 3 where id = 2;
data branch pick uo_src into uo_p keys(2) when conflict skip;
select id, v from uo_p order by id;
select id, pid from uo_c order by id;
-- @regex("conflict: uo_src UPDATE and uo_p INSERT on pk", true)
data branch pick uo_src into uo_p keys(2);
select id, v from uo_p order by id;
select id, pid from uo_c order by id;
-- @regex("conflict: uo_src UPDATE and uo_p INSERT on pk", true)
data branch pick uo_src into uo_p keys(2) when conflict fail;
select id, v from uo_p order by id;
select id, pid from uo_c order by id;
-- @regex("Duplicate entry '3' for key 'id'", true)
data branch pick uo_src into uo_p keys(2) when conflict accept;
select id, v from uo_p order by id;
select id, pid from uo_c order by id;

-- Era-4: repeated same-table move chains committed by two transactions both
-- commit cleanly, and picking the chain's first old key folds the whole
-- cross-transaction lineage into the destination.
create table pxn_p (id int primary key, v varchar(32));
create table pxn_c (
    id int primary key,
    pid int,
    constraint fk_pxn foreign key (pid) references pxn_p(id) on update cascade
);
insert into pxn_p values (2, 'two');
insert into pxn_c values (1, 2);
data branch create table pxn_src from pxn_p;
begin;
update pxn_src set id = 3 where id = 2;
update pxn_src set id = 4 where id = 3;
commit;
begin;
update pxn_src set id = 3 where id = 4;
update pxn_src set id = 5 where id = 3;
commit;
set @pxnl = (select rel_id from mo_catalog.mo_tables where relname = 'pxn_src' and reldatabase = 'branch_pick_fk_pk_move');
select count(*) from mo_catalog.mo_data_branch_update_link where table_id = @pxnl;
data branch pick pxn_src into pxn_p keys(2) when conflict accept;
select id, v from pxn_p order by id;
select id, pid from pxn_c order by id;

-- Era-4: a non-key edit before a move in one transaction keeps the pick
-- anchor on the durable pre-image; the edited payload travels.
create table peb_p (id int primary key, v varchar(32));
create table peb_c (
    id int primary key,
    pid int,
    constraint fk_peb foreign key (pid) references peb_p(id) on update cascade
);
insert into peb_p values (2, 'two');
insert into peb_c values (1, 2);
data branch create table peb_src from peb_p;
begin;
update peb_src set v = 'pre' where id = 2;
update peb_src set id = 3 where id = 2;
commit;
data branch pick peb_src into peb_p keys(2) when conflict accept;
select id, v from peb_p order by id;
select id, pid from peb_c order by id;

-- Era-4: a non-key edit after a move in one transaction: the continuity
-- marker retargets the recorded link and is dropped, exactly one link
-- commits, and the pick applies the edited row natively.
create table pee_p (id int primary key, v varchar(32));
create table pee_c (
    id int primary key,
    pid int,
    constraint fk_pee foreign key (pid) references pee_p(id) on update cascade
);
insert into pee_p values (2, 'two');
insert into pee_c values (1, 2);
data branch create table pee_src from pee_p;
begin;
update pee_src set id = 3 where id = 2;
update pee_src set v = 'post' where id = 3;
commit;
set @peel = (select rel_id from mo_catalog.mo_tables where relname = 'pee_src' and reldatabase = 'branch_pick_fk_pk_move');
select count(*) from mo_catalog.mo_data_branch_update_link where table_id = @peel;
data branch pick pee_src into pee_p keys(2) when conflict accept;
select id, v from pee_p order by id;
select id, pid from pee_c order by id;

-- Era-4: a non-key edit between two moves of one row in one transaction.
create table pem_p (id int primary key, v varchar(32));
create table pem_c (
    id int primary key,
    pid int,
    constraint fk_pem foreign key (pid) references pem_p(id) on update cascade
);
insert into pem_p values (2, 'two');
insert into pem_c values (1, 2);
data branch create table pem_src from pem_p;
begin;
update pem_src set id = 3 where id = 2;
update pem_src set v = 'mid' where id = 3;
update pem_src set id = 4 where id = 3;
commit;
set @peml = (select rel_id from mo_catalog.mo_tables where relname = 'pem_src' and reldatabase = 'branch_pick_fk_pk_move');
select count(*) from mo_catalog.mo_data_branch_update_link where table_id = @peml;
data branch pick pem_src into pem_p keys(2) when conflict accept;
select id, v from pem_p order by id;
select id, pid from pem_c order by id;

-- Era-4 guard: a move whose landed generation dies inside the same
-- transaction is not rescued by markers; the pick fails the FK atomically.
create table pea_p (id int primary key, v varchar(32));
create table pea_c (
    id int primary key,
    pid int,
    constraint fk_pea foreign key (pid) references pea_p(id) on update cascade
);
insert into pea_p values (2, 'two');
insert into pea_c values (1, 2);
data branch create table pea_src from pea_p;
begin;
update pea_src set id = 3 where id = 2;
delete from pea_src where id = 3;
insert into pea_src values (3, 'impostor');
commit;
set @peal = (select rel_id from mo_catalog.mo_tables where relname = 'pea_src' and reldatabase = 'branch_pick_fk_pk_move');
select count(*) from mo_catalog.mo_data_branch_update_link where table_id = @peal;
-- @regex("Cannot delete or update a parent row: a foreign key constraint fails", true)
data branch pick pea_src into pea_p keys(2) when conflict accept;
select id, v from pea_p order by id;
select id, pid from pea_c order by id;

-- Era-4: the move link is staged before the transaction crosses the 30-entry
-- compaction threshold; the re-key remap and spill ledger keep the pick
-- provable.
create table pec_p (id int primary key, v varchar(32));
create table pec_c (
    id int primary key,
    pid int,
    constraint fk_pec foreign key (pid) references pec_p(id) on update cascade
);
create table pec_fill (id int primary key);
insert into pec_p values (2, 'two');
insert into pec_c values (1, 2);
data branch create table pec_src from pec_p;
begin;
update pec_src set id = 3 where id = 2;
insert into pec_fill values (1);
insert into pec_fill values (2);
insert into pec_fill values (3);
insert into pec_fill values (4);
insert into pec_fill values (5);
insert into pec_fill values (6);
insert into pec_fill values (7);
insert into pec_fill values (8);
insert into pec_fill values (9);
insert into pec_fill values (10);
insert into pec_fill values (11);
insert into pec_fill values (12);
insert into pec_fill values (13);
insert into pec_fill values (14);
insert into pec_fill values (15);
insert into pec_fill values (16);
insert into pec_fill values (17);
insert into pec_fill values (18);
insert into pec_fill values (19);
insert into pec_fill values (20);
insert into pec_fill values (21);
insert into pec_fill values (22);
insert into pec_fill values (23);
insert into pec_fill values (24);
insert into pec_fill values (25);
insert into pec_fill values (26);
insert into pec_fill values (27);
insert into pec_fill values (28);
insert into pec_src values (99, 'tail');
commit;
set @pecl = (select rel_id from mo_catalog.mo_tables where relname = 'pec_src' and reldatabase = 'branch_pick_fk_pk_move');
select count(*) from mo_catalog.mo_data_branch_update_link where table_id = @pecl;
data branch pick pec_src into pec_p keys(2) when conflict accept;
select id, v from pec_p order by id;
select id, pid from pec_c order by id;

-- Era-4: both sides moved the same rows; the picked retargeted moves apply
-- in vacate order from their effective destination predicates, preserving
-- both parent rows and every child's lineage.
create table pr_p (id int primary key, v varchar(16));
create table pr_c (
    id int primary key,
    pid int,
    constraint fk_pr foreign key (pid) references pr_p(id) on update cascade
);
insert into pr_p values (1, 'A'), (2, 'B'), (3, 'C');
insert into pr_c values (11, 1), (12, 2);
data branch create table pr_src from pr_p;
delete from pr_p where id = 3;
update pr_p set id = 5 where id = 1;
update pr_p set id = 3 where id = 2;
delete from pr_src where id = 3;
update pr_src set id = 3, v = 'A2' where id = 1;
update pr_src set id = 4, v = 'B2' where id = 2;
data branch pick pr_src into pr_p keys(1, 2) when conflict accept;
select id, v from pr_p order by id;
select id, pid from pr_c order by id;

-- Era-4: the S3 producer route through PICK with bounded data. Wide payloads
-- cross the injected 512KB flush threshold so the producer flushes
-- mid-statement and at commit; picking two old keys applies them natively
-- and cascades, leaving the rest of the branch behind.
create table ps_p (id int primary key, v varchar(9024));
create table ps_c (
    id int primary key,
    pid int,
    constraint fk_ps foreign key (pid) references ps_p(id) on update cascade
);
insert into ps_p select result, repeat('d', 9000) from generate_series(1, 64) g;
insert into ps_c values (1, 1), (64, 64);
data branch create table ps_src from ps_p;
select enable_fault_injection();
select add_fault_point('fj/cn/flush_small_objs',':::','echo',40,'branch_pick_fk_pk_move.ps_src');
update ps_src set id = id + 1000;
set @psl = (select rel_id from mo_catalog.mo_tables where relname = 'ps_src' and reldatabase = 'branch_pick_fk_pk_move');
select count(*) from mo_catalog.mo_data_branch_update_link where table_id = @psl;
select remove_fault_point('fj/cn/flush_small_objs');
select disable_fault_injection();
data branch pick ps_src into ps_p keys(1, 64) when conflict accept;
select min(id), max(id), count(*) from ps_p;
select id, pid from ps_c order by id;
select count(*) from ps_p where id between 2 and 63;

-- Era-4: the same S3 route under a restrictive foreign key rejects the pick
-- atomically.
create table psr_p (id int primary key, v varchar(9024));
create table psr_c (
    id int primary key,
    pid int,
    constraint fk_psr foreign key (pid) references psr_p(id) on update restrict
);
insert into psr_p select result, repeat('e', 9000) from generate_series(1, 8) g;
insert into psr_c values (1, 1), (8, 8);
data branch create table psr_src from psr_p;
select enable_fault_injection();
select add_fault_point('fj/cn/flush_small_objs',':::','echo',40,'branch_pick_fk_pk_move.psr_src');
update psr_src set id = id + 100;
set @psrl = (select rel_id from mo_catalog.mo_tables where relname = 'psr_src' and reldatabase = 'branch_pick_fk_pk_move');
select count(*) from mo_catalog.mo_data_branch_update_link where table_id = @psrl;
select remove_fault_point('fj/cn/flush_small_objs');
select disable_fault_injection();
-- @regex("Cannot delete or update a parent row: a foreign key constraint fails", true)
data branch pick psr_src into psr_p keys(1) when conflict accept;
select id, length(v) from psr_p order by id;
select id, pid from psr_c order by id;

-- Era-5 (review F1): pick flavor of the S3-route move + delete +
-- impostor counterexample. The diverted death retires the link; the
-- pick falls back to legacy replay and the FK rejects it atomically.
create table psd_p (id int primary key, v varchar(9024));
create table psd_c (
    id int primary key,
    pid int,
    constraint fk_psd foreign key (pid) references psd_p(id) on update cascade
);
insert into psd_p select result, repeat('d', 9000) from generate_series(1, 8) g;
insert into psd_c values (77, 2);
data branch create table psd_src from psd_p;
select enable_fault_injection();
select add_fault_point('fj/cn/flush_small_objs',':::','echo',40,'branch_pick_fk_pk_move.psd_src');
begin;
update psd_src set id = 1000, v = repeat('m', 9000) where id = 2;
delete from psd_src where id = 1000;
insert into psd_src values (1000, repeat('z', 9000));
commit;
set @psdl = (select rel_id from mo_catalog.mo_tables where relname = 'psd_src' and reldatabase = 'branch_pick_fk_pk_move');
select count(*) from mo_catalog.mo_data_branch_update_link where table_id = @psdl;
select remove_fault_point('fj/cn/flush_small_objs');
select disable_fault_injection();
-- @regex("Cannot delete or update a parent row: a foreign key constraint fails", true)
data branch pick psd_src into psd_p keys(2) when conflict accept;
select count(*) from psd_p;
select id, length(v) from psd_p where id = 2;
select id, pid from psd_c order by id;

-- Era-5 (review F5): pick flavor of the same-final-key fold. The picked
-- lineages apply in place; no false cyclic plan.
create table psk_p (id int primary key, v varchar(32));
insert into psk_p values (1, 'a'), (2, 'b');
data branch create table psk_src from psk_p;
update psk_p set id = 3, v = 'A-dst' where id = 1;
update psk_p set id = 4, v = 'B-dst' where id = 2;
update psk_src set id = 3, v = 'A-src' where id = 1;
update psk_src set id = 4, v = 'B-src' where id = 2;
data branch pick psk_src into psk_p keys(1, 2) when conflict accept;
select id, v from psk_p order by id;

-- Era-5 (review F5): a same-final-key row picked together with real
-- cross-row moves applies with the rest of the plan.
create table psx_p (id int primary key, v varchar(32));
insert into psx_p values (1, 'a'), (2, 'b'), (6, 'six');
data branch create table psx_src from psx_p;
update psx_p set id = 3, v = 'A-dst' where id = 1;
update psx_src set id = 3, v = 'A-src' where id = 1;
update psx_src set id = 4, v = 'B-src' where id = 2;
update psx_src set id = 7, v = 'X-src' where id = 6;
data branch pick psx_src into psx_p keys(1, 2, 6) when conflict accept;
select id, v from psx_p order by id;

-- Era-5 (review F4): pick flavor of the edit-first, compaction-crossing
-- move. The old anchor rides the re-key back to the durable departing
-- identity and the pick cascades.
create table pdm_p (id int primary key, v varchar(32));
create table pdm_c (
    id int primary key,
    pid int,
    constraint fk_pdm foreign key (pid) references pdm_p(id) on update cascade
);
insert into pdm_p values (2, 'two');
insert into pdm_c values (1, 2);
data branch create table pdm_src from pdm_p;
begin;
update pdm_src set v = 'edited' where id = 2;
insert into pdm_src values (101, 'pad');
insert into pdm_src values (102, 'pad');
insert into pdm_src values (103, 'pad');
insert into pdm_src values (104, 'pad');
insert into pdm_src values (105, 'pad');
insert into pdm_src values (106, 'pad');
insert into pdm_src values (107, 'pad');
insert into pdm_src values (108, 'pad');
insert into pdm_src values (109, 'pad');
insert into pdm_src values (110, 'pad');
insert into pdm_src values (111, 'pad');
insert into pdm_src values (112, 'pad');
insert into pdm_src values (113, 'pad');
insert into pdm_src values (114, 'pad');
insert into pdm_src values (115, 'pad');
insert into pdm_src values (116, 'pad');
insert into pdm_src values (117, 'pad');
insert into pdm_src values (118, 'pad');
insert into pdm_src values (119, 'pad');
insert into pdm_src values (120, 'pad');
insert into pdm_src values (121, 'pad');
insert into pdm_src values (122, 'pad');
insert into pdm_src values (123, 'pad');
insert into pdm_src values (124, 'pad');
insert into pdm_src values (125, 'pad');
insert into pdm_src values (126, 'pad');
insert into pdm_src values (127, 'pad');
insert into pdm_src values (128, 'pad');
insert into pdm_src values (129, 'pad');
insert into pdm_src values (130, 'pad');
insert into pdm_src values (131, 'pad');
insert into pdm_src values (132, 'pad');
insert into pdm_src values (133, 'pad');
update pdm_src set id = 3 where id = 2;
commit;
set @pdml = (select rel_id from mo_catalog.mo_tables where relname = 'pdm_src' and reldatabase = 'branch_pick_fk_pk_move');
select count(*) from mo_catalog.mo_data_branch_update_link where table_id = @pdml;
select count(*) from mo_catalog.mo_data_branch_update_link where table_id = @pdml and hex(old_row_id) like '000000000000000000000000000000FF%';
data branch pick pdm_src into pdm_p keys(2) when conflict accept;
select id, v from pdm_p where id in (2, 3);
select count(*) from pdm_p;
select id, pid from pdm_c order by id;

-- Era-5 (review F2/F3): pick flavor of the failure-plus-retry rollback
-- shape; the pick cascades the RETRY's payload.
create table prb_p (id int primary key, v varchar(32));
create table prb_c (
    id int primary key,
    pid int,
    constraint fk_prb foreign key (pid) references prb_p(id) on update cascade
);
insert into prb_p values (2, 'two'), (8, 'eight'), (9, 'nine');
insert into prb_c values (1, 2);
data branch create table prb_src from prb_p;
alter table prb_src add unique key uq_prb(v);
begin;
update prb_src set id = 3 where id = 2;
update prb_src set v = 'edited' where id = 3;
-- @regex("Duplicate entry", true)
update prb_src set v = 'nine' where id in (3, 8);
update prb_src set v = 'final' where id = 3;
commit;
set @prbl = (select rel_id from mo_catalog.mo_tables where relname = 'prb_src' and reldatabase = 'branch_pick_fk_pk_move');
select count(*) from mo_catalog.mo_data_branch_update_link where table_id = @prbl;
data branch pick prb_src into prb_p keys(2) when conflict accept;
select id, v from prb_p order by id;
select id, pid from prb_c order by id;

-- Era-6 (review F1): pick flavor of the S3-route chain with an
-- unrelated reinsert at the vacated intermediate key. The fold departs
-- the intermediate through the recorded same-transaction departure and
-- lands at 1001; the impostor at 1000 was never picked and must not be
-- claimed as the lineage destination, so it stays behind on the branch.
create table pmc_p (id int primary key, v varchar(9024));
create table pmc_c (
    id int primary key,
    pid int,
    constraint fk_pmc foreign key (pid) references pmc_p(id) on update cascade
);
insert into pmc_p select result, repeat('l', 9000) from generate_series(1, 8) g;
insert into pmc_c values (77, 2);
data branch create table pmc_src from pmc_p;
select enable_fault_injection();
select add_fault_point('fj/cn/flush_small_objs',':::','echo',40,'branch_pick_fk_pk_move.pmc_src');
begin;
update pmc_src set id = 1000, v = repeat('m', 9000) where id = 2;
update pmc_src set id = 1001, v = repeat('n', 9000) where id = 1000;
insert into pmc_src values (1000, repeat('z', 9000));
commit;
select remove_fault_point('fj/cn/flush_small_objs');
select disable_fault_injection();
set @pmcl = (select rel_id from mo_catalog.mo_tables where relname = 'pmc_src' and reldatabase = 'branch_pick_fk_pk_move');
select count(*) from mo_catalog.mo_data_branch_update_link where table_id = @pmcl;
data branch pick pmc_src into pmc_p keys(2) when conflict accept;
select count(*) from pmc_p;
select id, substr(v, 1, 1) from pmc_p where id in (2, 1000, 1001) order by id;
select id, pid from pmc_c order by id;

-- Era-6 (review F2): pick flavor of the spilled-workspace replacement
-- deleted through the diverted path. The workspace identity recorded on
-- the link proves staging, not survival; the pk-level death ledger
-- retires the link at commit, and picking the old key replays the
-- independent delete+insert semantics, which the foreign key rejects
-- atomically.
create table psp_p (id int primary key, v varchar(32));
create table psp_c (
    id int primary key,
    pid int,
    constraint fk_psp foreign key (pid) references psp_p(id) on update cascade
);
insert into psp_p values (2, 'two');
insert into psp_c values (1, 2);
create table psp_filler (id int primary key, b int);
insert into psp_filler values (1, 1);
data branch create table psp_src from psp_p;
begin;
update psp_src set id = 3, v = 'two' where id = 2;
select enable_fault_injection();
select add_fault_point('fj/cn/workspace_force_flush',':::','echo',0,'');
update psp_filler set b = 9 where id = 1;
select remove_fault_point('fj/cn/workspace_force_flush');
select disable_fault_injection();
delete from psp_src where id = 3;
insert into psp_src values (3, 'impostor');
commit;
set @pspl = (select rel_id from mo_catalog.mo_tables where relname = 'psp_src' and reldatabase = 'branch_pick_fk_pk_move');
select count(*) from mo_catalog.mo_data_branch_update_link where table_id = @pspl;
-- @regex("Cannot delete or update a parent row: a foreign key constraint fails", true)
data branch pick psp_src into psp_p keys(2) when conflict accept;
select id, v from psp_p order by id;
select id, pid from psp_c order by id;

-- Era-6 (review F3): a transaction CREATED before the BETWEEN boundary
-- commits its move inside the window. write_ts is the transaction's
-- CREATE timestamp, so a lower-bound filter would hide that link and
-- replay the move as an independent delete+insert; the pick must read
-- the straddler's provenance and cascade. Session 2 holds the straddler
-- transaction open across the boundary snapshot.
drop snapshot if exists pbc_sa;
drop snapshot if exists pbc_sb;
create table pbc_p (id int primary key, v varchar(32));
create table pbc_c (
    id int primary key,
    pid int,
    constraint fk_pbc foreign key (pid) references pbc_p(id) on update cascade
);
insert into pbc_p values (2, 'two');
insert into pbc_c values (1, 2);
data branch create table pbc_src from pbc_p;
-- @session:id=2{
use branch_pick_fk_pk_move;
begin;
update pbc_src set id = 3, v = 'twoA' where id = 2;
-- @session}
create snapshot pbc_sa for account sys;
-- @session:id=2{
commit;
-- @session}
create snapshot pbc_sb for account sys;
set @pbcl = (select rel_id from mo_catalog.mo_tables where relname = 'pbc_src' and reldatabase = 'branch_pick_fk_pk_move');
select count(*) from mo_catalog.mo_data_branch_update_link where table_id = @pbcl;
data branch pick pbc_src into pbc_p between snapshot pbc_sa and pbc_sb keys(2) when conflict accept;
select id, v from pbc_p order by id;
select id, pid from pbc_c order by id;
drop snapshot pbc_sa;
drop snapshot pbc_sb;

-- Era-6 (review F3): straddler hazard shape. Transaction O (session 2,
-- created before the boundary) moves 3 -> 4 and inserts an impostor at
-- the vacated 3, while transaction N (created after the boundary) moved
-- 2 -> 3. Both belong to the window by commit instant, so the fold
-- follows the recorded chain 2 -> 3 -> 4, re-parents the child to 4,
-- and never claims the impostor; an omitted O would instead read as a
-- same-key tail and land the child on 3.
drop snapshot if exists pbs_ha;
drop snapshot if exists pbs_hb;
create table pbs_p (id int primary key, v varchar(32));
create table pbs_c (
    id int primary key,
    pid int,
    constraint fk_pbs foreign key (pid) references pbs_p(id) on update cascade
);
insert into pbs_p values (2, 'two');
insert into pbs_c values (1, 2);
data branch create table pbs_src from pbs_p;
-- @session:id=2{
use branch_pick_fk_pk_move;
begin;
-- @session}
create snapshot pbs_ha for account sys;
begin;
update pbs_src set id = 3, v = 'hopN' where id = 2;
commit;
-- @session:id=2{
update pbs_src set id = 4, v = 'hopO' where id = 3;
insert into pbs_src values (3, 'impostor');
commit;
-- @session}
create snapshot pbs_hb for account sys;
set @pbsl = (select rel_id from mo_catalog.mo_tables where relname = 'pbs_src' and reldatabase = 'branch_pick_fk_pk_move');
select count(*) from mo_catalog.mo_data_branch_update_link where table_id = @pbsl;
data branch pick pbs_src into pbs_p between snapshot pbs_ha and pbs_hb keys(2) when conflict accept;
select id, v from pbs_p order by id;
select id, pid from pbs_c order by id;
drop snapshot pbs_ha;
drop snapshot pbs_hb;

-- Era-7 (review F4): a straddling SUCCESSOR move is future history. The
-- anchor transaction moves 2 -> 3 inside the window; another session's
-- transaction was created before the window endpoint but commits its
-- 3 -> 4 continuation after it, leaving the successor link visible to the
-- probe while its departure belongs to a later instant. The fold must end
-- the lineage at key 3 instead of failing closed: the picked old key
-- replays as one native update and the child cascades to 3.
drop snapshot if exists efw_a;
drop snapshot if exists efw_b;
create table efw_p (id int primary key, v varchar(32));
create table efw_c (
    id int primary key,
    pid int,
    constraint fk_efw foreign key (pid) references efw_p(id) on update cascade
);
insert into efw_p values (2, 'two');
insert into efw_c values (1, 2);
data branch create table efw_src from efw_p;
create snapshot efw_a for account sys;
update efw_src set id = 3, v = 'twoA' where id = 2;
-- @session:id=2{
use branch_pick_fk_pk_move;
begin;
update efw_src set id = 4 where id = 3;
-- @session}
create snapshot efw_b for account sys;
-- @session:id=2{
commit;
-- @session}
set @efwl = (select rel_id from mo_catalog.mo_tables where relname = 'efw_src' and reldatabase = 'branch_pick_fk_pk_move');
select count(*) from mo_catalog.mo_data_branch_update_link where table_id = @efwl;
data branch pick efw_src into efw_p between snapshot efw_a and efw_b keys(2) when conflict accept;
select id, v from efw_p order by id;
select id, pid from efw_c order by id;
drop snapshot efw_a;
drop snapshot efw_b;

-- Era-8 (review F1): the PICK flavor of the S3-route same-key edit
-- before a vacate-before-arrival pair. B is edited at 3 (the
-- S3-buffered replacement never stages), then B moves 3 -> 4 and A
-- moves 2 -> 3 in one transaction. The edit's continuity marker binds
-- B's durable origin and the move's recorded old identity heals back
-- to it, so the picked keys replay as native updates: A's lineage
-- folds to (3, 'A0') and B's to (4, 'B1'), each child following its
-- own parent.
create table ewp_p (id int primary key, v varchar(32));
create table ewp_c (
    id int primary key,
    pid int,
    constraint fk_ewp foreign key (pid) references ewp_p(id) on update cascade
);
insert into ewp_p values (2, 'A0'), (3, 'B0');
insert into ewp_c values (1, 2), (2, 3);
data branch create table ewp_src from ewp_p;
select enable_fault_injection();
select add_fault_point('fj/cn/flush_small_objs',':::','echo',40,'branch_pick_fk_pk_move.ewp_src');
begin;
update ewp_src set v = 'B1' where id = 3;
update ewp_src set id = 4 where id = 3;
update ewp_src set id = 3 where id = 2;
commit;
select remove_fault_point('fj/cn/flush_small_objs');
select disable_fault_injection();
set @ewpl = (select rel_id from mo_catalog.mo_tables where relname = 'ewp_src' and reldatabase = 'branch_pick_fk_pk_move');
select count(*) from mo_catalog.mo_data_branch_update_link where table_id = @ewpl;
data branch pick ewp_src into ewp_p keys(2, 3) when conflict accept;
select id, v from ewp_p order by id;
select id, pid from ewp_c order by id;

-- Era-9 (review F2): the PICK flavor of the recorded key-rotation
-- cycle. No apply order replays a key rotation as in-place native
-- UPDATEs, and legacy DELETE+INSERT replay would swap payloads while
-- keeping the destination keys so each child follows the WRONG parent
-- with no FK violation to stop it. The pick must ABORT
-- statement-atomically, leaving the destination tables byte-identical.
create table ecp_p (id int primary key, v varchar(32));
create table ecp_c (
    id int primary key,
    pid int,
    constraint fk_ecp foreign key (pid) references ecp_p(id) on update cascade on delete cascade
);
insert into ecp_p values (2, 'two'), (3, 'three');
insert into ecp_c values (1, 2), (2, 3);
data branch create table ecp_src from ecp_p;
update ecp_src set id = 4 where id = 2;
update ecp_src set id = 2 where id = 3;
update ecp_src set id = 3 where id = 4;
set @ecpl = (select rel_id from mo_catalog.mo_tables where relname = 'ecp_src' and reldatabase = 'branch_pick_fk_pk_move');
select count(*) from mo_catalog.mo_data_branch_update_link where table_id = @ecpl;
-- @regex("key rotation cycle", true)
data branch pick ecp_src into ecp_p keys(2, 3) when conflict accept;
select id, v from ecp_p order by id;
select id, pid from ecp_c order by id;

-- Era-9 (review F4): a source-only lineage that inserts, moves, and
-- deletes one row in three statements nets to an EMPTY diff. Its
-- retained provenance walk cannot resolve, yet none of its keys is in
-- the pick selection, so the pick must succeed as a no-op instead of
-- aborting on unresolved provenance.
create table esp_p (id int primary key, v varchar(32));
create table esp_c (
    id int primary key,
    pid int,
    constraint fk_esp foreign key (pid) references esp_p(id) on update cascade
);
insert into esp_p values (99, 'stay');
insert into esp_c values (1, 99);
data branch create table esp_src from esp_p;
insert into esp_src values (2, 'gone');
update esp_src set id = 3 where id = 2;
delete from esp_src where id = 3;
set @espl = (select rel_id from mo_catalog.mo_tables where relname = 'esp_src' and reldatabase = 'branch_pick_fk_pk_move');
select count(*) from mo_catalog.mo_data_branch_update_link where table_id = @espl;
data branch pick esp_src into esp_p keys(99) when conflict accept;
select id, v from esp_p order by id;
select id, pid from esp_c order by id;

-- Era-10 (review F1): the PICK flavor of the S3-route move followed by
-- TWO unarmed same-key payload edits in one transaction. The first
-- edit diverts the death of the S3-staged replacement and heals the
-- link's continuity; the second edit must preserve that evidence
-- instead of overwriting the marker statement. Pre-fix the commit
-- prune dropped the live link and the pick fell back to legacy
-- DELETE+INSERT replay, deleting the child through ON DELETE CASCADE.
-- Post-fix the single recorded move survives: the pick replays one
-- native UPDATE carrying the final payload 'e2' and the child follows
-- 2 -> 3.
create table erep_p (id int primary key, v varchar(32));
create table erep_c (
    id int primary key,
    pid int,
    constraint fk_erep foreign key (pid) references erep_p(id) on update cascade on delete cascade
);
insert into erep_p values (2, 'two');
insert into erep_c values (1, 2);
data branch create table erep_src from erep_p;
select enable_fault_injection();
select add_fault_point('fj/cn/flush_small_objs',':::','echo',40,'branch_pick_fk_pk_move.erep_src');
begin;
update erep_src set id = 3 where id = 2;
select remove_fault_point('fj/cn/flush_small_objs');
select disable_fault_injection();
update erep_src set v = 'e1' where id = 3;
update erep_src set v = 'e2' where id = 3;
commit;
set @erepl = (select rel_id from mo_catalog.mo_tables where relname = 'erep_src' and reldatabase = 'branch_pick_fk_pk_move');
select count(*) from mo_catalog.mo_data_branch_update_link where table_id = @erepl;
data branch pick erep_src into erep_p keys(2) when conflict accept;
select id, v from erep_p order by id;
select id, pid from erep_c order by id;

-- Era-10 (review F5): the PICK flavor of the forced workspace flush
-- landing BETWEEN two same-transaction moves. Statement-boundary
-- dumps keep every link owner table's staged inserts in memory until
-- the commit dump, so the first link's workspace identities stay
-- joinable with the second link. Pre-fix the chain severed and the
-- pick aborted on unresolved provenance; post-fix both links fold
-- into one native UPDATE ending at key 4 and the child follows.
create table ewsp_p (id int primary key, v varchar(32));
create table ewsp_c (
    id int primary key,
    pid int,
    constraint fk_ewsp foreign key (pid) references ewsp_p(id) on update cascade
);
insert into ewsp_p values (2, 'two');
insert into ewsp_c values (1, 2);
create table ewsp_filler (id int primary key, b int);
insert into ewsp_filler values (1, 1);
data branch create table ewsp_src from ewsp_p;
begin;
update ewsp_src set id = 3 where id = 2;
select enable_fault_injection();
select add_fault_point('fj/cn/workspace_force_flush',':::','echo',0,'');
update ewsp_filler set b = 9 where id = 1;
select remove_fault_point('fj/cn/workspace_force_flush');
select disable_fault_injection();
update ewsp_src set id = 4 where id = 3;
commit;
set @ewspl = (select rel_id from mo_catalog.mo_tables where relname = 'ewsp_src' and reldatabase = 'branch_pick_fk_pk_move');
select count(*) from mo_catalog.mo_data_branch_update_link where table_id = @ewspl;
data branch pick ewsp_src into ewsp_p keys(2) when conflict accept;
select id, v from ewsp_p order by id;
select id, pid from ewsp_c order by id;

-- Era-10 (review F6): the PICK flavor of the return to the original
-- key with a CHANGED payload. Two separately committed moves 2 -> 3
-- -> 2 share one key between departure and arrival; the planner must
-- fold them as an in-place native UPDATE at key 2 instead of
-- rejecting the plan as a key-rotation cycle. Pre-fix the pick
-- aborted; post-fix the destination payload becomes 'two-b' and the
-- child never moves.
create table erop_p (id int primary key, v varchar(32));
create table erop_c (
    id int primary key,
    pid int,
    constraint fk_erop foreign key (pid) references erop_p(id) on update cascade
);
insert into erop_p values (2, 'two');
insert into erop_c values (1, 2);
data branch create table erop_src from erop_p;
update erop_src set id = 3 where id = 2;
update erop_src set id = 2, v = 'two-b' where id = 3;
set @eropl = (select rel_id from mo_catalog.mo_tables where relname = 'erop_src' and reldatabase = 'branch_pick_fk_pk_move');
select count(*) from mo_catalog.mo_data_branch_update_link where table_id = @eropl;
data branch pick erop_src into erop_p keys(2) when conflict accept;
select id, v from erop_p order by id;
select id, pid from erop_c order by id;

-- Era-10 (review F6): the PICK flavor of the unchanged-payload return
-- home. The final image equals the original image, yet the recorded
-- chain 2 -> 3 -> 2 still folds through one self-key lineage; pre-fix
-- the planner rejected it as a key-rotation cycle. Post-fix the pick
-- applies as an in-place no-op and both tables keep their exact
-- original rows.
create table erup_p (id int primary key, v varchar(32));
create table erup_c (
    id int primary key,
    pid int,
    constraint fk_erup foreign key (pid) references erup_p(id) on update cascade
);
insert into erup_p values (2, 'two');
insert into erup_c values (1, 2);
data branch create table erup_src from erup_p;
update erup_src set id = 3 where id = 2;
update erup_src set id = 2 where id = 3;
set @erupl = (select rel_id from mo_catalog.mo_tables where relname = 'erup_src' and reldatabase = 'branch_pick_fk_pk_move');
select count(*) from mo_catalog.mo_data_branch_update_link where table_id = @erupl;
data branch pick erup_src into erup_p keys(2) when conflict accept;
select id, v from erup_p order by id;
select id, pid from erup_c order by id;

-- Era-11 (review F1): PICK mirror of the merge eef_ section — a payload
-- edit first, a forced workspace spill, then the primary-key move, all in
-- one transaction. The flush must keep the provenance-pinned edited row
-- in memory so the move's link names the durable tombstone; pre-fix the
-- link named the flushed uncommitted-object rowid and the pick replayed
-- DELETE+INSERT, destroying the child through declared ON DELETE CASCADE.
-- Post-fix keys(2) folds the recorded pair into one native UPDATE and the
-- child follows.
create table eefp_p (id int primary key, v varchar(32));
create table eefp_c (
    id int primary key,
    pid int,
    constraint fk_eefp foreign key (pid) references eefp_p(id)
    on update cascade on delete cascade
);
insert into eefp_p values (2, 'two');
insert into eefp_c values (1, 2);
create table eefp_f (id int primary key, b int);
insert into eefp_f values (1, 1);
data branch create table eefp_src from eefp_p;
begin;
update eefp_src set v = 'edited' where id = 2;
select enable_fault_injection();
select add_fault_point('fj/cn/workspace_force_flush',':::','echo',0,'');
update eefp_f set b = 9 where id = 1;
select remove_fault_point('fj/cn/workspace_force_flush');
select disable_fault_injection();
update eefp_src set id = 3 where id = 2;
commit;
set @eefpl = (select rel_id from mo_catalog.mo_tables where relname = 'eefp_src' and reldatabase = 'branch_pick_fk_pk_move');
select count(*) from mo_catalog.mo_data_branch_update_link where table_id = @eefpl;
data branch pick eefp_src into eefp_p keys(2) when conflict accept;
select id, v from eefp_p order by id;
select id, pid from eefp_c order by id;

-- Era-11 (review F4): PICK mirror of ehc_ — a PROVEN same-transaction
-- return to the original key with a CHANGED payload. Recorded workspace
-- identities join both moves of 2 -> 3 -> 2 in one commit; pre-fix the
-- revisit guard rejected the equal-instant revisit and the pick aborted
-- on unresolved provenance, post-fix keys(2) applies the chain as one
-- in-place native UPDATE at the original key.
create table ehcp_p (id int primary key, v varchar(32));
create table ehcp_c (
    id int primary key,
    pid int,
    constraint fk_ehcp foreign key (pid) references ehcp_p(id) on update cascade
);
insert into ehcp_p values (2, 'two');
insert into ehcp_c values (1, 2);
data branch create table ehcp_src from ehcp_p;
begin;
update ehcp_src set id = 3 where id = 2;
update ehcp_src set id = 2, v = 'two-b' where id = 3;
commit;
set @ehcpl = (select rel_id from mo_catalog.mo_tables where relname = 'ehcp_src' and reldatabase = 'branch_pick_fk_pk_move');
select count(*) from mo_catalog.mo_data_branch_update_link where table_id = @ehcpl;
data branch pick ehcp_src into ehcp_p keys(2) when conflict accept;
select id, v from ehcp_p order by id;
select id, pid from ehcp_c order by id;

-- Era-11 (review F4): PICK mirror of ehu_ — the same single-transaction
-- homecoming with an UNCHANGED payload applies as an in-place no-op and
-- both tables keep their exact original rows.
create table ehup_p (id int primary key, v varchar(32));
create table ehup_c (
    id int primary key,
    pid int,
    constraint fk_ehup foreign key (pid) references ehup_p(id) on update cascade
);
insert into ehup_p values (2, 'two');
insert into ehup_c values (1, 2);
data branch create table ehup_src from ehup_p;
begin;
update ehup_src set id = 3 where id = 2;
update ehup_src set id = 2 where id = 3;
commit;
set @ehupl = (select rel_id from mo_catalog.mo_tables where relname = 'ehup_src' and reldatabase = 'branch_pick_fk_pk_move');
select count(*) from mo_catalog.mo_data_branch_update_link where table_id = @ehupl;
data branch pick ehup_src into ehup_p keys(2) when conflict accept;
select id, v from ehup_p order by id;
select id, pid from ehup_c order by id;

-- Era-11 (review F5): PICK mirror of etn_ — move 2 -> 3 in one commit,
-- delete at 3 in a later commit, on a table WITHOUT foreign keys. The
-- evidence proves a TERMINAL deletion; keys(2) picks the anchor key
-- literally, and the destination row at key 2 is deleted. Pre-fix the
-- pick aborted on unresolved provenance.
create table etnp_p (id int primary key, v varchar(32));
insert into etnp_p values (2, 'two');
data branch create table etnp_src from etnp_p;
update etnp_src set id = 3 where id = 2;
delete from etnp_src where id = 3;
set @etnpl = (select rel_id from mo_catalog.mo_tables where relname = 'etnp_src' and reldatabase = 'branch_pick_fk_pk_move');
select count(*) from mo_catalog.mo_data_branch_update_link where table_id = @etnpl;
data branch pick etnp_src into etnp_p keys(2) when conflict accept;
select id, v from etnp_p order by id;

-- Era-11 (review F5): PICK mirror of etc_ — the proven terminal deletion
-- deletes the destination parent natively, so a declared ON DELETE
-- CASCADE child is cleaned up: both tables end EMPTY.
create table etcp_p (id int primary key, v varchar(32));
create table etcp_c (
    id int primary key,
    pid int,
    constraint fk_etcp foreign key (pid) references etcp_p(id)
    on update cascade on delete cascade
);
insert into etcp_p values (2, 'two');
insert into etcp_c values (1, 2);
data branch create table etcp_src from etcp_p;
update etcp_src set id = 3 where id = 2;
delete from etcp_src where id = 3;
set @etcpl = (select rel_id from mo_catalog.mo_tables where relname = 'etcp_src' and reldatabase = 'branch_pick_fk_pk_move');
select count(*) from mo_catalog.mo_data_branch_update_link where table_id = @etcpl;
data branch pick etcp_src into etcp_p keys(2) when conflict accept;
select id, v from etcp_p order by id;
select id, pid from etcp_c order by id;

-- Era-11 (review F5): PICK mirror of etr_ — the proven terminal deletion
-- applies as a native DELETE of the destination parent, so the RESTRICT
-- child makes the statement fail loudly and atomically: both tables keep
-- their exact original rows.
create table etrp_p (id int primary key, v varchar(32));
create table etrp_c (
    id int primary key,
    pid int,
    constraint fk_etrp foreign key (pid) references etrp_p(id) on update cascade
);
insert into etrp_p values (2, 'two');
insert into etrp_c values (1, 2);
data branch create table etrp_src from etrp_p;
update etrp_src set id = 3 where id = 2;
delete from etrp_src where id = 3;
set @etrpl = (select rel_id from mo_catalog.mo_tables where relname = 'etrp_src' and reldatabase = 'branch_pick_fk_pk_move');
select count(*) from mo_catalog.mo_data_branch_update_link where table_id = @etrpl;
-- @regex("a foreign key constraint fails", true)
data branch pick etrp_src into etrp_p keys(2) when conflict accept;
select id, v from etrp_p order by id;
select id, pid from etrp_c order by id;

-- Era-12 (review F1): PICK mirror of the merge edv_ section — an S3-route
-- same-key edit, a diverted DELETE of the edited row, a spilled unrelated
-- reinsert at the same key, and a primary-key move, all in one
-- transaction. The delete retires the by-key lineage binding, so the
-- reinsert is a NEW row: pre-fix the stale binding healed it back to the
-- durable origin and the pick replayed one native UPDATE that resurrected
-- the cascade-deleted child onto the intruder. Post-fix no link is
-- recorded; picking both involved keys replays plain DELETE+INSERT: the
-- declared ON DELETE CASCADE cleans the child and the parent ends holding
-- only the new row, exactly like the same statements on plain tables.
create table edvp_p (id int primary key, v varchar(32));
create table edvp_c (
    id int primary key,
    pid int,
    constraint fk_edvp foreign key (pid) references edvp_p(id)
    on update cascade on delete cascade
);
insert into edvp_p values (2, 'two');
insert into edvp_c values (1, 2);
create table edvp_f (id int primary key, b int);
insert into edvp_f values (1, 1);
data branch create table edvp_src from edvp_p;
begin;
select enable_fault_injection();
select add_fault_point('fj/cn/flush_small_objs',':::','echo',40,'branch_pick_fk_pk_move.edvp_src');
update edvp_src set v = 'edited' where id = 2;
select remove_fault_point('fj/cn/flush_small_objs');
select disable_fault_injection();
delete from edvp_src where id = 2;
insert into edvp_src values (2, 'intruder');
select enable_fault_injection();
select add_fault_point('fj/cn/workspace_force_flush',':::','echo',0,'');
update edvp_f set b = 9 where id = 1;
select remove_fault_point('fj/cn/workspace_force_flush');
select disable_fault_injection();
update edvp_src set id = 3 where id = 2;
commit;
set @edvpl = (select rel_id from mo_catalog.mo_tables where relname = 'edvp_src' and reldatabase = 'branch_pick_fk_pk_move');
select count(*) from mo_catalog.mo_data_branch_update_link where table_id = @edvpl;
data branch pick edvp_src into edvp_p keys(2, 3) when conflict accept;
select id, v from edvp_p order by id;
select id, pid from edvp_c order by id;

-- Era-12 (review F1): PICK mirror of edr_ — the same history against a
-- child whose delete action is RESTRICT. Post-fix the picked replay is
-- plain DELETE+INSERT, so the destination parent delete fires the declared
-- reject loudly and the pick statement is atomic: both tables keep their
-- exact original rows.
create table edrp_p (id int primary key, v varchar(32));
create table edrp_c (
    id int primary key,
    pid int,
    constraint fk_edrp foreign key (pid) references edrp_p(id) on update cascade
);
insert into edrp_p values (2, 'two');
insert into edrp_c values (1, 2);
create table edrp_f (id int primary key, b int);
insert into edrp_f values (1, 1);
data branch create table edrp_src from edrp_p;
begin;
select enable_fault_injection();
select add_fault_point('fj/cn/flush_small_objs',':::','echo',40,'branch_pick_fk_pk_move.edrp_src');
update edrp_src set v = 'edited' where id = 2;
select remove_fault_point('fj/cn/flush_small_objs');
select disable_fault_injection();
delete from edrp_src where id = 2;
insert into edrp_src values (2, 'intruder');
select enable_fault_injection();
select add_fault_point('fj/cn/workspace_force_flush',':::','echo',0,'');
update edrp_f set b = 9 where id = 1;
select remove_fault_point('fj/cn/workspace_force_flush');
select disable_fault_injection();
update edrp_src set id = 3 where id = 2;
commit;
set @edrpl = (select rel_id from mo_catalog.mo_tables where relname = 'edrp_src' and reldatabase = 'branch_pick_fk_pk_move');
select count(*) from mo_catalog.mo_data_branch_update_link where table_id = @edrpl;
-- @regex("a foreign key constraint fails", true)
data branch pick edrp_src into edrp_p keys(2, 3) when conflict accept;
select id, v from edrp_p order by id;
select id, pid from edrp_c order by id;

-- Era-12 (review F2): PICK mirror of ewt_ — a workspace primary-key move
-- 2 -> 3, then (same transaction) an S3-route same-key payload edit of 3.
-- Pre-fix the edit's replacement harvest could return the logically
-- deleted workspace row and the genuine link was pruned, so the pick
-- replayed DELETE+INSERT and declared ON DELETE CASCADE destroyed the
-- child. Post-fix one link survives onto the final image; keys(2) folds it
-- into one native UPDATE and the child follows to pid 3.
create table ewtp_p (id int primary key, v varchar(32));
create table ewtp_c (
    id int primary key,
    pid int,
    constraint fk_ewtp foreign key (pid) references ewtp_p(id)
    on update cascade on delete cascade
);
insert into ewtp_p values (2, 'two');
insert into ewtp_c values (1, 2);
data branch create table ewtp_src from ewtp_p;
begin;
update ewtp_src set id = 3 where id = 2;
select enable_fault_injection();
select add_fault_point('fj/cn/flush_small_objs',':::','echo',40,'branch_pick_fk_pk_move.ewtp_src');
update ewtp_src set v = 'edited' where id = 3;
select remove_fault_point('fj/cn/flush_small_objs');
select disable_fault_injection();
commit;
set @ewtpl = (select rel_id from mo_catalog.mo_tables where relname = 'ewtp_src' and reldatabase = 'branch_pick_fk_pk_move');
select count(*) from mo_catalog.mo_data_branch_update_link where table_id = @ewtpl;
data branch pick ewtp_src into ewtp_p keys(2) when conflict accept;
select id, v from ewtp_p order by id;
select id, pid from ewtp_c order by id;

-- Era-13 (review F1): PICK mirror of esw_ — one transaction stages a
-- workspace payload edit of key 2, switches the lineage to the S3 route
-- with a second payload edit whose pre-image is the workspace identity,
-- then moves the primary key 2 -> 3. Pre-fix the route switch lost the
-- durable origin, the pick replayed DELETE+INSERT, and declared
-- ON DELETE CASCADE destroyed the child. Post-fix one link carries the
-- durable origin and keys(2) folds it into one native UPDATE: parent
-- (3,'edited2') with the child following to pid 3.
create table eswp_p (id int primary key, v varchar(32));
create table eswp_c (
    id int primary key,
    pid int,
    constraint fk_eswp foreign key (pid) references eswp_p(id)
    on update cascade on delete cascade
);
insert into eswp_p values (2, 'two');
insert into eswp_c values (1, 2);
data branch create table eswp_src from eswp_p;
begin;
update eswp_src set v = 'edited1' where id = 2;
select enable_fault_injection();
select add_fault_point('fj/cn/flush_small_objs',':::','echo',40,'branch_pick_fk_pk_move.eswp_src');
update eswp_src set v = 'edited2' where id = 2;
select remove_fault_point('fj/cn/flush_small_objs');
select disable_fault_injection();
update eswp_src set id = 3 where id = 2;
commit;
set @eswpl = (select rel_id from mo_catalog.mo_tables where relname = 'eswp_src' and reldatabase = 'branch_pick_fk_pk_move');
select count(*) from mo_catalog.mo_data_branch_update_link where table_id = @eswpl;
data branch pick eswp_src into eswp_p keys(2) when conflict accept;
select id, v from eswp_p order by id;
select id, pid from eswp_c order by id;

-- Era-14 (review F1): same murdered-lineage history as the merge esx_
-- section — two workspace edits, a plain DELETE that swallows the staged
-- replacement without tombstone or staged DELETE, an independent
-- re-INSERT forced down the flush route, and the 2 -> 3 move. The
-- swallowed death retires the binding it murdered, so the reincarnation
-- never inherits the dead lineage and the pick replays its own
-- DELETE+INSERT: ON DELETE CASCADE empties the child, and the
-- never-committed reincarnation anchors nowhere.
create table eszp_p (id int primary key, v varchar(32));
create table eszp_c (
    id int primary key,
    pid int,
    constraint fk_eszp foreign key (pid) references eszp_p(id)
    on update cascade on delete cascade
);
insert into eszp_p values (2, 'two');
insert into eszp_c values (1, 2);
data branch create table eszp_src from eszp_p;
begin;
update eszp_src set v = 'edited1' where id = 2;
update eszp_src set v = 'edited2' where id = 2;
delete from eszp_src where id = 2;
select enable_fault_injection();
select add_fault_point('fj/cn/workspace_force_flush',':::','echo',0,'');
insert into eszp_src values (2, 'intruder');
select remove_fault_point('fj/cn/workspace_force_flush');
select disable_fault_injection();
update eszp_src set id = 3 where id = 2;
commit;
set @eszpl = (select rel_id from mo_catalog.mo_tables where relname = 'eszp_src' and reldatabase = 'branch_pick_fk_pk_move');
select count(*) from mo_catalog.mo_data_branch_update_link where table_id = @eszpl;
data branch pick eszp_src into eszp_p keys(2) when conflict accept;
select id, v from eszp_p order by id;
select id, pid from eszp_c order by id;

-- RESTRICT flavor: the replayed parent delete violates the constraint and
-- the pick aborts statement-atomically with both tables untouched.
create table eszpr_p (id int primary key, v varchar(32));
create table eszpr_c (
    id int primary key,
    pid int,
    constraint fk_eszpr foreign key (pid) references eszpr_p(id)
    on update cascade on delete restrict
);
insert into eszpr_p values (2, 'two');
insert into eszpr_c values (1, 2);
data branch create table eszpr_src from eszpr_p;
begin;
update eszpr_src set v = 'edited1' where id = 2;
update eszpr_src set v = 'edited2' where id = 2;
delete from eszpr_src where id = 2;
select enable_fault_injection();
select add_fault_point('fj/cn/workspace_force_flush',':::','echo',0,'');
insert into eszpr_src values (2, 'intruder');
select remove_fault_point('fj/cn/workspace_force_flush');
select disable_fault_injection();
update eszpr_src set id = 3 where id = 2;
commit;
-- @regex("Cannot delete or update a parent row: a foreign key constraint fails", true)
data branch pick eszpr_src into eszpr_p keys(2) when conflict accept;
select id, v from eszpr_p order by id;
select id, pid from eszpr_c order by id;

-- Era-14 (review F2): snapshots bracketing a move on an UNBRANCHED table,
-- then the source drops. The retained snapshot keeps the lineage's links
-- grounding historical branches (1 -> 1) — the direct reclamation used to
-- delete them unconditionally. PICK keys(2) across the move window is a
-- no-op exactly like the live-parent control: the picked key's lineage
-- ended before both sibling branches existed, so nothing moves and no
-- destination delete is attempted.
create table esyp_p (id int primary key, v varchar(32));
insert into esyp_p values (2, 'two');
drop snapshot if exists esyp_s1;
create snapshot esyp_s1 for table branch_pick_fk_pk_move esyp_p;
update esyp_p set id = 3 where id = 2;
drop snapshot if exists esyp_s2;
create snapshot esyp_s2 for table branch_pick_fk_pk_move esyp_p;
set @esypl = (select rel_id from mo_catalog.mo_tables where relname = 'esyp_p' and reldatabase = 'branch_pick_fk_pk_move');
select count(*) from mo_catalog.mo_data_branch_update_link where table_id = @esypl;
drop table esyp_p;
select count(*) from mo_catalog.mo_data_branch_update_link where table_id = @esypl;
data branch create table esyp_b1 from esyp_p{snapshot="esyp_s1"};
create table esyp_c (
    id int primary key,
    pid int,
    constraint fk_esyp foreign key (pid) references esyp_b1(id)
    on update cascade on delete cascade
);
insert into esyp_c values (1, 2);
data branch create table esyp_b2 from esyp_p{snapshot="esyp_s2"};
data branch pick esyp_b2 into esyp_b1 keys(2) when conflict accept;
select id, v from esyp_b1 order by id;
select id, pid from esyp_c order by id;

-- Live-parent control for esyp_: identical snapshots and branches without
-- the drop; the outcome rows must match esyp_ exactly (drop transparency).
create table esypx_p (id int primary key, v varchar(32));
insert into esypx_p values (2, 'two');
drop snapshot if exists esypx_s1;
create snapshot esypx_s1 for table branch_pick_fk_pk_move esypx_p;
update esypx_p set id = 3 where id = 2;
drop snapshot if exists esypx_s2;
create snapshot esypx_s2 for table branch_pick_fk_pk_move esypx_p;
set @esypxl = (select rel_id from mo_catalog.mo_tables where relname = 'esypx_p' and reldatabase = 'branch_pick_fk_pk_move');
select count(*) from mo_catalog.mo_data_branch_update_link where table_id = @esypxl;
data branch create table esypx_b1 from esypx_p{snapshot="esypx_s1"};
create table esypx_c (
    id int primary key,
    pid int,
    constraint fk_esypx foreign key (pid) references esypx_b1(id)
    on update cascade on delete cascade
);
insert into esypx_c values (1, 2);
data branch create table esypx_b2 from esypx_p{snapshot="esypx_s2"};
data branch pick esypx_b2 into esypx_b1 keys(2) when conflict accept;
select id, v from esypx_b1 order by id;
select id, pid from esypx_c order by id;

-- Counterexample: with no snapshot the plain unbranched drop still
-- reclaims — the grounding gate never becomes a retention leak. The drop
-- flow probes coverage under the plan's own identity because the relation
-- row is already retired by reclaim time.
create table esyp2_p (id int primary key, v varchar(32));
insert into esyp2_p values (2, 'two');
update esyp2_p set id = 3 where id = 2;
set @esyp2l = (select rel_id from mo_catalog.mo_tables where relname = 'esyp2_p' and reldatabase = 'branch_pick_fk_pk_move');
select count(*) from mo_catalog.mo_data_branch_update_link where table_id = @esyp2l;
drop table esyp2_p;
select count(*) from mo_catalog.mo_data_branch_update_link where table_id = @esyp2l;

drop snapshot if exists esyp_s1;
drop snapshot if exists esyp_s2;
drop snapshot if exists esypx_s1;
drop snapshot if exists esypx_s2;

-- era-15 (r21 F3): the destination-deleted/source-moved restore shape under
-- the two losing conflict policies. SKIP keeps the destination's delete and
-- never installs the lineage; FAIL aborts atomically with the destination
-- untouched. Neither may resurrect ambiguously, unlike ACCEPT which restores
-- exactly the row the source moved (see delpick above).
create table esqs_p (id int primary key, v varchar(32));
insert into esqs_p values (2, 'two'), (9, 'nine');
data branch create table esqs_src from esqs_p;
update esqs_src set id = 3, v = 'three' where id = 2;
delete from esqs_p where id = 2;
data branch pick esqs_src into esqs_p keys(2) when conflict skip;
select id, v from esqs_p order by id;

create table esqf_p (id int primary key, v varchar(32));
insert into esqf_p values (2, 'two'), (9, 'nine');
data branch create table esqf_src from esqf_p;
update esqf_src set id = 3, v = 'three' where id = 2;
delete from esqf_p where id = 2;
-- @regex("with different values", true)
data branch pick esqf_src into esqf_p keys(2) when conflict fail;
select id, v from esqf_p order by id;

-- era-16 (r22 F4): an occupied landed key on the restore shape, mirroring
-- the MERGE etja_/etjs_/etjf_ sections (see that file). ACCEPT routes the
-- restored row through the shared landed-key conflict resolution (and
-- preserves the tracked occupant in the conflict stream); SKIP drops the
-- restore so the destination keeps its occupant; FAIL aborts atomically
-- on the preserved old-key conflict with the destination untouched.
create table etjpa_p (id int primary key, v varchar(32));
insert into etjpa_p values (2, 'two');
data branch create table etjpa_src from etjpa_p;
update etjpa_src set id = 3, v = 'three' where id = 2;
delete from etjpa_p where id = 2;
insert into etjpa_p values (3, 'destination');
data branch pick etjpa_src into etjpa_p keys(2) when conflict accept;
select id, v from etjpa_p order by id;

create table etjps_p (id int primary key, v varchar(32));
insert into etjps_p values (2, 'two');
data branch create table etjps_src from etjps_p;
update etjps_src set id = 3, v = 'three' where id = 2;
delete from etjps_p where id = 2;
insert into etjps_p values (3, 'destination');
data branch pick etjps_src into etjps_p keys(2) when conflict skip;
select id, v from etjps_p order by id;

create table etjpf_p (id int primary key, v varchar(32));
insert into etjpf_p values (2, 'two');
data branch create table etjpf_src from etjpf_p;
update etjpf_src set id = 3, v = 'three' where id = 2;
delete from etjpf_p where id = 2;
insert into etjpf_p values (3, 'destination');
-- @regex("with different values", true)
data branch pick etjpf_src into etjpf_p keys(2) when conflict fail;
select id, v from etjpf_p order by id;

-- era-16 (r22 F1): multi-survivor overlapping workspace shrink, mirroring
-- the MERGE etjms_ section. The picked survivors' lineages ride exactly
-- to their new identities and cascade their own children; the unpicked
-- row (key 12) and its child stay regardless of the source-side murder.
create table etjmsp_p (id int primary key, v varchar(32));
create table etjmsp_c (
    id int primary key,
    pid int,
    constraint fk_etjmsp foreign key (pid) references etjmsp_p(id)
    on update cascade on delete cascade
);
insert into etjmsp_p values (11, 'a'), (12, 'b'), (13, 'c');
insert into etjmsp_c values (1, 11), (2, 12), (3, 13);
data branch create table etjmsp_src from etjmsp_p;
begin;
update etjmsp_src set id = id + 100 where id in (11, 12, 13);
delete from etjmsp_src where id = 112;
commit;
data branch pick etjmsp_src into etjmsp_p keys(11, 13) when conflict accept;
select id, v from etjmsp_p order by id;
select id, pid from etjmsp_c order by id;

-- era-16 (r22 F2 ambiguity contract, pins current behavior): mirrors the
-- MERGE etj2a_/etj2b_/etj2d_ sections — a same-transaction same-key
-- DELETE+INSERT is stream-identical to a stable-key UPDATE and folds
-- alike; see that file for the full rationale and the native controls.
create table etj2pa_p (id int primary key, v varchar(32));
create table etj2pa_c (
    id int primary key,
    pid int,
    constraint fk_etj2pa foreign key (pid) references etj2pa_p(id)
    on update cascade on delete cascade
);
insert into etj2pa_p values (2, 'two');
insert into etj2pa_c values (1, 2);
data branch create table etj2pa_src from etj2pa_p;
update etj2pa_src set v = 'v1' where id = 2;
update etj2pa_src set id = 3, v = 'a-lane' where id = 2;
data branch pick etj2pa_src into etj2pa_p keys(2) when conflict accept;
select id, v from etj2pa_p order by id;
select id, pid from etj2pa_c order by id;

create table etj2pb_p (id int primary key, v varchar(32));
create table etj2pb_c (
    id int primary key,
    pid int,
    constraint fk_etj2pb foreign key (pid) references etj2pb_p(id)
    on update cascade on delete cascade
);
insert into etj2pb_p values (2, 'two');
insert into etj2pb_c values (1, 2);
data branch create table etj2pb_src from etj2pb_p;
begin;
delete from etj2pb_src where id = 2;
insert into etj2pb_src values (2, 'unrelated');
commit;
update etj2pb_src set id = 3, v = 'b-lane' where id = 2;
data branch pick etj2pb_src into etj2pb_p keys(2) when conflict accept;
select id, v from etj2pb_p order by id;
select id, pid from etj2pb_c order by id;

create table etj2pd_p (id int primary key, v varchar(32));
create table etj2pd_c (
    id int primary key,
    pid int,
    constraint fk_etj2pd foreign key (pid) references etj2pd_p(id)
    on update cascade on delete cascade
);
insert into etj2pd_p values (2, 'two');
insert into etj2pd_c values (1, 2);
data branch create table etj2pd_src from etj2pd_p;
begin;
delete from etj2pd_src where id = 2;
insert into etj2pd_src values (2, 'other');
commit;
data branch pick etj2pd_src into etj2pd_p keys(2) when conflict accept;
select id, v from etj2pd_p order by id;
select id, pid from etj2pd_c order by id;

-- Era-17 (r23 review F1), PICK flavor: the picked restore must not be
-- emitted ahead of the picked surviving move. Destination holds
-- (1,'A'),(2,'B') and deletes 1; the source moves B 2 -> 3 and A 1 -> 2
-- and both moves are picked. ACCEPT restores A at 2 and relocates B to
-- 3 with the child re-parented by the cascade; restore-first replay
-- loses both rows and the child.
create table etkf1p_p (id int primary key, v varchar(32));
create table etkf1p_c (
    id int primary key,
    pid int,
    constraint fk_etkf1p foreign key (pid) references etkf1p_p(id)
    on update cascade on delete cascade
);
insert into etkf1p_p values (1, 'A'), (2, 'B');
insert into etkf1p_c values (100, 2);
data branch create table etkf1p_src from etkf1p_p;
update etkf1p_src set id = 3 where id = 2;
update etkf1p_src set id = 2 where id = 1;
delete from etkf1p_p where id = 1;
data branch pick etkf1p_src into etkf1p_p keys(1,2) when conflict accept;
select id, v from etkf1p_p order by id;
select id, pid from etkf1p_c order by id;

-- Era-17 (r23 review F2), PICK flavor: a retained user snapshot of a
-- branch member guards its pk-move provenance through reclaim; picking
-- the old key from a historical clone built off that snapshot folds the
-- move as one native UPDATE so the child cascades instead of dying.
create table etkf2p_p (id int primary key, v varchar(32));
create table etkf2p_c (
    id int primary key,
    pid int,
    constraint fk_etkf2p foreign key (pid) references etkf2p_p(id)
    on update cascade on delete cascade
);
insert into etkf2p_p values (2, 'two');
insert into etkf2p_c values (1, 2);
data branch create table etkf2p_b from etkf2p_p;
update etkf2p_b set id = 3 where id = 2;
set @etkf2pb = (select rel_id from mo_catalog.mo_tables where relname = 'etkf2p_b' and reldatabase = 'branch_pick_fk_pk_move');
select count(*) from mo_catalog.mo_data_branch_update_link where table_id = @etkf2pb;
drop snapshot if exists etkf2p_snap;
create snapshot etkf2p_snap for table branch_pick_fk_pk_move etkf2p_b;
drop table etkf2p_b;
-- the retained snapshot keeps the dropped member's provenance row
select count(*) from mo_catalog.mo_data_branch_update_link where table_id = @etkf2pb;
data branch create table etkf2p_h from etkf2p_b{snapshot="etkf2p_snap"};
data branch pick etkf2p_h into etkf2p_p keys(2) when conflict accept;
select id, v from etkf2p_p order by id;
select id, pid from etkf2p_c order by id;
drop snapshot etkf2p_snap;

-- Era-18 (r24 review F1), PICK flavor: SKIP must propagate through
-- move dependencies. Destination holds (1,'A'),(2,'B') with B's child
-- plus an independent destination-only row (3,'C'); the source moves
-- B 2 -> 3 and A 1 -> 2 and both keys are picked. B's move conflicts
-- with (3,'C') and SKIP drops it; A's move needs B's lineage to
-- vacate key 2, so it must drop as well. Without the cascade, A's
-- skipped-dependent move keeps its authorized victim delete at key 2
-- and silently removes B and its cascaded child. Destination and
-- child must stay exactly as before.
create table etkf4p_p (id int primary key, v varchar(32));
create table etkf4p_c (
    id int primary key,
    pid int,
    constraint fk_etkf4p foreign key (pid) references etkf4p_p(id)
    on update cascade on delete cascade
);
insert into etkf4p_p values (1, 'A'), (2, 'B');
insert into etkf4p_c values (100, 2);
data branch create table etkf4p_src from etkf4p_p;
update etkf4p_src set id = 3 where id = 2;
update etkf4p_src set id = 2 where id = 1;
insert into etkf4p_p values (3, 'C');
data branch pick etkf4p_src into etkf4p_p keys(1,2) when conflict skip;
select id, v from etkf4p_p order by id;
select id, pid from etkf4p_c order by id;

-- Era-18 (r24 review F2), PICK flavor: snapshot coverage must match
-- the table's physical identity across renames; a rename between the
-- snapshot and the drop must not forfeit the retained provenance, and
-- picking the old key from a historical clone folds the move as one
-- native UPDATE so the child cascades instead of dying.
create table etkf2rp_p (id int primary key, v varchar(32));
create table etkf2rp_c (
    id int primary key,
    pid int,
    constraint fk_etkf2rp foreign key (pid) references etkf2rp_p(id)
    on update cascade on delete cascade
);
insert into etkf2rp_p values (2, 'two');
insert into etkf2rp_c values (1, 2);
data branch create table etkf2rp_b from etkf2rp_p;
update etkf2rp_b set id = 3 where id = 2;
set @etkf2rpb = (select rel_id from mo_catalog.mo_tables where relname = 'etkf2rp_b' and reldatabase = 'branch_pick_fk_pk_move');
select count(*) from mo_catalog.mo_data_branch_update_link where table_id = @etkf2rpb;
drop snapshot if exists etkf2rp_snap;
create snapshot etkf2rp_snap for table branch_pick_fk_pk_move etkf2rp_b;
alter table etkf2rp_b rename to etkf2rp_b2;
drop table etkf2rp_b2;
-- the retained snapshot keeps the renamed member's provenance row
select count(*) from mo_catalog.mo_data_branch_update_link where table_id = @etkf2rpb;
data branch create table etkf2rp_h from etkf2rp_b{snapshot="etkf2rp_snap"};
data branch pick etkf2rp_h into etkf2rp_p keys(2) when conflict accept;
select id, v from etkf2rp_p order by id;
select id, pid from etkf2rp_c order by id;
drop snapshot etkf2rp_snap;

-- Era-19 (r25 review F1), PICK flavor: the consumer-side skip decisions
-- must cascade through move dependencies. Destination holds (1,'A'),
-- (2,'B') with B's child, and the destination itself edits B's payload;
-- the source moves B 2 -> 3 and A 1 -> 2. PICK KEYS(1,2) SKIP records
-- key 2 as skipped and suppresses B's move; A's move needs B's lineage
-- to vacate key 2, so it must drop as well — otherwise its surviving
-- victim delete at key 2 silently removes B and cascades the child away
-- under reported success. Destination and child must stay exactly as
-- before.
create table etkf5p_p (id int primary key, v varchar(32));
create table etkf5p_c (
    id int primary key,
    pid int,
    constraint fk_etkf5p foreign key (pid) references etkf5p_p(id)
    on update cascade on delete cascade
);
insert into etkf5p_p values (1, 'A'), (2, 'B');
insert into etkf5p_c values (100, 2);
data branch create table etkf5p_src from etkf5p_p;
update etkf5p_src set id = 3 where id = 2;
update etkf5p_src set id = 2 where id = 1;
update etkf5p_p set v = 'B2' where id = 2;
data branch pick etkf5p_src into etkf5p_p keys(1,2) when conflict skip;
select id, v from etkf5p_p order by id;
select id, pid from etkf5p_c order by id;

-- Era-19 (r25 review F2), PICK flavor: a forced workspace spill, an
-- intra-transaction delete of a spilled row — the statement boundary
-- rewrites the dirty object and re-mints the SURVIVOR's identity — then
-- a pk move of that survivor. Pre-fix the commit lost the update link at
-- the rewrite, the pick replayed DELETE/INSERT, and ON DELETE CASCADE
-- erased the child. Post-fix one link survives and the picked key folds
-- as one native UPDATE: parent (3,'b2') next to the untouched sibling
-- (1,'a'), with the child following to pid 3.
create table etkf6p_p (id int primary key, v varchar(32));
create table etkf6p_c (
    id int primary key,
    pid int,
    constraint fk_etkf6p foreign key (pid) references etkf6p_p(id)
    on update cascade on delete cascade
);
insert into etkf6p_p values (1, 'a'), (2, 'b');
insert into etkf6p_c values (100, 2);
create table etkf6p_f (id int primary key, b int);
insert into etkf6p_f values (1, 1);
data branch create table etkf6p_s from etkf6p_p;
begin;
update etkf6p_s set v = 'a2' where id = 1;
update etkf6p_s set v = 'b2' where id = 2;
select enable_fault_injection();
select add_fault_point('fj/cn/workspace_force_flush',':::','echo',0,'');
update etkf6p_f set b = 9 where id = 1;
select remove_fault_point('fj/cn/workspace_force_flush');
select disable_fault_injection();
delete from etkf6p_s where id = 1;
update etkf6p_f set b = 10 where id = 1;
update etkf6p_s set id = 3 where id = 2;
commit;
set @etkf6pl = (select rel_id from mo_catalog.mo_tables where relname = 'etkf6p_s' and reldatabase = 'branch_pick_fk_pk_move');
select count(*) from mo_catalog.mo_data_branch_update_link where table_id = @etkf6pl;
data branch pick etkf6p_s into etkf6p_p keys(2) when conflict accept;
select id, v from etkf6p_p order by id;
select id, pid from etkf6p_c order by id;

-- Era-19 (r25 review F3), PICK flavor: under READ COMMITTED a source
-- transaction stages its pk move, then a committed-object L0 compaction
-- lands so the tombstone transfer re-mints the move's pending delete
-- onto the compacted generation at the next statement boundary. The
-- staged link must ride the identical re-key, or the committed tombstone
-- and the link diverge and the pick replays DELETE/INSERT, failing the
-- RESTRICT child loudly. Post-fix the picked key folds as one native
-- UPDATE: parent (3,'two') with the child following to pid 3.
set session transaction isolation level read committed;
create table etkf7p_p (id int primary key, v varchar(32));
create table etkf7p_c (
    id int primary key,
    pid int,
    constraint fk_etkf7p foreign key (pid) references etkf7p_p(id)
    on update cascade
);
insert into etkf7p_p values (2, 'two');
insert into etkf7p_c values (100, 2);
data branch create table etkf7p_s from etkf7p_p;
select mo_ctl('dn','flush','branch_pick_fk_pk_move.etkf7p_s');
insert into etkf7p_s values (900, 'pad');
select mo_ctl('dn','flush','branch_pick_fk_pk_move.etkf7p_s');
delete from etkf7p_s where id = 900;
select mo_ctl('dn','flush','branch_pick_fk_pk_move.etkf7p_s');
begin;
update etkf7p_s set id = 3 where id = 2;
-- @ignore:0
select mo_ctl('dn','inspect','merge trigger -t branch_pick_fk_pk_move.etkf7p_s --kind l0 --l0-oneshot') regexp 'trigger l0 merge for table [0-9]+-etkf7p_s, oneshot: true' as l0_triggered;
-- @wait_expect(1, 60)
select count(*) as merged_objects from metadata_scan('branch_pick_fk_pk_move.etkf7p_s', 'id') having count(*) = 1;
select count(*) as advance_snapshot from etkf7p_p;
select id, v from etkf7p_s;
commit;
set @etkf7pl = (select rel_id from mo_catalog.mo_tables where relname = 'etkf7p_s' and reldatabase = 'branch_pick_fk_pk_move');
select count(*) from mo_catalog.mo_data_branch_update_link where table_id = @etkf7pl;
data branch pick etkf7p_s into etkf7p_p keys(2) when conflict accept;
select id, v from etkf7p_p order by id;
select id, pid from etkf7p_c order by id;

-- Era-19 (r25 review F3), PICK rollback twin: the same transfer land-
-- then-rollback. The transferred tombstone and the staged provenance die
-- with the transaction: no link row is recorded, the branch keeps its
-- rows, and the destination is untouched; a fresh move afterwards
-- behaves like plain tables, proving the aborted transfer wedged
-- nothing.
create table etkf7rp_p (id int primary key, v varchar(32));
create table etkf7rp_c (
    id int primary key,
    pid int,
    constraint fk_etkf7rp foreign key (pid) references etkf7rp_p(id)
    on update cascade
);
insert into etkf7rp_p values (2, 'two');
insert into etkf7rp_c values (100, 2);
data branch create table etkf7rp_s from etkf7rp_p;
select mo_ctl('dn','flush','branch_pick_fk_pk_move.etkf7rp_s');
insert into etkf7rp_s values (900, 'pad');
select mo_ctl('dn','flush','branch_pick_fk_pk_move.etkf7rp_s');
delete from etkf7rp_s where id = 900;
select mo_ctl('dn','flush','branch_pick_fk_pk_move.etkf7rp_s');
begin;
update etkf7rp_s set id = 3 where id = 2;
-- @ignore:0
select mo_ctl('dn','inspect','merge trigger -t branch_pick_fk_pk_move.etkf7rp_s --kind l0 --l0-oneshot') regexp 'trigger l0 merge for table [0-9]+-etkf7rp_s, oneshot: true' as l0_triggered;
-- @wait_expect(1, 60)
select count(*) as merged_objects_r from metadata_scan('branch_pick_fk_pk_move.etkf7rp_s', 'id') having count(*) = 1;
select count(*) as advance_snapshot_r from etkf7rp_p;
rollback;
set @etkf7rpl = (select rel_id from mo_catalog.mo_tables where relname = 'etkf7rp_s' and reldatabase = 'branch_pick_fk_pk_move');
select count(*) from mo_catalog.mo_data_branch_update_link where table_id = @etkf7rpl;
select id, v from etkf7rp_s order by id;
select id, v from etkf7rp_p order by id;
select id, pid from etkf7rp_c order by id;
update etkf7rp_s set id = 4 where id = 2;
data branch pick etkf7rp_s into etkf7rp_p keys(2) when conflict accept;
select id, v from etkf7rp_p order by id;
select id, pid from etkf7rp_c order by id;
set session transaction isolation level repeatable read;

-- Era-20 (r26 review F2): composite PRIMARY KEY, edit-then-move within
-- ONE transaction, PICK by the departed composite key. Same defect as
-- the merge twin: the first replacement's workspace identity was
-- invisible to the provenance harvest, and the pick lost the move and
-- failed the child foreign key. Post-fix: one native UPDATE, child (2,2).
create table etkf8p_p (a int, b int, v varchar(32), primary key (a, b));
create table etkf8p_c (
    id int primary key,
    pa int,
    pb int,
    constraint fk_etkf8p foreign key (pa, pb) references etkf8p_p(a, b)
    on update cascade
);
insert into etkf8p_p values (1, 1, 'one');
insert into etkf8p_c values (100, 1, 1);
data branch create table etkf8p_s from etkf8p_p;
begin;
update etkf8p_s set v = 'one2' where a = 1 and b = 1;
update etkf8p_s set a = 2, b = 2 where a = 1 and b = 1;
commit;
set @etkf8pl = (select rel_id from mo_catalog.mo_tables where relname = 'etkf8p_s' and reldatabase = 'branch_pick_fk_pk_move');
select count(*) from mo_catalog.mo_data_branch_update_link where table_id = @etkf8pl;
data branch pick etkf8p_s into etkf8p_p keys((1,1)) when conflict accept;
select a, b, v from etkf8p_p order by a;
select id, pa, pb from etkf8p_c order by id;

-- Era-20 (r26 review F2), PICK spill twin: the same edit-then-composite
-- move with the edit's workspace anchor spilled to S3 mid-transaction.
create table etkf8sp_p (a int, b int, v varchar(32), primary key (a, b));
create table etkf8sp_c (
    id int primary key,
    pa int,
    pb int,
    constraint fk_etkf8sp foreign key (pa, pb) references etkf8sp_p(a, b)
    on update cascade
);
create table etkf8sp_f (id int primary key, b int);
insert into etkf8sp_p values (1, 1, 'one');
insert into etkf8sp_c values (100, 1, 1);
insert into etkf8sp_f values (1, 1);
data branch create table etkf8sp_s from etkf8sp_p;
begin;
update etkf8sp_s set v = 'one2' where a = 1 and b = 1;
select enable_fault_injection();
select add_fault_point('fj/cn/workspace_force_flush',':::','echo',0,'');
update etkf8sp_f set b = 9 where id = 1;
select remove_fault_point('fj/cn/workspace_force_flush');
select disable_fault_injection();
update etkf8sp_s set a = 2, b = 2 where a = 1 and b = 1;
commit;
set @etkf8spl = (select rel_id from mo_catalog.mo_tables where relname = 'etkf8sp_s' and reldatabase = 'branch_pick_fk_pk_move');
select count(*) from mo_catalog.mo_data_branch_update_link where table_id = @etkf8spl;
data branch pick etkf8sp_s into etkf8sp_p keys((1,1)) when conflict accept;
select a, b, v from etkf8sp_p order by a;
select id, pa, pb from etkf8sp_c order by id;

-- Era-20 (r26 review F3): PERSISTED tombstones through the RC transfer,
-- PICK twin of etkf9. Arming the flush-small-objects route publishes the
-- move's delete via the S3 writer; the L0 merge plus snapshot advance
-- transfers that persisted tombstone onto the compacted generation.
-- Post-fix the pick replays one native UPDATE: parent (3,'two'), child 3.
set session transaction isolation level read committed;
create table etkf9p_p (id int primary key, v varchar(32));
create table etkf9p_c (
    id int primary key,
    pid int,
    constraint fk_etkf9p foreign key (pid) references etkf9p_p(id)
    on update cascade
);
insert into etkf9p_p values (2, 'two');
insert into etkf9p_c values (100, 2);
data branch create table etkf9p_s from etkf9p_p;
select mo_ctl('dn','flush','branch_pick_fk_pk_move.etkf9p_s');
insert into etkf9p_s values (900, 'pad');
select mo_ctl('dn','flush','branch_pick_fk_pk_move.etkf9p_s');
delete from etkf9p_s where id = 900;
select mo_ctl('dn','flush','branch_pick_fk_pk_move.etkf9p_s');
begin;
select enable_fault_injection();
select add_fault_point('fj/cn/flush_small_objs',':::','echo',40,'branch_pick_fk_pk_move.etkf9p_s');
update etkf9p_s set id = 3 where id = 2;
select remove_fault_point('fj/cn/flush_small_objs');
select disable_fault_injection();
-- @ignore:0
select mo_ctl('dn','inspect','merge trigger -t branch_pick_fk_pk_move.etkf9p_s --kind l0 --l0-oneshot') regexp 'trigger l0 merge for table [0-9]+-etkf9p_s, oneshot: true' as l0_triggered;
-- @wait_expect(1, 60)
select count(*) as merged_objects_9p from metadata_scan('branch_pick_fk_pk_move.etkf9p_s', 'id') having count(*) = 1;
select count(*) as advance_snapshot_9p from etkf9p_p;
commit;
set @etkf9pl = (select rel_id from mo_catalog.mo_tables where relname = 'etkf9p_s' and reldatabase = 'branch_pick_fk_pk_move');
select count(*) from mo_catalog.mo_data_branch_update_link where table_id = @etkf9pl;
data branch pick etkf9p_s into etkf9p_p keys(2) when conflict accept;
select id, v from etkf9p_p order by id;
select id, pid from etkf9p_c order by id;

-- Era-20 (r26 review F3), PICK rollback twin: the persisted tombstone
-- transfers, then the transaction rolls back. No link row is recorded,
-- the branch keeps its rows, the destination is untouched, and a fresh
-- move afterwards behaves like plain tables.
create table etkf9rp_p (id int primary key, v varchar(32));
create table etkf9rp_c (
    id int primary key,
    pid int,
    constraint fk_etkf9rp foreign key (pid) references etkf9rp_p(id)
    on update cascade
);
insert into etkf9rp_p values (2, 'two');
insert into etkf9rp_c values (100, 2);
data branch create table etkf9rp_s from etkf9rp_p;
select mo_ctl('dn','flush','branch_pick_fk_pk_move.etkf9rp_s');
insert into etkf9rp_s values (900, 'pad');
select mo_ctl('dn','flush','branch_pick_fk_pk_move.etkf9rp_s');
delete from etkf9rp_s where id = 900;
select mo_ctl('dn','flush','branch_pick_fk_pk_move.etkf9rp_s');
begin;
select enable_fault_injection();
select add_fault_point('fj/cn/flush_small_objs',':::','echo',40,'branch_pick_fk_pk_move.etkf9rp_s');
update etkf9rp_s set id = 3 where id = 2;
select remove_fault_point('fj/cn/flush_small_objs');
select disable_fault_injection();
-- @ignore:0
select mo_ctl('dn','inspect','merge trigger -t branch_pick_fk_pk_move.etkf9rp_s --kind l0 --l0-oneshot') regexp 'trigger l0 merge for table [0-9]+-etkf9rp_s, oneshot: true' as l0_triggered;
-- @wait_expect(1, 60)
select count(*) as merged_objects_9rp from metadata_scan('branch_pick_fk_pk_move.etkf9rp_s', 'id') having count(*) = 1;
select count(*) as advance_snapshot_9rp from etkf9rp_p;
rollback;
set @etkf9rpl = (select rel_id from mo_catalog.mo_tables where relname = 'etkf9rp_s' and reldatabase = 'branch_pick_fk_pk_move');
select count(*) from mo_catalog.mo_data_branch_update_link where table_id = @etkf9rpl;
select id, v from etkf9rp_s order by id;
select id, v from etkf9rp_p order by id;
select id, pid from etkf9rp_c order by id;
update etkf9rp_s set id = 4 where id = 2;
data branch pick etkf9rp_s into etkf9rp_p keys(2) when conflict accept;
select id, v from etkf9rp_p order by id;
select id, pid from etkf9rp_c order by id;
set session transaction isolation level repeatable read;

-- Era-20 (r26 review F4), PICK twin: workspace-route payload edit, then
-- S3-route payload edit (lineage bind), then the RC transfer of the
-- persisted tombstone, then the move. The binding must follow the
-- transfer's re-key or the move heals onto a stale origin and the pick
-- replays DELETE/INSERT. Post-fix: one native UPDATE, parent (3,'two3').
set session transaction isolation level read committed;
create table etkf10p_p (id int primary key, v varchar(32));
create table etkf10p_c (
    id int primary key,
    pid int,
    constraint fk_etkf10p foreign key (pid) references etkf10p_p(id)
    on update cascade
);
insert into etkf10p_p values (2, 'two');
insert into etkf10p_c values (100, 2);
data branch create table etkf10p_s from etkf10p_p;
select mo_ctl('dn','flush','branch_pick_fk_pk_move.etkf10p_s');
insert into etkf10p_s values (900, 'pad');
select mo_ctl('dn','flush','branch_pick_fk_pk_move.etkf10p_s');
delete from etkf10p_s where id = 900;
select mo_ctl('dn','flush','branch_pick_fk_pk_move.etkf10p_s');
begin;
update etkf10p_s set v = 'two2' where id = 2;
select enable_fault_injection();
select add_fault_point('fj/cn/flush_small_objs',':::','echo',40,'branch_pick_fk_pk_move.etkf10p_s');
update etkf10p_s set v = 'two3' where id = 2;
select remove_fault_point('fj/cn/flush_small_objs');
select disable_fault_injection();
-- @ignore:0
select mo_ctl('dn','inspect','merge trigger -t branch_pick_fk_pk_move.etkf10p_s --kind l0 --l0-oneshot') regexp 'trigger l0 merge for table [0-9]+-etkf10p_s, oneshot: true' as l0_triggered;
-- @wait_expect(1, 60)
select count(*) as merged_objects_10p from metadata_scan('branch_pick_fk_pk_move.etkf10p_s', 'id') having count(*) = 1;
select count(*) as advance_snapshot_10p from etkf10p_p;
update etkf10p_s set id = 3 where id = 2;
commit;
set @etkf10pl = (select rel_id from mo_catalog.mo_tables where relname = 'etkf10p_s' and reldatabase = 'branch_pick_fk_pk_move');
select count(*) from mo_catalog.mo_data_branch_update_link where table_id = @etkf10pl;
data branch pick etkf10p_s into etkf10p_p keys(2) when conflict accept;
select id, v from etkf10p_p order by id;
select id, pid from etkf10p_c order by id;

-- Era-20 (r26 review F3 residual, D113): PICK twin of the persisted
-- tombstone transfer's atomic replacement. The armed move stages a
-- file-backed tombstone; the L0 oneshot re-mints the old row's committed
-- object, and the RC transfer re-keys the pending tombstone onto the new
-- identity while carrying the link. The superseded source object must never
-- publish beside that replacement: pre-D113 both committed, the picked key
-- surfaced two tombstones, the chain evidence failed closed unresolved, and
-- the pick died with ERROR 20101. The move now applies as one native UPDATE.
set session transaction isolation level read committed;
create table etkf11p_p (id int primary key, v varchar(32));
create table etkf11p_c (
    id int primary key,
    pid int,
    constraint fk_etkf11p foreign key (pid) references etkf11p_p(id)
    on update cascade
);
insert into etkf11p_p values (2, 'two');
insert into etkf11p_c values (100, 2);
data branch create table etkf11p_s from etkf11p_p;
select mo_ctl('dn','flush','branch_pick_fk_pk_move.etkf11p_s');
insert into etkf11p_s values (900, 'pad');
select mo_ctl('dn','flush','branch_pick_fk_pk_move.etkf11p_s');
delete from etkf11p_s where id = 900;
select mo_ctl('dn','flush','branch_pick_fk_pk_move.etkf11p_s');
begin;
select enable_fault_injection();
select add_fault_point('fj/cn/flush_small_objs',':::','echo',40,'branch_pick_fk_pk_move.etkf11p_s');
update etkf11p_s set id = 3 where id = 2;
select remove_fault_point('fj/cn/flush_small_objs');
select disable_fault_injection();
-- @ignore:0
select mo_ctl('dn','inspect','merge trigger -t branch_pick_fk_pk_move.etkf11p_s --kind l0 --l0-oneshot') regexp 'trigger l0 merge for table [0-9]+-etkf11p_s, oneshot: true' as l0_triggered;
-- @wait_expect(1, 60)
select count(*) as merged_objects_8p from metadata_scan('branch_pick_fk_pk_move.etkf11p_s', 'id') having count(*) = 1;
select count(*) as advance_snapshot_8p from etkf11p_p;
select id, v from etkf11p_s;
commit;
set @etkf11pl = (select rel_id from mo_catalog.mo_tables where relname = 'etkf11p_s' and reldatabase = 'branch_pick_fk_pk_move');
select count(*) from mo_catalog.mo_data_branch_update_link where table_id = @etkf11pl;
data branch pick etkf11p_s into etkf11p_p keys(2) when conflict accept;
select id, v from etkf11p_p order by id;
select id, pid from etkf11p_c order by id;

-- Era-20 (D113), rollback twin: the same persisted transfer fires inside the
-- txn, then the txn rolls back. Retirement dies with the transaction: no
-- link rows, the branch keeps its rows, the destination stays put; a fresh
-- move afterwards picks and cascades.
create table etkf11rp_p (id int primary key, v varchar(32));
create table etkf11rp_c (
    id int primary key,
    pid int,
    constraint fk_etkf11rp foreign key (pid) references etkf11rp_p(id)
    on update cascade
);
insert into etkf11rp_p values (2, 'two');
insert into etkf11rp_c values (100, 2);
data branch create table etkf11rp_s from etkf11rp_p;
select mo_ctl('dn','flush','branch_pick_fk_pk_move.etkf11rp_s');
insert into etkf11rp_s values (900, 'pad');
select mo_ctl('dn','flush','branch_pick_fk_pk_move.etkf11rp_s');
delete from etkf11rp_s where id = 900;
select mo_ctl('dn','flush','branch_pick_fk_pk_move.etkf11rp_s');
begin;
select enable_fault_injection();
select add_fault_point('fj/cn/flush_small_objs',':::','echo',40,'branch_pick_fk_pk_move.etkf11rp_s');
update etkf11rp_s set id = 3 where id = 2;
select remove_fault_point('fj/cn/flush_small_objs');
select disable_fault_injection();
-- @ignore:0
select mo_ctl('dn','inspect','merge trigger -t branch_pick_fk_pk_move.etkf11rp_s --kind l0 --l0-oneshot') regexp 'trigger l0 merge for table [0-9]+-etkf11rp_s, oneshot: true' as l0_triggered;
-- @wait_expect(1, 60)
select count(*) as merged_objects_8rp from metadata_scan('branch_pick_fk_pk_move.etkf11rp_s', 'id') having count(*) = 1;
select count(*) as advance_snapshot_8rp from etkf11rp_p;
rollback;
set @etkf11rpl = (select rel_id from mo_catalog.mo_tables where relname = 'etkf11rp_s' and reldatabase = 'branch_pick_fk_pk_move');
select count(*) from mo_catalog.mo_data_branch_update_link where table_id = @etkf11rpl;
select id, v from etkf11rp_s order by id;
select id, v from etkf11rp_p order by id;
select id, pid from etkf11rp_c order by id;
update etkf11rp_s set id = 4 where id = 2;
data branch pick etkf11rp_s into etkf11rp_p keys(2) when conflict accept;
select id, v from etkf11rp_p order by id;
select id, pid from etkf11rp_c order by id;

-- Era-21 (r27 review F1): PICK twin of the two persisted-tombstone transfer
-- windows in one transaction. Window A's transfer retires its consumed
-- staged tombstone entries (bat=nil shells carrying the fileName); window
-- B's persisted-DELETE scan crosses those retired slots and must skip them
-- (D114) rather than dereference the nil batch. The duplicate insert
-- between the windows forces a statement rollback mid-transaction (ERROR
-- 1062 captured by genrs); the commit still succeeds and each moved key
-- publishes its link exactly once, so the four picks of the old keys apply
-- the moves as native UPDATEs with the children following.
set session transaction isolation level read committed;
create table etkf12p_p (id int primary key, v varchar(32));
create table etkf12p_c (
    id int primary key,
    pid int,
    constraint fk_etkf12p foreign key (pid) references etkf12p_p(id)
    on update cascade
);
insert into etkf12p_p values (2, 'two'), (3, 'three'), (4, 'four'), (5, 'five');
insert into etkf12p_c values (100, 2), (200, 3), (300, 4), (400, 5);
data branch create table etkf12p_s from etkf12p_p;
select mo_ctl('dn','flush','branch_pick_fk_pk_move.etkf12p_s');
insert into etkf12p_s values (900, 'pad');
select mo_ctl('dn','flush','branch_pick_fk_pk_move.etkf12p_s');
delete from etkf12p_s where id = 900;
select mo_ctl('dn','flush','branch_pick_fk_pk_move.etkf12p_s');
begin;
select enable_fault_injection();
select add_fault_point('fj/cn/flush_small_objs',':::','echo',40,'branch_pick_fk_pk_move.etkf12p_s');
update etkf12p_s set id = 12 where id = 2;
update etkf12p_s set id = 13 where id = 3;
select remove_fault_point('fj/cn/flush_small_objs');
select disable_fault_injection();
-- @ignore:0
select mo_ctl('dn','inspect','merge trigger -t branch_pick_fk_pk_move.etkf12p_s --kind l0 --l0-oneshot') regexp 'trigger l0 merge for table [0-9]+-etkf12p_s, oneshot: true' as l0_triggered;
-- @wait_expect(1, 60)
select count(*) as merged_objects_12ap from metadata_scan('branch_pick_fk_pk_move.etkf12p_s', 'id') having count(*) = 1;
select count(*) as advance_snapshot_12ap from etkf12p_p;
insert into etkf12p_s values (12, 'dupe');
select enable_fault_injection();
select add_fault_point('fj/cn/flush_small_objs',':::','echo',40,'branch_pick_fk_pk_move.etkf12p_s');
update etkf12p_s set id = 14 where id = 4;
update etkf12p_s set id = 15 where id = 5;
select remove_fault_point('fj/cn/flush_small_objs');
select disable_fault_injection();
-- @ignore:0
select mo_ctl('dn','inspect','merge trigger -t branch_pick_fk_pk_move.etkf12p_s --kind l0 --l0-oneshot') regexp 'trigger l0 merge for table [0-9]+-etkf12p_s, oneshot: true' as l0_triggered;
-- @wait_expect(1, 60)
select count(*) as merged_objects_12bp from metadata_scan('branch_pick_fk_pk_move.etkf12p_s', 'id') having count(*) = 1;
select count(*) as advance_snapshot_12bp from etkf12p_p;
select id, v from etkf12p_s order by id;
commit;
set @etkf12pl = (select rel_id from mo_catalog.mo_tables where relname = 'etkf12p_s' and reldatabase = 'branch_pick_fk_pk_move');
select count(*) from mo_catalog.mo_data_branch_update_link where table_id = @etkf12pl;
data branch pick etkf12p_s into etkf12p_p keys(2) when conflict accept;
data branch pick etkf12p_s into etkf12p_p keys(3) when conflict accept;
data branch pick etkf12p_s into etkf12p_p keys(4) when conflict accept;
data branch pick etkf12p_s into etkf12p_p keys(5) when conflict accept;
select id, v from etkf12p_p order by id;
select id, pid from etkf12p_c order by id;
set session transaction isolation level repeatable read;

-- Era-21 (r47 D154-A): PICK flavor of the etkf2 reorder twin. The
-- retained user snapshot is dropped BETWEEN the historical branch-create
-- and the pick; the live descendant holds the member's link and ledger
-- rows (ownership transfer). Picking the OLD key of the retained move
-- folds it as one native UPDATE cascading the child; retiring the final
-- descendant discharges both rows.
create table etkf2op_p (id int primary key, v varchar(32));
create table etkf2op_c (
    id int primary key,
    pid int,
    constraint fk_etkf2op foreign key (pid) references etkf2op_p(id)
    on update cascade on delete cascade
);
insert into etkf2op_p values (2, 'two');
insert into etkf2op_c values (1, 2);
data branch create table etkf2op_b from etkf2op_p;
update etkf2op_b set id = 3 where id = 2;
set @etkf2opb = (select rel_id from mo_catalog.mo_tables where relname = 'etkf2op_b' and reldatabase = 'branch_pick_fk_pk_move');
drop snapshot if exists etkf2op_snap;
create snapshot etkf2op_snap for table branch_pick_fk_pk_move etkf2op_b;
drop table etkf2op_b;
data branch create table etkf2op_h from etkf2op_b{snapshot="etkf2op_snap"};
drop snapshot etkf2op_snap;
select count(*) from mo_catalog.mo_data_branch_update_link where table_id = @etkf2opb;
select count(*) from mo_catalog.mo_data_branch_link_retention where table_id = @etkf2opb;
data branch pick etkf2op_h into etkf2op_p keys(2) when conflict accept;
select id, v from etkf2op_p order by id;
select id, pid from etkf2op_c order by id;
drop table etkf2op_h;
select count(*) from mo_catalog.mo_data_branch_update_link where table_id = @etkf2opb;
select count(*) from mo_catalog.mo_data_branch_link_retention where table_id = @etkf2opb;

-- Era-21: RESTRICT PICK reorder twin. Same shape without ON UPDATE
-- actions: the fold trips the foreign key, the pick aborts atomically
-- leaving both tables byte-identical, and retiring the descendant
-- discharges the retention rows.
create table etkf2orp_p (id int primary key, v varchar(32));
create table etkf2orp_c (
    id int primary key,
    pid int,
    constraint fk_etkf2orp foreign key (pid) references etkf2orp_p(id)
);
insert into etkf2orp_p values (2, 'two');
insert into etkf2orp_c values (1, 2);
data branch create table etkf2orp_b from etkf2orp_p;
update etkf2orp_b set id = 3 where id = 2;
set @etkf2orpb = (select rel_id from mo_catalog.mo_tables where relname = 'etkf2orp_b' and reldatabase = 'branch_pick_fk_pk_move');
drop snapshot if exists etkf2orp_snap;
create snapshot etkf2orp_snap for table branch_pick_fk_pk_move etkf2orp_b;
drop table etkf2orp_b;
data branch create table etkf2orp_h from etkf2orp_b{snapshot="etkf2orp_snap"};
drop snapshot etkf2orp_snap;
select count(*) from mo_catalog.mo_data_branch_update_link where table_id = @etkf2orpb;
data branch pick etkf2orp_h into etkf2orp_p keys(2) when conflict accept;
select id, v from etkf2orp_p order by id;
select id, pid from etkf2orp_c order by id;
drop table etkf2orp_h;
select count(*) from mo_catalog.mo_data_branch_update_link where table_id = @etkf2orpb;
select count(*) from mo_catalog.mo_data_branch_link_retention where table_id = @etkf2orpb;

-- Era-21 (r48 D155-A): sys-owned owner variant, PICK flavor. The cluster
-- snapshot row lives only in SYS's mo_snapshots; it grounds the retired
-- member, the descendant pick folds the retained move, and the final
-- retire discharges both rows.
create table etkf2ocp_p (id int primary key, v varchar(32));
create table etkf2ocp_c (
    id int primary key,
    pid int,
    constraint fk_etkf2ocp foreign key (pid) references etkf2ocp_p(id)
    on update cascade on delete cascade
);
insert into etkf2ocp_p values (2, 'two');
insert into etkf2ocp_c values (1, 2);
data branch create table etkf2ocp_b from etkf2ocp_p;
update etkf2ocp_b set id = 3 where id = 2;
set @etkf2ocpb = (select rel_id from mo_catalog.mo_tables where relname = 'etkf2ocp_b' and reldatabase = 'branch_pick_fk_pk_move');
drop snapshot if exists etkf2ocp_snap;
create snapshot etkf2ocp_snap for cluster;
drop table etkf2ocp_b;
select count(*) from mo_catalog.mo_data_branch_update_link where table_id = @etkf2ocpb;
select count(*) from mo_catalog.mo_data_branch_link_retention where table_id = @etkf2ocpb;
data branch create table etkf2ocp_h from etkf2ocp_b{snapshot="etkf2ocp_snap"};
drop snapshot etkf2ocp_snap;
select count(*) from mo_catalog.mo_data_branch_update_link where table_id = @etkf2ocpb;
data branch pick etkf2ocp_h into etkf2ocp_p keys(2) when conflict accept;
select id, v from etkf2ocp_p order by id;
select id, pid from etkf2ocp_c order by id;
drop table etkf2ocp_h;
select count(*) from mo_catalog.mo_data_branch_update_link where table_id = @etkf2ocpb;
select count(*) from mo_catalog.mo_data_branch_link_retention where table_id = @etkf2ocpb;

-- Era-22 (r50 P1): PICK twin of the database-level owner vs the
-- orphan-parent KEEP row. The branch key move picks into the parent as
-- one native UPDATE (parent carried link), a DATABASE-scope snapshot
-- grounds the namespace, retiring the last branch persists the parent's
-- KEEP row WITH identity, the unrelated-owner reap cannot delete it, a
-- historical clone still folds, and the owner drop discharges the rows.
create table etkf3pp_p (id int primary key, v varchar(32));
create table etkf3pp_c (
    id int primary key,
    pid int,
    constraint fk_etkf3pp foreign key (pid) references etkf3pp_p(id)
    on update cascade on delete cascade
);
insert into etkf3pp_p values (2, 'two');
insert into etkf3pp_c values (1, 2);
data branch create table etkf3pp_b from etkf3pp_p;
data branch create table etkf3pp_x from etkf3pp_p;
update etkf3pp_b set id = 3 where id = 2;
set @etkf3ppp = (select rel_id from mo_catalog.mo_tables where relname = 'etkf3pp_p' and reldatabase = 'branch_pick_fk_pk_move');
set @etkf3ppb = (select rel_id from mo_catalog.mo_tables where relname = 'etkf3pp_b' and reldatabase = 'branch_pick_fk_pk_move');
set @etkf3ppx = (select rel_id from mo_catalog.mo_tables where relname = 'etkf3pp_x' and reldatabase = 'branch_pick_fk_pk_move');
data branch pick etkf3pp_b into etkf3pp_p keys(2) when conflict accept;
select id, v from etkf3pp_p order by id;
select id, pid from etkf3pp_c order by id;
create snapshot etkf3pp_snap for database branch_pick_fk_pk_move;
drop table etkf3pp_b;
drop table etkf3pp_x;
select count(*) from mo_catalog.mo_data_branch_update_link where table_id = @etkf3ppp;
select count(*) from mo_catalog.mo_data_branch_link_retention where table_id = @etkf3ppp;
select count(*) from mo_catalog.mo_data_branch_link_retention where table_id = @etkf3ppb;
select count(*) from mo_catalog.mo_data_branch_link_retention where table_id = @etkf3ppx;
create table etkf3pp_n (id int primary key);
drop snapshot if exists etkf3pp_noise;
create snapshot etkf3pp_noise for table branch_pick_fk_pk_move etkf3pp_n;
drop snapshot etkf3pp_noise;
select count(*) from mo_catalog.mo_data_branch_update_link where table_id = @etkf3ppp;
select count(*) from mo_catalog.mo_data_branch_link_retention where table_id = @etkf3ppp;
select count(*) from mo_catalog.mo_data_branch_link_retention where table_id = @etkf3ppb;
select count(*) from mo_catalog.mo_data_branch_link_retention where table_id = @etkf3ppx;
data branch create table etkf3pp_h from etkf3pp_x{snapshot="etkf3pp_snap"};
data branch pick etkf3pp_h into etkf3pp_p keys(2) when conflict accept;
select id, v from etkf3pp_p order by id;
select id, pid from etkf3pp_c order by id;
select count(*) from mo_catalog.mo_data_branch_update_link where table_id = @etkf3ppp;
drop table etkf3pp_h;
drop snapshot etkf3pp_snap;
select count(*) from mo_catalog.mo_data_branch_update_link where table_id = @etkf3ppp;
select count(*) from mo_catalog.mo_data_branch_link_retention where table_id = @etkf3ppp;
select count(*) from mo_catalog.mo_data_branch_link_retention where table_id = @etkf3ppb;
select count(*) from mo_catalog.mo_data_branch_link_retention where table_id = @etkf3ppx;

-- Era-23 (review r51 F1): the destination's own move committed AFTER the
-- BETWEEN upper snapshot still belongs to the closure window. The parent
-- table is shared by the source and destination paths, so its window
-- must carry the GREATEST required endpoint; keeping only the source
-- (BETWEEN-narrowed) bound hides the destination's late 2 -> 4 link and
-- aborts the fold. ACCEPT lands the source parent at 3 and cascades the
-- child back to 3.
drop snapshot if exists dwm_a;
drop snapshot if exists dwm_b;
create table dwm_p (id int primary key, v varchar(32));
create table dwm_c (
    id int primary key,
    pid int,
    constraint fk_dwm foreign key (pid) references dwm_p(id) on update cascade
);
insert into dwm_p values (2, 'two');
insert into dwm_c values (1, 2);
data branch create table dwm_src from dwm_p;
create snapshot dwm_a for account sys;
update dwm_src set id = 3, v = 'three' where id = 2;
create snapshot dwm_b for account sys;
update dwm_p set id = 4, v = 'four' where id = 2;
select id, v from dwm_p order by id;
select id, pid from dwm_c order by id;
set @dwml = (select rel_id from mo_catalog.mo_tables where relname = 'dwm_src' and reldatabase = 'branch_pick_fk_pk_move');
select count(*) from mo_catalog.mo_data_branch_update_link where table_id = @dwml;
data branch pick dwm_src into dwm_p between snapshot dwm_a and dwm_b keys(2) when conflict accept;
select id, v from dwm_p order by id;
select id, pid from dwm_c order by id;
drop snapshot dwm_a;
drop snapshot dwm_b;

-- era-24 dlpk_: PICK flavor of the competing-lineage counterexample.
-- The same lineage confusion as dlp_ (MERGE) must not rewrite parent B
-- as parent A and drop child 20: after the pick, the destination holds
-- exactly parent (3,'A') and child (20,3).
drop snapshot if exists dlpk_a;
drop snapshot if exists dlpk_b;
create table dlpk_p (id int primary key, v varchar(16));
create table dlpk_c (id int primary key, pid int,
    constraint fk_dlpk foreign key (pid) references dlpk_p(id) on update cascade on delete cascade);
insert into dlpk_p values (1,'B'),(2,'A');
insert into dlpk_c values (10,1),(20,2);
data branch create table dlpk_src from dlpk_p;
create snapshot dlpk_a for account sys;
begin;
delete from dlpk_src where id=1;
update dlpk_src set id=1 where id=2;
commit;
update dlpk_src set id=3 where id=1;
create snapshot dlpk_b for account sys;
data branch pick dlpk_src into dlpk_p between snapshot dlpk_a and dlpk_b keys(1,2) when conflict accept;
select id, v from dlpk_p order by id;
select id, pid from dlpk_c order by id;
drop snapshot dlpk_a;
drop snapshot dlpk_b;

-- era-24 dlpmk_: reversed key ordering under PICK (delete the higher
-- key, move the lower key up into it).
drop snapshot if exists dlpmk_a;
drop snapshot if exists dlpmk_b;
create table dlpmk_p (id int primary key, v varchar(16));
create table dlpmk_c (id int primary key, pid int,
    constraint fk_dlpmk foreign key (pid) references dlpmk_p(id) on update cascade on delete cascade);
insert into dlpmk_p values (1,'B'),(2,'A');
insert into dlpmk_c values (10,1),(20,2);
data branch create table dlpmk_src from dlpmk_p;
create snapshot dlpmk_a for account sys;
begin;
delete from dlpmk_src where id=2;
update dlpmk_src set id=2 where id=1;
commit;
update dlpmk_src set id=3 where id=2;
create snapshot dlpmk_b for account sys;
data branch pick dlpmk_src into dlpmk_p between snapshot dlpmk_a and dlpmk_b keys(1,2) when conflict accept;
select id, v from dlpmk_p order by id;
select id, pid from dlpmk_c order by id;
drop snapshot dlpmk_a;
drop snapshot dlpmk_b;

-- era-25 (r53 P1): same-transaction arrival chains 2 -> 4 -> 1 -> 3
-- through PICK BETWEEN. KEYS(1) applies only key 1's changes: the
-- genuine B lineage is deleted with its declared cascade (child 10),
-- the unselected key-2 lineage stays untouched at (2,'A')/(20,2).
drop snapshot if exists clk1_a;
drop snapshot if exists clk1_b;
create table clk1_p (id int primary key, v varchar(16));
create table clk1_c (id int primary key, pid int,
    constraint fk_clk1 foreign key (pid) references clk1_p(id) on update cascade on delete cascade);
insert into clk1_p values (1,'B'),(2,'A');
insert into clk1_c values (10,1),(20,2);
data branch create table clk1_src from clk1_p;
create snapshot clk1_a for account sys;
begin;
delete from clk1_src where id=1;
update clk1_src set id=4 where id=2;
update clk1_src set id=1 where id=4;
commit;
update clk1_src set id=3 where id=1;
create snapshot clk1_b for account sys;
data branch pick clk1_src into clk1_p between snapshot clk1_a and clk1_b keys(1) when conflict accept;
select id, v from clk1_p order by id;
select id, pid from clk1_c order by id;
drop snapshot clk1_a;
drop snapshot clk1_b;

-- KEYS(1,2) selects both anchors: the same complete-provenance MERGE
-- semantics apply — parent (3,'A') with child (20,3).
drop snapshot if exists clk12_a;
drop snapshot if exists clk12_b;
create table clk12_p (id int primary key, v varchar(16));
create table clk12_c (id int primary key, pid int,
    constraint fk_clk12 foreign key (pid) references clk12_p(id) on update cascade on delete cascade);
insert into clk12_p values (1,'B'),(2,'A');
insert into clk12_c values (10,1),(20,2);
data branch create table clk12_src from clk12_p;
create snapshot clk12_a for account sys;
begin;
delete from clk12_src where id=1;
update clk12_src set id=4 where id=2;
update clk12_src set id=1 where id=4;
commit;
update clk12_src set id=3 where id=1;
create snapshot clk12_b for account sys;
data branch pick clk12_src into clk12_p between snapshot clk12_a and clk12_b keys(1,2) when conflict accept;
select id, v from clk12_p order by id;
select id, pid from clk12_c order by id;
drop snapshot clk12_a;
drop snapshot clk12_b;

-- Reversed-key mirror: delete key 2, move 1 -> 4 -> 2 in one
-- transaction, then 2 -> 3; KEYS(1,2) yields parent (3,'B'), child (10,3).
drop snapshot if exists clkm_a;
drop snapshot if exists clkm_b;
create table clkm_p (id int primary key, v varchar(16));
create table clkm_c (id int primary key, pid int,
    constraint fk_clkm foreign key (pid) references clkm_p(id) on update cascade on delete cascade);
insert into clkm_p values (1,'B'),(2,'A');
insert into clkm_c values (10,1),(20,2);
data branch create table clkm_src from clkm_p;
create snapshot clkm_a for account sys;
begin;
delete from clkm_src where id=2;
update clkm_src set id=4 where id=1;
update clkm_src set id=2 where id=4;
commit;
update clkm_src set id=3 where id=2;
create snapshot clkm_b for account sys;
data branch pick clkm_src into clkm_p between snapshot clkm_a and clkm_b keys(1,2) when conflict accept;
select id, v from clkm_p order by id;
select id, pid from clkm_c order by id;
drop snapshot clkm_a;
drop snapshot clkm_b;

-- era-26 (r54 F1): after the whole-window link scan overflows the reader
-- budget, per-key OLD-key probes cannot enumerate arrivals AT a probed
-- key; the continuity tie must fail closed and abort atomically rather
-- than fold the unenumerated 2->4->1 arrival onto the standing version.
-- The armed budget (3) narrows the window: the 3-link chain plus 2
-- unrelated moves cross it.
create table cov1_p (id int primary key, v varchar(16));
create table cov1_c (id int primary key, pid int,
    constraint fk_cov1 foreign key (pid) references cov1_p(id) on update cascade on delete cascade);
insert into cov1_p values (1,'B'),(2,'A'),(7,'G'),(8,'H');
insert into cov1_c values (10,1),(20,2);
drop snapshot if exists cov1_a;
drop snapshot if exists cov1_b;
data branch create table cov1_src from cov1_p;
create snapshot cov1_a for account sys;
begin;
delete from cov1_src where id=1;
update cov1_src set id=4 where id=2;
update cov1_src set id=1 where id=4;
commit;
update cov1_src set id=3 where id=1;
-- Unrelated move history pushing the window past the armed reader budget.
update cov1_src set id=71 where id=7;
update cov1_src set id=81 where id=8;
create snapshot cov1_b for account sys;
select enable_fault_injection();
select add_fault_point('fj/cn/data_branch_pk_move_link_probe_budget', ':::', 'echo', 3, '');
data branch pick cov1_src into cov1_p between snapshot cov1_a and cov1_b keys(1) when conflict accept;
select remove_fault_point('fj/cn/data_branch_pk_move_link_probe_budget');
select disable_fault_injection();
-- Failure atomicity: the aborted pick left every destination row untouched.
select id, v from cov1_p order by id;
select id, pid from cov1_c order by id;
drop snapshot cov1_a;
drop snapshot cov1_b;

-- era-26 (r54 F2): two lineages legitimately share temporary key 4 in
-- one transaction (A: 2->4->1->3, C: 5->4->6). Attribution binds each
-- chain by exact workspace identity, so both survive PICK with their
-- cascades: parents (3,'A'),(6,'C') and children (20,3),(30,6); key 1
-- keeps its own delete semantics (child 10 cascades away).
drop snapshot if exists ctkp_a;
drop snapshot if exists ctkp_b;
create table ctkp_p (id int primary key, v varchar(16));
create table ctkp_c (id int primary key, pid int,
    constraint fk_ctkp foreign key (pid) references ctkp_p(id) on update cascade on delete cascade);
insert into ctkp_p values (1,'B'),(2,'A'),(5,'C');
insert into ctkp_c values (10,1),(20,2),(30,5);
data branch create table ctkp_src from ctkp_p;
create snapshot ctkp_a for account sys;
begin;
delete from ctkp_src where id=1;
update ctkp_src set id=4 where id=2;
update ctkp_src set id=1 where id=4;
update ctkp_src set id=4 where id=5;
update ctkp_src set id=6 where id=4;
commit;
update ctkp_src set id=3 where id=1;
create snapshot ctkp_b for account sys;
data branch pick ctkp_src into ctkp_p between snapshot ctkp_a and ctkp_b keys(1,2,5) when conflict accept;
select id, v from ctkp_p order by id;
select id, pid from ctkp_c order by id;
drop snapshot ctkp_a;
drop snapshot ctkp_b;

drop snapshot if exists etkf2p_snap;
drop snapshot if exists etkf2rp_snap;

use branch_pick_fk_pk_move;

-- r58 review F1 (issue #28026): a picked lineage whose move-chain tail
-- died linkless must not adopt a FOREIGN lineage's row re-born on that
-- same key at the same instant. A moves 1->2 in its own transaction;
-- one later transaction deletes the linkless landed row at 2 and moves
-- B 3->2. Picking A's OLD key (1) must replay only A's terminal delete
-- (cascading child 10 away), leaving the unselected destination row at
-- 3 untouched — the foreign row at 2 belongs to B's lineage.
create table stail_p (id int primary key, v varchar(16));
create table stail_c (id int primary key, pid int,
    constraint fk_stail foreign key (pid) references stail_p(id) on update cascade on delete cascade);
insert into stail_p values (1,'A'),(3,'B');
insert into stail_c values (10,1),(30,3);
data branch create table stail_s from stail_p;
update stail_s set id=2 where id=1;
begin;
delete from stail_s where id=2;
update stail_s set id=2 where id=3;
commit;
data branch pick stail_s into stail_p keys(1) when conflict accept;
select id, v from stail_p order by id;
select id, pid from stail_c order by id;

-- Companion control: picking the foreign lineage's OWN old key (3)
-- still folds B's move natively, re-parenting child 30 onto key 2.
data branch pick stail_s into stail_p keys(3) when conflict accept;
select id, v from stail_p order by id;
select id, pid from stail_c order by id;

-- r59 review F1 (issue #28026): a truncated foreign arrival must fail
-- the tail fold CLOSED at commit time, never at creation time. A moves
-- 1->2 in its own transaction; one later transaction (creation instant
-- distinct from its commit instant) deletes A's landed row at 2 and
-- moves B through 3->4->...->42->2 in forty hops, so B's origin's
-- grounding tombstone commits in-window beyond the attribution hop cap.
-- Picking A's old key (1) must reject the unresolved evidence
-- atomically, leaving every destination parent/child row unchanged.
create table tc_p (id int primary key, v varchar(16));
create table tc_c (id int primary key, pid int,
    constraint fk_tc foreign key (pid) references tc_p(id) on update cascade on delete cascade);
insert into tc_p values (1,'A'),(3,'B');
insert into tc_c values (10,1),(30,3);
data branch create table tc_s from tc_p;
update tc_s set id=2 where id=1;
begin;
delete from tc_s where id=2;
update tc_s set id=4 where id=3;
update tc_s set id=5 where id=4;
update tc_s set id=6 where id=5;
update tc_s set id=7 where id=6;
update tc_s set id=8 where id=7;
update tc_s set id=9 where id=8;
update tc_s set id=10 where id=9;
update tc_s set id=11 where id=10;
update tc_s set id=12 where id=11;
update tc_s set id=13 where id=12;
update tc_s set id=14 where id=13;
update tc_s set id=15 where id=14;
update tc_s set id=16 where id=15;
update tc_s set id=17 where id=16;
update tc_s set id=18 where id=17;
update tc_s set id=19 where id=18;
update tc_s set id=20 where id=19;
update tc_s set id=21 where id=20;
update tc_s set id=22 where id=21;
update tc_s set id=23 where id=22;
update tc_s set id=24 where id=23;
update tc_s set id=25 where id=24;
update tc_s set id=26 where id=25;
update tc_s set id=27 where id=26;
update tc_s set id=28 where id=27;
update tc_s set id=29 where id=28;
update tc_s set id=30 where id=29;
update tc_s set id=31 where id=30;
update tc_s set id=32 where id=31;
update tc_s set id=33 where id=32;
update tc_s set id=34 where id=33;
update tc_s set id=35 where id=34;
update tc_s set id=36 where id=35;
update tc_s set id=37 where id=36;
update tc_s set id=38 where id=37;
update tc_s set id=39 where id=38;
update tc_s set id=40 where id=39;
update tc_s set id=41 where id=40;
update tc_s set id=42 where id=41;
update tc_s set id=2 where id=42;
commit;
-- @regex("could not be resolved completely", true)
data branch pick tc_s into tc_p keys(1) when conflict accept;
select id, v from tc_p order by id;
select id, pid from tc_c order by id;


-- xr1 (issue #28026 r53): a pk move at T1 followed by a payload edit at
-- T2 stays pickable by its landed key with the edit intact.
create table xr1_p (id int primary key, v varchar(16));
create table xr1_c (id int primary key, pid int,
    constraint fk_xr1 foreign key (pid) references xr1_p(id) on update cascade);
insert into xr1_p values (1,'orig');
insert into xr1_c values (900,1);
data branch create table xr1_s from xr1_p;
update xr1_s set id=3 where id=1;
update xr1_s set v='edited' where id=3;
data branch pick xr1_s into xr1_p keys(3) when conflict accept;
select id, v from xr1_p order by id;
select id, pid from xr1_c order by id;

-- xr2 (issue #28026 r53): an explicit delete+insert of the moved key in
-- one transaction at T2 leaves the landed row pickable by its key.
create table xr2_p (id int primary key, v varchar(16));
create table xr2_c (id int primary key, pid int,
    constraint fk_xr2 foreign key (pid) references xr2_p(id) on update cascade);
insert into xr2_p values (1,'orig');
insert into xr2_c values (900,1);
data branch create table xr2_s from xr2_p;
update xr2_s set id=3 where id=1;
begin;
delete from xr2_s where id=3;
insert into xr2_s values (3,'reborn');
commit;
data branch pick xr2_s into xr2_p keys(3) when conflict accept;
select id, v from xr2_p order by id;
select id, pid from xr2_c order by id;

drop database branch_pick_fk_pk_move;

-- Restore the default isolation this file replaced above: mo-tester
-- reuses the default connection across files, and an inherited
-- repeatable-read session makes later lifecycle statements fail the
-- pessimistic-RC admission gates.
set session transaction isolation level read committed;

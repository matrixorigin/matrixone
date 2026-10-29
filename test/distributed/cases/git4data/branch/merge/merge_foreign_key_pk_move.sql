-- Branch-side primary-key moves must replay as one native UPDATE so declared
-- foreign-key ON UPDATE actions fire; an independent delete+insert pair at
-- the same keys records no update intent and keeps delete+insert replay
-- (issue #28026).
drop database if exists branch_merge_fk_pk_move;
create database branch_merge_fk_pk_move;
use branch_merge_fk_pk_move;

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
insert into parent_t values (2, 'two');
insert into child_t values (1, 2);

-- The issue case: merging a source pk move cascades to the child row.
data branch create table move_src from parent_t;
update move_src set id = 3 where id = 2;
data branch merge move_src into parent_t when conflict accept;
select id, payload from parent_t order by id;
select id, parent_id from child_t order by id;

-- Consecutive moves of one row still replay as one native update.
data branch create table chain_src from parent_t;
update chain_src set id = 4 where id = 3;
update chain_src set id = 5 where id = 4;
data branch merge chain_src into parent_t when conflict accept;
select id, payload from parent_t order by id;
select id, parent_id from child_t order by id;

-- Payload edits riding along with the move reach the destination too.
data branch create table payload_src from parent_t;
update payload_src set id = 6, payload = 'six' where id = 5;
data branch merge payload_src into parent_t when conflict accept;
select id, payload from parent_t order by id;
select id, parent_id from child_t order by id;

-- Counterexample: an independent delete+insert at neighboring keys is not an
-- update, keeps delete+insert replay, and fails the foreign-key check
-- atomically.
data branch create table delins_src from parent_t;
delete from delins_src where id = 6;
insert into delins_src values (7, 'seven');
-- @regex("Cannot delete or update a parent row: a foreign key constraint fails", true)
data branch merge delins_src into parent_t when conflict accept;
select id, payload from parent_t order by id;
select id, parent_id from child_t order by id;

-- A foreign key without ON UPDATE action rejects the native update; the
-- merge aborts atomically and both tables keep their previous rows.
create table rparent_t (id int primary key, payload varchar(32));
create table rchild_t (
    id int primary key,
    parent_id int,
    constraint fk_rchild_rparent foreign key (parent_id) references rparent_t(id)
);
insert into rparent_t values (2, 'two');
insert into rchild_t values (1, 2);
data branch create table rmove_src from rparent_t;
update rmove_src set id = 3 where id = 2;
-- @regex("Cannot delete or update a parent row: a foreign key constraint fails", true)
data branch merge rmove_src into rparent_t when conflict accept;
select id, payload from rparent_t order by id;
select id, parent_id from rchild_t order by id;

-- Composite primary keys move just the same and cascade.
create table cparent_t (k1 int, k2 int, payload varchar(32), primary key (k1, k2));
create table cchild_t (
    id int primary key,
    fk1 int,
    fk2 int,
    constraint fk_cchild_cparent foreign key (fk1, fk2) references cparent_t(k1, k2) on update cascade
);
insert into cparent_t values (1, 1, 'one');
insert into cchild_t values (1, 1, 1);
data branch create table cmove_src from cparent_t;
update cmove_src set k2 = 2 where k1 = 1 and k2 = 1;
data branch merge cmove_src into cparent_t when conflict accept;
select k1, k2, payload from cparent_t order by k1, k2;
select id, fk1, fk2 from cchild_t order by id;

-- A chain of moves (2->3->4) plus an independent insert at the vacated key 3:
-- the moved row lands at its final key with its payload edit, and the fresh
-- occupant at key 3 survives untouched.
create table chain_p (id int primary key, v varchar(32));
create table chain_c (
    id int primary key,
    pid int,
    constraint fk_chain foreign key (pid) references chain_p(id) on update cascade
);
insert into chain_p values (2, 'two');
insert into chain_c values (1, 2);
data branch create table chainmove_src from chain_p;
update chainmove_src set id = 3 where id = 2;
update chainmove_src set id = 4 where id = 3;
insert into chainmove_src values (3, 'three-fresh');
update chainmove_src set v = 'TWO' where id = 4;
data branch merge chainmove_src into chain_p when conflict accept;
select id, v from chain_p order by id;
select id, pid from chain_c order by id;

-- Counterexample: a delete and an insert committed as one transaction are two
-- independent operations, never an update; replay keeps delete semantics and
-- fails the foreign-key check atomically.
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
insert into tx_src values (3, 'three');
commit;
-- @regex("Cannot delete or update a parent row: a foreign key constraint fails", true)
data branch merge tx_src into tx_p when conflict accept;
select id, v from tx_p order by id;
select id, pid from tx_c order by id;

-- Both branches moved the same row to different keys: ACCEPT applies the
-- source version at the destination's current key, and the child re-parents
-- to the source key.
create table acc_p (id int primary key, v varchar(32));
create table acc_c (
    id int primary key,
    pid int,
    constraint fk_acc foreign key (pid) references acc_p(id) on update cascade
);
insert into acc_p values (2, 'two');
insert into acc_c values (1, 2);
data branch create table acc_src from acc_p;
update acc_src set id = 3, v = 'three' where id = 2;
update acc_p set id = 4, v = 'four' where id = 2;
data branch merge acc_src into acc_p when conflict accept;
select id, v from acc_p order by id;
select id, pid from acc_c order by id;

-- SKIP keeps the destination move.
create table skip_p (id int primary key, v varchar(32));
create table skip_c (
    id int primary key,
    pid int,
    constraint fk_skip foreign key (pid) references skip_p(id) on update cascade
);
insert into skip_p values (2, 'two');
insert into skip_c values (1, 2);
data branch create table skip_src from skip_p;
update skip_src set id = 3, v = 'three' where id = 2;
update skip_p set id = 4, v = 'four' where id = 2;
data branch merge skip_src into skip_p when conflict skip;
select id, v from skip_p order by id;
select id, pid from skip_c order by id;

-- FAIL aborts atomically.
create table failmove_p (id int primary key, v varchar(32));
insert into failmove_p values (2, 'two');
data branch create table failmove_src from failmove_p;
update failmove_src set id = 3, v = 'three' where id = 2;
update failmove_p set id = 4, v = 'four' where id = 2;
-- @regex("conflict: failmove_src UPDATE and failmove_p UPDATE on pk", true)
data branch merge failmove_src into failmove_p;
select id, v from failmove_p order by id;

-- Destination deleted the row, source moved it: the move registers as the
-- update side of a delete/update conflict, mirroring stable-key semantics.
-- ACCEPT resurrects the source version at its new key.
create table del_p (id int primary key, v varchar(32));
insert into del_p values (2, 'two'), (9, 'nine');
data branch create table del_src from del_p;
update del_src set id = 3, v = 'three' where id = 2;
delete from del_p where id = 2;
data branch merge del_src into del_p when conflict accept;
select id, v from del_p order by id;

-- SKIP keeps the destination delete.
create table delskip_p (id int primary key, v varchar(32));
insert into delskip_p values (2, 'two');
data branch create table delskip_src from delskip_p;
update delskip_src set id = 3, v = 'three' where id = 2;
delete from delskip_p where id = 2;
data branch merge delskip_src into delskip_p when conflict skip;
select id, v from delskip_p order by id;

-- FAIL aborts atomically.
create table delfail_p (id int primary key, v varchar(32));
insert into delfail_p values (2, 'two');
data branch create table delfail_src from delfail_p;
update delfail_src set id = 3, v = 'three' where id = 2;
delete from delfail_p where id = 2;
-- @regex("conflict: delfail_src UPDATE and delfail_p DELETE on pk", true)
data branch merge delfail_src into delfail_p;
select id, v from delfail_p order by id;

-- Dropping or truncating a table reclaims its pk-move provenance links in
-- mo_catalog.mo_data_branch_update_link.
create table reclaim_tb (id int primary key, v int);
insert into reclaim_tb values (1, 1);
data branch create table reclaim_br from reclaim_tb;
update reclaim_br set id = 2 where id = 1;
set @tid = (select rel_id from mo_catalog.mo_tables where relname = 'reclaim_br' and reldatabase = 'branch_merge_fk_pk_move');
select count(*) from mo_catalog.mo_data_branch_update_link where table_id = @tid;
drop table reclaim_br;
select count(*) from mo_catalog.mo_data_branch_update_link where table_id = @tid;
data branch create table reclaim_br from reclaim_tb;
update reclaim_br set id = 7 where id = 1;
set @tid = (select rel_id from mo_catalog.mo_tables where relname = 'reclaim_br' and reldatabase = 'branch_merge_fk_pk_move');
select count(*) from mo_catalog.mo_data_branch_update_link where table_id = @tid;
truncate table reclaim_br;
select count(*) from mo_catalog.mo_data_branch_update_link where table_id = @tid;

-- A destination row independently occupying the move's NEW key is a
-- conflict. FAIL aborts atomically and both tables keep their rows.
create table nkfail_p (id int primary key, v varchar(32));
create table nkfail_c (
    id int primary key,
    pid int,
    constraint fk_nkfail foreign key (pid) references nkfail_p(id) on update cascade
);
insert into nkfail_p values (2, 'two');
insert into nkfail_c values (1, 2);
data branch create table nkfail_src from nkfail_p;
insert into nkfail_p values (3, 'dst-three');
insert into nkfail_c values (2, 3);
update nkfail_src set id = 3, v = 'three-src' where id = 2;
-- @regex("conflict: nkfail_src", true)
data branch merge nkfail_src into nkfail_p;
select id, v from nkfail_p order by id;
select id, pid from nkfail_c order by id;

-- SKIP keeps the destination occupant and drops the move as a whole: the
-- destination row at the move's old key survives untouched.
create table nkskip_p (id int primary key, v varchar(32));
create table nkskip_c (
    id int primary key,
    pid int,
    constraint fk_nkskip foreign key (pid) references nkskip_p(id) on update cascade
);
insert into nkskip_p values (2, 'two');
insert into nkskip_c values (1, 2);
data branch create table nkskip_src from nkskip_p;
insert into nkskip_p values (3, 'dst-three');
insert into nkskip_c values (2, 3);
update nkskip_src set id = 3, v = 'three-src' where id = 2;
data branch merge nkskip_src into nkskip_p when conflict skip;
select id, v from nkskip_p order by id;
select id, pid from nkskip_c order by id;

-- ACCEPT replaces the destination occupant inline: its ON DELETE CASCADE
-- child leaves with it, the source move lands at the contested key, and the
-- moved row's child re-parents there.
create table nkacc_p (id int primary key, v varchar(32));
create table nkacc_c (
    id int primary key,
    pid int,
    constraint fk_nkacc foreign key (pid) references nkacc_p(id)
        on update cascade on delete cascade
);
insert into nkacc_p values (2, 'two');
insert into nkacc_c values (1, 2);
data branch create table nkacc_src from nkacc_p;
insert into nkacc_p values (3, 'dst-three');
insert into nkacc_c values (2, 3);
update nkacc_src set id = 3, v = 'three-src' where id = 2;
data branch merge nkacc_src into nkacc_p when conflict accept;
select id, v from nkacc_p order by id;
select id, pid from nkacc_c order by id;

-- ACCEPT with a RESTRICT child on the destination occupant rejects the
-- inline victim delete; the merge aborts atomically and keeps both tables.
create table nkres_p (id int primary key, v varchar(32));
create table nkres_c (
    id int primary key,
    pid int,
    constraint fk_nkres foreign key (pid) references nkres_p(id)
);
insert into nkres_p values (2, 'two');
insert into nkres_c values (1, 2);
data branch create table nkres_src from nkres_p;
insert into nkres_p values (3, 'dst-three');
insert into nkres_c values (2, 3);
update nkres_src set id = 3, v = 'three-src' where id = 2;
-- @regex("Cannot delete or update a parent row: a foreign key constraint fails", true)
data branch merge nkres_src into nkres_p when conflict accept;
select id, v from nkres_p order by id;
select id, pid from nkres_c order by id;

-- A multi-generation branch folds moves recorded on every physical table on
-- the branch path: 2->3 on the first generation, 3->4 on the leaf, and the
-- merge into the root replays one native update with the cascade.
create table mg_p (id int primary key, v varchar(32));
create table mg_c (
    id int primary key,
    pid int,
    constraint fk_mg foreign key (pid) references mg_p(id) on update cascade
);
insert into mg_p values (2, 'two');
insert into mg_c values (1, 2);
data branch create table mg1 from mg_p;
update mg1 set id = 3 where id = 2;
data branch create table mg2 from mg1;
update mg2 set id = 4 where id = 3;
data branch merge mg2 into mg_p when conflict accept;
select id, v from mg_p order by id;
select id, pid from mg_c order by id;

-- A retired intermediate generation keeps its links while a live descendant
-- still diffs against its stream: dropping mg1 above must not strip the
-- provenance the fold through rt1 needs, and dropping the last live
-- descendant reclaims the whole retired chain.
create table rt_p (id int primary key, v varchar(32));
create table rt_c (
    id int primary key,
    pid int,
    constraint fk_rt foreign key (pid) references rt_p(id) on update cascade
);
insert into rt_p values (2, 'two');
insert into rt_c values (1, 2);
data branch create table rt1 from rt_p;
update rt1 set id = 3 where id = 2;
data branch create table rt2 from rt1;
update rt2 set id = 4 where id = 3;
set @rt1 = (select rel_id from mo_catalog.mo_tables where relname = 'rt1' and reldatabase = 'branch_merge_fk_pk_move');
set @rt2 = (select rel_id from mo_catalog.mo_tables where relname = 'rt2' and reldatabase = 'branch_merge_fk_pk_move');
drop table rt1;
select count(*) from mo_catalog.mo_data_branch_update_link where table_id = @rt1;
data branch merge rt2 into rt_p when conflict accept;
select id, v from rt_p order by id;
select id, pid from rt_c order by id;
drop table rt2;
select count(*) from mo_catalog.mo_data_branch_update_link where table_id = @rt1;
select count(*) from mo_catalog.mo_data_branch_update_link where table_id = @rt2;

-- A move recorded before the branch fork never folds an in-window delete of
-- a different row that later reused the key: the replay stays a plain
-- delete and the historical occupant with its child is untouched.
create table hw_p (id int primary key, v varchar(32));
create table hw_c (
    id int primary key,
    pid int,
    constraint fk_hw foreign key (pid) references hw_p(id) on update cascade
);
insert into hw_p values (2, 'two');
update hw_p set id = 8 where id = 2;
insert into hw_c values (1, 8);
insert into hw_p values (2, 'two-ii');
data branch create table hw_src from hw_p;
delete from hw_src where id = 2;
data branch merge hw_src into hw_p when conflict accept;
select id, v from hw_p order by id;
select id, pid from hw_c order by id;

-- Link rows commit with their moving UPDATE: a rolled-back move leaves no
-- provenance behind, while a committed one records exactly one row.
create table rb_p (id int primary key, v int);
insert into rb_p values (1, 1);
data branch create table rb_br from rb_p;
set @rb = (select rel_id from mo_catalog.mo_tables where relname = 'rb_br' and reldatabase = 'branch_merge_fk_pk_move');
begin;
update rb_br set id = 2 where id = 1;
rollback;
select count(*) from mo_catalog.mo_data_branch_update_link where table_id = @rb;
update rb_br set id = 2 where id = 1;
select count(*) from mo_catalog.mo_data_branch_update_link where table_id = @rb;

-- A replacement deleted and reincarnated as an unrelated row inside the
-- moving transaction is ambiguous, not an update: provenance prunes the
-- recorded intent, the delta replays as delete+insert, and the foreign key
-- rejects the parent delete atomically. Both tables keep their rows.
create table am_p (id int primary key, v varchar(32));
create table am_c (
    id int primary key,
    pid int,
    constraint fk_am foreign key (pid) references am_p(id) on update cascade
);
insert into am_p values (2, 'two');
insert into am_c values (1, 2);
data branch create table am_src from am_p;
begin;
update am_src set id = 3 where id = 2;
delete from am_src where id = 3;
insert into am_src values (3, 'unrelated');
commit;
set @am = (select rel_id from mo_catalog.mo_tables where relname = 'am_src' and reldatabase = 'branch_merge_fk_pk_move');
select count(*) from mo_catalog.mo_data_branch_update_link where table_id = @am;
-- @regex("Cannot delete or update a parent row: a foreign key constraint fails", true)
data branch merge am_src into am_p when conflict accept;
select id, v from am_p order by id;
select id, pid from am_c order by id;

-- Control: a same-transaction chain with live continuations keeps its
-- intermediate links and folds with the cascade.
create table ct_p (id int primary key, v varchar(32));
create table ct_c (
    id int primary key,
    pid int,
    constraint fk_ct foreign key (pid) references ct_p(id) on update cascade
);
insert into ct_p values (2, 'two');
insert into ct_c values (1, 2);
data branch create table ct_src from ct_p;
begin;
update ct_src set id = 3 where id = 2;
update ct_src set id = 4 where id = 3;
commit;
set @ct = (select rel_id from mo_catalog.mo_tables where relname = 'ct_src' and reldatabase = 'branch_merge_fk_pk_move');
select count(*) from mo_catalog.mo_data_branch_update_link where table_id = @ct;
data branch merge ct_src into ct_p when conflict accept;
select id, v from ct_p order by id;
select id, pid from ct_c order by id;

-- Separately committed DELETE 2; INSERT (2, 'unrelated'); UPDATE 2->3 keeps
-- delete semantics: the anchor does not descend from the LCA row and a fold
-- would cascade destination children onto an unrelated lineage, so the
-- replay fails the foreign key atomically instead.
create table ri_p (id int primary key, v varchar(32));
create table ri_c (
    id int primary key,
    pid int,
    constraint fk_ri foreign key (pid) references ri_p(id) on update cascade
);
insert into ri_p values (2, 'two');
insert into ri_c values (1, 2);
data branch create table ri_src from ri_p;
delete from ri_src where id = 2;
insert into ri_src values (2, 'unrelated');
update ri_src set id = 3 where id = 2;
-- @regex("Cannot delete or update a parent row: a foreign key constraint fails", true)
data branch merge ri_src into ri_p when conflict accept;
select id, v from ri_p order by id;
select id, pid from ri_c order by id;

-- Interacting moves apply in dependency order: moving 3->4 first vacates 3
-- for the 2->3 move, so both destination rows and both children survive.
create table im_p (id int primary key, v varchar(32));
create table im_c (
    id int primary key,
    pid int,
    constraint fk_im foreign key (pid) references im_p(id) on update cascade
);
insert into im_p values (2, 'two'), (3, 'three');
insert into im_c values (1, 2), (2, 3);
data branch create table im_src from im_p;
update im_src set id = 4 where id = 3;
update im_src set id = 3 where id = 2;
data branch merge im_src into im_p when conflict accept;
select id, v from im_p order by id;
select id, pid from im_c order by id;

-- Merging the same branch twice is a clean no-op: the first apply's
-- authorized victim deletion is not mistaken for lineage ambiguity.
create table mt_p (id int primary key, v varchar(32));
create table mt_c (
    id int primary key,
    pid int,
    constraint fk_mt foreign key (pid) references mt_p(id) on update cascade
);
insert into mt_p values (2, 'two'), (3, 'three');
insert into mt_c values (1, 2);
data branch create table mt_src from mt_p;
delete from mt_src where id = 3;
update mt_src set id = 3 where id = 2;
data branch merge mt_src into mt_p when conflict accept;
select id, v from mt_p order by id;
select id, pid from mt_c order by id;
data branch merge mt_src into mt_p when conflict accept;
select id, v from mt_p order by id;
select id, pid from mt_c order by id;

-- A move flushed to storage before the merge folds the same: the fold reads
-- provenance, not workspace residue.
create table fl_p (id int primary key, v varchar(32));
create table fl_c (
    id int primary key,
    pid int,
    constraint fk_fl foreign key (pid) references fl_p(id) on update cascade
);
insert into fl_p values (2, 'two');
insert into fl_c values (1, 2);
data branch create table fl_src from fl_p;
update fl_src set id = 3 where id = 2;
select mo_ctl('dn','flush','branch_merge_fk_pk_move.fl_src');
data branch merge fl_src into fl_p when conflict accept;
select id, v from fl_p order by id;
select id, pid from fl_c order by id;

-- Era-4: repeated same-table move chains in two committed transactions must
-- both commit (their transaction-local row identities never collide) and the
-- cross-transaction lineage still folds: 2->3->4 in txn one, 4->3->5 in txn
-- two nets to 2->...->5 through the stream.
create table xn_p (id int primary key, v varchar(32));
create table xn_c (
    id int primary key,
    pid int,
    constraint fk_xn foreign key (pid) references xn_p(id) on update cascade
);
insert into xn_p values (2, 'two');
insert into xn_c values (1, 2);
data branch create table xn_src from xn_p;
begin;
update xn_src set id = 3 where id = 2;
update xn_src set id = 4 where id = 3;
commit;
begin;
update xn_src set id = 3 where id = 4;
update xn_src set id = 5 where id = 3;
commit;
set @xnl = (select rel_id from mo_catalog.mo_tables where relname = 'xn_src' and reldatabase = 'branch_merge_fk_pk_move');
select count(*) from mo_catalog.mo_data_branch_update_link where table_id = @xnl;
data branch merge xn_src into xn_p when conflict accept;
select id, v from xn_p order by id;
select id, pid from xn_c order by id;

-- Era-4: a non-key edit BEFORE a move in one transaction keeps the lineage
-- anchored on the departing durable row; the edited payload travels.
create table eb_p (id int primary key, v varchar(32));
create table eb_c (
    id int primary key,
    pid int,
    constraint fk_eb foreign key (pid) references eb_p(id) on update cascade
);
insert into eb_p values (2, 'two');
insert into eb_c values (1, 2);
data branch create table eb_src from eb_p;
begin;
update eb_src set v = 'pre' where id = 2;
update eb_src set id = 3 where id = 2;
commit;
data branch merge eb_src into eb_p when conflict accept;
select id, v from eb_p order by id;
select id, pid from eb_c order by id;

-- Era-4: a non-key edit AFTER a move in one transaction replaces the recorded
-- replacement row; the continuity marker retargets the link and is dropped,
-- so exactly one link commits and the fold applies the edited row natively.
create table ee_p (id int primary key, v varchar(32));
create table ee_c (
    id int primary key,
    pid int,
    constraint fk_ee foreign key (pid) references ee_p(id) on update cascade
);
insert into ee_p values (2, 'two');
insert into ee_c values (1, 2);
data branch create table ee_src from ee_p;
begin;
update ee_src set id = 3 where id = 2;
update ee_src set v = 'post' where id = 3;
commit;
set @eel = (select rel_id from mo_catalog.mo_tables where relname = 'ee_src' and reldatabase = 'branch_merge_fk_pk_move');
select count(*) from mo_catalog.mo_data_branch_update_link where table_id = @eel;
data branch merge ee_src into ee_p when conflict accept;
select id, v from ee_p order by id;
select id, pid from ee_c order by id;

-- Era-4: a non-key edit BETWEEN two moves of one row in one transaction keeps
-- both moves recorded and carries the intermediate payload to the final key.
create table em_p (id int primary key, v varchar(32));
create table em_c (
    id int primary key,
    pid int,
    constraint fk_em foreign key (pid) references em_p(id) on update cascade
);
insert into em_p values (2, 'two');
insert into em_c values (1, 2);
data branch create table em_src from em_p;
begin;
update em_src set id = 3 where id = 2;
update em_src set v = 'mid' where id = 3;
update em_src set id = 4 where id = 3;
commit;
set @eml = (select rel_id from mo_catalog.mo_tables where relname = 'em_src' and reldatabase = 'branch_merge_fk_pk_move');
select count(*) from mo_catalog.mo_data_branch_update_link where table_id = @eml;
data branch merge em_src into em_p when conflict accept;
select id, v from em_p order by id;
select id, pid from em_c order by id;

-- Era-4 guard: a move whose landed generation dies inside the same
-- transaction is still no update at all; markers must not rescue it. The
-- reincarnated row keeps plain insert replay and the delete fails the FK
-- atomically.
create table ea_p (id int primary key, v varchar(32));
create table ea_c (
    id int primary key,
    pid int,
    constraint fk_ea foreign key (pid) references ea_p(id) on update cascade
);
insert into ea_p values (2, 'two');
insert into ea_c values (1, 2);
data branch create table ea_src from ea_p;
begin;
update ea_src set id = 3 where id = 2;
delete from ea_src where id = 3;
insert into ea_src values (3, 'impostor');
commit;
set @eal = (select rel_id from mo_catalog.mo_tables where relname = 'ea_src' and reldatabase = 'branch_merge_fk_pk_move');
select count(*) from mo_catalog.mo_data_branch_update_link where table_id = @eal;
-- @regex("Cannot delete or update a parent row: a foreign key constraint fails", true)
data branch merge ea_src into ea_p when conflict accept;
select id, v from ea_p order by id;
select id, pid from ea_c order by id;

-- Era-4: moves recorded first must survive statement-time workspace
-- compaction: this transaction crosses the 30-entry merge threshold AFTER
-- the link was staged, re-keying staged inserts and forcing spills; the
-- provenance remap and spill ledger keep the fold provable.
create table ec_p (id int primary key, v varchar(32));
create table ec_c (
    id int primary key,
    pid int,
    constraint fk_ec foreign key (pid) references ec_p(id) on update cascade
);
create table ec_fill (id int primary key);
insert into ec_p values (2, 'two');
insert into ec_c values (1, 2);
data branch create table ec_src from ec_p;
begin;
update ec_src set id = 3 where id = 2;
insert into ec_fill values (1);
insert into ec_fill values (2);
insert into ec_fill values (3);
insert into ec_fill values (4);
insert into ec_fill values (5);
insert into ec_fill values (6);
insert into ec_fill values (7);
insert into ec_fill values (8);
insert into ec_fill values (9);
insert into ec_fill values (10);
insert into ec_fill values (11);
insert into ec_fill values (12);
insert into ec_fill values (13);
insert into ec_fill values (14);
insert into ec_fill values (15);
insert into ec_fill values (16);
insert into ec_fill values (17);
insert into ec_fill values (18);
insert into ec_fill values (19);
insert into ec_fill values (20);
insert into ec_fill values (21);
insert into ec_fill values (22);
insert into ec_fill values (23);
insert into ec_fill values (24);
insert into ec_fill values (25);
insert into ec_fill values (26);
insert into ec_fill values (27);
insert into ec_fill values (28);
insert into ec_src values (99, 'tail');
commit;
set @ecl = (select rel_id from mo_catalog.mo_tables where relname = 'ec_src' and reldatabase = 'branch_merge_fk_pk_move');
select count(*) from mo_catalog.mo_data_branch_update_link where table_id = @ecl;
data branch merge ec_src into ec_p when conflict accept;
select id, v from ec_p order by id;
select id, pid from ec_c order by id;

-- Era-4: both branches moved the same rows; under ACCEPT the retargeted
-- moves must apply in vacate order computed from their EFFECTIVE destination
-- predicates, or one UPDATE rewrites the row the other just installed.
create table er_p (id int primary key, v varchar(16));
create table er_c (
    id int primary key,
    pid int,
    constraint fk_er foreign key (pid) references er_p(id) on update cascade
);
insert into er_p values (1, 'A'), (2, 'B'), (3, 'C');
insert into er_c values (11, 1), (12, 2);
data branch create table er_src from er_p;
delete from er_p where id = 3;
update er_p set id = 5 where id = 1;
update er_p set id = 3 where id = 2;
delete from er_src where id = 3;
update er_src set id = 3, v = 'A2' where id = 1;
update er_src set id = 4, v = 'B2' where id = 2;
data branch merge er_src into er_p when conflict accept;
select id, v from er_p order by id;
select id, pid from er_c order by id;

-- Era-4: the S3 producer route exercised end-to-end with bounded data. The
-- fault point drives small updates through UpdateWriteS3; wide payloads keep
-- the committed images realistic. 48 rows x ~9KB stays under the injected
-- 512KB flush threshold: links are recorded when the commit flushes.
create table es_p (id int primary key, v varchar(9024));
create table es_c (
    id int primary key,
    pid int,
    constraint fk_es foreign key (pid) references es_p(id) on update cascade
);
insert into es_p select result, repeat('a', 9000) from generate_series(1, 48) g;
insert into es_c values (1, 1), (48, 48);
data branch create table es_src from es_p;
select enable_fault_injection();
select add_fault_point('fj/cn/flush_small_objs',':::','echo',40,'branch_merge_fk_pk_move.es_src');
update es_src set id = id + 1000;
select remove_fault_point('fj/cn/flush_small_objs');
select disable_fault_injection();
data branch merge es_src into es_p when conflict accept;
select min(id), max(id), count(*) from es_p;
select count(*) from es_c where pid between 1001 and 1048;
set @esl = (select rel_id from mo_catalog.mo_tables where relname = 'es_src' and reldatabase = 'branch_merge_fk_pk_move');
select count(*) from mo_catalog.mo_data_branch_update_link where table_id = @esl;

-- Era-4: 64 rows x ~9KB crosses the injected 512KB flush threshold, so the S3
-- producer flushes mid-statement and again at commit; every move still
-- records one link and the whole apply cascades to completion.
create table esf_p (id int primary key, v varchar(9024));
create table esf_c (
    id int primary key,
    pid int,
    constraint fk_esf foreign key (pid) references esf_p(id) on update cascade
);
insert into esf_p select result, repeat('b', 9000) from generate_series(1, 64) g;
insert into esf_c values (1, 1), (64, 64);
data branch create table esf_src from esf_p;
select enable_fault_injection();
select add_fault_point('fj/cn/flush_small_objs',':::','echo',40,'branch_merge_fk_pk_move.esf_src');
update esf_src set id = id + 1000;
select remove_fault_point('fj/cn/flush_small_objs');
select disable_fault_injection();
data branch merge esf_src into esf_p when conflict accept;
select min(id), max(id), count(*) from esf_p;
select count(*) from esf_c where pid between 1001 and 1064;
set @esfl = (select rel_id from mo_catalog.mo_tables where relname = 'esf_src' and reldatabase = 'branch_merge_fk_pk_move');
select count(*) from mo_catalog.mo_data_branch_update_link where table_id = @esfl;

-- Era-4: the same S3 route fails atomically under a restrictive foreign key:
-- links are recorded by the producer, the apply is rejected, and the
-- destination keeps its exact pre-merge rows.
create table esr_p (id int primary key, v varchar(9024));
create table esr_c (
    id int primary key,
    pid int,
    constraint fk_esr foreign key (pid) references esr_p(id) on update restrict
);
insert into esr_p select result, repeat('c', 9000) from generate_series(1, 8) g;
insert into esr_c values (1, 1), (8, 8);
data branch create table esr_src from esr_p;
select enable_fault_injection();
select add_fault_point('fj/cn/flush_small_objs',':::','echo',40,'branch_merge_fk_pk_move.esr_src');
update esr_src set id = id + 100;
set @esrl = (select rel_id from mo_catalog.mo_tables where relname = 'esr_src' and reldatabase = 'branch_merge_fk_pk_move');
select count(*) from mo_catalog.mo_data_branch_update_link where table_id = @esrl;
select remove_fault_point('fj/cn/flush_small_objs');
select disable_fault_injection();
-- @regex("Cannot delete or update a parent row: a foreign key constraint fails", true)
data branch merge esr_src into esr_p when conflict accept;
select id, length(v) from esr_p order by id;
select id, pid from esr_c order by id;

-- Era-5 (review F1): an S3-route move whose replacement is deleted and
-- reinserted as an unrelated row in the same transaction. The diverted
-- death is now recorded at the statement that causes it; the commit
-- prune retires the link instead of letting the committed impostor claim
-- the fold. The merge falls back to legacy replay, the FK rejects the
-- old-parent delete, and the destination keeps its exact rows.
create table esd_p (id int primary key, v varchar(9024));
create table esd_c (
    id int primary key,
    pid int,
    constraint fk_esd foreign key (pid) references esd_p(id) on update cascade
);
insert into esd_p select result, repeat('d', 9000) from generate_series(1, 8) g;
insert into esd_c values (1, 1), (8, 8), (77, 2);
data branch create table esd_src from esd_p;
select enable_fault_injection();
select add_fault_point('fj/cn/flush_small_objs',':::','echo',40,'branch_merge_fk_pk_move.esd_src');
begin;
update esd_src set id = 1000, v = repeat('m', 9000) where id = 2;
delete from esd_src where id = 1000;
insert into esd_src values (1000, repeat('z', 9000));
commit;
set @esdl = (select rel_id from mo_catalog.mo_tables where relname = 'esd_src' and reldatabase = 'branch_merge_fk_pk_move');
select count(*) from mo_catalog.mo_data_branch_update_link where table_id = @esdl;
select remove_fault_point('fj/cn/flush_small_objs');
select disable_fault_injection();
-- @regex("Cannot delete or update a parent row: a foreign key constraint fails", true)
data branch merge esd_src into esd_p when conflict accept;
select count(*) from esd_p;
select id, length(v) from esd_p where id = 2;
select id, pid from esd_c order by id;

-- Era-5: an S3-route move followed by an UNARMED same-key payload edit of
-- the moved key in one transaction keeps its link. The edit's delete of
-- the uncommitted S3 row is the continuity marker's own consumption, and
-- the marker binds the NULL-identity link to the staged replacement, so
-- the apply still cascades with the edit's payload.
create table esh_p (id int primary key, v varchar(9024));
create table esh_c (
    id int primary key,
    pid int,
    constraint fk_esh foreign key (pid) references esh_p(id) on update cascade
);
insert into esh_p select result, repeat('h', 9000) from generate_series(1, 8) g;
insert into esh_c values (77, 2);
data branch create table esh_src from esh_p;
select enable_fault_injection();
select add_fault_point('fj/cn/flush_small_objs',':::','echo',40,'branch_merge_fk_pk_move.esh_src');
begin;
update esh_src set id = 1000, v = repeat('m', 9000) where id = 2;
select remove_fault_point('fj/cn/flush_small_objs');
select disable_fault_injection();
update esh_src set v = repeat('n', 9000) where id = 1000;
commit;
set @eshl = (select rel_id from mo_catalog.mo_tables where relname = 'esh_src' and reldatabase = 'branch_merge_fk_pk_move');
select count(*) from mo_catalog.mo_data_branch_update_link where table_id = @eshl;
data branch merge esh_src into esh_p when conflict accept;
select id, substr(v, 1, 1) from esh_p where id = 1000;
select id, pid from esh_c order by id;

-- Era-5: an S3-route move followed by an ARMED same-key edit in one
-- transaction keeps the link through the same-statement marker rescue
-- even though the edit stages no replacement row entry; the apply
-- cascades with the edit's committed payload.
create table esa_p (id int primary key, v varchar(9024));
create table esa_c (
    id int primary key,
    pid int,
    constraint fk_esa foreign key (pid) references esa_p(id) on update cascade
);
insert into esa_p select result, repeat('a', 9000) from generate_series(1, 8) g;
insert into esa_c values (77, 2);
data branch create table esa_src from esa_p;
select enable_fault_injection();
select add_fault_point('fj/cn/flush_small_objs',':::','echo',40,'branch_merge_fk_pk_move.esa_src');
begin;
update esa_src set id = 1000, v = repeat('m', 9000) where id = 2;
update esa_src set v = repeat('n', 9000) where id = 1000;
commit;
select remove_fault_point('fj/cn/flush_small_objs');
select disable_fault_injection();
set @esal = (select rel_id from mo_catalog.mo_tables where relname = 'esa_src' and reldatabase = 'branch_merge_fk_pk_move');
select count(*) from mo_catalog.mo_data_branch_update_link where table_id = @esal;
data branch merge esa_src into esa_p when conflict accept;
select id, substr(v, 1, 1) from esa_p where id = 1000;
select id, pid from esa_c order by id;

-- Era-6 (review F1): an S3-route chain inside one transaction keeps BOTH
-- links (the continuer rescue), and the fold now crosses the netted
-- intermediate through the recorded same-transaction departure: the
-- commit-time prune already grouped that departure with the
-- NULL-identity link (its rescue groups departures of the same
-- transaction by key), so the consumer mirrors that decision instead of
-- falling back to the bare commit-timestamp tie. The apply follows
-- 2 -> 1000 -> 1001 as one native move and cascades.
create table esl_p (id int primary key, v varchar(9024));
create table esl_c (
    id int primary key,
    pid int,
    constraint fk_esl foreign key (pid) references esl_p(id) on update cascade
);
insert into esl_p select result, repeat('l', 9000) from generate_series(1, 8) g;
insert into esl_c values (77, 2);
data branch create table esl_src from esl_p;
select enable_fault_injection();
select add_fault_point('fj/cn/flush_small_objs',':::','echo',40,'branch_merge_fk_pk_move.esl_src');
begin;
update esl_src set id = 1000, v = repeat('m', 9000) where id = 2;
update esl_src set id = 1001, v = repeat('n', 9000) where id = 1000;
commit;
select remove_fault_point('fj/cn/flush_small_objs');
select disable_fault_injection();
set @esll = (select rel_id from mo_catalog.mo_tables where relname = 'esl_src' and reldatabase = 'branch_merge_fk_pk_move');
select count(*) from mo_catalog.mo_data_branch_update_link where table_id = @esll;
data branch merge esl_src into esl_p when conflict accept;
select count(*) from esl_p;
select id, length(v) from esl_p where id = 2;
select id, length(v) from esl_p where id = 1001;
select id, pid from esl_c order by id;

-- Era-6 (review F1): the same S3-route chain plus an unrelated reinsert
-- at the vacated intermediate key. The fold departs the intermediate by
-- the recorded same-transaction departure, never by the timestamp tie,
-- so the impostor keeps its own identity: it lands as a plain insert
-- while the lineage ends at 1001 and the child re-parents there.
create table emc_p (id int primary key, v varchar(9024));
create table emc_c (
    id int primary key,
    pid int,
    constraint fk_emc foreign key (pid) references emc_p(id) on update cascade
);
insert into emc_p select result, repeat('l', 9000) from generate_series(1, 8) g;
insert into emc_c values (77, 2);
data branch create table emc_src from emc_p;
select enable_fault_injection();
select add_fault_point('fj/cn/flush_small_objs',':::','echo',40,'branch_merge_fk_pk_move.emc_src');
begin;
update emc_src set id = 1000, v = repeat('m', 9000) where id = 2;
update emc_src set id = 1001, v = repeat('n', 9000) where id = 1000;
insert into emc_src values (1000, repeat('z', 9000));
commit;
select remove_fault_point('fj/cn/flush_small_objs');
select disable_fault_injection();
set @emcl = (select rel_id from mo_catalog.mo_tables where relname = 'emc_src' and reldatabase = 'branch_merge_fk_pk_move');
select count(*) from mo_catalog.mo_data_branch_update_link where table_id = @emcl;
data branch merge emc_src into emc_p when conflict accept;
select count(*) from emc_p;
select id, substr(v, 1, 1) from emc_p where id in (2, 1000, 1001) order by id;
select id, pid from emc_c order by id;

-- Era-6 (review F2): the replacement is staged in the workspace, spilled
-- into an uncommitted object by a forced workspace flush, and then
-- deleted: the delete is diverted (object rewrite, no tombstone), so the
-- spilled workspace identity recorded on the link proves staging, not
-- survival. The pk-level death ledger retires the link at commit and the
-- apply fails closed atomically; the impostor stays source-side.
create table esp_p (id int primary key, v varchar(32));
create table esp_c (
    id int primary key,
    pid int,
    constraint fk_esp foreign key (pid) references esp_p(id) on update cascade
);
insert into esp_p values (2, 'two');
insert into esp_c values (1, 2);
create table esp_filler (id int primary key, b int);
insert into esp_filler values (1, 1);
data branch create table esp_src from esp_p;
begin;
update esp_src set id = 3, v = 'two' where id = 2;
select enable_fault_injection();
select add_fault_point('fj/cn/workspace_force_flush',':::','echo',0,'');
update esp_filler set b = 9 where id = 1;
select remove_fault_point('fj/cn/workspace_force_flush');
select disable_fault_injection();
delete from esp_src where id = 3;
insert into esp_src values (3, 'impostor');
commit;
set @espl = (select rel_id from mo_catalog.mo_tables where relname = 'esp_src' and reldatabase = 'branch_merge_fk_pk_move');
select count(*) from mo_catalog.mo_data_branch_update_link where table_id = @espl;
-- @regex("Cannot delete or update a parent row: a foreign key constraint fails", true)
data branch merge esp_src into esp_p when conflict accept;
select id, v from esp_p order by id;
select id, pid from esp_c order by id;

-- Era-5 (review F5): source and destination moved the same rows to the
-- same final keys with differing payloads, so every effective WHERE key
-- equals its NEW key. Those rows apply in place instead of deadlocking
-- the apply ranker into a false cyclic plan.
create table esk_p (id int primary key, v varchar(32));
insert into esk_p values (1, 'a'), (2, 'b');
data branch create table esk_src from esk_p;
update esk_p set id = 3, v = 'A-dst' where id = 1;
update esk_p set id = 4, v = 'B-dst' where id = 2;
update esk_src set id = 3, v = 'A-src' where id = 1;
update esk_src set id = 4, v = 'B-src' where id = 2;
data branch merge esk_src into esk_p when conflict accept;
select id, v from esk_p order by id;

-- Era-5 (review F5): a same-final-key row batched with real cross-row
-- moves applies with the rest of the plan.
create table esx_p (id int primary key, v varchar(32));
insert into esx_p values (1, 'a'), (2, 'b'), (6, 'six');
data branch create table esx_src from esx_p;
update esx_p set id = 3, v = 'A-dst' where id = 1;
update esx_src set id = 3, v = 'A-src' where id = 1;
update esx_src set id = 4, v = 'B-src' where id = 2;
update esx_src set id = 7, v = 'X-src' where id = 6;
data branch merge esx_src into esx_p when conflict accept;
select id, v from esx_p order by id;

-- Era-5 (review F4): edit-first, then the moving table itself crosses
-- the 30-entry workspace compaction threshold, then the pk move. The
-- durable-origin map must ride the compaction re-key: the old anchor
-- stays the durable departing identity (never the 0xFF workspace
-- segment) and the apply cascades.
create table ecm_p (id int primary key, v varchar(32));
create table ecm_c (
    id int primary key,
    pid int,
    constraint fk_ecm foreign key (pid) references ecm_p(id) on update cascade
);
insert into ecm_p values (2, 'two');
insert into ecm_c values (1, 2);
data branch create table ecm_src from ecm_p;
begin;
update ecm_src set v = 'edited' where id = 2;
insert into ecm_src values (101, 'pad');
insert into ecm_src values (102, 'pad');
insert into ecm_src values (103, 'pad');
insert into ecm_src values (104, 'pad');
insert into ecm_src values (105, 'pad');
insert into ecm_src values (106, 'pad');
insert into ecm_src values (107, 'pad');
insert into ecm_src values (108, 'pad');
insert into ecm_src values (109, 'pad');
insert into ecm_src values (110, 'pad');
insert into ecm_src values (111, 'pad');
insert into ecm_src values (112, 'pad');
insert into ecm_src values (113, 'pad');
insert into ecm_src values (114, 'pad');
insert into ecm_src values (115, 'pad');
insert into ecm_src values (116, 'pad');
insert into ecm_src values (117, 'pad');
insert into ecm_src values (118, 'pad');
insert into ecm_src values (119, 'pad');
insert into ecm_src values (120, 'pad');
insert into ecm_src values (121, 'pad');
insert into ecm_src values (122, 'pad');
insert into ecm_src values (123, 'pad');
insert into ecm_src values (124, 'pad');
insert into ecm_src values (125, 'pad');
insert into ecm_src values (126, 'pad');
insert into ecm_src values (127, 'pad');
insert into ecm_src values (128, 'pad');
insert into ecm_src values (129, 'pad');
insert into ecm_src values (130, 'pad');
insert into ecm_src values (131, 'pad');
insert into ecm_src values (132, 'pad');
insert into ecm_src values (133, 'pad');
update ecm_src set id = 3 where id = 2;
commit;
set @ecml = (select rel_id from mo_catalog.mo_tables where relname = 'ecm_src' and reldatabase = 'branch_merge_fk_pk_move');
select count(*) from mo_catalog.mo_data_branch_update_link where table_id = @ecml;
select count(*) from mo_catalog.mo_data_branch_update_link where table_id = @ecml and hex(old_row_id) like '000000000000000000000000000000FF%';
data branch merge ecm_src into ecm_p when conflict accept;
select count(*) from ecm_p;
select id, v from ecm_p where id in (2, 3);
select id, pid from ecm_c order by id;

-- Era-5 (review F2/F3): a statement that fails after the continuity
-- stage must roll back the retarget and the harvest it folded; the
-- retry re-resolves the lineage, and the apply cascades the RETRY's
-- payload instead of dropping the first move's link.
create table erb_p (id int primary key, v varchar(32));
create table erb_c (
    id int primary key,
    pid int,
    constraint fk_erb foreign key (pid) references erb_p(id) on update cascade
);
insert into erb_p values (2, 'two'), (8, 'eight'), (9, 'nine');
insert into erb_c values (1, 2);
data branch create table erb_src from erb_p;
alter table erb_src add unique key uq_erb(v);
begin;
update erb_src set id = 3 where id = 2;
update erb_src set v = 'edited' where id = 3;
-- @regex("Duplicate entry", true)
update erb_src set v = 'nine' where id in (3, 8);
update erb_src set v = 'final' where id = 3;
commit;
set @erbl = (select rel_id from mo_catalog.mo_tables where relname = 'erb_src' and reldatabase = 'branch_merge_fk_pk_move');
select count(*) from mo_catalog.mo_data_branch_update_link where table_id = @erbl;
data branch merge erb_src into erb_p when conflict accept;
select id, v from erb_p order by id;
select id, pid from erb_c order by id;

-- Era-7 (review F1): an S3-route vacate-before-arrival pair inside one
-- transaction. B moves 3 -> 4 first (vacating a COMMITTED row), then A
-- moves 2 -> 3, landing at the key B just vacated. Both links carry NULL
-- replacement identities under one create timestamp. The by-key hop for
-- A's arrival at 3 must screen out B's same-transaction 3 -> 4 departure
-- (its old row id matches the committed stream tombstone at 3, i.e. it
-- consumed B's own pre-image, not A's landed generation): A's lineage
-- ends at 3, B's ends at 4, and each child follows its own parent.
create table ev_p (id int primary key, v varchar(9024));
create table ev_c (
    id int primary key,
    pid int,
    constraint fk_ev foreign key (pid) references ev_p(id) on update cascade
);
insert into ev_p values (2, repeat('a', 9000)), (3, repeat('b', 9000));
insert into ev_c values (1, 2), (2, 3);
data branch create table ev_src from ev_p;
select enable_fault_injection();
select add_fault_point('fj/cn/flush_small_objs',':::','echo',40,'branch_merge_fk_pk_move.ev_src');
begin;
update ev_src set id = 4 where id = 3;
update ev_src set id = 3 where id = 2;
commit;
select remove_fault_point('fj/cn/flush_small_objs');
select disable_fault_injection();
set @evl = (select rel_id from mo_catalog.mo_tables where relname = 'ev_src' and reldatabase = 'branch_merge_fk_pk_move');
select count(*) from mo_catalog.mo_data_branch_update_link where table_id = @evl;
data branch merge ev_src into ev_p when conflict accept;
select id, substr(v, 1, 1) from ev_p order by id;
select id, pid from ev_c order by id;

-- Era-7 (review F5): dropping the last live branch must keep the source
-- table's provenance links while a retained user snapshot can still ground
-- a historical branch of the lineage; the links retire only at the first
-- reclaim event after the snapshot is gone.
create table esv_p (id int primary key, v varchar(32));
create table esv_c (
    id int primary key,
    pid int,
    constraint fk_esv foreign key (pid) references esv_p(id) on update cascade
);
insert into esv_p values (2, 'two');
insert into esv_c values (1, 2);
data branch create table esv_br from esv_p;
update esv_br set id = 3 where id = 2;
data branch merge esv_br into esv_p when conflict accept;
set @esvp = (select rel_id from mo_catalog.mo_tables where relname = 'esv_p' and reldatabase = 'branch_merge_fk_pk_move');
select count(*) from mo_catalog.mo_data_branch_update_link where table_id = @esvp;
drop snapshot if exists esv_sp;
create snapshot esv_sp for table branch_merge_fk_pk_move esv_p;
drop table esv_br;
select count(*) from mo_catalog.mo_data_branch_update_link where table_id = @esvp;
drop snapshot esv_sp;
data branch create table esv_b2 from esv_p;
drop table esv_b2;
select count(*) from mo_catalog.mo_data_branch_update_link where table_id = @esvp;

-- Era-8 (review F1): a same-key S3-route edit on B BEFORE the
-- vacate-before-arrival pair. B is edited at 3 (the S3-buffered
-- replacement never stages in the workspace), then B moves 3 -> 4 and
-- A moves 2 -> 3 inside one transaction. The move link records the
-- S3-buffered replacement identity the statement read, while the
-- committed stream tombstone at 3 names B's committed pre-image: the
-- edit's continuity marker must bind B's durable origin and the move's
-- recorded old identity must heal back to it, or A's arrival at 3
-- adopts B's departure and each child follows the WRONG parent. Each
-- lineage keeps its own payload and child.
create table ew_p (id int primary key, v varchar(32));
create table ew_c (
    id int primary key,
    pid int,
    constraint fk_ew foreign key (pid) references ew_p(id) on update cascade
);
insert into ew_p values (2, 'A0'), (3, 'B0');
insert into ew_c values (1, 2), (2, 3);
data branch create table ew_src from ew_p;
select enable_fault_injection();
select add_fault_point('fj/cn/flush_small_objs',':::','echo',40,'branch_merge_fk_pk_move.ew_src');
begin;
update ew_src set v = 'B1' where id = 3;
update ew_src set id = 4 where id = 3;
update ew_src set id = 3 where id = 2;
commit;
select remove_fault_point('fj/cn/flush_small_objs');
select disable_fault_injection();
set @ewl = (select rel_id from mo_catalog.mo_tables where relname = 'ew_src' and reldatabase = 'branch_merge_fk_pk_move');
select count(*) from mo_catalog.mo_data_branch_update_link where table_id = @ewl;
data branch merge ew_src into ew_p when conflict accept;
select id, v from ew_p order by id;
select id, pid from ew_c order by id;


-- Era-8 (review F3): chain/hop budget boundaries. Recorded chains of 31
-- and exactly 32 source-side moves fold into one native parent update
-- whose declared ON UPDATE action fires; a 33-move chain exceeds the
-- hop budget, and the unresolved provenance must ABORT the apply
-- statement-atomically instead of degrading to legacy DELETE+INSERT
-- replay — on a parent declaring BOTH ON UPDATE CASCADE and ON DELETE
-- CASCADE the destructive replay would otherwise delete the child and
-- insert the final parent. SET NULL declared actions ride the fold
-- with native UPDATE semantics.
create table eh31_p (id int primary key, v varchar(32));
create table eh31_c (
    id int primary key,
    pid int,
    constraint fk_eh31 foreign key (pid) references eh31_p(id) on update cascade
);
insert into eh31_p values (1, 'p1');
insert into eh31_c values (1, 1);
data branch create table eh31_s from eh31_p;
update eh31_s set id = 2 where id = 1;
update eh31_s set id = 3 where id = 2;
update eh31_s set id = 4 where id = 3;
update eh31_s set id = 5 where id = 4;
update eh31_s set id = 6 where id = 5;
update eh31_s set id = 7 where id = 6;
update eh31_s set id = 8 where id = 7;
update eh31_s set id = 9 where id = 8;
update eh31_s set id = 10 where id = 9;
update eh31_s set id = 11 where id = 10;
update eh31_s set id = 12 where id = 11;
update eh31_s set id = 13 where id = 12;
update eh31_s set id = 14 where id = 13;
update eh31_s set id = 15 where id = 14;
update eh31_s set id = 16 where id = 15;
update eh31_s set id = 17 where id = 16;
update eh31_s set id = 18 where id = 17;
update eh31_s set id = 19 where id = 18;
update eh31_s set id = 20 where id = 19;
update eh31_s set id = 21 where id = 20;
update eh31_s set id = 22 where id = 21;
update eh31_s set id = 23 where id = 22;
update eh31_s set id = 24 where id = 23;
update eh31_s set id = 25 where id = 24;
update eh31_s set id = 26 where id = 25;
update eh31_s set id = 27 where id = 26;
update eh31_s set id = 28 where id = 27;
update eh31_s set id = 29 where id = 28;
update eh31_s set id = 30 where id = 29;
update eh31_s set id = 31 where id = 30;
update eh31_s set id = 32 where id = 31;
set @eh31l = (select rel_id from mo_catalog.mo_tables where relname = 'eh31_s' and reldatabase = 'branch_merge_fk_pk_move');
select count(*) from mo_catalog.mo_data_branch_update_link where table_id = @eh31l;
data branch merge eh31_s into eh31_p when conflict accept;
select id, v from eh31_p order by id;
select id, pid from eh31_c order by id;

create table eh32_p (id int primary key, v varchar(32));
create table eh32_c (
    id int primary key,
    pid int,
    constraint fk_eh32 foreign key (pid) references eh32_p(id) on update cascade
);
insert into eh32_p values (1, 'p1');
insert into eh32_c values (1, 1);
data branch create table eh32_s from eh32_p;
update eh32_s set id = 2 where id = 1;
update eh32_s set id = 3 where id = 2;
update eh32_s set id = 4 where id = 3;
update eh32_s set id = 5 where id = 4;
update eh32_s set id = 6 where id = 5;
update eh32_s set id = 7 where id = 6;
update eh32_s set id = 8 where id = 7;
update eh32_s set id = 9 where id = 8;
update eh32_s set id = 10 where id = 9;
update eh32_s set id = 11 where id = 10;
update eh32_s set id = 12 where id = 11;
update eh32_s set id = 13 where id = 12;
update eh32_s set id = 14 where id = 13;
update eh32_s set id = 15 where id = 14;
update eh32_s set id = 16 where id = 15;
update eh32_s set id = 17 where id = 16;
update eh32_s set id = 18 where id = 17;
update eh32_s set id = 19 where id = 18;
update eh32_s set id = 20 where id = 19;
update eh32_s set id = 21 where id = 20;
update eh32_s set id = 22 where id = 21;
update eh32_s set id = 23 where id = 22;
update eh32_s set id = 24 where id = 23;
update eh32_s set id = 25 where id = 24;
update eh32_s set id = 26 where id = 25;
update eh32_s set id = 27 where id = 26;
update eh32_s set id = 28 where id = 27;
update eh32_s set id = 29 where id = 28;
update eh32_s set id = 30 where id = 29;
update eh32_s set id = 31 where id = 30;
update eh32_s set id = 32 where id = 31;
update eh32_s set id = 33 where id = 32;
set @eh32l = (select rel_id from mo_catalog.mo_tables where relname = 'eh32_s' and reldatabase = 'branch_merge_fk_pk_move');
select count(*) from mo_catalog.mo_data_branch_update_link where table_id = @eh32l;
data branch merge eh32_s into eh32_p when conflict accept;
select id, v from eh32_p order by id;
select id, pid from eh32_c order by id;

create table eh33_p (id int primary key, v varchar(32));
create table eh33_c (
    id int primary key,
    pid int,
    constraint fk_eh33 foreign key (pid) references eh33_p(id) on update cascade on delete cascade
);
insert into eh33_p values (1, 'p1');
insert into eh33_c values (1, 1);
data branch create table eh33_s from eh33_p;
update eh33_s set id = 2 where id = 1;
update eh33_s set id = 3 where id = 2;
update eh33_s set id = 4 where id = 3;
update eh33_s set id = 5 where id = 4;
update eh33_s set id = 6 where id = 5;
update eh33_s set id = 7 where id = 6;
update eh33_s set id = 8 where id = 7;
update eh33_s set id = 9 where id = 8;
update eh33_s set id = 10 where id = 9;
update eh33_s set id = 11 where id = 10;
update eh33_s set id = 12 where id = 11;
update eh33_s set id = 13 where id = 12;
update eh33_s set id = 14 where id = 13;
update eh33_s set id = 15 where id = 14;
update eh33_s set id = 16 where id = 15;
update eh33_s set id = 17 where id = 16;
update eh33_s set id = 18 where id = 17;
update eh33_s set id = 19 where id = 18;
update eh33_s set id = 20 where id = 19;
update eh33_s set id = 21 where id = 20;
update eh33_s set id = 22 where id = 21;
update eh33_s set id = 23 where id = 22;
update eh33_s set id = 24 where id = 23;
update eh33_s set id = 25 where id = 24;
update eh33_s set id = 26 where id = 25;
update eh33_s set id = 27 where id = 26;
update eh33_s set id = 28 where id = 27;
update eh33_s set id = 29 where id = 28;
update eh33_s set id = 30 where id = 29;
update eh33_s set id = 31 where id = 30;
update eh33_s set id = 32 where id = 31;
update eh33_s set id = 33 where id = 32;
update eh33_s set id = 34 where id = 33;
set @eh33l = (select rel_id from mo_catalog.mo_tables where relname = 'eh33_s' and reldatabase = 'branch_merge_fk_pk_move');
select count(*) from mo_catalog.mo_data_branch_update_link where table_id = @eh33l;
-- @regex("could not be resolved completely", true)
data branch merge eh33_s into eh33_p when conflict accept;
select id, v from eh33_p order by id;
select id, pid from eh33_c order by id;

create table esn_p (id int primary key, v varchar(32));
create table esn_c (
    id int primary key,
    pid int,
    constraint fk_esn foreign key (pid) references esn_p(id) on update set null
);
insert into esn_p values (2, 'two');
insert into esn_c values (1, 2);
data branch create table esn_s from esn_p;
update esn_s set id = 3 where id = 2;
data branch merge esn_s into esn_p when conflict accept;
select id, v from esn_p order by id;
select id, pid from esn_c order by id;

-- Era-9 (review F2): a recorded key-rotation cycle cannot replay as
-- in-place native UPDATEs (every landing key still holds a cycle
-- member), and legacy DELETE+INSERT replay would swap payloads while
-- keeping the destination keys so each child follows the WRONG parent
-- with no FK violation to stop it. The apply must ABORT
-- statement-atomically; the destination tables stay byte-identical.
-- The three moves 2 -> 4, 3 -> 2, 4 -> 3 close the cycle 2 -> 4 -> 3.
create table ecyc_p (id int primary key, v varchar(32));
create table ecyc_c (
    id int primary key,
    pid int,
    constraint fk_ecyc foreign key (pid) references ecyc_p(id) on update cascade on delete cascade
);
insert into ecyc_p values (2, 'two'), (3, 'three');
insert into ecyc_c values (1, 2), (2, 3);
data branch create table ecyc_s from ecyc_p;
update ecyc_s set id = 4 where id = 2;
update ecyc_s set id = 2 where id = 3;
update ecyc_s set id = 3 where id = 4;
set @ecycl = (select rel_id from mo_catalog.mo_tables where relname = 'ecyc_s' and reldatabase = 'branch_merge_fk_pk_move');
select count(*) from mo_catalog.mo_data_branch_update_link where table_id = @ecycl;
-- @regex("key rotation cycle", true)
data branch merge ecyc_s into ecyc_p when conflict accept;
select id, v from ecyc_p order by id;
select id, pid from ecyc_c order by id;

-- Era-9 (review F4): a source-only lineage that inserts, moves, and
-- deletes one row in three statements nets to an EMPTY diff. Its
-- retained provenance walk cannot resolve (the departure's replacement
-- never committed to the stream), yet the lineage never touches the
-- destination, so the merge must succeed as a no-op instead of
-- aborting on unresolved provenance.
create table eso_p (id int primary key, v varchar(32));
create table eso_c (
    id int primary key,
    pid int,
    constraint fk_eso foreign key (pid) references eso_p(id) on update cascade
);
insert into eso_p values (99, 'stay');
insert into eso_c values (1, 99);
data branch create table eso_s from eso_p;
insert into eso_s values (2, 'gone');
update eso_s set id = 3 where id = 2;
delete from eso_s where id = 3;
set @esol = (select rel_id from mo_catalog.mo_tables where relname = 'eso_s' and reldatabase = 'branch_merge_fk_pk_move');
select count(*) from mo_catalog.mo_data_branch_update_link where table_id = @esol;
data branch merge eso_s into eso_p when conflict accept;
select id, v from eso_p order by id;
select id, pid from eso_c order by id;

-- Era-10 (review F1): an S3-route move followed by TWO unarmed
-- same-key payload edits in one transaction. The first edit diverts
-- the death of the S3-staged replacement and heals the link's
-- continuity; the second edit must preserve that evidence instead of
-- overwriting the marker statement. Pre-fix the commit prune dropped
-- the live link, the merge fell back to legacy DELETE+INSERT replay,
-- and ON DELETE CASCADE deleted the child. Post-fix the single
-- recorded move survives both edits: the merge replays one native
-- UPDATE carrying the final payload 'e2' and the child follows
-- 2 -> 3.
create table ere_p (id int primary key, v varchar(32));
create table ere_c (
    id int primary key,
    pid int,
    constraint fk_ere foreign key (pid) references ere_p(id) on update cascade on delete cascade
);
insert into ere_p values (2, 'two');
insert into ere_c values (1, 2);
data branch create table ere_s from ere_p;
select enable_fault_injection();
select add_fault_point('fj/cn/flush_small_objs',':::','echo',40,'branch_merge_fk_pk_move.ere_s');
begin;
update ere_s set id = 3 where id = 2;
select remove_fault_point('fj/cn/flush_small_objs');
select disable_fault_injection();
update ere_s set v = 'e1' where id = 3;
update ere_s set v = 'e2' where id = 3;
commit;
set @erel = (select rel_id from mo_catalog.mo_tables where relname = 'ere_s' and reldatabase = 'branch_merge_fk_pk_move');
select count(*) from mo_catalog.mo_data_branch_update_link where table_id = @erel;
data branch merge ere_s into ere_p when conflict accept;
select id, v from ere_p order by id;
select id, pid from ere_c order by id;

-- Era-10 (review F5): a forced workspace flush lands BETWEEN two
-- same-transaction moves. The flush must not convert the workspace
-- identities the first link names into durable object row ids the
-- second link cannot join: statement-boundary dumps keep every link
-- owner table's staged inserts in memory until the commit dump.
-- Pre-fix the chain severed and the merge aborted on unresolved
-- provenance; post-fix both links fold into one native UPDATE ending
-- at key 4 and the child follows. The filler table proves the flush
-- fault fired and keeps dumping unrelated tables.
create table ews_p (id int primary key, v varchar(32));
create table ews_c (
    id int primary key,
    pid int,
    constraint fk_ews foreign key (pid) references ews_p(id) on update cascade
);
insert into ews_p values (2, 'two');
insert into ews_c values (1, 2);
create table ews_filler (id int primary key, b int);
insert into ews_filler values (1, 1);
data branch create table ews_s from ews_p;
begin;
update ews_s set id = 3 where id = 2;
select enable_fault_injection();
select add_fault_point('fj/cn/workspace_force_flush',':::','echo',0,'');
update ews_filler set b = 9 where id = 1;
select remove_fault_point('fj/cn/workspace_force_flush');
select disable_fault_injection();
update ews_s set id = 4 where id = 3;
commit;
set @ewsl = (select rel_id from mo_catalog.mo_tables where relname = 'ews_s' and reldatabase = 'branch_merge_fk_pk_move');
select count(*) from mo_catalog.mo_data_branch_update_link where table_id = @ewsl;
data branch merge ews_s into ews_p when conflict accept;
select id, v from ews_p order by id;
select id, pid from ews_c order by id;

-- Era-10 (review F6): two separately committed moves return the row
-- to its original key with a CHANGED payload. The chain 2 -> 3 -> 2
-- is not a key-rotation cycle: the fold's departure and arrival share
-- one key, and the planner must treat it as an in-place native UPDATE
-- at the original key instead of rejecting the plan as cyclic.
-- Pre-fix the merge aborted with "key rotation cycle"; post-fix the
-- destination payload becomes 'two-b' and the child never moves.
create table ero_p (id int primary key, v varchar(32));
create table ero_c (
    id int primary key,
    pid int,
    constraint fk_ero foreign key (pid) references ero_p(id) on update cascade
);
insert into ero_p values (2, 'two');
insert into ero_c values (1, 2);
data branch create table ero_s from ero_p;
update ero_s set id = 3 where id = 2;
update ero_s set id = 2, v = 'two-b' where id = 3;
set @erol = (select rel_id from mo_catalog.mo_tables where relname = 'ero_s' and reldatabase = 'branch_merge_fk_pk_move');
select count(*) from mo_catalog.mo_data_branch_update_link where table_id = @erol;
data branch merge ero_s into ero_p when conflict accept;
select id, v from ero_p order by id;
select id, pid from ero_c order by id;

-- Era-10 (review F6): the same return home with an UNCHANGED payload.
-- The row's final image equals its original image, yet the recorded
-- chain 2 -> 3 -> 2 still folds through one self-key lineage;
-- pre-fix the planner rejected it as a key-rotation cycle. Post-fix
-- the merge applies as an in-place no-op and both tables keep their
-- exact original rows.
create table eru_p (id int primary key, v varchar(32));
create table eru_c (
    id int primary key,
    pid int,
    constraint fk_eru foreign key (pid) references eru_p(id) on update cascade
);
insert into eru_p values (2, 'two');
insert into eru_c values (1, 2);
data branch create table eru_s from eru_p;
update eru_s set id = 3 where id = 2;
update eru_s set id = 2 where id = 3;
set @erul = (select rel_id from mo_catalog.mo_tables where relname = 'eru_s' and reldatabase = 'branch_merge_fk_pk_move');
select count(*) from mo_catalog.mo_data_branch_update_link where table_id = @erul;
data branch merge eru_s into eru_p when conflict accept;
select id, v from eru_p order by id;
select id, pid from eru_c order by id;

-- Era-11 (review F1): a payload EDIT first, then a forced workspace spill,
-- then the primary-key move, all in one transaction. The pre-flush marker
-- records the durable origin under its replacement workspace rowid; the
-- flush must keep that provenance-pinned row in memory, or the move's link
-- names the uncommitted-object rowid instead of the durable tombstone and
-- the merge replays DELETE+INSERT — destroying the child through declared
-- ON DELETE CASCADE. The filler table forces the flush between the edit
-- and the move. Post-fix the link names the durable tombstone, the pair
-- folds to one native UPDATE, and the child follows.
create table eef_p (id int primary key, v varchar(32));
create table eef_c (
    id int primary key,
    pid int,
    constraint fk_eef foreign key (pid) references eef_p(id)
    on update cascade on delete cascade
);
insert into eef_p values (2, 'two');
insert into eef_c values (1, 2);
create table eef_f (id int primary key, b int);
insert into eef_f values (1, 1);
data branch create table eef_s from eef_p;
begin;
update eef_s set v = 'edited' where id = 2;
select enable_fault_injection();
select add_fault_point('fj/cn/workspace_force_flush',':::','echo',0,'');
update eef_f set b = 9 where id = 1;
select remove_fault_point('fj/cn/workspace_force_flush');
select disable_fault_injection();
update eef_s set id = 3 where id = 2;
commit;
set @eefl = (select rel_id from mo_catalog.mo_tables where relname = 'eef_s' and reldatabase = 'branch_merge_fk_pk_move');
select count(*) from mo_catalog.mo_data_branch_update_link where table_id = @eefl;
data branch merge eef_s into eef_p when conflict accept;
select id, v from eef_p order by id;
select id, pid from eef_c order by id;

-- Era-11 (review F4): a PROVEN same-transaction return to the original key
-- with a CHANGED payload. The two moves of row 2 -> 3 -> 2 commit as one
-- transaction and are joined by recorded workspace identities, so the walk
-- must resolve the homecoming (only a strictly-backward birth certificate
-- fails closed). Pre-fix the revisit guard rejected the equal-instant
-- revisit and the merge aborted on unresolved provenance; post-fix the
-- fold lands at the original key as an in-place native UPDATE.
create table ehc_p (id int primary key, v varchar(32));
create table ehc_c (
    id int primary key,
    pid int,
    constraint fk_ehc foreign key (pid) references ehc_p(id) on update cascade
);
insert into ehc_p values (2, 'two');
insert into ehc_c values (1, 2);
data branch create table ehc_s from ehc_p;
begin;
update ehc_s set id = 3 where id = 2;
update ehc_s set id = 2, v = 'two-b' where id = 3;
commit;
set @ehcl = (select rel_id from mo_catalog.mo_tables where relname = 'ehc_s' and reldatabase = 'branch_merge_fk_pk_move');
select count(*) from mo_catalog.mo_data_branch_update_link where table_id = @ehcl;
data branch merge ehc_s into ehc_p when conflict accept;
select id, v from ehc_p order by id;
select id, pid from ehc_c order by id;

-- Era-11 (review F4): the same single-transaction homecoming with an
-- UNCHANGED payload. The row's final image equals its original image; the
-- recorded 2 -> 3 -> 2 chain still folds, and the merge applies as an
-- in-place no-op keeping both tables byte-identical.
create table ehu_p (id int primary key, v varchar(32));
create table ehu_c (
    id int primary key,
    pid int,
    constraint fk_ehu foreign key (pid) references ehu_p(id) on update cascade
);
insert into ehu_p values (2, 'two');
insert into ehu_c values (1, 2);
data branch create table ehu_s from ehu_p;
begin;
update ehu_s set id = 3 where id = 2;
update ehu_s set id = 2 where id = 3;
commit;
set @ehul = (select rel_id from mo_catalog.mo_tables where relname = 'ehu_s' and reldatabase = 'branch_merge_fk_pk_move');
select count(*) from mo_catalog.mo_data_branch_update_link where table_id = @ehul;
data branch merge ehu_s into ehu_p when conflict accept;
select id, v from ehu_p order by id;
select id, pid from ehu_c order by id;

-- Era-11 (review F5): move 2 -> 3 in one commit, delete at 3 in a later
-- commit, on a table WITHOUT foreign keys. Complete evidence (the link
-- plus the same-key tail tombstone with no later continuation) proves a
-- TERMINAL deletion, which is defined delete semantics — not incomplete
-- provenance. Pre-fix the walk returned unresolved and even this
-- conflict-free merge aborted; post-fix the destination row at key 2 is
-- deleted and the table ends EMPTY.
create table etn_p (id int primary key, v varchar(32));
insert into etn_p values (2, 'two');
data branch create table etn_s from etn_p;
update etn_s set id = 3 where id = 2;
delete from etn_s where id = 3;
set @etnl = (select rel_id from mo_catalog.mo_tables where relname = 'etn_s' and reldatabase = 'branch_merge_fk_pk_move');
select count(*) from mo_catalog.mo_data_branch_update_link where table_id = @etnl;
data branch merge etn_s into etn_p when conflict accept;
select id, v from etn_p order by id;

-- Era-11 (review F5): the same move-then-delete with a declared
-- ON DELETE CASCADE child. The proven terminal deletion deletes the
-- destination parent natively, so the declared child cleanup runs:
-- both tables end EMPTY. Pre-fix the merge aborted on unresolved
-- provenance and touched nothing.
create table etc_p (id int primary key, v varchar(32));
create table etc_c (
    id int primary key,
    pid int,
    constraint fk_etc foreign key (pid) references etc_p(id)
    on update cascade on delete cascade
);
insert into etc_p values (2, 'two');
insert into etc_c values (1, 2);
data branch create table etc_s from etc_p;
update etc_s set id = 3 where id = 2;
delete from etc_s where id = 3;
set @etcl = (select rel_id from mo_catalog.mo_tables where relname = 'etc_s' and reldatabase = 'branch_merge_fk_pk_move');
select count(*) from mo_catalog.mo_data_branch_update_link where table_id = @etcl;
data branch merge etc_s into etc_p when conflict accept;
select id, v from etc_p order by id;
select id, pid from etc_c order by id;

-- Era-11 (review F5): move-then-delete against a RESTRICT child. The
-- proven terminal deletion applies as a native DELETE of the destination
-- parent, so the declared reject fires loudly and the statement is
-- atomic: both tables keep their exact original rows.
create table etr_p (id int primary key, v varchar(32));
create table etr_c (
    id int primary key,
    pid int,
    constraint fk_etr foreign key (pid) references etr_p(id) on update cascade
);
insert into etr_p values (2, 'two');
insert into etr_c values (1, 2);
data branch create table etr_s from etr_p;
update etr_s set id = 3 where id = 2;
delete from etr_s where id = 3;
set @etrl = (select rel_id from mo_catalog.mo_tables where relname = 'etr_s' and reldatabase = 'branch_merge_fk_pk_move');
select count(*) from mo_catalog.mo_data_branch_update_link where table_id = @etrl;
-- @regex("a foreign key constraint fails", true)
data branch merge etr_s into etr_p when conflict accept;
select id, v from etr_p order by id;
select id, pid from etr_c order by id;

-- Era-12 (review F1): an S3-route same-key edit, then a pure DELETE of the
-- edited row (diverted; no move link exists yet), then a spilled unrelated
-- reinsert at the same key, then a primary-key move, all in one
-- transaction. The delete retires the by-key lineage binding, so the
-- reinsert is a NEW row with no provenance to the original: pre-fix the
-- stale binding healed the intruder back to the durable origin and the
-- merge replayed one native UPDATE that resurrected the cascade-deleted
-- child onto the intruder. Post-fix no link is recorded and the replay is
-- plain DELETE+INSERT: the declared ON DELETE CASCADE cleans the child and
-- the parent ends holding only the new row, exactly like the same
-- statement history on plain tables.
create table edv_p (id int primary key, v varchar(32));
create table edv_c (
    id int primary key,
    pid int,
    constraint fk_edv foreign key (pid) references edv_p(id)
    on update cascade on delete cascade
);
insert into edv_p values (2, 'two');
insert into edv_c values (1, 2);
create table edv_f (id int primary key, b int);
insert into edv_f values (1, 1);
data branch create table edv_s from edv_p;
begin;
select enable_fault_injection();
select add_fault_point('fj/cn/flush_small_objs',':::','echo',40,'branch_merge_fk_pk_move.edv_s');
update edv_s set v = 'edited' where id = 2;
select remove_fault_point('fj/cn/flush_small_objs');
select disable_fault_injection();
delete from edv_s where id = 2;
insert into edv_s values (2, 'intruder');
select enable_fault_injection();
select add_fault_point('fj/cn/workspace_force_flush',':::','echo',0,'');
update edv_f set b = 9 where id = 1;
select remove_fault_point('fj/cn/workspace_force_flush');
select disable_fault_injection();
update edv_s set id = 3 where id = 2;
commit;
set @edvl = (select rel_id from mo_catalog.mo_tables where relname = 'edv_s' and reldatabase = 'branch_merge_fk_pk_move');
select count(*) from mo_catalog.mo_data_branch_update_link where table_id = @edvl;
data branch merge edv_s into edv_p when conflict accept;
select id, v from edv_p order by id;
select id, pid from edv_c order by id;

-- Era-12 (review F1): the same edit-delete-reinsert-move history against a
-- child whose delete action is RESTRICT. Post-fix the replay is plain
-- DELETE+INSERT, so the destination parent delete fires the declared
-- reject loudly and the merge statement is atomic: both tables keep their
-- exact original rows.
create table edr_p (id int primary key, v varchar(32));
create table edr_c (
    id int primary key,
    pid int,
    constraint fk_edr foreign key (pid) references edr_p(id) on update cascade
);
insert into edr_p values (2, 'two');
insert into edr_c values (1, 2);
create table edr_f (id int primary key, b int);
insert into edr_f values (1, 1);
data branch create table edr_s from edr_p;
begin;
select enable_fault_injection();
select add_fault_point('fj/cn/flush_small_objs',':::','echo',40,'branch_merge_fk_pk_move.edr_s');
update edr_s set v = 'edited' where id = 2;
select remove_fault_point('fj/cn/flush_small_objs');
select disable_fault_injection();
delete from edr_s where id = 2;
insert into edr_s values (2, 'intruder');
select enable_fault_injection();
select add_fault_point('fj/cn/workspace_force_flush',':::','echo',0,'');
update edr_f set b = 9 where id = 1;
select remove_fault_point('fj/cn/workspace_force_flush');
select disable_fault_injection();
update edr_s set id = 3 where id = 2;
commit;
set @edrl = (select rel_id from mo_catalog.mo_tables where relname = 'edr_s' and reldatabase = 'branch_merge_fk_pk_move');
select count(*) from mo_catalog.mo_data_branch_update_link where table_id = @edrl;
-- @regex("a foreign key constraint fails", true)
data branch merge edr_s into edr_p when conflict accept;
select id, v from edr_p order by id;
select id, pid from edr_c order by id;

-- Era-12 (review F2): a workspace primary-key move 2 -> 3, then (same
-- transaction) an S3-route same-key payload edit of 3. The move's link
-- must follow the row onto its S3 replacement: pre-fix the edit's
-- replacement harvest could return the logically deleted workspace row,
-- the commit-time shrink then removed it, and the genuine link was pruned,
-- so the merge replayed DELETE+INSERT and declared ON DELETE CASCADE
-- destroyed the child. Post-fix one link survives onto the final image and
-- the replay is one native UPDATE: parent (3,'edited') with the child
-- following to pid 3, exactly like the same statements on plain tables.
create table ewt_p (id int primary key, v varchar(32));
create table ewt_c (
    id int primary key,
    pid int,
    constraint fk_ewt foreign key (pid) references ewt_p(id)
    on update cascade on delete cascade
);
insert into ewt_p values (2, 'two');
insert into ewt_c values (1, 2);
data branch create table ewt_s from ewt_p;
begin;
update ewt_s set id = 3 where id = 2;
select enable_fault_injection();
select add_fault_point('fj/cn/flush_small_objs',':::','echo',40,'branch_merge_fk_pk_move.ewt_s');
update ewt_s set v = 'edited' where id = 3;
select remove_fault_point('fj/cn/flush_small_objs');
select disable_fault_injection();
commit;
set @ewtl = (select rel_id from mo_catalog.mo_tables where relname = 'ewt_s' and reldatabase = 'branch_merge_fk_pk_move');
select count(*) from mo_catalog.mo_data_branch_update_link where table_id = @ewtl;
data branch merge ewt_s into ewt_p when conflict accept;
select id, v from ewt_p order by id;
select id, pid from ewt_c order by id;

-- Era-13 (review F1): in one transaction a workspace payload edit of key 2
-- (stages a workspace row whose origin is the durable identity), then an
-- S3-route payload edit of the same row whose pre-image is exactly that
-- workspace identity, then the PK move 2 -> 3. The lineage must carry the
-- durable origin across the route switch: pre-fix the binder excluded every
-- workspace pre-image before composing the lineage's recorded evidence, so
-- the move kept the S3-buffered uncommitted identity as its old anchor,
-- that identity never appears in the committed change stream, and the merge
-- replayed DELETE+INSERT — declared ON DELETE CASCADE destroyed the child.
-- Post-fix the lineage composes the earlier edit's durable origin first, so
-- the update replays as one native destination UPDATE: parent (3,'edited2')
-- with the child following to pid 3, byte-identical to the same statements
-- on plain tables.
create table esw_p (id int primary key, v varchar(32));
create table esw_c (
    id int primary key,
    pid int,
    constraint fk_esw foreign key (pid) references esw_p(id)
    on update cascade on delete cascade
);
insert into esw_p values (2, 'two');
insert into esw_c values (1, 2);
data branch create table esw_s from esw_p;
begin;
update esw_s set v = 'edited1' where id = 2;
select enable_fault_injection();
select add_fault_point('fj/cn/flush_small_objs',':::','echo',40,'branch_merge_fk_pk_move.esw_s');
update esw_s set v = 'edited2' where id = 2;
select remove_fault_point('fj/cn/flush_small_objs');
select disable_fault_injection();
update esw_s set id = 3 where id = 2;
commit;
set @eswl = (select rel_id from mo_catalog.mo_tables where relname = 'esw_s' and reldatabase = 'branch_merge_fk_pk_move');
select count(*) from mo_catalog.mo_data_branch_update_link where table_id = @eswl;
data branch merge esw_s into esw_p when conflict accept;
select id, v from esw_p order by id;
select id, pid from esw_c order by id;

-- Era-14 (review F1): one transaction edits a row twice — the second
-- edit's replacement lives only in the workspace — then plainly DELETEs
-- it: the raw workspace swallow leaves neither a tombstone nor a staged
-- DELETE, so the death used to go unrecorded and a by-key lineage binding
-- could outlive the murdered generation. The transaction then
-- independently re-INSERTs the key (forced down the flush route) and moves
-- it 2 -> 3. The swallowed death must retire the binding it murdered:
-- nothing that re-arrives at the key afterwards inherits the dead lineage,
-- so the merge replays the reincarnation's own DELETE+INSERT — ON DELETE
-- CASCADE removes the child before the intruder lands, and the link
-- belongs to the move alone.
create table esz_p (id int primary key, v varchar(32));
create table esz_c (
    id int primary key,
    pid int,
    constraint fk_esz foreign key (pid) references esz_p(id)
    on update cascade on delete cascade
);
insert into esz_p values (2, 'two');
insert into esz_c values (1, 2);
data branch create table esz_s from esz_p;
begin;
update esz_s set v = 'edited1' where id = 2;
update esz_s set v = 'edited2' where id = 2;
delete from esz_s where id = 2;
select enable_fault_injection();
select add_fault_point('fj/cn/workspace_force_flush',':::','echo',0,'');
insert into esz_s values (2, 'intruder');
select remove_fault_point('fj/cn/workspace_force_flush');
select disable_fault_injection();
update esz_s set id = 3 where id = 2;
commit;
set @eszl = (select rel_id from mo_catalog.mo_tables where relname = 'esz_s' and reldatabase = 'branch_merge_fk_pk_move');
select count(*) from mo_catalog.mo_data_branch_update_link where table_id = @eszl;
data branch merge esz_s into esz_p when conflict accept;
select id, v from esz_p order by id;
select id, pid from esz_c order by id;

-- Same murdered-lineage history against a RESTRICT child: the replayed
-- parent delete violates the constraint, the merge aborts statement-
-- atomically, and both tables keep their previous rows.
create table eszr_p (id int primary key, v varchar(32));
create table eszr_c (
    id int primary key,
    pid int,
    constraint fk_eszr foreign key (pid) references eszr_p(id)
    on update cascade on delete restrict
);
insert into eszr_p values (2, 'two');
insert into eszr_c values (1, 2);
data branch create table eszr_s from eszr_p;
begin;
update eszr_s set v = 'edited1' where id = 2;
update eszr_s set v = 'edited2' where id = 2;
delete from eszr_s where id = 2;
select enable_fault_injection();
select add_fault_point('fj/cn/workspace_force_flush',':::','echo',0,'');
insert into eszr_s values (2, 'intruder');
select remove_fault_point('fj/cn/workspace_force_flush');
select disable_fault_injection();
update eszr_s set id = 3 where id = 2;
commit;
-- @regex("Cannot delete or update a parent row: a foreign key constraint fails", true)
data branch merge eszr_s into eszr_p when conflict accept;
select id, v from eszr_p order by id;
select id, pid from eszr_c order by id;

-- Era-14 (review F2): snapshots bracketing a move on an UNBRANCHED table,
-- then the source is dropped. The direct reclamation used to delete the
-- links unconditionally; post-fix a retained snapshot still grounds a
-- historical branch of the lineage, so the links survive the drop (1 -> 1).
-- The MERGE between the snapshot-instantiated siblings then behaves exactly
-- as with a live source: a move recorded on the common ancestor predates
-- both branches, so the replay brings the src-only row and keeps the
-- dst-only row (no destination delete is attempted on this stack — verified
-- byte-identical to the esyx_ control whose parent stays alive).
create table esy_p (id int primary key, v varchar(32));
insert into esy_p values (2, 'two');
drop snapshot if exists esy_s1;
create snapshot esy_s1 for table branch_merge_fk_pk_move esy_p;
update esy_p set id = 3 where id = 2;
drop snapshot if exists esy_s2;
create snapshot esy_s2 for table branch_merge_fk_pk_move esy_p;
set @esyl = (select rel_id from mo_catalog.mo_tables where relname = 'esy_p' and reldatabase = 'branch_merge_fk_pk_move');
select count(*) from mo_catalog.mo_data_branch_update_link where table_id = @esyl;
drop table esy_p;
select count(*) from mo_catalog.mo_data_branch_update_link where table_id = @esyl;
data branch create table esy_b1 from esy_p{snapshot="esy_s1"};
create table esy_c (
    id int primary key,
    pid int,
    constraint fk_esy foreign key (pid) references esy_b1(id)
    on update cascade on delete cascade
);
insert into esy_c values (1, 2);
data branch create table esy_b2 from esy_p{snapshot="esy_s2"};
data branch merge esy_b2 into esy_b1 when conflict accept;
select id, v from esy_b1 order by id;
select id, pid from esy_c order by id;

-- Live-parent control for esy_: identical snapshots, identical branches,
-- identical merge — only the drop is missing. The outcome rows must match
-- esy_ exactly (drop transparency: the grounded drop loses nothing).
create table esyx_p (id int primary key, v varchar(32));
insert into esyx_p values (2, 'two');
drop snapshot if exists esyx_s1;
create snapshot esyx_s1 for table branch_merge_fk_pk_move esyx_p;
update esyx_p set id = 3 where id = 2;
drop snapshot if exists esyx_s2;
create snapshot esyx_s2 for table branch_merge_fk_pk_move esyx_p;
set @esyxl = (select rel_id from mo_catalog.mo_tables where relname = 'esyx_p' and reldatabase = 'branch_merge_fk_pk_move');
select count(*) from mo_catalog.mo_data_branch_update_link where table_id = @esyxl;
data branch create table esyx_b1 from esyx_p{snapshot="esyx_s1"};
create table esyx_c (
    id int primary key,
    pid int,
    constraint fk_esyx foreign key (pid) references esyx_b1(id)
    on update cascade on delete cascade
);
insert into esyx_c values (1, 2);
data branch create table esyx_b2 from esyx_p{snapshot="esyx_s2"};
data branch merge esyx_b2 into esyx_b1 when conflict accept;
select id, v from esyx_b1 order by id;
select id, pid from esyx_c order by id;

-- Same snapshot-bracketed drop against a RESTRICT child: the replay never
-- attempts the destination delete for an ancestor-recorded move, so no
-- constraint rejection fires and both generations stay, child untouched.
create table esyr_p (id int primary key, v varchar(32));
insert into esyr_p values (2, 'two');
drop snapshot if exists esyr_s1;
create snapshot esyr_s1 for table branch_merge_fk_pk_move esyr_p;
update esyr_p set id = 3 where id = 2;
drop snapshot if exists esyr_s2;
create snapshot esyr_s2 for table branch_merge_fk_pk_move esyr_p;
set @esyrl = (select rel_id from mo_catalog.mo_tables where relname = 'esyr_p' and reldatabase = 'branch_merge_fk_pk_move');
select count(*) from mo_catalog.mo_data_branch_update_link where table_id = @esyrl;
drop table esyr_p;
select count(*) from mo_catalog.mo_data_branch_update_link where table_id = @esyrl;
data branch create table esyr_b1 from esyr_p{snapshot="esyr_s1"};
create table esyr_c (
    id int primary key,
    pid int,
    constraint fk_esyr foreign key (pid) references esyr_b1(id)
    on update cascade on delete restrict
);
insert into esyr_c values (1, 2);
data branch create table esyr_b2 from esyr_p{snapshot="esyr_s2"};
data branch merge esyr_b2 into esyr_b1 when conflict accept;
select id, v from esyr_b1 order by id;
select id, pid from esyr_c order by id;

-- Counterexample: with NO snapshot grounding the lineage, the plain
-- unbranched drop still reclaims the links — the grounding gate must not
-- become a retention leak. The drop flow probes coverage under the plan's
-- own identity because the relation row is already retired by reclaim
-- time (by-id fallback fails closed instead).
create table esy2_p (id int primary key, v varchar(32));
insert into esy2_p values (2, 'two');
update esy2_p set id = 3 where id = 2;
set @esy2l = (select rel_id from mo_catalog.mo_tables where relname = 'esy2_p' and reldatabase = 'branch_merge_fk_pk_move');
select count(*) from mo_catalog.mo_data_branch_update_link where table_id = @esy2l;
drop table esy2_p;
select count(*) from mo_catalog.mo_data_branch_update_link where table_id = @esy2l;

-- TRUNCATE routes the alter lineage machinery: with a retained snapshot
-- grounding the pre-truncate move, the old generation's links survive
-- (1 -> 1) so historical branches of that generation keep their evidence.
create table esy3_p (id int primary key, v varchar(32));
insert into esy3_p values (2, 'two');
update esy3_p set id = 3 where id = 2;
drop snapshot if exists esy3_s;
create snapshot esy3_s for table branch_merge_fk_pk_move esy3_p;
set @esy3l = (select rel_id from mo_catalog.mo_tables where relname = 'esy3_p' and reldatabase = 'branch_merge_fk_pk_move');
select count(*) from mo_catalog.mo_data_branch_update_link where table_id = @esy3l;
truncate table esy3_p;
select count(*) from mo_catalog.mo_data_branch_update_link where table_id = @esy3l;

-- era-15 (r21 F3): destination-deleted/source-moved restore shapes in MERGE,
-- mirroring the PICK delpick/esqs_/esqf_ sections. ACCEPT wins the lineage
-- back through the recorded provenance and restores the deleted row at its
-- landed key; SKIP keeps the destination's delete; FAIL aborts atomically
-- with the destination untouched.
create table esqm_p (id int primary key, v varchar(32));
insert into esqm_p values (2, 'two'), (9, 'nine');
data branch create table esqm_src from esqm_p;
update esqm_src set id = 3, v = 'three' where id = 2;
delete from esqm_p where id = 2;
data branch merge esqm_src into esqm_p when conflict accept;
select id, v from esqm_p order by id;

create table esqms_p (id int primary key, v varchar(32));
insert into esqms_p values (2, 'two'), (9, 'nine');
data branch create table esqms_src from esqms_p;
update esqms_src set id = 3, v = 'three' where id = 2;
delete from esqms_p where id = 2;
data branch merge esqms_src into esqms_p when conflict skip;
select id, v from esqms_p order by id;

create table esqmf_p (id int primary key, v varchar(32));
insert into esqmf_p values (2, 'two'), (9, 'nine');
data branch create table esqmf_src from esqmf_p;
update esqmf_src set id = 3, v = 'three' where id = 2;
delete from esqmf_p where id = 2;
-- @regex("esqmf_src UPDATE and esqmf_p DELETE", true)
data branch merge esqmf_src into esqmf_p when conflict fail;
select id, v from esqmf_p order by id;

-- era-16 (r22 F4): an occupied landed key on the restore shape. The
-- destination deleted the move's old key AND inserted its own row at the
-- landed key; ACCEPT must route the restored row through the same
-- landed-key conflict resolution every folded move obeys and win the key
-- back, SKIP keeps the destination's occupant, FAIL aborts atomically.
-- Before the fix the ACCEPT restore insert met the tracked occupant as a
-- duplicate key (issue-verified live RED: ERROR 1062 Duplicate entry '3').
create table etja_p (id int primary key, v varchar(32));
insert into etja_p values (2, 'two');
data branch create table etja_src from etja_p;
update etja_src set id = 3, v = 'three' where id = 2;
delete from etja_p where id = 2;
insert into etja_p values (3, 'destination');
data branch merge etja_src into etja_p when conflict accept;
select id, v from etja_p order by id;

create table etjs_p (id int primary key, v varchar(32));
insert into etjs_p values (2, 'two');
data branch create table etjs_src from etjs_p;
update etjs_src set id = 3, v = 'three' where id = 2;
delete from etjs_p where id = 2;
insert into etjs_p values (3, 'destination');
data branch merge etjs_src into etjs_p when conflict skip;
select id, v from etjs_p order by id;

create table etjf_p (id int primary key, v varchar(32));
insert into etjf_p values (2, 'two');
data branch create table etjf_src from etjf_p;
update etjf_src set id = 3, v = 'three' where id = 2;
delete from etjf_p where id = 2;
insert into etjf_p values (3, 'destination');
-- @regex("etjf_src UPDATE and etjf_p DELETE", true)
data branch merge etjf_src into etjf_p when conflict fail;
select id, v from etjf_p order by id;

-- era-16 (r22 F1): multi-survivor overlapping workspace shrink. One
-- transaction moves three rows (one insert batch, so the shrink remap
-- chains onto itself) and deletes the middle generation; each survivor's
-- lineage must ride exactly to its new identity, folding to one native
-- UPDATE that cascades the survivor's own child, while the murdered
-- generation departs plainly. Untouched rows and their children stay.
create table etjms_p (id int primary key, v varchar(32));
create table etjms_c (
    id int primary key,
    pid int,
    constraint fk_etjms foreign key (pid) references etjms_p(id)
    on update cascade on delete cascade
);
insert into etjms_p values (11, 'a'), (12, 'b'), (13, 'c');
insert into etjms_c values (1, 11), (2, 12), (3, 13);
data branch create table etjms_src from etjms_p;
begin;
update etjms_src set id = id + 100 where id in (11, 12, 13);
delete from etjms_src where id = 112;
commit;
data branch merge etjms_src into etjms_p when conflict accept;
select id, v from etjms_p order by id;
select id, pid from etjms_c order by id;

-- era-16 (r22 F2 ambiguity contract, pins current behavior): a
-- same-transaction same-key DELETE+INSERT is byte-identical in the
-- committed change stream to multi_update's stable-key UPDATE, and
-- #27996 defines the pair as update replay, so both histories fold and
-- cascade alike through a later move. A = payload update then move;
-- B = explicit txn DELETE+INSERT then move; D = B without the move
-- (the plain path replays it as an UPDATE as well). The native controls
-- diverge by upstream design: a native DELETE cascade-empties children
-- and RESTRICT rejects — see merge_foreign_key_update.sql.
create table etj2a_p (id int primary key, v varchar(32));
create table etj2a_c (
    id int primary key,
    pid int,
    constraint fk_etj2a foreign key (pid) references etj2a_p(id)
    on update cascade on delete cascade
);
insert into etj2a_p values (2, 'two');
insert into etj2a_c values (1, 2);
data branch create table etj2a_src from etj2a_p;
update etj2a_src set v = 'v1' where id = 2;
update etj2a_src set id = 3, v = 'a-lane' where id = 2;
data branch merge etj2a_src into etj2a_p when conflict accept;
select id, v from etj2a_p order by id;
select id, pid from etj2a_c order by id;

create table etj2b_p (id int primary key, v varchar(32));
create table etj2b_c (
    id int primary key,
    pid int,
    constraint fk_etj2b foreign key (pid) references etj2b_p(id)
    on update cascade on delete cascade
);
insert into etj2b_p values (2, 'two');
insert into etj2b_c values (1, 2);
data branch create table etj2b_src from etj2b_p;
begin;
delete from etj2b_src where id = 2;
insert into etj2b_src values (2, 'unrelated');
commit;
update etj2b_src set id = 3, v = 'b-lane' where id = 2;
data branch merge etj2b_src into etj2b_p when conflict accept;
select id, v from etj2b_p order by id;
select id, pid from etj2b_c order by id;

create table etj2d_p (id int primary key, v varchar(32));
create table etj2d_c (
    id int primary key,
    pid int,
    constraint fk_etj2d foreign key (pid) references etj2d_p(id)
    on update cascade on delete cascade
);
insert into etj2d_p values (2, 'two');
insert into etj2d_c values (1, 2);
data branch create table etj2d_src from etj2d_p;
begin;
delete from etj2d_src where id = 2;
insert into etj2d_src values (2, 'other');
commit;
data branch merge etj2d_src into etj2d_p when conflict accept;
select id, v from etj2d_p order by id;
select id, pid from etj2d_c order by id;

-- Era-17 (r23 review F1): a restore must not be emitted ahead of the
-- surviving move that first vacates its landed key. Destination holds
-- (1,'A'),(2,'B') and deletes 1; the source moves B 2 -> 3 and A 1 -> 2.
-- ACCEPT restores A at 2 and relocates B to 3: replayed restore-first,
-- the restore's authorized victim delete removes B (cascading its
-- child) and the move's UPDATE then rewrites the restored row, so both
-- (2,'A') and B's lineage vanish. Both parent rows and B's child must
-- survive, with B's child re-parented by the cascade.
create table etkf1_p (id int primary key, v varchar(32));
create table etkf1_c (
    id int primary key,
    pid int,
    constraint fk_etkf1 foreign key (pid) references etkf1_p(id)
    on update cascade on delete cascade
);
insert into etkf1_p values (1, 'A'), (2, 'B');
insert into etkf1_c values (100, 2);
data branch create table etkf1_src from etkf1_p;
update etkf1_src set id = 3 where id = 2;
update etkf1_src set id = 2 where id = 1;
delete from etkf1_p where id = 1;
data branch merge etkf1_src into etkf1_p when conflict accept;
select id, v from etkf1_p order by id;
select id, pid from etkf1_c order by id;

-- Era-17 (r23 review F2): a retained user snapshot of a branch member
-- guards the member's pk-move provenance through its reclaim exactly
-- like a row-less parent's. Drop the branch, clone a historical
-- descendant off the retained snapshot, and merge it back: the move
-- recorded on the dropped member must still fold as one native UPDATE,
-- letting the dual-cascade child follow instead of dying with the
-- replayed parent delete.
create table etkf2_p (id int primary key, v varchar(32));
create table etkf2_c (
    id int primary key,
    pid int,
    constraint fk_etkf2 foreign key (pid) references etkf2_p(id)
    on update cascade on delete cascade
);
insert into etkf2_p values (2, 'two');
insert into etkf2_c values (1, 2);
data branch create table etkf2_b from etkf2_p;
update etkf2_b set id = 3 where id = 2;
set @etkf2b = (select rel_id from mo_catalog.mo_tables where relname = 'etkf2_b' and reldatabase = 'branch_merge_fk_pk_move');
select count(*) from mo_catalog.mo_data_branch_update_link where table_id = @etkf2b;
drop snapshot if exists etkf2_snap;
create snapshot etkf2_snap for table branch_merge_fk_pk_move etkf2_b;
drop table etkf2_b;
-- the retained snapshot keeps the dropped member's provenance row
select count(*) from mo_catalog.mo_data_branch_update_link where table_id = @etkf2b;
data branch create table etkf2_h from etkf2_b{snapshot="etkf2_snap"};
data branch merge etkf2_h into etkf2_p when conflict accept;
select id, v from etkf2_p order by id;
select id, pid from etkf2_c order by id;
drop snapshot etkf2_snap;

-- Era-17 (r23 review F3): an ordinary bulk non-key UPDATE inside an
-- explicit transaction retains bookkeeping through the same per-statement
-- context a pk move would use, but it participates in no provenance
-- link; the delete and dump boundaries must stay exempt like the fill
-- boundary, or the transaction never commits. 300000 rows of bookkeeping
-- cross the 64MiB coordinator budget on the unguarded path, so a
-- reintroduced refusal fails this section.
create table etkf3_t (id int primary key, payload bigint);
insert into etkf3_t select result, result from generate_series(1, 300000) g;
begin;
update etkf3_t set payload = payload + 1;
delete from etkf3_t where id <= 90;
update etkf3_t set payload = payload + 1 where id between 91 and 180;
commit;
select count(*) from etkf3_t;
select sum(payload) from etkf3_t;

-- Era-18 (r24 review F1): SKIP must propagate through move
-- dependencies. Destination holds (1,'A'),(2,'B') with B's child plus
-- an independent destination-only row (3,'C'); the source moves
-- B 2 -> 3 and A 1 -> 2. B's move conflicts with (3,'C') and SKIP
-- drops it; A's move needs B's lineage to vacate key 2, so it must
-- drop as well. Without the cascade, A's skipped-dependent move keeps
-- its authorized victim delete at key 2 and silently removes B and its
-- cascaded child. Destination and child must stay exactly as before.
create table etkf4_p (id int primary key, v varchar(32));
create table etkf4_c (
    id int primary key,
    pid int,
    constraint fk_etkf4 foreign key (pid) references etkf4_p(id)
    on update cascade on delete cascade
);
insert into etkf4_p values (1, 'A'), (2, 'B');
insert into etkf4_c values (100, 2);
data branch create table etkf4_src from etkf4_p;
update etkf4_src set id = 3 where id = 2;
update etkf4_src set id = 2 where id = 1;
insert into etkf4_p values (3, 'C');
data branch merge etkf4_src into etkf4_p when conflict skip;
select id, v from etkf4_p order by id;
select id, pid from etkf4_c order by id;

-- Era-18 (r24 review F2): snapshot coverage must match the table's
-- physical identity, not its current name; a rename between the
-- snapshot and the drop must not forfeit the retained provenance.
-- Same shape as etkf2_ with an extra RENAME: clone a historical
-- descendant off the retained snapshot and merge it back; the move
-- recorded on the renamed member must still fold as one native UPDATE
-- so the dual-cascade child follows.
create table etkf2r_p (id int primary key, v varchar(32));
create table etkf2r_c (
    id int primary key,
    pid int,
    constraint fk_etkf2r foreign key (pid) references etkf2r_p(id)
    on update cascade on delete cascade
);
insert into etkf2r_p values (2, 'two');
insert into etkf2r_c values (1, 2);
data branch create table etkf2r_b from etkf2r_p;
update etkf2r_b set id = 3 where id = 2;
set @etkf2rb = (select rel_id from mo_catalog.mo_tables where relname = 'etkf2r_b' and reldatabase = 'branch_merge_fk_pk_move');
select count(*) from mo_catalog.mo_data_branch_update_link where table_id = @etkf2rb;
drop snapshot if exists etkf2r_snap;
create snapshot etkf2r_snap for table branch_merge_fk_pk_move etkf2r_b;
alter table etkf2r_b rename to etkf2r_b2;
drop table etkf2r_b2;
-- the retained snapshot keeps the renamed member's provenance row
select count(*) from mo_catalog.mo_data_branch_update_link where table_id = @etkf2rb;
data branch create table etkf2r_h from etkf2r_b{snapshot="etkf2r_snap"};
data branch merge etkf2r_h into etkf2r_p when conflict accept;
select id, v from etkf2r_p order by id;
select id, pid from etkf2r_c order by id;
drop snapshot etkf2r_snap;

-- Era-19 (r25 review F1): SKIP must cascade through move dependencies
-- for a payload conflict too, not only for a conflicting whole row.
-- Destination holds (1,'A'),(2,'B') with B's child, and the destination
-- itself edits B's payload; the source moves B 2 -> 3 and A 1 -> 2.
-- B's move conflicts with the destination's own edit of key 2 and SKIP
-- drops it; A's move needs B's lineage to vacate key 2, so it must drop
-- as well — otherwise its surviving victim delete at key 2 silently
-- removes B and cascades the child away under reported success.
-- Destination and child must stay exactly as before.
create table etkf5_p (id int primary key, v varchar(32));
create table etkf5_c (
    id int primary key,
    pid int,
    constraint fk_etkf5 foreign key (pid) references etkf5_p(id)
    on update cascade on delete cascade
);
insert into etkf5_p values (1, 'A'), (2, 'B');
insert into etkf5_c values (100, 2);
data branch create table etkf5_src from etkf5_p;
update etkf5_src set id = 3 where id = 2;
update etkf5_src set id = 2 where id = 1;
update etkf5_p set v = 'B2' where id = 2;
data branch merge etkf5_src into etkf5_p when conflict skip;
select id, v from etkf5_p order by id;
select id, pid from etkf5_c order by id;

-- Era-19 (r25 review F2): a forced workspace spill, then an intra-
-- transaction delete of a spilled row — the statement boundary rewrites
-- the dirty object and re-mints the SURVIVOR's identity — then a pk move
-- of that survivor. The provenance recorded before the rewrite names the
-- survivor's pre-rewrite identity and must ride the rewrite, or the
-- commit loses the update link and the merge replays DELETE/INSERT, so
-- ON DELETE CASCADE erases the child. Post-fix one link survives and the
-- replay is one native UPDATE: parent (3,'b2') with the child following
-- to pid 3, exactly like the same statements on plain tables.
create table etkf6_p (id int primary key, v varchar(32));
create table etkf6_c (
    id int primary key,
    pid int,
    constraint fk_etkf6 foreign key (pid) references etkf6_p(id)
    on update cascade on delete cascade
);
insert into etkf6_p values (1, 'a'), (2, 'b');
insert into etkf6_c values (100, 2);
create table etkf6_f (id int primary key, b int);
insert into etkf6_f values (1, 1);
data branch create table etkf6_s from etkf6_p;
begin;
update etkf6_s set v = 'a2' where id = 1;
update etkf6_s set v = 'b2' where id = 2;
select enable_fault_injection();
select add_fault_point('fj/cn/workspace_force_flush',':::','echo',0,'');
update etkf6_f set b = 9 where id = 1;
select remove_fault_point('fj/cn/workspace_force_flush');
select disable_fault_injection();
delete from etkf6_s where id = 1;
update etkf6_f set b = 10 where id = 1;
update etkf6_s set id = 3 where id = 2;
commit;
set @etkf6l = (select rel_id from mo_catalog.mo_tables where relname = 'etkf6_s' and reldatabase = 'branch_merge_fk_pk_move');
select count(*) from mo_catalog.mo_data_branch_update_link where table_id = @etkf6l;
data branch merge etkf6_s into etkf6_p when conflict accept;
select id, v from etkf6_p order by id;
select id, pid from etkf6_c order by id;

-- Era-19 (r25 review F3): under READ COMMITTED a source transaction
-- stages its pk move, then a committed-object L0 compaction lands so the
-- tombstone transfer re-mints the move's pending delete onto the
-- compacted generation at the next statement boundary. The staged link
-- must ride the identical re-key, or the committed tombstone and the
-- link diverge and the merge replays DELETE/INSERT, failing the RESTRICT
-- child loudly. Post-fix the replay is one native UPDATE: parent
-- (3,'two') with the child following to pid 3.
set session transaction isolation level read committed;
create table etkf7_p (id int primary key, v varchar(32));
create table etkf7_c (
    id int primary key,
    pid int,
    constraint fk_etkf7 foreign key (pid) references etkf7_p(id)
    on update cascade
);
insert into etkf7_p values (2, 'two');
insert into etkf7_c values (100, 2);
data branch create table etkf7_s from etkf7_p;
select mo_ctl('dn','flush','branch_merge_fk_pk_move.etkf7_s');
insert into etkf7_s values (900, 'pad');
select mo_ctl('dn','flush','branch_merge_fk_pk_move.etkf7_s');
delete from etkf7_s where id = 900;
select mo_ctl('dn','flush','branch_merge_fk_pk_move.etkf7_s');
begin;
update etkf7_s set id = 3 where id = 2;
-- @ignore:0
select mo_ctl('dn','inspect','merge trigger -t branch_merge_fk_pk_move.etkf7_s --kind l0 --l0-oneshot') regexp 'trigger l0 merge for table [0-9]+-etkf7_s, oneshot: true' as l0_triggered;
-- @wait_expect(1, 60)
select count(*) as merged_objects from metadata_scan('branch_merge_fk_pk_move.etkf7_s', 'id') having count(*) = 1;
select count(*) as advance_snapshot from etkf7_p;
select id, v from etkf7_s;
commit;
set @etkf7l = (select rel_id from mo_catalog.mo_tables where relname = 'etkf7_s' and reldatabase = 'branch_merge_fk_pk_move');
select count(*) from mo_catalog.mo_data_branch_update_link where table_id = @etkf7l;
data branch merge etkf7_s into etkf7_p when conflict accept;
select id, v from etkf7_p order by id;
select id, pid from etkf7_c order by id;

-- Era-19 (r25 review F3), rollback twin: the same transfer land-then-
-- rollback. The transferred tombstone and the staged provenance die with
-- the transaction: no link row is recorded, the branch keeps its rows,
-- and the destination is untouched; a fresh move afterwards behaves like
-- plain tables, proving the aborted transfer wedged nothing.
create table etkf7r_p (id int primary key, v varchar(32));
create table etkf7r_c (
    id int primary key,
    pid int,
    constraint fk_etkf7r foreign key (pid) references etkf7r_p(id)
    on update cascade
);
insert into etkf7r_p values (2, 'two');
insert into etkf7r_c values (100, 2);
data branch create table etkf7r_s from etkf7r_p;
select mo_ctl('dn','flush','branch_merge_fk_pk_move.etkf7r_s');
insert into etkf7r_s values (900, 'pad');
select mo_ctl('dn','flush','branch_merge_fk_pk_move.etkf7r_s');
delete from etkf7r_s where id = 900;
select mo_ctl('dn','flush','branch_merge_fk_pk_move.etkf7r_s');
begin;
update etkf7r_s set id = 3 where id = 2;
-- @ignore:0
select mo_ctl('dn','inspect','merge trigger -t branch_merge_fk_pk_move.etkf7r_s --kind l0 --l0-oneshot') regexp 'trigger l0 merge for table [0-9]+-etkf7r_s, oneshot: true' as l0_triggered;
-- @wait_expect(1, 60)
select count(*) as merged_objects_r from metadata_scan('branch_merge_fk_pk_move.etkf7r_s', 'id') having count(*) = 1;
select count(*) as advance_snapshot_r from etkf7r_p;
rollback;
set @etkf7rl = (select rel_id from mo_catalog.mo_tables where relname = 'etkf7r_s' and reldatabase = 'branch_merge_fk_pk_move');
select count(*) from mo_catalog.mo_data_branch_update_link where table_id = @etkf7rl;
select id, v from etkf7r_s order by id;
select id, v from etkf7r_p order by id;
select id, pid from etkf7r_c order by id;
update etkf7r_s set id = 4 where id = 2;
data branch merge etkf7r_s into etkf7r_p when conflict accept;
select id, v from etkf7r_p order by id;
select id, pid from etkf7r_c order by id;
set session transaction isolation level repeatable read;

-- Era-20 (r26 review F2): composite PRIMARY KEY, edit-then-move within
-- ONE transaction. The source first edits the payload (a same-key
-- replacement into the workspace) and then moves the composite key. The
-- first replacement's workspace identity used to be invisible to the
-- provenance harvest (the duplicate-check position deliberately rejects
-- composite batches), so the later move replayed DELETE/INSERT and
-- failed the child foreign key. Post-fix the merge replays one native
-- UPDATE and the composite child follows (2,2).
create table etkf8_p (a int, b int, v varchar(32), primary key (a, b));
create table etkf8_c (
    id int primary key,
    pa int,
    pb int,
    constraint fk_etkf8 foreign key (pa, pb) references etkf8_p(a, b)
    on update cascade
);
insert into etkf8_p values (1, 1, 'one');
insert into etkf8_c values (100, 1, 1);
data branch create table etkf8_s from etkf8_p;
begin;
update etkf8_s set v = 'one2' where a = 1 and b = 1;
update etkf8_s set a = 2, b = 2 where a = 1 and b = 1;
commit;
set @etkf8l = (select rel_id from mo_catalog.mo_tables where relname = 'etkf8_s' and reldatabase = 'branch_merge_fk_pk_move');
select count(*) from mo_catalog.mo_data_branch_update_link where table_id = @etkf8l;
data branch merge etkf8_s into etkf8_p when conflict accept;
select a, b, v from etkf8_p order by a;
select id, pa, pb from etkf8_c order by id;

-- Era-20 (r26 review F2), spill twin: the same edit-then-composite-move
-- with the edit's workspace anchor spilled to S3 mid-transaction. The
-- move's provenance must survive the spill boundary either way.
create table etkf8s_p (a int, b int, v varchar(32), primary key (a, b));
create table etkf8s_c (
    id int primary key,
    pa int,
    pb int,
    constraint fk_etkf8s foreign key (pa, pb) references etkf8s_p(a, b)
    on update cascade
);
create table etkf8s_f (id int primary key, b int);
insert into etkf8s_p values (1, 1, 'one');
insert into etkf8s_c values (100, 1, 1);
insert into etkf8s_f values (1, 1);
data branch create table etkf8s_s from etkf8s_p;
begin;
update etkf8s_s set v = 'one2' where a = 1 and b = 1;
select enable_fault_injection();
select add_fault_point('fj/cn/workspace_force_flush',':::','echo',0,'');
update etkf8s_f set b = 9 where id = 1;
select remove_fault_point('fj/cn/workspace_force_flush');
select disable_fault_injection();
update etkf8s_s set a = 2, b = 2 where a = 1 and b = 1;
commit;
set @etkf8sl = (select rel_id from mo_catalog.mo_tables where relname = 'etkf8s_s' and reldatabase = 'branch_merge_fk_pk_move');
select count(*) from mo_catalog.mo_data_branch_update_link where table_id = @etkf8sl;
data branch merge etkf8s_s into etkf8s_p when conflict accept;
select a, b, v from etkf8s_p order by a;
select id, pa, pb from etkf8s_c order by id;

-- Era-20 (r26 review F3): PERSISTED tombstones through the RC transfer.
-- Arming the flush-small-objects route publishes the move's delete via
-- the S3 writer, so the pending tombstone lives in a written object (the
-- in-memory path never sees it). A committed-object L0 merge then lands
-- inside the transaction and the snapshot advance transfers that
-- persisted tombstone onto the compacted generation: the staged link must
-- ride the identical re-key, which pre-fix it did not — the merge lost
-- the move's provenance and aborted fail-closed. Post-fix the replay is
-- one native UPDATE: parent (3,'two'), child pid 3.
set session transaction isolation level read committed;
create table etkf9_p (id int primary key, v varchar(32));
create table etkf9_c (
    id int primary key,
    pid int,
    constraint fk_etkf9 foreign key (pid) references etkf9_p(id)
    on update cascade
);
insert into etkf9_p values (2, 'two');
insert into etkf9_c values (100, 2);
data branch create table etkf9_s from etkf9_p;
select mo_ctl('dn','flush','branch_merge_fk_pk_move.etkf9_s');
insert into etkf9_s values (900, 'pad');
select mo_ctl('dn','flush','branch_merge_fk_pk_move.etkf9_s');
delete from etkf9_s where id = 900;
select mo_ctl('dn','flush','branch_merge_fk_pk_move.etkf9_s');
begin;
select enable_fault_injection();
select add_fault_point('fj/cn/flush_small_objs',':::','echo',40,'branch_merge_fk_pk_move.etkf9_s');
update etkf9_s set id = 3 where id = 2;
select remove_fault_point('fj/cn/flush_small_objs');
select disable_fault_injection();
-- @ignore:0
select mo_ctl('dn','inspect','merge trigger -t branch_merge_fk_pk_move.etkf9_s --kind l0 --l0-oneshot') regexp 'trigger l0 merge for table [0-9]+-etkf9_s, oneshot: true' as l0_triggered;
-- @wait_expect(1, 60)
select count(*) as merged_objects_9 from metadata_scan('branch_merge_fk_pk_move.etkf9_s', 'id') having count(*) = 1;
select count(*) as advance_snapshot_9 from etkf9_p;
commit;
set @etkf9l = (select rel_id from mo_catalog.mo_tables where relname = 'etkf9_s' and reldatabase = 'branch_merge_fk_pk_move');
select count(*) from mo_catalog.mo_data_branch_update_link where table_id = @etkf9l;
data branch merge etkf9_s into etkf9_p when conflict accept;
select id, v from etkf9_p order by id;
select id, pid from etkf9_c order by id;

-- Era-20 (r26 review F3), rollback twin: the persisted tombstone
-- transfers, then the transaction rolls back. Transferred tombstone and
-- staged provenance die together: no link row is recorded, the branch
-- keeps its rows, the destination is untouched, and a fresh move
-- afterwards behaves like plain tables.
create table etkf9r_p (id int primary key, v varchar(32));
create table etkf9r_c (
    id int primary key,
    pid int,
    constraint fk_etkf9r foreign key (pid) references etkf9r_p(id)
    on update cascade
);
insert into etkf9r_p values (2, 'two');
insert into etkf9r_c values (100, 2);
data branch create table etkf9r_s from etkf9r_p;
select mo_ctl('dn','flush','branch_merge_fk_pk_move.etkf9r_s');
insert into etkf9r_s values (900, 'pad');
select mo_ctl('dn','flush','branch_merge_fk_pk_move.etkf9r_s');
delete from etkf9r_s where id = 900;
select mo_ctl('dn','flush','branch_merge_fk_pk_move.etkf9r_s');
begin;
select enable_fault_injection();
select add_fault_point('fj/cn/flush_small_objs',':::','echo',40,'branch_merge_fk_pk_move.etkf9r_s');
update etkf9r_s set id = 3 where id = 2;
select remove_fault_point('fj/cn/flush_small_objs');
select disable_fault_injection();
-- @ignore:0
select mo_ctl('dn','inspect','merge trigger -t branch_merge_fk_pk_move.etkf9r_s --kind l0 --l0-oneshot') regexp 'trigger l0 merge for table [0-9]+-etkf9r_s, oneshot: true' as l0_triggered;
-- @wait_expect(1, 60)
select count(*) as merged_objects_9r from metadata_scan('branch_merge_fk_pk_move.etkf9r_s', 'id') having count(*) = 1;
select count(*) as advance_snapshot_9r from etkf9r_p;
rollback;
set @etkf9rl = (select rel_id from mo_catalog.mo_tables where relname = 'etkf9r_s' and reldatabase = 'branch_merge_fk_pk_move');
select count(*) from mo_catalog.mo_data_branch_update_link where table_id = @etkf9rl;
select id, v from etkf9r_s order by id;
select id, v from etkf9r_p order by id;
select id, pid from etkf9r_c order by id;
update etkf9r_s set id = 4 where id = 2;
data branch merge etkf9r_s into etkf9r_p when conflict accept;
select id, v from etkf9r_p order by id;
select id, pid from etkf9r_c order by id;
set session transaction isolation level repeatable read;

-- Era-20 (r26 review F4): a workspace-route payload edit followed by an
-- S3-route payload edit binds the key's lineage to the S3 generation's
-- durable origin. When the RC snapshot advance transfers that persisted
-- tombstone onto the compacted generation, the BINDING must follow the
-- re-key, or a later move heals onto an identity the committed tombstones
-- no longer name and the merge replays DELETE/INSERT, failing the child
-- foreign key. Post-fix: one native UPDATE, parent (3,'two3'), child 3.
set session transaction isolation level read committed;
create table etkf10_p (id int primary key, v varchar(32));
create table etkf10_c (
    id int primary key,
    pid int,
    constraint fk_etkf10 foreign key (pid) references etkf10_p(id)
    on update cascade
);
insert into etkf10_p values (2, 'two');
insert into etkf10_c values (100, 2);
data branch create table etkf10_s from etkf10_p;
select mo_ctl('dn','flush','branch_merge_fk_pk_move.etkf10_s');
insert into etkf10_s values (900, 'pad');
select mo_ctl('dn','flush','branch_merge_fk_pk_move.etkf10_s');
delete from etkf10_s where id = 900;
select mo_ctl('dn','flush','branch_merge_fk_pk_move.etkf10_s');
begin;
update etkf10_s set v = 'two2' where id = 2;
select enable_fault_injection();
select add_fault_point('fj/cn/flush_small_objs',':::','echo',40,'branch_merge_fk_pk_move.etkf10_s');
update etkf10_s set v = 'two3' where id = 2;
select remove_fault_point('fj/cn/flush_small_objs');
select disable_fault_injection();
-- @ignore:0
select mo_ctl('dn','inspect','merge trigger -t branch_merge_fk_pk_move.etkf10_s --kind l0 --l0-oneshot') regexp 'trigger l0 merge for table [0-9]+-etkf10_s, oneshot: true' as l0_triggered;
-- @wait_expect(1, 60)
select count(*) as merged_objects_10 from metadata_scan('branch_merge_fk_pk_move.etkf10_s', 'id') having count(*) = 1;
select count(*) as advance_snapshot_10 from etkf10_p;
update etkf10_s set id = 3 where id = 2;
commit;
set @etkf10l = (select rel_id from mo_catalog.mo_tables where relname = 'etkf10_s' and reldatabase = 'branch_merge_fk_pk_move');
select count(*) from mo_catalog.mo_data_branch_update_link where table_id = @etkf10l;
data branch merge etkf10_s into etkf10_p when conflict accept;
select id, v from etkf10_p order by id;
select id, pid from etkf10_c order by id;

-- Era-20 (r26 review F3 residual, D113): a persisted (S3-route) tombstone
-- transfer must replace its input objects ATOMICALLY. The armed move stages
-- a file-backed tombstone; the L0 oneshot re-mints the old row's committed
-- object, and the RC transfer re-keys the pending tombstone onto the new
-- identity while carrying the link. The superseded source object must never
-- publish beside that replacement: pre-D113 both committed, so the anchor
-- key surfaced two tombstones, the chain evidence failed closed unresolved,
-- and the merge died with ERROR 20101. The move now applies as one native
-- UPDATE with the child following to pid 3.
set session transaction isolation level read committed;
create table etkf11_p (id int primary key, v varchar(32));
create table etkf11_c (
    id int primary key,
    pid int,
    constraint fk_etkf11 foreign key (pid) references etkf11_p(id)
    on update cascade
);
insert into etkf11_p values (2, 'two');
insert into etkf11_c values (100, 2);
data branch create table etkf11_s from etkf11_p;
select mo_ctl('dn','flush','branch_merge_fk_pk_move.etkf11_s');
insert into etkf11_s values (900, 'pad');
select mo_ctl('dn','flush','branch_merge_fk_pk_move.etkf11_s');
delete from etkf11_s where id = 900;
select mo_ctl('dn','flush','branch_merge_fk_pk_move.etkf11_s');
begin;
select enable_fault_injection();
select add_fault_point('fj/cn/flush_small_objs',':::','echo',40,'branch_merge_fk_pk_move.etkf11_s');
update etkf11_s set id = 3 where id = 2;
select remove_fault_point('fj/cn/flush_small_objs');
select disable_fault_injection();
-- @ignore:0
select mo_ctl('dn','inspect','merge trigger -t branch_merge_fk_pk_move.etkf11_s --kind l0 --l0-oneshot') regexp 'trigger l0 merge for table [0-9]+-etkf11_s, oneshot: true' as l0_triggered;
-- @wait_expect(1, 60)
select count(*) as merged_objects_8 from metadata_scan('branch_merge_fk_pk_move.etkf11_s', 'id') having count(*) = 1;
select count(*) as advance_snapshot_8 from etkf11_p;
select id, v from etkf11_s;
commit;
set @etkf11l = (select rel_id from mo_catalog.mo_tables where relname = 'etkf11_s' and reldatabase = 'branch_merge_fk_pk_move');
select count(*) from mo_catalog.mo_data_branch_update_link where table_id = @etkf11l;
data branch merge etkf11_s into etkf11_p when conflict accept;
select id, v from etkf11_p order by id;
select id, pid from etkf11_c order by id;

-- Era-20 (D113), rollback twin: the same persisted transfer fires inside the
-- txn, then the txn rolls back. The retirement ledger dies with it — a full
-- rollback resurrects nothing and close-time cleanup collects the retired
-- files exactly once: no link rows survive, the branch keeps its rows, and
-- the destination never moved; a fresh move afterwards applies and cascades,
-- proving the aborted transfer wedged nothing.
create table etkf11r_p (id int primary key, v varchar(32));
create table etkf11r_c (
    id int primary key,
    pid int,
    constraint fk_etkf11r foreign key (pid) references etkf11r_p(id)
    on update cascade
);
insert into etkf11r_p values (2, 'two');
insert into etkf11r_c values (100, 2);
data branch create table etkf11r_s from etkf11r_p;
select mo_ctl('dn','flush','branch_merge_fk_pk_move.etkf11r_s');
insert into etkf11r_s values (900, 'pad');
select mo_ctl('dn','flush','branch_merge_fk_pk_move.etkf11r_s');
delete from etkf11r_s where id = 900;
select mo_ctl('dn','flush','branch_merge_fk_pk_move.etkf11r_s');
begin;
select enable_fault_injection();
select add_fault_point('fj/cn/flush_small_objs',':::','echo',40,'branch_merge_fk_pk_move.etkf11r_s');
update etkf11r_s set id = 3 where id = 2;
select remove_fault_point('fj/cn/flush_small_objs');
select disable_fault_injection();
-- @ignore:0
select mo_ctl('dn','inspect','merge trigger -t branch_merge_fk_pk_move.etkf11r_s --kind l0 --l0-oneshot') regexp 'trigger l0 merge for table [0-9]+-etkf11r_s, oneshot: true' as l0_triggered;
-- @wait_expect(1, 60)
select count(*) as merged_objects_8r from metadata_scan('branch_merge_fk_pk_move.etkf11r_s', 'id') having count(*) = 1;
select count(*) as advance_snapshot_8r from etkf11r_p;
rollback;
set @etkf11rl = (select rel_id from mo_catalog.mo_tables where relname = 'etkf11r_s' and reldatabase = 'branch_merge_fk_pk_move');
select count(*) from mo_catalog.mo_data_branch_update_link where table_id = @etkf11rl;
select id, v from etkf11r_s order by id;
select id, v from etkf11r_p order by id;
select id, pid from etkf11r_c order by id;
update etkf11r_s set id = 4 where id = 2;
data branch merge etkf11r_s into etkf11r_p when conflict accept;
select id, v from etkf11r_p order by id;
select id, pid from etkf11r_c order by id;

-- Era-21 (r27 review F1): TWO qualifying persisted-tombstone transfer
-- windows inside ONE explicit transaction, with a statement rollback held
-- between them. Each window arms TWO anchors so its L0 tombstone oneshot
-- has enough candidates to merge for real; window A's transfer retires the
-- staged tombstone entries it consumed (D113 leaves bat=nil shells that
-- still carry the fileName), and window B's persisted-DELETE scan then
-- crosses those retired slots. Pre-fix the collector dereferenced
-- e.bat.Vecs[0] on the nil batch and died; post-fix it skips them (D114),
-- so the commit succeeds and every moved key publishes its link exactly
-- once. The duplicate insert between the windows re-inserts armed key 12
-- as the statement-rollback vehicle (ERROR 1062 captured by genrs); window
-- B must still survive reconciliation against the retired slots afterwards.
set session transaction isolation level read committed;
create table etkf12_p (id int primary key, v varchar(32));
create table etkf12_c (
    id int primary key,
    pid int,
    constraint fk_etkf12 foreign key (pid) references etkf12_p(id)
    on update cascade
);
insert into etkf12_p values (2, 'two'), (3, 'three'), (4, 'four'), (5, 'five');
insert into etkf12_c values (100, 2), (200, 3), (300, 4), (400, 5);
data branch create table etkf12_s from etkf12_p;
select mo_ctl('dn','flush','branch_merge_fk_pk_move.etkf12_s');
insert into etkf12_s values (900, 'pad');
select mo_ctl('dn','flush','branch_merge_fk_pk_move.etkf12_s');
delete from etkf12_s where id = 900;
select mo_ctl('dn','flush','branch_merge_fk_pk_move.etkf12_s');
begin;
select enable_fault_injection();
select add_fault_point('fj/cn/flush_small_objs',':::','echo',40,'branch_merge_fk_pk_move.etkf12_s');
update etkf12_s set id = 12 where id = 2;
update etkf12_s set id = 13 where id = 3;
select remove_fault_point('fj/cn/flush_small_objs');
select disable_fault_injection();
-- @ignore:0
select mo_ctl('dn','inspect','merge trigger -t branch_merge_fk_pk_move.etkf12_s --kind l0 --l0-oneshot') regexp 'trigger l0 merge for table [0-9]+-etkf12_s, oneshot: true' as l0_triggered;
-- @wait_expect(1, 60)
select count(*) as merged_objects_12a from metadata_scan('branch_merge_fk_pk_move.etkf12_s', 'id') having count(*) = 1;
select count(*) as advance_snapshot_12a from etkf12_p;
insert into etkf12_s values (12, 'dupe');
select enable_fault_injection();
select add_fault_point('fj/cn/flush_small_objs',':::','echo',40,'branch_merge_fk_pk_move.etkf12_s');
update etkf12_s set id = 14 where id = 4;
update etkf12_s set id = 15 where id = 5;
select remove_fault_point('fj/cn/flush_small_objs');
select disable_fault_injection();
-- @ignore:0
select mo_ctl('dn','inspect','merge trigger -t branch_merge_fk_pk_move.etkf12_s --kind l0 --l0-oneshot') regexp 'trigger l0 merge for table [0-9]+-etkf12_s, oneshot: true' as l0_triggered;
-- @wait_expect(1, 60)
select count(*) as merged_objects_12b from metadata_scan('branch_merge_fk_pk_move.etkf12_s', 'id') having count(*) = 1;
select count(*) as advance_snapshot_12b from etkf12_p;
select id, v from etkf12_s order by id;
commit;
set @etkf12l = (select rel_id from mo_catalog.mo_tables where relname = 'etkf12_s' and reldatabase = 'branch_merge_fk_pk_move');
select count(*) from mo_catalog.mo_data_branch_update_link where table_id = @etkf12l;
data branch merge etkf12_s into etkf12_p when conflict accept;
select id, v from etkf12_p order by id;
select id, pid from etkf12_c order by id;
set session transaction isolation level repeatable read;

-- Era-21 (r47 D154-A): the etkf2 reorder twin. The retained user
-- snapshot is dropped BETWEEN the historical branch-create and the
-- apply. The owner-removal reap re-evaluates coverage, finds none, but
-- the live lineage descendant carries the lineage (ownership transfer):
-- the dropped member's link row and its retention ledger row both stay.
-- The descendant merge then folds the move as one native UPDATE with
-- the dual cascade; retiring the final descendant discharges both rows
-- with no owner at all.
create table etkf2o_p (id int primary key, v varchar(32));
create table etkf2o_c (
    id int primary key,
    pid int,
    constraint fk_etkf2o foreign key (pid) references etkf2o_p(id)
    on update cascade on delete cascade
);
insert into etkf2o_p values (2, 'two');
insert into etkf2o_c values (1, 2);
data branch create table etkf2o_b from etkf2o_p;
update etkf2o_b set id = 3 where id = 2;
set @etkf2ob = (select rel_id from mo_catalog.mo_tables where relname = 'etkf2o_b' and reldatabase = 'branch_merge_fk_pk_move');
drop snapshot if exists etkf2o_snap;
create snapshot etkf2o_snap for table branch_merge_fk_pk_move etkf2o_b;
drop table etkf2o_b;
data branch create table etkf2o_h from etkf2o_b{snapshot="etkf2o_snap"};
drop snapshot etkf2o_snap;
select count(*) from mo_catalog.mo_data_branch_update_link where table_id = @etkf2ob;
select count(*) from mo_catalog.mo_data_branch_link_retention where table_id = @etkf2ob;
data branch merge etkf2o_h into etkf2o_p when conflict accept;
select id, v from etkf2o_p order by id;
select id, pid from etkf2o_c order by id;
drop table etkf2o_h;
select count(*) from mo_catalog.mo_data_branch_update_link where table_id = @etkf2ob;
select count(*) from mo_catalog.mo_data_branch_link_retention where table_id = @etkf2ob;

-- Era-21: RESTRICT reorder twin. Same shape without ON UPDATE actions:
-- the folded parent-key move trips the foreign key, the merge aborts
-- atomically leaving both tables byte-identical, and retiring the
-- descendant discharges the retention rows.
create table etkf2or_p (id int primary key, v varchar(32));
create table etkf2or_c (
    id int primary key,
    pid int,
    constraint fk_etkf2or foreign key (pid) references etkf2or_p(id)
);
insert into etkf2or_p values (2, 'two');
insert into etkf2or_c values (1, 2);
data branch create table etkf2or_b from etkf2or_p;
update etkf2or_b set id = 3 where id = 2;
set @etkf2orb = (select rel_id from mo_catalog.mo_tables where relname = 'etkf2or_b' and reldatabase = 'branch_merge_fk_pk_move');
drop snapshot if exists etkf2or_snap;
create snapshot etkf2or_snap for table branch_merge_fk_pk_move etkf2or_b;
drop table etkf2or_b;
data branch create table etkf2or_h from etkf2or_b{snapshot="etkf2or_snap"};
drop snapshot etkf2or_snap;
select count(*) from mo_catalog.mo_data_branch_update_link where table_id = @etkf2orb;
data branch merge etkf2or_h into etkf2or_p when conflict accept;
select id, v from etkf2or_p order by id;
select id, pid from etkf2or_c order by id;
drop table etkf2or_h;
select count(*) from mo_catalog.mo_data_branch_update_link where table_id = @etkf2orb;
select count(*) from mo_catalog.mo_data_branch_link_retention where table_id = @etkf2orb;

-- Era-21 (r48 D155-A): sys-owned owner variant. A cluster snapshot's
-- row lives only in SYS's mo_snapshots with level='cluster'; it grounds
-- a retired branch member exactly like an account-local owner (the
-- account-local probe alone cannot see it). Removing it reaps.
create table etkf2oc_p (id int primary key, v varchar(32));
create table etkf2oc_c (
    id int primary key,
    pid int,
    constraint fk_etkf2oc foreign key (pid) references etkf2oc_p(id)
    on update cascade on delete cascade
);
insert into etkf2oc_p values (2, 'two');
insert into etkf2oc_c values (1, 2);
data branch create table etkf2oc_b from etkf2oc_p;
update etkf2oc_b set id = 3 where id = 2;
set @etkf2ocb = (select rel_id from mo_catalog.mo_tables where relname = 'etkf2oc_b' and reldatabase = 'branch_merge_fk_pk_move');
drop snapshot if exists etkf2oc_snap;
create snapshot etkf2oc_snap for cluster;
drop table etkf2oc_b;
select count(*) from mo_catalog.mo_data_branch_update_link where table_id = @etkf2ocb;
select count(*) from mo_catalog.mo_data_branch_link_retention where table_id = @etkf2ocb;
drop snapshot etkf2oc_snap;
select count(*) from mo_catalog.mo_data_branch_update_link where table_id = @etkf2ocb;
select count(*) from mo_catalog.mo_data_branch_link_retention where table_id = @etkf2ocb;

-- Era-21 (r48 D155-B account scope): a sys-created account snapshot for
-- account sys grounds the member through its account_name mapping; its
-- removal reaps the rows.
create table etkf2oa_p (id int primary key, v varchar(32));
create table etkf2oa_c (
    id int primary key,
    pid int,
    constraint fk_etkf2oa foreign key (pid) references etkf2oa_p(id)
    on update cascade on delete cascade
);
insert into etkf2oa_p values (2, 'two');
insert into etkf2oa_c values (1, 2);
data branch create table etkf2oa_b from etkf2oa_p;
update etkf2oa_b set id = 3 where id = 2;
set @etkf2oab = (select rel_id from mo_catalog.mo_tables where relname = 'etkf2oa_b' and reldatabase = 'branch_merge_fk_pk_move');
drop snapshot if exists etkf2oa_snap;
create snapshot etkf2oa_snap for account sys;
drop table etkf2oa_b;
select count(*) from mo_catalog.mo_data_branch_update_link where table_id = @etkf2oab;
select count(*) from mo_catalog.mo_data_branch_link_retention where table_id = @etkf2oab;
drop snapshot etkf2oa_snap;
select count(*) from mo_catalog.mo_data_branch_update_link where table_id = @etkf2oab;
select count(*) from mo_catalog.mo_data_branch_link_retention where table_id = @etkf2oab;

-- Era-22 (r50 P1): database-level owner vs the orphan-parent KEEP row.
-- A branch-side key move folds into the parent as one native UPDATE,
-- which emits the parent's own carried link; a DATABASE-scope snapshot
-- grounds the whole namespace. Retiring the last branch then runs the
-- orphan-parent funnel, which persists the parent's KEEP row WITH its
-- resolved identity (pre-fix it recorded an empty-name residue row).
-- Dropping an unrelated owner fires the owner-removal reap: the residue
-- row could never match name-keyed database coverage and had no live
-- descendant, so the pre-fix reap deleted the parent's provenance
-- although the database-level owner still grounds it. The fixed funnel
-- keeps link and row; a historical clone still folds, and dropping the
-- owner discharges the rows.
create table etkf3o_p (id int primary key, v varchar(32));
create table etkf3o_c (
    id int primary key,
    pid int,
    constraint fk_etkf3o foreign key (pid) references etkf3o_p(id)
    on update cascade on delete cascade
);
insert into etkf3o_p values (2, 'two');
insert into etkf3o_c values (1, 2);
data branch create table etkf3o_b from etkf3o_p;
data branch create table etkf3o_x from etkf3o_p;
update etkf3o_b set id = 3 where id = 2;
set @etkf3op = (select rel_id from mo_catalog.mo_tables where relname = 'etkf3o_p' and reldatabase = 'branch_merge_fk_pk_move');
set @etkf3ob = (select rel_id from mo_catalog.mo_tables where relname = 'etkf3o_b' and reldatabase = 'branch_merge_fk_pk_move');
set @etkf3ox = (select rel_id from mo_catalog.mo_tables where relname = 'etkf3o_x' and reldatabase = 'branch_merge_fk_pk_move');
data branch merge etkf3o_b into etkf3o_p when conflict accept;
select id, v from etkf3o_p order by id;
select id, pid from etkf3o_c order by id;
create snapshot etkf3o_snap for database branch_merge_fk_pk_move;
drop table etkf3o_b;
drop table etkf3o_x;
select count(*) from mo_catalog.mo_data_branch_update_link where table_id = @etkf3op;
select count(*) from mo_catalog.mo_data_branch_link_retention where table_id = @etkf3op;
select count(*) from mo_catalog.mo_data_branch_link_retention where table_id = @etkf3ob;
select count(*) from mo_catalog.mo_data_branch_link_retention where table_id = @etkf3ox;
create table etkf3o_n (id int primary key);
drop snapshot if exists etkf3o_noise;
create snapshot etkf3o_noise for table branch_merge_fk_pk_move etkf3o_n;
drop snapshot etkf3o_noise;
select count(*) from mo_catalog.mo_data_branch_update_link where table_id = @etkf3op;
select count(*) from mo_catalog.mo_data_branch_link_retention where table_id = @etkf3op;
select count(*) from mo_catalog.mo_data_branch_link_retention where table_id = @etkf3ob;
select count(*) from mo_catalog.mo_data_branch_link_retention where table_id = @etkf3ox;
data branch create table etkf3o_h from etkf3o_x{snapshot="etkf3o_snap"};
data branch merge etkf3o_h into etkf3o_p when conflict accept;
select id, v from etkf3o_p order by id;
select id, pid from etkf3o_c order by id;
select count(*) from mo_catalog.mo_data_branch_update_link where table_id = @etkf3op;
drop table etkf3o_h;
drop snapshot etkf3o_snap;
select count(*) from mo_catalog.mo_data_branch_update_link where table_id = @etkf3op;
select count(*) from mo_catalog.mo_data_branch_link_retention where table_id = @etkf3op;
select count(*) from mo_catalog.mo_data_branch_link_retention where table_id = @etkf3ob;
select count(*) from mo_catalog.mo_data_branch_link_retention where table_id = @etkf3ox;

drop snapshot if exists etkf2_snap;
drop snapshot if exists etkf2r_snap;
drop snapshot if exists esy_s1;
drop snapshot if exists esy_s2;
drop snapshot if exists esyx_s1;
drop snapshot if exists esyx_s2;
drop snapshot if exists esyr_s1;
drop snapshot if exists esyr_s2;
drop snapshot if exists esy3_s;

-- era-24 dlp_: competing lineages may never claim one replacement (pre-PR
-- review F1). One source transaction deletes the key-1 parent and moves
-- key 2's parent into key 1, the next moves it on to key 3. The anchor at
-- key 1 must not fold (its version arrived by a recorded 2->1 move; its
-- LCA lineage is key 2's), so the destination keeps exactly the lineage
-- the source holds: parent (3,'A'), child (20,3). The native control runs
-- the same statements directly.
create table dlp_p (id int primary key, v varchar(16));
create table dlp_c (id int primary key, pid int,
    constraint fk_dlp foreign key (pid) references dlp_p(id) on update cascade on delete cascade);
insert into dlp_p values (1,'B'),(2,'A');
insert into dlp_c values (10,1),(20,2);
data branch create table dlp_src from dlp_p;
begin;
delete from dlp_src where id=1;
update dlp_src set id=1 where id=2;
commit;
update dlp_src set id=3 where id=1;
data branch merge dlp_src into dlp_p when conflict accept;
select id, v from dlp_p order by id;
select id, pid from dlp_c order by id;

create table dlp_np (id int primary key, v varchar(16));
create table dlp_nc (id int primary key, pid int,
    constraint fk_dlpn foreign key (pid) references dlp_np(id) on update cascade on delete cascade);
insert into dlp_np values (1,'B'),(2,'A');
insert into dlp_nc values (10,1),(20,2);
begin;
delete from dlp_np where id=1;
update dlp_np set id=1 where id=2;
commit;
update dlp_np set id=3 where id=1;
select id, v from dlp_np order by id;
select id, pid from dlp_nc order by id;

-- era-24 dlpm_: reversed key ordering (delete the higher key, move the
-- lower key up into it). The mirror must land the same correct rows, not
-- accidentally-correct rows from fold pop ordering.
create table dlpm_p (id int primary key, v varchar(16));
create table dlpm_c (id int primary key, pid int,
    constraint fk_dlpm foreign key (pid) references dlpm_p(id) on update cascade on delete cascade);
insert into dlpm_p values (1,'B'),(2,'A');
insert into dlpm_c values (10,1),(20,2);
data branch create table dlpm_src from dlpm_p;
begin;
delete from dlpm_src where id=2;
update dlpm_src set id=2 where id=1;
commit;
update dlpm_src set id=3 where id=2;
data branch merge dlpm_src into dlpm_p when conflict accept;
select id, v from dlpm_p order by id;
select id, pid from dlpm_c order by id;

-- era-25 (r53 P1): a same-transaction arrival CHAIN whose intermediate
-- key is netted out of the committed stream. One source transaction
-- deletes key 1 and moves 2 -> 4 -> 1 (key 4 never commits a version),
-- the next moves 1 -> 3. The standing version at 1 descends from the
-- key-2 lineage, provably attributed through the recorded chain, so the
-- key-1 anchor keeps its own delete semantics and MERGE matches the
-- native control: parent (3,'A') with child (20,3).
create table clp_p (id int primary key, v varchar(16));
create table clp_c (id int primary key, pid int,
    constraint fk_clp foreign key (pid) references clp_p(id) on update cascade on delete cascade);
insert into clp_p values (1,'B'),(2,'A');
insert into clp_c values (10,1),(20,2);
data branch create table clp_src from clp_p;
begin;
delete from clp_src where id=1;
update clp_src set id=4 where id=2;
update clp_src set id=1 where id=4;
commit;
update clp_src set id=3 where id=1;
data branch merge clp_src into clp_p when conflict accept;
select id, v from clp_p order by id;
select id, pid from clp_c order by id;

-- Native control for the same statement sequence: identical rows prove
-- the branch apply preserved native cascade semantics.
create table clnp_p (id int primary key, v varchar(16));
create table clnp_c (id int primary key, pid int,
    constraint fk_clnp foreign key (pid) references clnp_p(id) on update cascade on delete cascade);
insert into clnp_p values (1,'B'),(2,'A');
insert into clnp_c values (10,1),(20,2);
begin;
delete from clnp_p where id=1;
update clnp_p set id=4 where id=2;
update clnp_p set id=1 where id=4;
commit;
update clnp_p set id=3 where id=1;
select id, v from clnp_p order by id;
select id, pid from clnp_c order by id;

-- Reversed-key mirror: delete key 2, move 1 -> 4 -> 2 in one
-- transaction, then 2 -> 3. The B lineage lands at 3 with its child,
-- the A lineage is deleted by its own declared action.
create table clmp_p (id int primary key, v varchar(16));
create table clmp_c (id int primary key, pid int,
    constraint fk_clmp foreign key (pid) references clmp_p(id) on update cascade on delete cascade);
insert into clmp_p values (1,'B'),(2,'A');
insert into clmp_c values (10,1),(20,2);
data branch create table clmp_src from clmp_p;
begin;
delete from clmp_src where id=2;
update clmp_src set id=4 where id=1;
update clmp_src set id=2 where id=4;
commit;
update clmp_src set id=3 where id=2;
data branch merge clmp_src into clmp_p when conflict accept;
select id, v from clmp_p order by id;
select id, pid from clmp_c order by id;

-- era-26 (r54 F2): the shared-temporary-key shape through MERGE: A's
-- chain 2->4->1->3 and C's chain 5->4->6 share netted key 4 inside one
-- source transaction. Exact workspace identity binds each chain, so both
-- lineages fold natively, matching the native control below exactly.
create table ctp_p (id int primary key, v varchar(16));
create table ctp_c (id int primary key, pid int,
    constraint fk_ctp foreign key (pid) references ctp_p(id) on update cascade on delete cascade);
insert into ctp_p values (1,'B'),(2,'A'),(5,'C');
insert into ctp_c values (10,1),(20,2),(30,5);
data branch create table ctp_src from ctp_p;
begin;
delete from ctp_src where id=1;
update ctp_src set id=4 where id=2;
update ctp_src set id=1 where id=4;
update ctp_src set id=4 where id=5;
update ctp_src set id=6 where id=4;
commit;
update ctp_src set id=3 where id=1;
data branch merge ctp_src into ctp_p when conflict accept;
select id, v from ctp_p order by id;
select id, pid from ctp_c order by id;

-- Native control for the shared temporary key: identical statement
-- sequence without the branch, proving the apply preserved native
-- cascade semantics exactly.
create table ctnp_p (id int primary key, v varchar(16));
create table ctnp_c (id int primary key, pid int,
    constraint fk_ctnp foreign key (pid) references ctnp_p(id) on update cascade on delete cascade);
insert into ctnp_p values (1,'B'),(2,'A'),(5,'C');
insert into ctnp_c values (10,1),(20,2),(30,5);
begin;
delete from ctnp_p where id=1;
update ctnp_p set id=4 where id=2;
update ctnp_p set id=1 where id=4;
update ctnp_p set id=4 where id=5;
update ctnp_p set id=6 where id=4;
commit;
update ctnp_p set id=3 where id=1;
select id, v from ctnp_p order by id;
select id, pid from ctnp_c order by id;

-- r58 review F1 (merge twin of the pick regression): the same
-- terminated-lineage-vs-foreign-arrival shape. The merge must replay
-- A's terminal delete (child 10 cascades away) and B's native move
-- 3->2 (child 30 re-parents) instead of aborting on competing claims.
create table mta_p (id int primary key, v varchar(16));
create table mta_c (id int primary key, pid int,
    constraint fk_mta foreign key (pid) references mta_p(id) on update cascade on delete cascade);
insert into mta_p values (1,'A'),(3,'B');
insert into mta_c values (10,1),(30,3);
data branch create table mta_s from mta_p;
update mta_s set id=2 where id=1;
begin;
delete from mta_s where id=2;
update mta_s set id=2 where id=3;
commit;
data branch merge mta_s into mta_p when conflict accept;
select id, v from mta_p order by id;
select id, pid from mta_c order by id;

-- r58 review F2: a long same-transaction chain committed BEFORE the
-- destination fork must not poison arrival evidence for the in-window
-- instants that follow. One transaction moves the row 1->...->41 (40
-- hops, over the walk's own chain cap); the destination branch forks;
-- the source then commits a payload edit at 41 and a one-hop move
-- 41->42, both with complete in-window evidence. The merge must apply
-- them natively, cascading the destination child onto key 42.
create table hpo_p (id int primary key, v varchar(16));
insert into hpo_p values (1,'seed');
data branch create table hpo_s from hpo_p;
begin;
update hpo_s set id=2 where id=1;
update hpo_s set id=3 where id=2;
update hpo_s set id=4 where id=3;
update hpo_s set id=5 where id=4;
update hpo_s set id=6 where id=5;
update hpo_s set id=7 where id=6;
update hpo_s set id=8 where id=7;
update hpo_s set id=9 where id=8;
update hpo_s set id=10 where id=9;
update hpo_s set id=11 where id=10;
update hpo_s set id=12 where id=11;
update hpo_s set id=13 where id=12;
update hpo_s set id=14 where id=13;
update hpo_s set id=15 where id=14;
update hpo_s set id=16 where id=15;
update hpo_s set id=17 where id=16;
update hpo_s set id=18 where id=17;
update hpo_s set id=19 where id=18;
update hpo_s set id=20 where id=19;
update hpo_s set id=21 where id=20;
update hpo_s set id=22 where id=21;
update hpo_s set id=23 where id=22;
update hpo_s set id=24 where id=23;
update hpo_s set id=25 where id=24;
update hpo_s set id=26 where id=25;
update hpo_s set id=27 where id=26;
update hpo_s set id=28 where id=27;
update hpo_s set id=29 where id=28;
update hpo_s set id=30 where id=29;
update hpo_s set id=31 where id=30;
update hpo_s set id=32 where id=31;
update hpo_s set id=33 where id=32;
update hpo_s set id=34 where id=33;
update hpo_s set id=35 where id=34;
update hpo_s set id=36 where id=35;
update hpo_s set id=37 where id=36;
update hpo_s set id=38 where id=37;
update hpo_s set id=39 where id=38;
update hpo_s set id=40 where id=39;
update hpo_s set id=41 where id=40;
commit;
data branch create table hpo_d from hpo_s;
create table hpo_c (id int primary key, pid int,
    constraint fk_hpo foreign key (pid) references hpo_d(id) on update cascade);
insert into hpo_c values (900,41);
update hpo_s set v='edited' where id=41;
update hpo_s set id=42 where id=41;
data branch merge hpo_s into hpo_d when conflict accept;
select id, v from hpo_d order by id;
select id, pid from hpo_c order by id;


-- xr1 (issue #28026 r53): a pk move at T1 followed by an explicit
-- delete+insert of the moved key in one transaction at T2 folds as a
-- stable-key update of the landed row: the re-birth shares the deleting
-- transaction's commit instant, so the cross-transaction re-birth screen
-- resolves through the recorded arrival evidence instead of aborting.
create table xr1_p (id int primary key, v varchar(16));
create table xr1_c (id int primary key, pid int,
    constraint fk_xr1 foreign key (pid) references xr1_p(id) on update cascade);
insert into xr1_p values (1,'orig');
insert into xr1_c values (900,1);
data branch create table xr1_s from xr1_p;
update xr1_s set id=3 where id=1;
begin;
delete from xr1_s where id=3;
insert into xr1_s values (3,'reborn');
commit;
data branch merge xr1_s into xr1_p when conflict accept;
select id, v from xr1_p order by id;
select id, pid from xr1_c order by id;

-- xr2 (issue #28026 r53): a pk move at T1 followed by a payload edit at
-- T2 folds with the edit intact and the declared cascade honored.
create table xr2_p (id int primary key, v varchar(16));
create table xr2_c (id int primary key, pid int,
    constraint fk_xr2 foreign key (pid) references xr2_p(id) on update cascade);
insert into xr2_p values (1,'orig');
insert into xr2_c values (900,1);
data branch create table xr2_s from xr2_p;
update xr2_s set id=3 where id=1;
update xr2_s set v='edited' where id=3;
data branch merge xr2_s into xr2_p when conflict accept;
select id, v from xr2_p order by id;
select id, pid from xr2_c order by id;

drop database branch_merge_fk_pk_move;

-- Restore the default isolation this file replaced above: mo-tester
-- reuses the default connection across files, and an inherited
-- repeatable-read session makes later lifecycle statements fail the
-- pessimistic-RC admission gates.
set session transaction isolation level read committed;

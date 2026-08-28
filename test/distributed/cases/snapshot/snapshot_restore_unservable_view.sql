-- A snapshot holding a view whose MATCH() no FULLTEXT index can serve must still restore.
--
-- Since #27027 such a definition is refused at CREATE, but views created before that guard
-- existed are exactly the population the issue describes, so a snapshot may legitimately
-- contain one. Restore replays the stored CREATE VIEW, and it does so TWICE: a dependency
-- toposort plans every view to discover inter-view edges before the loop that creates them.
-- The refusal therefore surfaces in the sort, not at create time, and an earlier fix that
-- guarded only the create loop still aborted the entire restore -- with the view already
-- dropped, leaving the account unrecoverable since the definition lives only in the snapshot.
--
-- Everything runs inside a dedicated account so the restore is self-contained: restoring at
-- snapshot scope rebuilds the account, which would be destructive against sys.
create account acc_ftview admin_name = 'test_account' identified by '111';

-- @session:id=1&user=acc_ftview:test_account&password=111
set experimental_fulltext_index = 1;
-- Pinned inside THIS session: the pin must live where the fulltext queries run, not in the
-- outer sys session.
set ft_relevancy_algorithm="TF-IDF";
create database ftv;
use ftv;
create table docs(id int primary key, body text);
insert into docs values (1,'hello a'),(2,'other'),(3,'hello b');

-- a view that is servable when created ...
create fulltext index ft on docs(body);
create view v_ft as select id from docs where match(body) against('hello');
select id from v_ft order by id;

-- ... and an ordinary view that must survive the restore
create view v_plain as select id, body from docs;

-- dropping the index leaves v_ft stored but unrunnable: the same end state as a view
-- created before the guard existed
drop index ft on docs;
select count(*) as plain_rows_before from v_plain;
-- @session

create snapshot sp_ftview for account acc_ftview;

-- @session:id=1&user=acc_ftview:test_account&password=111
insert into ftv.docs values (4,'hello d');
select count(*) as docs_rows_before_restore from ftv.docs;
-- @session

-- the restore must SUCCEED, not abort on the unrunnable view
restore account acc_ftview{snapshot="sp_ftview"};

-- @session:id=1&user=acc_ftview:test_account&password=111
-- the ordinary view and the table data came back
select count(*) as plain_rows_after from ftv.v_plain;
select count(*) as docs_rows_after from ftv.docs;
-- restore the default in the session that changed it
set ft_relevancy_algorithm="TF-IDF";
-- @session

drop snapshot sp_ftview;
drop account acc_ftview;

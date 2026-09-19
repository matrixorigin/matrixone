-- Regression for #28494: FULLTEXT ODKU maintenance must compare the final
-- stored VARCHAR/TEXT bytes, not the SQL comparison identity. The current
-- mainline SQL comparator is bytewise; the token-changing values below keep
-- this test valid today and catch a future collation-aware comparator that
-- would otherwise skip the maintenance branch.

-- @suite
-- @case
-- @desc: FULLTEXT ODKU uses stored-value identity
-- @label:bvt
set experimental_fulltext_index = 1;
drop database if exists issue28494_fulltext;
create database issue28494_fulltext;
use issue28494_fulltext;

create table docs(
    id int primary key,
    body varchar(256) character set utf8mb4 collate utf8mb4_general_ci,
    notes text character set utf8mb4 collate utf8mb4_general_ci,
    payload int
);
insert into docs values (1, 'Résumé alpha', 'noteold', 1);
create fulltext index ft_body on docs(body);
create fulltext index ft_notes on docs(notes);

select id, length(body) as stored_len, length(notes) as notes_len from docs;
select id from docs where match(body) against('résumé') order by id;
select id from docs where match(notes) against('noteold') order by id;
insert into docs values (1, 'resume alpha', 'notenew', 2)
    on duplicate key update body = values(body), notes = 'notenew', payload = values(payload);
select id, body, notes, payload from docs order by id;
select id from docs where match(body) against('resume') order by id;
select id from docs where match(body) against('résumé') order by id;
select id from docs where match(notes) against('notenew') order by id;
select id from docs where match(notes) against('noteold') order by id;

-- Case-only and PAD SPACE changes replace the stored value. The length
-- assertion observes the trailing byte; the notes token proves a maintenance
-- posting changed in the same ODKU.
insert into docs values (1, 'RESUME ALPHA ', 'notecase', 3)
    on duplicate key update body = values(body), notes = values(notes), payload = values(payload);
select id, length(body) as stored_len, length(notes) as notes_len, payload from docs;
select id from docs where match(notes) against('notecase') order by id;

-- NUL changes an existing row through ODKU, so the stored-byte marker is
-- executed instead of only exercising the ordinary INSERT path.
insert into docs values (2, 'nulold', 'nulnoteold', 4);
insert into docs values (2, concat('nul', char(0), 'token'), 'nulnotenew', 5)
    on duplicate key update body = values(body), notes = values(notes), payload = values(payload);
select id, length(body) as stored_len from docs where id = 2;
select id from docs where match(body) against('token') order by id;
select id from docs where match(notes) against('nulnotenew') order by id;

-- A long VARCHAR and a long TEXT value exercise the complete binary payload
-- without relying on truncation or SQL-mode-specific assignment behavior.
insert into docs values (1, concat('longtoken ', repeat('x', 128)), concat('texttailtoken ', repeat('y', 4096)), 6)
    on duplicate key update body = values(body), notes = values(notes), payload = values(payload);
select id, length(body) as stored_len, length(notes) as notes_len, payload from docs where id = 1;
select id from docs where match(body) against('longtoken') order by id;
select id from docs where match(notes) against('texttailtoken') order by id;

-- A mixed batch must maintain a changed conflict, leave its final bytes visible,
-- and index the fresh row. The changed body and notes also prove the two
-- FULLTEXT hooks receive the final stored values rather than the old image.
insert into docs values
    (1, concat('changedlongtoken ', repeat('x', 128)), 'changednote', 7),
    (3, 'freshword', 'freshnote', 8)
    on duplicate key update body = values(body), notes = values(notes), payload = values(payload);
select id, payload from docs order by id;
select id from docs where match(body) against('changedlongtoken') order by id;
select id from docs where match(notes) against('changednote') order by id;
select id from docs where match(body) against('freshword') order by id;
select id from docs where match(notes) against('freshnote') order by id;

-- Rollback must restore both the base value and the hidden FULLTEXT entries.
begin;
insert into docs values (1, 'rollbacktoken', 'rollbacknote', 9)
    on duplicate key update body = values(body), notes = values(notes), payload = values(payload);
rollback;
select id, length(body) as stored_len, length(notes) as notes_len, payload from docs where id = 1;
select id from docs where match(body) against('rollbacktoken') order by id;
select id from docs where match(body) against('changedlongtoken') order by id;
select id from docs where match(notes) against('rollbacknote') order by id;
select id from docs where match(notes) against('changednote') order by id;

-- NULL-safe equality must distinguish NULL-to-NULL from NULL-to-value and
-- value-to-NULL transitions while keeping the base and postings consistent.
insert into docs values (4, NULL, 'nullnote', 10);
insert into docs values (4, NULL, 'nullnote', 11)
    on duplicate key update body = values(body), notes = values(notes), payload = values(payload);
select id, length(body) as stored_len, payload from docs where id = 4;
insert into docs values (4, 'nullvalue', 'nullnote', 12)
    on duplicate key update body = values(body), notes = values(notes), payload = values(payload);
select id from docs where match(body) against('nullvalue') order by id;
insert into docs values (4, NULL, 'nullnote', 13)
    on duplicate key update body = values(body), notes = values(notes), payload = values(payload);
select id from docs where match(body) against('nullvalue') order by id;

drop database issue28494_fulltext;

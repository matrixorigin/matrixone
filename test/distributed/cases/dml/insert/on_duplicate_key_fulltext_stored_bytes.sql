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

-- One batch combines an equal conflict, a changed conflict, and a new row.
-- Both assignments mention both indexes; eligibility depends on final bytes.
insert into docs values (11, 'stablebody', 'stablenote', 1), (12, 'priorbody', 'priornote', 2);
insert into docs values (11, 'stablebody', 'stablenote', 11), (12, 'changedbody', 'changednotes', 12), (13, 'freshbody', 'freshnotes', 13) on duplicate key update body = values(body), notes = values(notes), payload = values(payload);
select id, body, notes, payload from docs where id >= 11 order by id;
select id from docs where match(body) against('stablebody') order by id;
select id from docs where match(body) against('changedbody') order by id;
select id from docs where match(body) against('freshbody') order by id;
select id from docs where match(notes) against('stablenote') order by id;
select id from docs where match(notes) against('changednotes') order by id;
select id from docs where match(notes) against('freshnotes') order by id;
select id from docs where match(body) against('priorbody') order by id;
select id from docs where match(notes) against('priornote') order by id;

-- Independent indexes must make asymmetric decisions, then exchange roles.
insert into docs values (11, 'stablebody', 'asymmetricnote', 21) on duplicate key update body = values(body), notes = values(notes), payload = values(payload);
select id from docs where match(body) against('stablebody') order by id;
select id from docs where match(notes) against('stablenote') order by id;
select id from docs where match(notes) against('asymmetricnote') order by id;
insert into docs values (11, 'asymmetricbody', 'asymmetricnote', 22) on duplicate key update body = values(body), notes = values(notes), payload = values(payload);
select id, body, notes, payload from docs where id = 11;
select id from docs where match(body) against('stablebody') order by id;
select id from docs where match(body) against('asymmetricbody') order by id;
select id from docs where match(notes) against('asymmetricnote') order by id;

-- Equal long prefixes must not hide a changed final token beyond 4 KiB.
insert into docs values (14, 'tailbody', concat(repeat('prefix ', 600), 'oldtailtoken'), 30);
insert into docs values (14, 'tailbody', concat(repeat('prefix ', 600), 'newtailtoken'), 31) on duplicate key update body = values(body), notes = values(notes), payload = values(payload);
select id, length(notes) as notes_len, hex(notes) = hex(concat(repeat('prefix ', 600), 'newtailtoken')) as exact_bytes, payload from docs where id = 14;
select id from docs where match(notes) against('oldtailtoken') order by id;
select id from docs where match(notes) against('newtailtoken') order by id;
select id from docs where match(body) against('tailbody') order by id;

-- Roll back changes to both indexes, including the long tail and a fresh row.
begin;
insert into docs values (11, 'abortbody', 'abortnote', 41), (14, 'aborttailbody', concat(repeat('prefix ', 600), 'aborttailtoken'), 42), (15, 'abortfreshbody', 'abortfreshnote', 43) on duplicate key update body = values(body), notes = values(notes), payload = values(payload);
rollback;
select id, body, payload from docs where id >= 11 order by id;
select id, length(notes) as notes_len, hex(notes) = hex(concat(repeat('prefix ', 600), 'newtailtoken')) as exact_bytes from docs where id = 14;
select id from docs where match(body) against('abortbody') order by id;
select id from docs where match(body) against('aborttailbody') order by id;
select id from docs where match(body) against('abortfreshbody') order by id;
select id from docs where match(notes) against('abortnote') order by id;
select id from docs where match(notes) against('aborttailtoken') order by id;
select id from docs where match(notes) against('abortfreshnote') order by id;
select id from docs where match(body) against('asymmetricbody') order by id;
select id from docs where match(body) against('tailbody') order by id;
select id from docs where match(notes) against('asymmetricnote') order by id;
select id from docs where match(notes) against('newtailtoken') order by id;

-- Replay both final indexed values while changing payload; no extra document.
insert into docs values (11, 'asymmetricbody', 'asymmetricnote', 51), (14, 'tailbody', concat(repeat('prefix ', 600), 'newtailtoken'), 52) on duplicate key update body = values(body), notes = values(notes), payload = values(payload);
select id, payload from docs where id in (11, 14) order by id;
select count(*) as document_count from docs where id >= 11;
select id from docs where match(body) against('asymmetricbody') order by id;
select id from docs where match(body) against('tailbody') order by id;
select id from docs where match(notes) against('asymmetricnote') order by id;
select id from docs where match(notes) against('newtailtoken') order by id;

drop database issue28494_fulltext;

-- @suite

-- @case
-- @desc: REPLACE conflict lookup with a primary key and a secondary unique key
-- @label:bvt
drop database if exists replace_conflict_lookup_27141;
create database replace_conflict_lookup_27141;
use replace_conflict_lookup_27141;

create table replace_record (
    id bigint primary key,
    external_key varchar(80) not null unique,
    payload varchar(120) not null,
    revision int not null
);
insert into replace_record values
    (1, 'key-1', 'initial', 0),
    (2, 'key-2', 'initial', 0),
    (3, 'key-3', 'initial', 0),
    (4, 'key-4', 'initial', 0),
    (5, 'key-5', 'initial', 0),
    (6, 'key-6', 'initial', 0);

begin;
-- Both constraints find the same old row; it must be deleted only once.
replace into replace_record values (1, 'key-1', 'replaced-same-row', 1);
select row_count();

-- The PK and UK find different old rows; both must be deleted before inserting once.
replace into replace_record values (2, 'key-3', 'replaced-split-row', 1);
select row_count();

-- Only the PK finds an old row; the UK branch is unmatched.
replace into replace_record values (4, 'key-8', 'replaced-pk-only', 1);
select row_count();

-- Only the UK finds an old row; the PK branch is unmatched.
replace into replace_record values (7, 'key-5', 'replaced-uk-only', 1);
select row_count();

-- No constraint finds an old row.
replace into replace_record values (10, 'key-10', 'replaced-insert-only', 1);
select row_count();

-- A VALUES batch mixes replacement and insertion in the same transaction.
replace into replace_record values
    (6, 'key-6', 'replaced-batch-conflict', 1),
    (9, 'key-9', 'replaced-batch-insert', 1);
select row_count();

-- Branch merging must preserve the source order used by keep-last deduplication.
replace into replace_record values
    (11, 'key-11', 'replaced-duplicate-first', 1),
    (12, 'key-11', 'replaced-duplicate-last', 1);
select id, payload from replace_record where external_key = 'key-11';
commit;

select count(*) as total,
       sum(case when revision = 1 then 1 else 0 end) as replaced,
       sum(case when payload like 'replaced-%' then 1 else 0 end) as payload_replaced
from replace_record;
select * from replace_record order by id;

drop database replace_conflict_lookup_27141;

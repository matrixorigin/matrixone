-- fulltext2 with a BIT primary key. build_ddl accepts a BIT column as a primary
-- key, so the segment pk codec (encodePk/decodePk/decodeDocmap) must support it.
-- A synchronous CREATE FULLTEXT2 builds the tag=0 base from source: each segment's
-- docmap is serialized via encodePk(T_bit) and reloaded via decodePk(T_bit) at
-- query time, so a plain CREATE + MATCH round-trips the BIT pk end-to-end. Before
-- the codec fix this returned "unsupported pk type" and stalled index maintenance.

drop database if exists test_fulltext2_bitpk;
create database test_fulltext2_bitpk;
use test_fulltext2_bitpk;

set experimental_fulltext2_index = 1;

drop table if exists docs;
create table docs(id bit(16) primary key, body text);
insert into docs values
 (10,'the quick brown fox jumps'),
 (20,'a quick brown dog'),
 (30,'the lazy fox sleeps'),
 (40,'brown bear and lazy cat'),
 (50,'quick quick quick fox fox');
create fulltext2 index ft on docs(body);
show create table docs;

-- NL exact-phrase; cast the BIT pk to unsigned for stable rendering.
select cast(id as unsigned) as id from docs where match(body) against('quick brown fox') order by id;
select cast(id as unsigned) as id from docs where match(body) against('brown fox') order by id;
select cast(id as unsigned) as id from docs where match(body) against('lazy') order by id;
select cast(id as unsigned) as id from docs where match(body) against('unicorn') order by id;

-- boolean mode over a BIT pk.
select cast(id as unsigned) as id from docs where match(body) against('+quick +fox' in boolean mode) order by id;
select cast(id as unsigned) as id from docs where match(body) against('+quick -fox' in boolean mode) order by id;
select cast(id as unsigned) as id from docs where match(body) against('quic*' in boolean mode) order by id;

drop database test_fulltext2_bitpk;

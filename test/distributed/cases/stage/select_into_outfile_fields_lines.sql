-- Coverage for FIELDS ESCAPED BY and LINES STARTING BY in SELECT INTO OUTFILE
-- (issue #30710 / consolidated #30754: grammar parity with LOAD DATA, and the
-- writer honoring both clauses byte-exactly).
drop database if exists outfile_fields_lines;
create database outfile_fields_lines;
use outfile_fields_lines;

drop stage if exists outfile_fields_lines_stage;
create stage outfile_fields_lines_stage URL='file://$resources/into_outfile/outfile_fields_lines';

create table src (id int, s varchar(20));
insert into src values
(1,'alpha'),
(2,'be,ns'),
(3,'has"quote'),
(4,'#hash'),
(5,null);

-- The exact statements from #30754: previously a parse error, now exported.
select 1 into outfile 'stage://outfile_fields_lines_stage/o1.csv'
fields terminated by ',' enclosed by '' escaped by '\\' header 'false';
select 1 into outfile 'stage://outfile_fields_lines_stage/o2.csv'
lines starting by '>>' terminated by '\n' header 'false';
select load_file(cast('stage://outfile_fields_lines_stage/o1.csv' as datalink));
select load_file(cast('stage://outfile_fields_lines_stage/o2.csv' as datalink));

-- LINES STARTING BY prefixes every data record; the header line is NOT
-- prefixed, so LOAD skips it instead of importing it as a data row.
select id, s from src order by id
into outfile 'stage://outfile_fields_lines_stage/prefix.csv'
lines starting by '>>' terminated by '\n';
select load_file(cast('stage://outfile_fields_lines_stage/prefix.csv' as datalink));
create table back1 (id int, s varchar(20));
load data infile 'stage://outfile_fields_lines_stage/prefix.csv' into table back1
fields terminated by ',' enclosed by '"' lines starting by '>>' terminated by '\n';
select * from back1 order by id;

-- A custom FIELDS ESCAPED BY byte protects itself and the separators on write
-- and must round-trip through the matching LOAD clause.
select id, s from src order by id
into outfile 'stage://outfile_fields_lines_stage/esc.csv'
fields terminated by ',' enclosed by '' escaped by '#' header 'false';
select load_file(cast('stage://outfile_fields_lines_stage/esc.csv' as datalink));
create table back2 (id int, s varchar(20));
load data infile 'stage://outfile_fields_lines_stage/esc.csv' into table back2
fields terminated by ',' enclosed by '' escaped by '#' lines terminated by '\n';
select * from back2 order by id;

-- ESCAPED BY '' disables escaping: values are written verbatim. The embedded
-- separator can then no longer round-trip (documented MySQL semantics), so
-- only the write result is pinned here.
select id, s from src where id = 2
into outfile 'stage://outfile_fields_lines_stage/verbatim.csv'
fields terminated by ',' enclosed by '' escaped by '' header 'false';
select load_file(cast('stage://outfile_fields_lines_stage/verbatim.csv' as datalink));

-- Default framing (no clauses) stays byte-identical.
select id, s from src order by id
into outfile 'stage://outfile_fields_lines_stage/defaults.csv' header 'false';
select load_file(cast('stage://outfile_fields_lines_stage/defaults.csv' as datalink));

-- Header/prefix collision (review probe): LOAD's startingBy filter skips a
-- line only when the prefix is absent from it. A column named `name` with
-- LINES STARTING BY 'n' would load a mangled "ame" header row, so the export
-- is rejected before any file is opened. HEADER 'false' is the escape hatch
-- and round-trips with no ghost row.
select 'alpha' as name into outfile 'stage://outfile_fields_lines_stage/collide.csv'
lines starting by 'n';
select 1 as id, 'alpha' as data into outfile 'stage://outfile_fields_lines_stage/collide2.csv'
lines starting by 'd,d';
select 'alpha' as name into outfile 'stage://outfile_fields_lines_stage/tagged.csv'
lines starting by 'n' header 'false';
select load_file(cast('stage://outfile_fields_lines_stage/tagged.csv' as datalink));
create table hdr_back (s varchar(10));
load data infile 'stage://outfile_fields_lines_stage/tagged.csv' into table hdr_back
fields terminated by ',' lines starting by 'n' terminated by '\n' (s);
select * from hdr_back;
drop table hdr_back;

-- A literal '\N' value is LOAD's NULL token even inside quotes
-- (quotedNullIsText=false). With a non-default FIELDS ESCAPED BY the written
-- bytes would silently reload as SQL NULL, so the export is rejected up front
-- and no file may be created. Genuine NULLs are unaffected.
create table rt (id int, s varchar(20));
insert into rt values (1,'\\N'),(2,null),(3,'plain');
select id, s from rt order by id
into outfile 'stage://outfile_fields_lines_stage/nt_hash.csv'
fields terminated by ',' enclosed by '' escaped by '#' header 'false';
select count(*) from stage_list('stage://outfile_fields_lines_stage/nt_hash.csv');
select id, s from rt where id = 1
into outfile 'stage://outfile_fields_lines_stage/nt_empty.csv'
fields terminated by ',' enclosed by '' escaped by '' header 'false';
select count(*) from stage_list('stage://outfile_fields_lines_stage/nt_empty.csv');

-- The default escape rewrites the literal to \\N, so it round-trips as text
-- while a real NULL keeps the bare \N marker.
select id, s from rt order by id
into outfile 'stage://outfile_fields_lines_stage/nt_default.csv'
fields terminated by ',' enclosed by '' header 'false';
select load_file(cast('stage://outfile_fields_lines_stage/nt_default.csv' as datalink));
create table rt_back (id int, s varchar(20));
load data infile 'stage://outfile_fields_lines_stage/nt_default.csv' into table rt_back
fields terminated by ',' enclosed by '' lines terminated by '\n';
select id, s, s is null as is_null from rt_back order by id;

-- NULLs also export under a custom escape: the NULL marker is not a value.
select id, s from rt where id in (2,3) order by id
into outfile 'stage://outfile_fields_lines_stage/nt_nulls.csv'
fields terminated by ',' enclosed by '' escaped by '#' header 'false';
select load_file(cast('stage://outfile_fields_lines_stage/nt_nulls.csv' as datalink));
create table rt_nulls (id int, s varchar(20));
load data infile 'stage://outfile_fields_lines_stage/nt_nulls.csv' into table rt_nulls
fields terminated by ',' enclosed by '' escaped by '#' lines terminated by '\n';
select id, s is null as is_null from rt_nulls order by id;

-- With FIELDS ESCAPED BY '' the reader strips a trailing CR of the last
-- column's record under '\n' termination. The strip runs after unquoting, so
-- an enclosure does not protect it either: both exports are rejected. Any
-- escape byte rewrites the CR and round-trips.
create table rtcr (id int, s varchar(20));
insert into rtcr values (1,'a\r');
select id, s from rtcr
into outfile 'stage://outfile_fields_lines_stage/cr_bare.csv'
fields terminated by ',' enclosed by '' escaped by '' header 'false';
select count(*) from stage_list('stage://outfile_fields_lines_stage/cr_bare.csv');
select id, s from rtcr
into outfile 'stage://outfile_fields_lines_stage/cr_quoted.csv'
fields terminated by ',' enclosed by '"' escaped by '' header 'false';
select count(*) from stage_list('stage://outfile_fields_lines_stage/cr_quoted.csv');
-- CR in a non-last column is followed by the separator and keeps its byte
-- (file content pinned via hex: a raw CR would confuse line-based readers):
select s, id from rtcr
into outfile 'stage://outfile_fields_lines_stage/cr_first.csv'
fields terminated by ',' enclosed by '' escaped by '' header 'false';
select hex(load_file(cast('stage://outfile_fields_lines_stage/cr_first.csv' as datalink)));
create table rtcr_first (s varchar(20), id int);
load data infile 'stage://outfile_fields_lines_stage/cr_first.csv' into table rtcr_first
fields terminated by ',' enclosed by '' escaped by '' lines terminated by '\n';
select id, hex(s) as hs from rtcr_first;
-- A custom escape protects the CR in the last column:
select id, s from rtcr
into outfile 'stage://outfile_fields_lines_stage/cr_hash.csv'
fields terminated by ',' enclosed by '' escaped by '#' header 'false';
select load_file(cast('stage://outfile_fields_lines_stage/cr_hash.csv' as datalink));
create table rtcr_hash (id int, s varchar(20));
load data infile 'stage://outfile_fields_lines_stage/cr_hash.csv' into table rtcr_hash
fields terminated by ',' enclosed by '' escaped by '#' lines terminated by '\n';
select id, hex(s) as hs from rtcr_hash;

-- Escape byte collision with LOAD's translated escape letters ('0','b','n',
-- 'r','t','Z'): the writer doubles the letter, but LOAD decodes the pair as a
-- control character, so a value containing the escape byte can never
-- round-trip and the export is rejected before a file is written.
create table rl (id int, s varchar(20));
insert into rl values (1,'banana'),(2,'apple');
select id, s from rl order by id
into outfile 'stage://outfile_fields_lines_stage/esc_n_bad.csv'
fields terminated by ',' enclosed by '' escaped by 'n' header 'false';
select count(*) from stage_list('stage://outfile_fields_lines_stage/esc_n_bad.csv');
-- Letter-free values under the same clause round-trip:
select id, s from rl where id = 2
into outfile 'stage://outfile_fields_lines_stage/esc_n_ok.csv'
fields terminated by ',' enclosed by '' escaped by 'n' header 'false';
select load_file(cast('stage://outfile_fields_lines_stage/esc_n_ok.csv' as datalink));
create table rl_back (id int, s varchar(20));
load data infile 'stage://outfile_fields_lines_stage/esc_n_ok.csv' into table rl_back
fields terminated by ',' enclosed by '' escaped by 'n' lines terminated by '\n';
select * from rl_back;

-- ESCAPED BY 'N' collides with the NULL marker: LOAD consumes the byte after
-- the marker as escape content and merges records, so NULL exports are
-- rejected under that clause. Literal values containing N round-trip (NN->N).
create table rn (id int, s varchar(20));
insert into rn values (1,'Nyx'),(2,null);
select id, s from rn order by id
into outfile 'stage://outfile_fields_lines_stage/esc_uppern_null.csv'
fields terminated by ',' enclosed by '' escaped by 'N' header 'false';
select count(*) from stage_list('stage://outfile_fields_lines_stage/esc_uppern_null.csv');
select id, s from rn where id = 1
into outfile 'stage://outfile_fields_lines_stage/esc_uppern_ok.csv'
fields terminated by ',' enclosed by '' escaped by 'N' header 'false';
select load_file(cast('stage://outfile_fields_lines_stage/esc_uppern_ok.csv' as datalink));
create table rn_back (id int, s varchar(20));
load data infile 'stage://outfile_fields_lines_stage/esc_uppern_ok.csv' into table rn_back
fields terminated by ',' enclosed by '' escaped by 'N' lines terminated by '\n';
select * from rn_back;

-- The last-column CR strip only applies when LOAD's normalized terminator is
-- '\n'. With LINES TERMINATED BY '|' the same CR-final value round-trips.
select id, s from rtcr
into outfile 'stage://outfile_fields_lines_stage/cr_pipe.csv'
fields terminated by ',' enclosed by '' escaped by '' lines terminated by '|' header 'false';
select hex(load_file(cast('stage://outfile_fields_lines_stage/cr_pipe.csv' as datalink)));
create table rtcr_pipe (id int, s varchar(20));
load data infile 'stage://outfile_fields_lines_stage/cr_pipe.csv' into table rtcr_pipe
fields terminated by ',' enclosed by '' escaped by '' lines terminated by '|';
select id, hex(s) as hs from rtcr_pipe;

-- The header/prefix collision check covers the complete header record,
-- terminator included: with column X and TERMINATED BY '||' the emitted
-- header is 'X||', which prefix 'X|' matches; LOAD would mangle the first
-- value. HEADER 'false' remains the escape hatch and round-trips.
select 'one' as X into outfile 'stage://outfile_fields_lines_stage/hdr_term.csv'
lines starting by 'X|' terminated by '||';
select count(*) from stage_list('stage://outfile_fields_lines_stage/hdr_term.csv');
select 'one' as X into outfile 'stage://outfile_fields_lines_stage/hdr_term_nohead.csv'
lines starting by 'X|' terminated by '||' header 'false';
select load_file(cast('stage://outfile_fields_lines_stage/hdr_term_nohead.csv' as datalink));
create table ht_back (s varchar(10));
load data infile 'stage://outfile_fields_lines_stage/hdr_term_nohead.csv' into table ht_back
fields terminated by ',' lines starting by 'X|' terminated by '||' (s);
select * from ht_back;
drop table ht_back;

-- The last-column CR strip keys on LOAD's complete normalized terminator:
-- '\n|' is not LF, so the same CR-final value round-trips here.
select id, s from rtcr
into outfile 'stage://outfile_fields_lines_stage/cr_npipe.csv'
fields terminated by ',' enclosed by '' escaped by '' lines terminated by '\n|' header 'false';
select hex(load_file(cast('stage://outfile_fields_lines_stage/cr_npipe.csv' as datalink)));
create table rtcr_npipe (id int, s varchar(20));
load data infile 'stage://outfile_fields_lines_stage/cr_npipe.csv' into table rtcr_npipe
fields terminated by ',' enclosed by '' escaped by '' lines terminated by '\n|';
select id, hex(s) as hs from rtcr_npipe;
drop table rtcr_npipe;
drop table rtcr_pipe;
drop table rn_back;
drop table rn;
drop table rl_back;
drop table rl;
drop table rtcr_hash;
drop table rtcr_first;
drop table rtcr;
drop table rt_nulls;
drop table rt_back;
drop table rt;

-- A bare literal NULL word must survive when the configured ESCAPED BY byte
-- occurs inside it: prefixing only the first N would still let LOAD collapse
-- the inner escape bytes ('UNULL' reads as 'NLL', 'LNULL' as 'NUL'). The
-- writer encodes every occurrence, so LOAD returns the literal text NULL,
-- not SQL NULL and not a shortened word.
select 'NULL' as s into outfile 'stage://outfile_fields_lines_stage/null_esc_u.csv'
fields terminated by ',' enclosed by '' escaped by 'U' header 'false';
select hex(load_file(cast('stage://outfile_fields_lines_stage/null_esc_u.csv' as datalink)));
create table null_esc_back (s varchar(10));
load data infile 'stage://outfile_fields_lines_stage/null_esc_u.csv' into table null_esc_back
fields terminated by ',' enclosed by '' escaped by 'U' (s);
select s, s is null as isn from null_esc_back;

select 'NULL' as s into outfile 'stage://outfile_fields_lines_stage/null_esc_l.csv'
fields terminated by ',' enclosed by '' escaped by 'L' header 'false';
select hex(load_file(cast('stage://outfile_fields_lines_stage/null_esc_l.csv' as datalink)));
delete from null_esc_back;
load data infile 'stage://outfile_fields_lines_stage/null_esc_l.csv' into table null_esc_back
fields terminated by ',' enclosed by '' escaped by 'L' (s);
select s, s is null as isn from null_esc_back;
drop table null_esc_back;

-- A bare CR or LF ESCAPED BY cannot protect separators while LOAD's parser
-- still treats newlines as record boundaries (it consumes the newline before
-- applying the escape byte), so the export is rejected before opening the
-- file. A nonempty ENCLOSED BY keeps both bytes inside quotes and round-trips.
select id, s from src where id = 2
into outfile 'stage://outfile_fields_lines_stage/esc_cr_bad.csv'
fields terminated by ',' enclosed by '' escaped by '\r' lines starting by 'p:' header 'false';
select count(*) from stage_list('stage://outfile_fields_lines_stage/esc_cr_bad.csv');
select id, s from src where id = 2
into outfile 'stage://outfile_fields_lines_stage/esc_cr_ok.csv'
fields terminated by ',' enclosed by '"' escaped by '\r' lines starting by 'p:' header 'false';
select hex(load_file(cast('stage://outfile_fields_lines_stage/esc_cr_ok.csv' as datalink)));
create table esc_cr_back (id int, s varchar(20));
load data infile 'stage://outfile_fields_lines_stage/esc_cr_ok.csv' into table esc_cr_back
fields terminated by ',' enclosed by '"' escaped by '\r' lines starting by 'p:';
select id, hex(s) as hs from esc_cr_back;
drop table esc_cr_back;

-- A value whose bare encoding would emit the CR/LF escape byte cannot stay
-- bare: LOAD consumes newlines as record boundaries before decoding escapes,
-- so BIT 6368354 (0x612C62, bytes 'a,b') under ESCAPED BY '\r' reloaded as
-- 0x61. The shared formatter encloses such values even when the column never
-- asks for quotes (BIT passes quote=false to both CSV writers).
select 1 as id, cast(6368354 as bit(24)) as b
into outfile 'stage://outfile_fields_lines_stage/bit_cr_esc.csv'
fields terminated by ',' enclosed by '"' escaped by '\r' lines starting by 'p:' terminated by '\n' header 'false';
select hex(load_file(cast('stage://outfile_fields_lines_stage/bit_cr_esc.csv' as datalink)));
create table bit_cr_back (id int, b bit(24));
load data infile 'stage://outfile_fields_lines_stage/bit_cr_esc.csv' into table bit_cr_back
fields terminated by ',' enclosed by '"' escaped by '\r' lines starting by 'p:' terminated by '\n';
select id, hex(b) as hb from bit_cr_back;
drop table bit_cr_back;

-- The writer replays every emitted field through LOAD's own parser before
-- publishing: with ENCLOSED BY equal to ESCAPED BY, or marker bytes \N
-- colliding with the enclosure, encodings the scan cannot resolve are
-- rejected instead of writing an unparsable file. The walls stay narrow:
-- marker/enclosure shapes that genuinely reload stay allowed.
select id, s from (select 1 as id, null as s) t
into outfile 'stage://outfile_fields_lines_stage/null_enc_bs.csv'
fields terminated by ',' enclosed by '\\' escaped by '#' lines starting by 'p:' header 'false';
select count(*) from stage_list('stage://outfile_fields_lines_stage/null_enc_bs.csv');
select id, s from (select 1 as id, null as s) t
into outfile 'stage://outfile_fields_lines_stage/null_enc_n_ok.csv'
fields terminated by ',' enclosed by 'N' escaped by '#' lines starting by 'p:' header 'false';
select hex(load_file(cast('stage://outfile_fields_lines_stage/null_enc_n_ok.csv' as datalink)));
create table null_enc_n_back (id int, s varchar(20));
load data infile 'stage://outfile_fields_lines_stage/null_enc_n_ok.csv' into table null_enc_n_back
fields terminated by ',' enclosed by 'N' escaped by '#' lines starting by 'p:';
select id, s, s is null as isn from null_enc_n_back;
drop table null_enc_n_back;

drop table if exists enc_eq_src;
create table enc_eq_src (id int, s varchar(20));
insert into enc_eq_src values (1, unhex('610D'));
select id, s from enc_eq_src
into outfile 'stage://outfile_fields_lines_stage/enc_eq_r_bad.csv'
fields terminated by ',' enclosed by 'r' escaped by 'r' lines starting by 'p:' header 'false';
select count(*) from stage_list('stage://outfile_fields_lines_stage/enc_eq_r_bad.csv');
-- The identical value under a hash enclosure round-trips: only the shapes
-- the parser cannot resolve are rejected.
select id, s from enc_eq_src
into outfile 'stage://outfile_fields_lines_stage/enc_eq_hash_ok.csv'
fields terminated by ',' enclosed by '#' escaped by '#' lines starting by 'p:' header 'false';
select hex(load_file(cast('stage://outfile_fields_lines_stage/enc_eq_hash_ok.csv' as datalink)));
create table enc_eq_hash_back (id int, s varchar(20));
load data infile 'stage://outfile_fields_lines_stage/enc_eq_hash_ok.csv' into table enc_eq_hash_back
fields terminated by ',' enclosed by '#' escaped by '#' lines starting by 'p:';
select id, hex(s) as hs from enc_eq_hash_back;
drop table enc_eq_hash_back;
drop table enc_eq_src;

-- csvparser itself rejects CR, LF, a double quote or NUL as the field
-- delimiter (its validity predicate mirrors Go's encoding/csv), so LOAD
-- DATA with such a framing fails at parser construction. Export must
-- reject the same framings up front with a statement error instead of
-- writing a file no LOAD can ever read; loadable delimiters, including
-- multi-character ones, stay allowed and keep round-tripping.
select id, s from (select 1 as id, 'a' as s) t
into outfile 'stage://outfile_fields_lines_stage/delim_quote.csv'
fields terminated by '"' enclosed by '\\' escaped by '' lines starting by 'p:' header 'false';
select count(*) from stage_list('stage://outfile_fields_lines_stage/delim_quote.csv');
-- Deliberate compatibility change: even the pre-existing syntax without
-- ESCAPED BY or STARTING BY rejects this LOAD-incompatible delimiter.
select 1 into outfile 'stage://outfile_fields_lines_stage/delim_legacy.csv'
fields terminated by '"|' enclosed by '' header 'false';
select count(*) from stage_list('stage://outfile_fields_lines_stage/delim_legacy.csv');
select id, s from (select 1 as id, 'ab' as s) t
into outfile 'stage://outfile_fields_lines_stage/delim_multi_ok.csv'
fields terminated by '|@' enclosed by '' escaped by '' lines starting by 'p:' header 'false';
select hex(load_file(cast('stage://outfile_fields_lines_stage/delim_multi_ok.csv' as datalink)));
create table delim_multi_back (id int, s varchar(20));
load data infile 'stage://outfile_fields_lines_stage/delim_multi_ok.csv' into table delim_multi_back
fields terminated by '|@' enclosed by '' escaped by '' lines starting by 'p:';
select id, s from delim_multi_back;
drop table delim_multi_back;

-- The NULL marker is always a literal backslash + N, so a field
-- terminator starting with either byte splits the emitted marker into
-- two non-NULL fields on LOAD. Export rejects such framings; with the
-- default backslash escape the identical bytes still read back as NULL.
select id, s from (select 1 as id, null as s) t
into outfile 'stage://outfile_fields_lines_stage/null_term_n.csv'
fields terminated by 'N' enclosed by '"' escaped by '#' lines starting by 'p:' header 'false';
select count(*) from stage_list('stage://outfile_fields_lines_stage/null_term_n.csv');
select id, s from (select 1 as id, null as s) t
into outfile 'stage://outfile_fields_lines_stage/null_term_n_ok.csv'
fields terminated by 'N' enclosed by '"' escaped by '\\' lines starting by 'p:' header 'false';
select hex(load_file(cast('stage://outfile_fields_lines_stage/null_term_n_ok.csv' as datalink)));
create table null_term_n_back (id int, s varchar(20));
load data infile 'stage://outfile_fields_lines_stage/null_term_n_ok.csv' into table null_term_n_back
fields terminated by 'N' enclosed by '"' escaped by '\\' lines starting by 'p:';
select id, s, s is null as isn from null_term_n_back;
drop table null_term_n_back;

-- The actual following field delimiter matters too: NN overlaps the N at
-- the end of a bare NULL marker even though NN does not occur in the marker
-- alone. Reject first/middle NULLs before publishing any corrupt row.
select a, b from (select null as a, 12 as b) t
into outfile 'stage://outfile_fields_lines_stage/null_term_nn_first.csv'
fields terminated by 'NN' enclosed by '"' escaped by '#' header 'false';
select count(*) from stage_list('stage://outfile_fields_lines_stage/null_term_nn_first.csv');
select a, b, c from (select 1 as a, null as b, 12 as c) t
into outfile 'stage://outfile_fields_lines_stage/null_term_nn_middle.csv'
fields terminated by 'NN' enclosed by '"' escaped by '#' header 'false';
select count(*) from stage_list('stage://outfile_fields_lines_stage/null_term_nn_middle.csv');

-- A last-column NULL has a record terminator, not NN, after its marker;
-- retain that safe framing and the default-backslash control in all positions.
select a, b, c from (select 1 as a, 12 as b, null as c) t
into outfile 'stage://outfile_fields_lines_stage/null_term_nn_last.csv'
fields terminated by 'NN' enclosed by '"' escaped by '#' header 'false';
create table null_term_nn_back (a int, b int, c int);
load data infile 'stage://outfile_fields_lines_stage/null_term_nn_last.csv' into table null_term_nn_back
fields terminated by 'NN' enclosed by '"' escaped by '#';
select a, b, c, c is null as isn from null_term_nn_back;
truncate table null_term_nn_back;
select a, b, c from (select null as a, 12 as b, 23 as c union all select 12, null, 23 union all select 12, 23, null) t
order by coalesce(a, 0), coalesce(b, 0), coalesce(c, 0)
into outfile 'stage://outfile_fields_lines_stage/null_term_nn_ok.csv'
fields terminated by 'NN' enclosed by '"' escaped by '\\' header 'false';
load data infile 'stage://outfile_fields_lines_stage/null_term_nn_ok.csv' into table null_term_nn_back
fields terminated by 'NN' enclosed by '"' escaped by '\\';
select a, b, c, a is null as na, b is null as nb, c is null as nc from null_term_nn_back
order by coalesce(a, 0), coalesce(b, 0), coalesce(c, 0);
drop table null_term_nn_back;

remove files from stage if exists 'stage://outfile_fields_lines_stage/*.csv';
select count(*) from stage_list('stage://outfile_fields_lines_stage/');
drop stage outfile_fields_lines_stage;
drop table back2;
drop table back1;
drop table src;
drop database outfile_fields_lines;

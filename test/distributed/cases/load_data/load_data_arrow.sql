-- The shared compose profile is a production-like default configuration. It
-- must not opt in to Arrow LOAD merely because standard BVT runs on it.
-- Opted-in File/Stream/S3 and multi-CN behavior is covered by the dedicated
-- pkg/tests/arrowload cluster suite, whose fixture explicitly configures every
-- participating CN.
drop database if exists arrow_load_bvt;
create database arrow_load_bvt;
use arrow_load_bvt;

create table arrow_gate_off(id bigint, name varchar(50));
load data infile {'filepath'='$resources/load_data/arrow_file.arrow', 'format'='arrow'} into table arrow_gate_off;
select count(*) from arrow_gate_off;

drop database arrow_load_bvt;

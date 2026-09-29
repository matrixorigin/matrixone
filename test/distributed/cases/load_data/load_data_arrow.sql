-- The shared compose profile omits the Arrow section, so this case proves the
-- default-on local File/Stream contract through the real SQL path. S3/stage and
-- multi-CN coverage is exercised by the dedicated arrowload suite.
drop database if exists arrow_load_bvt;
create database arrow_load_bvt;
use arrow_load_bvt;

create table arrow_file_default(id bigint, name varchar(50));
load data infile {'filepath'='$resources/load_data/arrow_file.arrow', 'format'='arrow'} into table arrow_file_default;
select count(*) from arrow_file_default;

create table arrow_stream_default(id bigint, name varchar(50));
load data infile {'filepath'='$resources/load_data/arrow_stream.arrow', 'format'='arrow'} into table arrow_stream_default;
select count(*) from arrow_stream_default;

drop database arrow_load_bvt;

-- see prepare op in ../log/query_tcp.sql


-- case 1
-- stats[7] counts completed MySQL response protocol packets. This result has
-- 8192 row packets plus the result-set metadata and terminator packets.
set @packet_cnt=8195;
-- @ignore:2,3,4
select statement, json_unquote(json_extract(stats, '$[7]')) = @packet_cnt check_val, statement_id, stats, json_extract(stats, '$[7]') pkg_cnt from system.statement_info where account= 'bvt_query_tcp' and statement='select * from 32kb_8192row_int order by a' order by request_at desc limit 1;

-- case:insert_8192_one_sql
-- A successful INSERT returns one OK packet; request size is not output.
set @packet_cnt=1;
-- @ignore:2,3,4
select left(statement, 47) stmt, json_unquote(json_extract(stats, '$[7]')) = @packet_cnt check_val, statement_id, stats, json_extract(stats, '$[7]') pkg_cnt from system.statement_info where account= 'bvt_query_tcp' and statement like 'insert into 32kb_8192row_int values (1),(1),(1)%' order by request_at desc limit 1;

-- case:load_1751_rows
-- Server-side LOAD returns one OK packet.
set @packet_cnt=1;
-- @ignore:2,3,4
select left(statement, 16) as stmt, json_unquote(json_extract(stats, '$[7]')) = @packet_cnt check_val, statement_id, stats, json_extract(stats, '$[7]') pkg_cnt from system.statement_info where account= 'bvt_query_tcp' and statement like 'load data infile%rawlog_withnull.csv%' and statement_type = 'Load' order by request_at desc limit 1;
-- LOCAL LOAD returns one file-request packet followed by one OK packet.
set @packet_cnt=2;
-- @ignore:2,3,4
select left(statement, 22) as stmt, json_unquote(json_extract(stats, '$[7]')) = @packet_cnt check_val, statement_id, stats, json_extract(stats, '$[7]') pkg_cnt from system.statement_info where account= 'bvt_query_tcp' and statement like 'load data local%rawlog_withnull.csv%' and statement_type = 'Load' order by request_at desc limit 1;

-- case: verify 'use test
set @packet_cnt=1;
-- @ignore:2,3,4
select statement, json_unquote(json_extract(stats, '$[7]')) = @packet_cnt check_val, statement_id, stats, json_extract(stats, '$[7]') pkg_cnt from system.statement_info where account= 'bvt_query_tcp' and statement='use test' order by request_at desc limit 1;

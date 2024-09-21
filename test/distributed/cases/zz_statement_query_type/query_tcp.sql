-- see prepare op in ../log/query_tcp.sql


-- case 1
-- just check colomn statment, check_val
-- more details in https://github.com/matrixorigin/MO-Cloud/issues/2726#issuecomment-2008930179
-- file size: < 1KB, raw tcp packet: 15
-- stats[7] = {client sended pkg} + 15 ~= 18
set @tcp_cnt=3;
-- @ignore:2,3,4
select statement, json_unquote(json_extract(stats, '$[7]')) between (@tcp_cnt-2) and (@tcp_cnt+5) check_val, statement_id, stats, json_extract(stats, '$[7]') pkg_cnt from system.statement_info where account= 'bvt_query_tcp' and statement='select * from 32kb_8192row_int order by a' order by request_at desc limit 1;

-- case:insert_8192_one_sql
-- just check colomn statment, check_val
-- more details in https://github.com/matrixorigin/matrixone/issues/10863#issuecomment-1684904316
-- <insert with ## rows>
-- sql length: 32819
-- stats[7] = 32819 / 16KB + 1 ~= 3
set @tcp_cnt=3;
-- @ignore:2,3,4
select left(statement, 47) stmt, json_unquote(json_extract(stats, '$[7]')) between (@tcp_cnt-2) and (@tcp_cnt+5) check_val, statement_id, stats, json_extract(stats, '$[7]') pkg_cnt from system.statement_info where account= 'bvt_query_tcp' and statement like 'insert into 32kb_8192row_int values (1),(1),(1)%' order by request_at desc limit 1;

-- case:load_1751_rows
-- more in https://github.com/matrixorigin/matrixone/issues/10863#issuecomment-1684904316
-- file size: 404 KB, raw tcp packet: 14
-- stats[7] = (404 / 16) ~= 25
set @tcp_cnt=1;
-- @ignore:2,3,4
select left(statement, 16) as stmt, json_unquote(json_extract(stats, '$[7]')) between (@tcp_cnt-2) and (@tcp_cnt+5) check_val, statement_id, stats, json_extract(stats, '$[7]') pkg_cnt from system.statement_info where account= 'bvt_query_tcp' and statement like 'load data infile%rawlog_withnull.csv%' and statement_type = 'Load' order by request_at desc limit 1;
set @tcp_cnt=25;
-- @ignore:2,3,4
select left(statement, 22) as stmt, json_unquote(json_extract(stats, '$[7]')) between (@tcp_cnt-2) and (@tcp_cnt+5) check_val, statement_id, stats, json_extract(stats, '$[7]') pkg_cnt from system.statement_info where account= 'bvt_query_tcp' and statement like 'load data local%rawlog_withnull.csv%' and statement_type = 'Load' order by request_at desc limit 1;

-- case: verify 'use test
set @tcp_cnt=1;
-- @ignore:2,3,4
select statement, json_unquote(json_extract(stats, '$[7]')) between (@tcp_cnt-2) and (@tcp_cnt+5) check_val, statement_id, stats, json_extract(stats, '$[7]') pkg_cnt from system.statement_info where account= 'bvt_query_tcp' and statement='use test' order by request_at desc limit 1;

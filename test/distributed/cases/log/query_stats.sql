-- @bvt:issue#10506
set @stats="[1,118334200,2911386337083,0,0]";
select @stats, JSON_UNQUOTE(JSON_EXTRACT(@stats, '$[0]')) ver, JSON_UNQUOTE(JSON_EXTRACT(@stats, '$[1]'))  as var1, JSON_UNQUOTE(JSON_EXTRACT(@stats, '$[2]'))  as var2, JSON_UNQUOTE(JSON_EXTRACT(@stats, '$[3]'))  as var3, JSON_UNQUOTE(JSON_EXTRACT(@stats, '$[4]'))  as var4, JSON_UNQUOTE(JSON_EXTRACT(@stats, '$[1]')) + JSON_UNQUOTE(JSON_EXTRACT(@stats, '$[2]'))  as sum2, JSON_UNQUOTE(JSON_EXTRACT(@stats, '$[1]')) + JSON_UNQUOTE(JSON_EXTRACT(@stats, '$[2]')) + JSON_UNQUOTE(JSON_EXTRACT(@stats, '$[3]'))  as sum3, JSON_UNQUOTE(JSON_EXTRACT(@stats, '$[1]')) + JSON_UNQUOTE(JSON_EXTRACT(@stats, '$[2]')) + JSON_UNQUOTE(JSON_EXTRACT(@stats, '$[3]')) + JSON_UNQUOTE(JSON_EXTRACT(@stats, '$[4]')) as sum4;
-- @bvt:issue

-- check stats is valid json-arr
select JSON_UNQUOTE(JSON_EXTRACT(stats, '$[0]')) * 0 ver, (JSON_UNQUOTE(JSON_EXTRACT(stats, '$[1]')) + JSON_UNQUOTE(JSON_EXTRACT(stats, '$[2]'))*1e-9 + JSON_UNQUOTE(JSON_EXTRACT(stats, '$[3]')) + JSON_UNQUOTE(JSON_EXTRACT(stats, '$[4]')) ) * 0 as val from system.statement_info order by request_at desc limit 1;

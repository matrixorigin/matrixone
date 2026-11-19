set save_query_result = on;
show count(*) warnings;
show count(*) errors;
select uncompress(x'00000000') is null as corrupt_input_is_null;
show count(*) warnings;
set save_query_result = off;

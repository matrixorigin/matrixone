-- issue #28667: group_concat_max_len follows MySQL's unsigned assignment
-- range and clamps values below 4 with warning 1292.
select @@session.group_concat_max_len;

set session group_concat_max_len = 0;
show warnings;
select @@session.group_concat_max_len;

set session group_concat_max_len = 1;
show warnings;
select @@session.group_concat_max_len;

set session group_concat_max_len = 3;
show warnings;
select @@session.group_concat_max_len;

set session group_concat_max_len = 4;
show warnings;
select @@session.group_concat_max_len;

set session group_concat_max_len = 9223372036854775807;
show warnings;
select @@session.group_concat_max_len;

set session group_concat_max_len = 9223372036854775808;
show warnings;
select @@session.group_concat_max_len;

set session group_concat_max_len = 18446744073709551615;
show warnings;
select @@session.group_concat_max_len;

set session group_concat_max_len = -1;
show warnings;
select @@session.group_concat_max_len;

set session group_concat_max_len = default;
show warnings;
select @@session.group_concat_max_len;

-- A rejected assignment must not replace the last valid session value.
set session group_concat_max_len = '18446744073709551616';
select @@session.group_concat_max_len;

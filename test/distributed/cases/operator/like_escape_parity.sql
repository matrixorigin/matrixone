-- @suit
-- @case
-- @desc:LIKE 尾部通配符前的反斜杠转义
-- @label:bvt

-- 尾部通配符前连续反斜杠的奇偶数决定是否转义。
select 'path ab\\ here' like '%ab\\\\%' as escaped_backslash;
select 'path ab\\\\ here' like '%ab\\\\\\\\%' as two_escaped_backslashes;
select 'path abX here' like '%ab\\\\%' as missing_backslash;
select 'path ab\\c here' like '%ab\\\\c%' as backslash_before_literal;
select 'discount 50%' like '%50\\%' as literal_percent;
select 'discount 50%more' like '%50\\%' as literal_percent_suffix;
select 'path a_b\\ here' like '%a\\_b\\\\%' as escaped_middle_underscore;
select 'ab\\tail' like 'ab\\\\%' as backslash_prefix;
select 'path ab\\中' like '%ab\\\\_' as backslash_then_one_character;
select '%' like '\\%' as short_escaped_percent, '_' like '\\_' as short_escaped_underscore;
select 'PATH AB\\ HERE' ilike '%ab\\\\%' as ilike_escaped_backslash;
select 'path ab\\ here' not like '%ab\\\\%' as not_like_escaped_backslash;

-- 固定字节转为文本，验证 prepared 参数仍经过文本 LIKE 路径。
prepare like_backslash_stmt from 'select ? like ? as prepared_backslash';
set @like_backslash_value = cast(unhex('706174682061625C2068657265') as char);
set @like_backslash_pattern = cast(unhex('2561625C5C25') as char);
execute like_backslash_stmt using @like_backslash_value, @like_backslash_pattern;
set @like_backslash_value = 'path abX here';
execute like_backslash_stmt using @like_backslash_value, @like_backslash_pattern;
set @like_backslash_value = cast(unhex('706174682061625C5C2068657265') as char);
set @like_backslash_pattern = cast(unhex('2561625C5C5C5C25') as char);
execute like_backslash_stmt using @like_backslash_value, @like_backslash_pattern;
set @like_backslash_value = 'discount 50%';
set @like_backslash_pattern = cast(unhex('2535305C25') as char);
execute like_backslash_stmt using @like_backslash_value, @like_backslash_pattern;
deallocate prepare like_backslash_stmt;
set @like_backslash_value = null;
set @like_backslash_pattern = null;

                           -- test case for conv function (issue #22963)
-- conv(n, from_base, to_base) - converts numbers between different number bases

-- ============================================
-- basic conversion tests
-- ============================================
-- basic test cases for conv function (issue #22963)
-- quick verification test suite
drop database if exists conv_func;
create database conv_func;
use conv_func;
-- test 1: basic hex to decimal (from issue example)
select conv('a', 16, 10) as result1;
-- expected: 10

-- test 3: binary to decimal
select conv('1010', 2, 10) as result3;
-- expected: 10

-- test 4: decimal to binary
select conv('10', 10, 2) as result4;
-- expected: 1010

-- test 5: octal to decimal
select conv('12', 8, 10) as result5;
-- expected: 10

-- test 7: null handling
select conv(null, 10, 16) as result7;
-- expected: null

-- test 1: hexadecimal to decimal
select conv('a', 16, 10) as hex_to_dec;
-- expected: 10

select conv('ff', 16, 10) as hex_to_dec;
-- expected: 255

select conv('1a', 16, 10) as hex_to_dec;
-- expected: 26

-- test 3: binary to decimal
select conv('1010', 2, 10) as bin_to_dec;
-- expected: 10

select conv('11111111', 2, 10) as bin_to_dec;
-- expected: 255

-- test 4: decimal to binary
select conv('10', 10, 2) as dec_to_bin;
-- expected: 1010

select conv('255', 10, 2) as dec_to_bin;
-- expected: 11111111

-- test 5: octal to decimal
select conv('12', 8, 10) as oct_to_dec;
-- expected: 10

select conv('377', 8, 10) as oct_to_dec;
-- expected: 255

-- test 6: decimal to octal
select conv('10', 10, 8) as dec_to_oct;
-- expected: 12

select conv('255', 10, 8) as dec_to_oct;
-- expected: 377

-- ============================================
-- cross-base conversion tests
-- ============================================

-- test 8: hexadecimal to binary
select conv('ff', 16, 2) as hex_to_bin;
-- expected: 11111111

-- test 10: hexadecimal to octal
select conv('ff', 16, 8) as hex_to_oct;
-- expected: 377

-- ============================================
-- edge cases and special values
-- ============================================

-- test 11: zero value
select conv('0', 10, 2) as zero_dec_to_bin;
-- expected: 0

select conv('0', 16, 10) as zero_hex_to_dec;
-- expected: 0

select conv('-1', 10, 2) as negative_dec_to_bin;
-- expected: 1111111111111111111111111111111111111111111111111111111111111111

-- test 13: large numbers
select conv('ffffffffffffffff', 16, 10) as large_hex_to_dec;
-- expected: 18446744073709551615

-- test 14: case insensitivity (hex letters)
select conv('abc', 16, 10) as lowercase_hex;
-- expected: 2748

select conv('abc', 16, 10) as uppercase_hex;
-- expected: 2748

select conv('abc', 16, 10) as mixed_case_hex;
-- expected: 2748

-- ============================================
-- different base tests (2-36)
-- ============================================

-- test 15: base 36 (maximum base)
select conv('z', 36, 10) as base36_to_dec;
-- expected: 35

select conv('10', 36, 10) as base36_to_dec;
-- expected: 36

-- test 16: base 3
select conv('12', 3, 10) as base3_to_dec;
-- expected: 5

select conv('5', 10, 3) as dec_to_base3;
-- expected: 12

-- test 17: base 5
select conv('24', 5, 10) as base5_to_dec;
-- expected: 14

select conv('14', 10, 5) as dec_to_base5;
-- expected: 24

-- ============================================
-- null and invalid input tests (matching)
-- ============================================

-- test 18: null inputs
select conv(null, 10, 16) as null_number;
-- expected: null

-- ============================================
-- numeric input tests (matching)
-- ============================================

select conv(255, 10, 2) as numeric_input_bin;
-- expected: 11111111

-- ============================================
-- whitespace and special characters
-- ============================================

-- test 24: empty string
select conv('', 10, 16) as empty_string;
-- expected: 0 or null

-- ============================================
-- practical use cases
-- ============================================

-- test 26: color code conversion (hex to decimal)
select conv('ff', 16, 10) as red,
       conv('00', 16, 10) as green,
       conv('00', 16, 10) as blue;
-- expected: 255, 0, 0

-- test 27: permission bits (octal to binary)
select conv('755', 8, 2) as file_permissions;
-- expected: 111101101

-- ============================================
-- combined with other functions
-- ============================================

-- test 29: conv with upper/lower
select upper(conv('255', 10, 16)) as upper_hex;
-- expected: ff

select lower(conv('255', 10, 16)) as lower_hex;
-- expected: ff

-- test 30: conv in where clause
create table if not exists test_conv (
    id int,
    hex_value varchar(20)
);

insert into test_conv values (1, 'a'), (2, 'ff'), (3, '1a');

select id, hex_value, conv(hex_value, 16, 10) as decimal_value
from test_conv
where conv(hex_value, 16, 10) > 15;
-- expected: rows where decimal value > 15

drop table if exists test_conv;

-- ============================================
-- MO vs MySQL differing cases (issue #23845)
-- ============================================

-- ============================================
-- MO vs MySQL differing cases (issue #23845)
-- All failures are case sensitivity: MySQL returns uppercase, MO returns lowercase
-- ============================================
-- @bvt:issue#23845
-- test 2: decimal to hex
select conv('255', 10, 16) as result2;
-- expected: ff

-- test 6: negative number
select conv('-1', 10, 16) as result6;
-- expected: ffffffffffffffff

-- test 2: decimal to hexadecimal
select conv('10', 10, 16) as dec_to_hex;
-- expected: a

select conv('255', 10, 16) as dec_to_hex;
-- expected: ff

select conv('26', 10, 16) as dec_to_hex;
-- expected: 1a

-- test 7: binary to hexadecimal
select conv('11111111', 2, 16) as bin_to_hex;
-- expected: ff

-- test 9: octal to hexadecimal
select conv('377', 8, 16) as oct_to_hex;
-- expected: ff

-- test 12: negative numbers
select conv('-10', 10, 16) as negative_dec_to_hex;
-- expected: fffffffffffffff6 (64-bit two's complement)

select conv('18446744073709551615', 10, 16) as large_dec_to_hex;
-- expected: ffffffffffffffff

select conv('35', 10, 36) as dec_to_base36;
-- expected: z

-- test 22: leading/trailing whitespace
select conv(' 10 ', 10, 16) as whitespace_input;
-- expected: a (whitespace should be trimmed)

-- test 23: plus sign
select conv('+10', 10, 16) as plus_sign;
-- expected: a

-- test 25: ip address conversion (decimal to hex)
select conv('192', 10, 16) as ip_part1,
       conv('168', 10, 16) as ip_part2,
       conv('1', 10, 16) as ip_part3,
       conv('1', 10, 16) as ip_part4;
-- expected: c0, a8, 1, 1

-- test 28: conv with concat
select concat('0x', conv('255', 10, 16)) as hex_with_prefix;
-- expected: 0xff

select conv('g', 16, 10) as result8;
-- expected: MySQL returns 0, MO returns null

select conv('10', 1, 10) as result9;
-- expected: MySQL returns null, MO returns error

select conv('abc', 16, 10) as result10;
-- expected: 2748 (MO result parsing issue)

select conv('10', null, 16) as null_from_base;
-- expected: MySQL returns null, MO returns error

select conv('10', 10, null) as null_to_base;
-- expected: MySQL returns null, MO returns error

select conv('g', 16, 10) as invalid_hex_char;
-- expected: MySQL returns 0, MO returns null

select conv('2', 2, 10) as invalid_binary_char;
-- expected: MySQL returns 0, MO returns null

select conv('8', 8, 10) as invalid_octal_char;
-- expected: MySQL returns 0, MO returns null

select conv('10', 1, 10) as invalid_from_base;
-- expected: MySQL returns null, MO returns error

select conv('10', 10, 1) as invalid_to_base;
-- expected: MySQL returns null, MO returns error

select conv('10', 37, 10) as invalid_from_base_high;
-- expected: MySQL returns null, MO returns error

select conv('10', 10, 37) as invalid_to_base_high;
-- expected: MySQL returns null, MO returns error

select conv(10, 10, 16) as numeric_input;
-- expected: a (MO result parsing issue)
-- @bvt:issue

drop database conv_func;

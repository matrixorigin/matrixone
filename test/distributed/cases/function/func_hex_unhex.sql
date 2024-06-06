-- Numerical type
select hex(123);
select hex(32432);
select hex(2178342143);
select hex(2147483649);

-- Numerical type （Floating point number）
select hex(456.789);
select hex(321354.21321);
select hex(1092);
select hex(3215133.214321432);

-- Character type
select hex('abc');
select hex('qqqqq');
-- @bvt:issue#16710
select hex('abcdefghijklmnopqrstuvwxyz');
-- @bvt:issue
select hex('edwrgewgrewrwe');

-- Character type （Numeric string）
select hex('123');
select hex('4321413432');
select hex('325324213');
select hex('0000000000000');

-- Numeric string （single character）
select hex('A');
select hex('a');
select hex('V');
select hex('M');
select hex('m');

-- Numeric string （null string）
select hex('');

-- Numeric string (Special character)
select hex('!@#');
-- @bvt:issue#16710
select hex('@##%%&^^&#$#%^&^*(()#^&**(*(');
select hex('}}}{:"{:""');
-- @bvt:issue
select hex('%');

-- Special character (Blank space)
select hex(' ');

-- Chinese
-- @bvt:issue#16710
select hex('你好');
select hex('数据库');
select hex('数据库云平台');
select hex('欢迎来到 MatrixOne！');
-- @bvt:issue

-- Character type (emoji)
-- @bvt:issue#16710
select hex('😀');
-- @bvt:issue

-- null
select hex(CAST(NULL AS UNSIGNED));

-- Numeric type (negative)
select hex(-1);
select hex(-3489234);
select hex(-2147483648);
select hex(-3243123138294343);

-- Numeric type (0)
select hex(0);

-- Numeric type (large integer)
select hex(2147483647);
select hex(3278990242);

-- Numeric type (large integer, 64-bit unsigned maximum)
select hex(9223372036854775807);
select hex(3280483902382984924);

-- Mixed characters
select hex('a1b2c3d4');
select hex('12345678');
select hex('AABBCCDD');
select hex('ZZZ999');
-- @bvt:issue#16710
select hex('0123456789abcdef');
select hex('!"#$%&/()=?`~''_+-*^');
select hex(' 2543g4365435    423532543254&&*32grde5y43');
select hex('');
-- @bvt:issue

-- unhex
select unhex('7B');
select unhex('7D');
select unhex('30');
select unhex('31');
select unhex('32');
select unhex('33');
select unhex('34');
select unhex('35');
select unhex('36');
select unhex('37');
select unhex('38');
select unhex('39');
select unhex('41');
select unhex('42');
select unhex('43');
select unhex('44');
select unhex('45');
select unhex('46');
select unhex('61');
select unhex('62');
select unhex('63');
select unhex('64');
select unhex('65');
select unhex('66');
select unhex('2E');
select unhex('2C');
select unhex('21');
select unhex('22');
select unhex('25');
select unhex('26');
select unhex('2F');
select unhex('3D');
select unhex('40');
select unhex('5F');
select unhex('23');
select unhex('24');
select unhex('2A');
select unhex('28');
select unhex('29');
select unhex('3A');
select unhex('3B');
select unhex('3F');
select unhex('5E');
select unhex('60');
select unhex('7C');
select unhex('2D');
select unhex('5F');
select unhex('7E');
select unhex('5C');
select unhex('0A');
select unhex('7F');
select unhex('C3A9');
select unhex('C383');
select unhex('E28C85');
select unhex('F09F98A2');

select unhex('2032353433673433363534333534323335333235343332353426262a33326772646535793433');
select unhex('');
select unhex('2122232425262f28293d3f607e275f2b2d2a5e');
select unhex('30313233343536373839616263646566');
select unhex('4141424243434444');
select unhex('3132333435363738');
select unhex('5a5a5a393939');
select unhex('6131623263336434');

select unhex('33323830343833393032333832393834393031323833343234');

select unhex('41');
select unhex('61');
select unhex('56');
select unhex('4d');
select unhex('6d');

select unhex('313233');
select unhex('34333231343133343332');
select unhex('333235333234323133');
select unhex('30303030303030303030303030');

select unhex('333231353133332e323134333231343332');
select unhex('3332313335342e3231333231');
select unhex('3435362e373839');

select unhex('616263');
select unhex('7171717171');
select unhex('6162636465666768696a6b6c6d6e6f707172737475767778797a');
select unhex('6564777267657767726577727765');

select unhex('e4bda0e5a5bd');
select unhex('e695b0e68daee5ba93');
select unhex('e695b0e68daee5ba93e4ba91e5b9b3e58fb0');
select unhex('e6aca2e8bf8ee69da5e588b0204d61747269784f6e65efbc81');
select unhex('48656c6c6f20576f726c6421');

select unhex('214023');
select unhex('4023232525265e5e26232423255e265e2a282829235e262a2a282a28');
select unhex('7d7d7d7b3a227b3a2222');
select unhex('25');


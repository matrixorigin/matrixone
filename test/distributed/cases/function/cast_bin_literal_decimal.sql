-- Copyright 2026 Matrix Origin
--
-- Licensed under the Apache License, Version 2.0 (the "License");
-- you may not use this file except in compliance with the License.
-- You may obtain a copy of the License at
--
--     http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.

-- HEX/BIT literals keep their unsigned big-endian integer value in DECIMAL
-- (#31816): CAST applies the destination scale once, INSERT stores the same
-- value, out-of-range payloads error instead of wrapping negative, leading
-- zeros only pad, and interval/default literals follow the same decode.
-- The x'FFFFFFFFFFFFFFCD' payload (2^64-51 = 18446744073709551565) is out
-- of range for every DECIMAL64 target yet must succeed on the wider
-- carriers, so each width runs its own statement: a wide-carrier success
-- is never masked by another target's rejection, and the 16-byte
-- (2^128-1) value is rejected by DECIMAL(38,2) only on the declared bound
-- while DECIMAL(65,2) keeps its full value.
select cast(x'41' as decimal(3,1)), cast(x'41' as decimal(5,2)), cast(b'1000001' as decimal(5,0));
select cast(x'0100' as decimal(6,3)), cast(x'96' as decimal(5,2)), cast(x'00000096' as decimal(5,2));
select cast(x'41' as decimal(3,2));
select cast(x'FFFFFFFFFFFFFFCD' as decimal(18,0));
select cast(x'FFFFFFFFFFFFFFCD' as decimal(10,2));
select cast(x'FFFFFFFFFFFFFFCD' as decimal(38,2));
select cast(x'FFFFFFFFFFFFFFCD' as decimal(65,2));
select cast(x'FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF' as decimal(38,2));
select cast(x'FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF' as decimal(65,2));
select cast(x'00' as decimal(5,2)), cast(x'000000000000000096' as decimal(5,2));
drop database if exists binlit_dec;
create database binlit_dec;
use binlit_dec;
create table dec_bin(a decimal(5,2), b decimal(38,2));
insert into dec_bin values (x'96', x'96');
insert into dec_bin values (x'FFFFFFFFFFFFFFCD', x'96');
select a, b from dec_bin;
create table dec_def(a decimal(5,2) default x'96');
insert into dec_def values (default);
select a from dec_def;
select date_add('2020-01-01', interval cast(x'05' as decimal(10,2)) day), date_add('2020-01-01', interval cast(b'101' as decimal(10,2)) day), date_add('2020-01-01', interval 5 day);
drop database binlit_dec;

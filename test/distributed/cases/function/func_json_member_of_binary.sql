-- Constructor values must retain their binary subtype through MEMBER OF.
drop table if exists member_binary_constructor;
create table member_binary_constructor (
    b binary(3),
    b_other binary(3),
    v varbinary(3),
    v_other varbinary(3),
    bl blob,
    bl_other blob,
    bits bit(9),
    bits_other bit(9)
);
insert into member_binary_constructor values
    (x'000102', x'000103', x'000102', x'000103', x'000102', x'000103', 266, 267);

select
    b member of (json_array(b)) as binary_match,
    b member of (json_array(b_other)) as binary_nonmatch,
    null member of (json_array(b)) as binary_null,
    v member of (json_array(v)) as varbinary_match,
    v member of (json_array(v_other)) as varbinary_nonmatch,
    bl member of (json_array(bl)) as blob_match,
    bl member of (json_array(bl_other)) as blob_nonmatch,
    bits member of (json_array(bits)) as bit_match,
    bits member of (json_array(bits_other)) as bit_nonmatch
from member_binary_constructor;

set @member_binary_value = x'000102';
prepare member_binary_constructor_stmt from
    'select cast(? as binary(3)) member of (json_array(cast(? as binary(3)))) as prepared_binary_match';
execute member_binary_constructor_stmt using @member_binary_value, @member_binary_value;
deallocate prepare member_binary_constructor_stmt;

drop table member_binary_constructor;

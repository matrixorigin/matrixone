-- @case
-- @desc: JSON expression CAST serializes strings; assignment keeps its character payload.
-- @label:bvt

select cast(cast('"abc"' as json) as char), json_unquote(cast('"abc"' as json));
select cast('"abc"' as json) = 'abc', cast('"abc"' as json) like 'abc';
select cast(cast('"a\\"b"' as json) as varchar(20));
select cast(cast('42' as json) as char), cast(cast('null' as json) as char);

create table issue_29471_assignment(v varchar(10));
insert into issue_29471_assignment values (cast('"abc"' as json));
insert into issue_29471_assignment values (cast(cast('"abc"' as json) as char));
select v from issue_29471_assignment order by v;
drop table issue_29471_assignment;

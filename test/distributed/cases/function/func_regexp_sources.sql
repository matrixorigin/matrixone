-- @suite
-- @case
-- @desc: REGEXP static compatibility is independent of runtime operand decoding
-- @label:bvt
--- @metacmp(true)

drop database if exists regexp_operand_sources;
create database regexp_operand_sources;
use regexp_operand_sources;
create table operands(t varchar(16), b blob, f binary(3), v varbinary(16));
insert into operands values ('éa', _binary'éa', _binary'éa', _binary'éa');

-- Static column types differ from binary literals and explicit casts.
select regexp_instr(t,b), regexp_instr(t,f), regexp_instr(b,'a'), regexp_instr(f,'a') from operands;
select hex(regexp_substr(b,'.')), hex(regexp_substr(b,'.',2)), hex(regexp_substr(b,'.',2,1)) from operands;
select regexp_instr(t,v) from operands;
select regexp_like(_binary'abc','a');
select regexp_instr('abc',x'61');
select regexp_substr(cast('abc' as binary),'.');
select regexp_replace('abc','a',cast('X' as binary));
select cast(null as binary) regexp 'a';
select cast(null as char) regexp _binary'a';
select null regexp _binary'a', cast(null as binary) regexp _binary'a';
select _binary'abc' rlike _binary'a', _binary'abc' not regexp _binary'z';
prepare regexp_reject_binary from 'select regexp_instr(?, _binary''a'')';
prepare regexp_reject_nested from 'select regexp_instr(regexp_substr(?, ?), _binary''.'')';

-- A binary pattern never changes text subject character positions.
set @rs=_binary'éa', @rp=_binary'é';
select 'éa' regexp @rp, 'éa' rlike @rp, 'éa' not regexp @rp;
select regexp_like('éa',@rp), regexp_like('éa',@rp,'c');
select regexp_instr(@rs,'é'), regexp_instr('éa',@rp), regexp_instr(@rs,@rp);
select hex(regexp_substr(@rs,'é')), hex(regexp_substr('éa',@rp)), hex(regexp_substr(@rs,@rp));
set @rp=_binary'a';
select regexp_instr('éa',@rp), regexp_instr('éa',@rp,2), regexp_instr('éa',@rp,2,1), regexp_instr('éa',@rp,2,1,1);
select regexp_instr(@rs,'a'), regexp_instr(@rs,'a',2), regexp_instr(@rs,'a',2,1), regexp_instr(@rs,'a',2,1,1);
select hex(regexp_substr(@rs,'.')), hex(regexp_substr(@rs,'.',2)), hex(regexp_substr(@rs,'.',2,1));
select hex(regexp_substr('éa',@rp)), hex(regexp_substr('éa',@rp,2)), hex(regexp_substr('éa',@rp,2,1));
select hex(regexp_replace(@rs,'.','X')), hex(regexp_replace(@rs,'.','X',2)), hex(regexp_replace(@rs,'.','X',2,1));
select hex(regexp_replace('éa',@rp,'X')), hex(regexp_replace('éa',@rp,'X',2)), hex(regexp_replace('éa',@rp,'X',2,1));

-- Boundary and NULL inputs are validated for every supported value-function arity.
select regexp_instr(@rs,'a',0), regexp_substr(@rs,'.',0);
select regexp_substr(@rs,'.',4), regexp_substr(@rs,'.',1,4);
select hex(regexp_substr(@rs,'$',4)), hex(regexp_replace(@rs,'$','X',4));
select regexp_substr(@rs,'.',5);
select regexp_instr(@rs,'.',1,0), hex(regexp_substr(@rs,'.',1,-1)), hex(regexp_replace(@rs,'.','X',1,-1));
select regexp_replace(@rs,'.','X',0);
select regexp_instr(@rs,'a',null), regexp_instr(@rs,'a',1,null), regexp_instr(@rs,'a',1,1,null);
select regexp_substr(@rs,'.',null), regexp_substr(@rs,'.',1,null);
select regexp_replace(@rs,'.',null), regexp_replace(@rs,'.','X',null), regexp_replace(@rs,'.','X',1,null);
set @rs=x'ff61';
select @rs regexp 'a', regexp_like(@rs,'a'), regexp_instr(@rs,'a'), hex(regexp_substr(@rs,'.')), hex(regexp_replace(@rs,'a','X'));

-- SQL markers retain a text result charset, unlike bare binary variables.
prepare regexp_markers from 'select regexp_instr(?,?),hex(regexp_substr(?,?)),hex(regexp_replace(?,?,\'X\'))';
set @rs=_binary'éa', @rp=_binary'.';
execute regexp_markers using @rs,@rp,@rs,@rp,@rs,@rp;
set @rs='éa', @rp='.';
execute regexp_markers using @rs,@rp,@rs,@rp,@rs,@rp;
set @rs=_binary'éa', @rp=_binary'.';
execute regexp_markers using @rs,@rp,@rs,@rp,@rs,@rp;
deallocate prepare regexp_markers;
prepare regexp_fresh from 'select regexp_instr(?,?),hex(regexp_substr(?,?)),hex(regexp_replace(?,?,\'X\'))';
execute regexp_fresh using @rs,@rp,@rs,@rp,@rs,@rp;
deallocate prepare regexp_fresh;

set @rs=null, @rp=null;
drop database regexp_operand_sources;

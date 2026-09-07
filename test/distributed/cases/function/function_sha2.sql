select sha2("abc", 0);
select sha2("123", 256);
select sha2("好好学习 天天向上", 224);
select sha2("hello world", 384);
select sha2("sha512", 512);
select sha2("hello world", 66666);
select sha2("+++++-------,./;[p][]", 512);
select sha2(null, 512);
select sha2(null, null);

create table shatwo01 (a text);
insert into shatwo01 values("网络安全"),("database"),(null);
select a,sha2(a,0) from shatwo01;

drop table if exists sha2_longtext_repro;
create table sha2_longtext_repro (payload longtext not null);
insert into sha2_longtext_repro values (concat(
  repeat('x', 40000),
  repeat('x', 40000),
  repeat('x', 40000),
  repeat('x', 40000)
));
select octet_length(payload) as payload_bytes,
       sha2(payload, 256) as actual_full_digest,
       sha2(payload, 224) as actual_sha224_digest,
       sha2(payload, 384) as actual_sha384_digest,
       sha2(payload, 512) as actual_sha512_digest,
       sha2(left(payload, 65535), 256) as prefix_digest,
       octet_length(cast(payload as text)) as cast_text_bytes,
       sha2(cast(payload as text), 256) as cast_text_digest
from sha2_longtext_repro;
drop table sha2_longtext_repro;

drop table if exists sha2_mediumtext_repro;
create table sha2_mediumtext_repro (payload mediumtext not null);
insert into sha2_mediumtext_repro values (concat(repeat('a', 65535), 'b'));
select octet_length(payload) as payload_bytes,
       sha2(payload, 512) as actual_full_digest,
       sha2(left(payload, 65535), 512) as prefix_digest
from sha2_mediumtext_repro;
drop table sha2_mediumtext_repro;

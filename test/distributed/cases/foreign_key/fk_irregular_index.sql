-- Parent foreign key actions (DELETE, UPDATE and REPLACE on the parent) keep the
-- synchronous FULLTEXT, IVF_FLAT and MASTER indexes of the child rows exact: no
-- orphan entries and no missing entries after every statement.

set experimental_fulltext_index = 1;
set experimental_ivf_index = 1;
set experimental_hnsw_index = 1;

drop database if exists fk_irregular;
create database fk_irregular;
use fk_irregular;
-- A: REPLACE cascade, parent has its own fulltext index
create table rp (id int primary key, note text, fulltext rpn(note));
insert into rp values (1,'old one'),(2,'old two');
create table rc (id int primary key, pid int, body text, a varchar(10), v vecf32(2), foreign key (pid) references rp(id) on delete cascade);
create fulltext index ft on rc(body);
create index ix using ivfflat on rc(v) lists=1 op_type 'vector_l2_ops';
create index ms using master on rc(a);
insert into rc values (10,1,'apple','x1','[1,0]'),(20,2,'banana','x2','[0,1]');
set @crc_ft = (select index_table_name from mo_catalog.mo_indexes where name = 'ft' and table_id in (select rel_id from mo_catalog.mo_tables where reldatabase = database() and relname = 'rc') limit 1);
set @crc_ivf = (select index_table_name from mo_catalog.mo_indexes where name = 'ix' and algo_table_type = 'entries' and table_id in (select rel_id from mo_catalog.mo_tables where reldatabase = database() and relname = 'rc') limit 1);
set @crc_ms = (select index_table_name from mo_catalog.mo_indexes where name = 'ms' and table_id in (select rel_id from mo_catalog.mo_tables where reldatabase = database() and relname = 'rc') limit 1);
set @crc_sql = concat('select ''ft'' as idx, (select count(distinct doc_id) from `', database(), '`.`', @crc_ft, '` where doc_id not in (select id from rc)) as orphan, (select count(*) from rc where id not in (select doc_id from `', database(), '`.`', @crc_ft, '`)) as missing', ' union all ', 'select ''ivf'', (select count(*) from `', database(), '`.`', @crc_ivf, '` where __mo_index_pri_col not in (select id from rc)), (select count(*) from rc where id not in (select __mo_index_pri_col from `', database(), '`.`', @crc_ivf, '`))', ' union all ', 'select ''ms'', (select count(*) from `', database(), '`.`', @crc_ms, '` where __mo_index_pri_col not in (select id from rc)), (select count(*) from rc where id not in (select __mo_index_pri_col from `', database(), '`.`', @crc_ms, '`))', ' order by idx');
prepare crc from @crc_sql;
set @crp_ft = (select index_table_name from mo_catalog.mo_indexes where name = 'rpn' and table_id in (select rel_id from mo_catalog.mo_tables where reldatabase = database() and relname = 'rp') limit 1);
set @crp_sql = concat('select ''ft'' as idx, (select count(distinct doc_id) from `', database(), '`.`', @crp_ft, '` where doc_id not in (select id from rp)) as orphan, (select count(*) from rp where id not in (select doc_id from `', database(), '`.`', @crp_ft, '`)) as missing', ' order by idx');
prepare crp from @crp_sql;
replace into rp values (1,'new one');
execute crc;
execute crp;
select id from rp where match(note) against('new');
select id from rc order by id;
-- B: REPLACE with SET NULL child
create table sp (code varchar(10) primary key, note text);
insert into sp values ('c1','x'),('c2','y');
create table sc (id int primary key, code varchar(10), body text, v vecf32(2), foreign key (code) references sp(code) on delete set null);
create fulltext index ft2 on sc(body, code);
create index ix2 using ivfflat on sc(v) lists=1 op_type 'vector_l2_ops';
create index ms2 using master on sc(code);
insert into sc values (1,'c1','cat','[1,0]'),(2,'c2','dog','[0,1]');
set @csc_ft = (select index_table_name from mo_catalog.mo_indexes where name = 'ft2' and table_id in (select rel_id from mo_catalog.mo_tables where reldatabase = database() and relname = 'sc') limit 1);
set @csc_ivf = (select index_table_name from mo_catalog.mo_indexes where name = 'ix2' and algo_table_type = 'entries' and table_id in (select rel_id from mo_catalog.mo_tables where reldatabase = database() and relname = 'sc') limit 1);
set @csc_ms = (select index_table_name from mo_catalog.mo_indexes where name = 'ms2' and table_id in (select rel_id from mo_catalog.mo_tables where reldatabase = database() and relname = 'sc') limit 1);
set @csc_sql = concat('select ''ft'' as idx, (select count(distinct doc_id) from `', database(), '`.`', @csc_ft, '` where doc_id not in (select id from sc)) as orphan, (select count(*) from sc where id not in (select doc_id from `', database(), '`.`', @csc_ft, '`)) as missing', ' union all ', 'select ''ivf'', (select count(*) from `', database(), '`.`', @csc_ivf, '` where __mo_index_pri_col not in (select id from sc)), (select count(*) from sc where id not in (select __mo_index_pri_col from `', database(), '`.`', @csc_ivf, '`))', ' union all ', 'select ''ms'', (select count(*) from `', database(), '`.`', @csc_ms, '` where __mo_index_pri_col not in (select id from sc)), (select count(*) from sc where id not in (select __mo_index_pri_col from `', database(), '`.`', @csc_ms, '`))', ' order by idx');
prepare csc from @csc_sql;
replace into sp values ('c1','z');
execute csc;
select id, code from sc order by id;
select id from sc where match(body, code) against('cat');
select id from sc where code is null;
-- C: UPDATE parent -> ON UPDATE CASCADE (child pk includes fk col) and SET NULL
create table up (id int primary key, code varchar(10) unique);
insert into up values (1,'u1'),(2,'u2');
create table uc (pid int, id int, code varchar(10), body text, v vecf32(2), primary key (pid, id),
  foreign key (pid) references up(id) on update cascade, foreign key (code) references up(code) on update set null);
create fulltext index ft3 on uc(body, code);
create index ix3 using ivfflat on uc(v) lists=1 op_type 'vector_l2_ops';
create index ms3 using master on uc(code);
insert into uc values (1,1,'u1','red','[1,0]'),(1,2,'u2','green','[0,1]'),(2,1,'u2','blue','[1,1]');
set @cuc_ft = (select index_table_name from mo_catalog.mo_indexes where name = 'ft3' and table_id in (select rel_id from mo_catalog.mo_tables where reldatabase = database() and relname = 'uc') limit 1);
set @cuc_ivf = (select index_table_name from mo_catalog.mo_indexes where name = 'ix3' and algo_table_type = 'entries' and table_id in (select rel_id from mo_catalog.mo_tables where reldatabase = database() and relname = 'uc') limit 1);
set @cuc_ms = (select index_table_name from mo_catalog.mo_indexes where name = 'ms3' and table_id in (select rel_id from mo_catalog.mo_tables where reldatabase = database() and relname = 'uc') limit 1);
set @cuc_sql = concat('select ''ft'' as idx, (select count(distinct doc_id) from `', database(), '`.`', @cuc_ft, '` where doc_id not in (select serial(pid, id) from uc)) as orphan, (select count(*) from uc where serial(pid, id) not in (select doc_id from `', database(), '`.`', @cuc_ft, '`)) as missing', ' union all ', 'select ''ivf'', (select count(*) from `', database(), '`.`', @cuc_ivf, '` where __mo_index_pri_col not in (select serial(pid, id) from uc)), (select count(*) from uc where serial(pid, id) not in (select __mo_index_pri_col from `', database(), '`.`', @cuc_ivf, '`))', ' union all ', 'select ''ms'', (select count(*) from `', database(), '`.`', @cuc_ms, '` where __mo_index_pri_col not in (select serial(pid, id) from uc)), (select count(*) from uc where serial(pid, id) not in (select __mo_index_pri_col from `', database(), '`.`', @cuc_ms, '`))', ' order by idx');
prepare cuc from @cuc_sql;
update up set id = 5 where id = 1;
execute cuc;
select pid, id, code from uc order by pid, id;
select pid, id from uc where match(body, code) against('green');
update up set code = 'u9' where id = 2;
execute cuc;
select pid, id, code from uc order by pid, id;
select pid, id from uc where code is null order by pid, id;
select pid, id from uc order by l2_distance(v, '[1,0]') limit 1;
-- D: CASCADE + SET NULL from the same parent row, with a regular secondary index
create table dp (id int primary key, code varchar(10) unique);
insert into dp values (1,'d1'),(2,'d2'),(3,'d3');
create table dc (pid int, id int, code varchar(10), a varchar(10), body text, v vecf32(2), primary key (pid, id),
  foreign key (pid) references dp(id) on delete cascade, foreign key (code) references dp(code) on delete set null, key k_a (a));
create fulltext index ft4 on dc(body, code);
create index ix4 using ivfflat on dc(v) lists=1 op_type 'vector_l2_ops';
create index ms4 using master on dc(code);
insert into dc values (1,1,'d1','x1','one','[1,0]'),(1,2,'d2','x2','two','[0,1]'),(2,1,'d3','x3','three','[1,1]'),(3,1,'d2','x4','four','[2,2]');
set @chk_dc_ft = (select index_table_name from mo_catalog.mo_indexes where name = 'ft4' and table_id in (select rel_id from mo_catalog.mo_tables where reldatabase = database() and relname = 'dc') limit 1);
set @chk_dc_ivf = (select index_table_name from mo_catalog.mo_indexes where name = 'ix4' and algo_table_type = 'entries' and table_id in (select rel_id from mo_catalog.mo_tables where reldatabase = database() and relname = 'dc') limit 1);
set @chk_dc_ms = (select index_table_name from mo_catalog.mo_indexes where name = 'ms4' and table_id in (select rel_id from mo_catalog.mo_tables where reldatabase = database() and relname = 'dc') limit 1);
set @chk_dc_sql = concat('select ''ft'' as idx, (select count(distinct doc_id) from `', database(), '`.`', @chk_dc_ft, '` where doc_id not in (select serial(pid, id) from dc)) as orphan, (select count(*) from dc where serial(pid, id) not in (select doc_id from `', database(), '`.`', @chk_dc_ft, '`)) as missing', ' union all ', 'select ''ivf'', (select count(*) from `', database(), '`.`', @chk_dc_ivf, '` where __mo_index_pri_col not in (select serial(pid, id) from dc)), (select count(*) from dc where serial(pid, id) not in (select __mo_index_pri_col from `', database(), '`.`', @chk_dc_ivf, '`))', ' union all ', 'select ''ms'', (select count(*) from `', database(), '`.`', @chk_dc_ms, '` where __mo_index_pri_col not in (select serial(pid, id) from dc)), (select count(*) from dc where serial(pid, id) not in (select __mo_index_pri_col from `', database(), '`.`', @chk_dc_ms, '`))', ' order by idx');
prepare chk_dc from @chk_dc_sql;
delete from dp where id = 2;
execute chk_dc;
select pid, id, code from dc order by pid, id;
select pid, id from dc where a = 'x2';
select pid, id from dc where a = 'x4';
-- E: async-only child, UPDATE parent cascade
create table ep (id int primary key);
insert into ep values (1),(2);
create table ec (id bigint primary key, pid int, v vecf32(2), foreign key (pid) references ep(id) on update cascade);
create index hv using hnsw on ec(v) op_type 'vector_l2_ops';
insert into ec values (1,1,'[1,0]'),(2,2,'[0,1]');
update ep set id = 3 where id = 1;
select id, pid from ec order by id;

-- F: two SET NULL FKs + CASCADE on the same parent (combined path)
create table fp (id int primary key, c1 varchar(10) unique, c2 varchar(10) unique);
insert into fp values (1,'a1','b1'),(2,'a2','b2');
create table fc (id int primary key, pid int, x varchar(10), y varchar(10), body text, v vecf32(2),
  foreign key (pid) references fp(id) on delete cascade,
  foreign key (x) references fp(c1) on delete set null,
  foreign key (y) references fp(c2) on delete set null, key k_x (x));
create fulltext index ft on fc(body, x);
create index ix using ivfflat on fc(v) lists=1 op_type 'vector_l2_ops';
create index ms using master on fc(x, y);
insert into fc values (1,2,'a1','b1','one','[1,0]'),(2,1,'a2','b1','two','[0,1]'),(3,1,'a1','b2','three','[1,1]'),(4,2,'a2','b2','four','[2,2]');
set @chk_f_ft = (select index_table_name from mo_catalog.mo_indexes where name = 'ft' and table_id in (select rel_id from mo_catalog.mo_tables where reldatabase = database() and relname = 'fc') limit 1);
set @chk_f_ivf = (select index_table_name from mo_catalog.mo_indexes where name = 'ix' and algo_table_type = 'entries' and table_id in (select rel_id from mo_catalog.mo_tables where reldatabase = database() and relname = 'fc') limit 1);
set @chk_f_ms = (select index_table_name from mo_catalog.mo_indexes where name = 'ms' and table_id in (select rel_id from mo_catalog.mo_tables where reldatabase = database() and relname = 'fc') limit 1);
set @chk_f_sql = concat('select ''ft'' as idx, (select count(distinct doc_id) from `', database(), '`.`', @chk_f_ft, '` where doc_id not in (select id from fc)) as orphan, (select count(*) from fc where id not in (select doc_id from `', database(), '`.`', @chk_f_ft, '`)) as missing', ' union all ', 'select ''ivf'', (select count(*) from `', database(), '`.`', @chk_f_ivf, '` where __mo_index_pri_col not in (select id from fc)), (select count(*) from fc where id not in (select __mo_index_pri_col from `', database(), '`.`', @chk_f_ivf, '`))', ' union all ', 'select ''ms'', (select count(*) from `', database(), '`.`', @chk_f_ms, '` where __mo_index_pri_col not in (select id from fc)), (select count(*) from fc where id not in (select __mo_index_pri_col from `', database(), '`.`', @chk_f_ms, '`))', ' order by idx');
prepare chk_f from @chk_f_sql;

delete from fp where id = 2;
execute chk_f;
select id, pid, x, y from fc order by id;
select id from fc where x is null and y = 'b1';
-- G: self-referencing table with fulltext + ivf, UPDATE root pk cascades to children
create table g (id int primary key, parent int, body text, v vecf32(2), foreign key (parent) references g(id) on update cascade on delete cascade);
create fulltext index ftg on g(body);
create index ixg using ivfflat on g(v) lists=1 op_type 'vector_l2_ops';
insert into g values (1,null,'root','[1,0]'),(2,1,'kid','[0,1]'),(3,2,'grandkid','[1,1]');
set @chk_g_ft = (select index_table_name from mo_catalog.mo_indexes where name = 'ftg' and table_id in (select rel_id from mo_catalog.mo_tables where reldatabase = database() and relname = 'g') limit 1);
set @chk_g_ivf = (select index_table_name from mo_catalog.mo_indexes where name = 'ixg' and algo_table_type = 'entries' and table_id in (select rel_id from mo_catalog.mo_tables where reldatabase = database() and relname = 'g') limit 1);
set @chk_g_sql = concat('select ''ft'' as idx, (select count(distinct doc_id) from `', database(), '`.`', @chk_g_ft, '` where doc_id not in (select id from g)) as orphan, (select count(*) from g where id not in (select doc_id from `', database(), '`.`', @chk_g_ft, '`)) as missing', ' union all ', 'select ''ivf'', (select count(*) from `', database(), '`.`', @chk_g_ivf, '` where __mo_index_pri_col not in (select id from g)), (select count(*) from g where id not in (select __mo_index_pri_col from `', database(), '`.`', @chk_g_ivf, '`))', ' order by idx');
prepare chk_g from @chk_g_sql;

update g set id = 10 where id = 1;
execute chk_g;
select id, parent from g order by id;
delete from g where id = 10;
execute chk_g;
select count(*) from g;
-- H: two-level cascade through REPLACE
create table h1 (id int primary key);
create table h2 (id int primary key, p int, body text, foreign key (p) references h1(id) on delete cascade, fulltext f2(body));
create table h3 (id int primary key, p int, body text, v vecf32(2), foreign key (p) references h2(id) on delete cascade);
create fulltext index f3 on h3(body);
create index i3 using ivfflat on h3(v) lists=1 op_type 'vector_l2_ops';
insert into h1 values (1),(2);
insert into h2 values (10,1,'aa'),(20,2,'bb');
insert into h3 values (100,10,'xx','[1,0]'),(200,20,'yy','[0,1]');
set @chk_h2_ft = (select index_table_name from mo_catalog.mo_indexes where name = 'f2' and table_id in (select rel_id from mo_catalog.mo_tables where reldatabase = database() and relname = 'h2') limit 1);
set @chk_h2_sql = concat('select ''ft'' as idx, (select count(distinct doc_id) from `', database(), '`.`', @chk_h2_ft, '` where doc_id not in (select id from h2)) as orphan, (select count(*) from h2 where id not in (select doc_id from `', database(), '`.`', @chk_h2_ft, '`)) as missing', ' order by idx');
prepare chk_h2 from @chk_h2_sql;
set @chk_h3_ft = (select index_table_name from mo_catalog.mo_indexes where name = 'f3' and table_id in (select rel_id from mo_catalog.mo_tables where reldatabase = database() and relname = 'h3') limit 1);
set @chk_h3_ivf = (select index_table_name from mo_catalog.mo_indexes where name = 'i3' and algo_table_type = 'entries' and table_id in (select rel_id from mo_catalog.mo_tables where reldatabase = database() and relname = 'h3') limit 1);
set @chk_h3_sql = concat('select ''ft'' as idx, (select count(distinct doc_id) from `', database(), '`.`', @chk_h3_ft, '` where doc_id not in (select id from h3)) as orphan, (select count(*) from h3 where id not in (select doc_id from `', database(), '`.`', @chk_h3_ft, '`)) as missing', ' union all ', 'select ''ivf'', (select count(*) from `', database(), '`.`', @chk_h3_ivf, '` where __mo_index_pri_col not in (select id from h3)), (select count(*) from h3 where id not in (select __mo_index_pri_col from `', database(), '`.`', @chk_h3_ivf, '`))', ' order by idx');
prepare chk_h3 from @chk_h3_sql;

replace into h1 values (1);
execute chk_h2;
execute chk_h3;
select id from h2 order by id;
select id from h3 order by id;

-- I: REPLACE on a self-referencing table cascades to rows of the same table
create table s (id int primary key, parent int, body text, a varchar(10), v vecf32(2), foreign key (parent) references s(id) on delete cascade);
create fulltext index fts on s(body);
create index ixs using ivfflat on s(v) lists=1 op_type 'vector_l2_ops';
create index mss using master on s(a);
insert into s values (1,null,'root','r','[1,0]'),(2,1,'kid','k','[0,1]'),(3,2,'gk','g','[1,1]'),(4,null,'solo','s','[2,2]');
set @chk_s_ft = (select index_table_name from mo_catalog.mo_indexes where name = 'fts' and table_id in (select rel_id from mo_catalog.mo_tables where reldatabase = database() and relname = 's') limit 1);
set @chk_s_ivf = (select index_table_name from mo_catalog.mo_indexes where name = 'ixs' and algo_table_type = 'entries' and table_id in (select rel_id from mo_catalog.mo_tables where reldatabase = database() and relname = 's') limit 1);
set @chk_s_ms = (select index_table_name from mo_catalog.mo_indexes where name = 'mss' and table_id in (select rel_id from mo_catalog.mo_tables where reldatabase = database() and relname = 's') limit 1);
set @chk_s_sql = concat('select ''ft'' as idx, (select count(distinct doc_id) from `', database(), '`.`', @chk_s_ft, '` where doc_id not in (select id from s)) as orphan, (select count(*) from s where id not in (select doc_id from `', database(), '`.`', @chk_s_ft, '`)) as missing', ' union all ', 'select ''ivf'', (select count(*) from `', database(), '`.`', @chk_s_ivf, '` where __mo_index_pri_col not in (select id from s)), (select count(*) from s where id not in (select __mo_index_pri_col from `', database(), '`.`', @chk_s_ivf, '`))', ' union all ', 'select ''ms'', (select count(*) from `', database(), '`.`', @chk_s_ms, '` where __mo_index_pri_col not in (select id from s)), (select count(*) from s where id not in (select __mo_index_pri_col from `', database(), '`.`', @chk_s_ms, '`))', ' order by idx');
prepare chk_s from @chk_s_sql;

replace into s values (1,null,'newroot','r2','[3,3]');
execute chk_s;
select id, body from s order by id;
replace into s values (4,null,'solo2','s2','[4,4]'), (5,4,'child5','c5','[5,5]');
execute chk_s;
select id, body from s order by id;
replace into s values (4,null,'solo3','s3','[6,6]'), (5,4,'child5b','c6','[7,7]');
execute chk_s;
select id, body from s order by id;
select id from s where match(body) against('child5b');

-- J: self-referencing ON UPDATE CASCADE changes an indexed non-key column
create table j (id int primary key, code varchar(10) unique, pcode varchar(10), body text, v vecf32(2),
  foreign key (pcode) references j(code) on update cascade);
create fulltext index ftj on j(body, pcode);
create index ixj using ivfflat on j(v) lists=1 op_type 'vector_l2_ops';
create index msj using master on j(pcode);
insert into j values (1,'A',null,'root','[1,0]'),(2,'B','A','kid','[0,1]'),(3,'C','B','gk','[1,1]');
set @chk_j_ft = (select index_table_name from mo_catalog.mo_indexes where name = 'ftj' and table_id in (select rel_id from mo_catalog.mo_tables where reldatabase = database() and relname = 'j') limit 1);
set @chk_j_ivf = (select index_table_name from mo_catalog.mo_indexes where name = 'ixj' and algo_table_type = 'entries' and table_id in (select rel_id from mo_catalog.mo_tables where reldatabase = database() and relname = 'j') limit 1);
set @chk_j_ms = (select index_table_name from mo_catalog.mo_indexes where name = 'msj' and table_id in (select rel_id from mo_catalog.mo_tables where reldatabase = database() and relname = 'j') limit 1);
set @chk_j_sql = concat('select ''ft'' as idx, (select count(distinct doc_id) from `', database(), '`.`', @chk_j_ft, '` where doc_id not in (select id from j)) as orphan, (select count(*) from j where id not in (select doc_id from `', database(), '`.`', @chk_j_ft, '`)) as missing', ' union all ', 'select ''ivf'', (select count(*) from `', database(), '`.`', @chk_j_ivf, '` where __mo_index_pri_col not in (select id from j)), (select count(*) from j where id not in (select __mo_index_pri_col from `', database(), '`.`', @chk_j_ivf, '`))', ' union all ', 'select ''ms'', (select count(*) from `', database(), '`.`', @chk_j_ms, '` where __mo_index_pri_col not in (select id from j)), (select count(*) from j where id not in (select __mo_index_pri_col from `', database(), '`.`', @chk_j_ms, '`))', ' order by idx');
prepare chk_j from @chk_j_sql;

update j set code = 'Z' where code = 'A';
execute chk_j;
select id, pcode from j order by id;
select id from j where match(body, pcode) against('Z');
select count(*) from j where match(body, pcode) against('A');
select id from j where pcode = 'Z';

-- K: DELETE cascades to a child and sets NULL on a grandchild
create table kp (id int primary key);
create table kc (id int primary key, p int, foreign key (p) references kp(id) on delete cascade);
create table kg (gid int primary key, pid int, body text, v vecf32(2), a varchar(10), foreign key (pid) references kc(id) on delete set null);
create fulltext index ftk on kg(body, a);
create index ixk using ivfflat on kg(v) lists=1 op_type 'vector_l2_ops';
create index msk using master on kg(a);
insert into kp values (1),(2);
insert into kc values (10,1),(20,2);
insert into kg values (100,10,'gx','[1,0]','g1'),(200,20,'gy','[0,1]','g2');
set @chk_k_ft = (select index_table_name from mo_catalog.mo_indexes where name = 'ftk' and table_id in (select rel_id from mo_catalog.mo_tables where reldatabase = database() and relname = 'kg') limit 1);
set @chk_k_ivf = (select index_table_name from mo_catalog.mo_indexes where name = 'ixk' and algo_table_type = 'entries' and table_id in (select rel_id from mo_catalog.mo_tables where reldatabase = database() and relname = 'kg') limit 1);
set @chk_k_ms = (select index_table_name from mo_catalog.mo_indexes where name = 'msk' and table_id in (select rel_id from mo_catalog.mo_tables where reldatabase = database() and relname = 'kg') limit 1);
set @chk_k_sql = concat('select ''ft'' as idx, (select count(distinct doc_id) from `', database(), '`.`', @chk_k_ft, '` where doc_id not in (select gid from kg)) as orphan, (select count(*) from kg where gid not in (select doc_id from `', database(), '`.`', @chk_k_ft, '`)) as missing', ' union all ', 'select ''ivf'', (select count(*) from `', database(), '`.`', @chk_k_ivf, '` where __mo_index_pri_col not in (select gid from kg)), (select count(*) from kg where gid not in (select __mo_index_pri_col from `', database(), '`.`', @chk_k_ivf, '`))', ' union all ', 'select ''ms'', (select count(*) from `', database(), '`.`', @chk_k_ms, '` where __mo_index_pri_col not in (select gid from kg)), (select count(*) from kg where gid not in (select __mo_index_pri_col from `', database(), '`.`', @chk_k_ms, '`))', ' order by idx');
prepare chk_k from @chk_k_sql;

delete from kp where id = 2;
execute chk_k;
select gid, pid from kg order by gid;
select gid from kg where match(body, a) against('gy');
select gid from kg where a = 'g2';
select gid from kg order by l2_distance(v, '[0,1]') limit 1;

set experimental_fulltext_index = 0;
set experimental_ivf_index = 0;
set experimental_hnsw_index = 0;
drop database fk_irregular;

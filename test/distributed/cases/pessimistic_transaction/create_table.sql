create database if not exists test;
use test;
drop table if exists t;
begin;
create table t(a int);
alter table t add column b int;
commit;
select * from t;
drop database test;


create database if not exists ry_flowable;
use ry_flowable;
SET autocommit=0;

create table ACT_RU_IDENTITYLINK (
                                     ID_ varchar(64),
                                     REV_ integer,
                                     GROUP_ID_ varchar(255),
                                     TYPE_ varchar(255),
                                     USER_ID_ varchar(255),
                                     TASK_ID_ varchar(64),
                                     PROC_INST_ID_ varchar(64),
                                     PROC_DEF_ID_ varchar(64),
                                     SCOPE_ID_ varchar(255),
                                     SUB_SCOPE_ID_ varchar(255),
                                     SCOPE_TYPE_ varchar(255),
                                     SCOPE_DEFINITION_ID_ varchar(255),
                                     primary key (ID_)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE utf8_bin;

create index ACT_IDX_IDENT_LNK_USER on ACT_RU_IDENTITYLINK(USER_ID_);
create index ACT_IDX_IDENT_LNK_GROUP on ACT_RU_IDENTITYLINK(GROUP_ID_);
create index ACT_IDX_IDENT_LNK_SCOPE on ACT_RU_IDENTITYLINK(SCOPE_ID_, SCOPE_TYPE_);
create index ACT_IDX_IDENT_LNK_SUB_SCOPE on ACT_RU_IDENTITYLINK(SUB_SCOPE_ID_, SCOPE_TYPE_);

desc ACT_RU_IDENTITYLINK;

SELECT @@session.transaction_read_only;

SELECT TABLE_SCHEMA AS TABLE_CAT,
       NULL AS TABLE_SCHEM,
       TABLE_NAME,
       CASE
           WHEN TABLE_TYPE = 'BASE TABLE' THEN
               CASE
                   WHEN TABLE_SCHEMA = 'mysql'
                       OR TABLE_SCHEMA = 'mo_catalog' THEN 'SYSTEM TABLE'
                   ELSE 'TABLE'
                   END
           WHEN TABLE_TYPE = 'TEMPORARY' THEN 'LOCAL_TEMPORARY'
           ELSE TABLE_TYPE
           END AS TABLE_TYPE,
       TABLE_COMMENT AS REMARKS,
       NULL AS TYPE_CAT,
       NULL AS TYPE_SCHEM,
       NULL AS TYPE_NAME,
       NULL AS SELF_REFERENCING_COL_NAME,
       NULL AS REF_GENERATION
FROM INFORMATION_SCHEMA.TABLES
WHERE TABLE_SCHEMA = 'ry_flowable' AND TABLE_NAME LIKE 'act_ru_identitylink';

commit;
show tables;
drop database ry_flowable;

drop database if exists test_delete;
create database test_delete;
use test_delete;
create table `update_controller_portal5`(
    `id` int not null comment 'id',
    `mo_table_name` varchar(63) default null,
    `dataset_name` varchar(63) not null comment '知识库名称',
    `document_name` varchar(63) not null comment '文档名称',
    `status` int default null comment '是否调动，1为调动',
    `doc_type` int default null,
    `target_sql` text not null comment '同步目标sql',
    `table_name` varchar(255) not null comment '主表名',
    `keys` varchar(255) not null comment '联合键',
    `create_at` datetime default current_timestamp(0),
    `update_at` datetime default current_timestamp(0),
    `dataset_id` varchar(255) default null comment '知识库id',
    `ducoment_id` varchar(255) default null comment '文档id',
    primary key (`id`),
    KEY `this_id`(`id`)
);
insert into update_controller_portal5 values (13, 'aaa', 'aaa', 'aaa', 1, 1, 'select * from table01', 'primary_table', 'key01', '2020-10-10 11:11:11', '2022-10-10 12:12:12', 'dataset', 'document');
select * from update_controller_portal5;
delete from update_controller_portal5 where id = 13;
select * from update_controller_portal5;

drop table if exists t1;
create table t1 (a int primary key, b int, c int, d int, e int, f int, g int, key b_idx(b), key c_idx(c), key d_idx(d), key e_idx(d), key f_idx(f), key g_idx(g));
insert into t1 select *,*,*,*,*,*,* from generate_series(0,2000000,1)g;
delete from t1 where a < 1000002 and a > 1000000;
drop database test_delete;

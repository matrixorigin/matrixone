drop database if exists fk_name_case;
create database fk_name_case;
use fk_name_case;

create table parent_t (id bigint primary key);
create table child_t (
    parent_id bigint,
    constraint MixedFK foreign key (parent_id) references parent_t(id)
);
show create table child_t;

alter table child_t drop foreign key MixedFK;
show create table child_t;

create table duplicate_child_t (
    parent_id bigint,
    constraint MixedFK foreign key (parent_id) references parent_t(id),
    constraint mixedfk foreign key (parent_id) references parent_t(id)
);

drop database fk_name_case;

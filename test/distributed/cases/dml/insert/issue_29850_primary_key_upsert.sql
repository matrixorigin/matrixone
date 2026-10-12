-- Regression for direct primary-key assignments emitted by MySQL-style ODKU clients.
-- Only a runtime-equal PK assignment is accepted; actual mutations and ambiguous
-- secondary-UNIQUE arbitration remain unsupported.

drop table if exists t_issue_29850_pk;
create table t_issue_29850_pk (id int primary key, payload varchar(40));
insert into t_issue_29850_pk values (1, 'seed');

-- Existing VALUES(pk) no-op behavior remains supported.
insert into t_issue_29850_pk values (1, 'values-control')
    on duplicate key update id = values(id), payload = values(payload);
select * from t_issue_29850_pk order by id;

-- A direct literal matching the conflicting key is a runtime no-op.
insert into t_issue_29850_pk values (1, 'literal-equal')
    on duplicate key update id = (1), payload = values(payload);
select * from t_issue_29850_pk order by id;

-- A different direct literal is ignored on the fresh-insert path.
insert into t_issue_29850_pk values (2, 'fresh')
    on duplicate key update id = 99, payload = values(payload);
select * from t_issue_29850_pk order by id;

-- Repeated incoming keys in one statement use the first inserted row as the
-- target for the second action; equal key assignments remain valid.
insert into t_issue_29850_pk values (10, 'first'), (10, 'second')
    on duplicate key update id = 10, payload = values(payload);
select * from t_issue_29850_pk order by id;

-- A late runtime PK mismatch fails the statement, including earlier row writes
-- and non-PK assignments from the same statement.
insert into t_issue_29850_pk values (3, 'would-insert'), (1, 'would-update')
    on duplicate key update payload = values(payload), id = 99;
select * from t_issue_29850_pk order by id;

-- Assignment order matters: an intermediate primary-key mutation is rejected
-- even when a later assignment restores the original key.
insert into t_issue_29850_pk values (1, 'repeated-would-update')
    on duplicate key update id = 2, id = 1, payload = values(payload);
select * from t_issue_29850_pk order by id;

-- Each component of a composite key is guarded against the old conflicting row.
drop table if exists t_issue_29850_composite;
create table t_issue_29850_composite (a int, b int, payload varchar(40), primary key (a, b));
insert into t_issue_29850_composite values (1, 2, 'seed');
insert into t_issue_29850_composite values (1, 2, 'equal')
    on duplicate key update a = 1, b = 2, payload = values(payload);
select * from t_issue_29850_composite order by a, b;
insert into t_issue_29850_composite values (1, 2, 'would-update')
    on duplicate key update a = 1, b = 3, payload = values(payload);
select * from t_issue_29850_composite order by a, b;

-- A secondary UNIQUE arbiter can select a row with a different primary key;
-- keep the existing static rejection in that ambiguous case.
drop table if exists t_issue_29850_secondary;
create table t_issue_29850_secondary (id int primary key, uk int unique, payload varchar(40));
insert into t_issue_29850_secondary values (1, 10, 'seed');
insert into t_issue_29850_secondary values (2, 10, 'ambiguous')
    on duplicate key update id = values(id), payload = values(payload);
select * from t_issue_29850_secondary order by id;

-- A guarded PK no-op must not trigger an implicit ON UPDATE timestamp, while
-- changing another column still triggers it.
drop table if exists t_issue_29850_timestamp;
create table t_issue_29850_timestamp (
  id int primary key,
  payload int,
  updated_at timestamp default '2000-01-01 00:00:00' on update current_timestamp
);
insert into t_issue_29850_timestamp(id, payload) values (1, 10);
insert into t_issue_29850_timestamp(id, payload) values (1, 10)
    on duplicate key update id = 1;
select payload, updated_at = '2000-01-01 00:00:00' as timestamp_unchanged
    from t_issue_29850_timestamp where id = 1;
insert into t_issue_29850_timestamp(id, payload) values (1, 20)
    on duplicate key update id = 1, payload = values(payload);
select payload, updated_at > '2000-01-01 00:00:00' as timestamp_advanced
    from t_issue_29850_timestamp where id = 1;

drop table if exists t_issue_29850_timestamp;
drop table if exists t_issue_29850_secondary;
drop table if exists t_issue_29850_composite;
drop table if exists t_issue_29850_pk;

drop database if exists regular_index_point_skew;
create database regular_index_point_skew;
use regular_index_point_skew;

create table runs (
  id bigint primary key,
  workspace_id int not null,
  workflow_id int not null,
  task_id int not null,
  created_at bigint not null,
  body varchar(256) not null,
  index ix_flow (workspace_id, workflow_id),
  index ix_task (task_id)
);

-- Use a multi-block insert so the planner can compare persisted point ranges.
-- The correlated flow prefix covers most rows, while the rare task has one row.
insert into runs
select result,
       case when result <> 4 and mod(result * 79, 50000) < 37500 then 1 else result end,
       case when result <> 5 and mod(result * 79, 50000) < 37500 then 1 else result end,
       case when result = 6000 then 100010
            when mod(result * 79, 50000) < 5500 then 7
            else mod(result * 79, 256) + 10 end,
       case when result in (4, 5) then -1
            when result in (2, 3) then 0 else mod(result * 173, 20003) + 1 end,
       repeat('x', 200)
from generate_series(1, 50000) g;

-- Read the three persisted key columns before the first competing lookup.
select sum(workspace_id), sum(workflow_id), sum(task_id) from runs;

-- @regex("Index Table Scan on runs[.]ix_task",true)
-- @regex("Join Type: INDEX",true)
-- @regex("Filter Cond: prefix_eq[(]__mo_index_idx_col[)]",true)
-- @regex("Sort Key: runs[.]created_at INTERNAL, runs[.]id INTERNAL",true)
-- @regex("Limit: 1",true)
explain select * from runs
where workspace_id = 1 and workflow_id = 1 and task_id = 100010
order by created_at, id limit 1;

select * from runs
where workspace_id = 1 and workflow_id = 1 and task_id = 100010
order by created_at, id limit 1;

select * from runs ignore index (ix_flow)
where workspace_id = 1 and workflow_id = 1 and task_id = 100010
order by created_at, id limit 1;

-- Two dense rows tie on the earliest matching time. Each earlier decoy
-- fails a different flow predicate.
-- @regex("Sort Key: runs[.]created_at INTERNAL, runs[.]id INTERNAL",true)
-- @regex("Limit: 1",true)
explain select * from runs
where workspace_id = 1 and workflow_id = 1 and task_id = 7
order by created_at, id limit 1;

select * from runs
where workspace_id = 1 and workflow_id = 1 and task_id = 7
order by created_at, id limit 1;

select * from runs ignore index (ix_flow)
where workspace_id = 1 and workflow_id = 1 and task_id = 7
order by created_at, id limit 1;

truncate table runs;

-- In the reverse distribution, both flow predicates are common individually.
-- Only two rows match both flow keys, and the earlier one has the wrong task.
insert into runs
select result,
       case when result in (4, 6000) or mod(result * 79, 50000) < 20000 then 1 else mod(result, 16) + 2 end,
       case when result in (4, 6000) or mod(result * 79, 50000) between 20000 and 39999 then 1 else mod(result, 16) + 2 end,
       case when result = 4 then 100011
            when result = 6000 or mod(result * 79, 50000) < 37500 then 100010 else result + 200000 end,
       case when result = 4 then 0 else mod(result * 173, 20003) + 1 end,
       repeat('x', 200)
from generate_series(1, 50000) g;

-- Read the three persisted key columns before the first competing lookup.
select sum(workspace_id), sum(workflow_id), sum(task_id) from runs;

-- @regex("Index Table Scan on runs[.]ix_flow",true)
-- @regex("Join Type: INDEX",true)
-- @regex("Filter Cond: prefix_eq[(]__mo_index_idx_col[)]",true)
-- @regex("Sort Key: runs[.]created_at INTERNAL, runs[.]id INTERNAL",true)
-- @regex("Limit: 1",true)
explain select * from runs
where workspace_id = 1 and workflow_id = 1 and task_id = 100010
order by created_at, id limit 1;

select * from runs
where workspace_id = 1 and workflow_id = 1 and task_id = 100010
order by created_at, id limit 1;

select * from runs ignore index (ix_task)
where workspace_id = 1 and workflow_id = 1 and task_id = 100010
order by created_at, id limit 1;

drop database regular_index_point_skew;

-- @suit
-- @case
-- @desc:ALTER TABLE preserves columns whose names start with CHECK
-- @label:bvt

drop database if exists alter_check_prefix_26425;
create database alter_check_prefix_26425;
use alter_check_prefix_26425;

create table agent_runs (
    run_id varchar(64) not null,
    user_id varchar(128) not null,
    retry_scope varchar(16) not null default 'node',
    checkpoint_version varchar(32),
    checkpoint_json longtext,
    check$point varchar(32),
    error_code varchar(128),
    constraint chk_scope check (retry_scope in ('node', 'subtree', 'siblings')),
    primary key (user_id, run_id)
);

insert into agent_runs (run_id, user_id) values ('run-1', 'user-1');
alter table agent_runs add column model_offering_id varchar(64);

select run_id, user_id, retry_scope, checkpoint_version, checkpoint_json,
       check$point, error_code, model_offering_id
from agent_runs;
show create table agent_runs;

drop database alter_check_prefix_26425;

-- Full reproduction from issue #26425.
drop database if exists mo_issue_agent_runs_repro;
create database mo_issue_agent_runs_repro;
use mo_issue_agent_runs_repro;

create table agent_runs (
    run_id varchar(64) not null,
    user_id varchar(128) not null,
    session_id varchar(64) not null,
    parent_run_id varchar(64) null,
    root_run_id varchar(64) not null,
    ancestor_path varchar(2048) not null,
    depth int not null default 0,
    delegation_id varchar(64) null,
    agent_id varchar(255) null,
    retry_of varchar(64) null,
    retry_scope varchar(16) not null default 'node',
    status varchar(32) not null,
    execution_mode varchar(32) not null default 'web_agent',
    trigger_type varchar(64) null,
    trigger_event_id varchar(128) null,
    waiting_for varchar(64) null,
    owner_pod_id varchar(128) null,
    owner_lease_expires_at datetime(6) null,
    run_generation bigint not null default 0,
    last_event_idx bigint not null default -1,
    checkpoint_version varchar(32) null,
    checkpoint_json longtext null,
    error_code varchar(128) null,
    error_message text null,
    retry_count int not null default 0,
    total_prompt_tokens bigint not null default 0,
    total_completion_tokens bigint not null default 0,
    total_tool_calls bigint not null default 0,
    request_id varchar(64) null,
    trace_id varchar(64) null,
    agent_binding_id varchar(64) null,
    agent_binding_name varchar(255) null,
    agent_binding_schema_version varchar(32) null,
    selected_model_json longtext null,
    selected_model_name varchar(255) null,
    selected_model_gateway varchar(128) null,
    capability_server_refs_json longtext null,
    runtime_profile varchar(64) null,
    created_at datetime(6) not null default current_timestamp(6),
    updated_at datetime(6) not null default current_timestamp(6),
    constraint chk_agent_runs_retry_scope
        check (retry_scope in ('node', 'subtree', 'siblings')),
    primary key (user_id, run_id),
    index idx_agent_runs_user_updated_run (user_id, updated_at, run_id),
    index idx_agent_runs_user_session_status_updated (user_id, session_id, status, updated_at),
    index idx_agent_runs_owner_root_depth (user_id, root_run_id, depth, created_at),
    index idx_agent_runs_owner_parent_status_updated (user_id, parent_run_id, status, updated_at),
    index idx_agent_runs_owner_retry_of (user_id, retry_of),
    index idx_agent_runs_owner_lease (owner_pod_id, owner_lease_expires_at),
    index idx_agent_runs_binding (agent_binding_id, created_at),
    index idx_agent_runs_model_gateway (selected_model_gateway, created_at)
);

insert into agent_runs (
    run_id, user_id, session_id, root_run_id, ancestor_path, status
) values (
    'run-1', 'user-1', 'session-1', 'run-1', '/run-1', 'completed'
);

alter table agent_runs
    add column model_offering_id varchar(64) null;

select count(*) from agent_runs;
select count(*) as added_column_count
from information_schema.columns
where table_schema = 'mo_issue_agent_runs_repro'
  and table_name = 'agent_runs'
  and column_name = 'model_offering_id';

drop database mo_issue_agent_runs_repro;

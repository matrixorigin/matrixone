// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package catalog

// Lifecycle catalog DDL is kept in catalog instead of frontend so bootstrap,
// upgrade, and runtime adapters share one definition. Tenant upgrades create
// all tables except CleanupRoots. CleanupRoots is created only for the system
// account and survives tenant deletion long enough to reclaim external data.
// Its physical Cluster Table account_id remains 0 solely so an older CN safely
// filters the unknown table name during a rolling upgrade; owner_account_id is
// the only Lifecycle ownership field. The restore-table account_id columns are
// the equivalent old-CN DROP ACCOUNT compatibility sentinels and stay 0.
const (
	MoLifecycleBindingsDDL = `create table mo_catalog.mo_lifecycle_bindings (
		binding_id binary(16) not null,
		account_id int unsigned not null,
		database_id bigint unsigned not null,
		logical_table_id bigint unsigned not null,
		physical_table_id bigint unsigned not null,
		binding_generation bigint unsigned not null,
		schema_digest binary(32) not null,
		lifecycle_column_id bigint unsigned not null,
		action varchar(16) not null,
		expire_after_days int unsigned not null,
		late_arrival_grace_days int unsigned not null default 0,
		evaluation_timezone varchar(128) not null default 'UTC',
		stage_id bigint unsigned,
		stage_identity_digest binary(32),
		purge_after_days int unsigned,
		scan_snapshot_ts varbinary(64),
		scan_last_object_name varbinary(128),
		scan_wrapped bool not null default false,
		last_full_scan_at timestamp null,
		state varchar(16) not null,
		version bigint unsigned not null,
		created_at timestamp not null,
		updated_at timestamp not null,
		primary key (binding_id),
		unique key uk_lifecycle_binding_table (account_id, physical_table_id),
		key idx_lifecycle_binding_schedule (state, binding_id)
	)`

	MoLifecycleDatasetsDDL = `create table mo_catalog.mo_lifecycle_datasets (
		dataset_id binary(16) not null,
		account_id int unsigned not null,
		binding_id binary(16) not null,
		binding_generation bigint unsigned not null,
		logical_table_id bigint unsigned not null,
		source_physical_table_id bigint unsigned not null,
		source_snapshot_ts varbinary(64) not null,
		evaluation_time timestamp not null,
		cutoff timestamp not null,
		source_set_digest binary(32) not null,
		schema_descriptor_digest binary(32) not null,
		lifecycle_column_id bigint unsigned not null,
		lifecycle_column_type int unsigned not null,
		lifecycle_min bigint not null,
		lifecycle_max bigint not null,
		root_id binary(16) not null,
		attempt_id binary(16) not null,
		manifest_key text not null,
		manifest_sha256 binary(32) not null,
		content_hash binary(32) not null,
		row_count bigint unsigned not null,
		logical_bytes bigint unsigned not null,
		stage_id bigint unsigned not null,
		stage_identity_blob blob not null,
		purge_eligible_at timestamp not null,
		state varchar(20) not null,
		version bigint unsigned not null,
		access_generation bigint unsigned not null,
		restore_lease_id binary(16),
		restore_deadline timestamp null,
		publish_txn_id varbinary(128) not null,
		created_at timestamp not null,
		updated_at timestamp not null,
		primary key (dataset_id),
		unique key uk_lifecycle_dataset_attempt (root_id, attempt_id),
		key idx_lifecycle_dataset_table (account_id, logical_table_id, state),
		key idx_lifecycle_dataset_show
			(account_id, logical_table_id, created_at, dataset_id),
		key idx_lifecycle_dataset_purge (state, purge_eligible_at),
		key idx_lifecycle_dataset_terminal (state, updated_at, dataset_id),
		key idx_lifecycle_dataset_stage (stage_id, state)
	)`

	MoLifecycleTTLReceiptsDDL = `create table mo_catalog.mo_lifecycle_ttl_receipts (
		receipt_id binary(16) not null,
		account_id int unsigned not null,
		binding_id binary(16) not null,
		binding_generation bigint unsigned not null,
		physical_table_id bigint unsigned not null,
		source_snapshot_ts varbinary(64) not null,
		evaluation_time timestamp not null,
		cutoff timestamp not null,
		source_set_digest binary(32) not null,
		expired_rows bigint unsigned not null,
		retired_bytes bigint unsigned not null,
		root_id binary(16),
		attempt_id binary(16),
		publish_txn_id varbinary(128) not null,
		created_at timestamp not null,
		primary key (receipt_id),
		key idx_lifecycle_ttl_source (binding_id, source_set_digest),
		key idx_lifecycle_ttl_attempt (root_id, attempt_id),
		key idx_lifecycle_ttl_created (created_at)
	)`

	MoLifecycleRestoreAttemptsDDL = `create table mo_catalog.mo_lifecycle_restore_attempts (
		restore_id binary(16) not null,
		account_id int unsigned not null default 0,
		dataset_id binary(16) not null,
		scope varchar(16) not null,
		source_logical_table_id bigint unsigned not null,
		range_start bigint not null,
		range_end bigint not null,
		lifecycle_column_id bigint unsigned not null,
		lifecycle_column_type int unsigned not null,
		selection_digest binary(32) not null,
		dataset_selection blob not null,
		dataset_count int unsigned not null,
		total_chunk_count bigint unsigned not null,
		selected_logical_bytes bigint unsigned not null,
		lease_id binary(16) not null,
		deadline timestamp not null,
		staging_database_id bigint unsigned not null,
		staging_table_id bigint unsigned not null,
		hidden_name varchar(256) not null,
		target_database_id bigint unsigned not null,
		target_name varchar(256) not null,
		state varchar(16) not null,
		next_chunk_ordinal bigint unsigned not null,
		restored_rows bigint unsigned not null,
		verified_content_hash binary(32),
		last_error text,
		updated_at timestamp not null,
		primary key (restore_id),
		key idx_lifecycle_restore_dataset (dataset_id, state),
		key idx_lifecycle_restore_deadline (state, deadline),
		key idx_lifecycle_restore_terminal (state, updated_at, restore_id)
	)`

	MoLifecycleRestoreChunksDDL = `create table mo_catalog.mo_lifecycle_restore_chunks (
		restore_id binary(16) not null,
		account_id int unsigned not null default 0,
		dataset_id binary(16) not null,
		dataset_chunk_ordinal bigint unsigned not null,
		chunk_ordinal bigint unsigned not null,
		file_ordinal int unsigned not null,
		row_group_ordinal int unsigned not null,
		chunk_digest binary(32) not null,
		row_count bigint unsigned not null,
		logical_bytes bigint unsigned not null,
		canonical_content_hash binary(32) not null,
		created_at timestamp not null,
		primary key (restore_id, chunk_ordinal)
	)`

	MoLifecycleCleanupRootsDDL = `create cluster table mo_catalog.mo_lifecycle_cleanup_roots (
		root_id binary(16) not null,
		attempt_id binary(16) not null,
		mode varchar(24) not null,
		owner_account_id int unsigned not null,
		logical_table_id bigint unsigned not null,
		physical_table_id bigint unsigned not null,
		executor_epoch bigint unsigned not null,
		worker_lease_deadline timestamp not null,
		archive_namespace_blob blob,
		credential_handle text,
		archive_prefix text,
		manifest_key text,
		manifest_digest binary(32),
		tae_namespace_blob blob,
		segment_id varbinary(64),
		booking_prefix text,
		ordinal_upper_bound int unsigned,
		reserved_cleanup_bytes bigint unsigned not null,
		source_set_digest binary(32) not null,
		final_txn_id varbinary(128),
		state varchar(24) not null,
		state_version bigint unsigned not null,
		cleanup_after timestamp not null,
		temporary_cleanup_done bool not null default false,
		quiescence_since timestamp null,
		last_list_at timestamp null,
		last_error text,
		created_at timestamp not null,
		updated_at timestamp not null,
		primary key (root_id),
		unique key uk_lifecycle_cleanup_attempt (attempt_id),
		key idx_lifecycle_cleanup_work (state, cleanup_after, root_id),
		key idx_lifecycle_cleanup_temporary
			(state, temporary_cleanup_done, updated_at, root_id),
		key idx_lifecycle_cleanup_terminal (state, updated_at, root_id),
		key idx_lifecycle_cleanup_owner (owner_account_id, logical_table_id),
		key idx_lifecycle_cleanup_show (owner_account_id, updated_at, root_id)
	)`
)

type LifecycleTableDefinition struct {
	Schema string
	Name   string
	DDL    string
}

var LifecycleTenantTableDefinitions = []LifecycleTableDefinition{
	{Schema: MO_CATALOG, Name: MO_LIFECYCLE_BINDINGS, DDL: MoLifecycleBindingsDDL},
	{Schema: MO_CATALOG, Name: MO_LIFECYCLE_DATASETS, DDL: MoLifecycleDatasetsDDL},
	{Schema: MO_CATALOG, Name: MO_LIFECYCLE_TTL_RECEIPTS, DDL: MoLifecycleTTLReceiptsDDL},
	{Schema: MO_CATALOG, Name: MO_LIFECYCLE_RESTORE_ATTEMPTS, DDL: MoLifecycleRestoreAttemptsDDL},
	{Schema: MO_CATALOG, Name: MO_LIFECYCLE_RESTORE_CHUNKS, DDL: MoLifecycleRestoreChunksDDL},
}

var LifecycleClusterTableDefinitions = []LifecycleTableDefinition{
	{Schema: MO_CATALOG, Name: MO_LIFECYCLE_CLEANUP_ROOTS, DDL: MoLifecycleCleanupRootsDDL},
}

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

package iceberg

import "github.com/matrixorigin/matrixone/pkg/catalog"

const (
	TableCatalogs        = "mo_iceberg_catalogs"
	TablePrincipalMap    = "mo_iceberg_principal_map"
	TableResidencyPolicy = "mo_iceberg_residency_policy"
	TableTables          = "mo_iceberg_tables"
	TableRefs            = "mo_iceberg_refs"
	TablePublishJobs     = "mo_iceberg_publish_jobs"
	TableOrphanFiles     = "mo_iceberg_orphan_files"
	TableMaintenanceJobs = "mo_iceberg_maintenance_jobs"
)

const CatalogsDDL = `create table mo_catalog.mo_iceberg_catalogs (
	account_id int unsigned not null,
	catalog_id bigint unsigned not null auto_increment,
	name varchar(300) not null,
	type varchar(32) not null,
	uri text not null,
	warehouse text,
	auth_mode varchar(32) not null default 'none',
	token_secret_ref text,
	capabilities_json json,
	created_at timestamp not null default utc_timestamp,
	updated_at timestamp not null default utc_timestamp,
	disabled_at timestamp default null,
	version bigint unsigned not null default 1,
	primary key(account_id, catalog_id),
	unique key(account_id, name)
)`

const PrincipalMapDDL = `create table mo_catalog.mo_iceberg_principal_map (
	account_id int unsigned not null,
	catalog_id bigint unsigned not null,
	mo_role_id bigint unsigned not null default 0,
	mo_user_id bigint unsigned not null default 0,
	external_principal varchar(1024) not null,
	scope_json json,
	created_by bigint unsigned not null default 0,
	created_at timestamp not null default utc_timestamp,
	updated_at timestamp not null default utc_timestamp,
	version bigint unsigned not null default 1,
	primary key(account_id, catalog_id, mo_role_id, mo_user_id)
)`

const ResidencyPolicyDDL = `create table mo_catalog.mo_iceberg_residency_policy (
	scope_type varchar(16) not null,
	account_id int unsigned not null,
	catalog_id bigint unsigned not null,
	allowed_catalog_uri varchar(2048) not null,
	allowed_endpoint varchar(1024) not null,
	allowed_region varchar(128) not null,
	allowed_bucket varchar(1024) not null,
	policy_state varchar(16) not null default 'enabled',
	created_by bigint unsigned not null default 0,
	created_at timestamp not null default utc_timestamp,
	updated_at timestamp not null default utc_timestamp,
	version bigint unsigned not null default 1,
	primary key(scope_type, account_id, catalog_id, allowed_catalog_uri, allowed_endpoint, allowed_region, allowed_bucket)
)`

const TablesDDL = `create table mo_catalog.mo_iceberg_tables (
	account_id int unsigned not null,
	db_id bigint unsigned not null,
	table_id bigint unsigned not null,
	catalog_id bigint unsigned not null,
	namespace varchar(2048) not null,
	table_name varchar(1024) not null,
	default_ref varchar(256) not null default 'main',
	read_mode varchar(32) not null default 'append_only',
	write_mode varchar(32) not null default 'read_only',
	writer_owner_account_id int unsigned not null default 0,
	capabilities_json json,
	last_snapshot_id varchar(128),
	last_metadata_location_hash varchar(128),
	created_at timestamp not null default utc_timestamp,
	updated_at timestamp not null default utc_timestamp,
	version bigint unsigned not null default 1,
	primary key(account_id, db_id, table_id),
	unique key(account_id, catalog_id, namespace, table_name, default_ref)
)`

const RefsDDL = `create table mo_catalog.mo_iceberg_refs (
	account_id int unsigned not null,
	catalog_id bigint unsigned not null,
	namespace varchar(2048) not null,
	table_name varchar(1024) not null,
	ref_name varchar(256) not null,
	ref_type varchar(32) not null,
	snapshot_id varchar(128) not null,
	last_seen_at timestamp not null default utc_timestamp,
	source varchar(64) not null default 'catalog',
	created_at timestamp not null default utc_timestamp,
	updated_at timestamp not null default utc_timestamp,
	version bigint unsigned not null default 1,
	primary key(account_id, catalog_id, namespace, table_name, ref_name)
)`

const PublishJobsDDL = `create table mo_catalog.mo_iceberg_publish_jobs (
	job_id varchar(128) not null,
	account_id int unsigned not null,
	source_db varchar(1024),
	source_table varchar(1024),
	target_catalog_id bigint unsigned not null,
	target_namespace varchar(2048) not null,
	target_table varchar(1024) not null,
	source_batch varchar(1024),
	watermark_start varchar(128),
	watermark_end varchar(128),
	business_window varchar(256),
	snapshot_id varchar(128),
	commit_id varchar(128),
	row_count bigint unsigned not null default 0,
	file_count bigint unsigned not null default 0,
	status varchar(32) not null,
	error_category varchar(64),
	created_at timestamp not null default utc_timestamp,
	updated_at timestamp not null default utc_timestamp,
	status_updated_at timestamp not null default utc_timestamp,
	version bigint unsigned not null default 1,
	primary key(job_id),
	key idx_iceberg_publish_account_status(account_id, target_catalog_id, status, created_at)
)`

const OrphanFilesDDL = `create table mo_catalog.mo_iceberg_orphan_files (
	account_id int unsigned not null,
	job_id varchar(128) not null,
	catalog_id bigint unsigned not null,
	namespace varchar(2048) not null default '',
	table_name varchar(1024) not null default '',
	table_location_hash varchar(128) not null,
	file_path varchar(4096) not null default '',
	file_path_hash varchar(128) not null,
	file_path_redacted varchar(256) not null,
	written_at timestamp not null,
	expire_at timestamp not null,
	cleanup_status varchar(32) not null,
	created_at timestamp not null default utc_timestamp,
	updated_at timestamp not null default utc_timestamp,
	version bigint unsigned not null default 1,
	primary key(account_id, job_id, file_path_hash),
	key idx_iceberg_orphan_cleanup(account_id, catalog_id, cleanup_status, expire_at)
)`

const MaintenanceJobsDDL = `create table mo_catalog.mo_iceberg_maintenance_jobs (
	job_id varchar(128) not null,
	account_id int unsigned not null,
	catalog_id bigint unsigned not null,
	namespace varchar(2048) not null,
	table_name varchar(1024) not null,
	operation varchar(64) not null,
	target_ref varchar(256) not null default 'main',
	snapshot_before varchar(128),
	snapshot_after varchar(128),
	rewritten_file_count bigint unsigned not null default 0,
	removed_file_count bigint unsigned not null default 0,
	status varchar(32) not null,
	error_category varchar(64),
	created_at timestamp not null default utc_timestamp,
	updated_at timestamp not null default utc_timestamp,
	status_updated_at timestamp not null default utc_timestamp,
	version bigint unsigned not null default 1,
	primary key(job_id),
	key idx_iceberg_maintenance_account_status(account_id, catalog_id, status, created_at)
)`

type SystemTableDDL struct {
	Schema string
	Name   string
	DDL    string
}

var P0SystemTableDDLs = []SystemTableDDL{
	{Schema: catalog.MO_CATALOG, Name: TableCatalogs, DDL: CatalogsDDL},
	{Schema: catalog.MO_CATALOG, Name: TablePrincipalMap, DDL: PrincipalMapDDL},
	{Schema: catalog.MO_CATALOG, Name: TableResidencyPolicy, DDL: ResidencyPolicyDDL},
	{Schema: catalog.MO_CATALOG, Name: TableTables, DDL: TablesDDL},
	{Schema: catalog.MO_CATALOG, Name: TableRefs, DDL: RefsDDL},
}

var P1WriteSystemTableDDLs = []SystemTableDDL{
	{Schema: catalog.MO_CATALOG, Name: TablePublishJobs, DDL: PublishJobsDDL},
	{Schema: catalog.MO_CATALOG, Name: TableOrphanFiles, DDL: OrphanFilesDDL},
}

var P2MaintenanceSystemTableDDLs = []SystemTableDDL{
	{Schema: catalog.MO_CATALOG, Name: TableMaintenanceJobs, DDL: MaintenanceJobsDDL},
}

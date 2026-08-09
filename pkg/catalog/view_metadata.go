// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package catalog

const ViewRefreshStatusCurrent = "CURRENT"
const ViewRefreshStatusPending = "PENDING"
const ViewRefreshStatusDiscovering = "DISCOVERING"
const ViewRefreshStatusRunning = "RUNNING"
const ViewRefreshStatusInvalid = "INVALID"
const ViewRefreshStatusLegacyScan = "LEGACY_SCAN"

const LegacyViewScanCursorDatabase = "__mo_legacy_view_scan__"
const LegacyViewScanCursorRelation = "__mo_legacy_view_scan_cursor__"

const MoViewDependenciesColumns = "account_id,target_database_id,target_relation_id," +
	"target_logical_id,target_database_name,target_relation_name,dependency_ordinal,source_account_id," +
	"source_database_id,source_relation_id,source_logical_id,source_database_name," +
	"source_relation_name,source_database_name_key,source_relation_name_key," +
	"source_relation_kind,subscription_name,publisher_account_id,snapshot_data," +
	"lower_case_table_names,dependency_generation"

const MoViewRefreshColumns = "account_id,target_database_id,target_relation_id," +
	"target_logical_id,target_database_name,target_relation_name,target_generation," +
	"completed_generation,status,failure_code,next_retry_at,lease_owner,lease_epoch," +
	"lease_expires_at,attempts"

const MoViewDependenciesDDL = `create cluster table mo_catalog.mo_view_dependencies (
		target_database_id bigint unsigned not null,
		target_relation_id bigint unsigned not null,
		target_logical_id bigint unsigned not null,
		target_database_name varchar(5000) not null,
		target_relation_name varchar(5000) not null,
		dependency_ordinal int unsigned not null,
		source_account_id int unsigned not null,
		source_database_id bigint unsigned not null,
		source_relation_id bigint unsigned not null,
		source_logical_id bigint unsigned not null,
		source_database_name varchar(5000) not null,
		source_relation_name varchar(5000) not null,
		source_database_name_key varchar(64) not null,
		source_relation_name_key varchar(64) not null,
		source_relation_kind varchar(32) not null,
		subscription_name varchar(5000) not null default '',
		publisher_account_id int unsigned not null default 0,
		snapshot_data text,
		lower_case_table_names bigint not null,
		dependency_generation bigint unsigned not null,
		primary key(account_id, target_relation_id, dependency_ordinal),
		index idx_view_dependency_source_id(source_account_id, source_database_id,
			source_relation_id),
		index idx_view_dependency_source_logical(source_account_id, source_database_id,
			source_logical_id),
		index idx_view_dependency_source_name(source_account_id, source_database_name_key,
			source_relation_name_key)
)`

const MoViewRefreshDDL = `create cluster table mo_catalog.mo_view_refresh (
		target_database_id bigint unsigned not null,
		target_relation_id bigint unsigned not null,
		target_logical_id bigint unsigned not null,
		target_database_name varchar(5000) not null,
		target_relation_name varchar(5000) not null,
		target_generation bigint unsigned not null,
		completed_generation bigint unsigned not null,
		status varchar(32) not null,
		failure_code int unsigned not null default 0,
		next_retry_at timestamp null,
		lease_owner varchar(128) not null default '',
		lease_epoch bigint unsigned not null default 0,
		lease_expires_at timestamp null,
		attempts int unsigned not null default 0,
		primary key(account_id, target_relation_id),
		index idx_view_refresh_pending(status, next_retry_at, account_id,
			target_relation_id)
)`

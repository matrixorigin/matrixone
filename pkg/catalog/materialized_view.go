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

import (
	"encoding/json"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
)

const SystemMaterializedViewRel = "m"
const MaterializedViewProperty = "mo_materialized_view"
const MO_MATERIALIZED_VIEWS = "mo_materialized_views"
const MO_MV_DEPENDENCIES = "mo_mv_dependencies"
const MO_MV_REFRESH_RUNS = "mo_mv_refresh_runs"
const MO_MV_ACCOUNT_STATE = "mo_mv_account_state"

func IsMaterializedViewDatabase(database string) bool {
	switch database {
	case "mo_catalog", "information_schema", "system", "mysql", "mo_task", "system_metrics":
		return false
	default:
		return true
	}
}

type MVSource struct {
	AccountID  uint32 `json:"account_id"`
	DatabaseID uint64 `json:"database_id"`
	TableID    uint64 `json:"table_id"`
	LogicalID  uint64 `json:"logical_id"`
	Version    uint32 `json:"version"`
	Database   string `json:"database"`
	Table      string `json:"table"`
}

// MVDefinition is immutable for a stored relation. Runtime coverage and schedule
// are authoritative in mo_materialized_views, not in this property.
type MVDefinition struct {
	FormatVersion       uint32     `json:"format_version"`
	Query               string     `json:"query"`
	CreateSQL           string     `json:"create_sql"`
	Database            string     `json:"database"`
	Sources             []MVSource `json:"sources"`
	IntervalSeconds     int64      `json:"interval_seconds"`
	SQLMode             string     `json:"sql_mode"`
	TimeZone            string     `json:"time_zone"`
	TimeZoneOffset      int32      `json:"time_zone_offset_seconds"`
	LowerCaseTableNames int64      `json:"lower_case_table_names"`
	OwnerUser           uint32     `json:"owner_user"`
	OwnerRole           uint32     `json:"owner_role"`
	OwnerName           string     `json:"owner_name"`
}

func MaterializedViewDefinition(def *plan.TableDef) (*MVDefinition, error) {
	if def == nil || def.TableType != SystemMaterializedViewRel {
		return nil, nil
	}
	var value string
	for _, p := range def.Props {
		if p.Key == MaterializedViewProperty {
			value = p.Value
		}
	}
	for _, d := range def.Defs {
		if ps := d.GetProperties(); ps != nil {
			for _, p := range ps.Properties {
				if p.Key == MaterializedViewProperty {
					value = p.Value
				}
			}
		}
	}
	return DecodeMaterializedViewDefinition(value)
}

func DecodeMaterializedViewDefinition(value string) (*MVDefinition, error) {
	var result MVDefinition
	if value == "" {
		return nil, moerr.NewInternalErrorNoCtx("materialized view definition is missing")
	}
	if err := json.Unmarshal([]byte(value), &result); err != nil {
		return nil, err
	}
	if result.FormatVersion != 1 || result.Query == "" || len(result.Sources) == 0 {
		return nil, moerr.NewInternalErrorNoCtx("unsupported materialized view definition")
	}
	return &result, nil
}

const MoMaterializedViewsDDL = `create cluster table mo_catalog.mo_materialized_views (
 mv_id bigint unsigned not null,
 database_id bigint unsigned not null,
 database_name varchar(64) not null,
 name varchar(64) not null,
 definition text not null,
 epoch bigint unsigned not null default 1,
 requested_sequence bigint unsigned not null default 0,
 successful_sequence bigint unsigned not null default 0,
 populated bool not null default false,
 last_run varchar(64) not null default '',
 source_physical bigint not null default 0,
 source_logical int unsigned not null default 0,
 last_success timestamp null,
 interval_seconds bigint not null default 0,
 next_due timestamp null,
 next_cleanup timestamp not null default current_timestamp(),
 primary key(account_id,mv_id),
 unique key mv_name(account_id,database_name,name),
 index mv_due(next_due,account_id,mv_id),
 index mv_cleanup(next_cleanup,account_id,mv_id)
)`
const MoMVDependenciesDDL = `create cluster table mo_catalog.mo_mv_dependencies (
 mv_id bigint unsigned not null,
 source_database_id bigint unsigned not null,
 source_table_id bigint unsigned not null,
 source_logical_id bigint unsigned not null,
 source_version int unsigned not null,
 primary key(account_id,mv_id,source_table_id),
 index mv_source(account_id,source_table_id)
)`
const MoMVRefreshRunsDDL = `create cluster table mo_catalog.mo_mv_refresh_runs (
 mv_id bigint unsigned not null,
 run_id varchar(64) not null,
 sequence bigint unsigned not null,
 epoch bigint unsigned not null,
 status varchar(16) not null,
 started_at timestamp not null,
 finished_at timestamp null,
 source_physical bigint not null default 0,
 source_logical int unsigned not null default 0,
 error_message varchar(1024) not null default '',
 primary key(account_id,mv_id,run_id),
 index mv_history(account_id,mv_id,started_at),
 index mv_expired(status,started_at)
)`
const MoMVAccountStateDDL = `create cluster table mo_catalog.mo_mv_account_state (
 revision bigint unsigned not null default 0,
 primary key(account_id)
)`

var MaterializedViewCatalogDDLs = []string{MoMaterializedViewsDDL, MoMVDependenciesDDL, MoMVRefreshRunsDDL, MoMVAccountStateDDL}

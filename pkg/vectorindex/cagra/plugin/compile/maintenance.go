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

package compile

import (
	"encoding/json"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/vectorindex"
)

// ValidateIndexMaintenance binds an owner-generated literal to the live group.
func (Hooks) ValidateIndexMaintenance(function string, args []string, spec *executor.IndexMaintenanceSpec) error {
	if function != "cagra_create" || len(args) < 2 {
		return moerr.NewInvalidInputNoCtx("unexpected index maintenance function")
	}
	var cfg vectorindex.IndexTableConfig
	if err := json.Unmarshal([]byte(args[1]), &cfg); err != nil {
		return err
	}
	if cfg.SrcTable == "" {
		return moerr.NewInvalidInputNoCtx("index maintenance source is empty")
	}
	targets := map[string]string{catalog.Cagra_TblType_Metadata: cfg.MetadataTable, catalog.Cagra_TblType_Storage: cfg.IndexTable}
	if cfg.IgnoredTable != "" {
		targets[catalog.IndexIgnored_TblType] = cfg.IgnoredTable
	}
	return vectorindex.ValidateMaintenanceConfig(spec, function, cfg.DbName, cfg.SrcTable, targets)
}

func (Hooks) InitSQLMaintenanceFunction() string { return "cagra_create" }

func (Hooks) IndexMaintenanceFunctions() []string { return []string{"cagra_create"} }

func (Hooks) IndexMaintenanceRoles(function string, defs map[string]*plan.IndexDef) ([]string, error) {
	if function != "cagra_create" {
		return nil, moerr.NewInvalidInputNoCtx("unexpected index maintenance function")
	}
	roles, err := vectorindex.MaintenanceRoles(defs, catalog.Cagra_TblType_Metadata, catalog.Cagra_TblType_Storage)
	if err != nil {
		return nil, err
	}
	if defs[catalog.IndexIgnored_TblType] != nil {
		roles = append(roles, catalog.IndexIgnored_TblType)
	}
	return roles, nil
}

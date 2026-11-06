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
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/vectorindex"
	hnswruntime "github.com/matrixorigin/matrixone/pkg/vectorindex/hnsw/plugin/runtime"
)

// ValidateIndexMaintenance binds an owner-generated literal to the live group.
func (Hooks) ValidateIndexMaintenance(function string, args []string, spec *executor.IndexMaintenanceSpec) error {
	if function != "hnsw_create" || len(args) < 2 {
		return moerr.NewInvalidInputNoCtx("unexpected index maintenance function")
	}
	cfg, err := vectorindex.DecodeMaintenanceConfig(args[1])
	if err != nil {
		return err
	}

	return vectorindex.ValidateMaintenanceConfig(spec, function, cfg.DbName, cfg.SrcTable, hnswruntime.MaintenanceOperands(cfg))
}

func (Hooks) InitSQLMaintenanceFunction() string { return "" }

func (Hooks) IndexMaintenanceFunctions() []string { return []string{"hnsw_create"} }

func (Hooks) IndexMaintenanceRoles(function string, defs map[string]*plan.IndexDef) ([]string, error) {
	if function != "hnsw_create" {
		return nil, moerr.NewInvalidInputNoCtx("unexpected index maintenance function")
	}
	return vectorindex.MaintenanceRoles(defs,
		[]string{catalog.Hnsw_TblType_Metadata, catalog.Hnsw_TblType_Storage},
		catalog.Hnsw_TblType_Dict, catalog.IndexIgnored_TblType)
}

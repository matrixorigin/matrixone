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
	"github.com/matrixorigin/matrixone/pkg/fulltext2"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/vectorindex"
)

func (Hooks) ValidateIndexMaintenance(function string, args []string, spec *executor.IndexMaintenanceSpec) error {
	switch function {
	case "fulltext2_create":
		if len(args) < 2 {
			return moerr.NewInvalidInputNoCtx("missing fulltext2 config")
		}
		var cfg fulltext2.TableConfig
		if err := json.Unmarshal([]byte(args[1]), &cfg); err != nil {
			return err
		}
		if cfg.SrcTable == "" {
			return moerr.NewInvalidInputNoCtx("fulltext2 source is empty")
		}
		return vectorindex.ValidateMaintenanceConfig(spec, function, cfg.DbName, cfg.SrcTable, fulltext2.MaintenanceOperands(cfg))
	case "fulltext2_compact":
		if len(args) < 3 {
			return moerr.NewInvalidInputNoCtx("missing fulltext2 targets")
		}
		return vectorindex.ValidateMaintenanceConfig(spec, function, args[0], "", []vectorindex.MaintenanceOperand{
			{Role: catalog.FullText2Index_TblType_Storage, Name: args[1]},
			{Role: catalog.FullText2Index_TblType_Metadata, Name: args[2]},
		})
	default:
		return moerr.NewInvalidInputNoCtx("unexpected fulltext2 maintenance function")
	}
}
func (Hooks) InitSQLMaintenanceFunction() string { return "" }

func (Hooks) IndexMaintenanceFunctions() []string {
	return []string{"fulltext2_create", "fulltext2_compact"}
}

func (Hooks) IndexMaintenanceRoles(function string, defs map[string]*plan.IndexDef) ([]string, error) {
	required := []string{catalog.FullText2Index_TblType_Storage, catalog.FullText2Index_TblType_Metadata}
	switch function {
	case "fulltext2_create":
		return vectorindex.MaintenanceRoles(defs, required, catalog.IndexIgnored_TblType)
	case "fulltext2_compact":
		return vectorindex.MaintenanceRoles(defs, required)
	default:
		return nil, moerr.NewInvalidInputNoCtx("unexpected index maintenance function")
	}
}

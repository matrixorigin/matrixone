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

package vectorindex_test

import (
	"encoding/json"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	compileplugin "github.com/matrixorigin/matrixone/pkg/indexplugin/compile"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/vectorindex"
	cagracompile "github.com/matrixorigin/matrixone/pkg/vectorindex/cagra/plugin/compile"
	cagraruntime "github.com/matrixorigin/matrixone/pkg/vectorindex/cagra/plugin/runtime"
	hnswcompile "github.com/matrixorigin/matrixone/pkg/vectorindex/hnsw/plugin/compile"
	hnswruntime "github.com/matrixorigin/matrixone/pkg/vectorindex/hnsw/plugin/runtime"
	ivfflatcompile "github.com/matrixorigin/matrixone/pkg/vectorindex/ivfflat/plugin/compile"
	ivfflatruntime "github.com/matrixorigin/matrixone/pkg/vectorindex/ivfflat/plugin/runtime"
	ivfpqcompile "github.com/matrixorigin/matrixone/pkg/vectorindex/ivfpq/plugin/compile"
	ivfpqruntime "github.com/matrixorigin/matrixone/pkg/vectorindex/ivfpq/plugin/runtime"
	"github.com/stretchr/testify/require"
)

func TestMaintenanceDescriptorsAndOperands(t *testing.T) {
	for _, tc := range []struct {
		name, function, initFunction string
		hooks                        compileplugin.IndexMaintenanceDescriptor
		operands                     func(vectorindex.IndexTableConfig) []vectorindex.MaintenanceOperand
		want                         []vectorindex.MaintenanceOperand
	}{
		{"hnsw", "hnsw_create", "", hnswcompile.Hooks{}, hnswruntime.MaintenanceOperands, []vectorindex.MaintenanceOperand{
			{Role: catalog.Hnsw_TblType_Metadata, Name: "meta"}, {Role: catalog.Hnsw_TblType_Storage, Name: "store"},
			{Role: catalog.Hnsw_TblType_Dict, Name: "dict"}, {Role: catalog.IndexIgnored_TblType, Name: "ignored"},
		}},
		{"ivfflat", "ivf_create", "ivf_create", ivfflatcompile.Hooks{}, ivfflatruntime.MaintenanceOperands, []vectorindex.MaintenanceOperand{
			{Role: catalog.SystemSI_IVFFLAT_TblType_Metadata, Name: "meta"}, {Role: catalog.SystemSI_IVFFLAT_TblType_Centroids, Name: "store"},
		}},
		{"ivfpq", "ivfpq_create", "ivfpq_create", ivfpqcompile.Hooks{}, ivfpqruntime.MaintenanceOperands, []vectorindex.MaintenanceOperand{
			{Role: catalog.Ivfpq_TblType_Metadata, Name: "meta"}, {Role: catalog.Ivfpq_TblType_Storage, Name: "store"},
			{Role: catalog.IndexIgnored_TblType, Name: "ignored"},
		}},
		{"cagra", "cagra_create", "cagra_create", cagracompile.Hooks{}, cagraruntime.MaintenanceOperands, []vectorindex.MaintenanceOperand{
			{Role: catalog.Cagra_TblType_Metadata, Name: "meta"}, {Role: catalog.Cagra_TblType_Storage, Name: "store"},
			{Role: catalog.IndexIgnored_TblType, Name: "ignored"},
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := vectorindex.IndexTableConfig{DbName: "db", SrcTable: "base", MetadataTable: "meta", IndexTable: "store"}
			all := cfg
			all.DictTable, all.IgnoredTable = "dict", "ignored"
			require.Equal(t, tc.want, tc.operands(all))
			require.Equal(t, tc.want[:2], tc.operands(cfg))
			require.Equal(t, tc.initFunction, tc.hooks.InitSQLMaintenanceFunction())
			require.Equal(t, []string{tc.function}, tc.hooks.IndexMaintenanceFunctions())

			spec := &executor.IndexMaintenanceSpec{Function: tc.function, Database: "db", DatabaseID: 1, BaseName: "base", BaseID: 2}
			defs := make(map[string]*plan.IndexDef, len(tc.want))
			for i, operand := range tc.want[:2] {
				spec.Targets = append(spec.Targets, executor.IndexMaintenanceTarget{Role: operand.Role, Name: operand.Name, ID: uint64(i + 3)})
				defs[operand.Role] = &plan.IndexDef{}
			}
			literal := func(t *testing.T, cfg vectorindex.IndexTableConfig) []string {
				t.Helper()
				b, err := json.Marshal(cfg)
				require.NoError(t, err)
				return []string{"{}", string(b)}
			}
			require.Error(t, tc.hooks.ValidateIndexMaintenance("other", literal(t, cfg), spec))
			require.Error(t, tc.hooks.ValidateIndexMaintenance(tc.function, nil, spec))
			require.NoError(t, tc.hooks.ValidateIndexMaintenance(tc.function, literal(t, cfg), spec))
			_, err := tc.hooks.IndexMaintenanceRoles("other", defs)
			require.Error(t, err)
			_, err = tc.hooks.IndexMaintenanceRoles(tc.function, nil)
			require.Error(t, err)
			for _, missing := range tc.want[:2] {
				def := defs[missing.Role]
				delete(defs, missing.Role)
				_, err = tc.hooks.IndexMaintenanceRoles(tc.function, defs)
				require.Error(t, err)
				defs[missing.Role] = def
			}
			roles, err := tc.hooks.IndexMaintenanceRoles(tc.function, defs)
			require.NoError(t, err)
			require.Equal(t, []string{tc.want[0].Role, tc.want[1].Role}, roles)

			for i, optional := range tc.want[2:] {
				t.Run(optional.Role, func(t *testing.T) {
					withOptional := cfg
					if optional.Role == catalog.Hnsw_TblType_Dict {
						withOptional.DictTable = optional.Name
					} else {
						withOptional.IgnoredTable = optional.Name
					}
					require.Error(t, tc.hooks.ValidateIndexMaintenance(tc.function, literal(t, withOptional), spec), "unattested optional target")
					withTarget := spec.Clone()
					withTarget.Targets = append(withTarget.Targets, executor.IndexMaintenanceTarget{Role: optional.Role, Name: optional.Name, ID: uint64(i + 5)})
					require.NoError(t, tc.hooks.ValidateIndexMaintenance(tc.function, literal(t, withOptional), withTarget))
					if optional.Role == catalog.Hnsw_TblType_Dict {
						withOptional.DictTable = "forged"
					} else {
						withOptional.IgnoredTable = "forged"
					}
					require.Error(t, tc.hooks.ValidateIndexMaintenance(tc.function, literal(t, withOptional), withTarget))
					defs[optional.Role] = &plan.IndexDef{}
					roles, err := tc.hooks.IndexMaintenanceRoles(tc.function, defs)
					require.NoError(t, err)
					require.Equal(t, []string{tc.want[0].Role, tc.want[1].Role, optional.Role}, roles)
					delete(defs, optional.Role)
				})
			}
			allSpec := spec.Clone()
			for i, optional := range tc.want[2:] {
				allSpec.Targets = append(allSpec.Targets, executor.IndexMaintenanceTarget{Role: optional.Role, Name: optional.Name, ID: uint64(i + 5)})
				defs[optional.Role] = &plan.IndexDef{}
			}
			require.NoError(t, tc.hooks.ValidateIndexMaintenance(tc.function, literal(t, all), allSpec))
			roles, err = tc.hooks.IndexMaintenanceRoles(tc.function, defs)
			require.NoError(t, err)
			wantRoles := make([]string, 0, len(tc.want))
			for _, operand := range tc.want {
				wantRoles = append(wantRoles, operand.Role)
			}
			require.Equal(t, wantRoles, roles)
			cfg.MetadataTable, cfg.IndexTable = cfg.IndexTable, cfg.MetadataTable
			require.Error(t, tc.hooks.ValidateIndexMaintenance(tc.function, literal(t, cfg), spec))
			cfg.MetadataTable, cfg.IndexTable, cfg.SrcTable = "meta", "store", "other"
			require.Error(t, tc.hooks.ValidateIndexMaintenance(tc.function, literal(t, cfg), spec))
		})
	}
}

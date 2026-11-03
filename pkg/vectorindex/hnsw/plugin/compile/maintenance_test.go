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
	"testing"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/vectorindex"
	"github.com/stretchr/testify/require"
)

func TestMaintenanceDescriptorFencesGeneratedConfig(t *testing.T) {
	function := "hnsw_create"
	spec := &executor.IndexMaintenanceSpec{Function: function, Database: "db", DatabaseID: 1, BaseName: "base", BaseID: 2,
		Targets: []executor.IndexMaintenanceTarget{
			{Role: catalog.Hnsw_TblType_Metadata, Name: "meta", ID: 3},
			{Role: catalog.Hnsw_TblType_Storage, Name: "store", ID: 4},
		}}
	cfg := vectorindex.IndexTableConfig{DbName: "db", SrcTable: "base", MetadataTable: "meta", IndexTable: "store"}
	literal := func() []string {
		b, err := json.Marshal(cfg)
		require.NoError(t, err)
		return []string{"{}", string(b)}
	}
	h := Hooks{}
	require.Error(t, h.ValidateIndexMaintenance("other", literal(), spec))
	require.Error(t, h.ValidateIndexMaintenance(function, nil, spec))

	require.NoError(t, h.ValidateIndexMaintenance(function, literal(), spec))
	require.Empty(t, h.InitSQLMaintenanceFunction())
	roles, err := h.IndexMaintenanceRoles(function, map[string]*plan.IndexDef{
		catalog.Hnsw_TblType_Metadata: {}, catalog.Hnsw_TblType_Storage: {},
	})
	require.NoError(t, err)
	require.Len(t, roles, 2)
	cfg.IgnoredTable = "ignored"
	require.Error(t, h.ValidateIndexMaintenance(function, literal(), spec), "unattested optional target")
	spec.Targets = append(spec.Targets, executor.IndexMaintenanceTarget{Role: catalog.IndexIgnored_TblType, Name: "ignored", ID: 5})
	require.NoError(t, h.ValidateIndexMaintenance(function, literal(), spec))
	roles, err = h.IndexMaintenanceRoles(function, map[string]*plan.IndexDef{
		catalog.Hnsw_TblType_Metadata: {}, catalog.Hnsw_TblType_Storage: {}, catalog.IndexIgnored_TblType: {},
	})
	require.NoError(t, err)
	require.Contains(t, roles, catalog.IndexIgnored_TblType)
	cfg.IgnoredTable = "forged"
	require.Error(t, h.ValidateIndexMaintenance(function, literal(), spec))
	cfg.IgnoredTable = "ignored"
	cfg.MetadataTable, cfg.IndexTable = cfg.IndexTable, cfg.MetadataTable
	require.Error(t, h.ValidateIndexMaintenance(function, literal(), spec))
	cfg.MetadataTable, cfg.IndexTable, cfg.SrcTable = "meta", "store", "other"
	require.Error(t, h.ValidateIndexMaintenance(function, literal(), spec))
}

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
	function := "ivf_create"
	spec := &executor.IndexMaintenanceSpec{Function: function, Database: "db", DatabaseID: 1, BaseName: "base", BaseID: 2,
		Targets: []executor.IndexMaintenanceTarget{
			{Role: catalog.SystemSI_IVFFLAT_TblType_Metadata, Name: "meta", ID: 3},
			{Role: catalog.SystemSI_IVFFLAT_TblType_Centroids, Name: "store", ID: 4},
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
	require.Equal(t, function, h.InitSQLMaintenanceFunction())
	roles, err := h.IndexMaintenanceRoles(function, map[string]*plan.IndexDef{
		catalog.SystemSI_IVFFLAT_TblType_Metadata: {}, catalog.SystemSI_IVFFLAT_TblType_Centroids: {},
	})
	require.NoError(t, err)
	require.Len(t, roles, 2)
	cfg.MetadataTable, cfg.IndexTable = cfg.IndexTable, cfg.MetadataTable
	require.Error(t, h.ValidateIndexMaintenance(function, literal(), spec))
	cfg.MetadataTable, cfg.IndexTable, cfg.SrcTable = "meta", "store", "other"
	require.Error(t, h.ValidateIndexMaintenance(function, literal(), spec))
}

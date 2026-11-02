// Copyright 2026 Matrix Origin
// Licensed under the Apache License, Version 2.0. See the LICENSE file.

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
	function := "cagra_create"
	spec := &executor.IndexMaintenanceSpec{Function: function, Database: "db", DatabaseID: 1, BaseName: "base", BaseID: 2,
		Targets: []executor.IndexMaintenanceTarget{
			{Role: catalog.Cagra_TblType_Metadata, Name: "meta", ID: 3},
			{Role: catalog.Cagra_TblType_Storage, Name: "store", ID: 4},
		}}
	cfg := vectorindex.IndexTableConfig{DbName: "db", SrcTable: "base", MetadataTable: "meta", IndexTable: "store"}
	literal := func() []string {
		b, err := json.Marshal(cfg)
		require.NoError(t, err)
		return []string{"{}", string(b)}
	}
	h := Hooks{}
	require.NoError(t, h.ValidateIndexMaintenance(function, literal(), spec))
	require.Equal(t, function, h.InitSQLMaintenanceFunction())
	roles, err := h.IndexMaintenanceRoles(function, map[string]*plan.IndexDef{
		catalog.Cagra_TblType_Metadata: {}, catalog.Cagra_TblType_Storage: {},
	})
	require.NoError(t, err)
	require.Len(t, roles, 2)
	cfg.MetadataTable, cfg.IndexTable = cfg.IndexTable, cfg.MetadataTable
	require.Error(t, h.ValidateIndexMaintenance(function, literal(), spec))
	cfg.MetadataTable, cfg.IndexTable, cfg.SrcTable = "meta", "store", "other"
	require.Error(t, h.ValidateIndexMaintenance(function, literal(), spec))
}

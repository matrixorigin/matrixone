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

package tables

import (
	"context"
	"testing"

	pkgcatalog "github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/stretchr/testify/require"
)

func TestRelLogicalIDLegacyProofIsNarrow(t *testing.T) {
	moTables := catalog.SystemTableSchema
	logicalIdx := moTables.NameMap[pkgcatalog.SystemRelAttr_LogicalID]
	logicalSeqnum := moTables.ColDefs[logicalIdx].SeqNum
	require.Equal(t, uint16(pkgcatalog.MO_TABLES_LOGICAL_ID_IDX), logicalSeqnum)
	id := &common.ID{DbID: pkgcatalog.MO_CATALOG_ID, TableID: pkgcatalog.MO_TABLES_ID}
	ctx := withRelLogicalIDLegacyProof(context.Background(), id, moTables, "mo_tables-object")
	require.True(t, objectio.CanFillMissingColumn(ctx, "mo_tables-object", logicalSeqnum))
	require.False(t, objectio.CanFillMissingColumn(ctx, "other-object", logicalSeqnum))
	require.False(t, objectio.CanFillMissingColumn(ctx, "mo_tables-object", logicalSeqnum+1))

	for _, tc := range []struct {
		name   string
		id     *common.ID
		change func(*catalog.Schema)
	}{
		{name: "missing relation identity"},
		{name: "same table ID in another database", id: &common.ID{DbID: 100, TableID: pkgcatalog.MO_TABLES_ID}},
		{name: "same name in another table", id: &common.ID{DbID: pkgcatalog.MO_CATALOG_ID, TableID: 100}},
		{name: "wrong schema name", id: id, change: func(s *catalog.Schema) { s.Name = "user_table" }},
		{name: "missing logical ID", id: id, change: func(s *catalog.Schema) { s.ColDefs[logicalIdx] = nil }},
		{name: "wrong logical ID type", id: id, change: func(s *catalog.Schema) {
			s.ColDefs[logicalIdx].Type = types.T_varchar.ToType()
		}},
		{name: "wrong logical ID seqnum", id: id, change: func(s *catalog.Schema) { s.ColDefs[logicalIdx].SeqNum++ }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			schema := moTables.Clone()
			if tc.change != nil {
				tc.change(schema)
			}
			ctx := withRelLogicalIDLegacyProof(context.Background(), tc.id, schema, "test-object")
			for _, col := range schema.ColDefs {
				if col != nil {
					require.False(t, objectio.CanFillMissingColumn(ctx, "test-object", col.SeqNum))
				}
			}
		})
	}
	ctx = withRelLogicalIDLegacyProof(context.Background(), id, nil, "test-object")
	require.False(t, objectio.CanFillMissingColumn(ctx, "test-object", logicalSeqnum))
	ctx = withRelLogicalIDLegacyProof(context.Background(), id, moTables, "")
	require.False(t, objectio.CanFillMissingColumn(ctx, "", logicalSeqnum))
}

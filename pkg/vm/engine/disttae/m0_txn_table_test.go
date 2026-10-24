//go:build mo_m0

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

package disttae

import (
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/stretchr/testify/require"
)

func TestM0AlterRefreshesSameRelationDefinitionCache(t *testing.T) {
	nullable := &plan.Default{NullAbility: true}
	table := &txnTable{
		tableId:   9,
		tableName: "customer",
		db: &txnDatabase{
			databaseName: "m0_cache",
			databaseId:   7,
		},
		relKind:    "r",
		version:    0,
		primaryIdx: 0,
		defs: []engine.TableDef{
			&engine.AttributeDef{Attr: engine.Attribute{
				Name: "c_id", ID: 1, Type: types.T_int32.ToType(), Primary: true,
			}},
			&engine.AttributeDef{Attr: engine.Attribute{
				Name: "payload", ID: 2, Type: types.T_varchar.ToType(),
				Seqnum: 1, Default: nullable,
			}},
			&engine.AttributeDef{Attr: engine.Attribute{
				Name: catalog.Row_ID, ID: 3, Type: objectio.RowidType,
				IsHidden: true, Seqnum: 2,
			}},
		},
	}
	require.Len(t, table.GetTableDef(context.Background()).Cols, 3)

	oldDefs := table.defs
	var err error
	table.defs, err = m0AppendCNDefs(oldDefs, &plan.ColDef{
		Name: "added", ColId: 3, Typ: plan.Type{Id: int32(types.T_int32)},
		Seqnum: 2, Default: nullable,
	})
	require.NoError(t, err)
	require.Equal(t, uint64(3), oldDefs[2].(*engine.AttributeDef).Attr.ID)
	require.Equal(t, uint16(2), oldDefs[2].(*engine.AttributeDef).Attr.Seqnum)
	table.version = 1
	table.RefeshTableDef(context.Background())
	table.refreshM0DerivedIndices(context.Background())

	def := table.GetTableDef(context.Background())
	require.Equal(t, uint32(1), def.Version)
	require.Equal(t, int32(2), def.Name2ColIndex["added"])
	require.Equal(t, uint64(3), def.Cols[2].ColId)
	require.Equal(t, uint32(2), def.Cols[2].Seqnum)
	require.Equal(t, uint64(4), def.Cols[3].ColId)
	require.Equal(t, uint32(3), def.Cols[3].Seqnum)
	require.Equal(t, 0, table.primaryIdx)
	require.Equal(t, 0, table.primarySeqnum)

	columns, err := catalog.GenColumnsFromDefs(0, table.tableName, table.db.databaseName,
		table.tableId, table.db.databaseId, table.defs)
	require.NoError(t, err)
	require.Len(t, columns, len(def.Cols))
	for i, column := range columns {
		require.Equal(t, column.Name, def.Cols[i].Name)
		require.Equal(t, uint64(column.Num), def.Cols[i].ColId)
		require.Equal(t, uint32(column.Seqnum), def.Cols[i].Seqnum)
	}

	table.defs = oldDefs
	table.version = 0
	table.RefeshTableDef(context.Background())
	require.Len(t, table.GetTableDef(context.Background()).Cols, 3)
	require.Equal(t, uint64(3), table.GetTableDef(context.Background()).Cols[2].ColId)
}

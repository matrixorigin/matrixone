//go:build mo_m0

// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package txnimpl

import (
	"context"
	"fmt"
	"testing"

	pkgcatalog "github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/internal/m0binding"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/txn/txnbase"
	"github.com/stretchr/testify/require"
)

func TestM0WorkspaceCatalogRowBinding(t *testing.T) {
	owner := &txnStore{}
	objectID := objectio.NewObjectid()
	stats := objectio.NewObjectStatsWithObjectID(&objectID, false, false, false)
	entry := catalog.MockObjectEntry(catalog.MockStaloneTableEntry(1, catalog.MockSchemaAll(2, 1)), stats, false, nil, types.BuildTS(1, 0))
	bat := containers.NewBatch()
	defer bat.Close()
	for i := 0; i <= pkgcatalog.MO_COLUMNS_MAXIDX; i++ {
		vec := containers.MakeVector(types.T_int8.ToType(), common.DefaultAllocator)
		vec.Append(int8(1), false)
		bat.AddVector(fmt.Sprintf("c%d", i), vec)
	}
	space := &tableSpace{entry: entry, node: &anode{data: bat, rows: 1}}
	rel := &txnRelation{table: &txnTable{store: owner, dataTable: &baseTable{tableSpace: space}}}
	id := &common.ID{DbID: pkgcatalog.MO_CATALOG_ID, TableID: pkgcatalog.MO_COLUMNS_ID}
	id.SetObjectID(&objectID)
	local, err := m0WorkspaceCatalogRow(context.Background(), owner, rel, id, 0)
	require.NoError(t, err)
	require.True(t, local)

	_, err = m0WorkspaceCatalogRow(context.Background(), &txnStore{}, rel, id, 0)
	require.Error(t, err)
	_, err = m0WorkspaceCatalogRow(context.Background(), owner, rel, id, 1)
	require.Error(t, err)
	other := *id
	otherID := objectio.NewObjectid()
	other.SetObjectID(&otherID)
	local, err = m0WorkspaceCatalogRow(context.Background(), owner, rel, &other, 0)
	require.NoError(t, err)
	require.False(t, local)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = m0WorkspaceCatalogRow(ctx, owner, rel, id, 0)
	require.ErrorIs(t, err, context.Canceled)
	bat.Delete(0)
	_, err = m0WorkspaceCatalogRow(context.Background(), owner, rel, id, 0)
	require.Error(t, err)
}

func TestM0TNGeneratedBindingValidatesAndHydratesPrivateSchema(t *testing.T) {
	schema := catalog.NewEmptySchema("target")
	require.NoError(t, schema.AppendCol("a", types.T_int32.ToType()))
	require.NoError(t, schema.Finalize(false))
	bound := schema.Clone()
	bound.ColDefs[0].GeneratedCol = []byte{1}
	txn := txnbase.NewTxn(nil, nil, []byte("owner"), types.BuildTS(2, 0), types.TS{})
	binding := &m0TNAddBinding{dbID: 7, tableID: 9, txnID: txn.GetID(), start: txn.GetStartTS(), snapshot: txn.GetSnapshotTS(), expected: schema.Clone(), bound: bound}
	require.NoError(t, binding.Validate(7, 9, txn, schema))
	require.Error(t, binding.Validate(7, 10, txn, schema))
	other := txnbase.NewTxn(nil, nil, []byte("other"), types.BuildTS(2, 0), types.TS{})
	require.Error(t, binding.Validate(7, 9, other, schema))
	txn.SetSnapshotTS(types.BuildTS(9, 0))
	require.Error(t, binding.Validate(7, 9, txn, schema))
	txn.SetSnapshotTS(binding.snapshot)
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	binding.ctx = cancelled
	require.ErrorIs(t, binding.Validate(7, 9, txn, schema), context.Canceled)
	require.ErrorIs(t, binding.Hydrate(7, 9, txn, schema.Clone()), context.Canceled)
	binding.ctx = nil
	private := schema.Clone()
	require.NoError(t, binding.Hydrate(7, 9, txn, private))
	require.Equal(t, []byte{1}, private.ColDefs[0].GeneratedCol)
	require.Empty(t, schema.ColDefs[0].GeneratedCol)
	private.ColDefs[0].GeneratedCol[0] = 2
	require.Equal(t, []byte{1}, bound.ColDefs[0].GeneratedCol)
	schema.ColDefs[0].Type.Width++
	require.Error(t, binding.Validate(7, 9, txn, schema))
	stale := schema.Clone()
	require.Error(t, binding.Hydrate(7, 9, txn, stale))
	require.Empty(t, stale.ColDefs[0].GeneratedCol)
}

func TestM0TNAddBindingAppliesUnderCatalogLock(t *testing.T) {
	schema := catalog.MockSchemaAll(3, 1)
	entry := catalog.MockTableEntryWithDB(catalog.MockDBEntryWithAccInfo(7, 41), 42)
	entry.CreateWithTSLocked(types.BuildTS(1, 0), &catalog.TableMVCCNode{
		Schema: schema, TombstoneSchema: catalog.GetTombstoneSchema(schema),
	})
	txn := txnbase.NewTxn(nil, nil, []byte("m0-add"), types.BuildTS(2, 0), types.TS{})
	bound := schema.Clone()
	bound.ColDefs[0].GeneratedCol = []byte{1}
	binding := &m0TNAddBinding{dbID: 41, tableID: 42, txnID: txn.GetID(), start: txn.GetStartTS(), snapshot: txn.GetSnapshotTS(), expected: schema.Clone(), bound: bound}
	req := api.NewAddColumnReq(41, 42, "added", plan.Type{Id: int32(types.T_int32)}, -1)

	stale := *binding
	stale.expected = binding.expected.Clone()
	stale.expected.ColDefs[0].Type.Width++
	_, _, err := entry.AlterTable(m0binding.WithAdd(context.Background(), &stale), txn, req)
	require.Error(t, err)
	require.Empty(t, schema.ColDefs[0].GeneratedCol)
	_, exists := schema.NameMap["added"]
	require.False(t, exists)

	isNew, first, err := entry.AlterTable(m0binding.WithAdd(context.Background(), binding), txn, req)
	require.NoError(t, err)
	require.True(t, isNew)
	require.NotSame(t, schema, first)
	require.Equal(t, []byte{1}, first.ColDefs[0].GeneratedCol)
	require.Empty(t, schema.ColDefs[0].GeneratedCol)

	secondBinding := &m0TNAddBinding{dbID: 41, tableID: 42, txnID: txn.GetID(), start: txn.GetStartTS(), snapshot: txn.GetSnapshotTS(), expected: first.Clone(), bound: first.Clone()}
	secondReq := api.NewAddColumnReq(41, 42, "added_again", plan.Type{Id: int32(types.T_int32)}, -1)
	isNew, second, err := entry.AlterTable(m0binding.WithAdd(context.Background(), secondBinding), txn, secondReq)
	require.NoError(t, err)
	require.False(t, isNew)
	require.NotSame(t, first, second)
	_, exists = first.NameMap["added_again"]
	require.False(t, exists)
	_, exists = second.NameMap["added_again"]
	require.True(t, exists)
}

// Copyright 2024 Matrix Origin
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

package catalog

import (
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	apipb "github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/data"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/txn/txnbase"
)

type nilReplayDataFactory struct{}

func (*nilReplayDataFactory) MakeTableFactory() TableDataFactory {
	return func(*TableEntry) data.Table { return nil }
}

func (*nilReplayDataFactory) MakeObjectFactory() ObjectDataFactory {
	return func(*ObjectEntry) data.Object { return nil }
}

func TestTableObjectStats(t *testing.T) {
	db := MockDBEntryWithAccInfo(0, 0)
	tbl := MockTableEntryWithDB(db, 1)
	_, detail := tbl.ObjectStats(common.PPL4, 0, 1, false)
	require.Equal(t, "DATA\n", detail.String())

	tbl.dataObjects.Set(MockObjEntryWithTbl(tbl, 10, false))
	_, detail = tbl.ObjectStats(common.PPL3, 0, 1, false)
	require.Equal(t, "DATA\n\n00000000-0000-0000-0000-000000000000_0\n    loaded:true, lv: 0, oSize:0B, cSzie:10B, rows:1, zm: ZM(ANY)0[<nil>,<nil>]--\n", detail.String())

	tbl.tombstoneObjects.Set(MockObjEntryWithTbl(tbl, 20, true))
	_, detail = tbl.ObjectStats(common.PPL4, 0, 1, true)
	require.Equal(t, "TOMBSTONES\n\n000000000000_0 AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAEAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAABQAAAAAAAAAAA==\n", detail.String())
}

func TestReplayedPreparedDMLFenceLifecycle(t *testing.T) {
	table := MockTableEntryWithDB(nil, 1)
	other := MockTableEntryWithDB(nil, 2)
	oldStart := types.BuildTS(10, 0)

	table.RegisterReplayedPreparedDML("txn-1")
	table.RegisterReplayedPreparedDML("txn-1")
	table.RegisterReplayedPreparedDML("txn-2")
	require.False(t, other.ShouldRetryAutoIncrementAlter(oldStart))
	other.RegisterReplayedPreparedDML("other")
	require.True(t, table.ShouldRetryAutoIncrementAlter(oldStart))
	require.True(t, other.ShouldRetryAutoIncrementAlter(oldStart))

	table.ResolveReplayedPreparedDML("txn-1", nil)
	table.ResolveReplayedPreparedDML("txn-1", nil)
	require.True(t, table.ShouldRetryAutoIncrementAlter(oldStart))

	replayedPrepare := types.BuildTS(12, 0)
	replayedCommit := types.BuildTS(13, 0)
	table.ResolveReplayedPreparedDML("txn-2", &replayedCommit)
	require.True(t, table.ShouldRetryAutoIncrementAlter(oldStart))
	require.True(t, table.ShouldRetryAutoIncrementAlter(replayedPrepare))
	require.False(t, table.ShouldRetryAutoIncrementAlter(replayedCommit))
	table.ResolveReplayedPreparedDML("missing", &replayedCommit)
	require.False(t, table.ShouldRetryAutoIncrementAlter(types.BuildTS(22, 0)))
}

func TestAutoIncrementEpochTransition(t *testing.T) {
	schema := MockSchemaAll(3, 1)
	require.Error(t, schema.ApplyAlterTable(apipb.NewUpdateAutoIncrementReq(0, 1, 10, 2)))
	require.Equal(t, uint32(0), schema.Extra.AutoIncrEpoch)

	require.NoError(t, schema.ApplyAlterTable(apipb.NewUpdateAutoIncrementReq(0, 1, 10, 1)))
	require.Equal(t, uint32(1), schema.Extra.AutoIncrEpoch)
	require.NoError(t, schema.ApplyAlterTable(apipb.NewUpdateConstraintReq(0, 1, "constraint")))
	require.Equal(t, uint32(1), schema.Extra.AutoIncrEpoch)

	schema.Extra.AutoIncrEpoch = math.MaxUint32
	require.Error(t, schema.ApplyAlterTable(apipb.NewUpdateAutoIncrementReq(0, 1, 20, 0)))
	require.Equal(t, uint32(math.MaxUint32), schema.Extra.AutoIncrEpoch)
}

func TestSchemaExtraSerializationDoesNotMutateSchema(t *testing.T) {
	schema := MockSchemaAll(3, 1)
	schema.FromPublication = true
	require.False(t, schema.Extra.FromPublication)

	data := schema.MustGetExtraBytes()
	require.False(t, schema.Extra.FromPublication)

	var extra apipb.SchemaExtra
	require.NoError(t, extra.Unmarshal(data))
	require.True(t, extra.FromPublication)
}

func TestObjectList(t *testing.T) {
	ll := NewObjectList(false)
	nobjid := objectio.NewObjectid()
	entry1 := &ObjectEntry{
		ObjectNode: ObjectNode{SortHint: 1},
		EntryMVCCNode: EntryMVCCNode{
			CreatedAt: types.BuildTS(1, 0),
		},
		ObjectMVCCNode: ObjectMVCCNode{ObjectStats: *objectio.NewObjectStatsWithObjectID(&nobjid, true, false, false)},
		CreateNode:     txnbase.NewTxnMVCCNodeWithTS(types.BuildTS(1, 0)),
		ObjectState:    ObjectState_Create_ApplyCommit,
	}
	entry2 := entry1.Clone()
	entry2.DeletedAt = types.BuildTS(2, 0)
	entry2.ObjectState = ObjectState_Delete_ApplyCommit
	entry1.nextVersion = entry2
	entry2.prevVersion = entry1
	ll.Set(entry1)
	ll.Set(entry2)

	t.Log("\n", ll.Show())

	t.Log(ll.getNodes(entry1.ID(), true))
	t.Log(ll.getNodes(entry1.ID(), false))
}

func TestObjectListUpdateCreateTSWithDeleteEntry(t *testing.T) {
	ll := NewObjectList(false)
	nobjid := objectio.NewObjectid()
	createTS := types.BuildTS(10, 0)
	deleteTS := types.BuildTS(20, 0)
	updatedCreateTS := types.BuildTS(5, 0)
	createEntry := &ObjectEntry{
		ObjectNode: ObjectNode{SortHint: 1},
		EntryMVCCNode: EntryMVCCNode{
			CreatedAt: createTS,
		},
		ObjectMVCCNode: ObjectMVCCNode{ObjectStats: *objectio.NewObjectStatsWithObjectID(&nobjid, true, false, false)},
		CreateNode:     txnbase.NewTxnMVCCNodeWithTS(createTS),
		ObjectState:    ObjectState_Create_ApplyCommit,
	}
	deleteEntry := createEntry.Clone()
	deleteEntry.DeletedAt = deleteTS
	deleteEntry.DeleteNode = txnbase.NewTxnMVCCNodeWithTS(deleteTS)
	deleteEntry.ObjectState = ObjectState_Delete_ApplyCommit
	updatedCreateEntry := createEntry.Clone()
	updatedCreateEntry.nextVersion = deleteEntry
	deleteEntry.prevVersion = updatedCreateEntry

	ll.modify(nil, deleteEntry, updatedCreateEntry)
	updated, err := ll.UpdateCreateTS(createEntry.ID(), updatedCreateTS)
	require.NoError(t, err)
	require.True(t, updated.IsDEntry())

	nodes := ll.GetAllNodes(createEntry.ID())
	require.Len(t, nodes, 2)
	require.Equal(t, updatedCreateTS, nodes[0].CreatedAt)
	require.Equal(t, updatedCreateTS, nodes[0].CreateNode.GetPrepare())
	require.Equal(t, updatedCreateTS, nodes[1].CreatedAt)
	require.Equal(t, updatedCreateTS, nodes[1].CreateNode.GetPrepare())
	require.Same(t, nodes[0].prevVersion, nodes[1])
	require.Same(t, nodes[1].nextVersion, nodes[0])
	require.Equal(t, 2, ll.loadTree().Len())
	require.Equal(t, 1, ll.loadTrees().visible.Len())
	require.NoError(t, ll.DeleteAllEntries(createEntry.ID()))
	require.Zero(t, ll.loadTree().Len())
	require.Zero(t, ll.loadTrees().visible.Len())
}

func TestGetSoftdeleteObjects(t *testing.T) {
	db := MockDBEntryWithAccInfo(0, 0)
	tbl := MockTableEntryWithDB(db, 1)

	// Test empty table
	objs := tbl.GetSoftdeleteObjects(types.BuildTS(1, 0), types.BuildTS(2, 0))
	require.Equal(t, 0, len(objs))

	// Add some objects
	obj1 := MockObjEntryWithTbl(tbl, 10, false)
	obj1.DeletedAt = types.BuildTS(2, 0)
	obj1Create := obj1.Clone()
	obj1Create.DeletedAt = types.TS{}
	obj1Create.nextVersion = obj1
	obj1.prevVersion = obj1Create
	tbl.dataObjects.modify(nil, obj1, obj1Create)

	obj2 := MockObjEntryWithTbl(tbl, 20, false)
	obj2.DeletedAt = types.BuildTS(3, 0)
	obj2Create := obj2.Clone()
	obj2Create.DeletedAt = types.TS{}
	obj2Create.nextVersion = obj2
	obj2.prevVersion = obj2Create
	tbl.dataObjects.modify(nil, obj2, obj2Create)

	// Test getting objects between ts1 and ts2
	objs = tbl.GetSoftdeleteObjects(types.BuildTS(1, 0), types.BuildTS(2, 0))
	require.Equal(t, 1, len(objs))
	require.Equal(t, obj1.ID(), objs[0].ID())

	// Test getting objects between ts2 and ts3
	objs = tbl.GetSoftdeleteObjects(types.BuildTS(2, 1), types.BuildTS(3, 0))
	require.Equal(t, 1, len(objs))
	require.Equal(t, obj2.ID(), objs[0].ID())

	// Test getting all objects
	objs = tbl.GetSoftdeleteObjects(types.BuildTS(1, 0), types.BuildTS(3, 0))
	require.Equal(t, 2, len(objs))
}

func TestGetSoftdeleteObjects2(t *testing.T) {
	db := MockDBEntryWithAccInfo(0, 0)
	tbl := MockTableEntryWithDB(db, 1)

	addActiveObject := func(create int64) *ObjectEntry {
		object := MockObjEntryWithTbl(tbl, 10, false)
		object.CreatedAt = types.BuildTS(create, 0)
		object.ObjectState = ObjectState_Create_ApplyCommit
		object.CreateNode = txnbase.TxnMVCCNode{
			Start:   types.BuildTS(create-1, 0),
			Prepare: types.BuildTS(create, 0),
			End:     types.BuildTS(create, 0),
		}
		tbl.dataObjects.modify(nil, object, nil)
		return object
	}

	addSoftDeleteObject := func(create, delete int64) *ObjectEntry {
		createEntry := addActiveObject(create)
		dropEntry := createEntry.Clone()
		updatedCreate := createEntry.Clone()
		dropEntry.DeletedAt = types.BuildTS(delete, 0)
		dropEntry.ObjectState = ObjectState_Delete_ApplyCommit
		dropEntry.DeleteNode = txnbase.TxnMVCCNode{
			Start:   types.BuildTS(delete-1, 0),
			Prepare: types.BuildTS(delete, 0),
			End:     types.BuildTS(delete, 0),
		}
		updatedCreate.nextVersion = dropEntry
		dropEntry.prevVersion = updatedCreate
		tbl.dataObjects.modify(nil, dropEntry, updatedCreate)
		return dropEntry
	}

	addActiveObject(1)
	objs := tbl.GetSoftdeleteObjects(types.BuildTS(1, 0), types.BuildTS(2, 0))
	assert.Equal(t, 0, len(objs))
	addSoftDeleteObject(1, 2)
	objs = tbl.GetSoftdeleteObjects(types.BuildTS(1, 0), types.BuildTS(2, 0))
	assert.Equal(t, 1, len(objs))
	addSoftDeleteObject(1, 3)
	objs = tbl.GetSoftdeleteObjects(types.BuildTS(1, 0), types.BuildTS(3, 0))
	assert.Equal(t, 2, len(objs))
	addActiveObject(4)
	objs = tbl.GetSoftdeleteObjects(types.BuildTS(2, 0), types.BuildTS(5, 0))
	assert.Equal(t, 2, len(objs))
}

func TestReplayCheckpointDeleteObjectSoftDeleteCollection(t *testing.T) {
	catalog := MockCatalog(&nilReplayDataFactory{})
	db := NewReplayDBEntry()
	db.ID = 100
	db.catalog = catalog
	db.DBNode = &DBNode{name: "backup"}
	require.NoError(t, catalog.AddEntryLocked(db, nil, true))

	tbl := MockTableEntryWithDB(db, 200)
	require.NoError(t, db.AddEntryLocked(tbl, nil, true))

	createTS := types.BuildTS(10, 0)
	deleteTS := types.BuildTS(20, 0)
	endTS := types.BuildTS(30, 0)
	objID := objectio.NewObjectid()
	stats := objectio.NewObjectStatsWithObjectID(&objID, true, false, false)

	// Neither timestamp equals the checkpoint commit timestamp, reproducing the
	// backup-only replay branch that used to publish one combined C/D entry.
	catalog.onReplayCheckpointObject(
		db.ID,
		tbl.ID,
		&objID,
		createTS,
		deleteTS,
		createTS.Prev(),
		endTS,
		endTS,
		&ObjectMVCCNode{ObjectStats: *stats},
		false,
	)

	nodes := tbl.dataObjects.GetAllNodes(&objID)
	require.Len(t, nodes, 2)
	createEntry := nodes[1]
	deleteEntry := createEntry.GetNextVersion()
	require.NotNil(t, deleteEntry)
	require.Same(t, createEntry, deleteEntry.GetPrevVersion())
	require.True(t, createEntry.HasDCounterpart())
	require.True(t, deleteEntry.IsDEntry())
	require.Equal(t, createTS, createEntry.CreatedAt)
	require.True(t, createEntry.DeletedAt.IsEmpty())
	require.Equal(t, deleteTS, deleteEntry.DeletedAt)
	require.Same(t, deleteEntry, nodes[0])
	require.Same(t, createEntry, nodes[1])

	softDeletes := tbl.GetSoftdeleteObjects(createTS, deleteTS)
	require.Len(t, softDeletes, 1)
	require.Same(t, deleteEntry, softDeletes[0])

	// Replaying the same checkpoint record must reuse the linked versions
	// instead of inserting another pair.
	catalog.onReplayCheckpointObject(
		db.ID,
		tbl.ID,
		&objID,
		createTS,
		deleteTS,
		createTS.Prev(),
		endTS,
		endTS,
		&ObjectMVCCNode{ObjectStats: *stats},
		false,
	)
	require.Len(t, tbl.dataObjects.GetAllNodes(&objID), 2)
	require.Len(t, tbl.GetSoftdeleteObjects(createTS, deleteTS), 1)
}

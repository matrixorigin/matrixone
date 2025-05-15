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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/txn/txnbase"
)

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
	ll.Set(entry1)
	ll.Set(entry2)

	t.Log("\n", ll.Show())

	t.Log(ll.getNodes(entry1.ID(), true))
	t.Log(ll.getNodes(entry1.ID(), false))
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
	tbl.dataObjects.Set(obj1)

	obj2 := MockObjEntryWithTbl(tbl, 20, false)
	obj2.DeletedAt = types.BuildTS(3, 0)
	tbl.dataObjects.Set(obj2)

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
		dropEntry.DeletedAt = types.BuildTS(delete, 0)
		dropEntry.ObjectState = ObjectState_Delete_ApplyCommit
		dropEntry.CreateNode = txnbase.TxnMVCCNode{
			Start:   types.BuildTS(delete-1, 0),
			Prepare: types.BuildTS(delete, 0),
			End:     types.BuildTS(delete, 0),
		}
		tbl.dataObjects.modify(nil, dropEntry, nil)
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

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
	_, detail = tbl.ObjectStats(common.PPL4, 0, 1, false)
	require.Equal(t, "DATA\n\n00000000-0000-0000-0000-000000000000_0\n    loaded:true, oSize:0B, cSzie:10B rows:1, zm: ZM(ANY)0[<nil>,<nil>]--\n", detail.String())

	tbl.tombstoneObjects.Set(MockObjEntryWithTbl(tbl, 20, true))
	_, detail = tbl.ObjectStats(common.PPL4, 0, 1, true)
	require.Equal(t, "TOMBSTONES\n\n00000000-0000-0000-0000-000000000000_0\n    loaded:true, oSize:0B, cSzie:20B rows:1, zm: ZM(ANY)0[<nil>,<nil>]--\n", detail.String())
}

func TestObjectList(t *testing.T) {
	ll := NewObjectList(false)
	entry1 := &ObjectEntry{
		ObjectNode: ObjectNode{SortHint: 1},
		EntryMVCCNode: EntryMVCCNode{
			CreatedAt: types.BuildTS(1, 0),
		},
		ObjectMVCCNode: ObjectMVCCNode{ObjectStats: *objectio.NewObjectStatsWithObjectID(objectio.NewObjectid(), true, false, false)},
		CreateNode:     *txnbase.NewTxnMVCCNodeWithTS(types.BuildTS(1, 0)),
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

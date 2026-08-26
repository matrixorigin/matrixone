// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package txnimpl

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/btree"
)

func makeIncrementalObject(
	marker byte,
	appendable bool,
	createdAt, deletedAt int64,
) *catalog.ObjectEntry {
	var id objectio.ObjectId
	id[0] = marker
	entry := &catalog.ObjectEntry{
		EntryMVCCNode: catalog.EntryMVCCNode{
			CreatedAt: types.BuildTS(createdAt, 0),
		},
		ObjectMVCCNode: catalog.ObjectMVCCNode{
			ObjectStats: *objectio.NewObjectStatsWithObjectID(&id, appendable, false, false),
		},
	}
	if deletedAt != 0 {
		entry.DeletedAt = types.BuildTS(deletedAt, 0)
	}
	return entry
}

func TestForeachIncrementalObjectUsesGroupBounds(t *testing.T) {
	tree := btree.NewBTreeG((*catalog.ObjectEntry).Less)
	for _, entry := range []*catalog.ObjectEntry{
		makeIncrementalObject(1, true, 1, 0),
		makeIncrementalObject(2, true, 3, 0),
		makeIncrementalObject(3, true, 5, 0),
		makeIncrementalObject(4, true, 7, 0),
		makeIncrementalObject(8, false, 3, 0),
		makeIncrementalObject(9, false, 4, 0),
		makeIncrementalObject(10, false, 6, 0),
		makeIncrementalObject(11, false, 7, 0),
	} {
		tree.Set(entry)
	}

	it := tree.Iter()
	defer it.Release()
	var markers []byte
	err := foreachIncrementalObject(
		&it,
		types.BuildTS(4, 0),
		types.BuildTS(6, 0),
		func(entry *catalog.ObjectEntry) error {
			markers = append(markers, entry.ID()[0])
			return nil
		},
	)
	require.NoError(t, err)
	// Every appendable create entry at or before to is visited because an old
	// appendable object can contain an append prepared in the incremental range.
	require.Equal(t, []byte{1, 2, 3, 9, 10}, markers)
}

func TestForeachIncrementalObjectDropStartsAfterFrom(t *testing.T) {
	cata := catalog.MockCatalog(nil)
	db := catalog.MockDBEntryWithAccInfo(0, 1)
	table := catalog.MockTableEntryWithDB(db, 2)

	for _, deletedAt := range []int64{2, 3, 4, 5} {
		created := catalog.MockCreatedObjectEntry2List(
			table, cata, false, types.BuildTS(2, 0),
		)
		catalog.MockDroppedObjectEntry2List(created, types.BuildTS(deletedAt, 0))
	}

	it := table.MakeDataObjectIt()
	defer it.Release()
	var deletedAt []types.TS
	err := foreachIncrementalObject(
		&it,
		types.BuildTS(2, 0),
		types.BuildTS(4, 0),
		func(entry *catalog.ObjectEntry) error {
			deletedAt = append(deletedAt, entry.DeletedAt)
			return nil
		},
	)
	require.NoError(t, err)
	// DeletedAt == from is excluded; DeletedAt < to is no longer visible at to;
	// the scan continues through the group and keeps entries deleted at/after to.
	require.Equal(t, []types.TS{types.BuildTS(4, 0), types.BuildTS(5, 0)}, deletedAt)
}

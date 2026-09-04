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
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tables"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tables/updates"
	"github.com/stretchr/testify/require"
)

func TestForeachIncrementalObjectUsesGroupBounds(t *testing.T) {
	cata := catalog.MockCatalog(nil)
	db := catalog.MockDBEntryWithAccInfo(0, 1)
	table := catalog.MockTableEntryWithDB(db, 2)
	for _, appendable := range []bool{true, false} {
		for _, createdAt := range []int64{1, 3, 4, 5, 6, 7} {
			catalog.MockCreatedObjectEntry2ListWithAppendable(
				table, cata, false, appendable, types.BuildTS(createdAt, 0),
			)
		}
	}

	snapshot := table.MakeDataObjectSnapshot()
	var markers []int64
	err := foreachIncrementalObject(
		snapshot,
		types.BuildTS(4, 0),
		types.BuildTS(6, 0),
		func(entry *catalog.ObjectEntry) error {
			marker := entry.CreatedAt.Physical()
			if !entry.IsAppendable() {
				marker += 100
			}
			markers = append(markers, marker)
			return nil
		},
	)
	require.NoError(t, err)
	// Every appendable create entry at or before to is visited because an old
	// appendable object can contain an append prepared in the incremental range.
	require.Equal(t, []int64{1, 3, 4, 5, 6, 104, 105, 106}, markers)
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

	snapshot := table.MakeDataObjectSnapshot()
	var deletedAt []types.TS
	err := foreachIncrementalObject(
		snapshot,
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

func TestForeachIncrementalObjectSkipsFinalizedOldAppendableObject(t *testing.T) {
	factory := tables.NewDataFactory(nil, "")
	cata := catalog.MockCatalog(factory)
	defer cata.Close()
	db, err := cata.CreateDBEntry("db", "", "", nil)
	require.NoError(t, err)
	table, err := db.CreateTableEntry(catalog.MockSchema(1, 0), nil, nil)
	require.NoError(t, err)
	createObject := func(createdAt types.TS) *catalog.ObjectEntry {
		id := objectio.NewObjectid()
		stats := objectio.NewObjectStatsWithObjectID(&id, true, false, false)
		entry, createErr := table.CreateCommittedObject(
			createdAt,
			&objectio.CreateObjOpt{Stats: stats},
			factory.MakeObjectFactory(),
		)
		require.NoError(t, createErr)
		return entry
	}

	old := createObject(types.BuildTS(1, 0))
	old.GetObjectData().(interface{ SealAppend() }).SealAppend()

	equal := createObject(types.BuildTS(2, 0))
	require.NoError(t, equal.GetObjectData().OnReplayAppend(
		updates.MockAppendNode(types.BuildTS(4, 0), 0, 1, nil),
	))
	equal.GetObjectData().(interface{ SealAppend() }).SealAppend()

	createObject(types.BuildTS(3, 0))

	var visited []int64
	err = foreachIncrementalObject(
		table.MakeDataObjectSnapshot(),
		types.BuildTS(4, 0),
		types.BuildTS(6, 0),
		func(entry *catalog.ObjectEntry) error {
			visited = append(visited, entry.CreatedAt.Physical())
			return nil
		},
	)
	require.NoError(t, err)
	// The empty finalized object has maxCommitTS=0 and is skipped. The object
	// whose max equals from is retained because the optimization is strict <.
	// An object that has not been sealed/finalized is always retained.
	require.Equal(t, []int64{2, 3}, visited)
}

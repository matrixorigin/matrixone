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

package db

import (
	"context"
	"errors"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/objectio/ioutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/data"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tables"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/txn/txnbase"
	"github.com/stretchr/testify/require"
)

func TestReplayRejectsLegacyMultiTNTransaction(t *testing.T) {
	replayer := &WalReplayer{}
	cmd := txnbase.NewTxnCmd()
	cmd.Participants = []uint64{1, 2}

	require.PanicsWithError(t,
		moerr.NewNotSupportedNoCtxf(
			"replay of a transaction spanning %d TN shards", len(cmd.Participants),
		).Error(),
		func() {
			replayer.OnReplayTxn(cmd, 1)
		},
	)
}

type testReplayCommitter struct {
	err error
}

func (c testReplayCommitter) Commit(context.Context) error {
	return c.err
}

func TestCommitReplayTxn(t *testing.T) {
	require.NotPanics(t, func() {
		commitReplayTxn(context.Background(), testReplayCommitter{})
	})

	err := errors.New("commit failed")
	require.PanicsWithError(t, err.Error(), func() {
		commitReplayTxn(context.Background(), testReplayCommitter{err: err})
	})
}

func TestCheckpointReplayObjectData(t *testing.T) {
	for _, tc := range []struct {
		name       string
		appendable bool
		tombstone  bool
	}{
		{name: "appendable_data", appendable: true},
		{name: "persisted_data"},
		{name: "appendable_tombstone", appendable: true, tombstone: true},
		{name: "persisted_tombstone", tombstone: true},
	} {
		for _, order := range []string{"live", "create_then_drop", "first_seen_dropped"} {
			t.Run(tc.name+"/"+order, func(t *testing.T) {
				// Checkpoint objects use persisted nodes, so the real factory needs no I/O runtime.
				c := catalog.MockCatalog(tables.NewDataFactory(nil, ""))
				t.Cleanup(func() { require.NoError(t, c.Close()) })
				database := catalog.NewReplayDBEntry()
				database.ID = 100
				database.DBNode = &catalog.DBNode{}
				database.TestSetCatalog(c)
				require.NoError(t, c.AddEntryLocked(database, nil, true))
				table := catalog.MockTableEntryWithDB(database, 200)
				require.NoError(t, database.AddEntryLocked(table, nil, true))
				objectType := int8(ioutil.ObjectType_Data)
				makeIter := table.MakeDataObjectIt
				if tc.tombstone {
					objectType = ioutil.ObjectType_Tombstone
					makeIter = table.MakeTombstoneObjectIt
				}
				id := objectio.NewObjectid()
				stats := objectio.NewObjectStatsWithObjectID(&id, tc.appendable, false, false)
				createTS := types.BuildTS(10, 0)
				deleteTS := types.TS{}
				if order != "live" {
					deleteTS = types.BuildTS(20, 0)
				}
				var originalData data.Object
				if order == "create_then_drop" {
					c.OnReplayObjectBatch_V2(database.ID, table.ID, objectType, *stats, createTS, types.TS{})
					created, err := table.GetObjectByID(&id, tc.tombstone)
					require.NoError(t, err)
					originalData = created.GetObjectData()
					require.NotNil(t, originalData)
				}
				var originalEntry *catalog.ObjectEntry
				for replay := 0; replay < 2; replay++ {
					c.OnReplayObjectBatch_V2(database.ID, table.ID, objectType, *stats, createTS, deleteTS)
					entry, err := table.GetObjectByID(&id, tc.tombstone)
					require.NoError(t, err)
					require.NotNil(t, entry.GetObjectData())
					if originalData == nil {
						originalData = entry.GetObjectData()
					}
					require.Same(t, originalData, entry.GetObjectData())
					if originalEntry == nil {
						originalEntry = entry
					}
					require.Same(t, originalEntry, entry)
					require.Equal(t, createTS, entry.CreatedAt)
					require.Equal(t, deleteTS, entry.DeletedAt)
					require.Equal(t, tc.appendable, entry.IsAppendable())
					require.Equal(t, tc.tombstone, entry.IsTombstone)
					expectedEntries := 1
					if order == "live" {
						require.True(t, entry.IsCEntry())
						require.False(t, entry.HasDropCommitted())
						require.Nil(t, entry.GetPrevVersion())
						require.Nil(t, entry.GetNextVersion())
					} else {
						expectedEntries = 2
						require.True(t, entry.IsDEntry())
						require.True(t, entry.HasDropCommitted())
						created := entry.GetPrevVersion()
						require.NotNil(t, created)
						require.True(t, created.IsCEntry())
						require.Equal(t, createTS, created.CreatedAt)
						require.True(t, created.DeletedAt.IsEmpty())
						require.Same(t, entry, created.GetNextVersion())
						require.Same(t, originalData, created.GetObjectData())
					}
					entries := 0
					func() {
						it := makeIter()
						defer it.Release()
						for ok := it.First(); ok; ok = it.Next() {
							entries++
						}
					}()
					require.Equal(t, expectedEntries, entries)
					if tc.appendable && !tc.tombstone && order != "live" {
						replayer := newWalReplayer(&DB{Catalog: c}, types.BuildTS(30, 0), 0)
						replayer.PreReplayWal()
						require.Same(t, originalData, entry.GetObjectData())
						require.NoError(t, replayer.postReplayWal())
					}
				}
			})
		}
	}
}

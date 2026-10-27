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

package logtail

import (
	"sync"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/stretchr/testify/require"
)

func TestLogtailVisitsOlderAppendableWithLateCommit(t *testing.T) {
	for _, tombstone := range []bool{false, true} {
		name := "data"
		if tombstone {
			name = "tombstone"
		}
		t.Run(name, func(t *testing.T) {
			c := catalog.MockCatalog(nil)
			defer c.Close()
			db, err := c.CreateDBEntry("late_append", "", "", nil)
			require.NoError(t, err)
			table, err := db.CreateTableEntry(catalog.MockSchema(2, 0), nil, nil)
			require.NoError(t, err)

			create := func(ts int64) *catalog.ObjectEntry {
				id := objectio.NewObjectid()
				stats := objectio.NewObjectStatsWithObjectID(&id, true, false, false)
				entry, err := table.CreateCommittedObject(
					types.BuildTS(ts, 0),
					&objectio.CreateObjOpt{Stats: stats, IsTombstone: tombstone}, nil,
				)
				require.NoError(t, err)
				return entry
			}
			older := create(1)
			newer := create(2)
			createdInRange := create(3)
			create(5)

			from, to := types.BuildTS(3, 0), types.BuildTS(4, 0)
			var snapshot catalog.ObjectListSnapshot
			if tombstone {
				snapshot = table.MakeTombstoneObjectSnapshot()
			} else {
				snapshot = table.MakeDataObjectSnapshot()
			}
			var visited []*catalog.ObjectEntry
			require.NoError(t, visitLogtailObjects(snapshot, from, to, func(entry *catalog.ObjectEntry) error {
				visited = append(visited, entry)
				return nil
			}))
			require.ElementsMatch(t, []*catalog.ObjectEntry{older, newer, createdInRange}, visited)
		})
	}
}

func TestLogtailBookmarkKeepsInRangeObjectCreation(t *testing.T) {
	for _, tombstone := range []bool{false, true} {
		t.Run(map[bool]string{false: "data", true: "tombstone"}[tombstone], func(t *testing.T) {
			c := catalog.MockCatalog(nil)
			defer c.Close()
			db, err := c.CreateDBEntry("bookmark_logtail", "", "", nil)
			require.NoError(t, err)
			table, err := db.CreateTableEntry(catalog.MockSchema(2, 0), nil, nil)
			require.NoError(t, err)
			create := func(ts int64) *catalog.ObjectEntry {
				id := objectio.NewObjectid()
				stats := objectio.NewObjectStatsWithObjectID(&id, true, false, false)
				entry, err := table.CreateCommittedObject(types.BuildTS(ts, 0),
					&objectio.CreateObjOpt{Stats: stats, IsTombstone: tombstone}, nil)
				require.NoError(t, err)
				return entry
			}
			inRange := create(3)
			later := create(5)
			inRange.SetAppendMaxBookmarkScheduler(func(fn func() error) error { return fn() })
			inRange.UpdateAppendMaxCommit(types.TS{}) // sealed but empty/all-aborted
			later.MarkAppendMaxPending()
			var snapshot catalog.ObjectListSnapshot
			if tombstone {
				snapshot = table.MakeTombstoneObjectSnapshot()
			} else {
				snapshot = table.MakeDataObjectSnapshot()
			}
			start, valid := snapshot.CommitBookmarkStart(catalog.ObjectListGroupAppendableCreate, types.BuildTS(3, 0))
			require.True(t, valid)
			require.Same(t, later, start)
			var visited []*catalog.ObjectEntry
			require.NoError(t, visitLogtailObjects(snapshot, types.BuildTS(3, 0), types.BuildTS(4, 0),
				func(entry *catalog.ObjectEntry) error {
					visited = append(visited, entry)
					return nil
				}))
			require.Contains(t, visited, inRange)
		})
	}
}

func TestLogtailVisitorDoesNotHoldObjectTreeLock(t *testing.T) {
	c := catalog.MockCatalog(nil)
	defer c.Close()
	db, err := c.CreateDBEntry("logtail_wait", "", "", nil)
	require.NoError(t, err)
	table, err := db.CreateTableEntry(catalog.MockSchema(2, 0), nil, nil)
	require.NoError(t, err)
	newStats := func() *objectio.ObjectStats {
		id := objectio.NewObjectid()
		return objectio.NewObjectStatsWithObjectID(&id, true, false, false)
	}
	_, err = table.CreateCommittedObject(types.BuildTS(1, 0), &objectio.CreateObjOpt{Stats: newStats()}, nil)
	require.NoError(t, err)

	entered := make(chan struct{})
	release := make(chan struct{})
	var releaseOnce sync.Once
	defer releaseOnce.Do(func() { close(release) })
	scanDone := make(chan error, 1)
	go func() {
		var enteredOnce sync.Once
		scanDone <- visitLogtailObjects(
			table.MakeDataObjectSnapshot(), types.BuildTS(2, 0), types.BuildTS(3, 0),
			func(*catalog.ObjectEntry) error {
				enteredOnce.Do(func() { close(entered) })
				<-release
				return nil
			},
		)
	}()
	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		t.Fatal("logtail did not visit the old object")
	}

	writeDone := make(chan error, 1)
	go func() {
		_, err := table.CreateCommittedObject(
			types.BuildTS(4, 0), &objectio.CreateObjOpt{Stats: newStats()}, nil,
		)
		writeDone <- err
	}()
	select {
	case err := <-writeDone:
		require.NoError(t, err)
	case <-time.After(3 * time.Second):
		t.Fatal("catalog writer blocked behind logtail visitor")
	}
	releaseOnce.Do(func() { close(release) })
	select {
	case err := <-scanDone:
		require.NoError(t, err)
	case <-time.After(3 * time.Second):
		t.Fatal("logtail scan did not finish")
	}
}

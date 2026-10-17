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

package checkpoint

import (
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/dbutils"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/data"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tasks"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/buffer"
	"go.uber.org/zap/zapcore"
)

type flushReadyObject struct {
	data.Object
	ready bool
	calls int
}

func (o *flushReadyObject) PrepareCompact() bool {
	o.calls++
	return o.ready
}

type flushAdmissionScheduler struct {
	tasks.TaskScheduler
	scopes [][]common.ID
	failAt int
}

func (s *flushAdmissionScheduler) ScheduleMultiScopedTxnTask(
	_ *tasks.Context, taskType tasks.TaskType, scopes []common.ID, _ tasks.TxnTaskFactory,
) (tasks.Task, error) {
	if taskType != tasks.FlushTableTailTask {
		panic("unexpected task type")
	}
	// Retain the caller's slice so later chunk/retry mutation is observable.
	s.scopes = append(s.scopes, scopes)
	if len(s.scopes) == s.failAt {
		return nil, tasks.ErrScheduleScopeConflict
	}
	return nil, nil
}

type flushSelectionFixture struct {
	flusher   *flushImpl
	table     *catalog.TableEntry
	metas     []*catalog.ObjectEntry
	objects   []*flushReadyObject
	scheduler *flushAdmissionScheduler
}

func newFlushSelectionFixture(t testing.TB, dataReady, tombstoneReady []bool) *flushSelectionFixture {
	t.Helper()
	c := catalog.MockCatalog(nil)
	t.Cleanup(func() { require.NoError(t, c.Close()) })
	db, err := c.CreateDBEntry("db", "", "", nil)
	require.NoError(t, err)
	table, err := db.CreateTableEntry(catalog.MockSchema(1, 0), nil, nil)
	require.NoError(t, err)
	f := &flushSelectionFixture{table: table, scheduler: &flushAdmissionScheduler{}}
	for kind, readiness := range [][]bool{dataReady, tombstoneReady} {
		for _, ready := range readiness {
			object := &flushReadyObject{ready: ready}
			id := objectio.NewObjectid()
			stats := objectio.NewObjectStatsWithObjectID(&id, true, false, false)
			meta, err := table.CreateCommittedObject(
				types.BuildTS(int64(len(f.metas)+1), 0),
				&objectio.CreateObjOpt{Stats: stats, IsTombstone: kind == 1},
				func(*catalog.ObjectEntry) data.Object { return object },
			)
			require.NoError(t, err)
			f.metas = append(f.metas, meta)
			f.objects = append(f.objects, object)
		}
	}
	f.flusher = &flushImpl{
		rt:             &dbutils.Runtime{Scheduler: f.scheduler},
		flushInterval:  time.Hour,
		objMemSizeList: []tableAndSize{{tbl: table}},
	}
	return f
}

func (f *flushSelectionFixture) end() types.TS {
	return types.BuildTS(int64(len(f.metas)+1), 0)
}

func (f *flushSelectionFixture) scopes(indices ...int) []common.ID {
	result := make([]common.ID, 0, len(indices))
	for _, i := range indices {
		result = append(result, *f.metas[i].AsCommonID())
	}
	return result
}

func TestFireFlushTabletailSelectsReadyObjects(t *testing.T) {
	for _, tc := range []struct {
		name          string
		data          []bool
		tombstones    []bool
		wantScopes    [][]int
		wantDeferred  bool
		wantTombCalls int
	}{
		{"ready-busy-ready", []bool{true, false, true}, []bool{true, true}, [][]int{{0, 2, 3, 4}}, true, 1},
		{"all-busy-does-not-flush-tombstones", []bool{false, false}, []bool{true}, nil, true, 0},
		{"no-objects", nil, nil, nil, false, 0},
		{"tombstone-only", nil, []bool{true, true}, [][]int{{0, 1}}, false, 1},
		{"busy-tombstone-blocks-prepared-data", []bool{true}, []bool{false}, nil, true, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newFlushSelectionFixture(t, tc.data, tc.tombstones)
			err := f.flusher.fireFlushTabletail(f.table, f.end(), types.TS{})
			if tc.wantDeferred {
				require.True(t, moerr.IsMoErrCode(err, moerr.OkExpectedEOB), "got %v", err)
			} else {
				require.NoError(t, err)
			}
			require.Len(t, f.scheduler.scopes, len(tc.wantScopes))
			for i, indices := range tc.wantScopes {
				require.Equal(t, f.scopes(indices...), f.scheduler.scopes[i])
			}
			for i, object := range f.objects {
				wantCalls := 1
				if i >= len(tc.data) {
					wantCalls = tc.wantTombCalls
				}
				require.Equal(t, wantCalls, object.calls)
			}
		})
	}
}

func TestFireFlushTabletailPreservesChunkBoundaries(t *testing.T) {
	for _, scenario := range []string{"partial-first-chunk", "busy-first-chunk", "later-schedule-conflict"} {
		t.Run(scenario, func(t *testing.T) {
			ready := make([]bool, 401)
			firstIndices := make([]int, 0, 401)
			for i := range ready {
				ready[i] = scenario != "busy-first-chunk" || i == 400
				if scenario == "partial-first-chunk" && i == 1 {
					ready[i] = false
				}
				if ready[i] && i < 400 {
					firstIndices = append(firstIndices, i)
				}
			}
			f := newFlushSelectionFixture(t, ready, []bool{true})
			if scenario == "later-schedule-conflict" {
				f.scheduler.failAt = 2
			}
			err := f.flusher.fireFlushTabletail(f.table, f.end(), types.TS{})
			require.True(t, moerr.IsMoErrCode(err, moerr.OkExpectedEOB), "got %v", err)
			if scenario == "busy-first-chunk" {
				require.Equal(t, [][]common.ID{f.scopes(400, 401)}, f.scheduler.scopes)
			} else {
				firstIndices = append(firstIndices, 401)
				require.Equal(t, [][]common.ID{f.scopes(firstIndices...), f.scopes(400)}, f.scheduler.scopes)
			}
			require.Equal(t, 1, f.objects[401].calls, "tombstones must belong to the first admitted chunk only")
		})
	}
}

func TestFlushDeadlineWaitsForAllData(t *testing.T) {
	for _, tc := range []struct {
		name string
		mode flushScheduleMode
	}{
		{"cron", flushScheduleCron},
		{"force", flushScheduleForce},
		{"checkpoint-bounded", flushScheduleCheckpointBounded},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newFlushSelectionFixture(t, []bool{true, false, true}, []bool{true})
			f.table.Stats.Init(-time.Hour)
			deadline := f.table.Stats.GetFlushDeadline()
			entry := newTestDirtyTreeEntry(types.BuildTS(1, 0), f.end(), f.table.ID)
			f.flusher.checkFlushConditionAndFire(entry, tc.mode, 0, types.TS{})
			require.Equal(t, deadline, f.table.Stats.GetFlushDeadline())
			require.Equal(t, [][]common.ID{f.scopes(0, 2, 3)}, f.scheduler.scopes)
			firstScopes := slices.Clone(f.scheduler.scopes[0])

			f.objects[1].ready = true
			f.flusher.checkFlushConditionAndFire(entry, tc.mode, 0, types.TS{})
			require.Len(t, f.scheduler.scopes, 2)
			require.Equal(t, f.scopes(0, 1, 2, 3), f.scheduler.scopes[1])
			require.Equal(t, firstScopes, f.scheduler.scopes[0])
			if tc.mode.resetFlushDeadline() {
				require.True(t, f.table.Stats.GetFlushDeadline().After(deadline))
			} else {
				require.Equal(t, deadline, f.table.Stats.GetFlushDeadline())
			}
		})
	}
}

func TestFireFlushTabletailBoundsDeferredLogging(t *testing.T) {
	for _, tc := range []struct {
		name      string
		allReady  bool
		busyTomb  bool
		failAt    int
		wantBusy  int64
		wantScans int
	}{
		{name: "busy-across-chunks", wantBusy: 401, wantScans: 401},
		{name: "ready-has-no-deferred-log", allReady: true, wantScans: 401},
		{name: "busy-tombstone-early-return", busyTomb: true, wantBusy: 399, wantScans: 400},
		{name: "scheduler-early-return", failAt: 1, wantBusy: 399, wantScans: 400},
	} {
		t.Run(tc.name, func(t *testing.T) {
			readiness := make([]bool, 401)
			for i := range readiness {
				readiness[i] = tc.allReady
			}
			if tc.busyTomb || tc.failAt != 0 {
				readiness[0] = true
			}
			f := newFlushSelectionFixture(t, readiness, []bool{!tc.busyTomb})
			f.scheduler.failAt = tc.failAt
			// Reuse the existing reporter seam; do not replace the package logger.
			// The package's tests are serial, and cleanup restores the prior reporter.
			previous := logutil.GetReportZapFunc()
			var mu sync.Mutex
			var counts []int64
			logutil.SetLogReporter(&logutil.TraceReporter{ReportZap: func(enc zapcore.Encoder, entry zapcore.Entry, fields []zapcore.Field) (*buffer.Buffer, error) {
				if entry.Message == "flusher.data.prepare.compact.false" {
					count := int64(-1)
					for _, field := range fields {
						if field.Key == "deferred-count" {
							count = field.Integer
						}
					}
					mu.Lock()
					counts = append(counts, count)
					mu.Unlock()
				}
				return previous(enc, entry, fields)
			}})
			t.Cleanup(func() { logutil.SetLogReporter(&logutil.TraceReporter{ReportZap: previous}) })
			err := f.flusher.fireFlushTabletail(f.table, f.end(), types.TS{})
			if tc.wantBusy == 0 {
				require.NoError(t, err)
			} else {
				require.True(t, moerr.IsMoErrCode(err, moerr.OkExpectedEOB), "got %v", err)
			}
			mu.Lock()
			got := slices.Clone(counts)
			mu.Unlock()
			if tc.wantBusy == 0 {
				require.Empty(t, got)
			} else {
				require.Equal(t, []int64{tc.wantBusy}, got)
			}
			var scans int
			for _, object := range f.objects[:len(readiness)] {
				scans += object.calls
			}
			require.Equal(t, tc.wantScans, scans)
		})
	}
}

func BenchmarkFireFlushTabletailSelection(b *testing.B) {
	for _, allReady := range []bool{false, true} {
		name := "busy"
		if allReady {
			name = "ready"
		}
		b.Run(name, func(b *testing.B) {
			readiness := make([]bool, 400)
			for i := range readiness {
				readiness[i] = allReady
			}
			f := newFlushSelectionFixture(b, readiness, nil)
			end := f.end()
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				f.scheduler.scopes = f.scheduler.scopes[:0]
				err := f.flusher.fireFlushTabletail(f.table, end, types.TS{})
				if (allReady && err != nil) || (!allReady && !moerr.IsMoErrCode(err, moerr.OkExpectedEOB)) {
					b.Fatalf("unexpected admission result: %v", err)
				}
			}
		})
	}
}

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
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/objectio/ioutil"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae/logtailreplay"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

func newCatalogTombstoneFixture(t testing.TB, rows int, cn bool) (*process.Process, fileservice.FileService, *logtailreplay.PartitionState) {
	t.Helper()
	proc := testutil.NewProc(t)
	mp := proc.Mp()
	t.Cleanup(func() {
		proc.Free()
		proc.GetFileService().Close(context.Background())
		require.Zero(t, mp.CurrNB())
	})
	fs, err := fileservice.Get[fileservice.FileService](proc.GetFileService(), defines.SharedFileServiceName)
	require.NoError(t, err)
	ctx := proc.Ctx
	pkType := types.T_varchar.ToType()
	selection := objectio.HiddenColumnSelection_CommitTS
	columns := 3
	if cn {
		selection = objectio.HiddenColumnSelection_None
		columns = 2
	}
	writer := ioutil.ConstructTombstoneWriter(selection, fs)
	bat := batch.NewWithSize(columns)
	bat.Vecs[0] = vector.NewVec(types.T_Rowid.ToType())
	bat.Vecs[1] = vector.NewVec(pkType)
	if !cn {
		bat.Vecs[2] = vector.NewVec(types.T_TS.ToType())
	}
	t.Cleanup(func() { bat.Clean(mp) })
	objectID := objectio.NewObjectid()
	for row := 0; row < rows; row++ {
		require.NoError(t, vector.AppendFixed(bat.Vecs[0], types.NewRowIDWithObjectIDBlkNumAndRowID(
			objectID, uint16(row/objectio.BlockMaxRows), uint32(row%objectio.BlockMaxRows)), false, mp))
		require.NoError(t, vector.AppendBytes(bat.Vecs[1], []byte(fmt.Sprintf("other-%05d", rows-row)), false, mp))
		if !cn {
			require.NoError(t, vector.AppendFixed(bat.Vecs[2], types.BuildTS(20, 0), false, mp))
		}
		if bat.Vecs[0].Length() == objectio.BlockMaxRows || row == rows-1 {
			bat.SetRowCount(bat.Vecs[0].Length())
			_, err = writer.WriteBatch(bat)
			require.NoError(t, err)
			bat.CleanOnlyData()
		}
	}
	_, _, err = writer.Sync(ctx)
	require.NoError(t, err)
	stats := writer.GetObjectStats()
	if cn {
		objectio.WithCNCreated()(&stats)
	}
	require.EqualValues(t, rows, stats.Rows())
	state := logtailreplay.NewPartitionState("", true, 0, false)
	require.NoError(t, state.HandleObjectEntry(ctx, fs, objectio.ObjectEntry{
		ObjectStats: stats, CreateTime: types.BuildTS(40, 0),
	}, true))
	return proc, fs, state
}

// Exercise the production caller, not just a policy helper. One immutable
// persisted fixture crosses the old 50,000-row boundary. PKs are deliberately
// unrelated to rowid ordering, as in catalog tombstones after compaction.
func TestCatalogTombstonePKCheck(t *testing.T) {
	proc, fs, state := newCatalogTombstoneFixture(t, 50001, false)
	mp, ctx := proc.Mp(), proc.Ctx
	pkType := types.T_varchar.ToType()
	txn := &Transaction{engine: &Engine{fs: fs}}
	op := newTxnOperatorForTestWithWorkspace(t, txn)
	tbl := &txnTable{
		db: &txnDatabase{op: op}, primaryIdx: 0,
		tableDef: &plan.TableDef{
			Cols: []*plan.ColDef{{Name: "pk", Typ: plan.Type{Id: int32(types.T_varchar)}}},
			Pkey: &plan.PrimaryKeyDef{PkeyColName: "pk"}, Name2ColIndex: map[string]int32{"pk": 0},
		},
	}
	tbl.proc.Store(proc)
	for _, tc := range []struct {
		name     string
		id       uint64
		key      string
		from, to int64
		changed  bool
	}{
		{"unrelated-tables", catalog.MO_TABLES_ID, "target", 10, 30, false},
		{"unrelated-databases", catalog.MO_DATABASE_ID, "target", 10, 30, false},
		{"unrelated-columns", catalog.MO_COLUMNS_ID, "target", 10, 30, false},
		{"unrelated-logical-id-index", catalog.MO_TABLES_LOGICAL_ID_INDEX_ID, "target", 10, 30, false},
		{"real-delete-last-block", catalog.MO_TABLES_ID, "other-00001", 10, 20, true},
		{"old-delete-compacted-after-snapshot", catalog.MO_TABLES_ID, "other-00001", 20, 30, false},
		{"delete-after-upper-bound", catalog.MO_TABLES_ID, "other-00001", 10, 19, false},
		{"user-table-keeps-cost-guard", 1000, "target", 10, 30, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tbl.tableId = tc.id
			for _, keyCount := range []int{1, 9} {
				t.Run(fmt.Sprintf("keys=%d", keyCount), func(t *testing.T) {
					key := vector.NewVec(pkType)
					defer key.Free(mp)
					for i := 1; i < keyCount; i++ {
						require.NoError(t, vector.AppendBytes(key, []byte(fmt.Sprintf("missing-%d", i)), false, mp))
					}
					require.NoError(t, vector.AppendBytes(key, []byte(tc.key), false, mp))
					baseline := mp.CurrNB()
					changed, err := tbl.PKPersistedBetween(ctx, state, types.BuildTS(tc.from, 0), types.BuildTS(tc.to, 0), key, true)
					require.NoError(t, err)
					require.Equal(t, tc.changed, changed)
					require.Equal(t, baseline, mp.CurrNB())
					require.Empty(t, pkCheckSemaphore, "the check must release its I/O permit")
				})
			}
		})
	}
	// Cancellation must not turn into a successful negative check, or into an
	// apparent metadata conflict which can restart the expensive statement.
	t.Run("cancelled", func(t *testing.T) {
		cancelCtx, cancel := context.WithCancel(ctx)
		cancel()
		tbl.tableId = catalog.MO_TABLES_ID
		key := vector.NewVec(pkType)
		defer key.Free(mp)
		require.NoError(t, vector.AppendBytes(key, []byte("target"), false, mp))
		_, err := tbl.PKPersistedBetween(cancelCtx, state, types.BuildTS(10, 0), types.BuildTS(30, 0), key, true)
		require.ErrorIs(t, err, context.Canceled)
		require.Empty(t, pkCheckSemaphore)
	})
}

type tombstoneReadFS struct {
	fileservice.FileService
	read func(context.Context, *fileservice.IOVector) error
}

func (fs tombstoneReadFS) Read(ctx context.Context, v *fileservice.IOVector) error {
	return fs.read(ctx, v)
}

type tombstoneWaitContext struct {
	context.Context
	entered chan struct{}
	once    sync.Once
}

func (ctx *tombstoneWaitContext) Done() <-chan struct{} {
	ctx.once.Do(func() { close(ctx.entered) })
	return ctx.Context.Done()
}

func TestCatalogTombstoneCancellationAndFailure(t *testing.T) {
	proc, fs, state := newCatalogTombstoneFixture(t, 1, false)
	mp := proc.Mp()
	key := vector.NewVec(types.T_varchar.ToType())
	defer key.Free(mp)
	require.NoError(t, vector.AppendBytes(key, []byte("target"), false, mp))
	check := func(ctx context.Context, fs fileservice.FileService) (bool, string, error) {
		return tombstonePKExistsInRange(ctx, catalog.MO_TABLES_ID, state,
			types.BuildTS(10, 0), types.BuildTS(30, 0), key, *key.GetType(), fs, mp)
	}
	t.Run("read-error-remains-conservative", func(t *testing.T) {
		changed, reason, err := check(proc.Ctx, tombstoneReadFS{FileService: fs,
			read: func(context.Context, *fileservice.IOVector) error { return errors.New("injected read failure") },
		})
		require.NoError(t, err)
		require.True(t, changed)
		require.Equal(t, "tombstone_read_error", reason)
		require.Empty(t, pkCheckSemaphore)
	})
	for _, phase := range []string{"permit-wait", "read"} {
		t.Run(phase, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(proc.Ctx, 10*time.Second)
			defer cancel()
			entered := make(chan struct{})
			var checkCtx context.Context = ctx
			var checkFS fileservice.FileService = fs
			if phase == "permit-wait" {
				for i := 0; i < cap(pkCheckSemaphore); i++ {
					pkCheckSemaphore <- struct{}{}
				}
				defer func() {
					for i := 0; i < cap(pkCheckSemaphore); i++ {
						releasePKCheckSemaphore()
					}
				}()
				checkCtx = &tombstoneWaitContext{Context: ctx, entered: entered}
			} else {
				var once sync.Once
				checkFS = tombstoneReadFS{FileService: fs, read: func(ctx context.Context, _ *fileservice.IOVector) error {
					once.Do(func() { close(entered) })
					<-ctx.Done()
					return ctx.Err()
				}}
			}
			done := make(chan error, 1)
			finished := make(chan struct{})
			go func() {
				defer close(finished)
				_, _, err := check(checkCtx, checkFS)
				done <- err
			}()
			defer func() { cancel(); <-finished }()
			select {
			case <-entered:
			case <-ctx.Done():
				t.Fatal("check did not reach the controlled wait", ctx.Err())
			}
			cancel()
			require.ErrorIs(t, <-done, context.Canceled)
			if phase == "read" {
				require.Empty(t, pkCheckSemaphore)
			}
		})
	}
	changed, _, err := check(proc.Ctx, fs)
	require.NoError(t, err)
	require.False(t, changed, "cancellation must not poison subsequent checks")
	require.Empty(t, pkCheckSemaphore)
}

func TestCatalogCNTombstonePKCheck(t *testing.T) {
	proc, fs, state := newCatalogTombstoneFixture(t, 50001, true)
	key := vector.NewVec(types.T_varchar.ToType())
	defer key.Free(proc.Mp())
	for _, value := range []string{"target", "other-00001"} {
		key.CleanOnlyData()
		require.NoError(t, vector.AppendBytes(key, []byte(value), false, proc.Mp()))
		changed, _, err := tombstonePKExistsInRange(proc.Ctx, catalog.MO_TABLES_ID, state,
			types.BuildTS(10, 0), types.BuildTS(30, 0), key, *key.GetType(), fs, proc.Mp())
		require.NoError(t, err)
		require.Equal(t, value != "target", changed)
	}
	require.Empty(t, pkCheckSemaphore)
}

func BenchmarkCatalogTombstoneMultiKeyCheck(b *testing.B) {
	proc, fs, state := newCatalogTombstoneFixture(b, 50001, false)
	for _, count := range []int{1, 64, 1024, 4096} {
		b.Run(fmt.Sprintf("keys=%d", count), func(b *testing.B) {
			keys := vector.NewVec(types.T_varchar.ToType())
			defer keys.Free(proc.Mp())
			for key := 0; key < count; key++ {
				if err := vector.AppendBytes(keys, []byte(fmt.Sprintf("probe-%05d", key)), false, proc.Mp()); err != nil {
					b.Fatal(err)
				}
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				changed, _, err := tombstonePKExistsInRange(proc.Ctx, catalog.MO_TABLES_ID, state,
					types.BuildTS(10, 0), types.BuildTS(30, 0), keys, *keys.GetType(), fs, proc.Mp())
				if err != nil || changed {
					b.Fatalf("changed=%v err=%v", changed, err)
				}
			}
		})
	}
}

func BenchmarkCatalogTombstonePKCheck(b *testing.B) {
	for _, rows := range []int{1, 50000, 50001, 100000} {
		b.Run(fmt.Sprintf("rows=%d", rows), func(b *testing.B) {
			proc, fs, state := newCatalogTombstoneFixture(b, rows, false)
			key := vector.NewVec(types.T_varchar.ToType())
			defer key.Free(proc.Mp())
			require.NoError(b, vector.AppendBytes(key, []byte("target"), false, proc.Mp()))
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				changed, _, err := tombstonePKExistsInRange(proc.Ctx, catalog.MO_TABLES_ID, state,
					types.BuildTS(10, 0), types.BuildTS(30, 0), key, *key.GetType(), fs, proc.Mp())
				if err != nil || changed {
					b.Fatalf("changed=%v err=%v", changed, err)
				}
			}
		})
	}
}

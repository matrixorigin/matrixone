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

package disttae

import (
	"context"
	"errors"
	"testing"
	"time"

	rt "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/objectio/ioutil"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae/logtailreplay"
	"go.uber.org/zap"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/stretchr/testify/require"
)

// Record actual prefetch completions; metadata reads deliberately fail before
// row relocation, so this control needs no real object files.
type transferPrefetchFS struct {
	fileservice.FileService
	calls   chan string
	readErr error
}

func (fs *transferPrefetchFS) PrefetchFile(_ context.Context, name string) error {
	fs.calls <- name
	return nil
}
func (fs *transferPrefetchFS) Read(context.Context, *fileservice.IOVector) error { return fs.readErr }

func TestTransferTargetsRequireWorkspaceIntent(t *testing.T) {
	ctx := context.Background()
	proc := testutil.NewProc(t)
	state := logtailreplay.NewPartitionState("", false, 0x3fff, false)
	makeObject := func(created, deleted int64) objectio.ObjectEntry {
		id := types.NewObjectid()
		stats := objectio.NewObjectStatsWithObjectID(&id, false, true, false)
		require.NoError(t, objectio.SetObjectStatsSize(stats, 1024))
		require.NoError(t, objectio.SetObjectStatsRowCnt(stats, 1))
		require.NoError(t, objectio.SetObjectStatsBlkCnt(stats, 1))
		entry := objectio.ObjectEntry{ObjectStats: *stats, CreateTime: types.BuildTS(created, 0)}
		if deleted != 0 {
			entry.DeleteTime = types.BuildTS(deleted, 0)
		}
		require.NoError(t, state.HandleObjectEntry(ctx, nil, entry, false))
		return entry
	}
	stable := makeObject(5, 0)
	var source objectio.ObjectEntry
	for range 10 {
		source = makeObject(11, 15)
		makeObject(16, 0)
	}
	deleted, created := state.GetChangedObjsBetweenForTombstoneTransfer(types.BuildTS(10, 0), types.BuildTS(20, 0))
	require.Len(t, deleted, 10)
	require.Len(t, created, 10)
	require.NotContains(t, deleted, *stable.ObjectShortName())
	for _, name := range []string{"unrelated merges", "no target lookup", "matching intent"} {
		t.Run(name, func(t *testing.T) {
			// Isolate the existing pipeline runtime; any no-demand prefetch fails
			// synchronously at its lookup instead of racing an async zero-call check.
			previous := rt.ServiceRuntime("")
			runtime := rt.NewRuntime(metadata.ServiceType_CN, "", zap.NewNop())
			rt.SetupServiceBasedRuntime("", runtime)
			t.Cleanup(func() { rt.SetupServiceBasedRuntime("", previous) })
			runtime.SetGlobalVariables("blockio", "unexpected prefetch")
			fs := &transferPrefetchFS{FileService: proc.GetFileService(), calls: make(chan string, 10), readErr: errors.New("metadata read failed")}
			bat := batch.NewWithSize(2)
			t.Cleanup(func() { bat.Clean(proc.Mp()) })
			bat.Vecs[0] = vector.NewVec(types.T_Rowid.ToType())
			bat.Vecs[1] = vector.NewVec(types.T_int32.ToType())
			obj := stable
			if name == "matching intent" {
				obj = source
			}
			block := objectio.NewBlockidWithObjectID(obj.ObjectName().ObjectId(), 0)
			rowID := types.NewRowid(&block, 0)
			require.NoError(t, vector.AppendFixed(bat.Vecs[0], rowID, false, proc.Mp()))
			require.NoError(t, vector.AppendFixed(bat.Vecs[1], int32(7), false, proc.Mp()))
			bat.SetRowCount(1)
			txn := &Transaction{writes: []Entry{{typ: DELETE, tableId: 42, bat: bat}}}
			op := newTxnOperatorForTestWithWorkspace(t, txn)
			tbl := &txnTable{tableId: 42, db: &txnDatabase{op: op}, tableDef: &plan.TableDef{
				Name: "prefetch_intent", Cols: []*plan.ColDef{{Name: "pk", Typ: plan.Type{Id: int32(types.T_int32)}}},
				Pkey: &plan.PrimaryKeyDef{PkeyColName: "pk"}, Name2ColIndex: map[string]int32{"pk": 0},
			}}
			tbl.proc.Store(proc)
			partition := state
			if name == "no target lookup" {
				partition = nil
			}
			before := proc.Mp().CurrNB()
			if name == "matching intent" {
				require.True(t, runtime.CompareAndDeleteGlobalVariables("blockio", "unexpected prefetch"))
				ioutil.Start("")
				t.Cleanup(func() { ioutil.Stop("") })
				op.EXPECT().SnapshotTS().Return(timestamp.Timestamp{}).AnyTimes()
				require.ErrorIs(t, transferTombstones(ctx, tbl, partition, deleted, created, proc.Mp(), fs), fs.readErr)
				deadline := time.NewTimer(5 * time.Second)
				defer deadline.Stop()
				seen := make(map[string]struct{})
				for range 10 {
					select {
					case file := <-fs.calls:
						seen[file] = struct{}{}
					case <-deadline.C:
						t.Fatal("prefetch callbacks did not complete")
					}
				}
				require.Len(t, seen, 10)
			} else {
				require.NoError(t, transferTombstones(ctx, tbl, partition, deleted, created, proc.Mp(), fs))
				require.Empty(t, fs.calls)
			}
			require.Equal(t, before, proc.Mp().CurrNB(), "transfer scratch must be freed on success and read failure")
			require.Equal(t, rowID, vector.MustFixedColWithTypeCheck[types.Rowid](bat.Vecs[0])[0])
		})
	}
}

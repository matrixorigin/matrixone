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

package multi_update

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/objectio/ioutil"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/util/fault"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

type ownershipFailFS struct{ fileservice.FileService }

func (f *ownershipFailFS) Write(context.Context, fileservice.IOVector) error {
	return errors.New("injected object write failure")
}

type ownershipTrackingBuffer struct {
	*containers.BatchFreeList
	onClose func()
}

func (b *ownershipTrackingBuffer) Close(mp *mpool.MPool) {
	b.onClose()
	b.BatchFreeList.Close(mp)
}

func TestSortAndSyncDropsTransferredBatchOnWriteError(t *testing.T) {
	ctx := context.Background()
	enabledHere := fault.Enable()
	t.Cleanup(func() {
		_, _ = fault.RemoveFaultPoint(ctx, objectio.FJ_CNFlushSmallObjs)
		if enabledHere {
			fault.Disable()
		}
	})
	require.NoError(t, fault.AddFaultPoint(ctx, objectio.FJ_CNFlushSmallObjs, ":::", "ECHO", 0, "ownership_db", false))
	fs, err := fileservice.NewMemoryFS(defines.SharedFileServiceName, fileservice.DisabledCacheConfig, nil)
	require.NoError(t, err)
	t.Cleanup(func() { fs.Close(ctx) })
	proc := testutil.NewProcess(t, testutil.WithFileService(&ownershipFailFS{fs}))
	columnType := types.New(types.T_varchar, 256, 0)
	input := batch.NewOffHeap([]string{"v"})
	input.Vecs[0] = vector.NewOffHeapVecWithType(columnType)
	for range objectio.BlockMaxRows {
		require.NoError(t, vector.AppendBytes(input.Vecs[0], []byte(strings.Repeat("x", 128)), false, proc.Mp()))
	}
	input.SetRowCount(int(objectio.BlockMaxRows))
	bats := []*batch.Batch{input}
	seenClose, retained := false, false
	buf := &ownershipTrackingBuffer{BatchFreeList: containers.NewBatchFreeList(nil, nil, true)}
	buf.onClose = func() { seenClose = true; retained = bats[0] != nil }
	table := &planpb.TableDef{
		DbName: "ownership_db", Name: "ownership_table",
		Pkey: &planpb.PrimaryKeyDef{PkeyColName: "absent"},
		Cols: []*planpb.ColDef{{Name: "v", Typ: planpb.Type{Id: int32(types.T_varchar), Width: 256}}},
	}
	writer := &s3WriterDelegate{}
	err = writer.sortAndSyncOneTable(proc, table, process.NewTempAnalyzer(), 0, false, bats, true, ioutil.WithBuffer(buf, true))
	require.ErrorContains(t, err, "injected object write failure")
	require.True(t, seenClose, "the sinker must have closed after the failed write")
	require.False(t, retained, "the caller must drop a batch as soon as the sinker takes ownership")
}

func TestSortAndSyncRetainsUntransferredBatchOnWriteError(t *testing.T) {
	ctx := context.Background()
	fs, err := fileservice.NewMemoryFS(defines.SharedFileServiceName, fileservice.DisabledCacheConfig, nil)
	require.NoError(t, err)
	t.Cleanup(func() { fs.Close(ctx) })
	proc := testutil.NewProcess(t, testutil.WithFileService(fs))
	input := batch.NewOffHeap([]string{"v"})
	input.Vecs[0] = nil // Validation fails before ownership is transferred.
	input.SetRowCount(int(objectio.BlockMaxRows))
	bats := []*batch.Batch{input}
	seenClose, retained := false, false
	buf := &ownershipTrackingBuffer{BatchFreeList: containers.NewBatchFreeList(nil, nil, true)}
	buf.onClose = func() { seenClose = true; retained = bats[0] != nil }
	table := &planpb.TableDef{
		DbName: "ownership_db", Name: "ownership_table",
		Pkey: &planpb.PrimaryKeyDef{PkeyColName: "absent"},
		Cols: []*planpb.ColDef{{Name: "v", Typ: planpb.Type{Id: int32(types.T_varchar), Width: 256}}},
	}
	writer := &s3WriterDelegate{}
	err = writer.sortAndSyncOneTable(proc, table, process.NewTempAnalyzer(), 0, false, bats, true, ioutil.WithBuffer(buf, true))
	require.ErrorContains(t, err, "nil vector")
	require.True(t, seenClose)
	require.True(t, retained, "the caller retains a batch rejected before transfer")
	require.Nil(t, bats[0], "the caller's deferred cleanup releases the batch")
}

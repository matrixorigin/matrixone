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
	"fmt"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/objectio/ioutil"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
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
		// One 24-byte descriptor plus 40 payload bytes per row fills the
		// injected 512 KiB limit exactly, so ownership transfers before spill.
		require.NoError(t, vector.AppendBytes(input.Vecs[0], []byte(strings.Repeat("x", 40)), false, proc.Mp()))
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

type resetFailureFS struct {
	fileservice.FileService
	failWrite  bool
	failDelete bool
}

func (f *resetFailureFS) Write(ctx context.Context, data fileservice.IOVector) error {
	if err := f.FileService.Write(ctx, data); err != nil {
		return err
	}
	if f.failWrite {
		return errors.New("post-commit failure")
	}
	return nil
}
func (f *resetFailureFS) Delete(ctx context.Context, names ...string) error {
	if f.failDelete {
		return errors.New("delete failed")
	}
	return f.FileService.Delete(ctx, names...)
}

func TestPersistentSinkerResetRetriesOrReconstructs(t *testing.T) {
	for _, retryFails := range []bool{false, true} {
		t.Run(fmt.Sprintf("retryFails=%v", retryFails), func(t *testing.T) {
			ctx := context.Background()
			base, err := fileservice.NewMemoryFS(defines.SharedFileServiceName, fileservice.DisabledCacheConfig, nil)
			require.NoError(t, err)
			t.Cleanup(func() { base.Close(ctx) })
			fs := &resetFailureFS{FileService: base, failWrite: true, failDelete: true}
			proc := testutil.NewProcess(t, testutil.WithFileService(fs))
			table := &planpb.TableDef{
				Name: "reset_test", Pkey: &planpb.PrimaryKeyDef{PkeyColName: "absent"},
				Cols: []*planpb.ColDef{{Name: "v", Typ: planpb.Type{Id: int32(types.T_varchar), Width: 256}}},
			}
			first := colexec.NewCNS3DataWriter(proc.Mp(), fs, table, 1, false)
			failed := colexec.NewCNS3DataWriter(proc.Mp(), fs, table, 1, false)
			writer := &s3WriterDelegate{
				insertSinkers: []*colexec.CNS3Writer{first, failed},
				insertFreeLists: []*containers.BatchFreeList{
					containers.NewBatchFreeList(nil, nil, true), containers.NewBatchFreeList(nil, nil, true),
				},
				updateCtxs: []*MultiUpdateCtx{
					{TableDef: table, InsertCols: []int{0}}, {TableDef: table, InsertCols: []int{0}},
				},
			}
			t.Cleanup(func() { _ = writer.free(proc) })
			input := batch.NewWithSchema(false, []string{"v"}, []types.Type{types.New(types.T_varchar, 256, 0)})
			defer input.Clean(proc.Mp())
			require.NoError(t, vector.AppendBytes(input.Vecs[0], []byte("value"), false, proc.Mp()))
			input.SetRowCount(1)
			require.ErrorContains(t, failed.Write(ctx, input), "post-commit failure")
			names := failed.PendingUnpublishedRuns()
			require.Len(t, names, 1)
			fs.failWrite = false
			fs.failDelete = retryFails
			require.NoError(t, writer.reset(proc))
			require.Same(t, first, writer.insertSinkers[0])
			if retryFails {
				require.Nil(t, writer.insertSinkers[1])
				_, err = base.StatFile(ctx, names[0])
				require.NoError(t, err, "failed deletion is an explicit terminal-drop limitation")
				fs.failDelete = false
				require.NoError(t, base.Delete(ctx, names...))
				require.NoError(t, writer.ensureInsertSinkers(proc))
				require.NotNil(t, writer.insertSinkers[1])
				require.NotSame(t, failed, writer.insertSinkers[1])
			} else {
				require.Same(t, failed, writer.insertSinkers[1])
				_, err = base.StatFile(ctx, names[0])
				require.True(t, moerr.IsMoErrCode(err, moerr.ErrFileNotFound))
			}
			require.NoError(t, writer.insertSinkers[1].Write(ctx, input))
			_, err = writer.insertSinkers[1].Sync(ctx)
			require.NoError(t, err)
		})
	}
}

func TestDeleteOutputRetainsAllObjectStats(t *testing.T) {
	proc := testutil.NewProcess(t)
	input := colexec.AllocCNS3ResultBat(true)
	defer input.Clean(proc.Mp())
	names := make([]string, 0, 3)
	for i := range 3 {
		var id objectio.ObjectId
		id[0] = byte(i + 1)
		stats := objectio.NewObjectStatsWithObjectID(&id, false, true, true)
		names = append(names, stats.ObjectName().String())
		require.NoError(t, vector.AppendBytes(input.Vecs[0], stats.Marshal(), false, proc.Mp()))
	}
	input.SetRowCount(len(names))
	writer := &s3WriterDelegate{
		deleteBlockInfo: make([]*deleteBlockInfo, 1),
		outputBat:       makeS3OutputBatch(),
		updateCtxs:      []*MultiUpdateCtx{{TableDef: &planpb.TableDef{TblId: 1}}},
	}
	defer writer.free(proc)
	require.NoError(t, writer.fillDeleteBlockInfo(proc, 0, input, 9))
	info := writer.deleteBlockInfo[0]
	require.Equal(t, 3, info.bat.RowCount())
	require.Equal(t, uint64(9), info.rawRowCount)
	require.NoError(t, writer.addBatchToOutput(proc.Mp(), actionDelete, 0, info.rawRowCount, info.name, info.bat))
	decoded := batch.NewOffHeapEmpty()
	defer decoded.Clean(proc.Mp())
	require.NoError(t, decoded.UnmarshalBinaryWithAnyMp(writer.outputBat.Vecs[4].GetBytesAt(0), proc.Mp()))
	require.Equal(t, 3, decoded.RowCount())
	var got []string
	for row := 0; row < decoded.RowCount(); row++ {
		var stats objectio.ObjectStats
		stats.UnMarshal(decoded.Vecs[0].GetBytesAt(row))
		got = append(got, stats.ObjectName().String())
	}
	require.ElementsMatch(t, names, got)
}

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
	"sync/atomic"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/common/stopper"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/lockservice"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/objectio/ioutil"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
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
	// Descriptor plus payload is exactly the injected 512 KiB threshold, so
	// WriteOwned transfers the full batch and its spill fails synchronously.
	for range objectio.BlockMaxRows {
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
	input := batch.NewOffHeap([]string{"v"})
	input.Vecs[0] = vector.NewOffHeapVecWithType(types.New(types.T_varchar, 256, 0))
	// A partial block uses the copying path; its spill still reaches the
	// injected write failure before the caller can release the input.
	for range 4097 {
		require.NoError(t, vector.AppendBytes(input.Vecs[0], []byte(strings.Repeat("x", 128)), false, proc.Mp()))
	}
	input.SetRowCount(4097)
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
	require.True(t, seenClose)
	require.True(t, retained, "the caller retains a batch rejected before transfer")
	require.Nil(t, bats[0], "the caller's deferred cleanup releases the batch")
}

type cleanupTestLockService struct {
	lockservice.LockService
	id string
}

func (s cleanupTestLockService) GetConfig() lockservice.Config {
	return lockservice.Config{ServiceID: s.id}
}

type cleanupTestSharedFS struct {
	fileservice.FileService
	failDelete atomic.Bool
}

func (f *cleanupTestSharedFS) Delete(ctx context.Context, names ...string) error {
	if f.failDelete.Load() {
		return errors.New("injected shared storage outage")
	}
	return f.FileService.Delete(ctx, names...)
}

func TestMultiUpdateFreeRetriesUnpublishedSpillAfterCNRestart(t *testing.T) {
	ctx := t.Context()
	id := t.Name()
	runtime.SetupServiceBasedRuntime(id, runtime.NewRuntime(metadata.ServiceType_CN, id, nil))
	fs := &cleanupTestSharedFS{FileService: testutil.NewSharedFS()}
	t.Cleanup(func() { fs.Close(context.Background()) })
	proc := testutil.NewProcess(t, testutil.WithFileService(fs))
	proc.Base.LockService = cleanupTestLockService{id: id}
	root := t.TempDir()

	startCN := func() (*stopper.Stopper, *fileservice.LocalFS) {
		local, err := fileservice.NewLocalFS(ctx, defines.LocalFileServiceName, root, fileservice.DisabledCacheConfig, nil)
		require.NoError(t, err)
		st := stopper.NewStopper(id)
		t.Cleanup(func() {
			st.Stop()
			local.Close(context.Background())
		})
		require.NoError(t, colexec.NewServer(id).StartCNS3Cleanup(st, id, local, fs))
		return st, local
	}
	st, oldLocal := startCN()
	listObjects := func() []string {
		files, err := fileservice.SortedList(fs.List(ctx, ""))
		require.NoError(t, err)
		names := make([]string, 0, len(files))
		for _, file := range files {
			names = append(names, file.Name)
		}
		return names
	}

	typ := types.New(types.T_varchar, 256, 0)
	table := &planpb.TableDef{
		Name: "cleanup_table",
		Pkey: &planpb.PrimaryKeyDef{PkeyColName: "absent"},
		Cols: []*planpb.ColDef{{Name: "v", Typ: planpb.Type{Id: int32(types.T_varchar), Width: 256}}},
	}
	s3Writer := colexec.NewCNS3DataWriter(proc.Mp(), fs, table, 64, false)
	input := batch.NewWithSchema(false, []string{"v"}, []types.Type{typ})
	t.Cleanup(func() { input.Clean(proc.Mp()) })
	require.NoError(t, vector.AppendBytes(input.Vecs[0], []byte(strings.Repeat("x", 80)), false, proc.Mp()))
	input.SetRowCount(1)
	require.NoError(t, s3Writer.Write(ctx, input))
	published, err := s3Writer.Sync(ctx)
	require.NoError(t, err)
	require.Len(t, published, 1)
	publishedName := published[0].ObjectName().String()
	s3Writer.Reset()
	require.NoError(t, s3Writer.Write(ctx, input))
	require.Len(t, listObjects(), 2, "one published object and one unpublished spill")

	fs.failDelete.Store(true)
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	proc.Ctx = canceled
	update := &MultiUpdate{ctr: container{s3Writer: &s3WriterDelegate{
		insertSinkers: []*colexec.CNS3Writer{s3Writer},
	}}}
	update.Free(proc, true, context.Canceled)
	require.Nil(t, update.ctr.s3Writer)
	st.Stop()
	require.Len(t, listObjects(), 2, "the old CN cannot delete during the outage")
	records, err := fileservice.SortedList(oldLocal.List(ctx, "cn-spill-cleanup/"+id))
	require.NoError(t, err)
	require.Len(t, records, 1, "terminal teardown must leave a retry record")

	fs.failDelete.Store(false)
	_, local := startCN()
	require.Eventually(t, func() bool {
		files, listErr := fileservice.SortedList(fs.List(ctx, ""))
		return listErr == nil && len(files) == 1 && files[0].Name == publishedName
	}, 5*time.Second, 10*time.Millisecond)
	require.Eventually(t, func() bool {
		records, listErr := fileservice.SortedList(local.List(ctx, "cn-spill-cleanup/"+id))
		return listErr == nil && len(records) == 0
	}, 5*time.Second, 10*time.Millisecond)
}

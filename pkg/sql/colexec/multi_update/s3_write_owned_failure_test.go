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
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/util/fault"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

type ownershipFailFS struct {
	fileservice.FileService
	allow  int
	writes []string
}

func (f *ownershipFailFS) Write(ctx context.Context, v fileservice.IOVector) error {
	if len(f.writes) < f.allow {
		if err := f.FileService.Write(ctx, v); err != nil {
			return err
		}
		f.writes = append(f.writes, v.FilePath)
		return nil
	}
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
	for _, prior := range []int{0, 1} {
		t.Run(fmt.Sprint(prior), func(t *testing.T) {
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
			failing := &ownershipFailFS{FileService: fs, allow: prior}
			proc := testutil.NewProcess(t, testutil.WithFileService(failing))
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
			if prior > 0 {
				copied, err := input.Dup(proc.Mp())
				require.NoError(t, err)
				bats = append(bats, copied)
			}
			seenClose, retained := false, false
			buf := &ownershipTrackingBuffer{BatchFreeList: containers.NewBatchFreeList(nil, nil, true)}
			buf.onClose = func() { seenClose = true; retained = bats[0] != nil }
			table := &planpb.TableDef{
				DbName: "ownership_db", Name: "ownership_table",
				Pkey: &planpb.PrimaryKeyDef{PkeyColName: "absent"},
				Cols: []*planpb.ColDef{{Name: "v", Typ: planpb.Type{Id: int32(types.T_varchar), Width: 256}}},
			}
			writer := &s3WriterDelegate{cleanupUnpublished: prior > 0}
			err = writer.sortAndSyncOneTable(proc, table, process.NewTempAnalyzer(), 0, false, bats, true, ioutil.WithBuffer(buf, true))
			require.ErrorContains(t, err, "injected object write failure")
			require.True(t, seenClose, "the sinker must have closed after the failed write")
			require.False(t, retained, "the caller must drop a batch as soon as the sinker takes ownership")
			require.Len(t, failing.writes, prior)
			for _, path := range failing.writes {
				_, err := fs.StatFile(ctx, path)
				require.True(t, moerr.IsMoErrCode(err, moerr.ErrFileNotFound), "a failed later upload must not orphan its successful prefix")
			}
		})
	}
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
	proc := testutil.NewProcess(t, testutil.WithFileService(&ownershipFailFS{FileService: fs}))
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

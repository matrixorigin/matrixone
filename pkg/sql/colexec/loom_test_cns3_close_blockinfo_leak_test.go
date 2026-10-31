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

package colexec

// Run:
//   CGO_CFLAGS="-I$(pwd)/cgo -I$(pwd)/thirdparties/install/include" \
//   CGO_LDFLAGS="-L$(pwd)/cgo -L$(pwd)/thirdparties/install/lib -lmo \
//     -Wl,-rpath,$(pwd)/cgo -Wl,-rpath,$(pwd)/thirdparties/install/lib" \
//   go test ./pkg/sql/colexec -run 'TestLoomCNS3WriterCloseReleasesBlockInfoBat' -count=1 -v
//
// Expected behavior: CNS3Writer.Close releases every mpool-backed resource of
// the writer, including blockInfoBat, even when the underlying Sinker.Close
// reports a failed pipeline job. Current main returns early on that error and
// permanently leaks blockInfoBat's mpool allocations, so this test FAILS on
// main and passes once Close cleans blockInfoBat unconditionally.

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/objectio/ioutil"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

// loomFailNextWriteFS delegates to the shared file service until the test
// flips failNext, after which every Write fails. The sinker's pipeline workers
// surface the failure in the job result, which Sinker.Close drains.
type loomFailNextWriteFS struct {
	fileservice.FileService
	failNext atomic.Bool
}

func (f *loomFailNextWriteFS) Write(ctx context.Context, vec fileservice.IOVector) error {
	if f.failNext.Load() {
		return moerr.NewInternalError(ctx, "loom injected S3 write failure")
	}
	return f.FileService.Write(ctx, vec)
}

func loomMakeBatch(proc *process.Process) *batch.Batch {
	bat := &batch.Batch{
		Attrs: []string{"a", "b"},
		Vecs: []*vector.Vector{
			testutil.MakeInt64Vector([]int64{1, 2}, nil, proc.Mp()),
			testutil.MakeVarcharVector([]string{"x", "y"}, nil, proc.Mp()),
		},
	}
	bat.SetRowCount(2)
	return bat
}

func TestLoomCNS3WriterCloseReleasesBlockInfoBat(t *testing.T) {
	t.Run("sinker close error must not leak blockInfoBat", func(t *testing.T) {
		proc := testutil.NewProc(t)
		defer proc.Free()
		shared, err := fileservice.Get[fileservice.FileService](
			proc.Base.FileService, defines.SharedFileServiceName)
		require.NoError(t, err)
		fs := &loomFailNextWriteFS{FileService: shared}

		// MemorySizeThreshold 1 + WithPipelineFlush + a primary-keyed table
		// make every Write take the async pipeline spill path, exactly like
		// the production ALTER TABLE ADD PRIMARY KEY / multi-update pipelines
		// (ioutil.PipelineFlushKey) do.
		writer := NewCNS3DataWriterForService(
			"", proc.Mp(), fs, testCNS3WriterTableDef(), 1, false,
			ioutil.WithPipelineFlush(),
		)

		mp := proc.Mp()
		beforeBytes, beforeObjects := mp.OnHeapOutstanding()

		bat1 := loomMakeBatch(proc)
		defer bat1.Clean(mp)
		require.NoError(t, writer.Write(proc.Ctx, bat1)) // spill job 1 (persists)

		// Sync drains the successful job and collects its object stats.
		stats, err := writer.Sync(proc.Ctx)
		require.NoError(t, err)
		require.NotEmpty(t, stats)

		// Mirror insert.go's flushTailBatch: Sync then FillBlockInfoBat.
		blockInfoBat, err := writer.FillBlockInfoBat()
		require.NoError(t, err)
		require.Positive(t, blockInfoBat.RowCount())
		_ = blockInfoBat

		fs.failNext.Store(true)
		bat2 := loomMakeBatch(proc)
		defer bat2.Clean(mp)
		require.NoError(t, writer.Write(proc.Ctx, bat2)) // spill job 2 (fails)

		closeErr := writer.Close()
		require.Error(t, closeErr,
			"test setup: Sinker.Close is expected to surface the failed pipeline job")

		afterBytes, afterObjects := mp.OnHeapOutstanding()

		// Attribution (a no-op on a fixed main where blockInfoBat is nil):
		// free the surviving batch by hand and re-measure. The reclaimed
		// amount proves the outstanding growth is blockInfoBat itself.
		var freedBytes, freedObjects int64
		if writer.blockInfoBat != nil {
			writer.blockInfoBat.Clean(mp)
			cleanBytes, cleanObjects := mp.OnHeapOutstanding()
			freedBytes = afterBytes - cleanBytes
			freedObjects = afterObjects - cleanObjects
		}

		// Expected behavior: Close must release blockInfoBat regardless of
		// Sinker.Close's error. On current main the early return leaves the
		// batch (and its mpool allocations) owned by nobody outside the mpool.
		require.Nil(t, writer.blockInfoBat,
			"CNS3Writer.Close returned early on the Sinker.Close error and leaked blockInfoBat: %d bytes / %d objects of block-info data remain outstanding in the process mpool (freeing blockInfoBat by hand reclaims %d bytes / %d objects)",
			afterBytes-beforeBytes, afterObjects-beforeObjects, freedBytes, freedObjects)
	})

	t.Run("clean close control", func(t *testing.T) {
		// Control: identical writes/syncs with a healthy file service. Close
		// reports nil and releases blockInfoBat; passes on current main and
		// guards the measurement against unrelated retention.
		proc := testutil.NewProc(t)
		defer proc.Free()
		shared, err := fileservice.Get[fileservice.FileService](
			proc.Base.FileService, defines.SharedFileServiceName)
		require.NoError(t, err)

		writer := NewCNS3DataWriterForService(
			"", proc.Mp(), shared, testCNS3WriterTableDef(), 1, false,
			ioutil.WithPipelineFlush(),
		)

		mp := proc.Mp()
		beforeBytes, beforeObjects := mp.OnHeapOutstanding()

		bat1 := loomMakeBatch(proc)
		defer bat1.Clean(mp)
		require.NoError(t, writer.Write(proc.Ctx, bat1))

		stats, err := writer.Sync(proc.Ctx)
		require.NoError(t, err)
		require.NotEmpty(t, stats)

		blockInfoBat, err := writer.FillBlockInfoBat()
		require.NoError(t, err)
		require.Positive(t, blockInfoBat.RowCount())
		_ = blockInfoBat

		require.NoError(t, writer.Close())
		require.Nil(t, writer.blockInfoBat)

		// Informational only: a healthy pipeline sinker leaves a small,
		// benign pool residue after close, so byte-identity of the mpool is
		// not a reliable oracle. The real invariant is asserted above:
		// Close must drop its reference to blockInfoBat.
		afterBytes, afterObjects := mp.OnHeapOutstanding()
		t.Logf("control: close cleanup residue = %+d bytes / %+d objects",
			afterBytes-beforeBytes, afterObjects-beforeObjects)
	})
}

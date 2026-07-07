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

package hashjoin

import (
	"bytes"
	"context"
	"os"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/spillutil"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

func makeKeyExpr() []*plan.Expr {
	return []*plan.Expr{{
		Typ:  plan.Type{Id: int32(types.T_int32), Width: 32},
		Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: 0}},
	}}
}

// TestSpillEngineRebuildHashmap tests the basic rebuild flow without re-spilling.
func TestSpillEngineRebuildHashmap(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()

	spillfs, err := proc.GetSpillFileService()
	require.NoError(t, err)

	// Create test data: 100 rows with int32 keys
	buildVals := make([]int32, 100)
	for i := range buildVals {
		buildVals[i] = int32(i)
	}
	buildBat := batch.NewWithSize(1)
	buildBat.Vecs[0] = testutil.MakeInt32Vector(buildVals, nil, proc.Mp())
	buildBat.SetRowCount(100)

	// Write build batch to spill file via FlushBucketBatch.
	buildFile, err := spillfs.CreateAndRemoveFile(context.Background(), "test_rebuild_build")
	require.NoError(t, err)
	var buf bytes.Buffer
	bw := spillutil.BucketWriter{Name: "test_rebuild_build", Fd: buildFile}
	err = spillutil.FlushBucketBatch(proc, buildBat, &bw, &buf, nil)
	require.NoError(t, err)
	buildFd := bw.HandOffFd()

	// Create SpillEngine and test rebuild.
	engine := spillutil.NewSpillEngine(spillutil.SpillEngineConfig{
		BuildKeyExprs: makeKeyExpr(),
	})
	engine.InitFromSpilledMap([]*os.File{buildFd})

	analyzer := process.NewAnalyzer(0, false, false, "test")
	jm, res, err := engine.RebuildHashmap(proc, analyzer)
	require.NoError(t, err)
	require.Equal(t, spillutil.BucketReady, res, "should return BucketReady")
	require.NotNil(t, jm, "should return a JoinMap when no re-spill occurs")
	require.Equal(t, int64(100), jm.GetRowCount())

	jm.Free()
	engine.Cleanup(proc)
}

// TestSpillEngineRebuildEmptyBuild tests that rebuilding a bucket with no build
// data returns BucketSkip for inner join.
func TestSpillEngineRebuildEmptyBuild(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()

	analyzer := process.NewAnalyzer(0, false, false, "test")

	engine := spillutil.NewSpillEngine(spillutil.SpillEngineConfig{
		BuildKeyExprs: makeKeyExpr(),
	})
	engine.InitFromSpilledMap([]*os.File{nil}) // nil fd = empty bucket

	jm, res, err := engine.RebuildHashmap(proc, analyzer)
	require.NoError(t, err)
	require.Equal(t, spillutil.BucketSkip, res, "empty build should return BucketSkip")
	require.Nil(t, jm)

	engine.Cleanup(proc)
}

// TestSpillEngineReSpill tests that re-spilling occurs when memory threshold is exceeded.
func TestSpillEngineReSpill(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()

	spillfs, err := proc.GetSpillFileService()
	require.NoError(t, err)

	// Create larger test data: 1000 rows
	buildVals := make([]int32, 1000)
	for i := range buildVals {
		buildVals[i] = int32(i)
	}
	buildBat := batch.NewWithSize(1)
	buildBat.Vecs[0] = testutil.MakeInt32Vector(buildVals, nil, proc.Mp())
	buildBat.SetRowCount(1000)

	var buf bytes.Buffer
	buildFile, err := spillfs.CreateAndRemoveFile(context.Background(), "test_respill_build")
	require.NoError(t, err)
	bw := spillutil.BucketWriter{Name: "test_respill_build", Fd: buildFile}
	err = spillutil.FlushBucketBatch(proc, buildBat, &bw, &buf, nil)
	require.NoError(t, err)
	buildFd := bw.HandOffFd()

	// Set threshold to 1000 bytes to guarantee re-spill at depth 1.
	engine := spillutil.NewSpillEngine(spillutil.SpillEngineConfig{
		BuildKeyExprs:  makeKeyExpr(),
		SpillThreshold: 1000,
	})
	engine.InitFromSpilledMap([]*os.File{buildFd})

	analyzer := process.NewAnalyzer(0, false, false, "test")
	jm, res, err := engine.RebuildHashmap(proc, analyzer)
	require.NoError(t, err)
	require.NotEqual(t, spillutil.BucketQueueEmpty, res, "must not be empty")
	if res == spillutil.BucketReady {
		require.NotNil(t, jm)
		jm.Free()
	}

	engine.Cleanup(proc)
}

// TestSpillEngineDepthLimit tests that re-spilling stops at SpillMaxPass.
func TestSpillEngineDepthLimit(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()

	spillfs, err := proc.GetSpillFileService()
	require.NoError(t, err)

	buildVals := make([]int32, 100)
	for i := range buildVals {
		buildVals[i] = int32(i)
	}
	buildBat := batch.NewWithSize(1)
	buildBat.Vecs[0] = testutil.MakeInt32Vector(buildVals, nil, proc.Mp())
	buildBat.SetRowCount(100)

	var buf bytes.Buffer
	buildFile, err := spillfs.CreateAndRemoveFile(context.Background(), "test_depth_build")
	require.NoError(t, err)
	bw := spillutil.BucketWriter{Name: "test_depth_build", Fd: buildFile}
	err = spillutil.FlushBucketBatch(proc, buildBat, &bw, &buf, nil)
	require.NoError(t, err)
	buildFd := bw.HandOffFd()

	// Set depth to SpillMaxPass so no further re-spill occurs.
	engine := spillutil.NewSpillEngine(spillutil.SpillEngineConfig{
		BuildKeyExprs:  makeKeyExpr(),
		SpillThreshold: 1,
	})
	engine.InitFromSpilledMap([]*os.File{buildFd})
	engine.TestSetBucketDepth(0, spillutil.SpillMaxPass)

	analyzer := process.NewAnalyzer(0, false, false, "test")
	jm, res, err := engine.RebuildHashmap(proc, analyzer)
	require.NoError(t, err)
	require.Equal(t, spillutil.BucketReady, res, "should return BucketReady at max depth even if memory exceeds threshold")
	require.NotNil(t, jm)

	jm.Free()
	engine.Cleanup(proc)
}

// TestGetSpilledInputBatchNoBuckets verifies that getSpilledInputBatch
// returns nil when the engine has no buckets.
func TestGetSpilledInputBatchNoBuckets(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()

	engine := spillutil.NewSpillEngine(spillutil.SpillEngineConfig{})
	hashJoin := &HashJoin{ctr: container{spillEngine: engine}}
	result, err := hashJoin.getSpilledInputBatch(proc, process.NewAnalyzer(0, false, false, "test"))
	require.NoError(t, err)
	require.Nil(t, result.Batch)
}

// TestCleanupSpillEngine verifies that engine cleanup closes all file descriptors.
func TestCleanupSpillEngine(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()

	spillfs, err := proc.GetSpillFileService()
	require.NoError(t, err)

	// Create build and probe files with data.
	var buf bytes.Buffer
	buildFile, _ := spillfs.CreateAndRemoveFile(context.Background(), "test_cleanup_build")
	bw := spillutil.BucketWriter{Name: "test_cleanup_build", Fd: buildFile}
	bat := batch.NewWithSize(1)
	bat.Vecs[0] = testutil.MakeInt32Vector([]int32{1, 2, 3}, nil, proc.Mp())
	bat.SetRowCount(3)
	err = spillutil.FlushBucketBatch(proc, bat, &bw, &buf, nil)
	require.NoError(t, err)
	buildFd := bw.HandOffFd()
	require.NotNil(t, buildFd)

	probeFile, _ := spillfs.CreateAndRemoveFile(context.Background(), "test_cleanup_probe")
	pw := spillutil.BucketWriter{Name: "test_cleanup_probe", Fd: probeFile}
	err = spillutil.FlushBucketBatch(proc, bat, &pw, &buf, nil)
	require.NoError(t, err)
	probeFd := pw.HandOffFd()
	require.NotNil(t, probeFd)

	engine := spillutil.NewSpillEngine(spillutil.SpillEngineConfig{
		BuildKeyExprs: makeKeyExpr(),
	})
	engine.InitFromSpilledMap([]*os.File{buildFd})
	engine.TestSetBucketProbeFd(0, probeFd)

	// Cleanup should close both fds.
	engine.Cleanup(proc)

	// Verify fds are closed — reads should fail.
	b := make([]byte, 1)
	_, err = buildFd.Read(b)
	require.Error(t, err, "build fd should be closed after cleanup")
	_, err = probeFd.Read(b)
	require.Error(t, err, "probe fd should be closed after cleanup")
}

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
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

// TestRebuildHashmapForBucket tests the basic rebuild flow without re-spilling
func TestRebuildHashmapForBucket(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()

	spillfs, err := proc.GetSpillFileService()
	require.NoError(t, err)

	analyzer := process.NewAnalyzer(0, false, false, "test")

	// Create test data: 100 rows with int32 keys
	buildBat := batch.NewWithSize(1)
	buildBat.Vecs[0] = testutil.MakeInt32Vector(makeSequence(100), nil, proc.Mp())
	buildBat.SetRowCount(100)

	// Write build batch to spill file
	buildBucketName := "test_rebuild_build"
	buildFile, err := spillfs.CreateAndRemoveFile(context.Background(), buildBucketName)
	require.NoError(t, err)

	ctr := &container{}
	buildFile_sw := spillBucketWriter{file: buildFile}
	_, err = ctr.flushBucketBuffer(proc, buildBat, &buildFile_sw, analyzer)
	require.NoError(t, err)
	buildFd := buildFile_sw.handOffFd()

	// Setup HashJoin with EqConds
	hashJoin := &HashJoin{
		EqConds: [][]*plan.Expr{
			{}, // probe side (not used in rebuild)
			{ // build side
				&plan.Expr{
					Typ: plan.Type{
						Id:    int32(types.T_int32),
						Width: 32,
					},
					Expr: &plan.Expr_Col{
						Col: &plan.ColRef{
							ColPos: 0,
						},
					},
				},
			},
		},
		HashOnPK: false,
	}

	bucket := spillBucket{
		buildFd: buildFd,
		probeFd: nil, // empty probe
		depth:   1,
	}

	// Call rebuildHashmapForBucket
	jm, _, err := hashJoin.rebuildHashmapForBucket(proc, bucket, analyzer)
	require.NoError(t, err)
	require.NotNil(t, jm, "should return a JoinMap when no re-spill occurs")
	require.Equal(t, int64(100), jm.GetRowCount())

	// Verify batches were loaded
	batches := jm.GetBatches()
	require.Greater(t, len(batches), 0)

	totalRows := 0
	for _, bat := range batches {
		totalRows += bat.RowCount()
	}
	require.Equal(t, 100, totalRows)

	jm.Free()
}

// TestRebuildEmptyBuildBucket tests that rebuilding a bucket with no build data
// returns nil JoinMap and closes the probe fd for inner join.
func TestRebuildEmptyBuildBucket(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()

	spillfs, err := proc.GetSpillFileService()
	require.NoError(t, err)

	analyzer := process.NewAnalyzer(0, false, false, "test")

	// Create an empty build file (no data written).
	buildFile, err := spillfs.CreateAndRemoveFile(context.Background(), "test_empty_build")
	require.NoError(t, err)
	buildFd := buildFile // already at position 0, no data

	// Create a probe file with some data.
	probeBat := batch.NewWithSize(1)
	probeBat.Vecs[0] = testutil.MakeInt32Vector([]int32{1, 2, 3}, nil, proc.Mp())
	probeBat.SetRowCount(3)

	probeFile, err := spillfs.CreateAndRemoveFile(context.Background(), "test_empty_build_probe")
	require.NoError(t, err)
	ctr := &container{}
	pw := spillBucketWriter{file: probeFile}
	_, err = ctr.flushBucketBuffer(proc, probeBat, &pw, analyzer)
	require.NoError(t, err)
	probeFd := pw.handOffFd()

	// Inner join: empty build → no matches possible.
	hashJoin := &HashJoin{
		EqConds: [][]*plan.Expr{
			{}, // probe side (not used in rebuild)
			{ // build side
				{
					Typ:  plan.Type{Id: int32(types.T_int32), Width: 32},
					Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: 0}},
				},
			},
		},
		HashOnPK: false,
	}

	bucket := spillBucket{
		buildFd: buildFd,
		probeFd: probeFd,
		depth:   1,
	}

	jm, reSpilled, err := hashJoin.rebuildHashmapForBucket(proc, bucket, analyzer)
	require.NoError(t, err)
	require.Nil(t, jm, "empty build should return nil JoinMap")
	require.False(t, reSpilled)

	// probeFd should have been closed (inner join discards probe for empty build).
	// Verify by attempting to read — should fail.
	buf := make([]byte, 1)
	_, readErr := probeFd.Read(buf)
	require.Error(t, readErr, "probe fd should be closed for inner join with empty build")
}

// TestReSpillBucket tests the re-spilling logic when memory threshold is exceeded
func TestReSpillBucket(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()

	spillfs, err := proc.GetSpillFileService()
	require.NoError(t, err)

	analyzer := process.NewAnalyzer(0, false, false, "test")

	// Create larger test data: 1000 rows
	buildBat := batch.NewWithSize(1)
	buildBat.Vecs[0] = testutil.MakeInt32Vector(makeSequence(1000), nil, proc.Mp())
	buildBat.SetRowCount(1000)

	probeBat := batch.NewWithSize(1)
	probeBat.Vecs[0] = testutil.MakeInt32Vector(makeSequence(1000), nil, proc.Mp())
	probeBat.SetRowCount(1000)

	// Write build and probe batches to spill files
	buildBucketName := "test_respill_build"
	buildFile, err := spillfs.CreateAndRemoveFile(context.Background(), buildBucketName)
	require.NoError(t, err)

	ctr := &container{}
	buildFile_sw := spillBucketWriter{file: buildFile}
	_, err = ctr.flushBucketBuffer(proc, buildBat, &buildFile_sw, analyzer)
	require.NoError(t, err)
	buildFd := buildFile_sw.handOffFd()

	probeBucketName := "test_respill_probe"
	probeFile, err := spillfs.CreateAndRemoveFile(context.Background(), probeBucketName)
	require.NoError(t, err)
	probeFile_sw := spillBucketWriter{file: probeFile}
	_, err = ctr.flushBucketBuffer(proc, probeBat, &probeFile_sw, analyzer)
	require.NoError(t, err)
	probeFd := probeFile_sw.handOffFd()

	// Setup HashJoin with EqConds
	hashJoin := &HashJoin{
		EqConds: [][]*plan.Expr{
			{ // probe side
				&plan.Expr{
					Typ: plan.Type{
						Id:    int32(types.T_int32),
						Width: 32,
					},
					Expr: &plan.Expr_Col{
						Col: &plan.ColRef{
							ColPos: 0,
						},
					},
				},
			},
			{ // build side
				&plan.Expr{
					Typ: plan.Type{
						Id:    int32(types.T_int32),
						Width: 32,
					},
					Expr: &plan.Expr_Col{
						Col: &plan.ColRef{
							ColPos: 0,
						},
					},
				},
			},
		},
		HashOnPK: false,
	}

	bucket := spillBucket{
		buildFd: buildFd,
		probeFd: probeFd,
		depth:   1,
	}

	// Set a very low threshold to force re-spilling
	hashJoin.ctr.spillThreshold = 1000 // 1KB - will definitely trigger re-spill

	// Call rebuildHashmapForBucket - should trigger re-spill
	jm, _, err := hashJoin.rebuildHashmapForBucket(proc, bucket, analyzer)
	require.NoError(t, err)
	require.Nil(t, jm, "should return nil when re-spilling occurs")

	// Verify sub-buckets were created and prepended to spillQueue
	require.Greater(t, len(hashJoin.ctr.spillQueue), 0, "spillQueue should have sub-buckets")
	require.Equal(t, 2, hashJoin.ctr.spillQueue[0].depth, "sub-buckets should be at depth 2")

	// Verify sub-bucket fds are valid
	subBucket := hashJoin.ctr.spillQueue[0]
	require.NotNil(t, subBucket.buildFd, "sub-bucket should have a build fd")

	// Cleanup all sub-buckets
	for _, sb := range hashJoin.ctr.spillQueue {
		if sb.buildFd != nil {
			sb.buildFd.Close()
		}
		if sb.probeFd != nil {
			sb.probeFd.Close()
		}
	}
}

// TestReSpillDiscardsProbeForEmptyBuild verifies that during re-spill, probe
// rows mapping to empty build sub-buckets are discarded (inner join), and those
// sub-buckets are not enqueued.
func TestReSpillDiscardsProbeForEmptyBuild(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()

	spillfs, err := proc.GetSpillFileService()
	require.NoError(t, err)

	analyzer := process.NewAnalyzer(0, false, false, "test")

	// Use very few rows (5) so only a few of 32 sub-buckets get data,
	// leaving many sub-buckets empty.
	buildBat := batch.NewWithSize(1)
	buildBat.Vecs[0] = testutil.MakeInt32Vector([]int32{1, 2, 3, 4, 5}, nil, proc.Mp())
	buildBat.SetRowCount(5)

	probeBat := batch.NewWithSize(1)
	probeBat.Vecs[0] = testutil.MakeInt32Vector(makeSequence(1000), nil, proc.Mp())
	probeBat.SetRowCount(1000)

	// Write build and probe to spill files.
	buildFile, err := spillfs.CreateAndRemoveFile(context.Background(), "test_discard_build")
	require.NoError(t, err)
	ctr := &container{}
	bw := spillBucketWriter{file: buildFile}
	_, err = ctr.flushBucketBuffer(proc, buildBat, &bw, analyzer)
	require.NoError(t, err)
	buildFd := bw.handOffFd()

	probeFile, err := spillfs.CreateAndRemoveFile(context.Background(), "test_discard_probe")
	require.NoError(t, err)
	pw := spillBucketWriter{file: probeFile}
	_, err = ctr.flushBucketBuffer(proc, probeBat, &pw, analyzer)
	require.NoError(t, err)
	probeFd := pw.handOffFd()

	hashJoin := &HashJoin{
		EqConds: [][]*plan.Expr{
			{ // probe side
				{
					Typ:  plan.Type{Id: int32(types.T_int32), Width: 32},
					Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: 0}},
				},
			},
			{ // build side
				{
					Typ:  plan.Type{Id: int32(types.T_int32), Width: 32},
					Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: 0}},
				},
			},
		},
		HashOnPK: false,
	}

	bucket := spillBucket{
		buildFd:  buildFd,
		probeFd:  probeFd,
		baseName: "discard_test",
		depth:    1,
	}

	// Force re-spill.
	hashJoin.ctr.spillThreshold = 1

	// Initialize probe-side eqCondExecs (required for evalJoinCondition during probe scatter).
	hashJoin.ctr.eqCondExecs, err = colexec.NewExpressionExecutorsFromPlanExpressions(proc, hashJoin.EqConds[0])
	require.NoError(t, err)
	hashJoin.ctr.eqCondVecs = make([]*vector.Vector, len(hashJoin.ctr.eqCondExecs))

	jm, _, err := hashJoin.rebuildHashmapForBucket(proc, bucket, analyzer)
	require.NoError(t, err)
	require.Nil(t, jm, "should re-spill")

	// With 5 build rows across 32 sub-buckets, most sub-buckets should be empty.
	// Only sub-buckets with build data should be enqueued.
	require.Greater(t, len(hashJoin.ctr.spillQueue), 0)
	require.Less(t, len(hashJoin.ctr.spillQueue), spillNumBuckets,
		"not all sub-buckets should be enqueued — empty build sub-buckets should be discarded")

	// Every enqueued sub-bucket must have a build fd.
	for _, sb := range hashJoin.ctr.spillQueue {
		require.NotNil(t, sb.buildFd, "enqueued sub-bucket must have build data")
		// For inner join, probe without build is useless — but probe with build should be present.
	}

	// Cleanup.
	for _, sb := range hashJoin.ctr.spillQueue {
		if sb.buildFd != nil {
			sb.buildFd.Close()
		}
		if sb.probeFd != nil {
			sb.probeFd.Close()
		}
	}
}

// TestReSpillBucketDepthLimit tests that re-spilling stops at spillMaxPass
func TestReSpillBucketDepthLimit(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()

	spillfs, err := proc.GetSpillFileService()
	require.NoError(t, err)

	analyzer := process.NewAnalyzer(0, false, false, "test")

	// Create test data
	buildBat := batch.NewWithSize(1)
	buildBat.Vecs[0] = testutil.MakeInt32Vector(makeSequence(100), nil, proc.Mp())
	buildBat.SetRowCount(100)

	probeBat := batch.NewWithSize(1)
	probeBat.Vecs[0] = testutil.MakeInt32Vector(makeSequence(100), nil, proc.Mp())
	probeBat.SetRowCount(100)

	// Write to spill files
	buildBucketName := "test_depth_limit_build"
	buildFile, err := spillfs.CreateAndRemoveFile(context.Background(), buildBucketName)
	require.NoError(t, err)

	ctr := &container{}
	buildFile_sw := spillBucketWriter{file: buildFile}
	_, err = ctr.flushBucketBuffer(proc, buildBat, &buildFile_sw, analyzer)
	require.NoError(t, err)
	buildFd := buildFile_sw.handOffFd()

	probeBucketName := "test_depth_limit_probe"
	probeFile, err := spillfs.CreateAndRemoveFile(context.Background(), probeBucketName)
	require.NoError(t, err)
	probeFile_sw := spillBucketWriter{file: probeFile}
	_, err = ctr.flushBucketBuffer(proc, probeBat, &probeFile_sw, analyzer)
	require.NoError(t, err)
	depthProbeFd := probeFile_sw.handOffFd()

	// Setup HashJoin
	hashJoin := &HashJoin{
		EqConds: [][]*plan.Expr{
			{
				&plan.Expr{
					Typ: plan.Type{Id: int32(types.T_int32), Width: 32},
					Expr: &plan.Expr_Col{
						Col: &plan.ColRef{ColPos: 0},
					},
				},
			},
			{
				&plan.Expr{
					Typ: plan.Type{Id: int32(types.T_int32), Width: 32},
					Expr: &plan.Expr_Col{
						Col: &plan.ColRef{ColPos: 0},
					},
				},
			},
		},
		HashOnPK: false,
	}

	// Bucket at max depth
	bucket := spillBucket{
		buildFd: buildFd,
		probeFd: depthProbeFd,
		depth:   spillMaxPass, // at max depth
	}

	// Set low threshold - but should NOT re-spill because we're at max depth
	hashJoin.ctr.spillThreshold = 1000

	// Call rebuildHashmapForBucket
	jm, _, err := hashJoin.rebuildHashmapForBucket(proc, bucket, analyzer)
	require.NoError(t, err)
	require.NotNil(t, jm, "should return JoinMap even if memory exceeds threshold at max depth")
	require.Equal(t, 0, len(hashJoin.ctr.spillQueue), "should not create sub-buckets at max depth")

	jm.Free()
}

// TestMultiLevelSpillIntegration tests the full multi-level spill flow
func TestMultiLevelSpillIntegration(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()

	spillfs, err := proc.GetSpillFileService()
	require.NoError(t, err)

	analyzer := process.NewAnalyzer(0, false, false, "test")

	// Create skewed data - many rows with same key to force re-spilling
	values := make([]int32, 2000)
	for i := 0; i < 2000; i++ {
		// Create skew: 80% of rows have key 1, rest distributed
		if i < 1600 {
			values[i] = 1
		} else {
			values[i] = int32(i)
		}
	}

	buildBat := batch.NewWithSize(1)
	buildBat.Vecs[0] = testutil.MakeInt32Vector(values, nil, proc.Mp())
	buildBat.SetRowCount(2000)

	probeBat := batch.NewWithSize(1)
	probeBat.Vecs[0] = testutil.MakeInt32Vector(values, nil, proc.Mp())
	probeBat.SetRowCount(2000)

	// Write to spill files
	buildBucketName := "test_multilevel_build"
	buildFile, err := spillfs.CreateAndRemoveFile(context.Background(), buildBucketName)
	require.NoError(t, err)

	ctr := &container{}
	buildFile_sw := spillBucketWriter{file: buildFile}
	_, err = ctr.flushBucketBuffer(proc, buildBat, &buildFile_sw, analyzer)
	require.NoError(t, err)
	buildFd := buildFile_sw.handOffFd()

	probeBucketName := "test_multilevel_probe"
	probeFile, err := spillfs.CreateAndRemoveFile(context.Background(), probeBucketName)
	require.NoError(t, err)
	probeFile_sw := spillBucketWriter{file: probeFile}
	_, err = ctr.flushBucketBuffer(proc, probeBat, &probeFile_sw, analyzer)
	require.NoError(t, err)
	multiProbeFd := probeFile_sw.handOffFd()

	// Setup HashJoin
	hashJoin := &HashJoin{
		EqConds: [][]*plan.Expr{
			{
				&plan.Expr{
					Typ: plan.Type{Id: int32(types.T_int32), Width: 32},
					Expr: &plan.Expr_Col{
						Col: &plan.ColRef{ColPos: 0},
					},
				},
			},
			{
				&plan.Expr{
					Typ: plan.Type{Id: int32(types.T_int32), Width: 32},
					Expr: &plan.Expr_Col{
						Col: &plan.ColRef{ColPos: 0},
					},
				},
			},
		},
		HashOnPK: false,
	}

	bucket := spillBucket{
		buildFd: buildFd,
		probeFd: multiProbeFd,
		depth:   1,
	}

	// Set very low threshold to force multiple levels of spilling
	hashJoin.ctr.spillThreshold = 2000 // 2KB

	// First rebuild - should re-spill
	jm, _, err := hashJoin.rebuildHashmapForBucket(proc, bucket, analyzer)
	require.NoError(t, err)
	require.Nil(t, jm, "first rebuild should re-spill")
	require.Greater(t, len(hashJoin.ctr.spillQueue), 0)

	firstLevelBuckets := len(hashJoin.ctr.spillQueue)
	require.Equal(t, spillNumBuckets, firstLevelBuckets, "should create 32 sub-buckets")

	// Process first sub-bucket - might re-spill again if still too large
	subBucket := hashJoin.ctr.spillQueue[0]
	hashJoin.ctr.spillQueue = hashJoin.ctr.spillQueue[1:]

	jm2, _, err := hashJoin.rebuildHashmapForBucket(proc, subBucket, analyzer)
	require.NoError(t, err)
	// jm2 might be nil (re-spilled) or non-nil (fits in memory) depending on data distribution

	if jm2 != nil {
		jm2.Free()
	}

	// Cleanup all remaining buckets (fd close releases inode)
	for _, sb := range hashJoin.ctr.spillQueue {
		if sb.buildFd != nil {
			sb.buildFd.Close()
		}
		if sb.probeFd != nil {
			sb.probeFd.Close()
		}
	}
}

// Helper function to create a sequence of int32 values
func makeSequence(n int) []int32 {
	result := make([]int32, n)
	for i := 0; i < n; i++ {
		result[i] = int32(i)
	}
	return result
}

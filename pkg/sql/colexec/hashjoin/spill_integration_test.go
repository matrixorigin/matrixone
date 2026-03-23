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
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
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
	buildFile, err := spillfs.CreateFile(context.Background(), buildBucketName)
	require.NoError(t, err)

	ctr := &container{}
	buildFile_sw := spillBucketWriter{file: buildFile}
	_, err = ctr.flushBucketBuffer(proc, buildBat, &buildFile_sw, analyzer)
	require.NoError(t, err)
	buildFile.Close()

	// Create probe spill file (empty for this test)
	probeBucketName := "test_rebuild_probe"
	probeFile, err := spillfs.CreateFile(context.Background(), probeBucketName)
	require.NoError(t, err)
	probeFile.Close()

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
		buildFile: buildBucketName,
		probeFile: probeBucketName,
		depth:     0,
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

	// Verify build file was deleted
	_, err = spillfs.OpenFile(context.Background(), buildBucketName)
	require.Error(t, err, "build file should be deleted after rebuild")

	// Cleanup
	spillfs.Delete(context.Background(), probeBucketName)
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
	buildFile, err := spillfs.CreateFile(context.Background(), buildBucketName)
	require.NoError(t, err)

	ctr := &container{}
	buildFile_sw := spillBucketWriter{file: buildFile}
	_, err = ctr.flushBucketBuffer(proc, buildBat, &buildFile_sw, analyzer)
	require.NoError(t, err)
	buildFile.Close()

	probeBucketName := "test_respill_probe"
	probeFile, err := spillfs.CreateFile(context.Background(), probeBucketName)
	require.NoError(t, err)
	probeFile_sw := spillBucketWriter{file: probeFile}
	_, err = ctr.flushBucketBuffer(proc, probeBat, &probeFile_sw, analyzer)
	require.NoError(t, err)
	probeFile.Close()

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
		buildFile: buildBucketName,
		probeFile: probeBucketName,
		depth:     0,
	}

	// Set a very low threshold to force re-spilling
	hashJoin.ctr.spillThreshold = 1000 // 1KB - will definitely trigger re-spill

	// Call rebuildHashmapForBucket - should trigger re-spill
	jm, _, err := hashJoin.rebuildHashmapForBucket(proc, bucket, analyzer)
	require.NoError(t, err)
	require.Nil(t, jm, "should return nil when re-spilling occurs")

	// Verify sub-buckets were created and prepended to spillQueue
	require.Greater(t, len(hashJoin.ctr.spillQueue), 0, "spillQueue should have sub-buckets")
	require.Equal(t, 1, hashJoin.ctr.spillQueue[0].depth, "sub-buckets should be at depth 1")

	// Verify sub-bucket files exist
	subBucket := hashJoin.ctr.spillQueue[0]
	buildReader := &spillBucketReader{}
	err = buildReader.resetForFile(proc.Ctx, spillfs, subBucket.buildFile)
	require.NoError(t, err)
	buildReader.close()

	probeReader := &spillBucketReader{}
	err = probeReader.resetForFile(proc.Ctx, spillfs, subBucket.probeFile)
	require.NoError(t, err)
	probeReader.close()

	// Cleanup all sub-buckets
	for _, sb := range hashJoin.ctr.spillQueue {
		spillfs.Delete(context.Background(), sb.buildFile)
		spillfs.Delete(context.Background(), sb.probeFile)
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
	buildFile, err := spillfs.CreateFile(context.Background(), buildBucketName)
	require.NoError(t, err)

	ctr := &container{}
	buildFile_sw := spillBucketWriter{file: buildFile}
	_, err = ctr.flushBucketBuffer(proc, buildBat, &buildFile_sw, analyzer)
	require.NoError(t, err)
	buildFile.Close()

	probeBucketName := "test_depth_limit_probe"
	probeFile, err := spillfs.CreateFile(context.Background(), probeBucketName)
	require.NoError(t, err)
	probeFile_sw := spillBucketWriter{file: probeFile}
	_, err = ctr.flushBucketBuffer(proc, probeBat, &probeFile_sw, analyzer)
	require.NoError(t, err)
	probeFile.Close()

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
		buildFile: buildBucketName,
		probeFile: probeBucketName,
		depth:     spillMaxPass, // at max depth
	}

	// Set low threshold - but should NOT re-spill because we're at max depth
	hashJoin.ctr.spillThreshold = 1000

	// Call rebuildHashmapForBucket
	jm, _, err := hashJoin.rebuildHashmapForBucket(proc, bucket, analyzer)
	require.NoError(t, err)
	require.NotNil(t, jm, "should return JoinMap even if memory exceeds threshold at max depth")
	require.Equal(t, 0, len(hashJoin.ctr.spillQueue), "should not create sub-buckets at max depth")

	jm.Free()
	spillfs.Delete(context.Background(), probeBucketName)
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
	buildFile, err := spillfs.CreateFile(context.Background(), buildBucketName)
	require.NoError(t, err)

	ctr := &container{}
	buildFile_sw := spillBucketWriter{file: buildFile}
	_, err = ctr.flushBucketBuffer(proc, buildBat, &buildFile_sw, analyzer)
	require.NoError(t, err)
	buildFile.Close()

	probeBucketName := "test_multilevel_probe"
	probeFile, err := spillfs.CreateFile(context.Background(), probeBucketName)
	require.NoError(t, err)
	probeFile_sw := spillBucketWriter{file: probeFile}
	_, err = ctr.flushBucketBuffer(proc, probeBat, &probeFile_sw, analyzer)
	require.NoError(t, err)
	probeFile.Close()

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
		buildFile: buildBucketName,
		probeFile: probeBucketName,
		depth:     0,
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

	// Cleanup all remaining buckets
	for _, sb := range hashJoin.ctr.spillQueue {
		spillfs.Delete(context.Background(), sb.buildFile)
		spillfs.Delete(context.Background(), sb.probeFile)
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

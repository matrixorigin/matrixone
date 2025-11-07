// Copyright 2025 Matrix Origin
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

package hashbuild

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/stretchr/testify/require"
)

func TestHashBuildSpillBasic(t *testing.T) {
	defer cleanupSpillFiles(t)

	mp := mpool.MustNewZero()
	proc := testutil.NewProcessWithMPool(t, "", mp)
	defer proc.Free()

	// Create a hash build operator with low spill threshold
	hashBuild := &HashBuild{
		Spillable:            true,
		SpillMemoryThreshold: 1024, // Very low threshold to trigger spill
		NeedHashMap:          true,
		Conditions: []*plan.Expr{
			newExpr(0, types.T_int64.ToType()),
		},
		JoinMapTag:    1,
		JoinMapRefCnt: 1,
		OperatorBase: vm.OperatorBase{
			OperatorInfo: vm.OperatorInfo{
				Idx:     0,
				IsFirst: false,
				IsLast:  false,
			},
		},
	}

	// Prepare the operator
	err := hashBuild.Prepare(proc)
	require.NoError(t, err)
	defer hashBuild.Free(proc, false, nil)

	// Create a mock source that will generate enough data to trigger spill
	mockOp := colexec.NewMockOperator()

	// Generate data that will exceed the memory threshold
	bat := batch.NewWithSize(1)
	bat.Vecs[0] = testutil.MakeInt64Vector([]int64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}, nil)
	bat.SetRowCount(10)

	// Add more batches to trigger spill
	for i := 0; i < 100; i++ {
		mockOp.WithBatchs([]*batch.Batch{bat})
	}

	hashBuild.SetChildren([]vm.Operator{mockOp})

	// Execute the hash build
	result, err := vm.Exec(hashBuild, proc)
	require.NoError(t, err)
	_ = result

	// Check that spill occurred
	require.True(t, hashBuild.ctr.spilled, "Hash build should have spilled")
	require.Equal(t, 8, hashBuild.ctr.partitionCnt, "Should have 8 partitions")
	require.Greater(t, hashBuild.ctr.memUsed, hashBuild.SpillMemoryThreshold)

	// Check that partition 0 data remains in memory
	require.NotNil(t, hashBuild.ctr.hashmapBuilder.Batches.Buf)
	require.Greater(t, len(hashBuild.ctr.hashmapBuilder.Batches.Buf), 0)

	// Check that spill files were created
	for i := 1; i < 8; i++ {
		filename := filepath.Join(os.TempDir(), "hashbuild_spill_test_0_0")
		_, err := os.Stat(filename)
		require.NoError(t, err, "Spill file should exist for partition %d", i)
		os.Remove(filename) // Cleanup
	}
}

func cleanupSpillFiles(t *testing.T) {
	// Remove files matching the pattern hashbuild_spill_*
	matches, err := filepath.Glob(os.TempDir() + "/hashbuild_spill_*")
	if err != nil {
		t.Logf("Error globbing spill files: %v", err)
		return
	}

	for _, match := range matches {
		if err := os.Remove(match); err != nil {
			t.Logf("Error removing spill file %s: %v", match, err)
		}
	}

	// Remove files matching the pattern join_probe_spill_*
	matches, err = filepath.Glob(os.TempDir() + "/join_probe_spill_*")
	if err != nil {
		t.Logf("Error globbing probe spill files: %v", err)
		return
	}

	for _, match := range matches {
		if err := os.Remove(match); err != nil {
			t.Logf("Error removing probe spill file %s: %v", match, err)
		}
	}
}

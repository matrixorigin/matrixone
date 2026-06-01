// Copyright 2024 Matrix Origin
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

package disttae

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestShouldBailoutOnChangedObjects(t *testing.T) {
	assert.False(t, shouldBailoutOnChangedObjects(0))
	assert.False(t, shouldBailoutOnChangedObjects(maxChangedObjectsForIO))
	assert.True(t, shouldBailoutOnChangedObjects(maxChangedObjectsForIO+1))
	assert.True(t, shouldBailoutOnChangedObjects(1000))
}

func TestShouldBailoutOnCandidateBlocks(t *testing.T) {
	assert.False(t, shouldBailoutOnCandidateBlocks(0))
	assert.False(t, shouldBailoutOnCandidateBlocks(maxCandidateBlksForIO))
	assert.True(t, shouldBailoutOnCandidateBlocks(maxCandidateBlksForIO+1))
	assert.True(t, shouldBailoutOnCandidateBlocks(1000))
}

func TestPkCheckSemaphore_LimitsConcurrency(t *testing.T) {
	// Verify the semaphore actually limits concurrent goroutines.
	const workers = 100
	semCap := cap(pkCheckSemaphore) // should be 16

	var maxConcurrent atomic.Int32
	var current atomic.Int32
	var wg sync.WaitGroup

	ctx := context.Background()

	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			select {
			case pkCheckSemaphore <- struct{}{}:
			case <-ctx.Done():
				return
			}
			defer func() { <-pkCheckSemaphore }()

			c := current.Add(1)
			for {
				old := maxConcurrent.Load()
				if c <= old || maxConcurrent.CompareAndSwap(old, c) {
					break
				}
			}
			time.Sleep(time.Millisecond)
			current.Add(-1)
		}()
	}

	wg.Wait()

	assert.LessOrEqual(t, int(maxConcurrent.Load()), semCap,
		"concurrent goroutines should not exceed semaphore capacity %d", semCap)
	assert.Greater(t, int(maxConcurrent.Load()), 1,
		"should have some concurrency")
}

func TestPkCheckSemaphore_RespectsContextCancellation(t *testing.T) {
	// Fill the semaphore completely.
	for i := 0; i < cap(pkCheckSemaphore); i++ {
		pkCheckSemaphore <- struct{}{}
	}
	defer func() {
		for i := 0; i < cap(pkCheckSemaphore); i++ {
			<-pkCheckSemaphore
		}
	}()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	// Should not block — context is already cancelled.
	select {
	case pkCheckSemaphore <- struct{}{}:
		t.Fatal("should not have acquired semaphore")
		<-pkCheckSemaphore
	case <-ctx.Done():
		// expected
	}
}

func TestAcquireReleasePKCheckSemaphore(t *testing.T) {
	require.NoError(t, acquirePKCheckSemaphore(context.Background()))
	releasePKCheckSemaphore()

	for i := 0; i < cap(pkCheckSemaphore); i++ {
		require.NoError(t, acquirePKCheckSemaphore(context.Background()))
	}
	defer func() {
		for i := 0; i < cap(pkCheckSemaphore); i++ {
			releasePKCheckSemaphore()
		}
	}()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	require.ErrorIs(t, acquirePKCheckSemaphore(ctx), context.Canceled)
}

func TestPKCommitTSMatchedInRange(t *testing.T) {
	mp := mpool.MustNewZero()
	defer func() {
		require.Equal(t, int64(0), mp.CurrNB())
	}()

	from := types.BuildTS(10, 0)
	to := types.BuildTS(20, 0)

	vec := vector.NewVec(types.T_TS.ToType())
	defer vec.Free(mp)
	require.NoError(t, vector.AppendFixed(vec, types.BuildTS(10, 0), false, mp))
	require.NoError(t, vector.AppendFixed(vec, types.BuildTS(11, 0), false, mp))
	require.NoError(t, vector.AppendFixed(vec, types.BuildTS(20, 0), false, mp))
	require.NoError(t, vector.AppendFixed(vec, types.BuildTS(21, 0), false, mp))

	changed, ok := pkCommitTSMatchedInRange(vec, []int64{0, 3}, from, to)
	require.True(t, ok)
	require.False(t, changed)

	changed, ok = pkCommitTSMatchedInRange(vec, []int64{1}, from, to)
	require.True(t, ok)
	require.True(t, changed)

	changed, ok = pkCommitTSMatchedInRange(vec, []int64{2}, from, to)
	require.True(t, ok)
	require.True(t, changed)

	vec.SetNull(1)
	changed, ok = pkCommitTSMatchedInRange(vec, []int64{1}, from, to)
	require.False(t, ok)
	require.False(t, changed)

	constNull := vector.NewConstNull(types.T_TS.ToType(), 1, mp)
	defer constNull.Free(mp)
	changed, ok = pkCommitTSMatchedInRange(constNull, []int64{0}, from, to)
	require.False(t, ok)
	require.False(t, changed)

	// nil vector returns (false, false)
	changed, ok = pkCommitTSMatchedInRange(nil, []int64{0}, from, to)
	require.False(t, ok)
	require.False(t, changed)

	// Wrong type (non-TS) returns (false, false)
	wrongTypeVec := vector.NewVec(types.T_int64.ToType())
	defer wrongTypeVec.Free(mp)
	require.NoError(t, vector.AppendFixed[int64](wrongTypeVec, 42, false, mp))
	changed, ok = pkCommitTSMatchedInRange(wrongTypeVec, []int64{0}, from, to)
	require.False(t, ok)
	require.False(t, changed)

	// Out-of-range sel returns (false, false)
	vec2 := vector.NewVec(types.T_TS.ToType())
	defer vec2.Free(mp)
	require.NoError(t, vector.AppendFixed(vec2, types.BuildTS(15, 0), false, mp))
	changed, ok = pkCommitTSMatchedInRange(vec2, []int64{5}, from, to)
	require.False(t, ok)
	require.False(t, changed)

	// Negative sel returns (false, false)
	changed, ok = pkCommitTSMatchedInRange(vec2, []int64{-1}, from, to)
	require.False(t, ok)
	require.False(t, changed)
}

func TestPkCheckBailoutOnChangedObjects(t *testing.T) {
	// Below threshold — no bailout, no counter increment.
	assert.False(t, pkCheckBailoutOnChangedObjects(0))
	assert.False(t, pkCheckBailoutOnChangedObjects(maxChangedObjectsForIO))

	// Above threshold — bailout fires.
	assert.True(t, pkCheckBailoutOnChangedObjects(maxChangedObjectsForIO+1))
	assert.True(t, pkCheckBailoutOnChangedObjects(1000))
}

func TestPkCheckBailoutOnCandidateBlocks(t *testing.T) {
	// Below threshold — no bailout.
	assert.False(t, pkCheckBailoutOnCandidateBlocks(0))
	assert.False(t, pkCheckBailoutOnCandidateBlocks(maxCandidateBlksForIO))

	// Above threshold — bailout fires.
	assert.True(t, pkCheckBailoutOnCandidateBlocks(maxCandidateBlksForIO+1))
	assert.True(t, pkCheckBailoutOnCandidateBlocks(1000))
}

func TestShouldLogPKPersistedChanged(t *testing.T) {
	// System catalog tables should log.
	assert.True(t, shouldLogPKPersistedChanged(catalog.MO_DATABASE_ID))
	assert.True(t, shouldLogPKPersistedChanged(catalog.MO_TABLES_ID))
	assert.True(t, shouldLogPKPersistedChanged(catalog.MO_COLUMNS_ID))

	// Regular user tables should not log.
	assert.False(t, shouldLogPKPersistedChanged(0))
	assert.False(t, shouldLogPKPersistedChanged(1000))
	assert.False(t, shouldLogPKPersistedChanged(999999))
}

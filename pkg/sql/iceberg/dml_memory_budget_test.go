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

package iceberg

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/iceberg/api"
)

func TestDMLMemoryBudgetConcurrentReserveNeverExceedsLimit(t *testing.T) {
	budget := newDMLMemoryBudget(50, 0)
	var admitted atomic.Int64
	var wg sync.WaitGroup
	for range 100 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if budget.reserve(context.Background(), 1) == nil {
				admitted.Add(1)
			}
		}()
	}
	wg.Wait()
	require.Equal(t, int64(50), admitted.Load())
	require.Equal(t, int64(50), budget.used)
}

func TestDMLObjectReadAllowanceStopsAtExhaustedBudget(t *testing.T) {
	require.Equal(t, int64(0), dmlObjectReadAllowance(10, 10))
	require.Equal(t, int64(0), dmlObjectReadAllowance(10, 11))
	require.Equal(t, int64(1), dmlObjectReadAllowance(10, 9))
}

func TestEffectiveDMLInitialMemoryIncludesMaterializedWriteState(t *testing.T) {
	budget := newDMLMemoryBudget(100, 10)
	require.NoError(t, budget.reserve(context.Background(), 30))
	require.Equal(t, int64(40), effectiveDMLInitialMemory(10, budget))
	require.Equal(t, int64(50), effectiveDMLInitialMemory(50, budget))
	require.Equal(t, int64(10), effectiveDMLInitialMemory(10, nil))
}

func TestDMLMatchedCollectorRejectsBeforeRetainingOverLimitBatch(t *testing.T) {
	bat, cleanup := newMatchedScanBatch(t)
	defer cleanup()
	budget := newDMLMemoryBudget(1, 0)
	collector := NewDMLMatchedScanCollector(DMLMatchedScanCollectorSpec{
		DataFiles:               dmlDeleteCoordinatorDataFiles(),
		DataFilePathColumnIndex: 2,
		RowOrdinalColumnIndex:   3,
		IncludePositionRows:     true,
		MemoryBudget:            budget,
	})

	err := collector.AddBatch(context.Background(), bat)
	require.Error(t, err)
	require.Contains(t, err.Error(), string(api.ErrPlanningLimitExceeded))
	require.Empty(t, collector.Targets())
	require.Zero(t, budget.used)
}

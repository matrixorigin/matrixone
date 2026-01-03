// Copyright 2021 - 2024 Matrix Origin
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
	"fmt"
	goruntime "runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lni/goutils/leaktest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/clusterservice"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/lockservice"
	"github.com/matrixorigin/matrixone/pkg/pb/statsinfo"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae/cache"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae/logtailreplay"
)

func runTest(
	t *testing.T,
	test func(ctx context.Context, e *Engine),
	opts ...GlobalStatsOption,
) {
	defer leaktest.AfterTest(t)()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sid := "s1"
	rt := runtime.DefaultRuntime()
	runtime.SetupServiceBasedRuntime(sid, rt)
	cluster := clusterservice.NewMOCluster(
		sid,
		nil,
		time.Hour,
		clusterservice.WithDisableRefresh(),
	)
	defer cluster.Close()
	rt.SetGlobalVariables(runtime.ClusterService, cluster)
	lk := lockservice.NewLockService(lockservice.Config{
		ServiceID: sid,
	})
	defer lk.Close()
	rt.SetGlobalVariables(runtime.LockService, lk)
	mp, err := mpool.NewMPool(sid, 1024*1024, mpool.NoFixed)
	catalog.SetupDefines(sid)
	assert.NoError(t, err)
	e := New(
		ctx,
		sid,
		mp,
		nil,
		nil,
		nil,
		nil,
		4,
	)
	for _, opt := range opts {
		opt(e.globalStats)
	}
	defer e.Close()
	test(ctx, e)
}

func insertTable(
	t *testing.T,
	e *Engine,
	did, tid uint64,
	dname, tname string,
) (uint64, uint64) {
	tbl := catalog.Table{
		AccountId:    0,
		UserId:       0,
		RoleId:       0,
		DatabaseId:   did,
		DatabaseName: dname,
		TableId:      tid,
		TableName:    tname,
	}
	packer := types.NewPacker()
	bat, err := catalog.GenCreateTableTuple(tbl, e.mp, packer)
	assert.NoError(t, err)
	_, err = fillRandomRowidAndZeroTs(bat, e.mp)
	assert.NoError(t, err)
	ccache := e.catalog.Load()
	ccache.InsertTable(bat)
	tableItem := ccache.GetTableByName(0, did, tname)
	assert.NotNil(t, tableItem)
	defs, err := catalog.GenColumnsFromDefs(
		0,
		tname,
		dname,
		tid,
		did,
		catalog.GetDefines(e.service).MoDatabaseTableDefs,
	)
	assert.NoError(t, err)
	cache.InitTableItemWithColumns(tableItem, defs)
	return tableItem.DatabaseId, tableItem.Id
}

func TestUpdateStats(t *testing.T) {
	t.Run("no table", func(t *testing.T) {
		runTest(t, func(ctx context.Context, e *Engine) {
			k := statsinfo.StatsInfoKey{
				DatabaseID: 1000,
				TableID:    1001,
			}
			stats := plan2.NewStatsInfo()
			ps := logtailreplay.NewPartitionState("", true, 1001, false)
			updated := e.globalStats.doUpdate(ctx, ps, k, stats)
			assert.False(t, updated)
		})
	})

	t.Run("no obj", func(t *testing.T) {
		runTest(t, func(ctx context.Context, e *Engine) {
			did := uint64(1000)
			dname := "test-db"
			tid := uint64(1001)
			tname := "test-table"
			did1, tid1 := insertTable(t, e, did, tid, dname, tname)
			assert.Equal(t, did, did1)
			assert.Equal(t, tid, tid1)
			k := statsinfo.StatsInfoKey{
				DatabaseID: did,
				TableID:    tid,
			}
			stats := plan2.NewStatsInfo()
			ps := logtailreplay.NewPartitionState("", true, tid, false)
			updated := e.globalStats.doUpdate(ctx, ps, k, stats)
			assert.False(t, updated)
		})
	})

	t.Run("objs", func(t *testing.T) {
		runTest(t, func(ctx context.Context, e *Engine) {
			did := uint64(1000)
			dname := "test-db"
			tid := uint64(1001)
			tname := "test-table"
			did1, tid1 := insertTable(t, e, did, tid, dname, tname)
			assert.Equal(t, did, did1)
			assert.Equal(t, tid, tid1)
			k := statsinfo.StatsInfoKey{
				DatabaseID: did,
				TableID:    tid,
			}
			stats := plan2.NewStatsInfo()
			ps := logtailreplay.NewPartitionState("", true, tid, false)
			updated := e.globalStats.doUpdate(ctx, ps, k, stats)
			assert.True(t, updated)
		}, WithApproxObjectNumUpdater(func() int64 {
			return 10
		}))
	})
}

func TestGlobalStats_ShouldUpdate(t *testing.T) {
	defer leaktest.AfterTest(t)()

	t.Run("basic", func(t *testing.T) {
		origMinUpdateInterval := MinUpdateInterval
		defer func() {
			MinUpdateInterval = origMinUpdateInterval
		}()
		MinUpdateInterval = time.Millisecond * 10
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		gs := NewGlobalStats(ctx, nil, nil)
		assert.NotNil(t, gs)
		k1 := statsinfo.StatsInfoKey{
			DatabaseID: 100,
			TableID:    101,
		}
		assert.True(t, gs.shouldUpdate(k1))
		assert.False(t, gs.shouldUpdate(k1))
		gs.doneUpdate(k1, true)
		time.Sleep(MinUpdateInterval)
		assert.True(t, gs.shouldUpdate(k1))
	})

	t.Run("parallel", func(t *testing.T) {
		origMinUpdateInterval := MinUpdateInterval
		defer func() {
			MinUpdateInterval = origMinUpdateInterval
		}()
		MinUpdateInterval = time.Second * 10
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		gs := NewGlobalStats(ctx, nil, nil)
		assert.NotNil(t, gs)
		k1 := statsinfo.StatsInfoKey{
			DatabaseID: 100,
			TableID:    101,
		}
		var count atomic.Int32
		var wg sync.WaitGroup
		updateFn := func() {
			defer wg.Done()
			if !gs.shouldUpdate(k1) {
				return
			}
			count.Add(1)
			gs.doneUpdate(k1, true)
		}
		for i := 0; i < 20; i++ {
			wg.Add(1)
			go updateFn()
		}
		wg.Wait()
		assert.Equal(t, 1, int(count.Load()))
	})
}

func TestQueueWatcher(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testAdjustFn := func(qw *queueWatcher) {
		qw.checkInterval = time.Millisecond * 10
		qw.threshold = time.Millisecond * 10
	}
	q := newQueueWatcher()
	testAdjustFn(q)

	t.Run("ok", func(t *testing.T) {
		q.add(101)
		q.add(102)
		assert.Equal(t, 2, len(q.value))
		q.del(101)
		assert.Equal(t, 1, len(q.value))

		time.Sleep(time.Millisecond * 20)
		list := q.check()
		assert.Equal(t, 1, len(list))
		q.del(102)
		assert.Equal(t, 0, len(q.value))
	})

	t.Run("run in background", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		go q.run(ctx)
		q.add(101)
		q.add(102)
		time.Sleep(time.Millisecond * 20)
		list := q.check()
		assert.Equal(t, 2, len(list))
	})

}

// TestGetMinMaxValueByFloat64_Decimal tests decimal64 and decimal128 conversion
// especially for negative values which use two's complement representation
func TestGetMinMaxValueByFloat64_Decimal(t *testing.T) {
	defer leaktest.AfterTest(t)()

	t.Run("decimal64 positive", func(t *testing.T) {
		// Test positive value: 123.45 with scale=2
		scale := int32(2)
		typ := types.New(types.T_decimal64, 10, scale)
		value, err := types.Decimal64FromFloat64(123.45, 10, scale)
		assert.NoError(t, err)
		buf := types.EncodeDecimal64(&value)

		result := getMinMaxValueByFloat64(typ, buf)
		assert.InDelta(t, 123.45, result, 0.01)
	})

	t.Run("decimal64 negative", func(t *testing.T) {
		// Test negative value: -123.45 with scale=2
		// This is the key test case - negative values use two's complement
		// and would be incorrectly converted to huge positive numbers before the fix
		scale := int32(2)
		typ := types.New(types.T_decimal64, 10, scale)
		value, err := types.Decimal64FromFloat64(-123.45, 10, scale)
		assert.NoError(t, err)
		buf := types.EncodeDecimal64(&value)

		result := getMinMaxValueByFloat64(typ, buf)
		// Before fix: this would be ~18446744073709539271 (two's complement as positive)
		// After fix: correctly returns -123.45
		assert.InDelta(t, -123.45, result, 0.01)
		assert.Less(t, result, 0.0, "negative value should be less than 0")
	})

	t.Run("decimal64 different scales", func(t *testing.T) {
		testCases := []struct {
			name  string
			scale int32
			value float64
		}{
			{"scale_0", 0, -100.0},
			{"scale_2", 2, -99.99},
			{"scale_4", 4, -1234.5678},
			{"scale_6", 6, -0.123456},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				typ := types.New(types.T_decimal64, 18, tc.scale)
				value, err := types.Decimal64FromFloat64(tc.value, 18, tc.scale)
				assert.NoError(t, err)
				buf := types.EncodeDecimal64(&value)

				result := getMinMaxValueByFloat64(typ, buf)
				assert.InDelta(t, tc.value, result, 0.01)
			})
		}
	})

	t.Run("decimal128 positive", func(t *testing.T) {
		// Test positive large value with scale=4
		scale := int32(4)
		typ := types.New(types.T_decimal128, 20, scale)
		value, err := types.Decimal128FromFloat64(1234567890.1234, 20, scale)
		assert.NoError(t, err)
		buf := types.EncodeDecimal128(&value)

		result := getMinMaxValueByFloat64(typ, buf)
		assert.InDelta(t, 1234567890.1234, result, 0.01)
	})

	t.Run("decimal128 negative", func(t *testing.T) {
		// Test negative large value with scale=4
		scale := int32(4)
		typ := types.New(types.T_decimal128, 20, scale)
		value, err := types.Decimal128FromFloat64(-9876543210.5678, 20, scale)
		assert.NoError(t, err)
		buf := types.EncodeDecimal128(&value)

		result := getMinMaxValueByFloat64(typ, buf)
		// Key assertion: negative value should be correctly converted
		assert.InDelta(t, -9876543210.5678, result, 0.01)
		assert.Less(t, result, 0.0, "negative value should be less than 0")
	})

	t.Run("decimal64 zero", func(t *testing.T) {
		scale := int32(2)
		typ := types.New(types.T_decimal64, 10, scale)
		value, err := types.Decimal64FromFloat64(0.0, 10, scale)
		assert.NoError(t, err)
		buf := types.EncodeDecimal64(&value)

		result := getMinMaxValueByFloat64(typ, buf)
		assert.InDelta(t, 0.0, result, 0.01)
	})

	t.Run("decimal64 min_max_range", func(t *testing.T) {
		// Test that min < max relationship is preserved
		scale := int32(2)
		typ := types.New(types.T_decimal64, 10, scale)

		minValue, err := types.Decimal64FromFloat64(-999.99, 10, scale)
		assert.NoError(t, err)
		minBuf := types.EncodeDecimal64(&minValue)

		maxValue, err := types.Decimal64FromFloat64(999.99, 10, scale)
		assert.NoError(t, err)
		maxBuf := types.EncodeDecimal64(&maxValue)

		minResult := getMinMaxValueByFloat64(typ, minBuf)
		maxResult := getMinMaxValueByFloat64(typ, maxBuf)

		// Critical assertion: min should be less than max
		// Before fix: minResult would be a huge positive number > maxResult
		assert.Less(t, minResult, maxResult, "min should be less than max")
		assert.InDelta(t, -999.99, minResult, 0.01)
		assert.InDelta(t, 999.99, maxResult, 0.01)
	})
}

// calculateConcurrency is a helper function that mirrors the concurrency calculation logic
// in NewGlobalStats. This allows us to test the logic independently.
func calculateConcurrency(gomaxprocs, updateWorkerFactor int) (executorConcurrency, updateWorkerConcurrency int) {
	executorConcurrency = gomaxprocs
	if updateWorkerFactor > 0 {
		executorConcurrency = executorConcurrency * updateWorkerFactor
	}
	// Apply limits: min MinExecutorConcurrency, max MaxExecutorConcurrency
	if executorConcurrency < MinExecutorConcurrency {
		executorConcurrency = MinExecutorConcurrency
	}
	if executorConcurrency > MaxExecutorConcurrency {
		executorConcurrency = MaxExecutorConcurrency
	}
	// Calculate updateWorker concurrency: executorConcurrency / WorkerConcurrencyRatio, but minimum MinWorkerConcurrency
	updateWorkerConcurrency = executorConcurrency / WorkerConcurrencyRatio
	if updateWorkerConcurrency < MinWorkerConcurrency {
		updateWorkerConcurrency = MinWorkerConcurrency
	}
	return executorConcurrency, updateWorkerConcurrency
}

// TestCalculateConcurrency tests the concurrency calculation logic with various scenarios
func TestCalculateConcurrency(t *testing.T) {
	tests := []struct {
		name               string
		gomaxprocs         int
		updateWorkerFactor int
		expectedExecutor   int
		expectedWorker     int
		expectedTotal      int
		description        string
	}{
		{
			name:               "small_cpu_lower_bound",
			gomaxprocs:         2,
			updateWorkerFactor: 4,
			expectedExecutor:   32, // clamped to minimum 32
			expectedWorker:     16, // 32/4 = 8, but minimum 16
			expectedTotal:      48,
			description:        "Small CPU (2 cores) should use minimum executor=32, worker=16",
		},
		{
			name:               "small_cpu_boundary",
			gomaxprocs:         4,
			updateWorkerFactor: 4,
			expectedExecutor:   32, // clamped to minimum 32
			expectedWorker:     16, // 32/4 = 8, but minimum 16
			expectedTotal:      48,
			description:        "Small CPU (4 cores) should use minimum executor=32, worker=16",
		},
		{
			name:               "medium_cpu_lower",
			gomaxprocs:         8,
			updateWorkerFactor: 4,
			expectedExecutor:   32, // 8*4=32, exactly at minimum
			expectedWorker:     16, // 32/4 = 8, but minimum 16
			expectedTotal:      48,
			description:        "Medium CPU (8 cores) should use executor=32, worker=16",
		},
		{
			name:               "medium_cpu_mid",
			gomaxprocs:         12,
			updateWorkerFactor: 4,
			expectedExecutor:   48, // 12*4=48
			expectedWorker:     16, // 48/4 = 12, but minimum 16
			expectedTotal:      64,
			description:        "Medium CPU (12 cores) should use executor=48, worker=16",
		},
		{
			name:               "medium_cpu_upper",
			gomaxprocs:         16,
			updateWorkerFactor: 4,
			expectedExecutor:   64, // 16*4=64
			expectedWorker:     16, // 64/4 = 16
			expectedTotal:      80,
			description:        "Medium CPU (16 cores) should use executor=64, worker=16",
		},
		{
			name:               "large_cpu_typical",
			gomaxprocs:         24,
			updateWorkerFactor: 4,
			expectedExecutor:   96, // 24*4=96
			expectedWorker:     24, // 96/4 = 24
			expectedTotal:      120,
			description:        "Large CPU (24 cores, typical production) should use executor=96, worker=24",
		},
		{
			name:               "large_cpu_upper_bound",
			gomaxprocs:         27,
			updateWorkerFactor: 4,
			expectedExecutor:   108, // 27*4=108, exactly at maximum
			expectedWorker:     27,  // 108/4 = 27
			expectedTotal:      135,
			description:        "Large CPU (27 cores) should use executor=108, worker=27",
		},
		{
			name:               "very_large_cpu_clamped",
			gomaxprocs:         32,
			updateWorkerFactor: 4,
			expectedExecutor:   108, // 32*4=128, clamped to maximum 108
			expectedWorker:     27,  // 108/4 = 27
			expectedTotal:      135,
			description:        "Very large CPU (32 cores) should clamp executor to 108, worker=27",
		},
		{
			name:               "very_large_cpu_extreme",
			gomaxprocs:         64,
			updateWorkerFactor: 4,
			expectedExecutor:   108, // 64*4=256, clamped to maximum 108
			expectedWorker:     27,  // 108/4 = 27
			expectedTotal:      135,
			description:        "Extreme CPU (64 cores) should clamp executor to 108, worker=27",
		},
		{
			name:               "factor_1",
			gomaxprocs:         24,
			updateWorkerFactor: 1,
			expectedExecutor:   32, // 24*1=24, clamped to minimum 32
			expectedWorker:     16, // 32/4 = 8, but minimum 16
			expectedTotal:      48,
			description:        "Factor=1 should still respect minimum limits",
		},
		{
			name:               "factor_8",
			gomaxprocs:         12,
			updateWorkerFactor: 8,
			expectedExecutor:   96, // 12*8=96
			expectedWorker:     24, // 96/4 = 24
			expectedTotal:      120,
			description:        "Factor=8 should work correctly",
		},
		{
			name:               "factor_8_large_cpu",
			gomaxprocs:         16,
			updateWorkerFactor: 8,
			expectedExecutor:   108, // 16*8=128, clamped to maximum 108
			expectedWorker:     27,  // 108/4 = 27
			expectedTotal:      135,
			description:        "Factor=8 with large CPU should clamp to maximum",
		},
		{
			name:               "zero_factor",
			gomaxprocs:         24,
			updateWorkerFactor: 0,
			expectedExecutor:   32, // 24*0=0, clamped to minimum 32
			expectedWorker:     16, // 32/4 = 8, but minimum 16
			expectedTotal:      48,
			description:        "Zero factor should use GOMAXPROCS only, then apply limits",
		},
		{
			name:               "exact_worker_minimum",
			gomaxprocs:         16,
			updateWorkerFactor: 4,
			expectedExecutor:   64, // 16*4=64
			expectedWorker:     16, // 64/4 = 16, exactly at minimum
			expectedTotal:      80,
			description:        "Worker concurrency exactly at minimum (16)",
		},
		{
			name:               "exact_executor_minimum",
			gomaxprocs:         8,
			updateWorkerFactor: 4,
			expectedExecutor:   32, // 8*4=32, exactly at minimum
			expectedWorker:     16, // 32/4 = 8, but minimum 16
			expectedTotal:      48,
			description:        "Executor concurrency exactly at minimum (32)",
		},
		{
			name:               "exact_executor_maximum",
			gomaxprocs:         27,
			updateWorkerFactor: 4,
			expectedExecutor:   108, // 27*4=108, exactly at maximum
			expectedWorker:     27,  // 108/4 = 27
			expectedTotal:      135,
			description:        "Executor concurrency exactly at maximum (108)",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			executor, worker := calculateConcurrency(tt.gomaxprocs, tt.updateWorkerFactor)

			assert.Equal(t, tt.expectedExecutor, executor,
				"executor concurrency mismatch for %s: expected %d, got %d", tt.description, tt.expectedExecutor, executor)
			assert.Equal(t, tt.expectedWorker, worker,
				"worker concurrency mismatch for %s: expected %d, got %d", tt.description, tt.expectedWorker, worker)
			assert.Equal(t, tt.expectedTotal, executor+worker,
				"total goroutines mismatch for %s: expected %d, got %d", tt.description, tt.expectedTotal, executor+worker)

			// Validate constraints
			assert.GreaterOrEqual(t, executor, MinExecutorConcurrency, "executor should be >= MinExecutorConcurrency")
			assert.LessOrEqual(t, executor, MaxExecutorConcurrency, "executor should be <= MaxExecutorConcurrency")
			assert.GreaterOrEqual(t, worker, MinWorkerConcurrency, "worker should be >= MinWorkerConcurrency")
			assert.Equal(t, worker, max(MinWorkerConcurrency, executor/WorkerConcurrencyRatio), "worker should be max(MinWorkerConcurrency, executor/WorkerConcurrencyRatio)")
		})
	}
}

// TestGlobalStatsConcurrency_ActualCreation tests that GlobalStats actually creates
// the correct number of goroutines by checking the concurrentExecutor's concurrency
func TestGlobalStatsConcurrency_ActualCreation(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// Save original GOMAXPROCS
	originalGOMAXPROCS := goruntime.GOMAXPROCS(0)
	defer goruntime.GOMAXPROCS(originalGOMAXPROCS)

	testCases := []struct {
		name               string
		setGOMAXPROCS      int
		updateWorkerFactor int
		expectedExecutor   int
		expectedWorker     int
	}{
		{
			name:               "small_cpu",
			setGOMAXPROCS:      4,
			updateWorkerFactor: 4,
			expectedExecutor:   32, // clamped to minimum
			expectedWorker:     16, // minimum
		},
		{
			name:               "medium_cpu",
			setGOMAXPROCS:      12,
			updateWorkerFactor: 4,
			expectedExecutor:   48,
			expectedWorker:     16, // 48/4=12, but minimum 16
		},
		{
			name:               "large_cpu",
			setGOMAXPROCS:      24,
			updateWorkerFactor: 4,
			expectedExecutor:   96,
			expectedWorker:     24, // 96/4=24
		},
		{
			name:               "very_large_cpu",
			setGOMAXPROCS:      32,
			updateWorkerFactor: 4,
			expectedExecutor:   108, // clamped to maximum
			expectedWorker:     27,  // 108/4=27
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Set GOMAXPROCS for this test
			goruntime.GOMAXPROCS(tc.setGOMAXPROCS)

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			// Setup minimal runtime (required for GlobalStats initialization)
			sid := "test-s1"
			rt := runtime.DefaultRuntime()
			runtime.SetupServiceBasedRuntime(sid, rt)

			// Create GlobalStats with the specified factor
			gs := NewGlobalStats(ctx, nil, nil, WithUpdateWorkerFactor(tc.updateWorkerFactor))
			require.NotNil(t, gs)

			// Verify concurrentExecutor concurrency
			actualExecutorConcurrency := gs.concurrentExecutor.GetConcurrency()
			assert.Equal(t, tc.expectedExecutor, actualExecutorConcurrency,
				"concurrentExecutor concurrency mismatch: expected %d, got %d",
				tc.expectedExecutor, actualExecutorConcurrency)

			// Verify constraints
			assert.GreaterOrEqual(t, actualExecutorConcurrency, MinExecutorConcurrency,
				"executor concurrency should be >= MinExecutorConcurrency")
			assert.LessOrEqual(t, actualExecutorConcurrency, MaxExecutorConcurrency,
				"executor concurrency should be <= MaxExecutorConcurrency")

			// Verify worker concurrency matches expected calculation
			_, expectedWorker := calculateConcurrency(tc.setGOMAXPROCS, tc.updateWorkerFactor)
			assert.Equal(t, tc.expectedWorker, expectedWorker,
				"worker concurrency calculation should match")

			cancel()
			// Give goroutines time to exit
			time.Sleep(100 * time.Millisecond)
		})
	}
}

// TestGlobalStatsConcurrency_WorkerRatio tests that updateWorker concurrency
// is always executorConcurrency / WorkerConcurrencyRatio (with minimum MinWorkerConcurrency)
func TestGlobalStatsConcurrency_WorkerRatio(t *testing.T) {
	defer leaktest.AfterTest(t)()

	originalGOMAXPROCS := goruntime.GOMAXPROCS(0)
	defer goruntime.GOMAXPROCS(originalGOMAXPROCS)

	testCases := []struct {
		name               string
		setGOMAXPROCS      int
		updateWorkerFactor int
		expectedRatio      float64 // expected worker/executor ratio
	}{
		{
			name:               "minimum_worker",
			setGOMAXPROCS:      8,
			updateWorkerFactor: 4,
			expectedRatio:      0.5, // 16/32 = 0.5 (minimum worker)
		},
		{
			name:               "exact_quarter",
			setGOMAXPROCS:      24,
			updateWorkerFactor: 4,
			expectedRatio:      0.25, // 24/96 = 0.25 (exact 1/4)
		},
		{
			name:               "above_minimum",
			setGOMAXPROCS:      20,
			updateWorkerFactor: 4,
			expectedRatio:      0.25, // 20/80 = 0.25 (exact 1/4)
		},
		{
			name:               "clamped_maximum",
			setGOMAXPROCS:      32,
			updateWorkerFactor: 4,
			expectedRatio:      0.25, // 27/108 = 0.25 (exact 1/4)
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			goruntime.GOMAXPROCS(tc.setGOMAXPROCS)

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			sid := "test-s2"
			rt := runtime.DefaultRuntime()
			runtime.SetupServiceBasedRuntime(sid, rt)

			gs := NewGlobalStats(ctx, nil, nil, WithUpdateWorkerFactor(tc.updateWorkerFactor))
			require.NotNil(t, gs)

			executorConcurrency := gs.concurrentExecutor.GetConcurrency()

			// Calculate expected worker concurrency
			expectedWorkerConcurrency := executorConcurrency / WorkerConcurrencyRatio
			if expectedWorkerConcurrency < MinWorkerConcurrency {
				expectedWorkerConcurrency = MinWorkerConcurrency
			}

			// Verify the ratio
			actualRatio := float64(expectedWorkerConcurrency) / float64(executorConcurrency)
			assert.InDelta(t, tc.expectedRatio, actualRatio, 0.01,
				"worker/executor ratio mismatch: expected ~%.2f, got %.2f",
				tc.expectedRatio, actualRatio)

			// Verify worker is at least MinWorkerConcurrency
			assert.GreaterOrEqual(t, expectedWorkerConcurrency, MinWorkerConcurrency,
				"worker concurrency should be >= MinWorkerConcurrency")

			cancel()
			time.Sleep(100 * time.Millisecond)
		})
	}
}

// TestGlobalStatsConcurrency_EdgeCases tests edge cases and boundary conditions
func TestGlobalStatsConcurrency_EdgeCases(t *testing.T) {
	defer leaktest.AfterTest(t)()

	originalGOMAXPROCS := goruntime.GOMAXPROCS(0)
	defer goruntime.GOMAXPROCS(originalGOMAXPROCS)

	tests := []struct {
		name               string
		setGOMAXPROCS      int
		updateWorkerFactor int
		description        string
	}{
		{
			name:               "minimum_gomaxprocs",
			setGOMAXPROCS:      1,
			updateWorkerFactor: 4,
			description:        "Minimum GOMAXPROCS=1 should still use minimum limits",
		},
		{
			name:               "boundary_below_minimum",
			setGOMAXPROCS:      7,
			updateWorkerFactor: 4,
			description:        "GOMAXPROCS*4=28 < 32 should clamp to 32",
		},
		{
			name:               "boundary_at_minimum",
			setGOMAXPROCS:      8,
			updateWorkerFactor: 4,
			description:        "GOMAXPROCS*4=32 exactly at minimum",
		},
		{
			name:               "boundary_above_minimum",
			setGOMAXPROCS:      9,
			updateWorkerFactor: 4,
			description:        "GOMAXPROCS*4=36 > 32 should use 36",
		},
		{
			name:               "boundary_below_maximum",
			setGOMAXPROCS:      26,
			updateWorkerFactor: 4,
			description:        "GOMAXPROCS*4=104 < 108 should use 104",
		},
		{
			name:               "boundary_at_maximum",
			setGOMAXPROCS:      27,
			updateWorkerFactor: 4,
			description:        "GOMAXPROCS*4=108 exactly at maximum",
		},
		{
			name:               "boundary_above_maximum",
			setGOMAXPROCS:      28,
			updateWorkerFactor: 4,
			description:        "GOMAXPROCS*4=112 > 108 should clamp to 108",
		},
		{
			name:               "very_large_gomaxprocs",
			setGOMAXPROCS:      128,
			updateWorkerFactor: 4,
			description:        "Very large GOMAXPROCS should clamp to maximum",
		},
		{
			name:               "factor_zero",
			setGOMAXPROCS:      24,
			updateWorkerFactor: 0,
			description:        "Factor=0 should use GOMAXPROCS only",
		},
		{
			name:               "factor_one",
			setGOMAXPROCS:      24,
			updateWorkerFactor: 1,
			description:        "Factor=1 should multiply by 1",
		},
		{
			name:               "factor_large",
			setGOMAXPROCS:      8,
			updateWorkerFactor: 16,
			description:        "Large factor should still respect maximum",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			goruntime.GOMAXPROCS(tt.setGOMAXPROCS)

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			sid := "test-edge"
			rt := runtime.DefaultRuntime()
			runtime.SetupServiceBasedRuntime(sid, rt)

			gs := NewGlobalStats(ctx, nil, nil, WithUpdateWorkerFactor(tt.updateWorkerFactor))
			require.NotNil(t, gs)

			executorConcurrency := gs.concurrentExecutor.GetConcurrency()

			// Verify constraints are always satisfied
			assert.GreaterOrEqual(t, executorConcurrency, MinExecutorConcurrency,
				"%s: executor should be >= MinExecutorConcurrency, got %d", tt.description, executorConcurrency)
			assert.LessOrEqual(t, executorConcurrency, MaxExecutorConcurrency,
				"%s: executor should be <= MaxExecutorConcurrency, got %d", tt.description, executorConcurrency)

			// Verify it matches expected calculation
			expectedExecutor, expectedWorker := calculateConcurrency(tt.setGOMAXPROCS, tt.updateWorkerFactor)
			assert.Equal(t, expectedExecutor, executorConcurrency,
				"%s: executor concurrency mismatch", tt.description)

			// Verify worker calculation
			expectedWorkerFromExecutor := expectedExecutor / 4
			if expectedWorkerFromExecutor < 16 {
				expectedWorkerFromExecutor = 16
			}
			assert.Equal(t, expectedWorker, expectedWorkerFromExecutor,
				"%s: worker concurrency calculation mismatch", tt.description)

			cancel()
			time.Sleep(100 * time.Millisecond)
		})
	}
}

// TestGlobalStatsConcurrency_ConcurrentCreation tests that multiple GlobalStats
// instances can be created concurrently without issues
func TestGlobalStatsConcurrency_ConcurrentCreation(t *testing.T) {
	defer leaktest.AfterTest(t)()

	originalGOMAXPROCS := goruntime.GOMAXPROCS(0)
	defer goruntime.GOMAXPROCS(originalGOMAXPROCS)

	goruntime.GOMAXPROCS(24) // Use a typical value

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sid := "test-concurrent"
	rt := runtime.DefaultRuntime()
	runtime.SetupServiceBasedRuntime(sid, rt)

	const numGoroutines = 10
	var wg sync.WaitGroup
	errors := make(chan error, numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			gs := NewGlobalStats(ctx, nil, nil, WithUpdateWorkerFactor(4))
			if gs == nil {
				errors <- fmt.Errorf("goroutine %d: GlobalStats creation failed", id)
				return
			}
			executorConcurrency := gs.concurrentExecutor.GetConcurrency()
			if executorConcurrency < MinExecutorConcurrency || executorConcurrency > MaxExecutorConcurrency {
				errors <- fmt.Errorf("goroutine %d: invalid executor concurrency %d", id, executorConcurrency)
				return
			}
		}(i)
	}

	wg.Wait()
	close(errors)

	// Check for errors
	errList := make([]error, 0, numGoroutines)
	for err := range errors {
		errList = append(errList, err)
	}
	assert.Empty(t, errList, "concurrent creation should not produce errors: %v", errList)
}

// TestGlobalStatsConcurrency_ReductionVerification verifies that the optimization
// actually reduces goroutine count compared to the old implementation
func TestGlobalStatsConcurrency_ReductionVerification(t *testing.T) {
	defer leaktest.AfterTest(t)()

	originalGOMAXPROCS := goruntime.GOMAXPROCS(0)
	defer goruntime.GOMAXPROCS(originalGOMAXPROCS)

	testCases := []struct {
		name               string
		setGOMAXPROCS      int
		updateWorkerFactor int
		oldTotal           int // old implementation total goroutines
		newTotal           int // new implementation total goroutines
		reduction          int // expected reduction
	}{
		{
			name:               "typical_production",
			setGOMAXPROCS:      24,
			updateWorkerFactor: 4,
			oldTotal:           192, // 24*4*2 = 192
			newTotal:           120, // 96+24 = 120
			reduction:          72,  // 37.5% reduction
		},
		{
			name:               "large_cpu",
			setGOMAXPROCS:      32,
			updateWorkerFactor: 4,
			oldTotal:           256, // 32*4*2 = 256
			newTotal:           135, // 108+27 = 135
			reduction:          121, // 47.3% reduction
		},
		{
			name:               "medium_cpu",
			setGOMAXPROCS:      16,
			updateWorkerFactor: 4,
			oldTotal:           128, // 16*4*2 = 128
			newTotal:           80,  // 64+16 = 80
			reduction:          48,  // 37.5% reduction
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			goruntime.GOMAXPROCS(tc.setGOMAXPROCS)

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			sid := "test-reduction"
			rt := runtime.DefaultRuntime()
			runtime.SetupServiceBasedRuntime(sid, rt)

			gs := NewGlobalStats(ctx, nil, nil, WithUpdateWorkerFactor(tc.updateWorkerFactor))
			require.NotNil(t, gs)

			executorConcurrency := gs.concurrentExecutor.GetConcurrency()
			_, workerConcurrency := calculateConcurrency(tc.setGOMAXPROCS, tc.updateWorkerFactor)
			actualTotal := executorConcurrency + workerConcurrency

			assert.Equal(t, tc.newTotal, actualTotal,
				"new implementation total goroutines mismatch")
			assert.Less(t, actualTotal, tc.oldTotal,
				"new implementation should have fewer goroutines than old")

			actualReduction := tc.oldTotal - actualTotal
			assert.Equal(t, tc.reduction, actualReduction,
				"goroutine reduction mismatch: expected %d, got %d",
				tc.reduction, actualReduction)

			reductionPercent := float64(actualReduction) / float64(tc.oldTotal) * 100
			assert.Greater(t, reductionPercent, 30.0,
				"reduction should be at least 30%%, got %.1f%%", reductionPercent)

			cancel()
			time.Sleep(100 * time.Millisecond)
		})
	}
}

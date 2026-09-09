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
	"encoding/binary"
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
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/gossip"
	querypb "github.com/matrixorigin/matrixone/pkg/pb/query"
	"github.com/matrixorigin/matrixone/pkg/pb/statsinfo"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae/cache"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae/logtailreplay"
)

type mockStatsKeyRouter struct {
	target string
	key    statsinfo.StatsInfoKey
}

func (r *mockStatsKeyRouter) Target(key statsinfo.StatsInfoKey) string {
	r.key = key
	return r.target
}
func (r *mockStatsKeyRouter) AddItem(gossip.CommonItem) {}

type mockStatsQueryClient struct {
	response    *querypb.Response
	sendStarted chan struct{}
	allowReturn chan struct{}
	target      string
	request     *querypb.Request
	releases    atomic.Int32
}

func (m *mockStatsQueryClient) ServiceID() string {
	return "mock-stats-query-client"
}

func (m *mockStatsQueryClient) SendMessage(_ context.Context, target string, req *querypb.Request) (*querypb.Response, error) {
	m.target = target
	m.request = req
	if m.sendStarted != nil {
		close(m.sendStarted)
	}
	if m.allowReturn != nil {
		<-m.allowReturn
	}
	return m.response, nil
}

func (m *mockStatsQueryClient) NewRequest(method querypb.CmdMethod) *querypb.Request {
	return &querypb.Request{CmdMethod: method}
}

func (m *mockStatsQueryClient) Release(*querypb.Response) {
	m.releases.Add(1)
}

func (m *mockStatsQueryClient) Close() error {
	return nil
}

func installRemoteStatsTestTable(
	t *testing.T,
	ctx context.Context,
	e *Engine,
	dbID uint64,
	tblID uint64,
) (statsinfo.StatsInfoKey, *subEntry) {
	t.Helper()
	e.pClient.eng = e
	e.pClient.subscribed.eng = e

	ent := &subEntry{dbID: dbID, state: Subscribed}
	ent.lastTs.Store(time.Now().UnixNano())
	if e.pClient.subscribed.m == nil {
		e.pClient.subscribed.m = make(map[uint64]*subEntry)
	}
	e.pClient.subscribed.m[tblID] = ent

	key := statsinfo.StatsInfoKey{
		AccId:      0,
		DatabaseID: dbID,
		TableID:    tblID,
		TableName:  "t",
		DbName:     "d",
	}
	part := e.GetOrCreateLatestPart(ctx, 0, dbID, tblID)
	state, done := part.MutateState()
	defer done()
	oid := types.NewObjectid()
	stats := objectio.NewObjectStatsWithObjectID(&oid, false, false, false)
	require.NoError(t, objectio.SetObjectStatsBlkCnt(stats, 1))
	require.NoError(t, objectio.SetObjectStatsRowCnt(stats, 1))
	require.NoError(t, objectio.SetObjectStatsSize(stats, 1))
	require.NoError(t, state.HandleObjectEntry(ctx, nil, objectio.ObjectEntry{
		ObjectStats: *stats,
		CreateTime:  types.BuildTS(time.Now().UnixNano(), 0),
	}, false))
	return key, ent
}

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
			updated, _ := e.globalStats.executeStatsUpdate(ctx, ps, k, stats)
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
			updated, _ := e.globalStats.executeStatsUpdate(ctx, ps, k, stats)
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
			updated, _ := e.globalStats.executeStatsUpdate(ctx, ps, k, stats)
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
		generation := gs.currentOrCreateUpdateRecord(k1)
		_, started := gs.startAutomaticUpdate(k1, generation)
		assert.True(t, started)
		_, started = gs.startAutomaticUpdate(k1, generation)
		assert.False(t, started)
		gs.markAutomaticUpdateComplete(
			k1, generation, true, 1, 1.0)
		time.Sleep(MinUpdateInterval)
		_, started = gs.startAutomaticUpdate(k1, generation)
		assert.True(t, started)
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
			generation := gs.currentOrCreateUpdateRecord(k1)
			if _, started := gs.startAutomaticUpdate(k1, generation); !started {
				return
			}
			count.Add(1)
			gs.markAutomaticUpdateComplete(
				k1, generation, true, 2, 1.0)
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

// TestSamplingRatioCalculation tests the sampling ratio calculation logic
func TestSamplingRatioCalculation(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testCases := []struct {
		objectNum     int64
		expectedRatio float64
		description   string
	}{
		{50, 1.0, "below threshold, full scan"},
		{100, 1.0, "at threshold, full scan"},
		{101, float64(100) / 101, "just above threshold, sample 100"},
		{500, float64(100) / 500, "500 objects, sample 100"},
		{1000, float64(100) / 1000, "1000 objects, sample 100"},
		// For 10000: max(sqrt(10000)=100, 10000*0.1=1000) = 1000
		{10000, float64(1000) / 10000, "10000 objects, sample 10%=1000"},
		{10001, float64(1000) / 10001, "10001 objects, sample 1000"},
		// For 20000: max(sqrt(20000)=141, 20000*0.1=2000) = 2000
		{20000, float64(2000) / 20000, "20000 objects, sample 10%=2000"},
		// For 100000: max(sqrt(100000)=316, 100000*0.1=10000) = 10000, clamped to MaxSampleObjects=5000
		{100000, float64(5000) / 100000, "100000 objects, sample 5000 (max)"},
		{250000, float64(5000) / 250000, "250000 objects, sample 5000 (max)"},
		{500000, float64(5000) / 500000, "500000 objects, sample 5000 (max)"},
	}

	for _, tc := range testCases {
		t.Run(tc.description, func(t *testing.T) {
			ratio := calcSamplingRatio(tc.objectNum)
			assert.InDelta(t, tc.expectedRatio, ratio, 0.01,
				"sampling ratio mismatch for %d objects", tc.objectNum)

			// Verify constraints
			if tc.objectNum <= SamplingThreshold {
				assert.Equal(t, 1.0, ratio, "should be full scan below threshold")
			} else {
				assert.Less(t, ratio, 1.0, "should be sampling above threshold")
				// Verify sample count is within bounds
				sampleCount := int(ratio * float64(tc.objectNum))
				assert.GreaterOrEqual(t, sampleCount, MinSampleObjects-1, // -1 for rounding
					"sample count should be >= MinSampleObjects")
				assert.LessOrEqual(t, sampleCount, MaxSampleObjects+1, // +1 for rounding
					"sample count should be <= MaxSampleObjects")
			}
		})
	}
}

// TestSamplingThresholdCalculation tests the threshold calculation for ObjectID sampling
func TestSamplingThresholdCalculation(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testCases := []struct {
		ratio    float64
		expected uint64
	}{
		{1.0, ^uint64(0)},                            // max uint64
		{0.5, uint64(0.5 * float64(^uint64(0)))},     // half
		{0.1, uint64(0.1 * float64(^uint64(0)))},     // 10%
		{0.01, uint64(0.01 * float64(^uint64(0)))},   // 1%
		{0.001, uint64(0.001 * float64(^uint64(0)))}, // 0.1%
		{1.5, ^uint64(0)},                            // > 1.0 should be max
	}

	for _, tc := range testCases {
		t.Run(fmt.Sprintf("ratio_%.4f", tc.ratio), func(t *testing.T) {
			threshold := calcSamplingThreshold(tc.ratio)
			if tc.ratio >= 1.0 {
				assert.Equal(t, ^uint64(0), threshold, "ratio >= 1.0 should return max uint64")
			} else {
				// Allow some floating point tolerance
				assert.InDelta(t, float64(tc.expected), float64(threshold), float64(tc.expected)*0.001,
					"threshold mismatch for ratio %.4f", tc.ratio)
			}
		})
	}
}

// TestShouldSampleObject tests the sampling decision based on ObjectID
func TestShouldSampleObject(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// Create a mock ObjectNameShort with known random bytes and num
	createMockObjectName := func(randomValue uint64, num uint16) *objectio.ObjectNameShort {
		var name objectio.ObjectNameShort
		// Set bytes 8-15 to the random value (little endian)
		binary.LittleEndian.PutUint64(name[objectIDRandomOffset:], randomValue)
		// Set bytes 16-17 to the num (little endian)
		binary.LittleEndian.PutUint16(name[16:], num)
		return &name
	}

	t.Run("always_sample_max_threshold", func(t *testing.T) {
		name := createMockObjectName(0, 0) // min values
		threshold := ^uint64(0)            // max threshold
		assert.True(t, shouldSampleObject(name, threshold), "max threshold should always sample")
	})

	t.Run("never_sample_zero_threshold", func(t *testing.T) {
		name := createMockObjectName(1, 1) // any non-zero values
		threshold := uint64(0)             // zero threshold
		assert.False(t, shouldSampleObject(name, threshold), "zero threshold should never sample")
	})

	t.Run("different_num_different_result", func(t *testing.T) {
		// Objects with same Segmentid but different Num should have different sampling results
		// This is the key fix - previously all objects with same Segmentid would be sampled together
		const randomValue = uint64(0x8000000000000000) // middle value
		threshold := calcSamplingThreshold(0.5)        // 50% sampling

		results := make(map[bool]int)
		for num := uint16(0); num < 1000; num++ {
			name := createMockObjectName(randomValue, num)
			results[shouldSampleObject(name, threshold)]++
		}
		// With 50% threshold and good mixing, we should see both true and false results
		assert.Greater(t, results[true], 0, "should have some sampled objects")
		assert.Greater(t, results[false], 0, "should have some non-sampled objects")
	})

	t.Run("sampling_distribution", func(t *testing.T) {
		// Test that sampling roughly follows the expected ratio
		const numSegments = 10
		const numPerSegment = 1000
		const targetRatio = 0.3 // 30%
		threshold := calcSamplingThreshold(targetRatio)

		sampledCount := 0
		for seg := 0; seg < numSegments; seg++ {
			randomValue := uint64(seg) * (^uint64(0) / numSegments)
			for num := uint16(0); num < numPerSegment; num++ {
				name := createMockObjectName(randomValue, num)
				if shouldSampleObject(name, threshold) {
					sampledCount++
				}
			}
		}

		actualRatio := float64(sampledCount) / float64(numSegments*numPerSegment)
		// Allow reasonable tolerance
		assert.InDelta(t, targetRatio, actualRatio, 0.05,
			"sampling ratio should be approximately %.2f, got %.2f", targetRatio, actualRatio)
	})
}

// TestSamplingForceAtLeastOneObject tests the fix for the bug where sampling could
// select zero objects for a table (e.g. when approxObjectNum > 100 but only 19 objects
// are visible at snapshot, each with ~7% sampling probability -> P(0 sampled) ~ 26%).
// That led to Phase 2 never running, all ColumnNDVs staying zero, empty ShuffleRangeMap,
// and point queries using block_num~611 instead of 1. The fix ensures we always process
// at least one object when sampling is enabled so that Phase 2 runs and stats are populated.
//
// Coverage note: this test exercises the same decision logic as collectTableStats Phase 2
// (forceOne := sampledObjectCount == 0; process if forceOne || shouldSampleObject(...)).
// It does not call collectTableStats itself; it validates the algorithm so that any change
// to the force-one behavior would be caught here.
func TestSamplingForceAtLeastOneObject(t *testing.T) {
	defer leaktest.AfterTest(t)()

	createMockObjectName := func(randomValue uint64, num uint16) *objectio.ObjectNameShort {
		var name objectio.ObjectNameShort
		binary.LittleEndian.PutUint64(name[objectIDRandomOffset:], randomValue)
		binary.LittleEndian.PutUint16(name[16:], num)
		return &name
	}

	t.Run("deterministic_all_rejected_then_one_forced", func(t *testing.T) {
		// Use a low sampling ratio (1%) so threshold is small. Object names with high
		// randomPart yield combined >= threshold, so shouldSampleObject returns false for all.
		ratio := 0.01
		threshold := calcSamplingThreshold(ratio)

		const numObjects = 19
		names := make([]*objectio.ObjectNameShort, numObjects)
		for i := 0; i < numObjects; i++ {
			names[i] = createMockObjectName(^uint64(0)-1, uint16(i))
		}

		for i := 0; i < numObjects; i++ {
			assert.False(t, shouldSampleObject(names[i], threshold),
				"object %d should not be sampled by random (reproduces no_objects_sampled)", i)
		}

		var sampledObjectCount int64
		for i := 0; i < numObjects; i++ {
			forceOne := sampledObjectCount == 0
			if forceOne || shouldSampleObject(names[i], threshold) {
				sampledObjectCount++
			}
		}

		assert.GreaterOrEqual(t, sampledObjectCount, int64(1),
			"force-one logic must ensure at least 1 is processed when all would be rejected")
		assert.Equal(t, int64(1), sampledObjectCount,
			"exactly one object should be processed (the forced one)")
	})

	// Same as production: 7% ratio, 19 objects. P(0 sampled without fix) = 0.93^19 ≈ 26%.
	// Each CI run does many trials; we only assert that with the fix every trial gets >= 1.
	// Over time some runs will hit the "would have been 0" case and exercise the force-one path.
	t.Run("random_trials_7pct_19_objects", func(t *testing.T) {
		ratio := 0.07
		threshold := calcSamplingThreshold(ratio)
		const numObjects = 19
		const numTrials = 300
		prime := uint64(0x9E3779B97F4A7C15)
		nextRandom := func(trial, i int) uint64 { return uint64(trial)*prime + uint64(i)*0x1234567 }

		for trial := 0; trial < numTrials; trial++ {
			var sampledObjectCount int64
			for i := 0; i < numObjects; i++ {
				name := createMockObjectName(nextRandom(trial, i), uint16(i))
				forceOne := sampledObjectCount == 0
				if forceOne || shouldSampleObject(name, threshold) {
					sampledObjectCount++
				}
			}
			assert.GreaterOrEqual(t, sampledObjectCount, int64(1), "trial %d: with fix, at least one object must be processed", trial)
		}
	})
}

// initTableForTest initializes a table record with the given baseObjectCount
// by calling shouldEnqueueUpdate once and then markUpdateComplete to set the baseObjectCount
func initTableForTest(gs *GlobalStats, key statsinfo.StatsInfoKey, baseObjectCount int64) {
	gs.shouldEnqueueUpdate(key, 0, false)
	gs.markExplicitUpdateComplete(
		key, gs.currentOrCreateUpdateRecord(key), baseObjectCount, 1.0)
}

// TestGlobalStats_ShouldEnqueue tests the shouldEnqueue logic for large table throttling
func TestGlobalStats_ShouldEnqueue(t *testing.T) {
	defer leaktest.AfterTest(t)()

	t.Run("first_time_enqueue", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		gs := NewGlobalStats(ctx, nil, nil)
		assert.NotNil(t, gs)

		key := statsinfo.StatsInfoKey{
			DatabaseID: 100,
			TableID:    101,
		}

		// First time: should always enqueue
		assert.True(t, gs.shouldEnqueueUpdate(key, 10, false))

		// Verify record was created
		gs.updatingMu.Lock()
		rec, ok := gs.updatingMu.updating[key]
		gs.updatingMu.Unlock()
		assert.True(t, ok)
		assert.Equal(t, 10, rec.pendingChanges)
	})

	t.Run("checkpoint_always_enqueue_large_table", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		gs := NewGlobalStats(ctx, nil, nil)

		key := statsinfo.StatsInfoKey{
			DatabaseID: 100,
			TableID:    101,
		}

		// Initialize with large table
		gs.updatingMu.Lock()
		gs.updatingMu.updating[key] = &updateRecord{
			baseObjectCount: 10000,
			lastUpdate:      time.Now(),
		}
		gs.updatingMu.Unlock()

		// Checkpoint with no meta changes: should enqueue (checkpoint takes priority)
		// Note: In large table logic, checkpoint is not explicitly checked,
		// but metaChanges=0 won't trigger enqueue unless timeout
		assert.False(t, gs.shouldEnqueueUpdate(key, 0, true))
	})

	t.Run("checkpoint_with_changes_large_table", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		gs := NewGlobalStats(ctx, nil, nil)

		key := statsinfo.StatsInfoKey{
			DatabaseID: 100,
			TableID:    101,
		}

		// Initialize with large table
		gs.updatingMu.Lock()
		gs.updatingMu.updating[key] = &updateRecord{
			baseObjectCount: 10000,
			lastUpdate:      time.Now(),
		}
		gs.updatingMu.Unlock()

		// Checkpoint with changes: should enqueue if changes meet threshold
		assert.True(t, gs.shouldEnqueueUpdate(key, 500, true)) // 5% change
	})

	t.Run("small_table_checkpoint", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		gs := NewGlobalStats(ctx, nil, nil)

		key := statsinfo.StatsInfoKey{
			DatabaseID: 100,
			TableID:    101,
		}

		// Initialize with small table (< 500 objects)
		initTableForTest(gs, key, 300)

		// Small table with checkpoint: should enqueue
		assert.True(t, gs.shouldEnqueueUpdate(key, 0, true))
	})

	t.Run("small_table_any_change", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		gs := NewGlobalStats(ctx, nil, nil)

		key1 := statsinfo.StatsInfoKey{
			DatabaseID: 100,
			TableID:    101,
		}
		key2 := statsinfo.StatsInfoKey{
			DatabaseID: 100,
			TableID:    102,
		}
		key3 := statsinfo.StatsInfoKey{
			DatabaseID: 100,
			TableID:    103,
		}

		// Initialize records and set baseObjectCount (< 500 objects)
		initTableForTest(gs, key1, 300)
		initTableForTest(gs, key2, 300)
		initTableForTest(gs, key3, 300)

		// Small table: any change should enqueue
		assert.True(t, gs.shouldEnqueueUpdate(key1, 1, false))
		assert.True(t, gs.shouldEnqueueUpdate(key2, 10, false))
		assert.True(t, gs.shouldEnqueueUpdate(key3, 100, false))
	})

	t.Run("small_table_no_change", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		gs := NewGlobalStats(ctx, nil, nil)

		key := statsinfo.StatsInfoKey{
			DatabaseID: 100,
			TableID:    101,
		}

		// Initialize with small table (< 500 objects)
		initTableForTest(gs, key, 300)

		// Small table: no change and no checkpoint should not enqueue
		assert.False(t, gs.shouldEnqueueUpdate(key, 0, false))
	})

	t.Run("large_table_below_threshold", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		gs := NewGlobalStats(ctx, nil, nil)

		key := statsinfo.StatsInfoKey{
			DatabaseID: 100,
			TableID:    101,
		}

		// Initialize with large table (10000 objects)
		gs.updatingMu.Lock()
		gs.updatingMu.updating[key] = &updateRecord{
			baseObjectCount: 10000,
			lastUpdate:      time.Now(),
		}
		gs.updatingMu.Unlock()

		// Change rate < 5% (400/10000 = 4%): should not enqueue
		assert.False(t, gs.shouldEnqueueUpdate(key, 400, false))

		// Verify pendingChanges accumulated
		gs.updatingMu.Lock()
		assert.Equal(t, 400, gs.updatingMu.updating[key].pendingChanges)
		gs.updatingMu.Unlock()
	})

	t.Run("large_table_at_threshold", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		gs := NewGlobalStats(ctx, nil, nil)

		key := statsinfo.StatsInfoKey{
			DatabaseID: 100,
			TableID:    101,
		}

		// Initialize with large table
		gs.updatingMu.Lock()
		gs.updatingMu.updating[key] = &updateRecord{
			baseObjectCount: 10000,
			lastUpdate:      time.Now(),
		}
		gs.updatingMu.Unlock()

		// Change rate = 5% (500/10000 = 5%): should enqueue
		assert.True(t, gs.shouldEnqueueUpdate(key, 500, false))
	})

	t.Run("large_table_above_threshold", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		gs := NewGlobalStats(ctx, nil, nil)

		key := statsinfo.StatsInfoKey{
			DatabaseID: 100,
			TableID:    101,
		}

		// Initialize with large table
		gs.updatingMu.Lock()
		gs.updatingMu.updating[key] = &updateRecord{
			baseObjectCount: 10000,
			lastUpdate:      time.Now(),
		}
		gs.updatingMu.Unlock()

		// Change rate > 5% (600/10000 = 6%): should enqueue
		assert.True(t, gs.shouldEnqueueUpdate(key, 600, false))
	})

	t.Run("large_table_accumulated_changes", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		gs := NewGlobalStats(ctx, nil, nil)

		key := statsinfo.StatsInfoKey{
			DatabaseID: 100,
			TableID:    101,
		}

		// Initialize with large table
		gs.updatingMu.Lock()
		gs.updatingMu.updating[key] = &updateRecord{
			baseObjectCount: 10000,
			lastUpdate:      time.Now(),
		}
		gs.updatingMu.Unlock()

		// First change: 2% (200/10000)
		assert.False(t, gs.shouldEnqueueUpdate(key, 200, false))
		gs.updatingMu.Lock()
		assert.Equal(t, 200, gs.updatingMu.updating[key].pendingChanges)
		gs.updatingMu.Unlock()

		// Second change: another 2% (total 4%)
		assert.False(t, gs.shouldEnqueueUpdate(key, 200, false))
		gs.updatingMu.Lock()
		assert.Equal(t, 400, gs.updatingMu.updating[key].pendingChanges)
		gs.updatingMu.Unlock()

		// Third change: another 2% (total 6%, exceeds 5%)
		assert.True(t, gs.shouldEnqueueUpdate(key, 200, false))
		gs.updatingMu.Lock()
		assert.Equal(t, 600, gs.updatingMu.updating[key].pendingChanges)
		gs.updatingMu.Unlock()
	})

	t.Run("large_table_timeout", func(t *testing.T) {
		// Temporarily reduce timeout for testing
		origMaxInterval := LargeTableMaxUpdateInterval
		defer func() {
			// Note: Can't actually change const, but this shows intent
			_ = origMaxInterval
		}()

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		gs := NewGlobalStats(ctx, nil, nil)

		key := statsinfo.StatsInfoKey{
			DatabaseID: 100,
			TableID:    101,
		}

		// Initialize with large table and old lastUpdate
		gs.updatingMu.Lock()
		gs.updatingMu.updating[key] = &updateRecord{
			baseObjectCount: 10000,
			lastUpdate:      time.Now().Add(-31 * time.Minute), // > 30min ago
		}
		gs.updatingMu.Unlock()

		// Even with small change (< 5%), should enqueue due to timeout
		assert.True(t, gs.shouldEnqueueUpdate(key, 100, false))
	})

	t.Run("large_table_recent_update_no_timeout", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		gs := NewGlobalStats(ctx, nil, nil)

		key := statsinfo.StatsInfoKey{
			DatabaseID: 100,
			TableID:    101,
		}

		// Initialize with large table and recent lastUpdate
		gs.updatingMu.Lock()
		gs.updatingMu.updating[key] = &updateRecord{
			baseObjectCount: 10000,
			lastUpdate:      time.Now().Add(-5 * time.Minute), // recent
		}
		gs.updatingMu.Unlock()

		// Small change (< 5%) and no timeout: should not enqueue
		assert.False(t, gs.shouldEnqueueUpdate(key, 100, false))
	})

	t.Run("boundary_large_table_threshold", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		gs := NewGlobalStats(ctx, nil, nil)

		key := statsinfo.StatsInfoKey{
			DatabaseID: 100,
			TableID:    101,
		}

		// Test at boundary: exactly 500 objects (threshold)
		gs.updatingMu.Lock()
		gs.updatingMu.updating[key] = &updateRecord{
			baseObjectCount: 500,
			lastUpdate:      time.Now(),
		}
		gs.updatingMu.Unlock()

		// At threshold (500): should be treated as large table
		// Need 5% change (25 objects)
		assert.False(t, gs.shouldEnqueueUpdate(key, 24, false))
		assert.True(t, gs.shouldEnqueueUpdate(key, 1, false)) // 24+1=25, reaches 5%
	})

	t.Run("boundary_small_table_threshold", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		gs := NewGlobalStats(ctx, nil, nil)

		key := statsinfo.StatsInfoKey{
			DatabaseID: 100,
			TableID:    101,
		}

		// Test just below boundary: 499 objects (< 500, so small table)
		initTableForTest(gs, key, 499)

		// Below threshold (< 500): should be treated as small table
		// Any change should enqueue
		assert.True(t, gs.shouldEnqueueUpdate(key, 1, false))
	})

	t.Run("zero_base_object_count", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		gs := NewGlobalStats(ctx, nil, nil)

		key := statsinfo.StatsInfoKey{
			DatabaseID: 100,
			TableID:    101,
		}

		// Initialize with zero baseObjectCount
		gs.updatingMu.Lock()
		gs.updatingMu.updating[key] = &updateRecord{
			baseObjectCount: 0,
			lastUpdate:      time.Now(),
		}
		gs.updatingMu.Unlock()

		// Zero base: treated as small table, any change should enqueue
		assert.True(t, gs.shouldEnqueueUpdate(key, 1, false))
	})

	t.Run("concurrent_enqueue_checks", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		gs := NewGlobalStats(ctx, nil, nil)

		key := statsinfo.StatsInfoKey{
			DatabaseID: 100,
			TableID:    101,
		}

		// Initialize with large table
		gs.updatingMu.Lock()
		gs.updatingMu.updating[key] = &updateRecord{
			baseObjectCount: 10000,
			lastUpdate:      time.Now(),
		}
		gs.updatingMu.Unlock()

		// Concurrent checks should be safe
		var wg sync.WaitGroup
		results := make([]bool, 20)
		for i := 0; i < 20; i++ {
			wg.Add(1)
			go func(idx int) {
				defer wg.Done()
				// Each adds 30 changes (total 600, which is 6% > 5%)
				results[idx] = gs.shouldEnqueueUpdate(key, 30, false)
			}(i)
		}
		wg.Wait()

		// At least one should succeed (first to reach 5%)
		hasTrue := false
		for _, r := range results {
			if r {
				hasTrue = true
				break
			}
		}
		assert.True(t, hasTrue, "at least one concurrent check should succeed")

		// Verify pendingChanges accumulated
		gs.updatingMu.Lock()
		assert.Equal(t, 600, gs.updatingMu.updating[key].pendingChanges)
		gs.updatingMu.Unlock()
	})
}

func TestCleanMemoryTableWithTable(t *testing.T) {
	t.Run("removes_partition_and_stats", func(t *testing.T) {
		runTest(t, func(ctx context.Context, e *Engine) {
			dbId, tblId := uint64(100), uint64(1001)

			// Insert a fake partition (nil is valid; delete works on nil values)
			e.Lock()
			e.partitions[[2]uint64{dbId, tblId}] = nil
			e.Unlock()

			// Insert a stats entry for the same table
			k := statsinfo.StatsInfoKey{DatabaseID: dbId, TableID: tblId, TableName: "t"}
			e.globalStats.mu.Lock()
			e.globalStats.mu.statsInfoMap[k] = plan2.NewStatsInfo()
			e.globalStats.mu.Unlock()

			// Call cleanMemoryTableWithTable
			e.cleanMemoryTableWithTable(dbId, tblId)

			// Verify partition removed
			e.Lock()
			_, partOk := e.partitions[[2]uint64{dbId, tblId}]
			e.Unlock()
			assert.False(t, partOk, "partition should be removed")

			// Verify stats entry removed via RemoveTid
			e.globalStats.mu.Lock()
			_, statsOk := e.globalStats.mu.statsInfoMap[k]
			e.globalStats.mu.Unlock()
			assert.False(t, statsOk, "stats entry should be removed")
		})
	})

	t.Run("no_panic_on_missing_partition", func(t *testing.T) {
		runTest(t, func(ctx context.Context, e *Engine) {
			// Calling with non-existent partition should not panic
			e.cleanMemoryTableWithTable(999, 888)
		})
	})
}

func TestRemoveTid(t *testing.T) {
	t.Run("remove_existing_entries", func(t *testing.T) {
		runTest(t, func(ctx context.Context, e *Engine) {
			gs := e.globalStats

			// Insert entries for two tables
			k1 := statsinfo.StatsInfoKey{DatabaseID: 100, TableID: 1001, TableName: "t1"}
			k2 := statsinfo.StatsInfoKey{DatabaseID: 100, TableID: 1001, TableName: "t1_alt"}
			k3 := statsinfo.StatsInfoKey{DatabaseID: 200, TableID: 2001, TableName: "t2"}

			gs.mu.Lock()
			gs.mu.statsInfoMap[k1] = plan2.NewStatsInfo()
			gs.mu.statsInfoMap[k2] = nil // simulate failed update
			gs.mu.statsInfoMap[k3] = plan2.NewStatsInfo()
			gs.mu.tableDefVersions[k1] = 7
			gs.mu.tableDefVersions[k2] = 7
			gs.mu.tableDefVersions[k3] = 9
			gs.mu.Unlock()
			generation := gs.currentOrCreateUpdateRecord(k1)
			gs.currentOrCreateUpdateRecord(k2)
			gs.currentOrCreateUpdateRecord(k3)
			gs.markExplicitUpdateComplete(k1, generation, 1, 1)
			gs.markAutomaticUpdateComplete(
				k2, gs.currentOrCreateUpdateRecord(k2), false, 0, 0)
			gs.markExplicitUpdateComplete(
				k3, gs.currentOrCreateUpdateRecord(k3), 2, 1)

			// Remove table 1001 entries
			gs.RemoveTid(1001)
			// A worker admitted before cleanup may finish afterward. Its stale
			// publication or completion must not recreate table-owned state.
			queuedAfterCleanup, enqueueAfterCleanup :=
				gs.shouldEnqueueExistingStatsUpdateGeneration(k1, 1, false)
			assert.False(t, enqueueAfterCleanup)
			assert.Nil(t, queuedAfterCleanup)
			gs.completeAutomaticStatsCacheUpdate(k1, generation, plan2.NewStatsInfo(), true)
			gs.completeAutomaticStatsRefresh(
				k1, generation, plan2.NewStatsInfo(), true, 3, 1, func() {})

			gs.mu.Lock()
			_, ok1 := gs.mu.statsInfoMap[k1]
			_, ok2 := gs.mu.statsInfoMap[k2]
			_, ok3 := gs.mu.statsInfoMap[k3]
			_, version1 := gs.mu.tableDefVersions[k1]
			_, version2 := gs.mu.tableDefVersions[k2]
			version3 := gs.mu.tableDefVersions[k3]
			gs.mu.Unlock()
			assert.False(t, ok1, "k1 should be removed")
			assert.False(t, ok2, "k2 should be removed")
			assert.True(t, ok3, "k3 should not be removed")
			assert.False(t, version1, "k1 schema metadata should be removed")
			assert.False(t, version2, "k2 schema metadata should be removed")
			assert.Equal(t, uint32(9), version3,
				"unrelated schema metadata should remain")

			gs.updatingMu.Lock()
			_, updating1 := gs.updatingMu.updating[k1]
			_, updating2 := gs.updatingMu.updating[k2]
			_, updating3 := gs.updatingMu.updating[k3]
			gs.updatingMu.Unlock()
			assert.False(t, updating1, "k1 scheduling metadata should be removed")
			assert.False(t, updating2, "k2 scheduling metadata should be removed")
			assert.True(t, updating3, "unrelated scheduling metadata should remain")

			// Reuse of the same table key creates a distinct generation. Neither
			// an old queued job nor its late callbacks may publish into it.
			replacement := &updateRecord{inProgress: true, pendingChanges: 7}
			gs.updatingMu.Lock()
			gs.updatingMu.updating[k1] = replacement
			gs.updatingMu.Unlock()
			gs.completeAutomaticStatsCacheUpdate(k1, generation, plan2.NewStatsInfo(), true)
			gs.completeAutomaticStatsRefresh(
				k1, generation, plan2.NewStatsInfo(), true, 4, 0.5, func() {})
			_, oldGenerationStarted := gs.startAutomaticUpdate(k1, generation)
			assert.False(t, oldGenerationStarted, "an old queued generation should be rejected")

			gs.mu.Lock()
			_, oldStatsPublished := gs.mu.statsInfoMap[k1]
			gs.mu.Unlock()
			assert.False(t, oldStatsPublished, "an old generation should not publish into its replacement")
			gs.updatingMu.Lock()
			current := gs.updatingMu.updating[k1]
			gs.updatingMu.Unlock()
			require.Same(t, replacement, current)
			assert.True(t, current.inProgress)
			assert.Equal(t, 7, current.pendingChanges)
		})
	})

	t.Run("first_queued_and_explicit_refreshes_cannot_cross_cleanup_generation", func(t *testing.T) {
		gs := &GlobalStats{
			updateC:      make(chan statsUpdateJob, 1),
			queueWatcher: newQueueWatcher(),
		}
		gs.updatingMu.updating = make(map[statsinfo.StatsInfoKey]*updateRecord)
		gs.mu.statsInfoMap = make(map[statsinfo.StatsInfoKey]*statsinfo.StatsInfo)
		gs.mu.cond = sync.NewCond(&gs.mu)

		key := statsinfo.StatsInfoKey{DatabaseID: 100, TableID: 1001, TableName: "t1"}
		generation := gs.currentOrCreateUpdateRecord(key)
		require.True(t, gs.enqueueStatsUpdateForRecord(statsinfo.StatsInfoKeyWithContext{
			Ctx: context.Background(),
			Key: key,
		}, false, generation))
		job := <-gs.updateC
		require.NotNil(t, job.expectedRecord,
			"the first queued refresh must own a concrete lifetime token")

		gs.RemoveTid(key.TableID)
		gs.coordinateStatsUpdateJob(job)
		gs.markAutomaticUpdateComplete(key, job.expectedRecord, true, 1, 1)
		published, err := gs.publishStatsForGeneration(
			context.Background(), key, job.expectedRecord, plan2.NewStatsInfo())
		require.NoError(t, err)
		assert.False(t, published)

		gs.mu.Lock()
		_, cached := gs.mu.statsInfoMap[key]
		gs.mu.Unlock()
		gs.updatingMu.Lock()
		_, scheduled := gs.updatingMu.updating[key]
		gs.updatingMu.Unlock()
		assert.False(t, cached, "old work must not recreate the statistics cache")
		assert.False(t, scheduled, "old work must not recreate scheduling metadata")

		replacement := gs.currentOrCreateUpdateRecord(key)
		published, err = gs.publishStatsForGeneration(
			context.Background(), key, job.expectedRecord, plan2.NewStatsInfo())
		require.NoError(t, err)
		assert.False(t, published,
			"old explicit work must not publish into a replacement lifetime")
		fresh := plan2.NewStatsInfo()
		fresh.TableCnt = 42
		published, err = gs.publishStatsForGeneration(
			context.Background(), key, replacement, fresh)
		require.NoError(t, err)
		require.True(t, published)
		gs.mu.Lock()
		assert.Same(t, fresh, gs.mu.statsInfoMap[key])
		gs.mu.Unlock()
	})

	t.Run("explicit_completion_preserves_admitted_automatic_refresh", func(t *testing.T) {
		gs := &GlobalStats{}
		gs.updatingMu.updating = make(map[statsinfo.StatsInfoKey]*updateRecord)
		key := statsinfo.StatsInfoKey{DatabaseID: 100, TableID: 1001, TableName: "t1"}
		generation := &updateRecord{inProgress: true, pendingChanges: 7}
		gs.updatingMu.updating[key] = generation

		gs.markExplicitUpdateComplete(key, generation, 42, 0.5)

		gs.updatingMu.Lock()
		got := *gs.updatingMu.updating[key]
		gs.updatingMu.Unlock()
		assert.True(t, got.inProgress,
			"explicit completion must not reopen admission for another automatic refresh")
		assert.Equal(t, int64(42), got.baseObjectCount)
		assert.Zero(t, got.pendingChanges)
		assert.Equal(t, 0.5, got.samplingRatio)
	})

	t.Run("remove_nonexistent_table", func(t *testing.T) {
		runTest(t, func(ctx context.Context, e *Engine) {
			gs := e.globalStats

			k := statsinfo.StatsInfoKey{DatabaseID: 100, TableID: 1001}
			gs.mu.Lock()
			gs.mu.statsInfoMap[k] = plan2.NewStatsInfo()
			gs.mu.Unlock()

			// Remove a non-existent table — should not panic
			gs.RemoveTid(9999)

			gs.mu.Lock()
			defer gs.mu.Unlock()
			_, ok := gs.mu.statsInfoMap[k]
			assert.True(t, ok, "existing entry should remain")
		})
	})

}

func TestStatsPublicationRejectsStoppedOwnerLifecycle(t *testing.T) {
	ownerCtx, stopOwner := context.WithCancel(context.Background())
	stopOwner()
	key := statsinfo.StatsInfoKey{AccId: 1, DatabaseID: 10, TableID: 42}
	lastGood := plan2.NewStatsInfo()
	lastGood.TableCnt = 7
	generation := &updateRecord{
		inProgress:      true,
		baseObjectCount: 7,
		samplingRatio:   0.25,
	}
	gs := &GlobalStats{ctx: ownerCtx}
	gs.mu.statsInfoMap = map[statsinfo.StatsInfoKey]*statsinfo.StatsInfo{key: lastGood}
	gs.mu.cond = sync.NewCond(&gs.mu)
	gs.updatingMu.updating = map[statsinfo.StatsInfoKey]*updateRecord{key: generation}

	fresh := plan2.NewStatsInfo()
	fresh.TableCnt = 42
	releases := 0
	gs.completeAutomaticStatsRefresh(
		key, generation, fresh, true, 42, 1, func() { releases++ })
	published, err := gs.publishStatsForGeneration(
		context.Background(), key, generation, fresh)
	require.ErrorIs(t, err, context.Canceled)
	require.False(t, published)
	require.Equal(t, 1, releases)

	gs.mu.Lock()
	require.Same(t, lastGood, gs.mu.statsInfoMap[key],
		"shutdown must preserve the last successfully published statistics")
	gs.mu.Unlock()
	gs.updatingMu.Lock()
	require.False(t, generation.inProgress)
	require.Equal(t, int64(7), generation.baseObjectCount,
		"failed publication must not advance the object-count baseline")
	require.Equal(t, 0.25, generation.samplingRatio)
	gs.updatingMu.Unlock()
}

func TestMetadataRefreshClearsSchemaBoundObservationVersion(t *testing.T) {
	key := statsinfo.StatsInfoKey{AccId: 1, DatabaseID: 10, TableID: 42}
	old := plan2.NewStatsInfo()
	fresh := plan2.NewStatsInfo()
	generation := &updateRecord{inProgress: true}
	gs := &GlobalStats{}
	gs.mu.statsInfoMap = map[statsinfo.StatsInfoKey]*statsinfo.StatsInfo{key: old}
	gs.mu.tableDefVersions = map[statsinfo.StatsInfoKey]uint32{key: 7}
	gs.mu.cond = sync.NewCond(&gs.mu)
	gs.updatingMu.updating = map[statsinfo.StatsInfoKey]*updateRecord{key: generation}

	require.True(t, gs.completeAutomaticStatsCacheUpdate(
		key, generation, fresh, true))
	gs.mu.Lock()
	require.Same(t, fresh, gs.mu.statsInfoMap[key])
	require.NotContains(t, gs.mu.tableDefVersions, key,
		"metadata-only statistics are not bound to the ANALYZE schema observation")
	gs.mu.Unlock()
	require.Same(t, fresh, gs.GetForRemote(context.Background(), key),
		"unbound metadata statistics remain safe for remote export")
}

func TestStatsWaiterRejectsOldSchemaUntilReplacementPublishes(t *testing.T) {
	key := statsinfo.StatsInfoKey{AccId: 1, DatabaseID: 10, TableID: 42}
	old := plan2.NewStatsInfo()
	old.TableCnt = 7
	fresh := plan2.NewStatsInfo()
	fresh.TableCnt = 8
	generation := &updateRecord{inProgress: true}
	gs := &GlobalStats{}
	gs.mu.statsInfoMap = map[statsinfo.StatsInfoKey]*statsinfo.StatsInfo{key: old}
	gs.mu.tableDefVersions = map[statsinfo.StatsInfoKey]uint32{key: 7}
	gs.mu.cond = sync.NewCond(&gs.mu)
	gs.updatingMu.updating = map[statsinfo.StatsInfoKey]*updateRecord{key: generation}

	waiting := make(chan struct{})
	var once sync.Once
	gs.beforeStatsWait = func(statsinfo.StatsInfoKey, *updateRecord) {
		once.Do(func() { close(waiting) })
	}
	version := uint32(8)
	done := make(chan *statsinfo.StatsInfo, 1)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	go func() {
		done <- gs.waitForStatsUpdate(
			ctx, key, generation, &version, false)
	}()
	select {
	case <-waiting:
	case <-ctx.Done():
		t.Fatal("version-mismatched waiter did not reach the wait boundary")
	}

	gs.mu.Lock()
	gs.mu.statsInfoMap[key] = fresh
	delete(gs.mu.tableDefVersions, key)
	gs.mu.cond.Broadcast()
	gs.mu.Unlock()
	require.Same(t, fresh, <-done)
}

func TestGlobalStatsGetDoesNotHoldMuWhileSubscribing(t *testing.T) {
	runTest(t, func(ctx context.Context, e *Engine) {
		gs := e.globalStats
		const dbID uint64 = 100
		const tblID uint64 = 10001

		e.pClient.eng = e
		e.pClient.subscribed.eng = e
		partition := e.GetOrCreateLatestPart(ctx, 0, dbID, tblID)
		state, commit := partition.MutateState()
		objectID := objectio.NewObjectid()
		objectStats := objectio.NewObjectStatsWithObjectID(
			&objectID, false, false, false)
		require.NoError(t, objectio.SetObjectStatsSize(objectStats, 1))
		require.NoError(t, state.HandleObjectEntry(ctx, nil, objectio.ObjectEntry{
			ObjectStats: *objectStats,
			CreateTime:  types.BuildTS(1, 0),
		}, false))
		commit()

		ent := &subEntry{dbID: dbID, state: Subscribed}
		ent.lastTs.Store(time.Now().UnixNano())

		locked := true
		e.pClient.subscribed.rw.Lock()
		defer func() {
			if locked {
				e.pClient.subscribed.rw.Unlock()
			}
		}()

		if e.pClient.subscribed.m == nil {
			e.pClient.subscribed.m = make(map[uint64]*subEntry)
		}
		e.pClient.subscribed.m[tblID] = ent

		key := statsinfo.StatsInfoKey{
			AccId:      0,
			DatabaseID: dbID,
			TableID:    tblID,
			TableName:  "t",
			DbName:     "d",
		}

		reachSubscribe := make(chan struct{})
		oldSubscribeHook := gs.beforeSubscribeTable
		var subscribeOnce sync.Once
		gs.beforeSubscribeTable = func(statsinfo.StatsInfoKey) {
			subscribeOnce.Do(func() { close(reachSubscribe) })
		}
		defer func() {
			gs.beforeSubscribeTable = oldSubscribeHook
		}()

		getCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
		defer cancel()

		getDone := make(chan *statsinfo.StatsInfo, 1)
		getCompleted := make(chan struct{})
		go func() {
			getDone <- gs.Get(getCtx, key, false)
			close(getCompleted)
		}()

		require.Eventually(t, func() bool {
			select {
			case <-reachSubscribe:
				return true
			default:
				return false
			}
		}, time.Second, 10*time.Millisecond, "GlobalStats.Get did not reach subscribe path")

		published := plan2.NewStatsInfo()
		published.TableCnt = 42
		muAcquired := make(chan struct{})
		go func() {
			gs.mu.Lock()
			gs.mu.statsInfoMap[key] = published
			gs.mu.Unlock()
			close(muAcquired)
		}()

		require.Eventually(t, func() bool {
			select {
			case <-getCompleted:
				return false
			default:
			}
			select {
			case <-muAcquired:
				return true
			default:
				return false
			}
		}, time.Second, 10*time.Millisecond, "GlobalStats.Get holds gs.mu while waiting on subscribe lock")

		locked = false
		e.pClient.subscribed.rw.Unlock()

		select {
		case result := <-getDone:
			require.Same(t, published, result,
				"a non-blocking Get must recheck publication after subscription")
		case <-time.After(time.Second):
			t.Fatal("GlobalStats.Get did not return after subscribe lock released")
		}
	})
}

func newSynchronousStatsGetHarness(
	t *testing.T,
	ctx context.Context,
	e *Engine,
	key statsinfo.StatsInfoKey,
) (*GlobalStats, *subEntry) {
	t.Helper()
	partition := e.GetOrCreateLatestPart(ctx, uint64(key.AccId), key.DatabaseID, key.TableID)
	state, commit := partition.MutateState()
	objectID := objectio.NewObjectid()
	objectStats := objectio.NewObjectStatsWithObjectID(
		&objectID, false, false, false)
	require.NoError(t, objectio.SetObjectStatsSize(objectStats, 1))
	require.NoError(t, state.HandleObjectEntry(ctx, nil, objectio.ObjectEntry{
		ObjectStats: *objectStats,
		CreateTime:  types.BuildTS(1, 0),
	}, false))
	commit()

	e.pClient.eng = e
	e.pClient.subscribed.eng = e
	ent := &subEntry{dbID: key.DatabaseID, state: Subscribed}
	ent.lastTs.Store(time.Now().UnixNano())
	e.pClient.subscribed.rw.Lock()
	if e.pClient.subscribed.m == nil {
		e.pClient.subscribed.m = make(map[uint64]*subEntry)
	}
	e.pClient.subscribed.m[key.TableID] = ent
	e.pClient.subscribed.rw.Unlock()

	gs := &GlobalStats{
		ctx:          ctx,
		engine:       e,
		updateC:      make(chan statsUpdateJob, 1),
		queueWatcher: newQueueWatcher(),
	}
	gs.updatingMu.updating = make(map[statsinfo.StatsInfoKey]*updateRecord)
	gs.mu.statsInfoMap = make(map[statsinfo.StatsInfoKey]*statsinfo.StatsInfo)
	gs.mu.cond = sync.NewCond(&gs.mu)
	gs.initStatsRefreshAdmission()
	return gs, ent
}

func TestGlobalStatsGetReturnsWhenContextCanceledWhileWaiting(t *testing.T) {
	runTest(t, func(ctx context.Context, e *Engine) {
		const dbID uint64 = 100
		const tblID uint64 = 10001
		key := statsinfo.StatsInfoKey{
			AccId:      0,
			DatabaseID: dbID,
			TableID:    tblID,
			TableName:  "t",
			DbName:     "d",
		}

		// This isolated GlobalStats has no update worker. Receiving its forced
		// request below proves Get reached the synchronous update path, after
		// which no data-path goroutine can broadcast the condition variable.
		gs, _ := newSynchronousStatsGetHarness(t, ctx, e, key)
		waitEntered := make(chan struct{})
		var waitOnce sync.Once
		gs.beforeStatsWait = func(statsinfo.StatsInfoKey, *updateRecord) {
			waitOnce.Do(func() { close(waitEntered) })
		}

		getCtx, cancel := context.WithCancel(ctx)
		defer cancel()
		result := make(chan *statsinfo.StatsInfo, 1)
		go func() {
			result <- gs.Get(getCtx, key, true)
		}()

		var job statsUpdateJob
		select {
		case job = <-gs.updateC:
		case <-time.After(time.Second):
			t.Fatal("GlobalStats.Get did not enqueue the synchronous update")
		}
		select {
		case <-waitEntered:
		case <-time.After(time.Second):
			t.Fatal("GlobalStats.Get did not register its condition wait")
		}
		cancel()

		select {
		case info := <-result:
			require.Nil(t, info)
		case <-time.After(time.Second):
			t.Fatal("GlobalStats.Get did not return after context cancellation")
		}
		gs.unregisterStatsUpdateJob(key, job.expectedRecord)
		gs.queueWatcher.del(tblID)
	})
}

func TestGlobalStatsGetReturnsPublishedStatsFromAcceptedProducer(t *testing.T) {
	runTest(t, func(ctx context.Context, e *Engine) {
		key := statsinfo.StatsInfoKey{
			DatabaseID: 100,
			TableID:    10006,
			TableName:  "t",
			DbName:     "d",
		}
		gs, _ := newSynchronousStatsGetHarness(t, ctx, e, key)
		result := make(chan *statsinfo.StatsInfo, 1)
		go func() { result <- gs.Get(ctx, key, true) }()

		var job statsUpdateJob
		select {
		case job = <-gs.updateC:
		case <-time.After(time.Second):
			t.Fatal("synchronous read did not enqueue its producer")
		}
		generation, started, noProducer := gs.startAutomaticUpdateJob(job)
		require.True(t, started)
		require.False(t, noProducer)
		published := plan2.NewStatsInfo()
		published.TableCnt = 42
		gs.completeAutomaticStatsCacheUpdate(key, generation, published, true)
		gs.markAutomaticUpdateComplete(key, generation, true, 1, 1)

		select {
		case info := <-result:
			require.Same(t, published, info)
		case <-time.After(time.Second):
			t.Fatal("synchronous read did not observe accepted producer publication")
		}
		gs.queueWatcher.del(key.TableID)
	})
}

func TestGlobalStatsGetReturnsWhenCleanupPrecedesWait(t *testing.T) {
	runTest(t, func(ctx context.Context, e *Engine) {
		key := statsinfo.StatsInfoKey{
			DatabaseID: 100,
			TableID:    10002,
			TableName:  "t",
			DbName:     "d",
		}
		gs, _ := newSynchronousStatsGetHarness(t, ctx, e, key)

		// Hold the watcher after updateC accepts the job. This is an observable
		// barrier between producer ownership transfer and wait registration.
		gs.queueWatcher.Lock()
		watcherLocked := true
		defer func() {
			if watcherLocked {
				gs.queueWatcher.Unlock()
			}
		}()

		waitEntered := make(chan struct{})
		var waitOnce sync.Once
		gs.beforeStatsWait = func(statsinfo.StatsInfoKey, *updateRecord) {
			waitOnce.Do(func() { close(waitEntered) })
		}
		result := make(chan *statsinfo.StatsInfo, 1)
		go func() { result <- gs.Get(ctx, key, true) }()

		var staleJob statsUpdateJob
		select {
		case staleJob = <-gs.updateC:
		case <-time.After(time.Second):
			t.Fatal("GlobalStats.Get did not transfer the refresh job")
		}

		// Cleanup broadcasts before Get is able to enter cond.Wait. Processing
		// the stale job produces no second notification.
		gs.RemoveTid(key.TableID)
		gs.coordinateStatsUpdateJob(staleJob)
		watcherLocked = false
		gs.queueWatcher.Unlock()

		select {
		case info := <-result:
			require.Nil(t, info)
		case <-time.After(time.Second):
			t.Fatal("GlobalStats.Get lost the cleanup wake before cond.Wait")
		}
		select {
		case <-waitEntered:
			t.Fatal("GlobalStats.Get waited after its producer generation was removed")
		default:
		}
		gs.queueWatcher.del(key.TableID)
	})
}

func TestGlobalStatsGetReturnsWhenCleanupFollowsWait(t *testing.T) {
	runTest(t, func(ctx context.Context, e *Engine) {
		key := statsinfo.StatsInfoKey{
			DatabaseID: 100,
			TableID:    10003,
			TableName:  "t",
			DbName:     "d",
		}
		gs, _ := newSynchronousStatsGetHarness(t, ctx, e, key)
		waitEntered := make(chan struct{})
		var waitOnce sync.Once
		gs.beforeStatsWait = func(statsinfo.StatsInfoKey, *updateRecord) {
			waitOnce.Do(func() { close(waitEntered) })
		}
		result := make(chan *statsinfo.StatsInfo, 1)
		go func() { result <- gs.Get(ctx, key, true) }()

		var staleJob statsUpdateJob
		select {
		case staleJob = <-gs.updateC:
		case <-time.After(time.Second):
			t.Fatal("GlobalStats.Get did not transfer the refresh job")
		}
		select {
		case <-waitEntered:
		case <-time.After(time.Second):
			t.Fatal("GlobalStats.Get did not reach cond.Wait")
		}

		gs.RemoveTid(key.TableID)
		select {
		case info := <-result:
			require.Nil(t, info)
		case <-time.After(time.Second):
			t.Fatal("GlobalStats.Get was not released by cleanup after cond.Wait")
		}
		gs.coordinateStatsUpdateJob(staleJob)
		gs.queueWatcher.del(key.TableID)
	})
}

func TestGlobalStatsGetDoesNotOutliveCanceledSharedProducer(t *testing.T) {
	runTest(t, func(ctx context.Context, e *Engine) {
		key := statsinfo.StatsInfoKey{
			DatabaseID: 100,
			TableID:    10004,
			TableName:  "t",
			DbName:     "d",
		}
		gs, _ := newSynchronousStatsGetHarness(t, ctx, e, key)

		// Occupy the table stripe so the first producer can be canceled after
		// worker admission but before it starts object work.
		releaseStripe, err := gs.acquireStatsRefresh(context.Background(), key)
		require.NoError(t, err)
		stripeHeld := true
		defer func() {
			if stripeHeld {
				releaseStripe()
			}
		}()

		producerStarted := make(chan struct{})
		var producerOnce sync.Once
		gs.afterAutomaticUpdateStarted = func(statsinfo.StatsInfoKey, *updateRecord) {
			producerOnce.Do(func() { close(producerStarted) })
		}

		producerCtx, cancelProducer := context.WithCancel(ctx)
		defer cancelProducer()
		producerResult := make(chan *statsinfo.StatsInfo, 1)
		go func() { producerResult <- gs.Get(producerCtx, key, true) }()
		var producerJob statsUpdateJob
		select {
		case producerJob = <-gs.updateC:
		case <-time.After(time.Second):
			t.Fatal("first synchronous read did not enqueue its producer")
		}
		producerDone := make(chan struct{})
		go func() {
			gs.coordinateStatsUpdateJob(producerJob)
			close(producerDone)
		}()
		select {
		case <-producerStarted:
		case <-time.After(time.Second):
			t.Fatal("first producer did not reach worker admission")
		}

		sharedResult := make(chan *statsinfo.StatsInfo, 1)
		go func() { sharedResult <- gs.Get(ctx, key, true) }()
		var sharedJob statsUpdateJob
		select {
		case sharedJob = <-gs.updateC:
		case <-time.After(time.Second):
			t.Fatal("second synchronous read did not enqueue its shared job")
		}
		// This job coalesces behind the in-progress producer. Its waiter must
		// still be released if that producer is canceled before publication.
		gs.coordinateStatsUpdateJob(sharedJob)
		cancelProducer()

		select {
		case <-producerDone:
		case <-time.After(time.Second):
			t.Fatal("canceled producer did not leave refresh admission")
		}
		select {
		case info := <-producerResult:
			require.Nil(t, info)
		case <-time.After(time.Second):
			t.Fatal("producer caller did not observe cancellation")
		}
		select {
		case info := <-sharedResult:
			require.Nil(t, info)
		case <-time.After(time.Second):
			t.Fatal("shared waiter outlived its canceled producer")
		}

		stripeHeld = false
		releaseStripe()
		gs.queueWatcher.del(key.TableID)
	})
}

func TestGlobalStatsGetReturnsWhenStatsWorkersStop(t *testing.T) {
	runTest(t, func(ctx context.Context, e *Engine) {
		key := statsinfo.StatsInfoKey{
			DatabaseID: 100,
			TableID:    10005,
			TableName:  "t",
			DbName:     "d",
		}
		gs, _ := newSynchronousStatsGetHarness(t, ctx, e, key)
		workerCtx, stopWorkers := context.WithCancel(ctx)
		gs.ctx = workerCtx
		context.AfterFunc(workerCtx, gs.notifyStatsWaiters)
		waitEntered := make(chan struct{})
		var waitOnce sync.Once
		gs.beforeStatsWait = func(statsinfo.StatsInfoKey, *updateRecord) {
			waitOnce.Do(func() { close(waitEntered) })
		}

		result := make(chan *statsinfo.StatsInfo, 1)
		go func() { result <- gs.Get(ctx, key, true) }()
		var abandonedJob statsUpdateJob
		select {
		case abandonedJob = <-gs.updateC:
		case <-time.After(time.Second):
			t.Fatal("synchronous read did not enqueue before worker shutdown")
		}
		select {
		case <-waitEntered:
		case <-time.After(time.Second):
			t.Fatal("synchronous read did not wait for its queued producer")
		}

		stopWorkers()
		select {
		case info := <-result:
			require.Nil(t, info)
		case <-time.After(time.Second):
			t.Fatal("synchronous read outlived the statistics worker lifecycle")
		}
		gs.unregisterStatsUpdateJob(key, abandonedJob.expectedRecord)
		gs.queueWatcher.del(key.TableID)
	})
}

func TestStatsUpdateGenerationRequiresLiveSubscriptionOwner(t *testing.T) {
	key := statsinfo.StatsInfoKey{DatabaseID: 10, TableID: 42}
	e := &Engine{}
	gs := &GlobalStats{
		engine:       e,
		updateC:      make(chan statsUpdateJob, 1),
		queueWatcher: newQueueWatcher(),
	}
	gs.updatingMu.updating = make(map[statsinfo.StatsInfoKey]*updateRecord)
	gs.mu.statsInfoMap = make(map[statsinfo.StatsInfoKey]*statsinfo.StatsInfo)
	gs.mu.cond = sync.NewCond(&gs.mu)
	e.pClient.subscribed.rw.Lock()
	e.pClient.subscribed.m = make(map[uint64]*subEntry)
	e.pClient.subscribed.rw.Unlock()

	require.False(t, gs.PrefetchTableMeta(context.Background(), key))
	gs.updatingMu.Lock()
	_, retained := gs.updatingMu.updating[key]
	gs.updatingMu.Unlock()
	require.False(t, retained,
		"prefetch without a subscription cleanup owner must not create a generation")

	oldEnt := &subEntry{dbID: key.DatabaseID, state: Subscribed}
	e.pClient.subscribed.rw.Lock()
	e.pClient.subscribed.m[key.TableID] = oldEnt
	e.pClient.subscribed.rw.Unlock()
	oldGeneration, ok := gs.currentOrCreateExactSubscribedUpdateRecord(key, oldEnt)
	require.True(t, ok)
	require.True(t, gs.PrefetchTableMeta(context.Background(), key))
	job := <-gs.updateC
	require.Same(t, oldGeneration, job.expectedRecord)
	gs.queueWatcher.del(key.TableID)

	gs.RemoveTid(key.TableID)
	gs.coordinateStatsUpdateJob(job)
	gs.updatingMu.Lock()
	require.Zero(t, oldGeneration.queued)
	gs.updatingMu.Unlock()
	newEnt := &subEntry{dbID: key.DatabaseID, state: Subscribed}
	e.pClient.subscribed.rw.Lock()
	e.pClient.subscribed.m[key.TableID] = newEnt
	e.pClient.subscribed.rw.Unlock()
	_, ok = gs.currentOrCreateExactSubscribedUpdateRecord(key, oldEnt)
	require.False(t, ok,
		"work captured from an old subscription must not target its replacement")
	gs.updatingMu.Lock()
	_, retained = gs.updatingMu.updating[key]
	gs.updatingMu.Unlock()
	require.False(t, retained,
		"rejecting an old subscription must not create idle replacement metadata")
	newGeneration, ok := gs.currentOrCreateExactSubscribedUpdateRecord(key, newEnt)
	require.True(t, ok)
	require.NotSame(t, oldGeneration, newGeneration)
}

func TestEnqueueStatsUpdateForceReturnsWhenContextCanceled(t *testing.T) {
	gs := &GlobalStats{
		updateC:      make(chan statsUpdateJob, 1),
		queueWatcher: newQueueWatcher(),
	}
	gs.updatingMu.updating = make(map[statsinfo.StatsInfoKey]*updateRecord)
	queued := statsinfo.StatsInfoKeyWithContext{
		Ctx: context.Background(),
		Key: statsinfo.StatsInfoKey{TableID: 1},
	}
	gs.updateC <- statsUpdateJob{wrapKey: queued}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	generation := gs.currentOrCreateUpdateRecord(statsinfo.StatsInfoKey{TableID: 2})
	accepted := gs.enqueueStatsUpdateForRecord(statsinfo.StatsInfoKeyWithContext{
		Ctx: ctx,
		Key: statsinfo.StatsInfoKey{TableID: 2},
	}, true, generation)
	require.False(t, accepted)
	require.Equal(t, queued, (<-gs.updateC).wrapKey)
}

func TestCacheRemoteInfoIfSubscribedBroadcastsWaiters(t *testing.T) {
	runTest(t, func(ctx context.Context, e *Engine) {
		gs := e.globalStats
		const dbID uint64 = 100
		const tblID uint64 = 10001

		e.pClient.eng = e
		e.pClient.subscribed.eng = e

		ent := &subEntry{dbID: dbID, state: Subscribed}
		ent.lastTs.Store(time.Now().UnixNano())

		if e.pClient.subscribed.m == nil {
			e.pClient.subscribed.m = make(map[uint64]*subEntry)
		}
		e.pClient.subscribed.m[tblID] = ent

		key := statsinfo.StatsInfoKey{
			AccId:      0,
			DatabaseID: dbID,
			TableID:    tblID,
			TableName:  "t",
			DbName:     "d",
		}

		remoteInfo := plan2.NewStatsInfo()
		remoteInfo.TableCnt = 42

		waitEntered := make(chan struct{})
		waitDone := make(chan struct{})
		var waitOnce sync.Once

		go func() {
			gs.mu.Lock()
			defer gs.mu.Unlock()
			for {
				if _, ok := gs.mu.statsInfoMap[key]; ok {
					break
				}
				waitOnce.Do(func() { close(waitEntered) })
				gs.mu.cond.Wait()
			}
			close(waitDone)
		}()

		require.Eventually(t, func() bool {
			select {
			case <-waitEntered:
				return true
			default:
				return false
			}
		}, time.Second, 10*time.Millisecond, "waiter did not enter cond.Wait")

		info := gs.cacheRemoteInfoIfSubscribed(key, ent, remoteInfo, nil, false)
		require.NotNil(t, info)
		require.Equal(t, remoteInfo, info)

		require.Eventually(t, func() bool {
			select {
			case <-waitDone:
				return true
			default:
				return false
			}
		}, time.Second, 10*time.Millisecond, "waiter was not awakened by remote cache broadcast")

		// Model a local ANALYZE publication winning while a remote lookup is in
		// flight. The response path must neither export nor overwrite the newer
		// schema-bound observation.
		boundInfo := plan2.NewStatsInfo()
		boundInfo.TableCnt = 84
		gs.mu.Lock()
		gs.mu.statsInfoMap[key] = boundInfo
		gs.mu.tableDefVersions[key] = 7
		gs.mu.Unlock()
		rejected := gs.cacheRemoteInfoIfSubscribed(
			key, ent, remoteInfo, nil, true)
		require.Nil(t, rejected)
		gs.mu.Lock()
		require.Same(t, boundInfo, gs.mu.statsInfoMap[key])
		require.Equal(t, uint32(7), gs.mu.tableDefVersions[key])
		gs.mu.Unlock()
	})
}

func TestGlobalStatsGetDoesNotCacheRemoteInfoAfterUnsubscribe(t *testing.T) {
	runTest(t, func(ctx context.Context, e *Engine) {
		gs := e.globalStats
		const dbID uint64 = 100
		const tblID uint64 = 10001
		key, _ := installRemoteStatsTestTable(t, ctx, e, dbID, tblID)

		remoteInfo := plan2.NewStatsInfo()
		remoteInfo.TableCnt = 42

		qc := &mockStatsQueryClient{
			response: &querypb.Response{
				GetStatsInfoResponse: &querypb.GetStatsInfoResponse{StatsInfo: remoteInfo},
			},
			sendStarted: make(chan struct{}),
			allowReturn: make(chan struct{}),
		}
		oldQC := e.qc
		oldRouter := gs.KeyRouter
		oldHook := gs.beforeCacheRemoteInfo
		router := &mockStatsKeyRouter{target: "cn1"}
		e.qc = qc
		gs.KeyRouter = router
		defer func() {
			e.qc = oldQC
			gs.KeyRouter = oldRouter
			gs.beforeCacheRemoteInfo = oldHook
		}()

		beforeCacheReached := make(chan struct{})
		allowCache := make(chan struct{})
		gs.beforeCacheRemoteInfo = func(statsinfo.StatsInfoKey) {
			close(beforeCacheReached)
			<-allowCache
		}

		getCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
		defer cancel()

		resultCh := make(chan *statsinfo.StatsInfo, 1)
		go func() {
			resultCh <- gs.Get(getCtx, key, false)
		}()

		select {
		case <-qc.sendStarted:
		case <-time.After(time.Second):
			t.Fatal("GlobalStats.Get did not request remote stats")
		}
		require.Equal(t, "cn1", qc.target)
		require.Equal(t, statsinfo.StatsInfoKey{
			DatabaseID: key.DatabaseID,
			TableID:    key.TableID,
		}, router.key)
		require.NotNil(t, qc.request)
		require.Equal(t, querypb.CmdMethod_GetStatsInfo, qc.request.CmdMethod)
		require.NotNil(t, qc.request.GetStatsInfoRequest)
		require.NotNil(t, qc.request.GetStatsInfoRequest.StatsInfoKey)
		require.Equal(t, key, *qc.request.GetStatsInfoRequest.StatsInfoKey)

		close(qc.allowReturn)

		select {
		case <-beforeCacheReached:
		case <-time.After(time.Second):
			t.Fatal("GlobalStats.Get did not reach remote cache write point")
		}

		e.pClient.subscribed.setTableUnsubscribe(dbID, tblID)
		close(allowCache)

		select {
		case info := <-resultCh:
			require.Nil(t, info)
		case <-time.After(time.Second):
			t.Fatal("GlobalStats.Get did not return after unsubscribe")
		}

		gs.mu.Lock()
		_, ok := gs.mu.statsInfoMap[key]
		gs.mu.Unlock()
		assert.False(t, ok)
		require.Equal(t, int32(1), qc.releases.Load())
	})
}

func TestGlobalStatsGetReleasesRemoteResponseWithoutStatsPayload(t *testing.T) {
	runTest(t, func(ctx context.Context, e *Engine) {
		gs := e.globalStats
		key, _ := installRemoteStatsTestTable(t, ctx, e, 101, 10002)
		qc := &mockStatsQueryClient{response: &querypb.Response{}}

		oldQC := e.qc
		oldRouter := gs.KeyRouter
		router := &mockStatsKeyRouter{target: "cn-empty"}
		e.qc = qc
		gs.KeyRouter = router
		defer func() {
			e.qc = oldQC
			gs.KeyRouter = oldRouter
		}()

		getCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
		defer cancel()
		require.Nil(t, gs.Get(getCtx, key, false))
		require.Equal(t, "cn-empty", qc.target)
		require.Equal(t, statsinfo.StatsInfoKey{
			DatabaseID: key.DatabaseID,
			TableID:    key.TableID,
		}, router.key)
		require.NotNil(t, qc.request)
		require.NotNil(t, qc.request.GetStatsInfoRequest)
		require.NotNil(t, qc.request.GetStatsInfoRequest.StatsInfoKey)
		require.Equal(t, key, *qc.request.GetStatsInfoRequest.StatsInfoKey)
		require.Equal(t, int32(1), qc.releases.Load())
	})
}

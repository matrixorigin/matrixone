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
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lni/goutils/leaktest"
	"github.com/stretchr/testify/assert"

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

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
	"math/rand"
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
)

func TestGetStats(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	gs := NewGlobalStats(ctx, nil, nil,
		WithUpdateWorkerFactor(4),
		WithStatsUpdater(func(_ context.Context, key statsinfo.StatsInfoKey, info *statsinfo.StatsInfo) bool {
			info.BlockNumber = 20
			return true
		}),
	)

	tids := []uint64{2000, 2001, 2002}
	go func() {
		time.Sleep(time.Millisecond * 20)
		for _, tid := range tids {
			gs.notifyLogtailUpdate(tid)
		}
	}()
	var wg sync.WaitGroup
	wg.Add(100)
	for i := 0; i < 100; i++ {
		go func(j int) {
			defer wg.Done()
			rd := rand.New(rand.NewSource(time.Now().UnixNano()))
			time.Sleep(time.Millisecond * time.Duration(10+rd.Intn(20)))
			k := statsinfo.StatsInfoKey{
				DatabaseID: 1000,
				TableID:    tids[j%3],
			}
			info := gs.Get(ctx, k, true)
			assert.NotNil(t, info)
			assert.Equal(t, int64(20), info.BlockNumber)
		}(i)
	}
	wg.Wait()
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
	mp, err := mpool.NewMPool(sid, 1024*1024, 0)
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
			updated := e.globalStats.doUpdate(ctx, k, stats)
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
			updated := e.globalStats.doUpdate(ctx, k, stats)
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
			updated := e.globalStats.doUpdate(ctx, k, stats)
			assert.True(t, updated)
		}, WithApproxObjectNumUpdater(func() int64 {
			return 10
		}))
	})
}

func TestWaitLogtailUpdate(t *testing.T) {
	origInitCheckInterval := initCheckInterval
	origMaxCheckInterval := maxCheckInterval
	origCheckTimeout := checkTimeout
	defer func() {
		initCheckInterval = origInitCheckInterval
		maxCheckInterval = origMaxCheckInterval
		checkTimeout = origCheckTimeout
		leaktest.AfterTest(t)()
	}()
	initCheckInterval = time.Millisecond * 2
	maxCheckInterval = time.Millisecond * 10
	checkTimeout = time.Millisecond * 200

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	gs := NewGlobalStats(ctx, nil, nil)
	assert.NotNil(t, gs)
	tid := uint64(2)
	gs.waitLogtailUpdated(tid)

	tid = 200
	go func() {
		time.Sleep(time.Millisecond * 100)
		gs.notifyLogtailUpdate(tid)
	}()
	gs.waitLogtailUpdated(tid)
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

func TestGlobalStats_ClearTables(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	gs := NewGlobalStats(ctx, nil, nil)
	for i := 0; i < 10; i++ {
		gs.notifyLogtailUpdate(uint64(2000 + i))
	}
	assert.Equal(t, 10, len(gs.logtailUpdate.mu.updated))
	gs.clearTables()
	assert.Equal(t, 0, len(gs.logtailUpdate.mu.updated))
}

func TestWaitKeeper(t *testing.T) {
	defer leaktest.AfterTest(t)()

	var tid uint64 = 1
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	gs := NewGlobalStats(ctx, nil, nil)
	assert.False(t, gs.safeToUnsubscribe(tid))

	w := gs.waitKeeper
	w.add(tid)
	_, ok := w.records[tid]
	assert.True(t, ok)
	assert.True(t, gs.safeToUnsubscribe(tid))

	gs.RemoveTid(tid)
	_, ok = w.records[tid]
	assert.False(t, ok)
	assert.False(t, gs.safeToUnsubscribe(tid))

	w.add(tid)
	_, ok = w.records[tid]
	assert.True(t, ok)
	gs.clearTables()
	_, ok = w.records[tid]
	assert.False(t, ok)
	assert.False(t, gs.safeToUnsubscribe(tid))
}

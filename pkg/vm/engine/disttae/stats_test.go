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
			ps := logtailreplay.NewPartitionState("", true, 1001)
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
			ps := logtailreplay.NewPartitionState("", true, tid)
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
			ps := logtailreplay.NewPartitionState("", true, tid)
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

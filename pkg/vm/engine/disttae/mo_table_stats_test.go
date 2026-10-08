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
	"errors"
	"fmt"
	"math/rand"
	"strconv"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/panjf2000/ants/v2"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/txn/rpc"
	ie "github.com/matrixorigin/matrixone/pkg/util/internalExecutor"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/cmd_util"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae/logtailreplay"
	"github.com/stretchr/testify/require"
)

func TestGetChangedTableListWithNoTN(t *testing.T) {
	ctx := context.Background()

	client.RunTxnTests(func(cli client.TxnClient, _ rpc.TxnSender) {
		rt := runtime.ServiceRuntime("")
		oldCluster, hadOldCluster := rt.GetGlobalVariables(runtime.ClusterService)
		emptyCluster := mockCluster{}
		rt.SetGlobalVariables(runtime.ClusterService, emptyCluster)
		t.Cleanup(func() {
			if hadOldCluster {
				rt.SetGlobalVariables(runtime.ClusterService, oldCluster)
			} else {
				rt.CompareAndDeleteGlobalVariables(runtime.ClusterService, emptyCluster)
			}
		})

		eng := &Engine{cli: cli}
		var (
			pairs        []tablePair
			to, oldest   types.TS
			from, latest timestamp.Timestamp
		)

		err := getChangedTableList(
			ctx,
			"",
			eng,
			nil,
			nil,
			nil,
			[]timestamp.Timestamp{from, latest},
			&pairs,
			&to,
			&oldest,
			cmd_util.CollectChanged,
			nil,
		)

		require.Error(t, err)
		require.True(t, moerr.IsMoErrCode(err, moerr.ErrNoAvailableBackend), err)
	})
}

func TestMoTableStatsDispatchRejectsInvalidAndClosedEngines(t *testing.T) {
	ctx := context.Background()
	for name, fn := range map[string]func() *function.GetMoTableSizeRowsFuncType{
		"size": moTableSizeFunc,
		"rows": moTableRowsFunc,
	} {
		t.Run(name, func(t *testing.T) {
			callback := fn()
			_, err, _ := (*callback)(ctx, nil, nil, nil, nil, false, false)
			require.Error(t, err)

			closed := &Engine{}
			closed.dynamicCtx.closed.Store(true)
			_, err, _ = (*callback)(ctx, nil, nil, nil, closed, false, false)
			require.Error(t, err)
			require.Contains(t, err.Error(), "engine is closed")

			wrapped := &engine.EntireEngine{Engine: closed}
			_, err, _ = (*callback)(ctx, nil, nil, nil, wrapped, false, false)
			require.Error(t, err)
			require.Contains(t, err.Error(), "engine is closed")
		})
	}
}

func TestDynamicCtxCloseCancelsAndJoinsRoots(t *testing.T) {
	d := &dynamicCtx{
		cleanDeletesQueue:    make(chan struct{}),
		updateForgottenQueue: make(chan struct{}),
		insertNewTableQueue:  make(chan struct{}),
	}
	d.ctx, d.cancel = context.WithCancel(context.Background())
	d.roots.Add(1)
	go func() {
		defer d.roots.Done()
		<-d.ctx.Done()
	}()

	d.Close()
	d.Close()
	require.True(t, d.closed.Load())
	require.NotPanics(t, func() {
		d.NotifyCleanDeletes()
		d.NotifyUpdateForgotten()
		d.NotifyInsertNewTable()
	})
}

func Test_intsJoin(t *testing.T) {
	wg := sync.WaitGroup{}

	for range 100 {
		wg.Add(1)
		go func() {
			defer func() {
				wg.Done()
			}()

			item := make([]uint64, max(rand.Int()%5, 1))
			for i := range item {
				item[i] = rand.Uint64() % 10
			}

			ret, release := intsJoin(item, ",")
			defer func() {
				release()
			}()

			split := strings.Split(ret, ",")

			//fmt.Println(item, ret)
			for j, s := range split {
				require.Equal(t, strconv.FormatUint(item[j], 10), s, fmt.Sprintf("%v(%d) %v", item, j, ret))
			}
		}()
	}

	wg.Wait()
}

func Test_joinAccountDatabase(t *testing.T) {
	wg := sync.WaitGroup{}
	for range 100 {
		wg.Add(1)

		go func() {
			defer func() {
				wg.Done()
			}()

			cnt := int(max(rand.Uint64()%10, uint64(1)))
			acc := make([]uint64, cnt)
			db := make([]uint64, cnt)

			for i := range acc {
				acc[i] = rand.Uint64() % uint64(cnt*2)
				db[i] = rand.Uint64() % uint64(cnt*2)
			}

			ret, release := joinAccountDatabase(acc, db)

			andStrs := strings.Split(ret, " OR ")
			require.Equal(t, int(cnt), len(andStrs), fmt.Sprintf("cnt = %d, andStrs=%s", cnt, andStrs))

			for i, str := range andStrs {
				str = str[1:]
				str = str[:len(str)-1]

				equalStr := strings.Split(str, " AND ")
				ll := fmt.Sprintf("account_id = %d", acc[i])
				rr := fmt.Sprintf("database_id = %d", db[i])

				require.Equal(t, ll, equalStr[0], fmt.Sprintf("acc: %v, db: %v, ret: %v", acc, db, ret))
				require.Equal(t, rr, equalStr[1], fmt.Sprintf("acc: %v, db: %v, ret: %v", acc, db, ret))
			}

			release()
		}()
	}

	wg.Wait()
}

func Test_constructInStmtByTableId(t *testing.T) {

	t.Run("A", func(t *testing.T) {
		tblId := make([]uint64, 0, 4)
		tblId = append(tblId, 3)
		tblId = append(tblId, 5)
		tblId = append(tblId, 7)
		tblId = append(tblId, 9)

		str, release := constructInStmt(tblId, "table_id")
		defer release()

		require.Equal(t,
			"table_id in (3,5,7,9)",
			str)
	})

	t.Run("B", func(t *testing.T) {
		tblId := make([]uint64, 0, 1)
		tblId = append(tblId, 3)

		str, release := constructInStmt(tblId, "table_id")
		defer release()

		require.Equal(t,
			"table_id in (3)",
			str)
	})
}

func Test_joinAccountDatabaseTable(t *testing.T) {
	wg := sync.WaitGroup{}
	for range 100 {
		wg.Add(1)
		go func() {

			defer func() {
				wg.Done()
			}()

			cnt := int(max(rand.Uint64()%10, uint64(1)))
			acc := make([]uint64, cnt)
			db := make([]uint64, cnt)
			tbl := make([]uint64, cnt)

			for i := range acc {
				acc[i] = rand.Uint64() % uint64(cnt*2)
				db[i] = rand.Uint64() % uint64(cnt*2)
				tbl[i] = rand.Uint64() % uint64(cnt*2)
			}

			ret, release := joinAccountDatabaseTable(acc, db, tbl)

			andStrs := strings.Split(ret, " OR ")
			require.Equal(t, int(cnt), len(andStrs), fmt.Sprintf("cnt = %d, andStrs=%s", cnt, andStrs))

			for i, str := range andStrs {
				str = str[1:]
				str = str[:len(str)-1]

				equalStr := strings.Split(str, " AND ")
				ll := fmt.Sprintf("account_id = %d", acc[i])
				mm := fmt.Sprintf("database_id = %d", db[i])
				rr := fmt.Sprintf("table_id = %d", tbl[i])

				require.Equal(t, ll, equalStr[0], fmt.Sprintf("acc: %v, db: %v, tbl: %v, ret: %v", acc, db, tbl, ret))
				require.Equal(t, mm, equalStr[1], fmt.Sprintf("acc: %v, db: %v, tbl: %v, ret: %v", acc, db, tbl, ret))
				require.Equal(t, rr, equalStr[2], fmt.Sprintf("acc: %v, db: %v, tbl: %v, ret: %v", acc, db, tbl, ret))
			}

			release()
		}()
	}

	wg.Wait()
}

func Benchmark_intsJoin(b *testing.B) {
	item := make([]uint64, 100)
	for i := range item {
		item[i] = rand.Uint64()
	}

	for range b.N {
		_, f := intsJoin(item, ",")
		f()
	}
}

func Benchmark_joinAccountDatabase(b *testing.B) {
	acc := make([]uint64, 100*100)
	db := make([]uint64, 100*100)

	for i := range acc {
		acc[i] = rand.Uint64()
		db[i] = rand.Uint64()
	}

	for range b.N {
		_, f := joinAccountDatabase(acc, db)
		f()
	}
}

func Benchmark_joinAccountDatabaseTable(b *testing.B) {
	acc := make([]uint64, 100*100)
	db := make([]uint64, 100*100)
	tbl := make([]uint64, 100*100)

	for i := range acc {
		acc[i] = rand.Uint64()
		db[i] = rand.Uint64()
		tbl[i] = rand.Uint64()
	}

	for range b.N {
		_, f := joinAccountDatabaseTable(acc, db, tbl)
		f()
	}
}

func TestAlphaTask(t *testing.T) {
	ctx := context.Background()

	t.Run("forbidden beta", func(t *testing.T) {
		eng := &Engine{}

		initMoTableStatsConfig(ctx, eng)

		tps := []tablePair{
			{
				onlyUpdateTS: true,
				valid:        false,
			},
			{
				onlyUpdateTS: true,
				valid:        true,
			},
		}

		eng.dynamicCtx.beta.forbidden = true

		for _, tp := range tps {
			fmt.Println(tp.String())
		}

		fmt.Println(eng.dynamicCtx.beta.String())

		eng.dynamicCtx.alphaTask(ctx, "", tps, t.Name())
	})

	t.Run("normal", func(t *testing.T) {
		eng := &Engine{}

		initMoTableStatsConfig(ctx, eng)

		tps := []tablePair{
			{
				onlyUpdateTS: true,
				valid:        false,
			},
			{
				onlyUpdateTS: true,
				valid:        true,
			},
		}

		for _, tp := range tps {
			fmt.Println(tp.String())
		}

		fmt.Println(eng.dynamicCtx.beta.String())

		eng.dynamicCtx.alphaTask(ctx, "", tps, t.Name())
	})

}

// Distinct query errors make the executor's owner observable without a second
// SQL implementation or cluster fixture. Both engines remain open initially.
type statsDispatchExecutor struct {
	ie.InternalExecutor
	err error
}

func (e statsDispatchExecutor) Query(context.Context, string, ie.SessionOverrideOptions) ie.InternalExecResult {
	return statsDispatchResult{err: e.err}
}

type statsDispatchResult struct {
	ie.InternalExecResult
	err error
}

func (r statsDispatchResult) Error() error { return r.err }

func TestMoTableStatsDispatchUsesCallerOwner(t *testing.T) {
	for name, fn := range map[string]func() *function.GetMoTableSizeRowsFuncType{
		"size": moTableSizeFunc,
		"rows": moTableRowsFunc,
	} {
		t.Run(name, func(t *testing.T) {
			callback := fn()
			owners := []*Engine{{}, {}}
			errs := []error{errors.New("first owner's executor"), errors.New("second owner's executor")}
			for i, owner := range owners {
				t.Cleanup(func() { require.NoError(t, owner.Close()) })
				owner.dynamicCtx.executorPool.New = func() any { return statsDispatchExecutor{err: errs[i]} }
			}
			callers := []engine.Engine{owners[0], &engine.EntireEngine{Engine: owners[1]}}
			for i, caller := range callers {
				_, err, _ := (*callback)(context.Background(), []uint64{0}, []uint64{1}, []uint64{2}, caller, false, false)
				require.ErrorIs(t, err, errs[i])
			}
			require.NoError(t, owners[0].Close())
			_, err, _ := (*callback)(context.Background(), []uint64{0}, []uint64{1}, []uint64{2}, callers[1], false, false)
			require.ErrorIs(t, err, errs[1], "closing another CN must not retire this caller's statistics")
		})
	}
}

// A read/query already in progress may take time to observe cancellation.
// Gate that real dependency boundary without sleeping or building a cluster.
type statsJobGate struct {
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (g *statsJobGate) wait() {
	g.once.Do(func() { close(g.entered) })
	<-g.release
}

type gatedStatsFileService struct {
	fileservice.FileService
	gate *statsJobGate
}

func (f gatedStatsFileService) Name() string { return "stats-job-join" }
func (f gatedStatsFileService) Read(context.Context, *fileservice.IOVector) error {
	f.gate.wait()
	return context.Canceled
}

type gatedStatsExecutor struct {
	ie.InternalExecutor
	gate *statsJobGate
}

func (e gatedStatsExecutor) Query(context.Context, string, ie.SessionOverrideOptions) ie.InternalExecResult {
	e.gate.wait()
	return statsDispatchResult{err: context.Canceled}
}

func TestStatisticsRootsJoinSubmittedJobs(t *testing.T) {
	for _, task := range []string{"beta", "gama"} {
		t.Run(task, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				gate := &statsJobGate{entered: make(chan struct{}), release: make(chan struct{})}
				pool, err := ants.NewPool(1)
				require.NoError(t, err)
				done := make(chan struct{})
				started := false
				var releaseOnce sync.Once
				unblock := func() { releaseOnce.Do(func() { close(gate.release) }) }
				t.Cleanup(func() {
					cancel()
					unblock()
					if started {
						<-done
					}
					require.NoError(t, pool.ReleaseTimeout(time.Second))
				})
				d := &dynamicCtx{}
				e := &Engine{}
				if task == "beta" {
					d.beta.taskPool = pool
					d.tblQueue = make(chan tablePair, 1)
					e.fs = gatedStatsFileService{gate: gate}
					state := logtailreplay.NewPartitionState("", false, 1, false)
					id := objectio.NewObjectid()
					stats := objectio.NewObjectStatsWithObjectID(&id, true, false, false)
					require.NoError(t, objectio.SetObjectStatsSize(stats, 1))
					require.NoError(t, objectio.SetObjectStatsBlkCnt(stats, 1))
					require.NoError(t, objectio.SetObjectStatsRowCnt(stats, 1))
					require.NoError(t, objectio.SetObjectStatsExtent(stats, objectio.NewExtent(1, 1, 1, 1)))
					require.NoError(t, state.HandleObjectEntry(ctx, nil, objectio.ObjectEntry{
						ObjectStats: *stats, CreateTime: types.BuildTS(1, 0), DeleteTime: types.BuildTS(3, 0),
					}, false))
					started = true
					go func() { defer close(done); d.betaTask(ctx, "", e) }()
					d.tblQueue <- tablePair{valid: true, pState: state, snapshot: types.BuildTS(2, 0), errChan: make(chan alphaError, 1)}
				} else {
					d.gama.taskPool = pool
					d.conf.CorrectionDuration = time.Hour
					d.cleanDeletesQueue = make(chan struct{})
					d.executorPool.New = func() any { return gatedStatsExecutor{gate: gate} }
					started = true
					go func() { defer close(done); d.gamaTask(ctx, "", e) }()
					d.cleanDeletesQueue <- struct{}{}
				}
				<-gate.entered
				cancel()
				synctest.Wait()
				select {
				case <-done:
					t.Fatal("statistics root returned while its submitted job was running")
				default:
				}
				unblock()
				<-done
			})
		})
	}
}

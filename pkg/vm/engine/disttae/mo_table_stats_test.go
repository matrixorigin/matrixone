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
			_, err, _ := (*callback)(ctx, nil, nil, false, false)
			require.Error(t, err)

			closed := &Engine{}
			closed.dynamicCtx.closed.Store(true)
			_, err, _ = (*callback)(ctx, closed, nil, false, false)
			require.Error(t, err)
			require.Contains(t, err.Error(), "engine is closed")

			wrapped := &engine.EntireEngine{Engine: closed}
			_, err, _ = (*callback)(ctx, wrapped, nil, false, false)
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
				_, err, _ := (*callback)(context.Background(), caller, func() ([]uint64, []uint64, []uint64, error) { return []uint64{0}, []uint64{1}, []uint64{2}, nil }, false, false)
				require.ErrorIs(t, err, errs[i])
			}
			require.NoError(t, owners[0].Close())
			_, err, _ := (*callback)(context.Background(), callers[1], func() ([]uint64, []uint64, []uint64, error) { return []uint64{0}, []uint64{1}, []uint64{2}, nil }, false, false)
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

// A second tableStatsExecutor round can start while a previous round's alpha
// workers are still in flight: the cron framework holds the mo_table_stats
// concurrency limit through async-task rows, and a heartbeat expiry, restart
// catch-up or trigger racing the row update re-runs the executor for the same
// dynamicCtx (issue #27702 observed such an overlap on a loaded CI runner: an
// ants worker writing tbls[i].pState while getChangedTableList appended to the
// same backing array through runtime.growslice). Both scenarios below drive
// runStatsCollectionRound, the per-tick handoff production function of
// tableStatsExecutor itself, so the fix has to keep this exact test green:
// aliasing the stock inside that production function is what turns the race
// detector red on the assigned base.
func TestTableStatsExecutorDetachesStockFromAlphaTask(t *testing.T) {
	ctx := context.Background()

	newFixture := func(t *testing.T) *dynamicCtx {
		eng := &Engine{}
		require.NoError(t, initMoTableStatsConfig(ctx, eng))
		t.Cleanup(func() {
			require.NoError(t, eng.Close())
		})
		// The pairs below must skip subscribeTable (a bare engine has no
		// backend), so this fixture itself is the queue consumer and sends
		// exactly one Done per pair, the count alphaTask's errWaitToReceive
		// expects. launchTask becomes a no-op so no production beta task
		// consumes d.tblQueue: betaTask's own onlyUpdateTS batch path sends a
		// second Done per such pair, a separate latent behavior this test
		// avoids.
		eng.dynamicCtx.launchTask = func(string) {}
		// With no beta reported running, alphaTask returns timeout=true one
		// tick after a tiny batch (the not-running-beta shortcut in its
		// len(tbls)==0 branch), and a descheduled test goroutine would turn
		// even the correctly fixed implementation flaky. Represent the
		// fixture's active consumer as the running beta task: the round then
		// ends only through the consumer's Done's (all processed) or its
		// cancellation, never through the not-running-beta shortcut.
		eng.dynamicCtx.Lock()
		eng.dynamicCtx.beta.running = true
		eng.dynamicCtx.Unlock()
		return &eng.dynamicCtx
	}

	// cap == len: the growing appends of the overlapping collector round must
	// first rewrite in place and then copy the old elements, covering both the
	// append writes and the runtime.growslice read of the CI report.
	fillStock := func(d *dynamicCtx, pairs int) {
		d.Lock()
		defer d.Unlock()
		d.tableStock.tbls = make([]tablePair, 0, pairs)
		for i := range pairs {
			d.tableStock.tbls = append(d.tableStock.tbls, tablePair{
				valid:        true,
				onlyUpdateTS: true,
				acc:          int64(i + 1),
				db:           2,
				tbl:          3,
				dbName:       "d",
				tblName:      fmt.Sprintf("t%d", i),
			})
		}
	}

	// collectorRound reproduces what another tableStatsExecutor invocation does
	// to the shared stock: the pre-existing round's cleanTableStock, then a
	// prepare-style getChangedTableList append.
	collectorRound := func(d *dynamicCtx, pairs int) {
		d.cleanTableStock()
		for i := range pairs {
			d.Lock()
			d.tableStock.tbls = append(d.tableStock.tbls, tablePair{
				valid:        true,
				onlyUpdateTS: true,
				acc:          int64(100 + i),
			})
			d.Unlock()
		}
	}

	type roundResult struct {
		timeout bool
		err     error
	}

	// runRound starts one production collection round with its own
	// cancellation and registers a joined cleanup, so every round is canceled
	// and waited for before its fixture engine closes and no collector
	// goroutine outlives the test, whatever its assertions did.
	runRound := func(t *testing.T, ctx context.Context, d *dynamicCtx) (context.CancelFunc, <-chan roundResult) {
		roundCtx, roundCancel := context.WithCancel(ctx)
		done := make(chan roundResult, 1)
		go func() {
			timeout, err := d.runStatsCollectionRound(roundCtx, "")
			done <- roundResult{timeout: timeout, err: err}
			close(done)
		}()

		t.Cleanup(func() {
			roundCancel()
			<-done
		})

		return roundCancel, done
	}

	// takeProducedPair is the phase barrier: receiving one produced pair
	// proves its worker entered, wrote the tablePair element it was handed
	// (the payload error channel is set) and completed its transmission, while
	// the collection round itself is still running. Batch transmission
	// markers carry valid == false and are skipped.
	takeProducedPair := func(t *testing.T, d *dynamicCtx) tablePair {
		deadline := time.After(time.Minute)
		for {
			select {
			case tbl := <-d.tblQueue:
				if !tbl.valid {
					continue
				}
				require.NotNil(t, tbl.errChan,
					"a produced pair must carry the worker's write")
				return tbl
			case <-deadline:
				t.Fatal("no produced tablePair arrived")
				return tablePair{}
			}
		}
	}

	// stockAfter asserts the post-round stock under the collector's locking
	// discipline: no alpha write may have escaped into it - a worker element
	// write landing in tableStock is the lost-update half of the reported bug.
	stockAfter := func(t *testing.T, d *dynamicCtx, pairs int) {
		d.Lock()
		stock := append([]tablePair(nil), d.tableStock.tbls...)
		d.Unlock()
		require.Len(t, stock, pairs)
		for i := range stock {
			require.Zero(t, stock[i].alphaOrder)
			require.Nil(t, stock[i].errChan)
			require.True(t, stock[i].valid)
			require.True(t, stock[i].onlyUpdateTS)
		}
	}

	t.Run("collects a round while another collector mutates the stock", func(t *testing.T) {
		d := newFixture(t)
		fillStock(d, 2)

		_, roundDone := runRound(t, ctx, d)

		// one produced pair proves a worker already wrote and transmitted its
		// element while the round and the second worker are still in flight
		first := takeProducedPair(t, d)

		// the overlapping collector round runs while the second produced pair
		// is still in flight; under -race this is the interleaving observed by
		// CI and must stay race-free once the production handoff detaches the
		// stock
		collectorRound(d, 9)

		second := takeProducedPair(t, d)
		first.Done(nil)
		second.Done(nil)

		select {
		case result := <-roundDone:
			require.False(t, result.timeout, "the round must complete, not time out")
			require.NoError(t, result.err)
		case <-time.After(time.Minute):
			t.Fatal("collection round did not complete")
		}

		stockAfter(t, d, 9)
	})

	t.Run("cancellation of an in-flight round reports ctx error", func(t *testing.T) {
		d := newFixture(t)
		fillStock(d, 1)

		alphaCancel, roundDone := runRound(t, ctx, d)

		// the phase barrier guarantees the worker produced its pair and the
		// round is in flight when the executor-like cancellation fires
		takeProducedPair(t, d)

		collectorRound(d, 7)
		alphaCancel()

		select {
		case result := <-roundDone:
			require.ErrorIs(t, result.err, context.Canceled)
		case <-time.After(time.Minute):
			t.Fatal("canceled collection round did not return")
		}

		stockAfter(t, d, 7)
	})
}

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

package compile

import (
	"context"
	"errors"
	"math"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zapcore"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/frontend/databranchutils"
	"github.com/matrixorigin/matrixone/pkg/lockservice"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/txn/rpc"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
)

// D192/D195 cells over awaitLedgerWatermark / probeScratchWatermark with a
// REAL lockservice and txn client (the same RunLockServicesForTest canon
// as the load-promotion cells): the probe's owner rides the fencing pool
// beneath the saturation circuit breaker, its disposal is detached
// accounted work (D192(B.2)), and ONE absolute fold deadline covers every
// acquire wait (D195(B.1)). The reviewer-corrected oracles are honored
// exactly: saturation is asserted AT the configured capacity, the fold
// timeout is exercised SEPARATELY with completing disposals, and baseline
// restoration is required after the operator's observed close, not
// synchronously at statement return.

// foldStubEngine serves the probe's engine.New registration; the
// admission-only lock path never consults the engine.
type foldStubEngine struct{ engine.Engine }

func (foldStubEngine) New(context.Context, client.TxnOperator) error { return nil }

// foldStubExecutor satisfies the constructor's non-nil gate; the probe
// path never issues stage SQL.
type foldStubExecutor struct{ executor.SQLExecutor }

// foldRetentionWideDef is the wide retention-ledger key shape the
// scratch lock key needs (row_kind, table_id composite).
func foldRetentionWideDef() *plan.TableDef {
	return &plan.TableDef{
		Cols: []*plan.ColDef{
			{Name: catalog.CPrimaryKeyColName, Typ: plan.Type{Id: int32(types.T_varchar), Width: 65535}},
			{Name: catalog.DataBranchLinkRetentionColRowKind, Typ: plan.Type{Id: int32(types.T_int8)}},
			{Name: catalog.DataBranchLinkRetentionColTableID, Typ: plan.Type{Id: int32(types.T_uint64)}},
		},
		Pkey: &plan.PrimaryKeyDef{
			PkeyColName: catalog.CPrimaryKeyColName,
			Names: []string{
				catalog.DataBranchLinkRetentionColRowKind,
				catalog.DataBranchLinkRetentionColTableID,
			},
		},
	}
}

// withholdingTimestampWaiter withholds the snapshot notify until released;
// ctx-done arms the pre-bind cancellation cells (D195(C.1)/(C.2) shapes).
type withholdingTimestampWaiter struct {
	releaseC chan struct{}
	entered  chan struct{}
	once     sync.Once
}

func newWithholdingTimestampWaiter() *withholdingTimestampWaiter {
	return &withholdingTimestampWaiter{
		releaseC: make(chan struct{}),
		entered:  make(chan struct{}),
	}
}

func (w *withholdingTimestampWaiter) GetTimestamp(
	ctx context.Context, ts timestamp.Timestamp,
) (timestamp.Timestamp, error) {
	w.once.Do(func() { close(w.entered) })
	select {
	case <-w.releaseC:
		return ts.Next(), nil
	case <-ctx.Done():
		return timestamp.Timestamp{}, ctx.Err()
	}
}

func (w *withholdingTimestampWaiter) NotifyLatestCommitTS(timestamp.Timestamp) {}
func (w *withholdingTimestampWaiter) Close()                                   {}
func (w *withholdingTimestampWaiter) LatestTS() timestamp.Timestamp {
	return timestamp.Timestamp{}
}

// runFoldProbeFixture boots one real lockservice + txn client and invokes
// fn with a fold substrate on a generation-scoped fencing pool.
func runFoldProbeFixture(
	t *testing.T, service string, waiter client.TimestampWaiter,
	fn func(sub *DataBranchFencingRetireSubstrate, pool *databranchutils.DataBranchFencingPool),
) {
	t.Helper()
	moruntime.RunTest("", func(rt moruntime.Runtime) {
		moruntime.SetupServiceBasedRuntime(service, rt)
		lockservice.RunLockServicesForTest(
			zapcore.DebugLevel,
			[]string{service},
			time.Second,
			func(_ lockservice.LockTableAllocator, services []lockservice.LockService) {
				rt.SetGlobalVariables(moruntime.LockService, services[0])
				rt.SetGlobalVariables(moruntime.MOProtocolVersion, int64(defines.MORPCVersion39))

				sender, err := rpc.NewSender(rpc.Config{}, rt)
				require.NoError(t, err)
				txnClient := client.NewTxnClient(
					service,
					sender,
					client.WithLockService(services[0]),
					client.WithTimestampWaiter(waiter),
				)
				txnClient.Resume()
				defer func() { require.NoError(t, txnClient.Close()) }()

				gen := service + "-gen"
				pool := databranchutils.DataBranchFencingPoolFor(gen)
				start := pool.Dump()
				require.Zero(t, start.Charges, "generation pool starts uncharged")
				sub := NewDataBranchFencingRetireSubstrate(
					foldStubEngine{}, mpool.MustNewZero(), txnClient,
					nil, services[0], nil, nil, nil, nil, foldStubExecutor{},
				)
				require.NotNil(t, sub)
				sub.WithGenerationKey(gen)
				fn(sub, pool)
				end := pool.Dump()
				require.Zero(t, end.Charges, "every probe owner drains by fixture exit")
				require.Zero(t, end.Owners)
			},
			nil,
		)
	})
}

// waitFoldPoolIdle requires the pool charge to drain inside the budget —
// the restoration oracle that rides the operator's OBSERVED close (never
// a fixed sleep as proof).
func waitFoldPoolIdle(
	t *testing.T, pool *databranchutils.DataBranchFencingPool, budget time.Duration, what string,
) {
	t.Helper()
	deadline := time.Now().Add(budget)
	for {
		dump := pool.Dump()
		if dump.Charges == 0 && dump.Owners == 0 && len(dump.Retained) == 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("pool did not restore baseline (%s): %+v", what, dump)
		}
		time.Sleep(time.Millisecond)
	}
}

const (
	foldProbeLedgerID  = 4242
	foldProbeScratchID = 999
)

var farFutureFoldFloor = timestamp.Timestamp{PhysicalTime: math.MaxInt64 / 2}

// D192(C.3) the ordinary green twin with the r92-review-corrected disposal
// oracle: the fold completes on the first sample, the statement returns
// WITHOUT waiting on the probe's detached disposal, and the owner's charge
// restores at the operator's observed close — baseline restoration is
// asserted AFTER the close, not synchronously at statement return.
func TestAwaitLedgerWatermarkGreenFoldRestoresBaseline(t *testing.T) {
	runFoldProbeFixture(t, "s1-e114-fold-green", advancingLoadTimestampWaiter{},
		func(sub *DataBranchFencingRetireSubstrate, pool *databranchutils.DataBranchFencingPool) {
			before := pool.Dump()
			start := time.Now()
			// floor 1ms: every lock-table watermark is newer, so the first
			// sample folds and the loop never runs.
			err := sub.awaitLedgerWatermark(
				context.Background(), 7, foldProbeLedgerID,
				foldRetentionWideDef(), foldProbeScratchID,
				timestamp.Timestamp{PhysicalTime: 1},
			)
			require.NoError(t, err)
			require.Less(t, time.Since(start), 5*time.Second,
				"the green fold never approaches the fold budget")
			after := pool.Dump()
			require.GreaterOrEqual(t, after.ReserveAttempts, before.ReserveAttempts+1,
				"the sample minted exactly one probe owner")
			// The detached disposal is entitled to still be outstanding
			// here; restoration is REQUIRED after the observed close.
			waitFoldPoolIdle(t, pool, 10*time.Second, "probe charge releases at observed close")
		})
}

// D192(C.2) fold-loop variant, exercised SEPARATELY with COMPLETING
// disposals per the review correction: the watermark held below the floor
// polls until the absolute fold deadline fires the named fold-timeout
// class as the statement's determinate error with the caller thread back,
// every minted probe owner charged and detached (no saturation), and the
// pool restored afterwards.
func TestAwaitLedgerWatermarkFoldTimeoutWithCompletingDisposals(t *testing.T) {
	runFoldProbeFixture(t, "s1-e114-fold-timeout", advancingLoadTimestampWaiter{},
		func(sub *DataBranchFencingRetireSubstrate, pool *databranchutils.DataBranchFencingPool) {
			start := time.Now()
			err := sub.awaitLedgerWatermark(
				context.Background(), 7, foldProbeLedgerID,
				foldRetentionWideDef(), foldProbeScratchID, farFutureFoldFloor,
			)
			elapsed := time.Since(start)
			require.Error(t, err)
			require.Contains(t, err.Error(), "data branch fencing: watermark fold timeout",
				"the absolute fold deadline surfaces the named fold-timeout class")
			require.GreaterOrEqual(t, elapsed, dataBranchFencingWatermarkFoldTimeout-time.Second,
				"the return arrives at the deadline, never before")
			require.Less(t, elapsed, dataBranchFencingWatermarkFoldTimeout+10*time.Second,
				"the caller thread is back shortly after the deadline")
			// Completing disposals: thousands of poll iterations each
			// minted and disposed a probe owner; none retains.
			waitFoldPoolIdle(t, pool, 10*time.Second, "fold-timeout probe owners all drained")
		})
}

// D195(C.2) caller-cancellation class: genuine CALLER cancellation keeps
// the ctx.Err() class verbatim — the fold-timeout cause must NOT leak.
func TestAwaitLedgerWatermarkCallerCancelKeepsCtxClass(t *testing.T) {
	runFoldProbeFixture(t, "s1-e114-fold-cancel", advancingLoadTimestampWaiter{},
		func(sub *DataBranchFencingRetireSubstrate, pool *databranchutils.DataBranchFencingPool) {
			ctx, cancel := context.WithCancel(context.Background())
			time.AfterFunc(200*time.Millisecond, cancel)
			start := time.Now()
			err := sub.awaitLedgerWatermark(
				ctx, 7, foldProbeLedgerID,
				foldRetentionWideDef(), foldProbeScratchID, farFutureFoldFloor,
			)
			require.Error(t, err)
			require.True(t, errors.Is(err, context.Canceled),
				"caller cancellation keeps the ctx.Err() class: %v", err)
			require.NotContains(t, err.Error(), "watermark fold timeout")
			require.Less(t, time.Since(start), 5*time.Second,
				"cancellation returns promptly between poll cadences")
			waitFoldPoolIdle(t, pool, 10*time.Second, "cancel-path probe owners drained")
		})
}

// D192(C.2)'s capacity arm, review-corrected: saturation asserts AT the
// configured capacity — exactly LanePoolCapacity charges fill the pool,
// the next probe mint refuses with the saturation class, the fold-await
// surfaces it as a bounded failure, and releasing the holders restores
// the baseline.
func TestAwaitLedgerWatermarkProbeSaturationAtConfiguredCapacity(t *testing.T) {
	runFoldProbeFixture(t, "s1-e114-fold-sat", advancingLoadTimestampWaiter{},
		func(sub *DataBranchFencingRetireSubstrate, pool *databranchutils.DataBranchFencingPool) {
			capacity := pool.Dump().Capacity
			require.Equal(t, 16, capacity,
				"the default lane-pool capacity pins the saturation point")
			holders := make([]*databranchutils.DataBranchStageOwner, 0, capacity)
			for i := 1; i <= capacity; i++ {
				owner, class := pool.MintStageOwner(7777, uint64(i), "saturation-holder")
				require.Equal(t, databranchutils.DataBranchFencingRoundCommitted, class,
					"charge %d of %d mints", i, capacity)
				holders = append(holders, owner)
			}
			_, class := pool.MintLanelessStageOwner(7777, "extra")
			require.Equal(t, databranchutils.DataBranchFencingRoundSaturated, class,
				"saturation at the configured capacity refuses the next reservation")

			err := sub.awaitLedgerWatermark(
				context.Background(), 7, foldProbeLedgerID,
				foldRetentionWideDef(), foldProbeScratchID,
				timestamp.Timestamp{PhysicalTime: 1},
			)
			require.Error(t, err)
			var fenceErr *databranchutils.DataBranchFencingError
			require.True(t, errors.As(err, &fenceErr),
				"the probe's saturation refusal surfaces as the fencing error: %v", err)
			require.Equal(t, databranchutils.DataBranchFencingRoundSaturated, fenceErr.Class)

			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			for _, holder := range holders {
				// Unbound holders reap vacuously: the terminal ABANDON
				// decision plus no-more-lanes releases the charge (D205).
				holder.Abandon(ctx)
				holder.NoteNoMoreLanes()
			}
			waitFoldPoolIdle(t, pool, 10*time.Second, "holders released, baseline restored")
		})
}

// D195(C.1)/(C.2) pre-bind shape: the snapshot notify WITHHELD, the
// CALLER ctx canceled before the fold deadline — bounded return with the
// ctx.Err() class BEFORE the withheld dependency is released, then the
// released notify is a no-op with every baseline exact.
func TestAwaitLedgerWatermarkPreBindCancelWithheldNotify(t *testing.T) {
	waiter := newWithholdingTimestampWaiter()
	runFoldProbeFixture(t, "s1-e114-fold-prebind", waiter,
		func(sub *DataBranchFencingRetireSubstrate, pool *databranchutils.DataBranchFencingPool) {
			ctx, cancel := context.WithCancel(context.Background())
			start := time.Now()
			go func() {
				<-waiter.entered
				time.Sleep(50 * time.Millisecond)
				cancel()
			}()
			err := sub.awaitLedgerWatermark(
				ctx, 7, foldProbeLedgerID,
				foldRetentionWideDef(), foldProbeScratchID,
				timestamp.Timestamp{PhysicalTime: 1},
			)
			require.Error(t, err)
			require.True(t, errors.Is(err, context.Canceled),
				"caller cancellation wins the pre-bind wait: %v", err)
			require.NotContains(t, err.Error(), "watermark fold timeout")
			require.Less(t, time.Since(start), 5*time.Second,
				"the statement returns BEFORE the withheld notify is released")
			// The minted-but-never-bound probe owner reaps VACUOUSLY
			// (D205): no observed close, no notify awaited.
			waitFoldPoolIdle(t, pool, 10*time.Second, "pre-bind probe owner vacuously reaped")
			close(waiter.releaseC)
			waitFoldPoolIdle(t, pool, 10*time.Second, "released notify is a no-op")
		})
}

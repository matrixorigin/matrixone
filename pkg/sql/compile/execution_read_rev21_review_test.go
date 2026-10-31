// Copyright 2026 MatrixOne
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package compile

import (
	"context"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	pblock "github.com/matrixorigin/matrixone/pkg/pb/lock"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	pbtimestamp "github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	pbtxn "github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/txn/rpc"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	lockservice "github.com/matrixorigin/matrixone/pkg/lockservice"
)

// Pre-PR review round 8, finding 2, compile half: the pre-pipeline lockTable
// sweep acquires the staged physical dispositions of promoted marked Shared
// targets, and the sweep must carry the admitted current-read contract for
// exactly those targets — predicted from plan- and scope-derived facts
// alone — while every declinable or unmarked statement keeps today's
// whole-statement outcome byte-identically. This member drives the
// production Compile.lockTable against the real lock service with the
// mid-wait committed-writer shape (a promoted FOR SHARE / LOCK IN SHARE
// MODE reading a writer that committed while it waited), one case per
// gate verdict.
func TestLockTableCarriesMarkedSharedPromotionThroughPrePipeline(t *testing.T) {
	runtime.RunTest(
		"",
		func(rt runtime.Runtime) {
			runtime.SetupServiceBasedRuntime("rev21", rt)
			lockservice.RunLockServicesForTest(
				zap.DebugLevel,
				[]string{"rev21"},
				time.Second,
				func(_ lockservice.LockTableAllocator, services []lockservice.LockService) {
					rt.SetGlobalVariables(runtime.LockService, services[0])

					ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
					defer cancel()

					sender, err := rpc.NewSender(rpc.Config{}, rt)
					require.NoError(t, err)
					txnClient := client.NewTxnClient("", sender, client.WithLockService(services[0]))
					txnClient.Resume()
					defer func() { require.NoError(t, txnClient.Close()) }()

					txnOp, err := txnClient.New(ctx, pbtimestamp.Timestamp{},
						client.WithTxnIsolation(pbtxn.TxnIsolation_SI))
					require.NoError(t, err)
					defer func() { require.NoError(t, txnOp.Rollback(ctx)) }()
					sessionSnapshot := txnOp.Txn().SnapshotTS

					proc := process.NewTopProcess(
						ctx,
						mpool.MustNewZero(),
						txnClient,
						txnOp,
						nil,
						services[0],
						nil,
						nil,
						nil,
						nil,
						nil)
					proc.SetPlanSnapshotTS(sessionSnapshot)

					runtime.SetupServiceRuntimeTestingContext(proc.GetService())
					testingContext := runtime.MustGetTestingContext(proc.GetService())
					testingContext.SetBeforeLockFunc(func(_ []byte, _ uint64) {})
					higherCommit := sessionSnapshot.Next()
					testingContext.SetAdjustLockResultFunc(
						func(_ []byte, table uint64, result *pblock.Result) {
							if table != 42 {
								return
							}
							// The mid-wait committed writer: the promoted
							// shared table lock's waiter wake (the writer
							// committed after the reader's fixed snapshot).
							result.NewLockAdd = true
							result.HasConflict = true
							result.HasPrevCommit = true
							result.Timestamp = higherCommit
						})
					defer testingContext.SetAdjustLockResultFunc(nil)
					defer testingContext.SetBeforeLockFunc(nil)

					markedScan := func(id int32) *plan.Node {
						return &plan.Node{
							NodeId:       id,
							NodeType:     plan.Node_TABLE_SCAN,
							TableDef:     &plan.TableDef{TableType: "r", TblId: 42},
							ScanReadRole: plan.ScanReadRole_ScanOuterLocking,
						}
					}

					// The staged physical disposition of the promoted marked
					// Shared target — exactly what compileLock stages under the
					// rev-20 contract (c.lockTables), with the bind-time
					// marked provenance retained in the canonical plan.
					staged := &plan.LockTarget{
						TableId:       42,
						PrimaryColTyp: plan.Type{Id: int32(types.T_int32)},
						Mode:          pblock.LockMode_Shared,
						LockingRead:   true,
					}

					newCompile := func(pn *plan.Plan) *Compile {
						c := &Compile{proc: proc}
						c.addr = "cn-local"
						c.pn = pn
						c.lockTables = map[uint64]*plan.LockTarget{42: staged}
						return c
					}

					// (1) The admitted marked promoted shared statement: the
					// production sweep acquires the staged table lock WITH
					// the marked current-read contract and succeeds on the
					// mid-wait committed writer, with the fixed transaction
					// snapshot untouched — the reader then re-acquires its
					// read frontier after the grant and current-reads the
					// committed value.
					c := newCompile(presencePlan(markedScan(0)))
					require.NoError(t, c.lockTable(),
						"the promoted marked shared statement survives the mid-wait committed writer")
					require.True(t, txnOp.HasLockTable(42),
						"the staged table lock is really held by the reader transaction")
					require.Equal(t, sessionSnapshot, txnOp.Txn().SnapshotTS,
						"the marked pre-pipeline acquisition never moves the fixed SI snapshot")

					// (2) A predictable declinature conserves today's
					// whole-statement lock outcome (G4): a vanished mark
					// (LostMark in the R14-F2 closure walk) answers the
					// legacy call and keeps today's terminal conflict.
					lost := &plan.Node{
						NodeId:       1,
						NodeType:     plan.Node_TABLE_SCAN,
						TableDef:     &plan.TableDef{TableType: "r", TblId: 42},
						ScanReadRole: plan.ScanReadRole_ScanNestedBlock,
					}
					c = newCompile(presencePlan(markedScan(0), lost))
					err = c.lockTable()
					require.Error(t, err,
						"a vanished mark conserves today's terminal outcome on the same shape")
					require.True(t, moerr.IsMoErrCode(err, moerr.ErrTxnWWConflict), "got %v", err)
					require.Equal(t, sessionSnapshot, txnOp.Txn().SnapshotTS)

					// (3) A remote fragment predicts the remote declinature:
					// the same conserving terminal outcome.
					c = newCompile(presencePlan(markedScan(0)))
					c.scopes = []*Scope{{NodeInfo: engine.Node{Addr: "cn-remote"}}}
					err = c.lockTable()
					require.Error(t, err,
						"a remote placement conserves today's terminal outcome conservatively")
					require.True(t, moerr.IsMoErrCode(err, moerr.ErrTxnWWConflict), "got %v", err)

					// (4) An already-disabled attempt (the G4 rollback
					// already happened for a prior scope) is never re-marked.
					c = newCompile(presencePlan(markedScan(0)))
					c.execReadDisabled = true
					err = c.lockTable()
					require.Error(t, err, "a disabled attempt keeps the legacy call")
					require.True(t, moerr.IsMoErrCode(err, moerr.ErrTxnWWConflict), "got %v", err)

					// (5) A statement without bind-time presence at all (the
					// below-gate shape) keeps today's legacy behavior on the
					// same staged disposition.
					c = newCompile(&plan.Plan{Plan: &plan.Plan_Query{Query: &plan.Query{
						Nodes: []*plan.Node{{
							NodeType:    plan.Node_LOCK_OP,
							LockTargets: []*plan.LockTarget{{TableId: 42}},
						}},
					}}})
					err = c.lockTable()
					require.Error(t, err,
						"an unmarked statement keeps today's terminal outcome on the same shape")
					require.True(t, moerr.IsMoErrCode(err, moerr.ErrTxnWWConflict), "got %v", err)
					require.Equal(t, sessionSnapshot, txnOp.Txn().SnapshotTS,
						"no legacy terminal outcome moved the fixed SI snapshot either")
				},
				nil,
			)
		},
	)
}

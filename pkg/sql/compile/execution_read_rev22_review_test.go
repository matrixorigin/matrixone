// Copyright 2026 Matrix Origin
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

// rev22ValidWorkspace is the fixture workspace for the prelock admission
// member: disttae's txnIsValid only requires a non-nil workspace for a
// non-disttae.Transaction workspace kind and never calls into it during
// the statement-readiness validation, so the embedded-interface stub keeps
// the fixture operator workspace-viable without the synthetic disttae
// internals a rollback would dereference. The fixture never rolls the
// operator back; the txn client closes at teardown.
type rev22ValidWorkspace struct {
	client.Workspace
}

// Pre-PR review round 9, compile half: the whole-statement admission
// decision — including RELATION SUPPORT for every scan the
// source-initialization sweeps would bind — must be FINAL before
// lockTable() chooses the pre-pipeline lock options. A statement whose
// admitted scan's relation binding soft-refuses (the reviewer's
// transaction-created-relation class and every other excluded kind)
// declines the whole attempt BEFORE the promoted table lock can acquire
// the marked current-read contract, so a mid-wait committed writer
// surfaces today's terminal conflict instead of a suppressed conflict the
// same statement would afterwards restore onto snapshot semantics
// (D-10 clause 3/G4). The reverse ordering hole is exactly what round 8's
// gate left open; this member drives the production probe and the
// production lockTable() over the real lock service with the mid-wait
// committed-writer shape, one case per verdict, mirroring the round-21
// member's fixture.
func TestExecutionReadResolveRelationSupportFinalizesAdmissionBeforeLockOptions(t *testing.T) {
	runtime.RunTest(
		"",
		func(rt runtime.Runtime) {
			runtime.SetupServiceBasedRuntime("rev22", rt)
			lockservice.RunLockServicesForTest(
				zap.DebugLevel,
				[]string{"rev22"},
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
					// The prelock resolution resolves each admitted scan's
					// relation through the SAME operator/ctx construction
					// source initialization uses, so the fixture transaction
					// must be workspace-viable exactly like a real
					// statement. The stub workspace satisfies the validity
					// check without being a disttae.Transaction (whose
					// synthetic rollback would dereference absent
					// internals: txnIsValid only asserts a non-nil workspace
					// for non-disttan kinds); the fixture never rolls the
					// operator back and closes the client at teardown.
					txnOp.AddWorkspace(&rev22ValidWorkspace{})
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

					// The staged physical disposition of the promoted marked
					// Shared target, exactly what compileLock stages under
					// the rev-20 contract.
					staged := &plan.LockTarget{
						TableId:       42,
						PrimaryColTyp: plan.Type{Id: int32(types.T_int32)},
						Mode:          pblock.LockMode_Shared,
						LockingRead:   true,
					}

					newCompile := func(rel engine.Relation) *Compile {
						c := &Compile{proc: proc}
						c.addr = "cn-local"
						c.e = &reviewBarrierEngine{
							database: &reviewFakeDatabase{
								byName: map[string]engine.Relation{"t": rel},
							},
						}
						c.pn = presencePlan(reviewMarkedScan(0, "t"))
						c.lockTables = map[uint64]*plan.LockTarget{42: staged}
						c.scopes = []*Scope{{
							DataSource: &Source{node: reviewMarkedScan(0, "t")},
						}}
						return c
					}

					// (1) DECLINED: the admitted scan's relation binding
					// soft-refuses (the round-9 prelock resolution), the
					// FINAL whole-statement admission decision is shared
					// with source initialization (execReadDisabled is the
					// one declinature state both consume), the marked set
					// answers nil, and the staged table lock acquires
					// TODAY'S legacy options: the mid-wait committed writer
					// surfaces today's terminal conflict with the fixed SI
					// snapshot untouched.
					refusing := &reviewExclusiveRelation{refuse: true}
					c := newCompile(refusing)
					require.NoError(t, c.executionReadResolveRelationSupport(),
						"the prelock resolution itself never fails")
					require.True(t, c.execReadDisabled,
						"a refusing relation finalizes the whole-statement decline BEFORE the lock options are chosen")
					require.NotEmpty(t, c.execReadDisableReason)
					require.Nil(t, c.markedPrePipelineLockTables(),
						"the finalized decline removes every marked pre-pipeline option")
					err = c.lockTable()
					require.Error(t, err,
						"the declined statement keeps today's terminal legacy conflict on the mid-wait committed writer")
					require.True(t, moerr.IsMoErrCode(err, moerr.ErrTxnWWConflict), "got %v", err)
					require.Equal(t, sessionSnapshot, txnOp.Txn().SnapshotTS,
						"the legacy acquisition never moves the fixed SI snapshot")

					// (2) ADMITTED control: a SUPPORTING relation leaves the
					// marked route chosen (the same reviewMarkedScan, with
					// the relation binding refusing nothing), and the same
					// mid-wait committed writer is served by the round-8
					// marked current-read contract: the acquisition
					// succeeds, the table lock is held, and the fixed SI
					// snapshot is never advanced.
					supporting := &reviewExclusiveRelation{}
					c2 := newCompile(supporting)
					require.NoError(t, c2.executionReadResolveRelationSupport())
					require.False(t, c2.execReadDisabled,
						"a supporting relation declines nothing")
					require.Len(t, c2.markedPrePipelineLockTables(), 1,
						"the marked pre-pipeline option stays chosen for the admitted statement")
					require.NoError(t, c2.lockTable(),
						"the admitted marked promoted shared statement survives the mid-wait committed writer")
					require.True(t, txnOp.HasLockTable(42),
						"the marked staged table lock is really held by the reader transaction")
					require.Equal(t, sessionSnapshot, txnOp.Txn().SnapshotTS,
						"the marked pre-pipeline acquisition never moves the fixed SI snapshot")

					// (3) The probe resolves the relation through the SAME
					// exclusive-handle construction source initialization
					// uses: the refusing relation was probed (not bound) —
					// its bind counter stays zero and its handle never
					// re-based.
					require.Equal(t, 0, refusing.binds,
						"the prelock probe never consumes a binding")
					require.Nil(t, c.scopes[0].DataSource.Rel,
						"the prelock probe never installs a relation handle into the scope")
				},
				nil,
			)
		},
	)
}

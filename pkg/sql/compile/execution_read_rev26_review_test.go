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

// PR-review P2 (aunjgr, "Preserve historical lock semantics when promoted
// aliases share a table"): compileLock collapses every promoted occurrence
// of one physical table into the single staged acquisition
// (c.lockTables[TableId]) and drops that table's row targets, so NO
// per-occurrence validation survives the collapse for the mixed shape. The
// staged contract must therefore resolve from ALL lock occurrences that
// collapse into the acquisition, never from an any-admitted-table test: a
// same-physical-table current + engaging-historical promoted join (LOCK IN
// SHARE MODE) has its current occurrence admitted while the historical
// occurrence keeps its occurrence-level exemption (an unmarked/nested
// promoted occurrence keeps today's obligation the same way), and one
// acquisition cannot carry the marked current-read contract while
// preserving the historical occurrence's legacy wait-resolved conflict —
// so the whole acquisition conservatively declines to today's legacy
// options (G3 lock acquisition unchanged; G4 one acquisition never mixes
// contracts). These members pin the resolver over the collapsed-occurrence
// universes, the acquisition's observed mid-wait committed conflict, and
// the non-promoted control whose per-occurrence isolation stays untouched.

import (
	"context"
	"slices"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	lockservice "github.com/matrixorigin/matrixone/pkg/lockservice"
	pblock "github.com/matrixorigin/matrixone/pkg/pb/lock"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	pbtxn "github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/txn/rpc"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

// occurrenceScan builds one locked table occurrence's scan node: the scan
// role and the scan snapshot are exactly what binding emits for a locked
// join alias, and the table id names the PHYSICAL table every alias of the
// same base relation shares.
func occurrenceScan(id int32, tableID uint64, role plan.ScanReadRole, snap *timestamp.Timestamp) *plan.Node {
	n := &plan.Node{
		NodeId:       id,
		NodeType:     plan.Node_TABLE_SCAN,
		TableDef:     &plan.TableDef{TableType: "r", TblId: tableID},
		ScanReadRole: role,
		ObjRef:       &plan.ObjectRef{SchemaName: "test", ObjName: "t"},
	}
	if snap != nil {
		n.ScanSnapshot = &plan.Snapshot{TS: snap}
	}
	return n
}

// occurrenceTargetsPlan builds the canonical locked-join plan shape: every
// scan occurrence contributes its own per-occurrence LockTarget — presence
// and raw explicit-snapshot provenance per target, `collectLockTargets` +
// `applyLockTableFallback` emission order — over one physical table, with
// the promoted dispositions the cardinality fallback flips.
func occurrenceTargetsPlan(scans []*plan.Node, targets ...*plan.LockTarget) *plan.Plan {
	qry := &plan.Query{Nodes: append(slices.Clone(scans), &plan.Node{
		NodeType:    plan.Node_LOCK_OP,
		LockTargets: targets,
	})}
	return &plan.Plan{Plan: &plan.Plan_Query{Query: qry}}
}

func promotedCurrentTarget(tableID uint64) *plan.LockTarget {
	return &plan.LockTarget{TableId: tableID, Mode: pblock.LockMode_Shared, LockingRead: true, LockTable: true}
}

func promotedExplicitTarget(tableID uint64, h timestamp.Timestamp) *plan.LockTarget {
	return &plan.LockTarget{
		TableId:                  tableID,
		Mode:                     pblock.LockMode_Shared,
		LockingRead:              true,
		LockTable:                true,
		ExplicitSnapshotPhysical: h.PhysicalTime,
		ExplicitSnapshotLogical:  h.LogicalTime,
	}
}

// TestSameTablePromotedJoinDeclinesMarkedStagedLockFromCollapsedOccurrences
// resolves the staged contract of the single acquisition from the FULL
// promoted occurrence set that compileLock collapses into it.
func TestSameTablePromotedJoinDeclinesMarkedStagedLockFromCollapsedOccurrences(t *testing.T) {
	sessionTS := timestamp.Timestamp{PhysicalTime: 100, LogicalTime: 1}
	c, _, teardown := admissionFixture(t, sessionTS)
	defer teardown()

	engagingH := timestamp.Timestamp{PhysicalTime: 50, LogicalTime: 1}
	laterH := timestamp.Timestamp{PhysicalTime: 150, LogicalTime: 1}

	currentScan := occurrenceScan(0, 42, plan.ScanReadRole_ScanOuterLocking, nil)
	currentAlias := occurrenceScan(1, 42, plan.ScanReadRole_ScanOuterLocking, nil)
	engagingScan := occurrenceScan(2, 42, plan.ScanReadRole_ScanOuterLockingWithExplicitSnapshot, &engagingH)
	laterScan := occurrenceScan(3, 42, plan.ScanReadRole_ScanOuterLockingWithExplicitSnapshot, &laterH)

	// Occurrence-level admission sanity: the current occurrence claims table
	// 42 for this execution; the engaging-historical occurrence does not.
	require.True(t, c.executionReadScanAdmitted(currentScan))
	require.False(t, c.executionReadScanAdmitted(engagingScan))

	// (1) The reviewer's shape: BOTH occurrences of the same physical table
	// promote into the single staged acquisition. The historical occurrence
	// keeps its contract through the collapse — the acquisition declines.
	c.pn = occurrenceTargetsPlan(
		[]*plan.Node{currentScan, engagingScan},
		promotedCurrentTarget(42), promotedExplicitTarget(42, engagingH))
	require.Equal(t, map[uint64]struct{}{42: {}},
		c.executionReadStagedLegacyObligations(c.pn.GetQuery()),
		"the collapsed engaging-historical occurrence carries the legacy obligation")
	require.Empty(t, c.markedPrePipelineLockTables(),
		"a current alias must not turn off the collapsed historical occurrence's exemption")

	// (2) The nested-unmarked promoted occurrence of the same table — the
	// promoted variant of the round-10 finding-2 shape — declines the same
	// way: the collapsed occurrence binds no current-read contract.
	c.pn = occurrenceTargetsPlan(
		[]*plan.Node{currentScan, occurrenceScan(2, 42, plan.ScanReadRole_ScanNestedBlockExplicit, nil)},
		promotedCurrentTarget(42), &plan.LockTarget{TableId: 42, Mode: pblock.LockMode_Shared, LockTable: true})
	require.Empty(t, c.markedPrePipelineLockTables(),
		"a collapsed occurrence without locking-current-read presence keeps the acquisition legacy")

	// (3) The all-admitted same-table promoted join keeps the marked options:
	// two ordinary current aliases both collapse, no occurrence is exempt.
	c.pn = occurrenceTargetsPlan(
		[]*plan.Node{currentScan, currentAlias},
		promotedCurrentTarget(42), promotedCurrentTarget(42))
	require.Empty(t, c.executionReadStagedLegacyObligations(c.pn.GetQuery()))
	require.Contains(t, c.markedPrePipelineLockTables(), uint64(42),
		"the all-admitted promoted same-table join keeps the round-8 marked options")

	// (4) A NON-engaging collapsed occurrence (H >= executing SnapshotTS)
	// keeps the marked options: the decline only follows real engagement.
	c.pn = occurrenceTargetsPlan(
		[]*plan.Node{currentScan, laterScan},
		promotedCurrentTarget(42), promotedExplicitTarget(42, laterH))
	require.Contains(t, c.markedPrePipelineLockTables(), uint64(42),
		"the non-engaging occurrence is an admitted current read for this execution")

	// (5) Historical-only promoted stays legacy — the both-promoted variant
	// of the round-10 historical member: no admitted claim exists at all.
	c.pn = occurrenceTargetsPlan(
		[]*plan.Node{engagingScan},
		promotedExplicitTarget(42, engagingH))
	require.Empty(t, c.markedPrePipelineLockTables(),
		"the historical-only promoted statement keeps today's legacy options")

	// (6) The declination is per acquisition: a legacy obligation on ANOTHER
	// physical table never declines this table's marked acquisition. Table
	// 42 has two promoted current occurrences; table 43 additionally
	// carries a collapsed engaging-historical occurrence.
	otherCurrentScan := occurrenceScan(4, 42, plan.ScanReadRole_ScanOuterLocking, nil)
	unrelatedCurrentScan := occurrenceScan(5, 43, plan.ScanReadRole_ScanOuterLocking, nil)
	unrelatedEngagingScan := occurrenceScan(6, 43, plan.ScanReadRole_ScanOuterLockingWithExplicitSnapshot, &engagingH)
	c.pn = occurrenceTargetsPlan(
		[]*plan.Node{otherCurrentScan, unrelatedCurrentScan, unrelatedEngagingScan},
		promotedCurrentTarget(42), promotedCurrentTarget(43), promotedExplicitTarget(43, engagingH))
	marked := c.markedPrePipelineLockTables()
	require.Contains(t, marked, uint64(42),
		"the fully-admitted acquisition on its own physical table keeps its contract")
	require.NotContains(t, marked, uint64(43),
		"the acquisition whose collapsed occurrence is historical keeps the legacy options")

	// (7) A ROW occurrence never participates in the staged declination:
	// the engaging-historical occurrence that stayed a row target keeps its
	// own per-occurrence resolution in the lock operator, so the promoted
	// current occurrence's acquisition still carries its marked contract.
	c.pn = occurrenceTargetsPlan(
		[]*plan.Node{currentScan, engagingScan},
		promotedCurrentTarget(42),
		&plan.LockTarget{
			TableId:                  42,
			Mode:                     pblock.LockMode_Shared,
			LockingRead:              true,
			ExplicitSnapshotPhysical: engagingH.PhysicalTime,
			ExplicitSnapshotLogical:  engagingH.LogicalTime,
		})
	require.Contains(t, c.markedPrePipelineLockTables(), uint64(42),
		"only occurrences collapsed into the acquisition resolve its contract")
}

// The NON-PROMOTED control (the reviewer's requested companion): the same
// same-physical-table current + engaging-historical occurrence pair stays
// on the ROW path, and the lock operator keeps per-occurrence isolation
// exactly as round 10 finding 2 / rev 24 demand — the current occurrence's
// marks act for this attempt while the engaging-historical occurrence stays
// inert, so the promotion-collapse member above can never be excused by a
// per-occurrence validation that a promoted shape would have retained.
func TestSameTableMixedJoinRowControlKeepsPerOccurrenceMarks(t *testing.T) {
	sessionTS := timestamp.Timestamp{PhysicalTime: 100, LogicalTime: 1}
	c, _, teardown := admissionFixture(t, sessionTS)
	defer teardown()

	engagingH := timestamp.Timestamp{PhysicalTime: 50, LogicalTime: 1}
	// The row occurrences: neither target is promoted, so both keep their
	// own per-occurrence contract in the lock operator.
	arg, err := constructLockOp(&plan.Node{
		NodeType: plan.Node_LOCK_OP,
		LockTargets: []*plan.LockTarget{
			{TableId: 42, Mode: pblock.LockMode_Shared, LockingRead: true},
			{
				TableId:                  42,
				Mode:                     pblock.LockMode_Shared,
				LockingRead:              true,
				ExplicitSnapshotPhysical: engagingH.PhysicalTime,
				ExplicitSnapshotLogical:  engagingH.LogicalTime,
			},
		},
	}, nil)
	require.NoError(t, err)
	defer arg.Release()

	// Unstamped: neither row occurrence carries the marked contract.
	targets := arg.CopyToPipelineTarget()
	require.False(t, targets[0].GetLockingRead())
	require.False(t, targets[1].GetLockingRead())

	// The attempt stamp resolves per occurrence against the executing
	// transaction's SnapshotTS: the current occurrence keeps the marked
	// contract for this attempt, the engaging-historical occurrence stays
	// exempt — its wire presence stays off on the ROW path, unlike the
	// promoted collapse the declination above guards.
	c.pn = occurrenceTargetsPlan(
		[]*plan.Node{occurrenceScan(0, 42, plan.ScanReadRole_ScanOuterLocking, nil)},
		promotedCurrentTarget(42))
	c.scopes = []*Scope{{RootOp: arg}}
	c.execReadFrontierSet = true
	c.execReadFrontier = timestamp.Timestamp{PhysicalTime: 777, LogicalTime: 5}
	c.executionReadStampMarkingLockScopes()
	targets = arg.CopyToPipelineTarget()
	require.True(t, targets[0].GetLockingRead(),
		"the current row occurrence acts marked for this attempt")
	require.False(t, targets[1].GetLockingRead(),
		"the engaging-historical row occurrence stays exempt — per-occurrence resolution survives on the row path")
}

// The reviewer's requested ACQUISITION regression: the same-physical-table
// current + engaging-historical promoted join meets an observed mid-wait
// committed writer on its shared staged acquisition, and must surface
// TODAY'S terminal legacy conflict — the marked suppression must not ride
// the single acquisition a current alias happened to share. The current-only
// promoted control keeps the marked contract on the same fixture, proving
// the declination follows the collapsed historical occurrence alone.
func TestSameTablePromotedJoinKeepsLegacyAcquisitionOnMidWaitCommittedWriter(t *testing.T) {
	runtime.RunTest(
		"",
		func(rt runtime.Runtime) {
			runtime.SetupServiceBasedRuntime("rev26", rt)
			lockservice.RunLockServicesForTest(
				zap.DebugLevel,
				[]string{"rev26"},
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

					txnOp, err := txnClient.New(ctx, timestamp.Timestamp{},
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
							// The mid-wait committed writer observed on the
							// shared staged acquisition's waiter wake.
							result.NewLockAdd = true
							result.HasConflict = true
							result.HasPrevCommit = true
							result.Timestamp = higherCommit
						})
					defer testingContext.SetAdjustLockResultFunc(nil)
					defer testingContext.SetBeforeLockFunc(nil)

					// An engaging-historical occurrence: nonzero and strictly
					// older than the executing snapshot.
					engagingH := timestamp.Timestamp{
						PhysicalTime: sessionSnapshot.PhysicalTime - 1,
					}
					require.True(t, !engagingH.IsEmpty() && engagingH.Less(sessionSnapshot),
						"fixture validity: the stored H engages the historical cohort")

					currentScan := occurrenceScan(0, 42, plan.ScanReadRole_ScanOuterLocking, nil)
					currentAlias := occurrenceScan(1, 42, plan.ScanReadRole_ScanOuterLocking, nil)
					engagingScan := occurrenceScan(2, 42, plan.ScanReadRole_ScanOuterLockingWithExplicitSnapshot, &engagingH)

					// The staged physical disposition compileLock leaves in
					// c.lockTables for the collapsed table: one acquisition
					// whose per-target provenance survives the copy.
					staged := &plan.LockTarget{
						TableId:       42,
						PrimaryColTyp: plan.Type{Id: int32(types.T_int32)},
						Mode:          pblock.LockMode_Shared,
						LockingRead:   true,
					}

					newCompile := func(pn *plan.Plan) *Compile {
						c := &Compile{proc: proc}
						c.addr = "cn-local"
						c.e = &reviewBarrierEngine{
							database: &reviewFakeDatabase{
								byName: map[string]engine.Relation{"t": &reviewExclusiveRelation{}},
							},
						}
						c.pn = pn
						c.lockTables = map[uint64]*plan.LockTarget{42: staged}
						return c
					}

					// (1) The MIXED same-table promoted join: one current
					// alias plus one engaging-historical alias of the SAME
					// physical table, both promoted into the single staged
					// acquisition. The resolver declines the acquisition:
					// the whole staged lock stays legacy, and the observed
					// mid-wait committed writer surfaces today's terminal
					// conflict exactly as the historical occurrence's
					// exemption requires — the statement cannot succeed
					// with a historical value where a current alias of the
					// same table switched the acquisition to the marked
					// suppression.
					mixed := newCompile(occurrenceTargetsPlan(
						[]*plan.Node{currentScan, engagingScan},
						promotedCurrentTarget(42), promotedExplicitTarget(42, engagingH)))
					require.NoError(t, mixed.executionReadResolveRelationSupport(),
						"the declined acquisition skips the marked-route relation probe")
					require.Empty(t, mixed.markedPrePipelineLockTables(),
						"the shared acquisition cannot carry both contracts")
					err = mixed.lockTable()
					require.Error(t, err,
						"the mixed same-table promoted join keeps today's terminal conflict on the mid-wait committed writer")
					require.True(t, moerr.IsMoErrCode(err, moerr.ErrTxnWWConflict), "got %v", err)
					require.Equal(t, sessionSnapshot, txnOp.Txn().SnapshotTS,
						"the legacy acquisition never moves the fixed SI snapshot")

					// (2) The CONTROL: the same two-alias promoted join with
					// BOTH occurrences current (no engaging historical
					// occurrence) keeps the marked contract — the acquisition
					// survives the identical mid-wait committed writer, so
					// (1)'s terminal outcome follows the collapsed historical
					// occurrence, not some broader declination.
					currentOnly := newCompile(occurrenceTargetsPlan(
						[]*plan.Node{currentScan, currentAlias},
						promotedCurrentTarget(42), promotedCurrentTarget(42)))
					require.NoError(t, currentOnly.executionReadResolveRelationSupport())
					require.Len(t, currentOnly.markedPrePipelineLockTables(), 1,
						"the all-current promoted join keeps its marked acquisition")
					require.NoError(t, currentOnly.lockTable(),
						"the admitted marked promoted statement survives the mid-wait committed writer")
					require.True(t, txnOp.HasLockTable(42),
						"the marked staged table lock is really held by the reader transaction")
					require.Equal(t, sessionSnapshot, txnOp.Txn().SnapshotTS,
						"the marked pre-pipeline acquisition never moves the fixed SI snapshot")
				},
				nil,
			)
		},
	)
}

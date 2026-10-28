// Copyright 2026 MatrixOne
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
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	pbtxn "github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/perfcounter"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/dependent"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/lockop"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/merge"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae"
	"github.com/stretchr/testify/require"
)

// ---------------------------------------------------------------------------
// pre-PR review round 2 members: lazy-scope pre-admission (P1), the dependent
// (correlation) integration (P2), deep-copy presence (P2), the retry-frontier
// matrix, snapshot immutability and the cold Run-admission capability site.

// reviewBarrierEngine is the fixture engine for admission driven end to end:
// the publication barrier answers a deterministic frontier and the database
// layer routes relation opens to prepared exclusive-handle fakes.
type reviewBarrierEngine struct {
	engine.Engine
	frontier timestamp.Timestamp
	database engine.Database
}

func (e *reviewBarrierEngine) AcquireLogtailReadBarrier(ctx context.Context) (timestamp.Timestamp, error) {
	return e.frontier, nil
}

func (e *reviewBarrierEngine) Database(ctx context.Context, name string, op client.TxnOperator) (engine.Database, error) {
	return e.database, nil
}

// reviewFakeDatabase routes every relation open to the prepared fake table.
type reviewFakeDatabase struct {
	engine.Database
	byName map[string]engine.Relation
}

func (db *reviewFakeDatabase) Relation(ctx context.Context, name string, proc any) (engine.Relation, error) {
	rel, ok := db.byName[name]
	if !ok {
		return nil, moerr.NewInternalErrorNoCtxf("unknown test relation %s", name)
	}
	return rel, nil
}

// reviewExclusiveRelation is the EXCLUSIVE handle fake: bindable onto an
// execution carrier, Reset-able back onto the session, and able to refuse
// (soft) or raise a hard terminal binding failure (rev 17).
type reviewExclusiveRelation struct {
	engine.Relation
	binds   int
	resets  int
	refuse  bool
	hardErr error
	ranged  int
	readers int
}

func (r *reviewExclusiveRelation) BindExecutionReadOp(op client.TxnOperator) error {
	r.binds++
	if r.hardErr != nil {
		return r.hardErr
	}
	if r.refuse {
		return &engine.ExecutionReadUnsupportedError{Detail: "relation handling"}
	}
	return nil
}

// ProbeExecutionReadSupport mirrors the real delegate's probe (pre-PR review
// round 22): the SOFT refusal predict's bind's refusal exactly, without
// consuming a bind slot or surfacing bind-time hard failures.
func (r *reviewExclusiveRelation) ProbeExecutionReadSupport() error {
	if r.refuse {
		return &engine.ExecutionReadUnsupportedError{Detail: "relation handling"}
	}
	return nil
}

func (r *reviewExclusiveRelation) Reset(op client.TxnOperator) error {
	r.resets++
	return nil
}

func (r *reviewExclusiveRelation) NewRelationHandle() engine.Relation { return r }

func (r *reviewExclusiveRelation) Ranges(ctx context.Context, param engine.RangesParam) (engine.RelData, error) {
	r.ranged++
	return nil, nil
}

func (r *reviewExclusiveRelation) BuildReaders(ctx context.Context, proc any, expr *plan.Expr,
	relData engine.RelData, num int, txnOffset int, orderBy bool, policy engine.TombstoneApplyPolicy,
	filterHint engine.FilterHint) ([]engine.Reader, error) {
	r.readers++
	return nil, nil
}

func reviewMarkedScan(nodeID int32, tableName string) *plan.Node {
	return &plan.Node{
		NodeId:       nodeID,
		NodeType:     plan.Node_TABLE_SCAN,
		TableDef:     &plan.TableDef{TableType: "r", Name: tableName, TblId: 42},
		ObjRef:       &plan.ObjectRef{SchemaName: "test", ObjName: tableName},
		ScanReadRole: plan.ScanReadRole_ScanOuterLocking,
	}
}

func reviewLockOp() *lockop.LockOp {
	op, err := constructLockOp(&plan.Node{
		NodeType:    plan.Node_LOCK_OP,
		LockTargets: []*plan.LockTarget{{TableId: 42, LockingRead: true}},
	}, nil)
	if err != nil {
		panic(err)
	}
	return op
}

// ---------------------------------------------------------------------------
// P1: lazy branch admission completes BEFORE the stamp sweep publishes.

// The lazy pre-admission invariant through the REAL runPipelineAttempt
// sequence: a LazyPreScopes union whose branches carry marked scans admits,
// acquires the frontier and binds each branch relation BEFORE any branch
// could start, so the stamp sweep publishes a real frontier to every branch
// LockOp — a fully-lazy marked statement previously stamped nothing and a
// commit while waiting fell back to legacy SI conflict handling.
func TestExecutionReadLazyScopesAdmitBeforeStampSweep(t *testing.T) {
	sessionTS := timestamp.Timestamp{PhysicalTime: 100, LogicalTime: 1}
	frontier := timestamp.Timestamp{PhysicalTime: 999, LogicalTime: 9}
	c, _, teardown := admissionFixture(t, sessionTS)
	defer teardown()
	c.disableLock = true
	c.e = &reviewBarrierEngine{frontier: frontier,
		database: &reviewFakeDatabase{byName: map[string]engine.Relation{}}}
	c.pn = presencePlan(reviewMarkedScan(0, "t"))

	branchFixture := func() (*Scope, *reviewExclusiveRelation, *lockop.LockOp) {
		rel := &reviewExclusiveRelation{}
		op := reviewLockOp()
		return &Scope{
			RootOp:     op,
			DataSource: &Source{node: reviewMarkedScan(0, "t"), Rel: rel},
		}, rel, op
	}
	brA, relA, opA := branchFixture()
	brB, relB, opB := branchFixture()
	root := &Scope{RootOp: merge.NewArgument(), LazyPreScopes: true, PreScopes: []*Scope{brA, brB}}
	c.scopes = []*Scope{root}

	err := c.runPipelineAttempt(func() error {
		active, got := c.executionReadStatementFrontier()
		require.True(t, active, "the lazy branches' admission established the frontier before pipelines")
		require.Equal(t, frontier, got)

		for _, member := range []struct {
			s   *Scope
			rel *reviewExclusiveRelation
			op  *lockop.LockOp
		}{{brA, relA, opA}, {brB, relB, opB}} {
			require.Equal(t, 1, member.rel.binds,
				"each lazily initialized marked scan is bound before its branch starts")
			require.Equal(t, frontier, member.s.DataSource.Timestamp,
				"the lazy branch's data source is stamped at the frontier")
			targets := member.op.CopyToPipelineTarget()
			require.True(t, targets[0].GetLockingRead(),
				"the stamp sweep reached the lazy branch LockOps once the frontier exists")
			require.Equal(t, frontier.PhysicalTime, targets[0].GetCurrentReadFrontierPhysical())
			require.Equal(t, frontier.LogicalTime, targets[0].GetCurrentReadFrontierLogical())
		}
		require.Len(t, c.execReadBoundScopes, 2)
		return nil
	})
	require.NoError(t, err)
}

// The G4 half of the lazy fix: a later branch's binding refusal declines
// the WHOLE statement at pre-pipeline time — before any branch produced
// rows — resetting the already-bound branch handles onto the session
// operator and leaving every LockOp unstamped (all-or-nothing).
func TestExecutionReadLazyScopeRefusalDeclinesWholeStatement(t *testing.T) {
	sessionTS := timestamp.Timestamp{PhysicalTime: 100, LogicalTime: 1}
	frontier := timestamp.Timestamp{PhysicalTime: 999, LogicalTime: 9}
	c, _, teardown := admissionFixture(t, sessionTS)
	defer teardown()
	c.disableLock = true
	c.e = &reviewBarrierEngine{frontier: frontier,
		database: &reviewFakeDatabase{byName: map[string]engine.Relation{}}}
	c.pn = presencePlan(reviewMarkedScan(0, "t"))

	branch := func(refuse bool) (*Scope, *reviewExclusiveRelation, *lockop.LockOp) {
		rel := &reviewExclusiveRelation{refuse: refuse}
		op := reviewLockOp()
		return &Scope{
			RootOp:     op,
			DataSource: &Source{node: reviewMarkedScan(0, "t"), Rel: rel},
		}, rel, op
	}
	brA, relA, _ := branch(false)
	brB, relB, lockB := branch(true)
	root := &Scope{RootOp: merge.NewArgument(), LazyPreScopes: true, PreScopes: []*Scope{brA, brB}}
	c.scopes = []*Scope{root}

	err := c.runPipelineAttempt(func() error {
		require.True(t, c.execReadDisabled,
			"the refusing branch declines the whole statement before any branch runs")
		require.NotEmpty(t, c.execReadDisableReason)
		active, _ := c.executionReadStatementFrontier()
		require.False(t, active, "the decline retires the frontier")
		require.GreaterOrEqual(t, relA.resets, 1,
			"the already-bound branch is reset onto the session operator (G4 rollback)")
		require.Equal(t, 1, relB.binds,
			"the refusing branch's binding is attempted exactly once and refuses")
		require.Empty(t, c.execReadBoundScopes)
		targets := lockB.CopyToPipelineTarget()
		require.False(t, targets[0].GetLockingRead(),
			"unstamped marks stay inert: the statement conserves today's semantics")
		require.NotEqual(t, frontier, brA.DataSource.Timestamp,
			"the abandoned branch's source no longer rides the frontier")
		return nil
	})
	require.NoError(t, err)
}

// The presence gate of the lazy pass: an UNMARKED statement (below-gate
// cluster, ordinary plan) never touches lazily initialized scopes —
// byte-identical laziness today.
func TestExecutionReadLazyPassPresenceGated(t *testing.T) {
	sessionTS := timestamp.Timestamp{PhysicalTime: 100, LogicalTime: 1}
	frontier := timestamp.Timestamp{PhysicalTime: 999, LogicalTime: 9}
	c, _, teardown := admissionFixture(t, sessionTS)
	defer teardown()
	c.disableLock = true
	c.e = &reviewBarrierEngine{frontier: frontier,
		database: &reviewFakeDatabase{byName: map[string]engine.Relation{}}}
	// A presence plan whose LOCK target is unmarked: no statement marks,
	// so the pass must not touch the lazy scopes at all.
	c.pn = &plan.Plan{Plan: &plan.Plan_Query{Query: &plan.Query{
		Nodes: []*plan.Node{{
			NodeType:    plan.Node_LOCK_OP,
			LockTargets: []*plan.LockTarget{{TableId: 42}},
		}},
	}}}

	rel := &reviewExclusiveRelation{}
	branch := &Scope{
		RootOp:     reviewLockOp(),
		DataSource: &Source{node: reviewMarkedScan(0, "t"), Rel: rel},
	}
	root := &Scope{RootOp: merge.NewArgument(), LazyPreScopes: true, PreScopes: []*Scope{branch}}
	c.scopes = []*Scope{root}

	err := c.runPipelineAttempt(func() error {
		require.Equal(t, 0, rel.binds, "an unmarked statement never binds lazy scopes")
		require.Equal(t, timestamp.Timestamp{}, branch.DataSource.Timestamp,
			"the lazy branch's data source stays unstamped")
		require.False(t, c.execReadFrontierSet, "no frontier was acquired")
		return nil
	})
	require.NoError(t, err)
}

// ---------------------------------------------------------------------------
// P2: the dependent (correlation) integration.

// The dependent-source preparation: a correlation plan's marked TABLE_SCAN
// source is admitted and its relation BOUND onto the attempt's carrier at
// PRE-PIPELINE time — before any region demand opens sources mid-stream —
// while the explicit-nested source of the same plan keeps today's session
// path. The runtime accessor serves the bound handle under the attempt
// epoch, and an unmarked plan prepares nothing.
func TestExecutionReadDependentSourcePreparation(t *testing.T) {
	proc := testutil.NewProcess(t)
	proc.SetResolveVariableFunc(func(name string, system, global bool) (interface{}, error) {
		if name == "sql_mode" {
			return "", nil
		}
		return nil, nil
	})
	mock := plan2.NewMockOptimizer(true, proc)
	stmts, err := mysql.Parse(proc.Ctx,
		`SELECT (SELECT (SELECT COUNT(*) FROM NATION n2 WHERE n2.N_NATIONKEY <= n1.N_NATIONKEY)) FROM NATION n1`, 1)
	require.NoError(t, err)
	defer stmts[0].Free()
	q, err := plan2.NewBaseOptimizer(mock.CurrentContext()).Optimize(stmts[0], false)
	require.NoError(t, err)
	require.NotNil(t, q.Correlation,
		"the deep correlated shape is the plan family the dependent path routes")

	sessionTS := timestamp.Timestamp{PhysicalTime: 100, LogicalTime: 1}
	op, closeFn := client.NewTestTxnOperator(proc.Ctx)
	op.TxnRef().SnapshotTS = sessionTS
	// The stage-1 gate runs under SI: the bare test operator defaults to RC
	// and under RC the admission conserves the base behavior byte-identically.
	op.TxnRef().Isolation = pbtxn.TxnIsolation_SI
	// The carrier-owner viability check resolves a live workspace exactly
	// like a real statement.
	op.AddWorkspace(&disttae.Transaction{})
	defer closeFn()
	proc.Base.TxnOperator = op

	// Arm the roles/presence the stage-1 binder produces above the gate:
	// the outer FROM scan owns the locking statement, the subquery's scan
	// is explicit-nested, and the LOCK target records bind-time presence.
	var outerScan, innerScan, lockNode *plan.Node
	for _, n := range q.Nodes {
		switch {
		case n.NodeType == plan.Node_TABLE_SCAN && n.GetTableDef() != nil:
			if outerScan == nil {
				outerScan = n
			} else if innerScan == nil {
				innerScan = n
			}
		case n.NodeType == plan.Node_LOCK_OP:
			lockNode = n
		}
	}
	require.NotNil(t, outerScan, "the correlation plan has an outer scan")
	require.NotNil(t, innerScan, "the correlation plan has a nested scan")
	outerScan.ScanReadRole = plan.ScanReadRole_ScanOuterLocking
	outerScan.TableDef.TableType = "r"
	innerScan.ScanReadRole = plan.ScanReadRole_ScanNestedBlockExplicit
	if lockNode == nil {
		// The mock optimizer binds without pessimistic locks; fabricate the
		// LOCK node a FOR UPDATE statement's plan carries.
		q.Nodes = append(q.Nodes, &plan.Node{
			NodeType:    plan.Node_LOCK_OP,
			LockTargets: []*plan.LockTarget{{TableId: 42, LockingRead: true}},
		})
	} else {
		for _, target := range lockNode.LockTargets {
			target.LockingRead = true
		}
	}

	frontier := timestamp.Timestamp{PhysicalTime: 999, LogicalTime: 9}
	outerRel := &reviewExclusiveRelation{}
	innerRel := &reviewExclusiveRelation{}
	engFake := &reviewBarrierEngine{frontier: frontier,
		database: &reviewFakeDatabase{byName: map[string]engine.Relation{
			outerScan.GetTableDef().Name: outerRel,
		}}}

	c := &Compile{proc: proc, anal: &AnalyzeModule{qry: q}, ncpu: 1, e: engFake}
	c.pn = &plan.Plan{Plan: &plan.Plan_Query{Query: q}}

	template, err := dependent.Compile(proc.Ctx, q, c.compileDependentSource, c.compileDependentKernel)
	require.NoError(t, err)
	defer c.clearDependentSourceMetadata()
	c.scopes = []*Scope{{Proc: proc, RootOp: &dependent.Operator{Template: template}}}

	require.NoError(t, c.executionReadPrepareDependentSources())

	prep, ok := c.execReadDependentSources[outerScan.NodeId]
	require.True(t, ok, "the marked outer source is prepared")
	require.Equal(t, c.execReadEpoch, prep.epoch, "the preparation belongs to the current attempt")
	require.Equal(t, frontier, prep.frontier)
	require.Equal(t, 1, outerRel.binds, "the outer relation is bound onto the carrier up front")
	_, nestedPrepared := c.execReadDependentSources[innerScan.NodeId]
	require.False(t, nestedPrepared, "the explicit-nested source keeps today's session path")
	require.Equal(t, 0, innerRel.binds)

	require.NotNil(t, c.executionReadDependentBoundRelation(outerScan),
		"the runtime accessor serves the pre-bound handle to region demand")

	// Epoch discrimination: an attempt this preparation did not cover must
	// not serve the stale handle.
	c.execReadEpoch++
	require.Nil(t, c.executionReadDependentBoundRelation(outerScan),
		"a different attempt's preparation is not served")
	c.execReadEpoch--

	// Presence discrimination: an unmarked plan prepares nothing.
	c2 := &Compile{proc: proc, anal: &AnalyzeModule{qry: q}, ncpu: 1, e: engFake}
	c2.pn = &plan.Plan{Plan: &plan.Plan_Query{Query: &plan.Query{}}}
	c2.scopes = []*Scope{{Proc: proc, RootOp: &dependent.Operator{Template: template}}}
	require.NoError(t, c2.executionReadPrepareDependentSources())
	require.Empty(t, c2.execReadDependentSources, "an unmarked statement prepares no execution-read source")
}

// The dependent-path CALL-SITE member: through the REAL runPipelineAttempt ->
// prePipelineInitializer sequence, a marked correlation source must already
// be admitted, carrier-bound and frontier-stamped when the run body starts —
// region demand later in the pipeline then serves the pre-bound exclusive
// handle instead of opening the session relation and re-baselining on the
// RR snapshot mid-stream. The direct member above owns the preparation
// function's own contract; with the prePipelineInitializer call site
// disabled this member fails while that one cannot see the integration.
func TestDependentSourcesPreparedThroughPrePipeline(t *testing.T) {
	proc := testutil.NewProcess(t)
	proc.SetResolveVariableFunc(func(name string, system, global bool) (interface{}, error) {
		if name == "sql_mode" {
			return "", nil
		}
		return nil, nil
	})
	mock := plan2.NewMockOptimizer(true, proc)
	stmts, err := mysql.Parse(proc.Ctx,
		`SELECT (SELECT (SELECT COUNT(*) FROM NATION n2 WHERE n2.N_NATIONKEY <= n1.N_NATIONKEY)) FROM NATION n1`, 1)
	require.NoError(t, err)
	defer stmts[0].Free()
	q, err := plan2.NewBaseOptimizer(mock.CurrentContext()).Optimize(stmts[0], false)
	require.NoError(t, err)
	require.NotNil(t, q.Correlation,
		"the deep correlated shape is the plan family the dependent path routes")

	sessionTS := timestamp.Timestamp{PhysicalTime: 100, LogicalTime: 1}
	op, closeFn := admissionFixtureTxn(t, sessionTS)
	defer closeFn()
	proc.Base.TxnOperator = op

	var outerScan *plan.Node
	for _, n := range q.Nodes {
		if n.NodeType == plan.Node_TABLE_SCAN && n.GetTableDef() != nil {
			if outerScan == nil {
				outerScan = n
			}
		}
	}
	require.NotNil(t, outerScan, "the correlation plan has an outer scan")
	outerScan.ScanReadRole = plan.ScanReadRole_ScanOuterLocking
	outerScan.TableDef.TableType = "r"
	foundLock := false
	for _, n := range q.Nodes {
		if n.NodeType == plan.Node_LOCK_OP {
			foundLock = true
			for _, target := range n.LockTargets {
				target.LockingRead = true
			}
		}
	}
	if !foundLock {
		// The mock optimizer binds without pessimistic locks; fabricate the
		// LOCK node a FOR UPDATE statement's plan carries.
		q.Nodes = append(q.Nodes, &plan.Node{
			NodeType:    plan.Node_LOCK_OP,
			LockTargets: []*plan.LockTarget{{TableId: 42, LockingRead: true}},
		})
	}

	frontier := timestamp.Timestamp{PhysicalTime: 999, LogicalTime: 9}
	outerRel := &reviewExclusiveRelation{}
	engFake := &reviewBarrierEngine{frontier: frontier,
		database: &reviewFakeDatabase{byName: map[string]engine.Relation{
			outerScan.GetTableDef().Name: outerRel,
		}}}

	c := &Compile{proc: proc, anal: &AnalyzeModule{qry: q}, ncpu: 1, e: engFake}
	c.pn = &plan.Plan{Plan: &plan.Plan_Query{Query: q}}
	template, err := dependent.Compile(proc.Ctx, q, c.compileDependentSource, c.compileDependentKernel)
	require.NoError(t, err)
	defer c.clearDependentSourceMetadata()
	c.scopes = []*Scope{{Proc: proc, RootOp: &dependent.Operator{Template: template}}}
	c.disableLock = true

	err = c.runPipelineAttempt(func() error {
		prep, ok := c.execReadDependentSources[outerScan.NodeId]
		require.True(t, ok,
			"the pre-pipeline call site prepared the marked dependent source before pipelines")
		require.Equal(t, c.execReadEpoch, prep.epoch,
			"the preparation belongs to the attempt that runs the pipelines")
		require.Equal(t, frontier, prep.frontier,
			"the preparation rides the statement's publication frontier, not the session snapshot")
		require.Equal(t, 1, outerRel.binds,
			"the outer relation is exclusively bound before the run body could demand it")
		require.NotNil(t, c.executionReadDependentBoundRelation(outerScan),
			"the runtime accessor serves the pre-bound handle to region demand")
		return nil
	})
	require.NoError(t, err)
}

// The runtime-created LOCK kernel of a dependent plan receives the attempt's
// frontier at construction (the pre-pipeline sweep cannot see it): an active
// attempt stamps marks + frontier, an inactive attempt leaves them inert.
func TestDependentLockKernelStampedWhenAttemptActive(t *testing.T) {
	sessionTS := timestamp.Timestamp{PhysicalTime: 100, LogicalTime: 1}
	c, _, teardown := admissionFixture(t, sessionTS)
	defer teardown()
	c.disableLock = true

	frontier := timestamp.Timestamp{PhysicalTime: 887, LogicalTime: 7}
	lockNode := &plan.Node{
		NodeType:    plan.Node_LOCK_OP,
		LockTargets: []*plan.LockTarget{{TableId: 42, LockingRead: true}},
	}

	build := func() *lockop.LockOp {
		// The LOCK kernel consumes exactly one native input (dependent_kernel
		// enforces the single-child physical shape before the factory).
		child := &plan.Node{NodeType: plan.Node_TABLE_SCAN, ProjectList: []*plan.Expr{}}
		factory, _, err := c.compileDependentKernel(plan2.DeepCopyNode(lockNode), []*plan.Node{child})
		require.NoError(t, err)
		op, err := factory(c.proc, []*dependent.DemandInput{{}})
		require.NoError(t, err)
		lockOp, ok := op.(*lockop.LockOp)
		require.True(t, ok)
		return lockOp
	}

	// A fresh (inactive) attempt: marks inert — today's lock-only behavior.
	unstamped := build()
	targets := unstamped.CopyToPipelineTarget()
	require.False(t, targets[0].GetLockingRead(),
		"without an active attempt the runtime lock kernel stays unstamped")
	unstamped.Release()

	// Arm THIS attempt's frontier: the same factory stamps it.
	c.execReadFrontierSet = true
	c.execReadFrontier = frontier
	stamped := build()
	targets = stamped.CopyToPipelineTarget()
	require.True(t, targets[0].GetLockingRead(),
		"the runtime lock kernel construction installs the attempt's frontier")
	require.Equal(t, frontier.PhysicalTime, targets[0].GetCurrentReadFrontierPhysical())
	require.Equal(t, frontier.LogicalTime, targets[0].GetCurrentReadFrontierLogical())
	stamped.Release()
}

// The deep-copy presence contract through the ACTUAL dependent-kernel deep
// copy site: DeepCopyNode of a LOCK_OP node (the compileDependentKernel
// entry) preserves the target's LockingRead presence and the scan roles, so
// constructLockOp's derivation keeps the marked-table set intact (pre-PR
// review P2: the copy used to drop presence silently).
func TestDeepCopyNodeKeepsLockingReadPresence(t *testing.T) {
	marked := &plan.LockTarget{
		TableId:            42,
		ObjRef:             &plan.ObjectRef{SchemaName: "test", ObjName: "t"},
		PrimaryColIdxInBat: 1,
		LockingRead:        true,
	}
	lockNode := &plan.Node{
		NodeType:    plan.Node_LOCK_OP,
		LockTargets: []*plan.LockTarget{marked, {TableId: 43}},
	}
	// DeepCopyNode is what compileDependentKernel feeds constructLockOp.
	copiedNode := plan2.DeepCopyNode(lockNode)
	require.NotSame(t, lockNode, copiedNode)
	require.Len(t, copiedNode.LockTargets, 2)
	require.True(t, copiedNode.LockTargets[0].GetLockingRead(),
		"deep-copied marked targets keep presence")
	require.False(t, copiedNode.LockTargets[1].GetLockingRead(),
		"unmarked targets stay unmarked")
	require.Equal(t, uint64(42), copiedNode.LockTargets[0].GetTableId())
	require.Equal(t, int32(1), copiedNode.LockTargets[0].GetPrimaryColIdxInBat())
	require.Equal(t, uint64(43), copiedNode.LockTargets[1].GetTableId())

	arg, err := constructLockOp(lockNode, nil)
	require.NoError(t, err)
	defer arg.Release()
	copyArg, err := constructLockOp(copiedNode, nil)
	require.NoError(t, err)
	defer copyArg.Release()
	origin := arg.CopyToPipelineTarget()
	dup := copyArg.CopyToPipelineTarget()
	require.Len(t, dup, 2)
	for i := range origin {
		require.Equal(t, origin[i].GetLockingRead(), dup[i].GetLockingRead(),
			"the marked-table derivation survives the deep copy")
		require.Equal(t, origin[i].GetTableId(), dup[i].GetTableId())
	}

	// The scan-role side: DeepCopyNode preserves ScanReadRole too (the same
	// consumer path re-parents scan nodes), including the index-parent link.
	scan := reviewMarkedScan(7, "t")
	scan.IndexScanInfo = plan.IndexScanInfo{ParentTableId: 42}
	scanCopy := plan2.DeepCopyNode(scan)
	require.Equal(t, scan.GetScanReadRole(), scanCopy.GetScanReadRole())
	require.Equal(t, scan.IndexScanInfo.GetParentTableId(), scanCopy.IndexScanInfo.GetParentTableId(),
		"index-parent admission data survives the copy")
}

// ---------------------------------------------------------------------------
// The retry-frontier matrix (D-5) and snapshot immutability.

// The retry-frontier progression matrix: a token strictly beyond the
// execution's floor is the only ACCEPTED progression shape; an equal token
// and a regressing token fail the progressing predicate the Run loop
// applies, which terminates the statement through the recorded "no strict
// progress" internal error instead of re-running the identical marked read.
// A token-less retry never re-drives. Stamping never mutates the session
// snapshot.
func TestLockingReadRetryFrontierProgressMatrix(t *testing.T) {
	// rev-18 rewrite (pre-PR review round 4, finding 3): the matrix drives the
	// PRODUCTION validator the retry loop calls, no longer a local replica of
	// the comparison code. The shapes here exercise the degenerate no-published
	//-frontier state: the executing attempt failed before its first carrier,
	// so the progressing floor degenerates to the chain floor (D-5).
	sessionTS := timestamp.Timestamp{PhysicalTime: 100, LogicalTime: 1}
	c, op, teardown := admissionFixture(t, sessionTS)
	defer teardown()
	c.execReadRetryFloor = timestamp.Timestamp{PhysicalTime: 100, LogicalTime: 1}
	runC := &Compile{} // no published executing frontier

	for _, tc := range []struct {
		name  string
		front timestamp.Timestamp
		ok    bool
	}{
		{name: "strict physical progress", front: timestamp.Timestamp{PhysicalTime: 200, LogicalTime: 0}, ok: true},
		{name: "logical progress past equal physical", front: timestamp.Timestamp{PhysicalTime: 100, LogicalTime: 5}, ok: true},
		{name: "equal", front: timestamp.Timestamp{PhysicalTime: 100, LogicalTime: 1}, ok: false},
		{name: "regressing", front: timestamp.Timestamp{PhysicalTime: 60, LogicalTime: 9}, ok: false},
	} {
		err := moerr.NewTxnNeedRetryLockingRead(context.Background(),
			tc.front.PhysicalTime, tc.front.LogicalTime)
		require.True(t, moerr.IsMoErrCode(err, moerr.ErrTxnNeedRetryLockingRead))
		got := c.validateLockingReadRetryProgress(runC, err)
		if tc.ok {
			require.Same(t, err, got, "progressing shape %s passes through unchanged", tc.name)
		} else {
			require.NotSame(t, err, got, "non-progressing shape %s is replaced by the terminal error", tc.name)
			require.ErrorContains(t, got, "no strict progress",
				"non-progressing shape %s terminates the statement", tc.name)
		}
	}

	// A token-less error is never a locking-read retry: it passes through for
	// the rest of the loop's machinery to decide.
	_, _, ok := moerr.GetTxnNeedRetryLockingReadFrontier(moerr.NewTxnNeedRetryNoCtx())
	require.False(t, ok)

	// Snapshot immutability: stamping marks + frontier on a lock operator
	// never touches the session transaction's snapshot read TS.
	require.Equal(t, sessionTS, op.Txn().SnapshotTS)
	arg := reviewLockOp()
	defer arg.Release()
	arg.SetLockingReadCurrentFrontier(timestamp.Timestamp{PhysicalTime: 999, LogicalTime: 9}, sessionTS)
	require.Equal(t, sessionTS, op.Txn().SnapshotTS,
		"the marked stamp leaves the transaction snapshot untouched")
	require.Equal(t, sessionTS, c.proc.GetTxnOperator().Txn().SnapshotTS)
}

// The COLD Run-admission capability site (compile2.go Compile() calls
// validateLockingCurrentReadCapability before scheduling): a presence plan
// observed below the allocated gate is rejected with the recorded terminal
// error at that earliest admission member — including on the upstream-only
// aisles 131–134 (weighted percentile #31000, decimal float rounding
// #31191 / commit 861a2c4cb689, typed JSON MIN/MAX and the ordered ODKU
// summary at origin/main c4946bb74d43), keeping the mixed-version contract;
// the rejection precedes all source/scope
// construction.
func TestCompileColdSiteRejectsPresencePlanBelowGate(t *testing.T) {
	proc := testutil.NewProcess(t)
	require.NotNil(t, moruntime.ServiceRuntime(proc.GetService()))

	pn := presencePlan(&plan.Node{
		NodeId:       0,
		NodeType:     plan.Node_TABLE_SCAN,
		TableDef:     &plan.TableDef{TableType: "r", TblId: 42},
		ScanReadRole: plan.ScanReadRole_ScanOuterLocking,
	})
	pn.GetQuery().Steps = []int32{1}

	sessionTS := timestamp.Timestamp{PhysicalTime: 100, LogicalTime: 1}
	op, closeFn := client.NewTestTxnOperator(proc.Ctx)
	op.TxnRef().SnapshotTS = sessionTS
	defer closeFn()
	proc.Base.TxnOperator = op

	for _, version := range []int64{128, 131, 132, 133, 134} {
		require.Less(t, version, defines.MORPCVersion135,
			"the corpus stays below the re-minted allocated gate")
		label := "below-hole"
		switch version {
		case 131:
			label = "weighted-percentile-131-peer"
		case 132:
			label = "decimal-float-132-peer"
		case 133:
			label = "json-minmax-133-peer"
		case 134:
			label = "odku-last-insert-id-134-peer"
		}
		t.Run(label, func(t *testing.T) {
			// Resolve the runtime LATE: fixture setup (the test txn operator)
			// may (re)install the service runtime, and the gate reads must
			// observe the same instance the validator will resolve.
			rt := moruntime.ServiceRuntime(proc.GetService())
			orig, had := rt.GetGlobalVariables(moruntime.MOProtocolVersion)
			t.Cleanup(func() {
				if had {
					rt.SetGlobalVariables(moruntime.MOProtocolVersion, orig)
				}
			})
			rt.SetGlobalVariables(moruntime.MOProtocolVersion, version)
			// Stability anchor: the direct validator rejects the presence
			// plan under this gate state before any pipeline work.
			require.Error(t, validateLockingCurrentReadCapability(proc, pn))
			// A full Compile must carry the SAME membership to its cold
			// admission member: the rejection precedes all scheduling and
			// never surfaces later compile-stage panics or scope state.
			c := &Compile{proc: proc, ncpu: 1, counterSet: &perfcounter.CounterSet{}}
			err := c.Compile(context.Background(), pn, nil)
			require.Error(t, err)
			require.True(t, moerr.IsMoErrCode(err, moerr.ErrNotSupported), "%v", err)
			require.ErrorContains(t, err, "requires rebinding")
			require.ErrorContains(t, err, "MORPCVersion135")
			require.Nil(t, c.scopes,
				"the capability rejection precedes all scope construction")
		})
	}
}

// ---------------------------------------------------------------------------
// The enumerated copy/fabrication corpus for a locked statement (design
// verification item 8(iii)): every typed-plan copy helper preserves the
// bind-time locking-read presence and scan-role representation over a
// locked-statement corpus, so no copy can silently strip provenance that
// capability validation, closure checks, index-parent admission and LockOp
// marking depend on.
func TestLockedPlanCopyCorpusPreservesProvenance(t *testing.T) {
	baseScan := func(id int32, role plan.ScanReadRole) *plan.Node {
		return &plan.Node{
			NodeId:       id,
			NodeType:     plan.Node_TABLE_SCAN,
			TableDef:     &plan.TableDef{TableType: "r", Name: "t", TblId: 42},
			ObjRef:       &plan.ObjectRef{SchemaName: "test", ObjName: "t"},
			ScanReadRole: role,
		}
	}
	idxScan := baseScan(3, plan.ScanReadRole_ScanOuterLocking)
	idxScan.TableDef.TableType = "i"
	idxScan.IndexScanInfo = plan.IndexScanInfo{ParentTableId: 42}
	corpus := []struct {
		name  string
		query *plan.Query
	}{
		{name: "single locked scan", query: &plan.Query{
			Steps: []int32{1},
			Nodes: []*plan.Node{
				baseScan(0, plan.ScanReadRole_ScanOuterLocking),
				{
					NodeType:    plan.Node_LOCK_OP,
					LockTargets: []*plan.LockTarget{{TableId: 42, LockingRead: true}},
					Children:    []int32{0},
				},
			},
		}},
		{name: "locked scan with explicit-nested block", query: &plan.Query{
			Steps: []int32{1},
			Nodes: []*plan.Node{
				baseScan(0, plan.ScanReadRole_ScanOuterLocking),
				baseScan(2, plan.ScanReadRole_ScanNestedBlockExplicit),
				{
					NodeType:    plan.Node_LOCK_OP,
					LockTargets: []*plan.LockTarget{{TableId: 42, LockingRead: true}},
					Children:    []int32{0, 2},
				},
			},
		}},
		{name: "covered index surface through the marked parent", query: &plan.Query{
			Steps: []int32{3},
			Nodes: []*plan.Node{
				baseScan(0, plan.ScanReadRole_ScanOuterLocking),
				{
					NodeId:      1,
					NodeType:    plan.Node_PROJECT,
					Children:    []int32{0},
					ProjectList: []*plan.Expr{},
				},
				idxScan,
				{
					NodeType:    plan.Node_LOCK_OP,
					LockTargets: []*plan.LockTarget{{TableId: 42, LockingRead: true}},
					Children:    []int32{1, 3},
				},
			},
		}},
	}

	for _, member := range corpus {
		t.Run(member.name, func(t *testing.T) {
			require.True(t, plan2.PlanHasLockingCurrentRead(member.query))

			planCopy := plan2.DeepCopyPlan(&plan.Plan{Plan: &plan.Plan_Query{Query: member.query}})
			require.True(t, plan2.PlanHasLockingCurrentRead(planCopy.GetQuery()),
				"DeepCopyPlan keeps presence")
			queryCopy := plan2.DeepCopyQuery(member.query)
			require.True(t, plan2.PlanHasLockingCurrentRead(queryCopy),
				"DeepCopyQuery keeps presence")

			for i, n := range queryCopy.GetNodes() {
				require.Equal(t, member.query.Nodes[i].GetScanReadRole(), n.GetScanReadRole(),
					"scan roles survive the copy")
				require.Equal(t, member.query.Nodes[i].IndexScanInfo.GetParentTableId(),
					n.IndexScanInfo.GetParentTableId(),
					"index-parent admission data survives the copy")
				for j, target := range n.GetLockTargets() {
					require.Equal(t, member.query.Nodes[i].LockTargets[j].GetLockingRead(),
						target.GetLockingRead(), "every copied target keeps presence")
					require.Equal(t, member.query.Nodes[i].LockTargets[j].GetTableId(),
						target.GetTableId())
				}
			}
			// The plan-level wrapper agrees with the query copy.
			for i, n := range planCopy.GetQuery().GetNodes() {
				require.Equal(t, member.query.Nodes[i].GetScanReadRole(), n.GetScanReadRole())
			}
		})
	}
}

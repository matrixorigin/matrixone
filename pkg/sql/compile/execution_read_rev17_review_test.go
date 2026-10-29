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

// rev-17 members (pre-PR review round 3, P1; the capability-transition
// polarity member is removed with the gate by rev 15 R15-F1): the dependent
// scratch-scope lifetime and terminal hard binding failures through the
// compile binding caller.

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	pbtxn "github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/dependent"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/lockop"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/merge"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae"
)

// ---------------------------------------------------------------------------
// P1 (round 3): the dependent scratch-scope lifetime.

// The execution boundary retires the dependent scratch map: the pooled
// Compile's clear()/Reset path runs resetExecutionReadState, so a RECYCLED
// compile never serves another query's marked dependent relation. With the
// retirement line disabled this member fails: the stale scratch survives the
// boundary.
func TestExecutionReadScratchScopeRetiredAtExecutionBoundary(t *testing.T) {
	sessionTS := timestamp.Timestamp{PhysicalTime: 100, LogicalTime: 1}
	c, _, teardown := admissionFixture(t, sessionTS)
	defer teardown()

	node := reviewMarkedScan(5, "t")
	c.execReadDependentScopes = map[int32]*Scope{5: {
		Proc:       c.proc,
		DataSource: &Source{node: node, Rel: &reviewExclusiveRelation{}},
	}}
	c.execReadEpoch = 3

	c.resetExecutionReadState()

	require.Empty(t, c.execReadDependentScopes,
		"the execution boundary retires the dependent scratch map before a recycled compile can serve it")
	require.Zero(t, c.execReadEpoch,
		"the retirement belongs to the same execution-boundary reset the frontier dies in")
}

// The pool-reuse counterexample (pre-PR review round 3, P1): a recycled
// Compile whose map escaped the boundary reset must still never serve the
// PREVIOUS query's source. Two statements with the SAME plan node id but
// DIFFERENT node objects and DIFFERENT tables — the colliding-id shape a
// recycled compile produces — hit the map in sequence, and even across a
// TRANSACTION change: the second preparation must rebuild the scratch for
// the second statement's own node and table, binding the second table's
// relation exactly once and never re-binding the first table's relation.
// With the node-object identity check disabled this member fails by
// publishing the first table's relation to the second query.
func TestDependentScratchIdentityRebuildOnPooledCompile(t *testing.T) {
	sessionTS := timestamp.Timestamp{PhysicalTime: 100, LogicalTime: 1}
	frontier := timestamp.Timestamp{PhysicalTime: 999, LogicalTime: 9}
	proc := testutil.NewProcess(t)
	op, closeFn := client.NewTestTxnOperator(proc.Ctx)
	op.TxnRef().Isolation = pbtxn.TxnIsolation_SI
	op.TxnRef().SnapshotTS = sessionTS
	op.AddWorkspace(&disttae.Transaction{})
	defer closeFn()
	proc.Base.TxnOperator = op

	relA := &reviewExclusiveRelation{}
	relB := &reviewExclusiveRelation{}
	c := &Compile{proc: proc, ncpu: 1, e: &reviewBarrierEngine{frontier: frontier,
		database: &reviewFakeDatabase{byName: map[string]engine.Relation{
			"tA": relA,
			"tB": relB,
		}}}}

	nodeA := reviewMarkedScan(5, "tA")
	nodeA.TableDef.TblId = 42
	nodeB := reviewMarkedScan(5, "tB")
	nodeB.TableDef.TblId = 43
	c.pn = &plan.Plan{Plan: &plan.Plan_Query{Query: &plan.Query{Nodes: []*plan.Node{
		nodeA, nodeB,
		{
			NodeType: plan.Node_LOCK_OP,
			LockTargets: []*plan.LockTarget{
				{TableId: 42, LockingRead: true},
				{TableId: 43, LockingRead: true},
			},
		},
	}}}}

	// The FIRST statement's preparation: scratch keyed by node id 5 serves
	// table A's relation through the first transaction's operator.
	require.NoError(t, c.executionReadPrepareDependentSource(nodeA))
	first, ok := c.execReadDependentScopes[5]
	require.True(t, ok)
	require.Same(t, nodeA, first.DataSource.node)
	require.Equal(t, 1, relA.binds,
		"the first statement binds its OWN table's relation")

	// The recycle the identity check defends: WITHOUT the boundary reset the
	// map entry for colliding node id 5 survives; the second statement — a
	// different query on a recycled compile, run under a SECOND transaction —
	// must rebuild the source for ITS OWN node object and table.
	op2, closeFn2 := client.NewTestTxnOperator(proc.Ctx)
	op2.TxnRef().Isolation = pbtxn.TxnIsolation_SI
	op2.TxnRef().SnapshotTS = sessionTS
	op2.AddWorkspace(&disttae.Transaction{})
	defer closeFn2()
	proc.Base.TxnOperator = op2
	c.execReadEpoch++

	require.NoError(t, c.executionReadPrepareDependentSource(nodeB))
	second := c.execReadDependentScopes[5]
	require.NotNil(t, second)
	require.NotSame(t, first, second,
		"a colliding node id from a retired statement never feeds the new statement")
	require.Same(t, nodeB, second.DataSource.node,
		"the rebuilt scratch carries the CURRENT statement's node object")
	require.Equal(t, 1, relB.binds,
		"the second statement binds ITS OWN table's relation exactly once")
	require.Equal(t, 1, relA.binds,
		"the second statement never re-publishes the first query's relation")

	// The runtime preparation follows the rebuild: the served handle belongs
	// to the second table, under the CURRENT attempt's epoch.
	prep, ok := c.execReadDependentSources[5]
	require.True(t, ok)
	require.Equal(t, c.execReadEpoch, prep.epoch)
	require.Same(t, relB, prep.relation)
	require.Equal(t, frontier, prep.frontier)
}

// ---------------------------------------------------------------------------
// P1 (round 3): hard binding failures stay terminal through the caller.

// G4's failure half: a LAZY branch whose binding FAILS HARD (allocation
// error, unreadable tombstone — the errors the engine now propagates
// unchanged) terminates the WHOLE statement before any branch produced rows.
// The already-bound earlier branches are rolled back onto the session
// operator (cleanup), the frontier retires, and the statement surfaces the
// injected error itself — never a decline, never a mixed execution.
func TestExecutionReadLazyHardBindingFailureTerminatesWholeStatement(t *testing.T) {
	sessionTS := timestamp.Timestamp{PhysicalTime: 100, LogicalTime: 1}
	frontier := timestamp.Timestamp{PhysicalTime: 999, LogicalTime: 9}
	c, _, teardown := admissionFixture(t, sessionTS)
	defer teardown()
	c.disableLock = true
	c.e = &reviewBarrierEngine{frontier: frontier,
		database: &reviewFakeDatabase{byName: map[string]engine.Relation{}}}
	c.pn = presencePlan(reviewMarkedScan(0, "t"))

	branch := func(fail error) (*Scope, *reviewExclusiveRelation, *lockop.LockOp) {
		rel := &reviewExclusiveRelation{hardErr: fail}
		op := reviewLockOp()
		return &Scope{
			RootOp:     op,
			DataSource: &Source{node: reviewMarkedScan(0, "t"), Rel: rel},
		}, rel, op
	}
	brA, relA, lockA := branch(nil)
	brB, relB, _ := branch(moerr.NewInternalErrorNoCtxf("binding fault: allocation exhausted"))
	root := &Scope{RootOp: merge.NewArgument(), LazyPreScopes: true, PreScopes: []*Scope{brA, brB}}
	c.scopes = []*Scope{root}

	runReached := false
	err := c.runPipelineAttempt(func() error {
		runReached = true
		return nil
	})
	require.Error(t, err,
		"a hard binding failure on a later lazy branch terminates the statement")
	require.False(t, runReached,
		"no pipeline body ran: the earlier branch produced no rows (no mixed execution)")
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrInternal), "%v", err)
	require.ErrorContains(t, err, "binding fault: allocation exhausted",
		"the statement surfaces the injected failure itself, not a synthesized wrapper")
	require.False(t, engine.IsExecutionReadUnsupported(err),
		"a hard binding failure is never downgraded to a soft refusal")
	require.GreaterOrEqual(t, relA.resets, 1,
		"the already-bound branch is rolled back onto the session operator before the error surfaces")
	require.Equal(t, 1, relB.binds,
		"the failing branch's binding is attempted exactly once")
	require.Empty(t, c.execReadBoundScopes,
		"no binding stays published after the terminal failure")
	active, _ := c.executionReadStatementFrontier()
	require.False(t, active,
		"the frontier retires with the failed admission")
	require.Empty(t, c.execReadDisableReason,
		"a terminal failure is not a decline: nothing falls back to snapshot semantics")
	targets := lockA.CopyToPipelineTarget()
	require.False(t, targets[0].GetLockingRead(),
		"unstamped marks stay inert after the terminal failure")
}

// The dependent-preparation variant of the same contract: a hard binding
// failure while preparing a marked dependent source is terminal, never a
// silent absence.
func TestExecutionReadDependentHardBindingFailureIsTerminal(t *testing.T) {
	sessionTS := timestamp.Timestamp{PhysicalTime: 100, LogicalTime: 1}
	frontier := timestamp.Timestamp{PhysicalTime: 999, LogicalTime: 9}
	proc := testutil.NewProcess(t)
	op, closeFn := client.NewTestTxnOperator(proc.Ctx)
	op.TxnRef().Isolation = pbtxn.TxnIsolation_SI
	op.TxnRef().SnapshotTS = sessionTS
	op.AddWorkspace(&disttae.Transaction{})
	defer closeFn()
	proc.Base.TxnOperator = op

	rel := &reviewExclusiveRelation{
		hardErr: moerr.NewInternalErrorNoCtxf("binding fault: tombstone unreadable"),
	}
	c := &Compile{proc: proc, ncpu: 1, e: &reviewBarrierEngine{frontier: frontier,
		database: &reviewFakeDatabase{byName: map[string]engine.Relation{"tD": rel}}}}
	node := reviewMarkedScan(5, "tD")
	node.TableDef.TblId = 42
	c.pn = presencePlan(node)

	err := c.executionReadPrepareDependentSource(node)
	require.Error(t, err)
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrInternal), "%v", err)
	require.ErrorContains(t, err, "binding fault: tombstone unreadable")
	require.False(t, engine.IsExecutionReadUnsupported(err),
		"hard failures stay hard: no soft-refusal downgrade")
	_, prepared := c.execReadDependentSources[5]
	require.False(t, prepared,
		"the failed preparation publishes no dependent source")
	require.Empty(t, c.execReadBoundScopes)
}

// ---------------------------------------------------------------------------
// P2 (round 3, finding 4): the dependent path drives a REAL FOR UPDATE
// consumer with a provably correlated plan.

// A deep-existential FOR UPDATE statement — the parse the SQL layer really
// produces for the BVT deep member — plans with a NON-NIL Correlation (the
// dependent-path family) and executes its marked dependent source through
// the REAL runPipelineAttempt: the pre-pipeline preparation admits the
// marked outer source and binds it onto the execution carrier BEFORE the
// run body, and the runtime accessor the dependent kernel consumes serves
// exactly that pre-bound exclusive relation.
func TestDependentForUpdateDeepExistentialPreparesRealConsumers(t *testing.T) {
	proc := testutil.NewProcess(t)
	proc.SetResolveVariableFunc(func(name string, system, global bool) (interface{}, error) {
		if name == "sql_mode" {
			return "", nil
		}
		return nil, nil
	})
	mock := plan2.NewMockOptimizer(true, proc)
	// A REAL FOR UPDATE statement of the correlated-projection family, bound
	// through the frontend's own optimizer path (NewBaseOptimizer runs the
	// production BuildPlan plus the default rules, exactly what the executor
	// runs for a plain SELECT) and keeping its Query.Correlation: the
	// dependent (correlation) plan family. The deep-existential twin — the
	// shape of the test/distributed BVT member's live statement — is asserted
	// separately in TestDependentForUpdateDeepExistentialShapeIsCorrelationPlan.
	stmts, err := mysql.Parse(proc.Ctx,
		`SELECT (SELECT (SELECT COUNT(*) FROM NATION n2 WHERE n2.N_NATIONKEY <= n1.N_NATIONKEY)) FROM NATION n1 FOR UPDATE`, 1)
	require.NoError(t, err)
	defer stmts[0].Free()
	q, err := plan2.NewBaseOptimizer(mock.CurrentContext()).Optimize(stmts[0], false)
	require.NoError(t, err)
	require.NotNil(t, q.Correlation,
		"a FOR UPDATE statement of the correlated-projection family is the dependent-path plan family")

	// The production plan marks the statement's outer FROM occurrence; keep
	// that marking intact and locate the node STRUCTURALLY by walking the
	// root's input chain, never by array order (subquery scans precede the
	// outer FROM in the bound node list).
	root := q.Nodes[0]
	if len(q.Steps) > 0 {
		root = q.Nodes[q.Steps[0]]
	}
	outerScan := root
	for outerScan.NodeType != plan.Node_TABLE_SCAN {
		require.NotEmpty(t, outerScan.Children,
			"the root chain of a SELECT ... FOR UPDATE reaches its outer FROM scan")
		outerScan = q.Nodes[outerScan.Children[0]]
	}
	require.Equal(t, plan.ScanReadRole_ScanOuterLocking, outerScan.GetScanReadRole(),
		"the bound FOR UPDATE plan marks its own outer FROM occurrence outer-locking")
	// Every OTHER scan of the statement is a nested occurrence — including
	// the correlated scalar's FROM — and rides the SI snapshot: the
	// per-occurrence role contract the executor relies on.
	var nested []*plan.Node
	for _, n := range q.Nodes {
		if n.NodeType == plan.Node_TABLE_SCAN && n.NodeId != outerScan.NodeId {
			nested = append(nested, n)
		}
	}
	require.NotEmpty(t, nested, "the correlated-projection family carries nested scans")
	for _, n := range nested {
		require.Equal(t, plan.ScanReadRole_ScanNestedBlockExplicit, n.GetScanReadRole(),
			"a nested occurrence of a locking statement never joins the current-read admission set")
	}
	outerScan.TableDef.TableType = "r"
	foundLock := false
	for _, n := range q.Nodes {
		if n.NodeType == plan.Node_LOCK_OP {
			for _, target := range n.LockTargets {
				if target.GetLockingRead() {
					foundLock = true
				}
			}
		}
	}
	require.True(t, foundLock, "the bound plan carries its lock operator's locking-read marks")

	sessionTS := timestamp.Timestamp{PhysicalTime: 100, LogicalTime: 1}
	op, closeFn := client.NewTestTxnOperator(proc.Ctx)
	op.TxnRef().SnapshotTS = sessionTS
	op.TxnRef().Isolation = pbtxn.TxnIsolation_SI
	op.AddWorkspace(&disttae.Transaction{})
	defer closeFn()
	proc.Base.TxnOperator = op

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
			"the pre-pipeline call site prepared the FOR UPDATE dependent source before the run body")
		require.Equal(t, c.execReadEpoch, prep.epoch)
		require.Equal(t, frontier, prep.frontier,
			"the parsable locking statement rides the publication frontier, not the snapshot")
		require.Equal(t, 1, outerRel.binds,
			"the marked outer source of the parsable FOR UPDATE binds its exclusive relation exactly once")
		require.Same(t, outerRel, prep.relation,
			"the runtime consumers serve exactly the pre-bound exclusive relation")
		require.NotNil(t, c.executionReadDependentBoundRelation(outerScan),
			"the demand-time bound-relation accessor serves the prepared source")
		for _, n := range nested {
			_, admitted := c.execReadDependentSources[n.NodeId]
			require.False(t, admitted,
				"a nested occurrence never prepares a current-read source: it rides the SI snapshot")
		}
		return nil
	})
	require.NoError(t, err)
}

// The delivered BVT locking statement's OWN shape (test/distributed/
// cases/pessimistic_transaction/select_for_update_rr_current_read.sql) binds
// the dependent (correlation) plan family through the production optimizer:
// the correlated aggregate scalar in the projection list stays an apply, so
// the live statement's outer locked occurrence current-reads through the
// dependent path this round's members drive, while its nested occurrences
// keep the SI snapshot.
func TestDependentForUpdateDeliveredBvtShapeIsCorrelationPlan(t *testing.T) {
	proc := testutil.NewProcess(t)
	proc.SetResolveVariableFunc(func(name string, system, global bool) (interface{}, error) {
		if name == "sql_mode" {
			return "", nil
		}
		return nil, nil
	})
	mock := plan2.NewMockOptimizer(true, proc)
	stmts, err := mysql.Parse(proc.Ctx, `
SELECT o.N_NAME AS outer_locked,
       (SELECT (SELECT COUNT(*) FROM NATION i
                WHERE i.N_REGIONKEY = o.N_NATIONKEY AND i.N_NATIONKEY >= 1)) AS nested_correlated
FROM NATION o
WHERE o.N_NATIONKEY = 1
FOR UPDATE`, 1)
	require.NoError(t, err)
	defer stmts[0].Free()
	q, err := plan2.NewBaseOptimizer(mock.CurrentContext()).Optimize(stmts[0], false)
	require.NoError(t, err)
	require.NotNil(t, q.Correlation,
		"the delivered correlated-projection FOR UPDATE locking shape binds the dependent (correlation) plan family")
}

// ---------------------------------------------------------------------------
// P1 (round 3, finding 3's compile half): the marked discovery carries the
// front view through the EXCLUSIVE binding.

// handleDbRelContext's marked path returns an EXCLUSIVELY BOUND front
// relation — never a session-anchored shared canonical at discovery time
// and never a carrier-anchored shared entry (the round-3 G2 fix: resolving
// the transaction's shared tableCache through a carrier database re-anchored
// the cached canonical onto the frontier and poisoned every later ordinary
// read of that table). The bound relation is a fresh exclusive handle;
// the shared cache entry stays session-anchored for ordinary statements.
func TestExecutionReadDiscoveryBindsExclusiveFrontHandle(t *testing.T) {
	sessionTS := timestamp.Timestamp{PhysicalTime: 100, LogicalTime: 1}
	frontier := timestamp.Timestamp{PhysicalTime: 999, LogicalTime: 9}
	proc := testutil.NewProcess(t)
	op, closeFn := client.NewTestTxnOperator(proc.Ctx)
	op.TxnRef().Isolation = pbtxn.TxnIsolation_SI
	op.TxnRef().SnapshotTS = sessionTS
	op.AddWorkspace(&disttae.Transaction{})
	defer closeFn()
	proc.Base.TxnOperator = op

	rel := &reviewExclusiveRelation{}
	c := &Compile{proc: proc, ncpu: 1, e: &reviewBarrierEngine{frontier: frontier,
		database: &reviewFakeDatabase{byName: map[string]engine.Relation{"tD": rel}}}}
	node := reviewMarkedScan(5, "tD")
	node.TableDef.TblId = 42
	c.pn = presencePlan(node)

	// Arm this attempt's carrier so the discovery resolves the front view.
	declined, execOp, _, err := c.executionReadAdmitScan(&Scope{DataSource: &Source{node: node}})
	require.NoError(t, err)
	require.False(t, declined)

	gotRel, _, _, err := c.handleDbRelContext(node, false)
	require.NoError(t, err)
	require.Equal(t, 1, rel.binds,
		"the marked discovery binds the EXCLUSIVE handle onto the execution carrier")
	require.Same(t, rel, gotRel,
		"the discovery consumer serves the bound exclusive handle")

	// An UNMARKED node's discovery never binds: today's session path.
	plainRel := &reviewExclusiveRelation{}
	c.e = &reviewBarrierEngine{frontier: frontier,
		database: &reviewFakeDatabase{byName: map[string]engine.Relation{"tP": plainRel}}}
	plainNode := reviewMarkedScan(6, "tP")
	plainNode.TableDef.TblId = 43
	plainNode.ScanReadRole = plan.ScanReadRole_ScanNestedBlock
	gotPlain, _, _, err := c.handleDbRelContext(plainNode, false)
	require.NoError(t, err)
	require.Equal(t, 0, plainRel.binds,
		"an unmarked discovery never binds a front handle")
	require.Same(t, plainRel, gotPlain)
	_ = execOp
}

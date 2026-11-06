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

	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	pbtxn "github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae"
	"github.com/stretchr/testify/require"
)

// abandonFakeRelation records reset calls; all other Relation methods stay
// nil through the embedded interface (the rollback path only calls Reset).
type abandonFakeRelation struct {
	engine.Relation
	resets int
}

func (r *abandonFakeRelation) Reset(op client.TxnOperator) error {
	r.resets++
	return nil
}

// admissionFixture returns a Compile whose process carries a live test
// transaction operator with the session snapshot set to ts. The operator is
// pinned to SI explicitly: the bare test client defaults to RC, and under RC
// the admission keeps the base behavior byte-identically (refused). The
// operator also carries a (zero) disttae workspace so the carrier-owner
// guard's viability check resolves like a live statement.
func admissionFixture(t *testing.T, ts timestamp.Timestamp) (*Compile, client.TxnOperator, func()) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	op, closeFn := client.NewTestTxnOperator(ctx)
	op.TxnRef().Isolation = pbtxn.TxnIsolation_SI
	op.TxnRef().SnapshotTS = ts
	op.AddWorkspace(&disttae.Transaction{})
	proc := testutil.NewProcess(t)
	proc.Base.TxnOperator = op
	c := &Compile{proc: proc}
	teardown := func() {
		cancel()
		closeFn()
	}
	return c, op, teardown
}

func scanNode(role plan.ScanReadRole, tableType string, snapTS *timestamp.Timestamp) *plan.Node {
	node := &plan.Node{
		ScanReadRole: role,
		TableDef:     &plan.TableDef{TableType: tableType},
	}
	if snapTS != nil {
		node.ScanSnapshot = &plan.Snapshot{TS: snapTS}
	}
	return node
}

// The per-execution admission resolves the EFFECTIVE role of a marked scan
// against ITS OWN transaction snapshot (design D-1/D-8, R9-F3 in both
// directions): nested blocks never current-read; ordinary locked user
// tables under SI are admitted; an engaging explicit snapshot (H != 0 and
// strictly earlier than SnapshotTS — the same predicate that picks the
// historical clone op) rides the named-snapshot cohort and is refused; a
// non-engaging snapshot keeps the current read; other table types and a
// disabled execution are refused.
func TestExecutionReadScanAdmitted(t *testing.T) {
	sessionTS := timestamp.Timestamp{PhysicalTime: 100, LogicalTime: 1}
	c, _, teardown := admissionFixture(t, sessionTS)
	defer teardown()

	ordinary := func(role plan.ScanReadRole) *plan.Node {
		return scanNode(role, "r", nil)
	}

	// Nested provenance (subqueries, CTE bodies, view bodies, unmarked
	// statements) never current-reads.
	require.False(t, c.executionReadScanAdmitted(ordinary(plan.ScanReadRole_ScanNestedBlock)))
	// An ordinary locked user table under SI is admitted.
	require.True(t, c.executionReadScanAdmitted(ordinary(plan.ScanReadRole_ScanOuterLocking)))
	// Unknown/non-ordinary table kinds are not part of the surface.
	require.False(t, c.executionReadScanAdmitted(
		scanNode(plan.ScanReadRole_ScanOuterLocking, "v", nil)))
	require.False(t, c.executionReadScanAdmitted(
		scanNode(plan.ScanReadRole_ScanOuterLocking, "", nil)))
	// An engaging explicit snapshot (H < SnapshotTS, H != 0) rides the
	// historical cohort for this execution and gets no current read.
	hEngaged := timestamp.Timestamp{PhysicalTime: 50, LogicalTime: 1}
	require.False(t, c.executionReadScanAdmitted(
		scanNode(plan.ScanReadRole_ScanOuterLockingWithExplicitSnapshot, "r", &hEngaged)))
	// A non-engaging snapshot (H >= SnapshotTS) current-reads; the effective
	// role stays OuterLocking in BOTH crossing directions per execution.
	hLater := timestamp.Timestamp{PhysicalTime: 150, LogicalTime: 1}
	require.True(t, c.executionReadScanAdmitted(
		scanNode(plan.ScanReadRole_ScanOuterLockingWithExplicitSnapshot, "r", &hLater)))
	// Presence without a usable snapshot clause keeps the current read.
	require.True(t, c.executionReadScanAdmitted(
		scanNode(plan.ScanReadRole_ScanOuterLockingWithExplicitSnapshot, "r", nil)))
	zeroTS := timestamp.Timestamp{}
	require.True(t, c.executionReadScanAdmitted(
		scanNode(plan.ScanReadRole_ScanOuterLockingWithExplicitSnapshot, "r", &zeroTS)))

	// A disabled execution (G4 rollback already happened or a prior scope in
	// this execution could not bind) refuses every later marked scan.
	c.execReadDisabled = true
	require.False(t, c.executionReadScanAdmitted(ordinary(plan.ScanReadRole_ScanOuterLocking)))
	c.execReadDisabled = false
}

// The per-execution state dies with the execution (design D-8): after a
// reset, the next execution re-resolves its own admission, frontier and
// carrier from scratch.
func TestResetExecutionReadState(t *testing.T) {
	sessionTS := timestamp.Timestamp{PhysicalTime: 100, LogicalTime: 1}
	c, _, teardown := admissionFixture(t, sessionTS)
	defer teardown()

	c.execReadFrontier = timestamp.Timestamp{PhysicalTime: 200}
	c.execReadFrontierSet = true
	c.execReadOp = nil
	c.execReadOpOwner = c.proc.GetTxnOperator()
	c.execReadBoundScopes = []*Scope{{DataSource: &Source{}}}
	c.execReadDisabled = true
	c.execReadDisableReason = "x"
	c.resetExecutionReadState()
	require.Equal(t, timestamp.Timestamp{}, c.execReadFrontier)
	require.False(t, c.execReadFrontierSet)
	require.Nil(t, c.execReadOp)
	require.Nil(t, c.execReadOpOwner)
	require.Empty(t, c.execReadBoundScopes)
	require.False(t, c.execReadDisabled)
	require.Equal(t, "", c.execReadDisableReason)
}

// The rollback path (G4 all-or-nothing) resets every previously bound marked
// scope back onto the session operator with the session snapshot stamped on
// its DataSource, then disables the marking for the remainder of the
// execution.
func TestAbandonExecutionReadRollsBackAndDisables(t *testing.T) {
	sessionTS := timestamp.Timestamp{PhysicalTime: 100, LogicalTime: 1}
	c, _, teardown := admissionFixture(t, sessionTS)
	defer teardown()

	fakeRel := &abandonFakeRelation{}
	s := &Scope{DataSource: &Source{Rel: fakeRel, Timestamp: timestamp.Timestamp{PhysicalTime: 500}}}
	c.execReadBoundScopes = []*Scope{s}
	c.execReadFrontier = timestamp.Timestamp{PhysicalTime: 500}
	c.execReadFrontierSet = true

	require.NoError(t, c.abandonExecutionRead())
	require.Equal(t, 1, fakeRel.resets, "bound scope relation re-resolves onto the session operator")
	require.Equal(t, sessionTS, s.DataSource.Timestamp)
	require.Empty(t, c.execReadBoundScopes)
	require.True(t, c.execReadDisabled)
	require.False(t, c.execReadFrontierSet)
	require.Nil(t, c.execReadOp)

	require.False(t, c.executionReadScanAdmitted(
		scanNode(plan.ScanReadRole_ScanOuterLocking, "r", nil)),
		"a disabled execution refuses later marked scans")
}

// The cached carrier belongs to exactly ONE execution (D-8): while it wraps
// this execution's still-open operator the carrier is reused for the
// execution's later marked scopes, and a RECYCLED Compile (returned by the
// compile pool for a later statement without Reset running) must never
// re-serve that carrier — its wrapped transaction has typically committed or
// aborted, and handing it to disttae's validity guards would fail the whole
// statement with the PREVIOUS transaction's id.
func TestExecutionReadCarrierOwnerGuard(t *testing.T) {
	sessionTS := timestamp.Timestamp{PhysicalTime: 100, LogicalTime: 1}
	c, op, teardown := admissionFixture(t, sessionTS)
	defer teardown()

	// Within one execution a second marked scope reuses the same carrier.
	frontier := timestamp.Timestamp{PhysicalTime: 300, LogicalTime: 1}
	carrier := client.ApplyExecutionReadTS(op, frontier)
	c.execReadOp = carrier
	c.execReadOpOwner = op
	c.execReadFrontier = frontier
	c.execReadFrontierSet = true
	got, gotFrontier, err := c.executionReadOperator()
	require.NoError(t, err)
	require.Same(t, carrier, got, "same-execution marked scopes share one carrier")
	require.Equal(t, frontier, gotFrontier)

	// A recycled Compile serving a DIFFERENT execution (the previous
	// transaction already ended) must not re-serve the cached carrier: with
	// no barrier available here the freshness rebuild fails loudly instead
	// of returning a carrier that wraps the old, ended transaction.
	c2, op2, teardown2 := admissionFixture(t, sessionTS)
	defer teardown2()
	c2.execReadOp = carrier
	c2.execReadOpOwner = op
	c2.execReadFrontier = frontier
	c2.execReadFrontierSet = true
	require.NotSame(t, op2, op, "the fixture must present a different operator")
	staleOp, _, staleErr := c2.executionReadOperator()
	require.Error(t, staleErr, "a mismatched owner never re-serves the cached carrier")
	require.Nil(t, staleOp)
}

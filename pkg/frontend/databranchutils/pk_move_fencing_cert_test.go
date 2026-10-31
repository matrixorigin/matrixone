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

package databranchutils

import (
	"context"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/util/fault"
	"github.com/stretchr/testify/require"
)

// Certification cells (design D185(B.1)(vii), (H.10)(l)/(m), and the LEG1
// core of (H.10)(j) at fabric level). Every framework manipulation goes
// through the fault package's PRODUCTION exported surface — fault.Enable,
// fault.AddFaultPoint, fault.Disable, fault.Status, fault.TriggerFault —
// the very calls the debug surface wraps; no simulated check exists.

// certTestFloors is the fabric floor set: small enough to trip in tests.
func certTestFloors() DataBranchFencingWorkspaceFloors {
	return DataBranchFencingWorkspaceFloors{
		Commit: 1 << 20, Write: 5 << 20, Extra: 500 << 20, InsertEntry: 5000,
	}
}

// TestDataBranchFencingCertEnvelopeDisabledFramework: with the stock
// (disabled) framework, both checks pass; the certificate value the lane
// publishes reports both observations.
func TestDataBranchFencingCertEnvelopeDisabledFramework(t *testing.T) {
	require.False(t, fault.Status(), "baseline framework is disabled in tests")
	floors := certTestFloors()
	require.NoError(t, DataBranchFencingEnvelopeCertify(
		context.Background(), DataBranchFencingCertCheckA, 0, 0,
		DataBranchFencingStageShape{}, floors))
	shape := DataBranchFencingStageShape{TargetLedgerTable: true, InsertBatches: 1}
	require.NoError(t, DataBranchFencingEnvelopeCertify(
		context.Background(), DataBranchFencingCertCheckB, 1, 128, shape, floors))
	require.True(t, DataBranchFencingEnvelopeDisabled())
}

// TestDataBranchFencingCertEnvelopeViolation: an ENABLED framework —
// regardless of arming details the certificate cannot observe safely — is
// rejected deterministically at BOTH checks, and the typed failure
// survives the fencing error net.
func TestDataBranchFencingCertEnvelopeViolation(t *testing.T) {
	require.False(t, fault.Status())
	change := fault.Enable()
	require.True(t, change, "enable reports a fresh install")
	defer fault.Disable()
	require.False(t, DataBranchFencingEnvelopeDisabled())

	for _, check := range []DataBranchFencingCertCheck{
		DataBranchFencingCertCheckA, DataBranchFencingCertCheckB,
	} {
		err := DataBranchFencingEnvelopeCertify(
			context.Background(), check, 0, 0,
			DataBranchFencingStageShape{}, certTestFloors())
		require.Error(t, err)
		ce, ok := AsDataBranchFencingCertificationError(err)
		require.True(t, ok)
		require.Equal(t, check, ce.Check)
		require.Equal(t, DataBranchFencingCertEnvelopeViolation, ce.Class)
		// The fencing error net keeps the typed certification failure
		// extractable — the pool's lane classification reads it there.
		fe := &DataBranchFencingError{
			Class: DataBranchFencingRoundStageFailed, Step: DataBranchFencingStepRoundF1,
			Cause: err,
		}
		ce2, ok2 := AsDataBranchFencingCertificationError(fe)
		require.True(t, ok2)
		require.Equal(t, ce.Class, ce2.Class)
	}
}

// (H.10)(l) DELAYED-FREQUENCY ENVELOPE: the r74 counterexample schedule —
// the framework enabled and an arm whose frequency window fires only from
// invocation 2 ('2:::') — is rejected at CHECK B WITHOUT probing the
// point: the certification consumes no invocation, so the arming's own
// count starts at zero AFTER the rejection. The cell then probes manually
// to prove the distinction: invocation 1 does NOT fire (before its start
// window), and the bypass the envelope deterministically rejected would
// have fired on the next consult.
func TestDataBranchFencingCertDelayedFrequencyEnvelope(t *testing.T) {
	require.False(t, fault.Status())
	fault.Enable()
	cfg := context.Background()
	name := "dbfc.delayed.envelope.cell"
	require.NoError(t, fault.AddFaultPoint(cfg, name, "2:::", "ECHO", 42, "delayed", false))
	defer func() {
		_, _ = fault.RemoveFaultPoint(cfg, name)
		fault.Disable()
	}()

	// CHECK B with the armed delayed-frequency point: deterministic
	// envelope violation, no probe consumed.
	err := DataBranchFencingEnvelopeCertify(
		cfg, DataBranchFencingCertCheckB, 0, 0,
		DataBranchFencingStageShape{TargetLedgerTable: true, InsertBatches: 1},
		certTestFloors())
	require.Error(t, err)
	ce, ok := AsDataBranchFencingCertificationError(err)
	require.True(t, ok)
	require.Equal(t, DataBranchFencingCertEnvelopeViolation, ce.Class)

	// Non-probing proof: the point's invocation count starts at zero after
	// certification. Manual invocation 1 must NOT fire its ECHO ('2:::'
	// fires strictly from invocation 2); had certification probed, our
	// first manual consult would be invocation >= 2 and would have fired.
	// TriggerFault reports fired=true only when the invocation lands in
	// the frequency window; invocation 1 of '2:::' reports fired=false.
	_, _, fired := fault.TriggerFault(name)
	require.False(t, fired, "invocation 1 precedes the firing window; "+
		"certification consumed no consult")
	iret, sret, fired := fault.TriggerFault(name)
	require.True(t, fired, "invocation 2 fires the delayed arm")
	require.Equal(t, int64(42), iret)
	require.Equal(t, "delayed", sret)
}

// (H.10)(m) BLOCKING-ACTION BOUNDEDNESS: an armed WAIT point — the
// blocking shape an honest probe could never bound — costs the
// certification nothing: the check completes promptly with the envelope
// violation because it never executes any action. A sibling manual consult
// does block, demonstrating the hazard the envelope avoids on purpose.
func TestDataBranchFencingCertBlockingActionBoundedness(t *testing.T) {
	require.False(t, fault.Status())
	fault.Enable()
	cfg := context.Background()
	name := "dbfc.blocking.envelope.cell"
	require.NoError(t, fault.AddFaultPoint(cfg, name, ":::", "WAIT", 0, "w", false))
	defer func() {
		_, _ = fault.RemoveFaultPoint(cfg, name)
		fault.Disable()
	}()

	done := make(chan error, 1)
	go func() {
		done <- DataBranchFencingEnvelopeCertify(
			cfg, DataBranchFencingCertCheckB, 0, 0,
			DataBranchFencingStageShape{TargetLedgerTable: true, PointTombstones: 1},
			certTestFloors())
	}()
	select {
	case err := <-done:
		require.Error(t, err)
		ce, ok := AsDataBranchFencingCertificationError(err)
		require.True(t, ok)
		require.Equal(t, DataBranchFencingCertEnvelopeViolation, ce.Class)
	case <-time.After(3 * time.Second):
		t.Fatal("certification blocked on an armed WAIT action: it must never probe")
	}

	// Hazard sibling: a real consult DOES block on WAIT, released only
	// when its own context is cancelled — the exact unboundedness the
	// envelope load sidesteps by never executing actions.
	blocked := make(chan struct{})
	wctx, wcancel := context.WithCancel(context.Background())
	go func() {
		fault.TriggerFaultWithContext(wctx, name)
		close(blocked)
	}()
	select {
	case <-blocked:
		t.Fatal("the WAIT consult should block until its context cancels")
	case <-time.After(300 * time.Millisecond):
	}
	wcancel()
	select {
	case <-blocked:
	case <-time.After(2 * time.Second):
		t.Fatal("context cancellation did not release the WAIT waiter")
	}
}

// TestDataBranchFencingCertFloorBreach: CHECK B rejects a staged shape not
// strictly under the compiled floors at the equal boundary; CHECK A never
// consults floors; zero floors skip the comparison; the disabled
// certification defect flag (LEG2/LEG3 harnesses) bypasses the check
// entirely and restores afterwards.
func TestDataBranchFencingCertFloorBreach(t *testing.T) {
	require.False(t, fault.Status())
	ctx := context.Background()
	floors := certTestFloors()
	shape := DataBranchFencingStageShape{TargetLedgerTable: true, InsertBatches: 1}

	// Equal-to-floor breaches on every floor axis, one at a time.
	err := DataBranchFencingEnvelopeCertify(ctx, DataBranchFencingCertCheckB,
		int64(floors.InsertEntry), 1, shape, floors)
	require.Error(t, err)
	ce, _ := AsDataBranchFencingCertificationError(err)
	require.Equal(t, DataBranchFencingCertFloorBreach, ce.Class)

	err = DataBranchFencingEnvelopeCertify(ctx, DataBranchFencingCertCheckB,
		1, floors.Commit, shape, floors)
	require.Error(t, err)
	ce, _ = AsDataBranchFencingCertificationError(err)
	require.Equal(t, DataBranchFencingCertFloorBreach, ce.Class)

	err = DataBranchFencingEnvelopeCertify(ctx, DataBranchFencingCertCheckB,
		1, floors.Write, shape, floors)
	require.Error(t, err)
	err = DataBranchFencingEnvelopeCertify(ctx, DataBranchFencingCertCheckB,
		1, floors.Extra, shape, floors)
	require.Error(t, err)

	// Strictly under the floors passes; CHECK A ignores floors entirely.
	require.NoError(t, DataBranchFencingEnvelopeCertify(ctx, DataBranchFencingCertCheckB,
		int64(floors.InsertEntry)-1, floors.Commit-1, shape, floors))
	require.NoError(t, DataBranchFencingEnvelopeCertify(ctx, DataBranchFencingCertCheckA,
		int64(floors.InsertEntry)+100, floors.Extra+100, shape, floors))

	// Zero floors skip the byte/row comparisons.
	require.NoError(t, DataBranchFencingEnvelopeCertify(ctx, DataBranchFencingCertCheckB,
		1<<40, 1<<40, shape, DataBranchFencingWorkspaceFloors{}))

	// The certification-disabled defect flag passes even an ENABLED
	// framework's check, and restore returns to enforcement.
	fault.Enable()
	defer fault.Disable()
	require.Error(t, DataBranchFencingEnvelopeCertify(ctx, DataBranchFencingCertCheckA,
		0, 0, shape, floors))
	restore := DisableDataBranchFencingCertificationForTest(true)
	require.NoError(t, DataBranchFencingEnvelopeCertify(ctx, DataBranchFencingCertCheckB,
		1<<40, 1<<40, shape, floors))
	restore()
	require.Error(t, DataBranchFencingEnvelopeCertify(ctx, DataBranchFencingCertCheckA,
		0, 0, shape, floors))
}

// TestDataBranchFencingRecordCertificateMerge: record assembly merges the
// per-stage certificate values (observations OR'd; decisive stage's shape
// and floors preferred); zero certificates stay tolerated for legacy
// fabric producers, and Complete() reports the merged observation picture.
func TestDataBranchFencingRecordCertificateMerge(t *testing.T) {
	binding := csBinding(7)
	grant := DataBranchScratchGrant{Binding: binding, WitnessTS: timestamp.Timestamp{PhysicalTime: 5}}
	shape1 := DataBranchFencingStageShape{TargetLedgerTable: true, InsertBatches: 1}
	shape2 := DataBranchFencingStageShape{TargetLedgerTable: true, PointTombstones: 1}
	floors := certTestFloors()
	in := DataBranchFencingRoundInput{ScratchTableID: 9, IssueTimeNS: 1}

	f1 := DataBranchFencingF1Commit{
		CommitTS: timestamp.Timestamp{PhysicalTime: 10}, Shape: shape1,
		Cert: DataBranchFencingCertificate{
			CheckAEnvelopeDataObserved: true, CheckBEnvelopeDataObserved: true,
			Staged: shape1, StagedRows: 1, StagedBytes: 128, Floors: floors,
		},
	}
	f2 := DataBranchFencingF2Commit{
		ProbeFound: true, CommitTS: timestamp.Timestamp{PhysicalTime: 20}, Shape: shape2,
		Cert: DataBranchFencingCertificate{
			CheckAEnvelopeDataObserved: true, CheckBEnvelopeDataObserved: true,
			Staged: shape2, StagedRows: 1, StagedBytes: 128, Floors: floors,
		},
	}
	rec, err := AssembleDataBranchFencingRecord(grant, f1, f2, in)
	require.NoError(t, err)
	require.True(t, rec.Certificate.Complete())
	require.Equal(t, shape2, rec.Certificate.Staged, "the decisive stage's shape wins")
	require.Equal(t, floors, rec.Certificate.Floors)

	// Zero certificates merge without breaking assembly (legacy fabric).
	rec2, err := AssembleDataBranchFencingRecord(
		grant,
		DataBranchFencingF1Commit{CommitTS: timestamp.Timestamp{PhysicalTime: 10}, Shape: shape1},
		DataBranchFencingF2Commit{
			ProbeFound: true, CommitTS: timestamp.Timestamp{PhysicalTime: 20}, Shape: shape2,
		},
		in,
	)
	require.NoError(t, err)
	require.False(t, rec2.Certificate.Complete())
}

// (H.10)(j) LEG1 CORE at fabric level: a round armed mid-lane — the
// framework enabled between the stage lane's CHECK A and CHECK B — is
// rejected at CHECK B through the REAL certification call, the stage rolls
// back in body, NO commit is invoked, and the round classifies
// stage-failed through the driver.
func TestDataBranchFencingCertLeg1MidLaneArming(t *testing.T) {
	require.False(t, fault.Status())
	ctx := context.Background()
	binding := csBinding(11)
	floors := certTestFloors()

	locks := &fakeScratchLocks{binding: binding}
	exec := &certFabricExecutor{floors: floors, armBetween: true}
	pool := NewDataBranchFencingPool(DataBranchFencingDriverConfig{LanePoolCapacity: 4})
	driver := NewDataBranchFencingDriver(pool)
	in := DataBranchFencingRoundInput{ScratchTableID: 31337, IssueTimeNS: uint64(time.Now().UnixNano())}

	defer fault.Disable()
	rec, err := driver.RunRound(ctx, locks, exec, in)
	require.Error(t, err)
	require.Equal(t, DataBranchFencingRecord{}, rec, "no record assembles off a rejected round")
	ce, ok := AsDataBranchFencingCertificationError(err)
	require.True(t, ok, "the typed certification failure survives the driver classification")
	require.Equal(t, DataBranchFencingCertCheckB, ce.Check)
	require.Equal(t, DataBranchFencingCertEnvelopeViolation, ce.Class)
	fe, ok := AsDataBranchFencingError(err)
	require.True(t, ok)
	require.Equal(t, DataBranchFencingRoundStageFailed, fe.Class)
	require.Equal(t, 0, exec.commitAttempts, "NO commit RPC was sent at the violation")
	require.True(t, exec.rolledBack, "the stage rolled back in body")

	// The same fabric, never armed, certifies and commits the pair.
	fault.Disable()
	exec2 := &certFabricExecutor{floors: floors}
	rec2, err := driver.RunRound(ctx, locks, exec2, in)
	require.NoError(t, err)
	require.True(t, rec2.Certificate.Complete())
	require.Equal(t, 2, exec2.commitAttempts)
}

// certFabricExecutor is the fabric counterpart of the production stage
// executor's certification discipline: CHECK A at lane admission, CHECK B
// immediately before the commit is invoked, in-body rollback with no
// commit RPC on a certification failure.
type certFabricExecutor struct {
	floors         DataBranchFencingWorkspaceFloors
	armBetween     bool
	commitAttempts int
	rolledBack     bool
	seq            int
}

func (e *certFabricExecutor) certify(check DataBranchFencingCertCheck) error {
	return DataBranchFencingEnvelopeCertify(
		context.Background(), check, 1, 128,
		DataBranchFencingStageShape{TargetLedgerTable: true, InsertBatches: 1}, e.floors)
}

func (e *certFabricExecutor) CommitRoundF1(
	ctx context.Context, in DataBranchFencingRoundInput,
) (DataBranchFencingF1Commit, error) {
	if err := e.certify(DataBranchFencingCertCheckA); err != nil {
		e.rolledBack = true
		return DataBranchFencingF1Commit{}, err
	}
	if e.armBetween {
		// Mid-lane arming: enable the framework between CHECK A and
		// CHECK B — the LEG1 schedule's decisive move.
		fault.Enable()
	}
	if err := e.certify(DataBranchFencingCertCheckB); err != nil {
		e.rolledBack = true
		return DataBranchFencingF1Commit{}, err
	}
	e.commitAttempts++
	e.seq++
	params := timestamp.Timestamp{PhysicalTime: int64(1000 + e.seq)}
	shape := DataBranchFencingStageShape{TargetLedgerTable: true, InsertBatches: 1}
	return DataBranchFencingF1Commit{
		CommitTS: params, Shape: shape,
		Cert: DataBranchFencingCertificate{
			CheckAEnvelopeDataObserved: true, CheckBEnvelopeDataObserved: true,
			Staged: shape, StagedRows: 1, StagedBytes: 128, Floors: e.floors,
		},
	}, nil
}

func (e *certFabricExecutor) CommitRoundF2(
	ctx context.Context, in DataBranchFencingRoundInput,
) (DataBranchFencingF2Commit, error) {
	if err := e.certify(DataBranchFencingCertCheckA); err != nil {
		e.rolledBack = true
		return DataBranchFencingF2Commit{}, err
	}
	if e.armBetween {
		fault.Enable()
	}
	if err := e.certify(DataBranchFencingCertCheckB); err != nil {
		e.rolledBack = true
		return DataBranchFencingF2Commit{}, err
	}
	e.commitAttempts++
	e.seq++
	shape := DataBranchFencingStageShape{TargetLedgerTable: true, PointTombstones: 1}
	return DataBranchFencingF2Commit{
		ProbeFound: true,
		CommitTS:   timestamp.Timestamp{PhysicalTime: int64(1000 + e.seq)},
		Shape:      shape,
		Cert: DataBranchFencingCertificate{
			CheckAEnvelopeDataObserved: true, CheckBEnvelopeDataObserved: true,
			Staged: shape, StagedRows: 1, StagedBytes: 128, Floors: e.floors,
		},
	}, nil
}

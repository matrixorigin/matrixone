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
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/stretchr/testify/require"
)

// These fabric cells drive the shared retirement critical-section core
// (RunDataBranchFencingRetireCS) with scripted round/grant/frontier
// closures: acceptance ordering (grant -> accept -> advance -> reprobe),
// rejection side-effect-freedom, the D176(B.2) re-fence classes and the K
// bound, the round-failure budget (D177(B.4)), ErrTxnUnknown consumption
// upstream, and error propagation. They never touch services, engines, or
// lock services: production closure proof is the live battery's job.

// csFabric is one scripted critical-section environment.
type csFabric struct {
	mu       sync.Mutex
	order    []string
	records  []DataBranchFencingRecord // successive round outcomes
	roundErr []error                   // successive round errors (override)
	retired  bool

	grants   []DataBranchRetireLedgerGrant
	presence bool

	roundCalls  int
	grantCalls  int
	advanceCall int
	fences      []timestamp.Timestamp
	reprobeCall int
	inputCalls  int

	foldCalls  int
	foldFloors []timestamp.Timestamp
	foldErrs   []error // successive fold errors (unset entries succeed)
}

func (f *csFabric) note(step string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.order = append(f.order, step)
}

type csFabricRounds struct{ f *csFabric }

func (r *csFabricRounds) RunRound(
	ctx context.Context,
	locks DataBranchScratchLocks,
	exec DataBranchFencingStageExecutor,
	in DataBranchFencingRoundInput,
) (DataBranchFencingRecord, error) {
	f := r.f
	f.mu.Lock()
	defer f.mu.Unlock()
	f.order = append(f.order, "round")
	idx := f.roundCalls
	f.roundCalls++
	if idx < len(f.roundErr) && f.roundErr[idx] != nil {
		return DataBranchFencingRecord{}, f.roundErr[idx]
	}
	if len(f.records) > 0 {
		rec := f.records[0]
		f.records = f.records[1:]
		return rec, nil
	}
	return DataBranchFencingRecord{}, fmt.Errorf("unscripted round %d", idx)
}

func (r *csFabricRounds) Retire() {
	r.f.mu.Lock()
	defer r.f.mu.Unlock()
	r.f.order = append(r.f.order, "retire")
	r.f.retired = true
}

func (f *csFabric) deps(
	binding DataBranchFencingBinding,
) DataBranchRetireCSDeps {
	return DataBranchRetireCSDeps{
		Rounds: &csFabricRounds{f: f},
		Locks:  nil,
		Exec:   nil,
		AcquireLedgerGrant: func(ctx context.Context) DataBranchRetireLedgerGrant {
			f.mu.Lock()
			defer f.mu.Unlock()
			f.order = append(f.order, "grant")
			idx := f.grantCalls
			f.grantCalls++
			if idx < len(f.grants) {
				return f.grants[idx]
			}
			return DataBranchRetireLedgerGrant{Err: fmt.Errorf("unscripted grant %d", idx)}
		},
		AdvanceToFence: func(ctx context.Context, fence timestamp.Timestamp) error {
			f.mu.Lock()
			defer f.mu.Unlock()
			f.order = append(f.order, "advance")
			f.advanceCall++
			f.fences = append(f.fences, fence)
			return nil
		},
		ReprobeLinkPresence: func(ctx context.Context) (bool, error) {
			f.mu.Lock()
			defer f.mu.Unlock()
			f.order = append(f.order, "reprobe")
			f.reprobeCall++
			return f.presence, nil
		},
		NewRoundInput: func() (DataBranchFencingRoundInput, error) {
			f.mu.Lock()
			defer f.mu.Unlock()
			f.order = append(f.order, "input")
			f.inputCalls++
			return DataBranchFencingRoundInput{
				ScratchTableID: uint64(1000 + f.inputCalls),
				IssueTimeNS:    uint64(2000 + f.inputCalls),
			}, nil
		},
	}
}

// withFold wires the fabric's watermark-fold closure into the deps: the
// cells that leave it nil pin the legacy step order (the skip path), and
// the cells that wire it pin the fold's exact position and failure
// semantics.
func (f *csFabric) withFold(deps DataBranchRetireCSDeps) DataBranchRetireCSDeps {
	deps.AwaitWatermarkFold = func(ctx context.Context, floor timestamp.Timestamp) error {
		f.mu.Lock()
		defer f.mu.Unlock()
		f.order = append(f.order, "fold")
		f.foldFloors = append(f.foldFloors, floor)
		idx := f.foldCalls
		f.foldCalls++
		if idx < len(f.foldErrs) {
			return f.foldErrs[idx]
		}
		return nil
	}
	return deps
}

// fabricRecord builds one assembly-valid record with the given binding and
// commit watermark pair (C_f1 < C_f).
func fabricRecord(binding DataBranchFencingBinding, cf1, cf int64) DataBranchFencingRecord {
	return DataBranchFencingRecord{
		ScratchTableID: 4242,
		IssueTimeNS:    7,
		CommitTS1:      timestamp.Timestamp{PhysicalTime: cf1},
		CommitTS:       timestamp.Timestamp{PhysicalTime: cf},
		Binding:        binding,
	}
}

func csBinding(version uint64) DataBranchFencingBinding {
	return DataBranchFencingBinding{Table: 77, ServiceID: "cn-x", Version: version}
}

// TestDataBranchRetireCSAcceptOrder is the happy path: the round's record
// is accepted against the ledger grant, and the frontier advance plus the
// locked re-probe run in exactly that order before the caller stages any
// delete.
func TestDataBranchRetireCSAcceptOrder(t *testing.T) {
	f := &csFabric{presence: true}
	binding := csBinding(3)
	rec := fabricRecord(binding, 100, 200)
	f.records = []DataBranchFencingRecord{rec}
	f.grants = []DataBranchRetireLedgerGrant{{
		Binding: binding, Fence: timestamp.Timestamp{PhysicalTime: 250},
		BindingRecordedBefore: true,
	}}
	deps := f.deps(binding)
	presence, err := RunDataBranchFencingRetireCS(context.Background(), deps)
	require.NoError(t, err)
	require.True(t, presence)
	require.True(t, f.retired, "the CS retires the statement's driver")
	// Exact order: input, round, grant, advance, reprobe, retire and
	// precisely ONE of each per-address work item.
	require.Equal(t,
		[]string{"input", "round", "grant", "advance", "reprobe", "retire"},
		f.order)
	require.Equal(t, 1, f.advanceCall)
	require.Equal(t, timestamp.Timestamp{PhysicalTime: 250}, f.fences[0],
		"the frontier advances to the grant watermark, never a local sample")
}

// TestDataBranchRetireCSFoldLagRefenceThenAccept exercises class (ii): the
// same-generation grant watermark lags the round's C_f (fold lag) and the
// CS re-fences in place; a later round's record accepts. The rejection
// itself is side-effect-free: no advance, no reprobe between rounds.
func TestDataBranchRetireCSFoldLagRefenceThenAccept(t *testing.T) {
	f := &csFabric{presence: false}
	binding := csBinding(3)
	rec := fabricRecord(binding, 100, 200)
	f.records = []DataBranchFencingRecord{rec, rec}
	f.grants = []DataBranchRetireLedgerGrant{
		{Binding: binding, Fence: timestamp.Timestamp{PhysicalTime: 150}, BindingRecordedBefore: true},
		{Binding: binding, Fence: timestamp.Timestamp{PhysicalTime: 200}, BindingRecordedBefore: true},
	}
	deps := f.deps(binding)
	presence, err := RunDataBranchFencingRetireCS(context.Background(), deps)
	require.NoError(t, err)
	require.False(t, presence)
	require.Equal(t, 2, f.roundCalls)
	require.Equal(t, 1, f.advanceCall, "no advance happened at the rejection")
	require.Equal(t, 1, f.reprobeCall, "no reprobe happened at the rejection")
}

// TestDataBranchRetireCSPreGrantFlipRefence exercises class (iii): the
// retiring transaction had NOT recorded the ledger binding before its
// grant, and its first grant appended the fresh generation (fence below
// C_f): ONE in-place re-fence then acceptance.
func TestDataBranchRetireCSPreGrantFlipRefence(t *testing.T) {
	f := &csFabric{}
	binding := csBinding(9)
	rec := fabricRecord(binding, 100, 200)
	f.records = []DataBranchFencingRecord{rec, rec}
	f.grants = []DataBranchRetireLedgerGrant{
		{Binding: binding, Fence: timestamp.Timestamp{PhysicalTime: 150}, BindingRecordedBefore: false},
		{Binding: binding, Fence: timestamp.Timestamp{PhysicalTime: 200}, BindingRecordedBefore: false},
	}
	deps := f.deps(binding)
	_, err := RunDataBranchFencingRetireCS(context.Background(), deps)
	require.NoError(t, err)
	require.Equal(t, 2, f.roundCalls)
}

// TestDataBranchRetireCSBindingChangedGrantErr is class (i): the lock
// layer's own AddLockTable Changed branch raised ErrLockTableBindChanged
// inside the grant; it propagates VERBATIM (the repository's whole-
// transaction rollback boundary), with no re-fence and no advance.
func TestDataBranchRetireCSBindingChangedGrantErr(t *testing.T) {
	f := &csFabric{}
	binding := csBinding(3)
	f.records = []DataBranchFencingRecord{fabricRecord(binding, 100, 200)}
	bindChanged := moerr.NewLockTableBindChangedNoCtx()
	f.grants = []DataBranchRetireLedgerGrant{{Err: bindChanged}}
	deps := f.deps(binding)
	_, err := RunDataBranchFencingRetireCS(context.Background(), deps)
	require.ErrorIs(t, err, bindChanged, "grant errors propagate verbatim")
	require.Equal(t, 0, f.advanceCall)
	require.Equal(t, 1, f.roundCalls, "no re-fence on a terminal class")
}

// TestDataBranchRetireCSRefenceExhaustion binds the K bound: a persistent
// same-generation mismatch (B_r != B_f while the transaction had recorded
// the table BUT under a different generation) terminates in
// ErrLockTableBindChanged exactly at K+1 consecutive rejections.
func TestDataBranchRetireCSRefenceExhaustion(t *testing.T) {
	f := &csFabric{}
	binding := csBinding(3)
	rec := fabricRecord(binding, 100, 200)
	// K+1 scripted rounds and grants; every grant mismatches the binding
	// (a generation OTHER than B_f on a recorded table => class (i) is for
	// GrantErr; a same-view contradictory generation rides the counter).
	mismatch := csBinding(4)
	for i := 0; i <= DataBranchFencingMaxRefenceRounds; i++ {
		f.records = append(f.records, rec)
		f.grants = append(f.grants, DataBranchRetireLedgerGrant{
			Binding: mismatch, Fence: timestamp.Timestamp{PhysicalTime: 250},
			BindingRecordedBefore: true,
		})
	}
	deps := f.deps(binding)
	_, err := RunDataBranchFencingRetireCS(context.Background(), deps)
	require.Error(t, err)
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrLockTableBindChanged),
		"K+1 consecutive reachable rejections raise ErrLockTableBindChanged, got %v", err)
	require.Equal(t, 0, f.advanceCall, "rejection stays side-effect-free")
	require.LessOrEqual(t, f.roundCalls, DataBranchFencingMaxRefenceRounds+1)
}

// TestDataBranchRetireCSRoundFailureBudget is D177(B.4): rounds that fail
// before any acceptance rejection retry the whole pair within the bound,
// never advance the re-fence counter, and raise the determinate
// fencing-unavailable statement error at exhaustion.
func TestDataBranchRetireCSRoundFailureBudget(t *testing.T) {
	f := &csFabric{}
	roundErr := &DataBranchFencingError{
		Class: DataBranchFencingRoundExpired, Step: DataBranchFencingStepRoundF1,
		Cause: fmt.Errorf("t_round fired"),
	}
	for i := 0; i <= DataBranchFencingMaxRoundFailures; i++ {
		f.roundErr = append(f.roundErr, roundErr)
	}
	deps := f.deps(csBinding(3))
	_, err := RunDataBranchFencingRetireCS(context.Background(), deps)
	require.Error(t, err)
	require.Contains(t, err.Error(), "data branch fencing unavailable")
	require.Equal(t, DataBranchFencingMaxRoundFailures+1, f.roundCalls,
		"the whole-pair retry bound is MaxRoundFailures+1 attempts")
	require.Equal(t, 0, f.grantCalls, "failed rounds never reached the grant")
	// One failure short of exhaustion DOES accept: the budget is per
	// failure, and round failures never advance the re-fence counter.
	f2 := &csFabric{}
	binding := csBinding(3)
	rec := fabricRecord(binding, 100, 200)
	f2.roundErr = []error{roundErr, roundErr, roundErr}
	f2.records = []DataBranchFencingRecord{rec}
	f2.grants = []DataBranchRetireLedgerGrant{{
		Binding: binding, Fence: timestamp.Timestamp{PhysicalTime: 250}, BindingRecordedBefore: true,
	}}
	deps2 := f2.deps(binding)
	_, err = RunDataBranchFencingRetireCS(context.Background(), deps2)
	require.NoError(t, err)
	require.Equal(t, 4, f2.roundCalls)
	require.Equal(t, 1, f2.grantCalls)
}

// TestDataBranchRetireCSContextCancel is the interrupt path: a cancelled
// statement context surfaces context.Canceled before any round work.
func TestDataBranchRetireCSContextCancel(t *testing.T) {
	f := &csFabric{}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	deps := f.deps(csBinding(3))
	_, err := RunDataBranchFencingRetireCS(ctx, deps)
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, 0, f.roundCalls)
	require.True(t, f.retired, "the driver still retires on the way out")
}

// TestDataBranchRetireCSRoundInputFailure fails round-input construction
// loudly through the fencing-unavailable net (gate-rejected class): a CS
// that cannot draw a fresh scratch identity may never fall back to a
// reused one.
func TestDataBranchRetireCSRoundInputFailure(t *testing.T) {
	f := &csFabric{}
	deps := f.deps(csBinding(3))
	broken := fmt.Errorf("entropy draw failed")
	deps.NewRoundInput = func() (DataBranchFencingRoundInput, error) {
		return DataBranchFencingRoundInput{}, broken
	}
	_, err := RunDataBranchFencingRetireCS(context.Background(), deps)
	require.Error(t, err)
	require.Contains(t, err.Error(), "data branch fencing unavailable")
	require.Contains(t, err.Error(), broken.Error())
	require.Equal(t, 0, f.roundCalls)
}

// TestDataBranchRefenceCounterTerminalFirst is the counter contract: the
// terminal class binds on FIRST consumption (no K tolerance); only classes
// (ii)/(iii) spend K.
func TestDataBranchRefenceCounterTerminalFirst(t *testing.T) {
	c := NewDataBranchRefenceCounter(2)
	require.NoError(t, c.Consume(context.Background(), DataBranchRetireRejectionFoldLag))
	require.NoError(t, c.Consume(context.Background(), DataBranchRetireRejectionPreGrantFlip))
	require.Error(t, c.Consume(context.Background(), DataBranchRetireRejectionFoldLag),
		"K+1 consecutive reachable rejections exhaust")
	c2 := NewDataBranchRefenceCounter(2)
	err := c2.Consume(context.Background(), DataBranchRetireRejectionBindingChanged)
	require.Error(t, err)
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrLockTableBindChanged))
}

// TestDataBranchRetireCSGrantBindsFencingErrorNet converts a raw round
// failure lacking the typed net into the determinate statement error at
// budget exhaustion (statementErrorOfFencingRound's fallback arm).
func TestDataBranchRetireCSGrantBindsFencingErrorNet(t *testing.T) {
	f := &csFabric{}
	rawErr := errors.New("driver exploded")
	for i := 0; i <= DataBranchFencingMaxRoundFailures; i++ {
		f.roundErr = append(f.roundErr, rawErr)
	}
	deps := f.deps(csBinding(3))
	_, err := RunDataBranchFencingRetireCS(context.Background(), deps)
	require.Error(t, err)
	require.Contains(t, err.Error(), "data branch fencing unavailable")
	require.Contains(t, err.Error(), rawErr.Error())
}

// TestDataBranchRetireCSAdvanceAndReprobeErrors verifies the post-
// acceptance closure errors propagate verbatim: a frontier failure or a
// re-probe failure aborts the retirement before any paired delete.
func TestDataBranchRetireCSAdvanceAndReprobeErrors(t *testing.T) {
	mk := func() (*csFabric, DataBranchRetireCSDeps) {
		f := &csFabric{}
		binding := csBinding(3)
		f.records = []DataBranchFencingRecord{fabricRecord(binding, 100, 200)}
		f.grants = []DataBranchRetireLedgerGrant{{
			Binding: binding, Fence: timestamp.Timestamp{PhysicalTime: 250}, BindingRecordedBefore: true,
		}}
		deps := f.deps(binding)
		return f, deps
	}
	advErr := fmt.Errorf("wait log tail applied failed")
	f1, deps1 := mk()
	deps1.AdvanceToFence = func(ctx context.Context, fence timestamp.Timestamp) error {
		f1.note("advance")
		return advErr
	}
	_, err := RunDataBranchFencingRetireCS(context.Background(), deps1)
	require.ErrorIs(t, err, advErr)
	require.Equal(t, []string{"input", "round", "grant", "advance", "retire"}, f1.order)

	repErr := fmt.Errorf("reprove crashed")
	f2, deps2 := mk()
	f2.presence = false
	deps2.ReprobeLinkPresence = func(ctx context.Context) (bool, error) {
		f2.note("reprobe")
		return false, repErr
	}
	_, err = RunDataBranchFencingRetireCS(context.Background(), deps2)
	require.ErrorIs(t, err, repErr)
	require.Equal(t, []string{"input", "round", "grant", "advance", "reprobe", "retire"}, f2.order)
}

// Compile-time assurance that the fabric rounds type satisfies the CS's
// driver surface.
var _ DataBranchRetireFencingRounds = (*csFabricRounds)(nil)

// TestDataBranchRetireCSWatermarkFoldOrder pins the fold-wait's position
// in the critical section when the closure is wired: exactly once per
// round, after the round and BEFORE the ledger grant. The nil-fold cells
// above pin the legacy order (the skip path fabric rounds require: their
// mocks advance watermarks synchronously).
func TestDataBranchRetireCSWatermarkFoldOrder(t *testing.T) {
	f := &csFabric{presence: true}
	binding := csBinding(3)
	rec := fabricRecord(binding, 100, 200)
	f.records = []DataBranchFencingRecord{rec}
	f.grants = []DataBranchRetireLedgerGrant{{
		Binding: binding, Fence: timestamp.Timestamp{PhysicalTime: 250},
		BindingRecordedBefore: true,
	}}
	presence, err := RunDataBranchFencingRetireCS(
		context.Background(), f.withFold(f.deps(binding)))
	require.NoError(t, err)
	require.True(t, presence)
	require.Equal(t,
		[]string{"input", "round", "fold", "grant", "advance", "reprobe", "retire"},
		f.order)
	require.Equal(t, 1, f.foldCalls)
	require.Equal(t, []timestamp.Timestamp{{PhysicalTime: 200}}, f.foldFloors,
		"the fold floor is the round's own C_f2")
}

// TestDataBranchRetireCSWatermarkFoldFailureBudget treats a fold-wait
// failure as a whole-pair round failure: it consumes the round-failure
// budget (never the re-fence counter), retries the pair, and converts to
// the determinate stage-failed statement error at exhaustion; one failure
// short of exhaustion still accepts.
func TestDataBranchRetireCSWatermarkFoldFailureBudget(t *testing.T) {
	binding := csBinding(3)
	rec := fabricRecord(binding, 100, 200)
	foldErr := errors.New("fold probe exploded")
	f := &csFabric{}
	for i := 0; i <= DataBranchFencingMaxRoundFailures; i++ {
		f.records = append(f.records, rec)
		f.foldErrs = append(f.foldErrs, foldErr)
	}
	_, err := RunDataBranchFencingRetireCS(
		context.Background(), f.withFold(f.deps(binding)))
	require.Error(t, err)
	require.Contains(t, err.Error(), "data branch fencing unavailable")
	require.Equal(t, DataBranchFencingMaxRoundFailures+1, f.roundCalls,
		"fold failures retry the whole pair within the round budget")
	require.Equal(t, DataBranchFencingMaxRoundFailures+1, f.foldCalls)
	require.Equal(t, 0, f.grantCalls, "a failed fold never reached the grant")

	// One failure short of exhaustion DOES accept: the budget is per
	// failure and fold failures never advance the re-fence counter.
	f2 := &csFabric{presence: true}
	for i := 0; i <= DataBranchFencingMaxRoundFailures; i++ {
		f2.records = append(f2.records, rec)
	}
	for i := 0; i < DataBranchFencingMaxRoundFailures; i++ {
		f2.foldErrs = append(f2.foldErrs, foldErr)
	}
	f2.grants = []DataBranchRetireLedgerGrant{{
		Binding: binding, Fence: timestamp.Timestamp{PhysicalTime: 250},
		BindingRecordedBefore: true,
	}}
	presence, err := RunDataBranchFencingRetireCS(
		context.Background(), f2.withFold(f2.deps(binding)))
	require.NoError(t, err)
	require.True(t, presence)
	require.Equal(t, DataBranchFencingMaxRoundFailures+1, f2.roundCalls)
	require.Equal(t, DataBranchFencingMaxRoundFailures+1, f2.foldCalls)
	require.Equal(t, 1, f2.grantCalls)
	require.Equal(t, 1, f2.advanceCall)
}

// Sanity: DataBranchFencingNewRoundInput draws fresh scratch identities.
func TestDataBranchFencingNewRoundInputFreshness(t *testing.T) {
	first, err := DataBranchFencingNewRoundInput()
	require.NoError(t, err)
	second, err := DataBranchFencingNewRoundInput()
	require.NoError(t, err)
	require.NotEqual(t, first.ScratchTableID, second.ScratchTableID,
		"every round draws a fresh scratch identity")
	require.NotZero(t, first.IssueTimeNS)
	require.Greater(t, first.SweepAgeCutoffNS, uint64(0))
	require.Greater(t, int64(first.SweepLockWait), int64(0))
}

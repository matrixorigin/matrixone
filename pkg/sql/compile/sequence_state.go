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
	"maps"
	"time"

	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// sequenceStatementState is the session-visible sequence state at the start
// of a statement attempt. Sequence functions update SeqAddValues and
// SeqLastValue while the statement runs, but those updates are only published
// to the frontend session after the statement succeeds. A retry reuses the
// same Process, so the failed attempt must not leave its values behind.
type sequenceStatementState struct {
	curValues  map[uint64]string
	addValues  map[uint64]string
	deleteKeys []uint64
	lastValue  string
}

func captureSequenceStatementState(proc *process.Process) sequenceStatementState {
	if proc == nil {
		return sequenceStatementState{}
	}
	info := proc.GetSessionInfo()
	state := sequenceStatementState{
		curValues:  maps.Clone(info.SeqCurValues),
		addValues:  maps.Clone(info.SeqAddValues),
		deleteKeys: append([]uint64(nil), info.SeqDeleteKeys...),
	}
	if len(info.SeqLastValue) != 0 {
		state.lastValue = info.SeqLastValue[0]
	}
	return state
}

func (c *Compile) restoreSequenceStatementState() {
	if c == nil || c.proc == nil {
		return
	}
	c.sequenceState.apply(c.proc)
}

func (state sequenceStatementState) apply(proc *process.Process) {
	info := proc.GetSessionInfo()
	info.SeqCurValues = maps.Clone(state.curValues)
	info.SeqAddValues = maps.Clone(state.addValues)
	info.SeqDeleteKeys = append([]uint64(nil), state.deleteKeys...)
	if len(info.SeqLastValue) == 0 {
		info.SeqLastValue = make([]string, 1)
	}
	info.SeqLastValue[0] = state.lastValue
}

// arbitrateSequenceRetryAdmission is the authoritative second admission
// phase of tier-2 allocation retry (design
// docs/design/sequence_ownership_state_consistency.md §4 D, v13–v21). It
// runs at the retry quiescence boundary — after prepareRetryTransition's
// producer join, before the retry generation is rebuilt — and answers for
// the WHOLE consumed pending set of the current statement generation as a
// unit (B3): the frozen dispatch envelope facts {Autocommit=true,
// ByBegin=false}, RC isolation, generic canRetry acceptance, and the live
// terms (the synchronized publication flag, the exactly-once terminal
// stamp, the window D through the shared probe-first expiry decision)
// are re-verified once for the statement. Publication always wins: a
// statement whose first result batch already entered the client write path
// is never replayed. Both phases read ONLY that flag — the unsynchronized
// ResultRows accounting is never consulted (F1). On admission the retry
// window (min(statement, 10 s), evaluated at the first admission) is
// created — reused, never reset, while the episode is live (G2), opened
// fresh after an episode closure (Z1) — and the admission probe creates
// every newly contended watch entry carrying its committed-tuple baseline
// (A1/Z2); a failed sample records the entry young. A declined verdict
// surfaces the FIRST-recorded pending's named terminal error.
func (c *Compile) arbitrateSequenceRetryAdmission(retryState *process.SequenceRetryState, items []process.SequencePendingItem, err error) (bool, error) {
	if retryState.Published() {
		return false, nil
	}
	if _, terminal := retryState.TerminalIdentity(); terminal {
		return false, nil
	}
	op := c.proc.GetTxnOperator()
	if op == nil {
		return false, nil
	}
	opts := op.TxnOptions()
	if !opts.GetAutocommit() || opts.GetByBegin() {
		return false, nil
	}
	if !op.Txn().IsRCIsolation() {
		return false, nil
	}
	if !c.canRetry(err) {
		return false, nil
	}
	// The session statement deadline is a session fact; every conversion
	// observed it. The tightest observation caps the window and the probes.
	var stmtDeadline time.Time
	for _, item := range items {
		if !item.Pending.StmtDeadline.IsZero() && (stmtDeadline.IsZero() || item.Pending.StmtDeadline.Before(stmtDeadline)) {
			stmtDeadline = item.Pending.StmtDeadline
		}
	}
	if window, ok := retryState.RetryDeadline(); ok && !window.After(time.Now()) {
		// The window elapsed at the seam (site 1): the probe-first expiry
		// decision — never the wall clock alone — chooses between renewed
		// continuation, episode closure and the terminal.
		outcome := function.RunSequenceRetryExpiryDecision(c.proc.Ctx, c.proc, stmtDeadline)
		if outcome.Kind == process.DecideCancelled {
			// Caller's own cancellation outranks a pending's stale error:
			// the client receives its own cancellation (v21 precedence,
			// F2), never the old named terminal.
			return false, outcome.Err
		}
		if outcome.Kind == process.DecideExhausted {
			return false, nil
		}
	}
	candidate := time.Now().Add(function.SequenceAllocationRetryBudget())
	if !stmtDeadline.IsZero() && stmtDeadline.Before(candidate) {
		candidate = stmtDeadline
	}
	retryState.AdmitRetry(candidate)
	// The admission probe (A1/Z2): every newly contended watch entry is
	// created CARRYING its committed-tuple baseline; a failed sample
	// records the entry young and the next expiry decision retries its
	// initialization first inside the single probe round.
	for _, item := range items {
		if retryState.HasWatch(item.Identity) {
			continue
		}
		if tuple, err := function.SampleSequenceBaseline(c.proc.Ctx, c.proc, item.Identity, function.SequenceExpiryProbeBudget(), stmtDeadline); err == nil {
			retryState.RecordBaseline(item.Identity, tuple, time.Now())
		} else {
			retryState.EnsureWatching(item.Identity)
		}
	}
	return true, nil
}

// internalSequenceState is a synchronous CTAS handoff. The child owns its
// cloned attempt state; only a successful child supplies an output snapshot.
// The outer statement remains responsible for frontend publication and rollback.
//
// obligations carries the session-owned SequenceObligationSet itself (not a
// clone). The child's process comes from NewTopProcess with a fresh
// SessionInfo, so without this carry an allocation from a transaction-local
// sequence inside CTAS population would see a nil store: Raise cannot record
// the issued values, and the pre-commit stamp would never repair them
// (#30896; v12-review F5). The set is mutex-guarded and deliberately outside
// the statement snapshot/restore machinery (see SessionInfo.SeqObligations),
// so nothing here clones, restores, or clears it.
type internalSequenceState struct {
	input       sequenceStatementState
	output      *sequenceStatementState
	obligations *process.SequenceObligationSet
}

type internalSequenceStateKey struct{}

func attachInternalSequenceState(ctx context.Context, proc *process.Process) (context.Context, *internalSequenceState) {
	state := &internalSequenceState{
		input:       captureSequenceStatementState(proc),
		obligations: proc.GetSequenceObligationSet(),
	}
	return context.WithValue(ctx, internalSequenceStateKey{}, state), state
}

func consumeInternalSequenceState(ctx context.Context) (context.Context, *internalSequenceState) {
	state, _ := ctx.Value(internalSequenceStateKey{}).(*internalSequenceState)
	if state == nil {
		return ctx, nil
	}
	// Nested metadata SQL inherits this context. It must not consume or publish
	// the population statement's handoff while a sequence allocation is active.
	return context.WithValue(ctx, internalSequenceStateKey{}, (*internalSequenceState)(nil)), state
}

func initInternalSequenceState(proc *process.Process, database string, state *internalSequenceState) {
	proc.GetSessionInfo().Database = database
	proc.InitSeq()
	if state != nil {
		state.input.apply(proc)
		// A CTAS child allocating from a transaction-local sequence shares the
		// session's obligation store, so Raise records the issued values and
		// the outer transaction's pre-commit stamp can repair them (#30896;
		// v12-review F5). A nil handoff (nested metadata SQL) keeps the fresh
		// process's nil store: its allocations still fail loudly there.
		if state.obligations != nil {
			proc.GetSessionInfo().SeqObligations = state.obligations
		}
		// A caller without prior sequence state still needs writable containers.
		info := proc.GetSessionInfo()
		if info.SeqCurValues == nil {
			info.SeqCurValues = make(map[uint64]string)
		}
		if info.SeqAddValues == nil {
			info.SeqAddValues = make(map[uint64]string)
		}
	}
}

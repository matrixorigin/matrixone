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

package function

import (
	"context"
	"errors"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// Test hooks for the expiry-probe oracles: production keeps the calibrated
// constants (design §4 D, B2/A3) — one bounded min(statement, 1 s) probe
// round per decision, the ordinary bounded lock wait inside it — while the
// deterministic fixtures compress them. Production code never reassigns.
var (
	// seqProbeBudgetDur bounds ONE expiry decision's whole probe round
	// (tuple reads plus lock checks), capped by the statement deadline.
	seqProbeBudgetDur = time.Second
	// seqProbeLockWaitDur bounds a single lock-acquisition check inside the
	// round; the round's own context is the tighter bound under contention.
	seqProbeLockWaitDur = seqAutonomousLockWait
)

// SequenceExpiryProbeBudget exposes the calibrated per-decision probe-round
// budget so the compiler's arbitration seam can build the same bound.
func SequenceExpiryProbeBudget() time.Duration {
	return seqProbeBudgetDur
}

// errSeqProbeAcquired is the prober's acquire-and-release signal: the lock
// check's fn returns it AFTER reading the row FOR UPDATE, so the internal
// executor rolls the probe transaction back (releasing the row and
// consuming no values) while the wrapped error still reports the
// acquisition. errors.Is over the executor's error join recovers it.
var errSeqProbeAcquired = moerr.NewInternalErrorNoCtx("sequence probe: row acquired and released")

// seqProbeCtxMarkKey tags the probe transactions' contexts so test fakes of
// the internal executor can tell decision/probe transactions apart from the
// allocation attempts they interleave with. Production code never reads the
// marker.
type seqProbeCtxMarkKey struct{}

func markProbeCtx(ctx context.Context) context.Context {
	return context.WithValue(ctx, seqProbeCtxMarkKey{}, true)
}

// IsSequenceProbeCtx reports whether the context carries the probe marker:
// the test-detection handle for fakes of the internal SQL executor outside
// this package, needed because those fakes must keep decision/probe
// transactions strictly apart from the allocation attempts they interleave
// with. Production code never calls it.
func IsSequenceProbeCtx(ctx context.Context) bool {
	return ctx.Value(seqProbeCtxMarkKey{}) != nil
}

// seqProber is the production process.SeqProber over the statement's own
// internal executor and engine: both observations run inside fresh
// autonomous transactions derived from the decision's bounded probe
// context, so they read committed state only, never wait on the caller's
// transaction, and are bounded by the round's budget.
type seqProber struct {
	proc *process.Process
	eng  engine.Engine
}

// newSeqProber binds the probe machinery of one expiry decision. A missing
// engine or executor is a construction failure: the caller treats it as
// default-deny (no decision can run, so the exhaustion stays terminal).
func newSeqProber(proc *process.Process) (*seqProber, error) {
	if proc.GetTxnOperator() == nil {
		return nil, moerr.NewInternalError(proc.Ctx, "sequence: txn operator is nil")
	}
	eng, ok := proc.Ctx.Value(defines.EngineKey{}).(engine.Engine)
	if !ok || eng == nil {
		return nil, moerr.NewInternalError(proc.Ctx, "sequence: engine is unavailable for the expiry probe")
	}
	v, ok := runtime.ServiceRuntime(proc.GetService()).GetGlobalVariables(runtime.InternalSQLExecutor)
	if !ok {
		return nil, moerr.NewInternalError(proc.Ctx, "sequence: internal SQL executor is unavailable")
	}
	if _, ok = v.(executor.SQLExecutor); !ok {
		return nil, moerr.NewInternalError(proc.Ctx, "sequence: internal SQL executor has unexpected type")
	}
	return &seqProber{proc: proc, eng: eng}, nil
}

// ProbeCommittedTuple reads the B1 marker — the complete committed tuple
// (relation id, is_called, last_seq_num) — through an autonomous RC
// snapshot read that never waits on the sequence row lock. The resolve
// happens through the same engine path a routing probe uses: an identity
// that no longer resolves fails with process.ErrSeqProbeIdentityGone (the
// decision retires it, A1); every other failure keeps the entry
// unavailable. A committed generation replacement of the same name resolves
// to the fresh tuple; its relation id difference is the change evidence the
// B1 re-baseline rule consumes.
func (p *seqProber) ProbeCommittedTuple(ctx context.Context, db, tbl string) (process.SeqTuple, error) {
	env, err := seqExecutorEnv(ctx, p.proc, db)
	if err != nil {
		return process.SeqTuple{}, err
	}
	gone := func(err error) error {
		return process.SeqProbeIdentityGoneError(db, tbl)
	}
	var tuple process.SeqTuple
	fn := func(te executor.TxnExecutor) error {
		dbHandler, err := p.eng.Database(env.ctx, db, te.Txn())
		if err != nil {
			if isSeqNotVisibleErr(err) {
				return gone(err)
			}
			return err
		}
		rel, err := dbHandler.Relation(env.ctx, tbl, nil)
		if err != nil {
			if isSeqNotVisibleErr(err) {
				return gone(err)
			}
			return err
		}
		tuple.RelID = rel.GetTableID(env.ctx)
		res, err := te.Exec("select last_seq_num, min_value, max_value, start_value, increment_value, cycle, is_called from "+
			sequenceTableName(db, tbl), env.stmt)
		if err != nil {
			return err
		}
		values, err := readSeqRow(env.ctx, res)
		res.Close()
		if err != nil {
			return err
		}
		if values == nil {
			return moerr.NewInternalError(p.proc.Ctx, "Failed to get sequence meta data.")
		}
		r, err := seqRowFromValues(env.ctx, values)
		if err != nil {
			return err
		}
		tuple.LastSeq, tuple.IsCalled = r.last, r.isCalled
		return nil
	}
	if err := env.exec.ExecTxn(markProbeCtx(ctx), fn, env.opts.WithLockWaitTimeout(seqProbeLockWaitDur)); err != nil {
		return process.SeqTuple{}, err
	}
	return tuple, nil
}

// TryAcquireLock runs the A3 availability check of one expiry decision: a
// fresh autonomous transaction acquires the sequence row FOR UPDATE and
// immediately rolls back, releasing the row and consuming no values. A
// clean acquisition reports (true, nil) — the committed-unchanged row is
// acquirable again, so the recorded contention resolved. A lock-wait
// timeout/conflict, a timed-out or failed transaction, or any other error
// reports the row as still held.
func (p *seqProber) TryAcquireLock(ctx context.Context, db, tbl string) (bool, error) {
	env, err := seqExecutorEnv(ctx, p.proc, db)
	if err != nil {
		return false, err
	}
	fn := func(te executor.TxnExecutor) error {
		res, err := te.Exec("select last_seq_num, min_value, max_value, start_value, increment_value, cycle, is_called from "+
			sequenceTableName(db, tbl)+" for update", env.stmt)
		if err != nil {
			return err
		}
		res.Close()
		return errSeqProbeAcquired
	}
	err = env.exec.ExecTxn(markProbeCtx(ctx), fn, env.opts.WithLockWaitTimeout(seqProbeLockWaitDur))
	if errors.Is(err, errSeqProbeAcquired) {
		return true, nil
	}
	return false, err
}

// SampleSequenceBaseline takes ONE bounded committed-tuple sample of a
// contended sequence identity — the conversion path's creation-time
// baseline and the arbitration's first-admission probe (design §4 D,
// A1/Z2). The sample is a single lock-free RC read under a derived context
// bounded by min(statement, probeBudget); a failure leaves the caller to
// record the entry YOUNG.
func SampleSequenceBaseline(ctx context.Context, proc *process.Process, id process.WatchIdentity, probeBudget time.Duration, sessionDeadline time.Time) (process.SeqTuple, error) {
	budget := probeBudget
	if !sessionDeadline.IsZero() {
		if remaining := time.Until(sessionDeadline); remaining < budget {
			budget = remaining
		}
	}
	if budget <= 0 {
		budget = time.Millisecond
	}
	probeCtx, cancel := context.WithTimeout(ctx, budget)
	defer cancel()
	prober, err := newSeqProber(proc)
	if err != nil {
		return process.SeqTuple{}, err
	}
	return prober.ProbeCommittedTuple(probeCtx, id.DBName, id.TblName)
}

// RunSequenceRetryExpiryDecision executes the shared probe-first expiry
// decision (design §4 D, B2) for the statement's retry state from any of
// its four call sites — the arbitration seam, the zero-attempt entry, the
// conversion pre-filter, and the in-flight budget-expiry route (A2). The
// calibrated interval is the allocation budget (the same constant the
// window derives from), the round budget is min(statement, 1 s), and the
// caller's context is the cancellation source of step (1). A nil state or a
// state without a live window decides Proceed; a prober that cannot be
// constructed fails the decision safe to the terminal class (the
// default-deny envelope: an admission site can never observe, so it must
// not renew).
func RunSequenceRetryExpiryDecision(ctx context.Context, proc *process.Process, sessionDeadline time.Time) process.DecideOutcome {
	retryState := proc.GetSequenceRetryState()
	if retryState == nil {
		return process.DecideOutcome{Kind: process.DecideProceed}
	}
	prober, err := newSeqProber(proc)
	if err != nil {
		return process.DecideOutcome{Kind: process.DecideExhausted, Terminal: err}
	}
	return retryState.DecideExpiry(ctx, prober, seqProbeBudgetDur, seqAutonomousBudgetDur, sessionDeadline)
}

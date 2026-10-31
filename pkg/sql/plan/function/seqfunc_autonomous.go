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
	"fmt"
	"math"
	"math/rand"
	"strconv"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"golang.org/x/exp/constraints"
)

// Sequence allocation routing.
//
// Every nextval/setval first resolves the relation inside the caller's
// transaction (unchanged caller-visible errors), then picks a route by
// committed visibility:
//
//   - Committed generations allocate through a fresh autonomous transaction
//     (ExecTxn without WithTxn). The counter read-modify-write commits with
//     that transaction, so rolling back the caller's transaction can never
//     reclaim an issued value (#30896), and concurrent sessions serialize on
//     the committed row.
//   - Generations visible only inside the caller's transaction (created by
//     an explicit-transaction CLONE or snapshot restore on this base) keep
//     allocating in the caller transaction, tracked by a pass-scoped
//     SequenceObligation so statement-level rewinds cannot re-issue a value
//     and the pre-commit stamp can persist a safe position.
//
// Routing is probed inside the autonomous attempt with the attempt's own
// transaction: a generation absent from the committed view is
// transaction-local and falls through to the local route.
var errSeqLocalGeneration = moerr.NewInternalErrorNoCtx("sequence: generation is transaction-local")

const (
	// 8 attempts drained the observed worst case (2-CN, 16-client control);
	// each backoff step stays in the documented 25/50/100 ms window.
	seqAutonomousMaxAttempts = 8
	seqAutonomousBudget      = 10 * time.Second
	seqAutonomousLockWait    = 5 * time.Second
	seqAutonomousBackoff     = 25 * time.Millisecond
)

// Test hooks for the deadline/backoff oracles: the vars mirror the calibrated
// production schedule (8 attempts, deadline 10 s, 25/50/100 ms capped backoff)
// so tests can compress time deterministically; production code never
// reassigns them.
var (
	seqAutonomousBudgetDur      = seqAutonomousBudget
	seqAutonomousBackoffStepDur = func(attempt int) time.Duration {
		step := seqAutonomousBackoff << attempt
		if step > 4*seqAutonomousBackoff {
			step = 4 * seqAutonomousBackoff
		}
		return step
	}
	// seqAutonomousNow reads the wall clock for the attempt loop's
	// live-window comparison; tests pin it to script a window that reads
	// live at the entry check and has elapsed (in real time) by the loop
	// head — the expiry-before-first-attempt boundary — deterministically.
	// The expiry decision machinery keeps the real clock.
	seqAutonomousNow = time.Now
)

// SetSequenceAutonomousTimingForTesting compresses the tier-2 timing
// schedule — the allocation budget/renewal interval, the expiry-probe
// round budget, and the backoff step — so the deterministic Compile.Run
// oracles (design §4 D, §9 retry-boundary list) can observe elapsed
// windows, derivation and probe decisions in milliseconds. It returns the
// restore function; production code never calls it.
func SetSequenceAutonomousTimingForTesting(budget, probeBudget, backoff time.Duration) (restore func()) {
	prevBudget, prevProbe, prevStep := seqAutonomousBudgetDur, seqProbeBudgetDur, seqAutonomousBackoffStepDur
	seqAutonomousBudgetDur = budget
	seqProbeBudgetDur = probeBudget
	seqAutonomousBackoffStepDur = func(int) time.Duration { return backoff }
	return func() {
		seqAutonomousBudgetDur = prevBudget
		seqProbeBudgetDur = prevProbe
		seqAutonomousBackoffStepDur = prevStep
	}
}

// seqDerivedBudgetErr tags an attempt error with its cancellation
// provenance (design §4 D, A2 v20): the attempt was cancelled by the firing
// of its D-derived context while the caller's context stayed alive. The
// tagged class bypasses the transient classifier for the conversion gate
// ONLY — it routes into the shared probe-first expiry decision, which
// renews on observable change or resolution, or terminals exactly once.
// Caller-context cancellation is never tagged and keeps its existing
// precedence.
type seqDerivedBudgetErr struct{ err error }

func (e *seqDerivedBudgetErr) Error() string { return e.err.Error() }
func (e *seqDerivedBudgetErr) Unwrap() error { return e.err }

// isSeqDerivedBudgetErr reports whether err carries the A2 tag anywhere in
// its chain (the internal executor returns attempt failures as error joins,
// whose dynamic types defeat direct assertions).
func isSeqDerivedBudgetErr(err error) bool {
	var tagged *seqDerivedBudgetErr
	return errors.As(err, &tagged)
}

// seqHasMoErrCode reports whether err's chain carries the moerr code. The
// internal executor returns attempt failures as
// errors.Join(attemptErr, rollbackErr) (compile/sql_executor.go
// txnExecutor.rollback), whose dynamic type defeats moerr.IsMoErrCode's
// direct assertion, so classification must traverse wrappers.
func seqHasMoErrCode(err error, code uint16) bool {
	var me *moerr.Error
	if !errors.As(err, &me) {
		return false
	}
	return me.ErrorCode() == code
}

// isSeqNotVisibleErr classifies the probe miss that marks a transaction-local
// generation: the relation (or its database) does not exist in the
// attempting transaction's committed view. disttae reports the missing
// workspace/catalog entries as OkExpectedEOB and a missing relation as
// ErrNoSuchTable.
func isSeqNotVisibleErr(err error) bool {
	return seqHasMoErrCode(err, moerr.OkExpectedEOB) ||
		seqHasMoErrCode(err, moerr.ErrNoSuchTable)
}

// retryableSeqTxnErr lists the transient commit/lock conflicts of the
// autonomous attempt. Everything else (range exhaustion, catalog errors,
// guard failures) is deterministic and fails without consuming the budget.
func retryableSeqTxnErr(err error) bool {
	return seqHasMoErrCode(err, moerr.ErrLockWaitTimeout) ||
		seqHasMoErrCode(err, moerr.ErrDeadLockDetected) ||
		seqHasMoErrCode(err, moerr.ErrTxnNeedRetry) ||
		seqHasMoErrCode(err, moerr.ErrTxnNeedRetryWithDefChanged) ||
		seqHasMoErrCode(err, moerr.ErrLockTableBindChanged)
}

// seqExecEnv caches the internal-executor plumbing shared by the autonomous
// route and the in-caller sequenceSQL helper. The context carries BgKey so
// the planner permits the internal write of a sequence relation; opts carry
// no transaction so ExecTxn owns (and commits) its own transaction.
type seqExecEnv struct {
	exec executor.SQLExecutor
	ctx  context.Context
	opts executor.Options
	stmt executor.StatementOption
	// tier2Eligible marks the allocation route: only sequence ALLOCATION
	// (nextval/setval) may convert a tier-1 exhaustion to a statement retry
	// signal (design §4 D, v13+). The merge reconcile unit shares this
	// execution contract but is not allocation work, so it clears the flag
	// and keeps the terminal named error.
	tier2Eligible bool
}

func seqExecutorEnv(ctx context.Context, proc *process.Process, db string) (*seqExecEnv, error) {
	if proc.GetTxnOperator() == nil {
		return nil, moerr.NewInternalError(ctx, "sequence: txn operator is nil")
	}
	accountID, err := defines.GetAccountId(ctx)
	if err != nil {
		return nil, err
	}
	v, ok := runtime.ServiceRuntime(proc.GetService()).GetGlobalVariables(runtime.InternalSQLExecutor)
	if !ok {
		return nil, moerr.NewInternalError(proc.Ctx, "sequence: internal SQL executor is unavailable")
	}
	sqlExecutor, ok := v.(executor.SQLExecutor)
	if !ok {
		return nil, moerr.NewInternalError(proc.Ctx, "sequence: internal SQL executor has unexpected type")
	}
	lowerCaseTableNames := int64(1)
	if resolve := proc.GetResolveVariableFunc(); resolve != nil {
		if value, resolveErr := resolve("lower_case_table_names", true, false); resolveErr == nil {
			if resolved, ok := value.(int64); ok {
				lowerCaseTableNames = resolved
			}
		}
	}
	// Sequence metadata is a catalog-owned mutable row; the BgKey marker is
	// the internal-write contract that lets the planner accept the UPDATE.
	// The caller picks the base context: allocation paths use proc.Ctx (alive
	// for the whole expression evaluation), while the merge reconcile passes
	// its statement context so a session-process context that statement
	// machinery may reseat cannot cancel the committed unit (issue #30359).
	return &seqExecEnv{
		exec: sqlExecutor,
		ctx:  context.WithValue(ctx, defines.BgKey{}, true),
		opts: executor.Options{}.
			WithDisableIncrStatement().
			WithAccountID(accountID).
			WithDatabase(db).
			WithLowerCaseTableNames(&lowerCaseTableNames).
			WithTimeZone(proc.GetSessionInfo().TimeZone).
			WithResolveVariableFunc(proc.GetResolveVariableFunc()).
			WithFrontend(proc.Base.IsFrontend),
		stmt:          executor.StatementOption{}.WithAccountID(accountID),
		tier2Eligible: true,
	}, nil
}

// SequenceAllocationRetryBudget reports the wrapper's calibrated
// attempt-loop budget (min(statement, 10 s) before the statement cap). The
// compiler's tier-2 arbitration derives the admitted retry window from the
// same calibration, including any test compression of seqAutonomousBudgetDur.
func SequenceAllocationRetryBudget() time.Duration {
	return seqAutonomousBudgetDur
}

// execSeqAutonomous runs fn inside a fresh committed transaction with bounded
// retries. The budget, per-attempt lock wait and backoff keep a contended
// allocation from hanging the caller's statement; exhaustion is reported
// with the sequence name. fn's sentinel error aborts retries immediately for
// local-route fallback.
//
// The budget deadline min(statement, 10 s) is propagated INTO execution:
// every attempt, the backoff and the lock wait run on a derived context with
// that deadline, so a blocked attempt observes the budget in flight (its
// aborted transaction is released by ExecTxn's defer) instead of overrunning
// it and being checked only between attempts. When the budget fires, the
// caller still sees the documented named exhaustion error; when the caller's
// own statement context fires, its cancellation is returned unchanged.
// seqLiveWindowExpiryDecision is the shared zero-attempt expiry arbitration
// (design §4 D B2/Z1, v13–v21): an observed expiry of the tier-2 live
// window BEFORE any attempt of this execution ran routes through the same
// probe-first expiry decision as the allocation-entry and conversion
// surfaces — never the raw derived-context error, which the compiler's
// retry classifier does not accept (compile.go isRetryErr names only the
// txn-retry classes). Caller cancellation always wins unchanged. It applies
// at the allocation-entry check and at the attempt loop's first context
// check, closing the expiry-before-first-attempt boundary: a window that
// reads live at entry but has elapsed by the loop head takes the identical
// decision path. decided=false reports that arbitration does not apply (the
// route is not tier-2, the statement has no retry state / stamped terminal,
// or no expired live window) and the caller keeps its unchanged surface.
// On renewal/closure the returned deadline funds the continuation: the live
// (renewed) window, or a fresh base per-invocation budget once the episode
// closed (Z1).
func seqLiveWindowExpiryDecision(proc *process.Process, env *seqExecEnv, db, tblname string, wantRel uint64) (time.Time, bool, error) {
	if !env.tier2Eligible {
		return time.Time{}, false, nil
	}
	retryState := proc.GetSequenceRetryState()
	if retryState == nil {
		return time.Time{}, false, nil
	}
	if _, terminal := retryState.TerminalIdentity(); terminal {
		// The exactly-once terminal verdict is stamped for this statement:
		// every later allocation execution surfaces it and never re-admits
		// (B2 step 5).
		if err := env.ctx.Err(); err != nil {
			return time.Time{}, true, err
		}
		return time.Time{}, true, seqAllocationExhaustedError(proc, db, tblname, retryState.TerminalCause())
	}
	window, ok := retryState.RetryDeadline()
	if !ok || window.After(seqAutonomousNow()) {
		return time.Time{}, false, nil
	}
	if err := env.ctx.Err(); err != nil {
		return time.Time{}, true, err
	}
	stmtDeadline, _ := env.ctx.Deadline()
	outcome := RunSequenceRetryExpiryDecision(env.ctx, proc, stmtDeadline)
	switch outcome.Kind {
	case process.DecideCancelled:
		if outcome.Err != nil {
			return time.Time{}, true, outcome.Err
		}
		return time.Time{}, true, env.ctx.Err()
	case process.DecideExhausted:
		return time.Time{}, true, seqTerminalExhaustedError(proc, retryState, outcome, db, tblname, wantRel)
	}
	// Renew / Retire / Proceed: a live (renewed) window funds the
	// continuation; a retired window (the episode closed, Z1) returns the
	// execution to a fresh base per-invocation budget.
	if window, ok := retryState.RetryDeadline(); ok && window.After(time.Now()) {
		return window, true, nil
	}
	deadline := time.Now().Add(seqAutonomousBudgetDur)
	if !stmtDeadline.IsZero() && stmtDeadline.Before(deadline) {
		deadline = stmtDeadline
	}
	return deadline, true, nil
}

func execSeqAutonomous(proc *process.Process, env *seqExecEnv, db, tblname string, wantRel uint64, fn func(context.Context, executor.TxnExecutor) error) error {
	deadline := time.Now().Add(seqAutonomousBudgetDur)
	if statementDeadline, ok := env.ctx.Deadline(); ok && statementDeadline.Before(deadline) {
		deadline = statementDeadline
	}
	// Tier-2 progress-renewed window D (design §4 D, v13–v21): while an
	// admission episode is LIVE (the watch set retains unresolved
	// identities), every allocation execution of the statement — in any
	// retry generation — derives its attempt context from D (remaining =
	// D − now). An execution entered at or after the live D opens no new
	// attempt loop on the wall clock alone: the probe-first expiry decision
	// (B2) chooses between renewed continuation, episode closure (Z1) and
	// the exactly-once terminal. Caller cancellation always takes
	// precedence over any of these surfaces.
	if d, decided, err := seqLiveWindowExpiryDecision(proc, env, db, tblname, wantRel); decided {
		if err != nil {
			return err
		}
		deadline = d
	} else if env.tier2Eligible {
		if retryState := proc.GetSequenceRetryState(); retryState != nil {
			if window, ok := retryState.RetryDeadline(); ok {
				// The window is live and unexpired: derive the attempt
				// budget from it.
				deadline = window
			}
		}
	}
	var lastErr error
	// Each iteration binds its own attempt context and cancels exactly that
	// binding on every path out of the iteration: the tier-2 renewal cannot
	// rebind a scope-spanning cancel (the deferred call would capture the
	// first binding and leak the renewed context's timer).
	backoffFired := false
	for attempt := 0; attempt < seqAutonomousMaxAttempts && !backoffFired; {
		boundedCtx, cancel := context.WithDeadline(env.ctx, deadline)
		if err := boundedCtx.Err(); err != nil {
			cancel()
			// Caller cancellation propagates unchanged.
			if env.ctx.Err() != nil {
				return err
			}
			if lastErr == nil {
				// The derived deadline fired BEFORE any attempt ran. The
				// only derived deadline that can fire this early is the
				// tier-2 live window (the base budget is a per-invocation
				// min(statement, 10 s) — a fired statement deadline also
				// fires env.ctx above, and the base budget cannot be spent
				// before the first attempt): arbitrate the expiry with the
				// shared probe-first decision instead of surfacing a derived
				// context error the compiler's retry classifier rejects.
				next, decided, decErr := seqLiveWindowExpiryDecision(proc, env, db, tblname, wantRel)
				if !decided {
					// Non-tier-2 routes and pre-admission states keep their
					// unchanged context-error surface.
					return err
				}
				if decErr != nil {
					return decErr
				}
				// Renewed continuation or episode closure: no attempt was
				// consumed, so the loop restarts under the derived budget.
				deadline = next
				continue
			}
			break
		}
		boundedEnv := &seqExecEnv{exec: env.exec, ctx: boundedCtx, opts: env.opts, stmt: env.stmt}
		err := boundedEnv.exec.ExecTxn(boundedCtx, func(te executor.TxnExecutor) error {
			return fn(boundedCtx, te)
		}, boundedEnv.opts.WithLockWaitTimeout(seqAutonomousLockWait))
		if err == nil || errors.Is(err, errSeqLocalGeneration) {
			cancel()
			return err
		}
		if boundedCtx.Err() != nil && env.ctx.Err() == nil {
			// The budget fired while this attempt was in flight: tag the
			// attempt error with its D-derived cancellation provenance (A2).
			// The plain context error would fail the transient classifier
			// below; the tagged class routes into the shared probe-first
			// expiry decision at the conversion gate — renews on observable
			// progress or resolution, terminals exactly once otherwise. A
			// pre-admission firing keeps its unchanged named surface.
			lastErr = &seqDerivedBudgetErr{err: err}
			cancel()
			break
		}
		if !retryableSeqTxnErr(err) {
			cancel()
			return err
		}
		lastErr = err
		attempt++
		backoff := seqAutonomousBackoffStepDur(attempt) + time.Duration(rand.Int63n(int64(seqAutonomousBackoff)))
		timer := time.NewTimer(backoff)
		select {
		case <-boundedCtx.Done():
			timer.Stop()
			if env.ctx.Err() != nil {
				cancel()
				return boundedCtx.Err()
			}
			// The budget fired during the backoff: fall through to the named
			// exhaustion error without starting another attempt.
			backoffFired = true
		case <-timer.C:
		}
		cancel()
	}
	// Exhaustion of the tier-1 wrapper (approved design §4 D): the bounded
	// retry machine is wrapper-owned — 8 attempts, deadline min(statement,
	// 10 s), capped 25/50/100 ms backoff + jitter — and its default
	// exhaustion surface is the named lock-wait/conflict error, for conflict
	// residues and bounded-wait residues alike. Every review-approved bound
	// must be enforceable as written (§10 rejects unbounded retry: "caller
	// statements must stay deadline-bounded"), so a conflict residue may
	// convert to a statement-level retry signal ONLY through the tier-2
	// two-phase admission (pre-filter here, authoritative arbitration at the
	// compiler's retry quiescence boundary) and only while the shared
	// probe-first expiry decision renews; the retried executions then
	// derive their budgets from the renewed D while the episode stays live,
	// or from a fresh per-invocation budget once it closes (Z1).
	// Per-attempt transient conflicts still retry inside the wrapper, so a
	// contended allocation that can make progress within the reviewed budget
	// does.
	namedErr := seqAllocationExhaustedError(proc, db, tblname, seqUnwrapDerived(lastErr))
	if env.tier2Eligible && (retryableSeqTxnErr(lastErr) || isSeqDerivedBudgetErr(lastErr)) {
		if signal := seqConvertExhaustionToRetry(proc, env, db, tblname, wantRel, namedErr, lastErr); signal != nil {
			return signal
		}
	}
	return namedErr
}

// seqUnwrapDerived strips the A2 provenance tag from an attempt error: the
// composed named surface and the recorded conflict residue always name the
// underlying error, never the tag.
func seqUnwrapDerived(err error) error {
	var tagged *seqDerivedBudgetErr
	if errors.As(err, &tagged) {
		return tagged.err
	}
	return err
}

// seqTerminalExhaustedError composes the exactly-once terminal surface of a
// DecideExhausted outcome: the surfaced identity in first-recorded order,
// its recorded conflict residue (or the decision's terminal error).
func seqTerminalExhaustedError(proc *process.Process, retryState *process.SequenceRetryState, outcome process.DecideOutcome, db, tblname string, wantRel uint64) error {
	target := outcome.Target
	if target.WantRel == 0 {
		target = process.WatchIdentity{DBName: db, TblName: tblname, WantRel: wantRel}
	}
	cause := retryState.TerminalCause()
	if cause == nil {
		cause = outcome.Terminal
	}
	return seqAllocationExhaustedError(proc, target.DBName, target.TblName, cause)
}

// seqAllocationExhaustedError composes the named terminal exhaustion error:
// the surface every allocation failure outside tier-2 admission keeps.
func seqAllocationExhaustedError(proc *process.Process, db, tblname string, cause error) error {
	if cause == nil {
		cause = moerr.NewInternalErrorNoCtx("sequence retry window expired")
	}
	return moerr.NewInternalErrorf(
		proc.Ctx,
		"sequence %s.%s: allocation could not acquire the sequence row within %d attempts: %v",
		db, tblname, seqAutonomousMaxAttempts, cause)
}

// seqConvertExhaustionToRetry is the conversion-site PRE-FILTER of tier-2
// admission (design §4 D, v13–v21); nil means the exhaustion stays terminal.
// Only the allocation route (env.tier2Eligible) reaches it, and only for the
// transient-conflict class or the A2-tagged derived-budget expiry (checked
// by the caller). Terms:
//
//  1. envelope — the statement's transaction was dispatched
//     {Autocommit=true, ByBegin=false}; the facts are read from the frozen
//     dispatch-time operator options (frontend txn.WithBeginAutoCommit), and
//     a missing operator or an absent state (background/internal executor)
//     is default-deny;
//  2. isolation — RC only (tier-1 conflicts are RC-mode; SI sessions keep
//     their existing conflict policy);
//  3. publication — the statement's synchronized publication flag is unset
//     (no result batch entered the client write path); both admission phases
//     read ONLY this flag (F1);
//  4. terminal — the exactly-once verdict is unstamped (B2 step 5);
//  5. budget — while an admission episode is live, a conversion at or after
//     D runs the shared probe-first expiry decision (site 3): renewal and
//     episode closure admit, the terminal verdict keeps the named surface.
//
// A derived-budget expiry (A2) with NO live episode keeps its unchanged
// named surface — there is nothing to renew. Generic acceptance
// (disableRetry / IncrStatementDisabled) is left to the authoritative
// arbitration at the retry quiescence boundary, which re-verifies every
// term. On conversion the pending payload — original named error, observed
// statement deadline, unwrapped conflict residue — is recorded per identity
// (B3) and the compiler's RC retry signal class is returned. New contended
// identities take their creation-time baseline through the conversion
// path's own bounded committed-tuple sample BEFORE the watch entry is
// recorded (Z2); a failed sample records the entry YOUNG.
func seqConvertExhaustionToRetry(proc *process.Process, env *seqExecEnv, db, tblname string, wantRel uint64, namedErr, cause error) error {
	retryState := proc.GetSequenceRetryState()
	if retryState == nil || retryState.Published() {
		return nil
	}
	if _, terminal := retryState.TerminalIdentity(); terminal {
		return nil
	}
	op := proc.GetTxnOperator()
	if op == nil {
		return nil
	}
	opts := op.TxnOptions()
	if !opts.GetAutocommit() || opts.GetByBegin() {
		return nil
	}
	if !op.Txn().IsRCIsolation() {
		return nil
	}
	budgetExpired := isSeqDerivedBudgetErr(cause)
	residue := seqUnwrapDerived(cause)
	id := process.WatchIdentity{DBName: db, TblName: tblname, WantRel: wantRel}
	stmtDeadline, _ := env.ctx.Deadline()
	recordAndConvert := func() error {
		retryState.SetPending(id, &process.SequenceRetryPending{
			Original:     namedErr,
			Cause:        residue,
			StmtDeadline: stmtDeadline,
		})
		return moerr.NewTxnNeedRetry(proc.Ctx)
	}
	// createWatchWithBaseline records a NEW watch identity with its
	// creation-time baseline sample taken BEFORE the entry is recorded
	// (Z2); a failed or timed-out sample records the entry YOUNG, and its
	// initialization is retried first inside the next expiry decision's
	// single probe round.
	createWatchWithBaseline := func() {
		if retryState.HasWatch(id) {
			return
		}
		if tuple, err := SampleSequenceBaseline(env.ctx, proc, id, seqProbeBudgetDur, stmtDeadline); err == nil {
			retryState.SetWatchingBaselined(id, tuple, time.Now())
		} else {
			retryState.EnsureWatching(id)
		}
	}
	window, live := retryState.RetryDeadline()
	switch {
	case !live:
		if budgetExpired {
			// A derived-budget expiry BEFORE any admission keeps its
			// unchanged named surface — there is no episode to renew (A2).
			return nil
		}
		// Fresh first-admission candidate: the arbitration seam's admission
		// probe creates the watch entry carrying its baseline sample (Z2);
		// the conversion records only the payload.
		return recordAndConvert()
	case window.After(time.Now()):
		// Live episode, window unexpired: convert with the creation-time
		// baseline rule (Z2). An A2 tag cannot land here — the tagged
		// budget only fires at or after its window.
		createWatchWithBaseline()
		return recordAndConvert()
	default:
		// Live window elapsed (this is also where every A2-tagged expiry
		// lands): run the shared probe-first decision (site 3) instead of
		// the wall-clock deny.
		if err := env.ctx.Err(); err != nil {
			return err
		}
		outcome := RunSequenceRetryExpiryDecision(env.ctx, proc, stmtDeadline)
		switch outcome.Kind {
		case process.DecideCancelled:
			if outcome.Err != nil {
				return outcome.Err
			}
			return env.ctx.Err()
		case process.DecideExhausted:
			return seqTerminalExhaustedError(proc, retryState, outcome, db, tblname, wantRel)
		default:
			// Renew (the episode continues under the renewed window) and
			// Retire (the episode closed; this conversion re-admits it
			// fresh) both admit: record the payload and convert.
			createWatchWithBaseline()
			return recordAndConvert()
		}
	}
}

// SequenceDomain identifies the sequence row's numeric storage type — and
// the representable value domain a merge watermark is classified against.
// Reconciliation counts only merged keys the sequence could itself issue:
// the sequence's own storage domain — read from its metadata row — decides
// which captured keys can ever collide with a future emission (#30359
// pre-PR review: width-aware domains, not bit-width folds).
type SequenceDomain uint8

const (
	SeqDomainInt16 SequenceDomain = iota
	SeqDomainInt32
	SeqDomainInt64
	SeqDomainUint16
	SeqDomainUint32
	SeqDomainUint64
)

// SequenceDomainCount is the number of storage domains above.
const SequenceDomainCount = 6

// seqDomainBounds carries the exact representable range of one storage
// domain: an int64 is representable iff smin <= i <= smax; a uint64 is
// representable iff u <= umax (signed domains admit only magnitudes their
// positive half can hold).
type seqDomainBounds struct {
	smin int64
	smax int64
	umax uint64
}

var seqDomainTable = [SequenceDomainCount]seqDomainBounds{
	SeqDomainInt16:  {math.MinInt16, math.MaxInt16, math.MaxInt16},
	SeqDomainInt32:  {math.MinInt32, math.MaxInt32, math.MaxInt32},
	SeqDomainInt64:  {math.MinInt64, math.MaxInt64, math.MaxInt64},
	SeqDomainUint16: {0, math.MaxUint16, math.MaxUint16},
	SeqDomainUint32: {0, math.MaxUint32, math.MaxUint32},
	SeqDomainUint64: {0, math.MaxInt64, math.MaxUint64},
}

// ContainsInt64 reports whether the signed value i is representable in the
// storage domain.
func (d SequenceDomain) ContainsInt64(i int64) bool {
	b := seqDomainTable[d]
	return i >= b.smin && i <= b.smax
}

// ContainsUint64 reports whether the unsigned magnitude u is representable
// in the storage domain.
func (d SequenceDomain) ContainsUint64(u uint64) bool {
	return u <= seqDomainTable[d].umax
}

// UnsignedDomain reports whether the storage domain is one of the
// unsigned widths.
func (d SequenceDomain) UnsignedDomain() bool {
	return d == SeqDomainUint16 || d == SeqDomainUint32 || d == SeqDomainUint64
}

// seqRowData is the parsed sequence metadata row. last/minv/maxv are stored
// as bits: signed domains are sign-extended into the uint64 slot so
// int64(bits) round-trips the value.
type seqRowData struct {
	domain   SequenceDomain
	last     uint64
	minv     uint64
	maxv     uint64
	incr     int64
	cycle    bool
	isCalled bool
}

func (r seqRowData) unsigned() bool {
	return r.domain.UnsignedDomain()
}

// format renders a value in the row's numeric domain exactly as the
// historical typed %d formatting did.
func (r seqRowData) format(bits uint64) string {
	if r.unsigned() {
		return strconv.FormatUint(bits, 10)
	}
	return strconv.FormatInt(int64(bits), 10)
}

// seqBits stores a typed value in the uniform uint64 slot, sign-extending the
// signed kinds.
func seqBits[T constraints.Integer](v T) uint64 {
	switch t := any(v).(type) {
	case int16:
		return uint64(int64(t))
	case int32:
		return uint64(int64(t))
	case int64:
		return uint64(t)
	case uint16:
		return uint64(t)
	case uint32:
		return uint64(t)
	case uint64:
		return t
	}
	return 0
}

// seqRowFromValues interprets the 7-column metadata row produced by
// readSeqRow exactly as the historical per-type switch did.
func seqRowFromValues(ctx context.Context, values []interface{}) (seqRowData, error) {
	row := seqRowData{
		incr:     values[4].(int64),
		cycle:    values[5].(bool),
		isCalled: values[6].(bool),
	}
	switch values[0].(type) {
	case int16:
		row.domain = SeqDomainInt16
		row.last = seqBits(values[0].(int16))
		row.minv = seqBits(values[1].(int16))
		row.maxv = seqBits(values[2].(int16))
	case int32:
		row.domain = SeqDomainInt32
		row.last = seqBits(values[0].(int32))
		row.minv = seqBits(values[1].(int32))
		row.maxv = seqBits(values[2].(int32))
	case int64:
		row.domain = SeqDomainInt64
		row.last = seqBits(values[0].(int64))
		row.minv = seqBits(values[1].(int64))
		row.maxv = seqBits(values[2].(int64))
	case uint16:
		row.domain = SeqDomainUint16
		row.last = seqBits(values[0].(uint16))
		row.minv = seqBits(values[1].(uint16))
		row.maxv = seqBits(values[2].(uint16))
	case uint32:
		row.domain = SeqDomainUint32
		row.last = seqBits(values[0].(uint32))
		row.minv = seqBits(values[1].(uint32))
		row.maxv = seqBits(values[2].(uint32))
	case uint64:
		row.domain = SeqDomainUint64
		row.last = seqBits(values[0].(uint64))
		row.minv = seqBits(values[1].(uint64))
		row.maxv = seqBits(values[2].(uint64))
	default:
		return seqRowData{}, moerr.NewInternalError(ctx, "Wrong types of sequence number or failed to read the sequence table")
	}
	return row, nil
}

// seqEmission is the computed outcome of one nextval against a metadata row.
type seqEmission struct {
	bits     uint64
	wrapped  bool // pass wrapped (or narrow-width edge arm): reseed the obligation pass
	uncalled bool // the row was uncalled: emit last and mark it called
}

// emitFromRow computes the next emission with the historical per-type
// semantics, including the narrow-width edge arms and the exact overflow
// edge comparisons of advanceSeq. No SQL runs here.
func emitFromRow(ctx context.Context, r seqRowData, tblname string) (seqEmission, error) {
	if !r.isCalled {
		return seqEmission{bits: r.last, uncalled: true}, nil
	}
	minus := r.incr < 0
	switch r.domain {
	case SeqDomainInt16:
		lsn, minv, maxv := int16(r.last), int16(r.minv), int16(r.maxv)
		if r.incr > math.MaxInt16 || r.incr < math.MinInt16 {
			if !r.cycle {
				return seqEmission{}, moerr.NewInternalErrorf(ctx, "Reached maximum value of sequence %s", tblname)
			}
			return emitEdge(minv, maxv, minus), nil
		}
		return emitAdvance(ctx, lsn, minv, maxv, makePosIncr[int16](r.incr), r.cycle, minus, tblname)
	case SeqDomainInt32:
		return emitAdvance(ctx, int32(r.last), int32(r.minv), int32(r.maxv), makePosIncr[int32](r.incr), r.cycle, minus, tblname)
	case SeqDomainInt64:
		return emitAdvance(ctx, int64(r.last), int64(r.minv), int64(r.maxv), makePosIncr[int64](r.incr), r.cycle, minus, tblname)
	case SeqDomainUint16:
		lsn, minv, maxv := uint16(r.last), uint16(r.minv), uint16(r.maxv)
		if r.incr > math.MaxUint16 || -r.incr > math.MaxUint16 {
			if !r.cycle {
				return seqEmission{}, moerr.NewInternalErrorf(ctx, "Reached maximum value of sequence %s", tblname)
			}
			return emitEdge(minv, maxv, minus), nil
		}
		return emitAdvance(ctx, lsn, minv, maxv, makePosIncr[uint16](r.incr), r.cycle, minus, tblname)
	case SeqDomainUint32:
		lsn, minv, maxv := uint32(r.last), uint32(r.minv), uint32(r.maxv)
		if r.incr > math.MaxUint32 || -r.incr > math.MaxUint32 {
			if !r.cycle {
				return seqEmission{}, moerr.NewInternalErrorf(ctx, "Reached maximum value of sequence %s", tblname)
			}
			return emitEdge(minv, maxv, minus), nil
		}
		return emitAdvance(ctx, lsn, minv, maxv, makePosIncr[uint32](r.incr), r.cycle, minus, tblname)
	case SeqDomainUint64:
		return emitAdvance(ctx, r.last, r.minv, r.maxv, makePosIncr[uint64](r.incr), r.cycle, minus, tblname)
	}
	return seqEmission{}, moerr.NewInternalError(ctx, "Wrong types of sequence number or failed to read the sequence table")
}

// emitEdge replicates the narrow-width setEdge arm: a permanent
// out-of-width increment emits the far edge on every call.
func emitEdge[T constraints.Integer](minv, maxv T, minus bool) seqEmission {
	if minus {
		return seqEmission{bits: seqBits(maxv), wrapped: true}
	}
	return seqEmission{bits: seqBits(minv), wrapped: true}
}

// emitAdvance replicates advanceSeq's overflow-aware edge comparisons; the
// typed arithmetic wraps exactly as the historical per-type code did.
func emitAdvance[T constraints.Integer](ctx context.Context, lsn, minv, maxv, incrv T,
	cycle, minus bool, tblname string) (seqEmission, error) {
	var adseq T
	if minus {
		adseq = lsn - incrv
	} else {
		adseq = lsn + incrv
	}

	// check descending sequence and reach edge
	if minus && (adseq < minv || adseq > lsn) {
		if !cycle {
			return seqEmission{}, moerr.NewInternalErrorf(ctx, "Reached maximum value of sequence %s", tblname)
		}
		return seqEmission{bits: seqBits(maxv), wrapped: true}, nil
	}

	// checkout ascending sequence and reach edge
	if !minus && (adseq > maxv || adseq < lsn) {
		if !cycle {
			return seqEmission{}, moerr.NewInternalErrorf(ctx, "Reached maximum value of sequence %s", tblname)
		}
		return seqEmission{bits: seqBits(minv), wrapped: true}, nil
	}

	return seqEmission{bits: seqBits(adseq)}, nil
}

// nextvalAutonomousBatch allocates n values from a committed generation
// inside ONE fresh committed transaction and returns them in row order. It
// returns errSeqLocalGeneration when the identity the CALLER resolved
// (wantDB/wantRel) is not visible in the committed view — a name probe alone
// is not enough: an uncommitted DROP+recreate shadows a still-committed
// same-named generation, and routing by name would allocate from the OLD
// committed generation (and self-wait on the caller's lifecycle locks)
// instead of the caller's transaction-local one. A committed relation whose
// id differs from the caller's under the SAME committed database is a
// genuinely stale identity: the call fails retryably (bounded) and never
// falls through to the in-caller route, where caller rollback could reclaim
// a committed value. Session publication happens only after the autonomous
// transaction committed.
//
// One committed unit for a whole statement's emissions is the sequential
// single-value protocol composed in memory: the row lock serializes the
// burst exactly as n serialized transactions would be, so the emitted values
// and the final stored (last_seq_num, is_called) are identical; only the
// commit granularity changes (design §6 bulk-insert gate). A burst that
// fails mid-computation (e.g. NO CYCLE exhaustion at row k) commits nothing,
// where the serialized equivalent would have burned rows < k — both are
// failure-atomic shapes I4 permits for a failed statement (gaps, never
// reuse), and nothing was published to the session in either shape.
func nextvalAutonomousBatch(tblname, db string, proc *process.Process, e engine.Engine, wantDB string, wantRel uint64, n int) ([]string, error) {
	env, err := seqExecutorEnv(proc.Ctx, proc, db)
	if err != nil {
		return nil, err
	}
	var (
		outs  []string
		relID uint64
	)
	fn := func(attemptCtx context.Context, te executor.TxnExecutor) error {
		dbHandler, err := e.Database(attemptCtx, db, te.Txn())
		if err != nil {
			if isSeqNotVisibleErr(err) {
				return errSeqLocalGeneration
			}
			return err
		}
		if dbHandler.GetDatabaseId(attemptCtx) != wantDB {
			// The committed database is a different generation than the one
			// the caller resolved: the caller's copy exists only inside its
			// own transaction. Route in-caller.
			return errSeqLocalGeneration
		}
		rel, err := dbHandler.Relation(attemptCtx, tblname, nil)
		if err != nil {
			if isSeqNotVisibleErr(err) {
				return errSeqLocalGeneration
			}
			return err
		}
		gotRel := rel.GetTableID(attemptCtx)
		if gotRel != wantRel {
			// Same committed database, different committed generation: the
			// identity changed underneath the caller's resolution. Bounded
			// retry replans against the fresh committed view; it must not be
			// routed in-caller.
			return moerr.NewLockTableBindChanged(attemptCtx)
		}
		if err := requireSequence(attemptCtx, rel); err != nil {
			return err
		}
		res, err := te.Exec("select last_seq_num, min_value, max_value, start_value, increment_value, cycle, is_called from "+
			sequenceTableName(db, tblname)+" for update", env.stmt)
		if err != nil {
			return err
		}
		values, err := readSeqRow(attemptCtx, res)
		res.Close()
		if err != nil {
			return err
		}
		if values == nil {
			return moerr.NewInternalError(proc.Ctx, "Failed to get sequence meta data.")
		}
		r0, err := seqRowFromValues(attemptCtx, values)
		if err != nil {
			return err
		}
		r := r0
		emissions := make([]seqEmission, 0, n)
		for k := 0; k < n; k++ {
			emission, err := emitFromRow(attemptCtx, r, tblname)
			if err != nil {
				return err
			}
			emissions = append(emissions, emission)
			if emission.uncalled {
				// The fresh-sequence arm emits the stored start and only
				// flips the flag; the stored last stands.
				r.isCalled = true
			} else {
				r.last, r.isCalled = emission.bits, true
			}
		}
		// Persist the composed final state with the historical single-row
		// SQL shape: flag-only when the stored last is unchanged on a fresh
		// row, the combined write when the burst both advanced and marked a
		// fresh row called, last-only otherwise.
		var sql string
		switch {
		case !r0.isCalled && r.last == r0.last:
			sql = fmt.Sprintf("update %s set is_called = true", sequenceTableName(db, tblname))
		case !r0.isCalled:
			sql = fmt.Sprintf("update %s set last_seq_num = %s, is_called = true",
				sequenceTableName(db, tblname), r0.format(r.last))
		default:
			sql = fmt.Sprintf("update %s set last_seq_num = %s",
				sequenceTableName(db, tblname), r0.format(r.last))
		}
		res2, err := te.Exec(sql, env.stmt)
		if err != nil {
			return err
		}
		res2.Close()
		outs = outs[:0]
		for _, emission := range emissions {
			outs = append(outs, r0.format(emission.bits))
		}
		relID = gotRel
		return nil
	}
	if err := execSeqAutonomous(proc, env, db, tblname, wantRel, fn); err != nil {
		return nil, err
	}
	// The statement's own committed allocation on this identity resolved its
	// recorded contention (A1): remove the watch entry. An empty set closes
	// the episode (Z1) — the next conflict re-admits with a fresh window.
	if retryState := proc.GetSequenceRetryState(); retryState != nil {
		retryState.RemoveWatchByName(db, tblname)
	}
	for _, out := range outs {
		proc.GetSessionInfo().SeqAddValues[relID] = out
	}
	return outs, nil
}

// nextvalRun allocates n values of ONE caller-resolved identity, routing the
// identity's whole burst by committed visibility: the committed route takes
// one committed transaction; a transaction-local generation (visible only
// inside the caller's transaction) allocates row by row in the caller
// transaction under the pass-scoped obligation protocol. One identity cannot
// straddle both routes, so the in-caller fallback of one burst is all rows.
func nextvalRun(id nextvalIdentity, n int, proc *process.Process, e engine.Engine) ([]string, error) {
	outs, err := nextvalAutonomousBatch(id.tblname, id.db, proc, e, id.wantDB, id.wantRel, n)
	if err == nil {
		return outs, nil
	}
	if !errors.Is(err, errSeqLocalGeneration) {
		return nil, err
	}
	outs = make([]string, 0, n)
	for k := 0; k < n; k++ {
		out, lerr := nextvalLocal(id.tblname, id.db, proc, id.rel)
		if lerr != nil {
			return nil, lerr
		}
		outs = append(outs, out)
	}
	return outs, nil
}

// nextvalLocal allocates one value inside the caller's transaction and
// maintains the transaction-local generation's pass-scoped obligation. The
// obligation is raised before emission is committed to the caller, so the
// record stands independently of the statement's later fate: a statement
// rewind cannot re-issue a value (repair advances from the floor), a CYCLE
// wrap reseeds the pass, and the pre-commit stamp restores only positions
// that are unsafe for the final pass.
func nextvalLocal(tblname, db string, proc *process.Process, rel engine.Relation) (string, error) {
	_values, err := sequenceSQL(proc, db, "select last_seq_num, min_value, max_value, start_value, increment_value, cycle, is_called from "+
		sequenceTableName(db, tblname)+" for update")
	if err != nil {
		return "", err
	}
	if len(_values) != 1 {
		return "", moerr.NewInternalError(proc.Ctx, "Failed to get sequence meta data.")
	}
	r, err := seqRowFromValues(proc.Ctx, _values[0])
	if err != nil {
		return "", err
	}
	em, err := emitFromRow(proc.Ctx, r, tblname)
	if err != nil {
		return "", err
	}
	relID := rel.GetTableID(proc.Ctx)
	set := proc.GetSequenceObligationSet()
	ob, has := set.Get(relID)
	write, sql := em, ""
	switch {
	case !has && em.uncalled:
		// First emission of a fresh generation: mark the start called.
		sql = fmt.Sprintf("update %s set is_called = true", sequenceTableName(db, tblname))
	case !has || em.wrapped:
		sql = fmt.Sprintf("update %s set last_seq_num = %s",
			sequenceTableName(db, tblname), r.format(em.bits))
	case ob.IssuedContains(em.bits):
		// The workspace rewound into the issued range (a rolled-back
		// statement reclaimed counter writes). Repair forward from the
		// floor; never re-issue.
		repair := r
		repair.last, repair.isCalled = ob.Floor, true
		write, err = emitFromRow(proc.Ctx, repair, tblname)
		if err != nil {
			return "", err
		}
		sql = fmt.Sprintf("update %s set last_seq_num = %s, is_called = true",
			sequenceTableName(db, tblname), r.format(write.bits))
	default:
		if em.uncalled {
			// Defensive: an obligation exists while the row is uncalled at a
			// value outside the issued range; keep the combined write so the
			// emitted value is marked called.
			sql = fmt.Sprintf("update %s set last_seq_num = %s, is_called = true",
				sequenceTableName(db, tblname), r.format(em.bits))
		} else {
			sql = fmt.Sprintf("update %s set last_seq_num = %s",
				sequenceTableName(db, tblname), r.format(em.bits))
		}
	}
	if _, err := sequenceSQL(proc, db, sql); err != nil {
		return "", err
	}
	// Raise the obligation after the write succeeded, before handing the
	// value to the caller.
	if !has || write.wrapped {
		err = set.Raise(process.SequenceObligation{
			RelID: relID, Db: db, Table: tblname,
			PassStart: write.bits, Floor: write.bits,
			Unsigned: r.unsigned(), Minus: r.incr < 0,
		})
	} else {
		// Forward-floor invariant (K1): the emission may sit behind the
		// recorded floor when a statement rewind restored an earlier
		// workspace position while a pre-rewind emission was already
		// observed. Extend widens the envelope without ever lowering the
		// frontier, so the issued values stay protected and the commit stamp
		// cannot persist a regressed position.
		ob.Extend(write.bits)
		err = set.Raise(ob)
	}
	if err != nil {
		return "", err
	}
	out := r.format(write.bits)
	proc.GetSessionInfo().SeqAddValues[relID] = out
	return out, nil
}

// setvalParseAndSQL validates setnum against the row's range per numeric arm
// and builds the assignment UPDATE with the preserved called-state contract:
// the third argument may mark a fresh sequence called but cannot reset an
// already-called one.
func setvalParseAndSQL(proc *process.Process, values []interface{}, setnum string, iscalled bool, db, tblname string) (string, string, error) {
	prevCalled, _ := values[6].(bool)
	storedCalled := prevCalled || !iscalled
	build := func(setv interface{}) (string, string) {
		return fmt.Sprintf("update %s set last_seq_num = %d, is_called = %t",
				sequenceTableName(db, tblname), setv, storedCalled),
			fmt.Sprint(setv)
	}
	rangeErr := func() (string, string, error) {
		return "", "", moerr.NewInternalError(proc.Ctx, "Set value is not in range (minvalue, maxvlue).")
	}
	switch values[0].(type) {
	case int16:
		minv, maxv := values[1].(int16), values[2].(int16)
		n, err := strconv.ParseInt(setnum, 10, 64)
		if err != nil {
			return "", "", err
		}
		if n < math.MinInt16 || n > math.MaxInt16 || n < int64(minv) || n > int64(maxv) {
			return rangeErr()
		}
		sql, out := build(int16(n))
		return sql, out, nil
	case int32:
		minv, maxv := values[1].(int32), values[2].(int32)
		n, err := strconv.ParseInt(setnum, 10, 64)
		if err != nil {
			return "", "", err
		}
		if n < math.MinInt32 || n > math.MaxInt32 || n < int64(minv) || n > int64(maxv) {
			return rangeErr()
		}
		sql, out := build(int32(n))
		return sql, out, nil
	case int64:
		minv, maxv := values[1].(int64), values[2].(int64)
		n, err := strconv.ParseInt(setnum, 10, 64)
		if err != nil {
			return "", "", err
		}
		if n < minv || n > maxv {
			return rangeErr()
		}
		sql, out := build(n)
		return sql, out, nil
	case uint16:
		minv, maxv := values[1].(uint16), values[2].(uint16)
		n, err := strconv.ParseUint(setnum, 10, 64)
		if err != nil {
			return "", "", err
		}
		if n > math.MaxUint16 || n < uint64(minv) || n > uint64(maxv) {
			return rangeErr()
		}
		sql, out := build(uint16(n))
		return sql, out, nil
	case uint32:
		minv, maxv := values[1].(uint32), values[2].(uint32)
		n, err := strconv.ParseUint(setnum, 10, 64)
		if err != nil {
			return "", "", err
		}
		if n > math.MaxUint32 || n < uint64(minv) || n > uint64(maxv) {
			return rangeErr()
		}
		sql, out := build(uint32(n))
		return sql, out, nil
	case uint64:
		minv, maxv := values[1].(uint64), values[2].(uint64)
		n, err := strconv.ParseUint(setnum, 10, 64)
		if err != nil {
			return "", "", err
		}
		if n < minv || n > maxv {
			return rangeErr()
		}
		sql, out := build(n)
		return sql, out, nil
	}
	return "", "", moerr.NewInternalError(proc.Ctx, "Wrong types of sequence number or failed to read the sequence table")
}

// setvalPublish applies the established publication contract after the
// assignment committed: with a truthy third argument the session's current
// value and an already-initialized last value follow the assignment.
func setvalPublish(proc *process.Process, tblId uint64, out string, iscalled bool) {
	if !iscalled {
		return
	}
	proc.GetSessionInfo().SeqAddValues[tblId] = out
	if proc.GetSessionInfo().SeqLastValue[0] != "" {
		proc.GetSessionInfo().SeqLastValue[0] = out
	}
}

// setvalAutonomous assigns a committed generation inside a fresh transaction
// and returns errSeqLocalGeneration when the caller-resolved identity
// (wantDB/wantRel) is not committed-visible. Identity probes match
// nextvalAutonomous: a shadowed database generation routes in-caller, while a
// changed committed relation id is a stale identity that retries bounded and
// never routes in-caller.
func setvalAutonomous(tblname, setnum string, iscalled bool, db string, proc *process.Process, e engine.Engine, wantDB string, wantRel uint64) (string, error) {
	env, err := seqExecutorEnv(proc.Ctx, proc, db)
	if err != nil {
		return "", err
	}
	var (
		out   string
		relID uint64
	)
	fn := func(attemptCtx context.Context, te executor.TxnExecutor) error {
		dbHandler, err := e.Database(attemptCtx, db, te.Txn())
		if err != nil {
			if isSeqNotVisibleErr(err) {
				return errSeqLocalGeneration
			}
			return err
		}
		if dbHandler.GetDatabaseId(attemptCtx) != wantDB {
			return errSeqLocalGeneration
		}
		rel, err := dbHandler.Relation(attemptCtx, tblname, nil)
		if err != nil {
			if isSeqNotVisibleErr(err) {
				return errSeqLocalGeneration
			}
			return err
		}
		gotRel := rel.GetTableID(attemptCtx)
		if gotRel != wantRel {
			return moerr.NewLockTableBindChanged(attemptCtx)
		}
		if err := requireSequence(attemptCtx, rel); err != nil {
			return err
		}
		res, err := te.Exec("select last_seq_num, min_value, max_value, start_value, increment_value, cycle, is_called from "+
			sequenceTableName(db, tblname)+" for update", env.stmt)
		if err != nil {
			return err
		}
		values, err := readSeqRow(attemptCtx, res)
		res.Close()
		if err != nil {
			return err
		}
		if values == nil {
			return moerr.NewInternalError(proc.Ctx, "Failed to get sequence meta data.")
		}
		sql, publish, err := setvalParseAndSQL(proc, values, setnum, iscalled, db, tblname)
		if err != nil {
			return err
		}
		res2, err := te.Exec(sql, env.stmt)
		if err != nil {
			return err
		}
		res2.Close()
		out, relID = publish, gotRel
		return nil
	}
	if err := execSeqAutonomous(proc, env, db, tblname, wantRel, fn); err != nil {
		return "", err
	}
	// The statement's own committed assignment resolved its recorded
	// contention on this identity (A1); an empty watch set retires the
	// window (Z1).
	if retryState := proc.GetSequenceRetryState(); retryState != nil {
		retryState.RemoveWatchByName(db, tblname)
	}
	setvalPublish(proc, relID, out, iscalled)
	return out, nil
}

// setvalLocal assigns a transaction-local generation inside the caller's
// transaction. A successful assignment is a deliberate boundary for the
// pass-scoped obligation: the issued record is removed — but only as of
// *statement* success. The boundary is staged at execution time: if the
// statement that invoked SETVAL later fails, its workspace writes (including
// this reposition) are rewound, so the removal is restored with them and the
// previously issued values stay protected. The statement-success publication
// point applies the staged removal (J2).
func setvalLocal(tblname, setnum string, iscalled bool, db string, proc *process.Process, rel engine.Relation) (string, error) {
	_values, err := sequenceSQL(proc, db, "select last_seq_num, min_value, max_value, start_value, increment_value, cycle, is_called from "+
		sequenceTableName(db, tblname)+" for update")
	if err != nil {
		return "", err
	}
	if len(_values) != 1 {
		return "", moerr.NewInternalError(proc.Ctx, "Failed to get sequence meta data.")
	}
	sql, out, err := setvalParseAndSQL(proc, _values[0], setnum, iscalled, db, tblname)
	if err != nil {
		return "", err
	}
	if _, err := sequenceSQL(proc, db, sql); err != nil {
		return "", err
	}
	relID := rel.GetTableID(proc.Ctx)
	setvalPublish(proc, relID, out, iscalled)
	if set := proc.GetSequenceObligationSet(); set != nil {
		set.StageRemoval(relID)
	}
	return out, nil
}

// ---------------------------------------------------------------------
// Exported reconcile API (data branch merge; design §4 B).
// The merge reconcile unit in pkg/frontend executes through the same
// execution contract as allocation: a fresh committed transaction with the
// bounded retry wrapper, the same 7-column metadata row shape, and the same
// emission engine for the NO CYCLE range guard.
// ---------------------------------------------------------------------

// SequenceAutonomousEnv is the committed-transaction execution environment
// shared by sequence allocation and the merge reconcile unit.
type SequenceAutonomousEnv struct {
	env *seqExecEnv
}

// NewSequenceAutonomousEnv binds the shared environment to a caller-picked
// context. Allocation paths pass proc.Ctx; the merge reconcile unit passes its
// statement context, which stays alive through the reconcile unit's commit.
func NewSequenceAutonomousEnv(ctx context.Context, proc *process.Process, db string) (*SequenceAutonomousEnv, error) {
	env, err := seqExecutorEnv(ctx, proc, db)
	if err != nil {
		return nil, err
	}
	// The reconcile unit shares the bounded execution contract but is not
	// sequence ALLOCATION work: tier-2 statement-retry conversion covers the
	// allocation route only (design §4 D), so a reconcile exhaustion keeps
	// the terminal named error and the merge's failure-atomicity contract.
	env.tier2Eligible = false
	return &SequenceAutonomousEnv{env: env}, nil
}

// Ctx is the internal-write context (BgKey) for engine resolution inside the
// reconcile closure.
func (e *SequenceAutonomousEnv) Ctx() context.Context {
	return e.env.ctx
}

// StatementOption is the per-statement account binding for te.Exec calls
// inside the reconcile closure.
func (e *SequenceAutonomousEnv) StatementOption() executor.StatementOption {
	return e.env.stmt
}

// Run executes fn inside a fresh committed transaction under the allocation
// route's bounded retry contract (attempts, budget, lock wait, backoff).
// The reconcile unit's identity-mismatch abort maps to
// ErrLockTableBindChanged, which the retry matrix replans.
func (e *SequenceAutonomousEnv) Run(proc *process.Process, db, name string, fn func(context.Context, executor.TxnExecutor) error) error {
	// The reconcile unit is not allocation work (tier2Eligible is cleared
	// at construction): no identity is carried and no watch state is ever
	// recorded for it.
	return execSeqAutonomous(proc, e.env, db, name, 0, fn)
}

// ReadSequenceMetadataRow parses one 7-column sequence metadata row read
// through the reconcile unit's SELECT ... FOR UPDATE.
func ReadSequenceMetadataRow(ctx context.Context, res executor.Result) ([]interface{}, error) {
	return readSeqRow(ctx, res)
}

// RequireSequenceRel exposes the allocation route's catalog-kind fence to
// the merge reconcile unit: a relation resolved by name must carry the
// sequence relkind before any sequence-shaped row is read or written, so a
// same-named ordinary-table replacement is rejected instead of being
// autonomously modified.
func RequireSequenceRel(ctx context.Context, rel engine.Relation) error {
	return requireSequence(ctx, rel)
}

// ExecLocal runs one statement through the environment's internal executor
// under caller-supplied options; the merge reconcile unit binds the options
// to the caller transaction for its transaction-local partition.
func (e *SequenceAutonomousEnv) ExecLocal(sql string, opts executor.Options) (executor.Result, error) {
	return e.env.exec.Exec(e.env.ctx, sql, opts)
}

// SequenceWatermarkDecision is the reconcile-time decision for one
// generation row against one merge watermark.
type SequenceWatermarkDecision struct {
	// Transition reports whether the monotonic conditional UPDATE would move
	// the position to the watermark (false: a genuine already-ahead no-op).
	Transition bool
	// Descending carries the allocation direction for the UPDATE predicate.
	Descending bool
	// Literal is the watermark formatted in the row's numeric domain.
	Literal string
}

// SequenceWatermarkDomainExtrema is the extreme over the captured merged
// keys representable in ONE sequence storage domain: Max/Min are plain
// magnitudes for the unsigned domains and sign-extended bits for the
// signed ones (int64(bits) round-trips the stored value). Has marks that
// at least one captured key was representable in this domain; without it
// the extreme would carry a foreign domain's bit pattern.
type SequenceWatermarkDomainExtrema struct {
	Max uint64
	Min uint64
	Has bool
}

// SequenceWatermarkExtrema carries a merge watermark's per-storage-domain
// extremes. Every captured merged key feeds exactly the domains that can
// REPRESENT it (SequenceDomain.Contains*), at every width — a 2^63 key
// feeds only the uint64 arm, a -1 key only the signed arms, a 70000 key
// the int32/int64/uint32/uint64 arms but never the int16/uint16 arms. The
// sequence's own storage domain then picks the single arm that reconciles,
// so a key the sequence could never issue can neither move its watermark
// nor trip its range guard (#30359 pre-PR review).
type SequenceWatermarkExtrema struct {
	Arms [SequenceDomainCount]SequenceWatermarkDomainExtrema
}

// CheckSequenceWatermark evaluates a FOR UPDATE-read metadata row against a
// merge watermark. The watermark arrives as presence-flagged
// per-storage-domain extrema because the merged column's type does not
// decide the comparison domain (#30359 pre-PR review): a BIGINT UNSIGNED
// sequence feeding a VARCHAR default must order 2^63 above 2^63-1, and a
// SMALLINT UNSIGNED sequence must count a merged 70000 while ignoring a
// merged 18446744073709551615 it could never issue. The SEQUENCE's own
// storage domain — read from its metadata row — picks the arm, and the
// row's allocation direction picks the extreme that counts (ascending:
// max, descending: min). When the picked arm is empty — no captured merged
// value is representable in the sequence's storage domain — the decision
// is a no-transition: such values can never collide with a future issued
// value, and folding foreign-domain bits instead would false-exhaust or
// mis-order the reconcile. When the position would transition, it also
// runs the NO CYCLE range guard: if the post-transition position cannot
// issue a next value, the guard fails with the sequence's exhaustion error
// and the merge aborts failure-atomically.
func CheckSequenceWatermark(ctx context.Context, values []interface{}, ext SequenceWatermarkExtrema, tblname string) (SequenceWatermarkDecision, error) {
	if len(values) != 7 {
		return SequenceWatermarkDecision{}, moerr.NewInternalError(ctx, "invalid sequence metadata shape")
	}
	r, err := seqRowFromValues(ctx, values)
	if err != nil {
		return SequenceWatermarkDecision{}, err
	}
	arm := ext.Arms[r.domain]
	if !arm.Has {
		// The sequence's storage-domain arm is empty: every captured
		// merged key lies outside the domain of values the sequence can
		// still issue. Reconcile nothing rather than folding
		// foreign-domain bit patterns into a comparison.
		return SequenceWatermarkDecision{Descending: r.incr < 0}, nil
	}
	desc := r.incr < 0
	watermark := arm.Max
	if desc {
		watermark = arm.Min
	}
	transition := false
	if r.unsigned() {
		if desc {
			transition = watermark < r.last || (watermark == r.last && !r.isCalled)
		} else {
			transition = watermark > r.last || (watermark == r.last && !r.isCalled)
		}
	} else {
		w, last := int64(watermark), int64(r.last)
		if desc {
			transition = w < last || (w == last && !r.isCalled)
		} else {
			transition = w > last || (w == last && !r.isCalled)
		}
	}
	if !transition {
		return SequenceWatermarkDecision{Descending: desc, Literal: r.format(watermark)}, nil
	}
	// Range guard: the post-transition position must be able to issue a next
	// value; a NO CYCLE watermark at the edge aborts (CYCLE wraps pass).
	post := r
	post.last, post.isCalled = watermark, true
	if _, err := emitFromRow(ctx, post, tblname); err != nil {
		return SequenceWatermarkDecision{}, err
	}
	return SequenceWatermarkDecision{Transition: true, Descending: desc, Literal: r.format(watermark)}, nil
}

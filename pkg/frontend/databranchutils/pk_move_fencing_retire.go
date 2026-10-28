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
	"time"

	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
)

// This file is the shared retirement critical-section core of the pk-move
// retention-ledger fencing witness subsystem (design D168(B) as amended by
// D170/D172/D173/D174/D176/D177, with D179-D182 lane mechanics). Every
// D159(E) paired-delete caller (the compile drop-lifecycle funnels and the
// frontend owner-removal reap) performs each address's retirement as ONE
// serialized critical section driven by this loop on the retiring
// statement's thread:
//
//  1. FENCING ROUND: one full witness round through the statement's fencing
//     driver (scratch grant recording B_f, then the T_f1/T_f2 committed
//     pair), producing the fencing record (C_f, C_f1, B_f). A failed round
//     never advances the re-fence counter; bounded retries re-run the whole
//     pair, and round-budget exhaustion surfaces the determinate
//     fencing-unavailable statement error (D177(B.4)).
//  2. LEDGER GRANT: the retiring transaction's own EXCLUSIVE row lock on
//     the address's ledger-row key, recording B_r and the grant watermark
//     (fence). A binding change observed INSIDE the platform lock call is
//     transaction-fatal (ErrLockTableBindChanged, class (i)) and propagates
//     verbatim — the protocol performs no in-place re-fence against it.
//  3. ACCEPTANCE: B_r == B_f AND fence >= C_f (side-effect-free to reject:
//     nothing has advanced, re-probed, or staged). Rejections classify per
//     D176(B.2): same-generation fold lag and unbound pre-grant flips
//     re-fence in place for at most K rounds; a recorded-binding change and
//     K+1 consecutive rejections raise the existing ErrLockTableBindChanged
//     into the repository's whole-transaction rollback boundary.
//  4. FRONTIER ADVANCE (D173(B)): WaitLogTailAppliedAt(fence) then
//     UpdateSnapshot(fence) — never a local clock sample.
//  5. LOCKED RE-PROBE (D170(B)(3)): a fresh internal statement inside the
//     same transaction re-reads the address's link presence at the locked
//     view, so the caller's paired delete computes its set from THAT view.
//
// Only then does the caller execute the paired delete (every link row the
// locked view shows, plus the ledger row) and the D167 RETIRE journal.

// DataBranchFencingMaxRefenceRounds is K, the protocol's bound on in-place
// same-generation re-fence rounds per retired address (D174(G), D176(B.2)).
const DataBranchFencingMaxRefenceRounds = 3

// DataBranchFencingMaxRoundFailures is the round-failure budget: fencing
// rounds that fail before any acceptance rejection (grant failure, stage
// failure, consumed commit-unknown, interruption, gate rejection, expiry,
// saturation, pool seal) retry the whole pair at most this many times; the
// next failure raises the determinate fencing-unavailable statement error
// (D177(B.4)). Round failures never advance the re-fence counter.
const DataBranchFencingMaxRoundFailures = 3

// DataBranchFencingSweepAgeHint bounds the T_f1 sweep's age-eligibility
// cutoff computation: candidates strictly older than issue time minus this
// hint. It covers one round timeout plus the platform clock-offset
// tolerance and is never load-bearing (a candidate dies only under its
// witnessed lock grant; age is a scheduling hint).
const DataBranchFencingSweepAgeHint = 30 * time.Second

// DataBranchFencingSweepLockWaitBounds is the default per-candidate bounded
// witnessed lock-wait inside T_f1's sweep: a candidate whose exclusive
// grant cannot be observed within this budget is SKIPPED for a later sweep.
const DataBranchFencingSweepLockWaitBounds = 500 * time.Millisecond

// DataBranchRetireLedgerGrant is what the retiring transaction's EXCLUSIVE
// row lock on the address's ledger-row key delivers to the acceptance gate.
type DataBranchRetireLedgerGrant struct {
	// Binding is B_r, the lock-table binding generation the grant rode.
	Binding DataBranchFencingBinding
	// Fence is the grant watermark (lock.Result.Timestamp).
	Fence timestamp.Timestamp
	// BindingRecordedBefore reports whether the retiring transaction had
	// ALREADY recorded the ledger table's binding before this grant (any
	// earlier shared probe or insert lock on the ledger table records it):
	// the D176(B.2) class split between a recorded-binding change (i,
	// terminal), a same-generation fold lag (ii), and an unbound pre-grant
	// flip (iii) turns on this fact.
	BindingRecordedBefore bool
	// Err is the lock acquisition error, non-nil when the grant failed;
	// the platform's AddLockTable Changed branch surfaces
	// ErrLockTableBindChanged here (class (i), whole-transaction fatal).
	Err error
}

// DataBranchRetireFencingRounds is the one statement-level fencing-driver
// surface the critical section needs: run one full round, then retire the
// statement's driver when the section ends. The production value is the
// fencing driver over the shared CN lane pool; fabric tests substitute a
// scripted fake.
type DataBranchRetireFencingRounds interface {
	RunRound(
		ctx context.Context,
		locks DataBranchScratchLocks,
		exec DataBranchFencingStageExecutor,
		in DataBranchFencingRoundInput,
	) (DataBranchFencingRecord, error)
	// Retire releases the statement driver's own admission and joins its
	// completed/expired lanes (DrainLedger semantics are the driver's).
	Retire()
}

// DataBranchRetireCSDeps carries the statement- and substrate-level
// closures one retirement critical section composes. Every closure runs on
// the retiring statement's thread inside the retiring transaction.
type DataBranchRetireCSDeps struct {
	// Rounds is the statement's fencing driver.
	Rounds DataBranchRetireFencingRounds
	// Locks and Exec are the round's witness-lock and stage-transaction
	// surfaces. The production implementations compose
	// LockRowsForAdmissionResult and the account-scoped internal executor
	// family; fabric tests implement them in memory.
	Locks DataBranchScratchLocks
	Exec  DataBranchFencingStageExecutor
	// AwaitWatermarkFold, when non-nil, holds the round until the ledger
	// lock-table grant watermark can observe a fence >= floor BEFORE the
	// grant is acquired (floor is the round's own C_f2). The production
	// stage commits' unlocks fold the watermark ASYNCHRONOUSLY after the
	// commit response, and every re-fence round mints a FRESH higher C_f2,
	// so an unchecked grant deterministically chases its own rising C_f
	// until the re-fence budget exhausts. The closure is the round's own
	// bounded fold-wait — never a post-rejection wait (D173(B.2)) — and a
	// non-nil error consumes the whole-pair round-failure budget
	// (D177(B.4)). Fabric cells leave it nil (their round mocks advance
	// watermarks synchronously).
	AwaitWatermarkFold func(ctx context.Context, floor timestamp.Timestamp) error
	// AcquireLedgerGrant takes the retiring transaction's EXCLUSIVE row
	// lock on the address's ledger-row key (group 0) and reports B_r, the
	// fence watermark, the pre-grant recorded-binding fact, and any
	// lock-layer error (ErrLockTableBindChanged for class (i)).
	AcquireLedgerGrant func(ctx context.Context) DataBranchRetireLedgerGrant
	// AdvanceToFence installs the D173(B) causal frontier INSIDE the
	// retiring transaction: WaitLogTailAppliedAt(fence) followed by
	// UpdateSnapshot(fence), in that order.
	AdvanceToFence func(ctx context.Context, fence timestamp.Timestamp) error
	// ReprobeLinkPresence is the D170(B)(3) locked-view re-probe: a fresh
	// internal statement inside the retiring transaction reporting the
	// address's committed link presence. Production reasons only about the
	// error; the presence bit is informational for callers and cells.
	ReprobeLinkPresence func(ctx context.Context) (bool, error)
	// NewRoundInput draws one round's identity: a FRESH scratch key
	// (DataBranchFencingScratchDraw), the issue-time physical ns, the
	// last-writer commit the pair must out-order when known (empty
	// otherwise), and the sweep scheduling knobs.
	NewRoundInput func() (DataBranchFencingRoundInput, error)
	// MaxRefenceRounds bounds in-place acceptance re-fences (K); zero
	// takes DataBranchFencingMaxRefenceRounds.
	MaxRefenceRounds int
	// MaxRoundFailures bounds whole-pair round retries; zero takes
	// DataBranchFencingMaxRoundFailures.
	MaxRoundFailures int
}

// RunDataBranchFencingRetireCS drives the retirement critical section to
// acceptance: on nil return the ledger grant is HELD, the causal frontier
// is installed, and the locked re-probe has run — the caller executes its
// paired delete set (plus the D167 RETIRE journal) immediately. Every
// non-nil return routes through the existing statement-error surface:
// ErrLockTableBindChanged (class (i), terminal class (ii)/(iii) exhaustion)
// for the whole-transaction rollback boundary, the typed fencing classes
// converted by NewDataBranchFencingUnavailable for fencing-floor failures,
// and the closures' own errors verbatim.
func RunDataBranchFencingRetireCS(
	ctx context.Context,
	deps DataBranchRetireCSDeps,
) (linkPresence bool, err error) {
	maxRefence := deps.MaxRefenceRounds
	if maxRefence <= 0 {
		maxRefence = DataBranchFencingMaxRefenceRounds
	}
	maxFailures := deps.MaxRoundFailures
	if maxFailures <= 0 {
		maxFailures = DataBranchFencingMaxRoundFailures
	}
	counter := NewDataBranchRefenceCounter(maxRefence)
	if deps.Rounds != nil {
		defer deps.Rounds.Retire()
	}

	failures := 0
	for {
		if err = ctx.Err(); err != nil {
			return false, err
		}
		in, inErr := deps.NewRoundInput()
		if inErr != nil {
			return false, NewDataBranchFencingUnavailable(
				ctx, DataBranchFencingRoundGateRejected, inErr)
		}
		rec, roundErr := deps.Rounds.RunRound(ctx, deps.Locks, deps.Exec, in)
		if roundErr != nil {
			failures++
			if failures > maxFailures {
				return false, statementErrorOfFencingRound(ctx, roundErr)
			}
			// A failed round never advances the re-fence counter; the
			// bounded retry re-runs the whole pair.
			continue
		}
		if deps.AwaitWatermarkFold != nil {
			// Hold the round until the ledger lock-table grant watermark
			// can observe a fence >= C_f2: without the fold-wait the
			// grant deterministically observes a fence below this round's
			// fresh C_f2 because the stage-commit unlocks fold after the
			// commit response.
			if err = deps.AwaitWatermarkFold(ctx, rec.CommitTS); err != nil {
				failures++
				if failures > maxFailures {
					return false, statementErrorOfFencingRound(ctx, err)
				}
				continue
			}
		}
		grant := deps.AcquireLedgerGrant(ctx)
		if grant.Err != nil {
			// The lock layer's own classes propagate verbatim: a
			// recorded-binding change raised inside AddLockTable (class
			// (i)), a lock-wait timeout, or a deadlock victimization are
			// the platform's whole-transaction rollback codes (D171(B)).
			return false, grant.Err
		}
		if AcceptDataBranchFencingRecord(rec, grant.Binding, grant.Fence) {
			if err = deps.AdvanceToFence(ctx, grant.Fence); err != nil {
				return false, err
			}
			if linkPresence, err = deps.ReprobeLinkPresence(ctx); err != nil {
				return false, err
			}
			return linkPresence, nil
		}
		class := ClassifyDataBranchRetireRejection(
			DataBranchRetireGrantView{
				CallerBindingRecorded: grant.BindingRecordedBefore,
				GrantErr:              nil,
				GrantBinding:          grant.Binding,
				Fence:                 grant.Fence,
			},
			rec,
		)
		if err = counter.Consume(ctx, class); err != nil {
			// Terminal class (i) or K+1 consecutive reachable rejections:
			// the existing ErrLockTableBindChanged rides the repository's
			// whole-transaction rollback boundary (D171(B.3)/(C)).
			return false, err
		}
		// Within budget, classes (ii)/(iii): re-fence in place.
	}
}

// statementErrorOfFencingRound converts a round failure into the retiring
// statement's ONLY fencing-visible error class: a determinate internal
// error naming the round class (D177(B.4)); ErrTxnUnknown never reaches the
// statement through here — the driver consumed it into the commit-unknown
// class at the lane boundary.
func statementErrorOfFencingRound(ctx context.Context, err error) error {
	if fe, ok := AsDataBranchFencingError(err); ok {
		return fe.StatementError(ctx)
	}
	return NewDataBranchFencingUnavailable(ctx, DataBranchFencingRoundStageFailed, err)
}

// DataBranchFencingNewRoundInput builds the production round-input draw:
// one fresh scratch key per round (ownership demands freshness), the issue
// time as physical ns, the age-hint sweep cutoff, and the bounded
// per-candidate witnessed lock-wait. LastWriterCommitTS stays empty unless
// the caller proves one — record assembly skips the C comparison then, and
// C_f > C_f1 ordering still stands.
func DataBranchFencingNewRoundInput() (DataBranchFencingRoundInput, error) {
	draw, err := DataBranchFencingScratchDraw()
	if err != nil {
		return DataBranchFencingRoundInput{}, err
	}
	issue := uint64(time.Now().UnixNano())
	return DataBranchFencingRoundInput{
		ScratchTableID: draw,
		IssueTimeNS:    issue,
		// Empty: no caller-side reliable last-writer commit exists; the
		// TN allocator's per-shard monotonicity delivers C_f1 > C for every
		// writer committed before T_f1 began, and assembly still enforces
		// C_f > C_f1.
		LastWriterCommitTS: timestamp.Timestamp{},
		SweepAgeCutoffNS:   DataBranchFencingSweepAgeCutoff(issue, DataBranchFencingSweepAgeHint),
		SweepLockWait:      DataBranchFencingSweepLockWaitBounds,
	}, nil
}

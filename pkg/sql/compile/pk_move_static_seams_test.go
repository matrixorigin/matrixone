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
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// Static seam cells (D212(C.4) superseding D211(C.2); D213(C.4) gaining the
// fenced-outcome pins; D197(C.2); D199(C.2)/(C.3); D206(C.1)'s seal-removed
// control discipline; D192's disposal edge): byte-exact source pins over
// the CRUD partition of the retention identity row (0,T), every setter
// site of the expected-conflict mark, the carrier, the funnel call sites,
// the fenced grant's admission shape and both checker class lists.
// Modification of any pin REOPENS the owning decision.

func seamRepoRoot(t *testing.T) string {
	t.Helper()
	_, this, _, ok := runtime.Caller(0)
	require.True(t, ok)
	// <root>/pkg/sql/compile/pk_move_static_seams_test.go
	return filepath.Clean(filepath.Join(filepath.Dir(this), "..", "..", ".."))
}

func seamRead(t *testing.T, rel string) string {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(seamRepoRoot(t), rel))
	require.NoError(t, err, rel)
	return string(b)
}

// seamSpan extracts one top-level declaration's text from sig to the
// next top-level declaration (Go-formatted sources keep top-level decls
// at column zero).
func seamSpan(t *testing.T, src, sig string) string {
	t.Helper()
	start := strings.Index(src, sig)
	require.GreaterOrEqual(t, start, 0, "declaration missing: %s", sig)
	rest := src[start:]
	next := strings.Index(rest[len(sig):], "\nfunc ")
	if next < 0 {
		return rest
	}
	return rest[:len(sig)+next]
}

func requireContains(t *testing.T, src, pattern, why string) {
	t.Helper()
	require.True(t, strings.Contains(src, pattern),
		"seam pin missing %q (%s)", pattern, why)
}

func requireNotContains(t *testing.T, src, pattern, why string) {
	t.Helper()
	require.False(t, strings.Contains(src, pattern),
		"seam pin violated by %q (%s)", pattern, why)
}

// D212(C.4)/D213(C.4) THE CRUD-PARTITION SEAM: every kind=0 retention-row
// write repo-wide maps to exactly one setter site of the D206(B.1) mark,
// and every fabric transaction that can expect a conflict is marked.
func TestRetentionCRUDPartitionWriterSites(t *testing.T) {
	// Writers (i) and the write-mark admission gate: disttae workspace
	// insert of a retention identity row.
	disttaeTxn := seamRead(t, "pkg/vm/engine/disttae/txn.go")
	requireContains(t, disttaeTxn,
		"if pkMoveGenesisMarked {",
		"SITE (1) genesis registration gate stands")
	site1 := seamSpan(t, disttaeTxn, "\tpkMoveGenesisMarked := typ == INSERT")
	requireContains(t, site1,
		"client.MarkTxnExpectedConflictIfSupported(txn.op)",
		"SITE (1) marks the genesis-mover transaction")
	requireContains(t, site1,
		"txn.probePkMoveGenesis(",
		"SITE (1) precedes the keyed committed-state probe")

	// Writer (ii): the HIT-lane verdict seal (multi_update).
	muLink := seamRead(t, "pkg/sql/colexec/multi_update/pk_move_link.go")
	seal := seamSpan(t, muLink, "func sealPkMoveRetentionRowShared(")
	requireContains(t, seal,
		"client.MarkTxnExpectedConflictIfSupported(proc.GetTxnOperator())",
		"SITE (2) marks the sealing producer transaction")
	requireNotContains(t, seal, "lockop.",
		"the seal issues NO lock request of its own (D199(B.2))")
	requireContains(t, muLink,
		"var pkMoveRetentionSealDisabled atomic.Bool",
		"the seal-staging defect flag stands for (C.1)'s seal-removed arm")

	// Writer (iii) and the background retirement DELETE's transaction:
	// the frontend owner-removal reap.
	feReap := seamRead(t, "pkg/frontend/data_branch_link_retention.go")
	reap := seamSpan(t, feReap, "func reapDataBranchLinksAfterOwnerRemoval(")
	requireContains(t, reap,
		"client.MarkTxnExpectedConflictIfSupported(reapOp)",
		"SITE (4) marks the DROP's OWN background transaction")
	requireContains(t, reap,
		"pk-move reap mark: background txn operator absent",
		"SITE (4) fails closed on a nil background operator (D210(B))")
	requireNotContains(t, reap, "ses.proc.GetTxnOperator()",
		"the outer admission-set operator is never consulted (D210(B))")
	requireContains(t, feReap,
		"var resolveReapTxnOperator = backgroundExecTxnOperator",
		"the D210(B) resolution defaults to the background executor's own operator")

	// Writers (v)/(vi): the compile KEEP enrichment UPDATE pair and the
	// KEEP INSERT — SETTER SITE (5) at the funnel's entry.
	retention := seamRead(t, "pkg/sql/compile/pk_move_link_retention.go")
	keep := seamSpan(t, retention, "func insertPkMoveLinkRetention(")
	requireContains(t, keep,
		"client.MarkTxnExpectedConflictIfSupported(keepOp)",
		"SITE (5) marks the drop statement's own transaction")
	requireContains(t, keep,
		"DataBranch-PkMoveLink-RetentionMarkSkippedNoTxn",
		"SITE (5)'s nil observation keeps the funnel's keep shape")
	requireNotContains(t, keep, "PkMoveGenesisWriteMarked",
		"the KEEP funnel deliberately bypasses genesis admission (D211(A))")

	// Writer (vii): the compile retirement DELETE pair — SETTER SITE (6)
	// at the funnel's entry, BEFORE the fencing CS.
	retire := seamSpan(t, retention, "func deletePkMoveLinkAndRetention(")
	requireContains(t, retire,
		"client.MarkTxnExpectedConflictIfSupported(retireOp)",
		"SITE (6) marks the retiring transaction")
	requireContains(t, retire,
		"pk-move retirement mark: txn operator absent",
		"SITE (6) fails closed on a nil retiring operator (D210(B))")
	markIdx := strings.Index(retire, "client.MarkTxnExpectedConflictIfSupported(retireOp)")
	csIdx := strings.Index(retire, "runDataBranchPkMoveFencingRetire(c, accountID, tableID)")
	require.GreaterOrEqual(t, markIdx, 0)
	require.Greater(t, csIdx, markIdx,
		"SITE (6) precedes the fencing CS so no guard/degrade arm escapes it")
	// The DELETEs pin row_kind = 0 with the legacy names-only degrade.
	requireContains(t, retention,
		"catalog.DataBranchLinkRetentionColRowKind, catalog.DataBranchLinkRetentionRowKindReal",
		"the primary retirement and refresh UPDATEs pin the real row kind")
	requireContains(t, retention,
		"PkMoveLedgerMissingColumnErr",
		"the legacy names-only degrade stands")

	// The four retirement funnel call sites and their owner functions.
	ddl := seamRead(t, "pkg/sql/compile/ddl.go")
	require.Equal(t, 4, strings.Count(ddl, "deletePkMoveLinkAndRetention("),
		"the retirement funnel keeps its four pinned call sites")
	for _, owner := range []string{
		"deletePkMoveUpdateLinksForReclaimedSubtrees",
		"reclaimUnbranchedDropPkMoveLinks",
		"deletePkMoveUpdateLinksForIDs",
	} {
		requireContains(t, ddl, owner, "retirement funnel scope owner stands")
	}

	// The carrier: every funnel write rides the statement's own transaction.
	compileGo := seamRead(t, "pkg/sql/compile/compile.go")
	requireContains(t, compileGo,
		"WithTxn(c.proc.GetTxnOperator())",
		"internal funnel SQL carrier is the statement's own operator")
}

// D213(C.4)'s FENCED-OUTCOME pins: the fenced grant's admission shape
// (early return before any MVCC check, binding refusal, contention WAIT)
// and both checker class lists the fenced outcomes never match. Together
// with the three-outcome contract these make the Fenced path's mark INERT
// and the NO-Fatal-twin claim load-bearing.
func TestFencedOutcomeShapePins(t *testing.T) {
	cs := seamRead(t, "pkg/sql/compile/pk_move_fencing_cs.go")
	grant := seamSpan(t, cs, "func (s *DataBranchFencingRetireSubstrate) AcquireLedgerGrant(")
	requireContains(t, grant,
		"lockop.LockRowsForAdmissionResult(",
		"the fenced grant performs ADMISSION locking")
	requireContains(t, grant,
		"lock.LockMode_Exclusive",
		"the fenced grant is the EXCLUSIVE group-0 admission")

	// The guard: nil/non-pessimistic/non-RC retiring statements skip to the
	// plain paired DELETEs; substrate degradation skips likewise.
	guard := seamSpan(t, cs, "func RunDataBranchFencingRetirementCS(")
	requireContains(t, guard,
		"!txnOp.Txn().IsPessimistic() || !txnOp.Txn().IsRCIsolation()",
		"the skip guard stands (skip-path plain DELETE surface)")
	skip := seamSpan(t, cs, "func runDataBranchPkMoveFencingRetire(")
	requireContains(t, skip,
		"if sub == nil {\n\t\treturn false, false, nil\n\t}",
		"the substrate degrade arm stands")

	lockOp := seamRead(t, "pkg/sql/colexec/lockop/lock_op.go")
	requireContains(t, lockOp,
		"if opts.admissionOnly && (!result.LockedOn.Valid || result.LockedOn.Table != tableID || result.LockedOn.Group != opts.group) {",
		"the admission path's only lock-layer refusal is the binding check")
	requireContains(t, lockOp,
		"opts.admissionResult = result",
		"the admission early return ships the result before any MVCC segment")

	local := seamRead(t, "pkg/lockservice/lock_table_local.go")
	requireContains(t, local,
		"c.w = acquireWaiter(c.waitTxn, \"acquireRowLockLocked\", l.logger)",
		"ordinary contention creates a waiter")
	requireContains(t, local,
		"return l.handleLockConflictLocked(c, key, lock)",
		"contended acquisition WAITS (no checker-class error surfaces)")

	// The two checker class lists the fenced outcomes never match.
	op := seamRead(t, "pkg/txn/client/operator.go")
	requireContains(t, op,
		"if !tc.expectedConflictMark.Load() &&",
		"the watchdog mark consult gates the Fatal")
	requireContains(t, op,
		"moerr.IsMoErrCode(err, moerr.ErrTxnWWConflict) ||",
		"the watchdog consult covers WW")
	requireContains(t, op,
		"moerr.IsMoErrCode(err, moerr.ErrDuplicateEntry))",
		"the watchdog consult covers duplicate entry")
	requireContains(t, op,
		"commonruntime.EnableCheckInvalidRCErrors",
		"the watchdog consult fires only with the checker armed")

	compileGo := seamRead(t, "pkg/sql/compile/compile.go")
	fatalLog := seamSpan(t, compileGo, "func (c *Compile) fatalLog(")
	requireContains(t, fatalLog,
		"moerr.IsMoErrCode(err, moerr.ErrTxnWWConflict)",
		"fatalLog consults WW")
	requireContains(t, fatalLog,
		"ExpectedConflictMarked() bool",
		"fatalLog's mark consult stands and precedes the fatal arm")
	consultIdx := strings.Index(fatalLog, "ExpectedConflictMarked() bool")
	armIdx := strings.Index(fatalLog, "EnableCheckInvalidRCErrors")
	require.GreaterOrEqual(t, consultIdx, 0)
	require.Greater(t, armIdx, consultIdx,
		"the mark consult precedes the checker arming read")
}

// D197(C.2) requireRC PRECONDITION: every admission fabric caller is a
// pessimistic RC stage transaction, so the admission validation predicate
// can never fire; SITE (3) marks that stage transaction.
func TestStageTxnPessimisticRCAndMark(t *testing.T) {
	cs := seamRead(t, "pkg/sql/compile/pk_move_fencing_cs.go")
	stage := seamSpan(t, cs, "func (s *DataBranchFencingRetireSubstrate) newStageTxn(")
	requireContains(t, stage,
		"client.WithTxnIsolation(txn.TxnIsolation_RC),",
		"stage transactions are RC (admission requireRC satisfied)")
	requireContains(t, stage,
		"client.WithTxnMode(txn.TxnMode_Pessimistic),",
		"stage transactions are pessimistic")
	requireContains(t, stage,
		"\"databranch-fencing\"",
		"stage transactions carry the protocol creation label")
	requireContains(t, stage,
		"client.MarkTxnExpectedConflictIfSupported(op)",
		"SITE (3) marks the stage transaction")
	markIdx := strings.Index(stage, "client.MarkTxnExpectedConflictIfSupported(op)")
	engIdx := strings.Index(stage, "s.eng.New(ctx, op)")
	require.GreaterOrEqual(t, markIdx, 0)
	require.Greater(t, engIdx, markIdx,
		"the mark lands before engine registration")
	// The disjoint attribution: the stage mark is created INSIDE newStageTxn
	// and is never the retiring statement's operator.
	requireNotContains(t, stage, "c.proc.GetTxnOperator()",
		"the stage transaction is not the retiring transaction")
}

// D199(C.2)/(C.3) ONE-ACQUISITION-ONE-MODE: the HIT lane issues NO lock
// request of its own on any isolation shape — the retired shared anchor's
// symbol is gone repo-wide — and the producer path constructs no FastFail
// option and no writer-fair context for the ledger key.
func TestProducerOneModeSeamPins(t *testing.T) {
	// Repo-wide absence of the retired anchor helper in every non-test
	// production source: any surviving reference fails the seam.
	root := seamRepoRoot(t)
	var stale []string
	require.NoError(t, filepath.Walk(filepath.Join(root, "pkg"),
		func(path string, info os.FileInfo, err error) error {
			if err != nil || info.IsDir() ||
				!strings.HasSuffix(path, ".go") ||
				strings.HasSuffix(path, "_test.go") {
				return err
			}
			b, rerr := os.ReadFile(path)
			if rerr != nil {
				return rerr
			}
			if strings.Contains(string(b), "lockPkMoveRetentionRowShared") &&
				!strings.HasSuffix(path, "pk_move_static_seams_test.go") {
				stale = append(stale, path)
			}
			return nil
		}))
	require.Empty(t, stale,
		"the retired HIT anchor helper must not survive in any production source")

	muLink := seamRead(t, "pkg/sql/colexec/multi_update/pk_move_link.go")
	requireNotContains(t, muLink, "lock.WaitPolicy_FastFail",
		"the fabric constructs no FastFail policy for the ledger key")
	// The only surviving acquisition is the OWNED lane's first-touch
	// exclusive with the platform default WAIT policy (D186(B.4)(a)).
	owned := seamSpan(t, muLink, "func lockPkMoveRetentionRow(")
	requireContains(t, owned,
		"lock.LockMode_Exclusive, lock.Sharding_None, 0, lock.WaitPolicy_Wait)",
		"the OWNED lane's single exclusive stands on the platform default policy")
}

// D192(B.2)/(C.1) disposal edge and D195(B.1) fold deadline: the probe's
// disposal is the detached AbandonAsync (the synchronous-Abandon RED-twin
// shape is BY CONSTRUCTION any span placing .Abandon( on the statement
// thread), and ONE absolute fold deadline is minted at entry, before the
// first probe consumes foldCtx.
func TestProbeDisposalAndFoldShapePins(t *testing.T) {
	cs := seamRead(t, "pkg/sql/compile/pk_move_fencing_cs.go")
	probe := seamSpan(t, cs, "func (s *DataBranchFencingRetireSubstrate) probeScratchWatermark(")
	requireContains(t, probe,
		"defer owner.AbandonAsync()",
		"the probe's disposal is detached accounted work (D192(B.2))")
	requireNotContains(t, probe, ".Abandon(",
		"no synchronous Abandon remains on the statement thread (the RED twin)")

	fold := seamSpan(t, cs, "func (s *DataBranchFencingRetireSubstrate) awaitLedgerWatermark(")
	requireContains(t, fold,
		"foldCtx, foldCancel := context.WithDeadlineCause(ctx, deadline, errDataBranchWatermarkFoldTimeout)",
		"ONE absolute fold deadline with the named cause (D195(B.1))")
	mintIdx := strings.Index(fold, "context.WithDeadlineCause")
	firstProbeIdx := strings.Index(fold, "s.probeScratchWatermark(foldCtx")
	require.GreaterOrEqual(t, mintIdx, 0)
	require.Greater(t, firstProbeIdx, mintIdx,
		"the deadline is minted at ENTRY, strictly before the first probe")
	requireContains(t, fold,
		"s.txnClient.WaitLogTailAppliedAt(foldCtx, floor)",
		"the logtail pre-wait rides foldCtx")
}

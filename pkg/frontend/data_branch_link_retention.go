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

package frontend

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync/atomic"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/sqlquote"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/frontend/databranchutils"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"go.uber.org/zap"
)

// This file owns the frontend half of the pk-move link retention lifecycle
// (design D153/D154/D155): when a DROP PITR or DROP SNAPSHOT removes a
// retained OWNER, every account that owner could have protected is scanned
// and each retained dead table's links retire exactly when no remaining
// snapshot/pitr owner and no live lineage descendant still grounds them.
//
// Failure contract (D155-C): every error inside the reap — mo_account
// enumeration, per-account attach/scan, owner and lineage probes, the
// deletes themselves — fails the DROP statement, and because the reap rides
// the drop's pessimistic transaction the whole change rolls back: the owner
// row and every already-processed account's links/ledger survive
// byte-identically for an identical retry. There is deliberately no
// commit-with-WARN class: once the owner row is gone, a retried DROP ... IF
// EXISTS early-returns before any maintenance, so nothing could ever retry
// a half-committed reap. The only tolerated absence is a missing
// mo_data_branch_update_link / mo_data_branch_link_retention catalog in a
// pre-upgrade tenant (no rows is the empty fast path).

// pkMoveGenesisRetireSink is the single CN-workspace (disttae) capability
// the owner-removal reap needs for the D167 RETIRE transition. It is an
// interface here so the frontend half never imports the engine package.
type pkMoveGenesisRetireSink interface {
	// PkMoveRetireAddresses drops the transaction's genesis epoch for
	// every retired address in the same statement journaled segment as
	// the paired deletes; transactions that never tracked the address
	// (background reaps, ordinary drops) hit the uniform map-miss no-op.
	PkMoveRetireAddresses(accountID uint32, tableIDs []uint64)
}

// pkMoveGenesisRetireSinkOf resolves the drop transaction's workspace as a
// retire sink. Any context whose workspace is not the disttae registry
// (clone-snapshot reads, mock sessions) yields nil, and the reap simply
// skips the transition — the map-miss no-op is indistinguishable there.
func pkMoveGenesisRetireSinkOf(ses *Session) pkMoveGenesisRetireSink {
	if ses == nil {
		return nil
	}
	th := ses.GetTxnHandler()
	if th == nil {
		return nil
	}
	txnOp := th.GetTxn()
	if txnOp == nil {
		return nil
	}
	sink, _ := txnOp.GetWorkspace().(pkMoveGenesisRetireSink)
	return sink
}

// dataBranchRemovedOwnerScope is the classification projection of the owner
// row a DROP is about to delete, read pre-DELETE inside the drop's own
// pessimistic transaction (D155-B) so the classification cannot race the
// removal.
type dataBranchRemovedOwnerScope struct {
	level string
	// accountID is recorded only by mo_pitr rows (bigint unsigned);
	// mo_snapshots rows never carry it (zero there).
	accountID   uint64
	accountName string
	objID       uint64
}

// readDroppedPitrOwnerScopes returns the classification rows of every
// mo_pitr row the pending delete targets. The primary key is (pitr_name,
// create_account), so the result has at most one row; an empty result means
// the row is already gone and no reap fires for it.
func readDroppedPitrOwnerScopes(
	ctx context.Context, bh BackgroundExec, pitrName string, createAccount uint64,
) ([]dataBranchRemovedOwnerScope, error) {
	sql := fmt.Sprintf(
		"select level, ifnull(account_id, 0), ifnull(account_name, ''), ifnull(obj_id, 0) "+
			"from mo_catalog.mo_pitr where pitr_name = '%s' and create_account = %d",
		pitrName, createAccount,
	)
	bh.ClearExecResultSet()
	if err := bh.Exec(ctx, sql); err != nil {
		return nil, err
	}
	erArray, err := getResultSet(ctx, bh)
	if err != nil {
		return nil, err
	}
	var scopes []dataBranchRemovedOwnerScope
	if execResultArrayHasData(erArray) {
		var scope dataBranchRemovedOwnerScope
		if scope.level, err = erArray[0].GetString(ctx, 0, 0); err != nil {
			return nil, err
		}
		if scope.accountID, err = erArray[0].GetUint64(ctx, 0, 1); err != nil {
			return nil, err
		}
		if scope.accountName, err = erArray[0].GetString(ctx, 0, 2); err != nil {
			return nil, err
		}
		if scope.objID, err = erArray[0].GetUint64(ctx, 0, 3); err != nil {
			return nil, err
		}
		scopes = append(scopes, scope)
	}
	return scopes, nil
}

// readDroppedSnapshotOwnerScopes is the mo_snapshots twin of the pitr read:
// kind = 'branch' rows never reach it (doDropSnapshot rejects them before
// the delete), so every row here is a retained user/cluster snapshot.
func readDroppedSnapshotOwnerScopes(
	ctx context.Context, bh BackgroundExec, snapshotName string,
) ([]dataBranchRemovedOwnerScope, error) {
	if err := inputNameIsInvalid(ctx, snapshotName); err != nil {
		return nil, err
	}
	sql := fmt.Sprintf(
		"select level, ifnull(account_name, ''), ifnull(obj_id, 0) "+
			"from mo_catalog.mo_snapshots where sname = '%s' order by snapshot_id",
		snapshotName,
	)
	bh.ClearExecResultSet()
	if err := bh.Exec(ctx, sql); err != nil {
		return nil, err
	}
	erArray, err := getResultSet(ctx, bh)
	if err != nil {
		return nil, err
	}
	var scopes []dataBranchRemovedOwnerScope
	if execResultArrayHasData(erArray) {
		var scope dataBranchRemovedOwnerScope
		if scope.level, err = erArray[0].GetString(ctx, 0, 0); err != nil {
			return nil, err
		}
		if scope.accountName, err = erArray[0].GetString(ctx, 0, 1); err != nil {
			return nil, err
		}
		if scope.objID, err = erArray[0].GetUint64(ctx, 0, 2); err != nil {
			return nil, err
		}
		scopes = append(scopes, scope)
	}
	return scopes, nil
}

// classifyRemovedDataBranchOwnerAccounts maps one or more removed owner
// rows to the accounts whose retained links the removal could unprotect
// (D154-C/D155-B): a cluster-level owner covers every tenant in
// mo_account; an account-level owner covers its recorded target (obj_id /
// account_id, with the recorded account_name resolving legacy rows); a
// database/table/sys-local owner covers the owning account alone. The
// dropping account always joins the set — it committed the removal — and
// the union is sorted for deterministic scan order. Targets whose account
// no longer exists are skipped (their catalogs died with the account);
// every lookup FAILURE aborts the classification so the drop rolls back
// atomically (D155-C).
func classifyRemovedDataBranchOwnerAccounts(
	ctx context.Context, bh BackgroundExec,
	scopes []dataBranchRemovedOwnerScope, caller uint32,
) ([]uint32, error) {
	targets := map[uint32]struct{}{caller: {}}
	for _, scope := range scopes {
		switch scope.level {
		case "cluster":
			ids, err := selectMoAccountIDs(ctx, bh)
			if err != nil {
				return nil, err
			}
			for _, id := range ids {
				targets[id] = struct{}{}
			}
		case "account":
			target := uint32(0)
			switch {
			case scope.objID != 0:
				target = uint32(scope.objID)
			case scope.accountID != 0:
				target = uint32(scope.accountID)
			case scope.accountName != "":
				id, err := lookupMoAccountIDByName(ctx, bh, scope.accountName)
				if err != nil {
					return nil, err
				}
				if id == 0 {
					// The recorded account is gone; nothing can hold links
					// for it any more (catalogs die with the account).
					continue
				}
				target = uint32(id)
			default:
				continue
			}
			exists, err := moAccountExists(ctx, bh, target)
			if err != nil {
				return nil, err
			}
			if !exists {
				continue
			}
			targets[target] = struct{}{}
		default:
			// database/table/sys-local rows: the recorded owner account
			// when the row carries one (pitr), else the catalog account
			// that ran the drop — already in the set via caller.
			if scope.accountID != 0 {
				targets[uint32(scope.accountID)] = struct{}{}
			}
		}
	}
	accounts := make([]uint32, 0, len(targets))
	for account := range targets {
		accounts = append(accounts, account)
	}
	sort.Slice(accounts, func(i, j int) bool { return accounts[i] < accounts[j] })
	return accounts, nil
}

// ---------------------------------------------------------------------------
// D155-C' scoped defect-flag seam (D186(B.5)(i)): test-only, same
// discipline as the databranchutils defect flags — harness-only installer,
// never enabled on any production path, and certification is never
// consulted against the armed seam (the D185 envelope stays enforced end
// to end; the fault framework plays no part). Unlike the objectio seam
// above, this one is process-local and needs no fault-framework reboot
// window, which the pkg/tests/service transaction-backed cells (r)/(s)
// require.
// ---------------------------------------------------------------------------

var dataBranchOwnerRemovalReapSeamArmed atomic.Bool

// dataBranchOwnerRemovalReapSeamAccount holds the armed account name (or
// numeric id) as a string; written before the armed flag is set, read only
// under armed==true.
var dataBranchOwnerRemovalReapSeamAccount atomic.Value

// dataBranchOwnerRemovalReapObserver is the D155-C' ordered witness
// (D186(B.5)(ii), the driver trace family): a test-only observer invoked
// as each account's certified reap step COMPLETES, in step order. Same
// discipline as the seam - harness-only installer, never set on any
// production path; unset it costs one atomic load per account step. The
// nil-as-unset convention stores a no-op when cleared (atomic.Value needs
// a consistent concrete type).
var dataBranchOwnerRemovalReapObserver atomic.Value // func(accountID uint32)

// SetDataBranchOwnerRemovalReapObserverForTest installs (fn != nil) or
// clears (fn == nil) the ordered reap observer and returns a restore
// func; harness-only.
func SetDataBranchOwnerRemovalReapObserverForTest(fn func(accountID uint32)) (restore func()) {
	prev, _ := dataBranchOwnerRemovalReapObserver.Load().(func(uint32))
	if fn == nil {
		fn = func(uint32) {}
	}
	dataBranchOwnerRemovalReapObserver.Store(fn)
	return func() {
		if prev == nil {
			prev = func(uint32) {}
		}
		dataBranchOwnerRemovalReapObserver.Store(prev)
	}
}

// FailDataBranchOwnerRemovalReapAccountForTest arms (name != "") or
// disarms (name == "") the D155-C' seam for the NAMED account and returns
// a restore func; harness-only.
func FailDataBranchOwnerRemovalReapAccountForTest(name string) (restore func()) {
	prevArmed := dataBranchOwnerRemovalReapSeamArmed.Load()
	var prevName any
	if prevArmed {
		prevName = dataBranchOwnerRemovalReapSeamAccount.Load()
	}
	if name == "" {
		dataBranchOwnerRemovalReapSeamArmed.Store(false)
	} else {
		dataBranchOwnerRemovalReapSeamAccount.Store(name)
		dataBranchOwnerRemovalReapSeamArmed.Store(true)
	}
	return func() {
		if prevArmed {
			dataBranchOwnerRemovalReapSeamAccount.Store(prevName)
		}
		dataBranchOwnerRemovalReapSeamArmed.Store(prevArmed)
	}
}

// reapDataBranchLinksAfterOwnerRemoval scans every account a just-removed
// owner could have protected and retires each retained dead table whose
// links are no longer grounded: no remaining snapshot/pitr owner covers
// the table (the ONE shared coverage predicate, databranchutils) and no
// live lineage descendant survives in mo_branch_metadata. An account with
// an empty retention ledger skips every owner probe (the empty-ledger fast
// path, the common case on clusters without pk-move branches). Scopes
// empty means nothing was removed and no reap fires — the DROP ... IF
// EXISTS early-return path relies on that.
// resolveReapTxnOperator resolves the DROP's OWN background transaction
// operator for D206(B.1) SETTER SITE (4) (D210(B): backgroundExecTxnOperator
// unwraps the back session's own txn handler inside the live bh
// transaction; ses.proc.GetTxnOperator() is a DIFFERENT admission-set
// transaction and is never consulted). A variable so harness cells and the
// funnel UTs can supply an operator for non-*backExec executors; production
// performs the real unwrap.
var resolveReapTxnOperator = backgroundExecTxnOperator

// SetReapTxnOperatorResolverForTest wraps the reap driver's operator
// resolution (D155-C harness-only discipline; the variable exists so
// harness cells can supply or observe the operator) and returns a restore
// func. fn receives the background executor and the PREVIOUS resolver
// (production resolution unless re-wrapped), so an observing spy can
// delegate and return the production operator. A nil fn restores
// production resolution.
func SetReapTxnOperatorResolverForTest(
	fn func(bh BackgroundExec, next func(BackgroundExec) client.TxnOperator) client.TxnOperator,
) (restore func()) {
	prev := resolveReapTxnOperator
	if fn == nil {
		resolveReapTxnOperator = backgroundExecTxnOperator
	} else {
		resolveReapTxnOperator = func(bh BackgroundExec) client.TxnOperator {
			return fn(bh, prev)
		}
	}
	return func() { resolveReapTxnOperator = prev }
}

// dataBranchReapAccountError carries the failing account's identity at the
// per-account reap boundary while preserving the inner error's identity for
// ErrorIs provenance assertions (the SCA err-check forbids the fmt %w
// wrap constructor, and moerr flattening detaches the wrapped *moerr.Error).
type dataBranchReapAccountError struct {
	account uint32
	err     error
}

func (e *dataBranchReapAccountError) Error() string {
	return fmt.Sprintf("pk-move link reap of account %d: %v", e.account, e.err)
}

func (e *dataBranchReapAccountError) Unwrap() error { return e.err }

func reapDataBranchLinksAfterOwnerRemoval(
	ctx context.Context, bh BackgroundExec, sid string,
	retire pkMoveGenesisRetireSink,
	scopes []dataBranchRemovedOwnerScope, caller uint32,
) error {
	if len(scopes) == 0 {
		return nil
	}
	// D206(B.1) SETTER SITE (4) with D210(B)'s resolution: the DROP's own
	// background transaction runs every verdict branch (refresh-only keep,
	// retirement, residue keep), so mark it at reap-driver entry, BEFORE
	// the reap's first conflict-capable refresh/enrichment write.
	// Idempotent: a later fencing-scratch site (3) inside the same
	// transaction folds to a no-op. A nil observation FAILS CLOSED
	// (no mark, no write, error to caller).
	reapOp := resolveReapTxnOperator(bh)
	if reapOp == nil {
		return moerr.NewInternalErrorNoCtx(
			"pk-move reap mark: background txn operator absent")
	}
	client.MarkTxnExpectedConflictIfSupported(reapOp)
	accounts, err := classifyRemovedDataBranchOwnerAccounts(ctx, bh, scopes, caller)
	if err != nil {
		return err
	}
	for _, account := range accounts {
		// D155-C' scoped defect-flag seam (D186(B.5)(i)): harness-armed
		// for one NAMED account, it fails exactly that account's step as
		// the step begins — strictly after every earlier account's
		// certified reap work, with certification enforcement untouched
		// (the fault framework is never consulted for this seam). The
		// typed internal error aborts the drop's pessimistic transaction,
		// the same fail-atomic contract as the objectio seam below.
		if dataBranchOwnerRemovalReapSeamArmed.Load() {
			if armed, _ := dataBranchOwnerRemovalReapSeamAccount.Load().(string); armed != "" {
				matched := armed == fmt.Sprintf("%d", account)
				if !matched {
					var name string
					if name, err = lookupDataBranchAccountName(ctx, bh, account); err != nil {
						return &dataBranchReapAccountError{account: account, err: err}
					}
					matched = armed == name
				}
				if matched {
					return moerr.NewInternalErrorf(ctx,
						"data branch owner-removal reap seam: account %s step failed", armed)
				}
			}
		}
		// Test-only kill switch (D155-C regression): armed by
		// add_fault_point with the target account's NAME (or numeric id)
		// as sarg, it fails exactly that account's scan AFTER every
		// earlier account's verdicts issued inside the drop's pessimistic
		// transaction — the partial multi-account shape whose rollback and
		// retry the fail-atomic battery proves. Disarmed this is a single
		// fault-registry probe per account.
		if sarg, injected := objectio.CNDataBranchReapAccountFailedInjected(); injected && sarg != "" {
			matched := sarg == fmt.Sprintf("%d", account)
			if !matched {
				var name string
				if name, err = lookupDataBranchAccountName(ctx, bh, account); err != nil {
					return &dataBranchReapAccountError{account: account, err: err}
				}
				matched = sarg == name
			}
			if matched {
				return moerr.NewInternalErrorf(ctx,
					"injected pk-move link reap failure of account %s", sarg)
			}
		}
		if err := reapDataBranchLinksOfAccount(ctx, bh, sid, retire, account); err != nil {
			return &dataBranchReapAccountError{account: account, err: err}
		}
		// Ordered witness (D186(B.5)(ii)): the account's certified reap
		// step completed; fire the harness observer in step order.
		if ob, ok := dataBranchOwnerRemovalReapObserver.Load().(func(uint32)); ok && ob != nil {
			ob(account)
		}
	}
	return nil
}

// pkMoveRetentionLedgerRow aliases the one shared decode-contract row
// shape (databranchutils). Empty names are the first-class residue verdict
// (D154-D, owned by O1): identity was unresolvable at every KEEP verdict,
// so NAME-keyed coverage (database/table-scoped owners) can never decide
// it. The reap NEVER auto-deletes a residue row or its links: it is an
// explicit operator-visible class surfaced by WARN at every reap and
// discharged only by O1 maintenance or by the self-live refresh below
// healing the identity. Identity-bearing rows, by contrast, retire
// normally once no owner and no live descendant grounds them.
type pkMoveRetentionLedgerRow = databranchutils.PkMoveRetentionLedgerRow

func reapDataBranchLinksOfAccount(
	ctx context.Context, bh BackgroundExec, sid string,
	retire pkMoveGenesisRetireSink, accountID uint32,
) error {
	ledger, err := selectPkMoveRetentionLedgerRows(ctx, bh, accountID)
	if err != nil {
		return err
	}
	if len(ledger) == 0 {
		// Empty-ledger fast path: nothing this account retained can be
		// affected by any owner removal.
		return nil
	}

	// Owner rows for the coverage re-evaluation: the account's own
	// snapshots, sys-owned snapshots for non-sys accounts, and every
	// active mo_pitr row (sys-only catalog).
	localSnaps, err := selectDataBranchSnapshotOwnerRows(ctx, bh, accountID)
	if err != nil {
		return err
	}
	var sysSnaps []databranchutils.PkMoveSnapshotRow
	if accountID != catalog.System_Account {
		if sysSnaps, err = selectDataBranchSnapshotOwnerRows(ctx, bh, catalog.System_Account); err != nil {
			return err
		}
	}
	pitrRows, err := selectActiveDataBranchPitrOwnerRows(ctx, bh)
	if err != nil {
		return err
	}

	accountName := ""
	nameResolved := false
	uncovered := make([]pkMoveRetentionLedgerRow, 0, len(ledger))
	for _, row := range ledger {
		covered := databranchutils.PkMoveLocalSnapshotCoversTable(
			localSnaps, row.TableID, row.LogicalID, row.DatabaseName, row.TableName)
		if !covered {
			if !nameResolved &&
				(databranchutils.PkMoveSysSnapshotNeedsAccountName(sysSnaps, accountID) ||
					databranchutils.PkMovePitrNeedsAccountName(pitrRows, accountID)) {
				// Lazy single resolution per account; the id recorded at
				// creation is the invariant, the name a robustness extra.
				// Fail-atomic per D155-C: a lookup error aborts the drop.
				if accountName, err = lookupDataBranchAccountName(ctx, bh, accountID); err != nil {
					return err
				}
				nameResolved = true
			}
			covered = databranchutils.PkMoveSysSnapshotCoversTable(sysSnaps, accountID, accountName) ||
				databranchutils.PkMovePitrCoversTable(
					pitrRows, accountID, accountName, row.TableID, row.LogicalID, row.DatabaseName, row.TableName)
		}
		if !covered {
			uncovered = append(uncovered, row)
		}
	}
	if len(uncovered) == 0 {
		return nil
	}

	// One batched self-live probe (D159(D)), ordered after the coverage
	// miss and before the descendant walk: a table whose mo_tables row
	// still lives is its own surviving producer — no owner removal may
	// retire links a live table's own pending moves can still extend, so
	// the row KEEPS links and ledger row and is refreshed in place
	// (names + logical id; created_ts immutable).
	uncoveredIDs := make([]uint64, 0, len(uncovered))
	for _, row := range uncovered {
		uncoveredIDs = append(uncoveredIDs, row.TableID)
	}
	live, err := probePkMoveRetentionSelfLiveRows(ctx, bh, accountID, uncoveredIDs)
	if err != nil {
		return err
	}

	for _, row := range uncovered {
		if identity, ok := live[row.TableID]; ok {
			// fail-ALIVE on a concurrent drop: the refresh races the
			// table's own retirement and losing that race must neither
			// abort the reap nor retire fresh provenance; the next owner
			// removal re-evaluates the row.
			if rerr := refreshPkMoveRetentionLedgerRowUnder(
				ctx, bh, accountID, row.TableID, identity.logicalID, identity.db, identity.tbl,
			); rerr != nil {
				getLogger(sid).Warn(
					"DataBranch-PkMoveLink-RetentionSelfLiveRefreshFailed",
					zap.Uint32("account-id", accountID),
					zap.Uint64("table-id", row.TableID),
					zap.Error(rerr),
				)
			}
			continue
		}
		if row.DatabaseName == "" && row.TableName == "" {
			// D154-D identity-unresolved residue: NAME-keyed coverage
			// could never decide this row, so deleting it could erase
			// provenance a database- or table-scoped owner still
			// grounds. The reap NEVER auto-deletes the O1 residue
			// class; it keeps links and row and surfaces them here.
			getLogger(sid).Warn(
				"DataBranch-PkMoveLink-RetentionResidueKept",
				zap.Uint32("account-id", accountID),
				zap.Uint64("table-id", row.TableID),
			)
			continue
		}
		// Identity-bearing rows that no owner grounds still earn one
		// id-keyed chance: a live lineage descendant walks the same
		// DAG and keeps the links until the final branch retires
		// (D154-A ownership transfer).
		var hasLive bool
		if hasLive, err = dataBranchLineageHasLiveDescendant(ctx, bh, row.TableID); err != nil {
			return err
		}
		if hasLive {
			continue
		}
		getLogger(sid).Info(
			"DataBranch-PkMoveLink-OwnerRemovalReap",
			zap.Uint32("account-id", accountID),
			zap.Uint64("table-id", row.TableID),
		)
		// D168(B)/D176 retirement critical section: on a CN substrate the
		// paired deletes below run only after the fencing witness round is
		// accepted against this transaction's EXCLUSIVE ledger-row grant,
		// the causal frontier is installed, and the locked re-probe has
		// run. Every degrading class (test executors, non-pessimistic-RC
		// txns, pre-upgrade/legacy ledgers) runs the deletes as before.
		if _, _, err = runDataBranchFencingRetireForAddress(
			ctx, bh, sid, accountID, row.TableID); err != nil {
			return err
		}
		if err = deleteDataBranchUpdateLinksUnder(ctx, bh, accountID, row.TableID); err != nil {
			return err
		}
		if err = deletePkMoveRetentionLedgerRowUnder(ctx, bh, accountID, row.TableID); err != nil {
			return err
		}
		// D167 RETIRE: the paired deletes landed inside the drop's
		// pessimistic transaction, so any genesis epoch this transaction
		// holds for the address dies in the same journaled statement
		// segment (map entry + keep-lane membership); the next touch
		// re-probes as a NEW EPOCH first touch. A nil sink is the
		// map-miss no-op context the transition degenerates to.
		if retire != nil {
			retire.PkMoveRetireAddresses(accountID, []uint64{row.TableID})
		}
	}
	return nil
}

// selectPkMoveRetentionLedgerRows reads one account's retention ledger
// through the one shared decode contract (databranchutils): the wide
// projection first, pinned to the row_kind = 0 real-row namespace, and on
// the missing-column signal the legacy five-column projection with
// LogicalID assigned 0 — the verdict never outlives this read. A missing
// ledger catalog (pre-upgrade tenant) reads as empty — those tenants
// simply keep no ledger rows.
func selectPkMoveRetentionLedgerRows(
	ctx context.Context, bh BackgroundExec, accountID uint32,
) ([]pkMoveRetentionLedgerRow, error) {
	rows, err := queryPkMoveRetentionLedgerRows(ctx, bh, accountID, false)
	if err != nil && databranchutils.PkMoveLedgerMissingColumnErr(err) {
		rows, err = queryPkMoveRetentionLedgerRows(ctx, bh, accountID, true)
	}
	if err != nil {
		if moerr.IsMoErrCode(err, moerr.ErrNoSuchTable) {
			return nil, nil
		}
		return nil, err
	}
	return rows, nil
}

// queryPkMoveRetentionLedgerRows runs one ledger scan attempt; legacy=true
// drops the row_kind predicate and the logical_id projection.
func queryPkMoveRetentionLedgerRows(
	ctx context.Context, bh BackgroundExec, accountID uint32, legacy bool,
) ([]pkMoveRetentionLedgerRow, error) {
	sql := fmt.Sprintf(
		"select table_id, ifnull(database_name, ''), ifnull(table_name, ''), obj_id, created_ts, logical_id "+
			"from mo_catalog.%s where %s = %d order by table_id",
		catalog.MO_DATA_BRANCH_LINK_RETENTION,
		catalog.DataBranchLinkRetentionColRowKind, catalog.DataBranchLinkRetentionRowKindReal,
	)
	if legacy {
		sql = fmt.Sprintf(
			"select table_id, ifnull(database_name, ''), ifnull(table_name, ''), obj_id, created_ts "+
				"from mo_catalog.%s order by table_id",
			catalog.MO_DATA_BRANCH_LINK_RETENTION,
		)
	}
	actx := defines.AttachAccountId(ctx, accountID)
	bh.ClearExecResultSet()
	if err := bh.Exec(actx, sql); err != nil {
		return nil, err
	}
	erArray, err := getResultSet(actx, bh)
	if err != nil {
		return nil, err
	}
	var rows []pkMoveRetentionLedgerRow
	if execResultArrayHasData(erArray) {
		for i := uint64(0); i < erArray[0].GetRowCount(); i++ {
			var row pkMoveRetentionLedgerRow
			if row.TableID, err = erArray[0].GetUint64(ctx, i, 0); err != nil {
				return nil, err
			}
			if row.DatabaseName, err = erArray[0].GetString(ctx, i, 1); err != nil {
				return nil, err
			}
			if row.TableName, err = erArray[0].GetString(ctx, i, 2); err != nil {
				return nil, err
			}
			if row.ObjID, err = erArray[0].GetUint64(ctx, i, 3); err != nil {
				return nil, err
			}
			if row.CreatedTS, err = erArray[0].GetUint64(ctx, i, 4); err != nil {
				return nil, err
			}
			if !legacy {
				if row.LogicalID, err = erArray[0].GetUint64(ctx, i, 5); err != nil {
					return nil, err
				}
			}
			rows = append(rows, row)
		}
	}
	return rows, nil
}

// pkMoveRetentionSelfLiveIdentity is the live-catalog identity of one
// still-alive ledger address, resolved by the batched self-live probe.
type pkMoveRetentionSelfLiveIdentity struct {
	db        string
	tbl       string
	logicalID uint64
}

// probePkMoveRetentionSelfLiveRows resolves which probed addresses still
// have a live mo_tables row, in one batched chunked IN read per 512
// candidates (the descendant-walk page bound): a live table is its own
// surviving producer, so its ledger row and links KEEP across any owner
// removal (D159(D)). The durable rel_logical_id rides the same read (fresh
// creates default logical to physical).
func probePkMoveRetentionSelfLiveRows(
	ctx context.Context, bh BackgroundExec, accountID uint32, ids []uint64,
) (map[uint64]pkMoveRetentionSelfLiveIdentity, error) {
	live := make(map[uint64]pkMoveRetentionSelfLiveIdentity, len(ids))
	if len(ids) == 0 {
		return live, nil
	}
	actx := defines.AttachAccountId(ctx, accountID)
	const chunk = 512
	for start := 0; start < len(ids); start += chunk {
		end := min(start+chunk, len(ids))
		list := make([]string, 0, end-start)
		for _, id := range ids[start:end] {
			list = append(list, fmt.Sprintf("%d", id))
		}
		bh.ClearExecResultSet()
		if err := bh.Exec(actx, fmt.Sprintf(
			"select rel_id, reldatabase, relname, coalesce(nullif(rel_logical_id,0),rel_id) "+
				"from mo_catalog.mo_tables where rel_id in (%s)",
			strings.Join(list, ","),
		)); err != nil {
			if moerr.IsMoErrCode(err, moerr.ErrNoSuchTable) {
				return live, nil
			}
			return nil, err
		}
		erArray, err := getResultSet(actx, bh)
		if err != nil {
			return nil, err
		}
		if !execResultArrayHasData(erArray) {
			continue
		}
		for i := uint64(0); i < erArray[0].GetRowCount(); i++ {
			var id uint64
			if id, err = erArray[0].GetUint64(ctx, i, 0); err != nil {
				return nil, err
			}
			identity := pkMoveRetentionSelfLiveIdentity{}
			if identity.db, err = erArray[0].GetString(ctx, i, 1); err != nil {
				return nil, err
			}
			if identity.tbl, err = erArray[0].GetString(ctx, i, 2); err != nil {
				return nil, err
			}
			if identity.logicalID, err = erArray[0].GetUint64(ctx, i, 3); err != nil {
				return nil, err
			}
			live[id] = identity
		}
	}
	return live, nil
}

// refreshPkMoveRetentionLedgerRowUnder refreshes a kept self-live ledger
// row in place: names and the durable logical id, never created_ts (first
// registration instant is immutable). The update pins row_kind = 0 and
// degrades to the names-only legacy shape on the missing-column signal.
func refreshPkMoveRetentionLedgerRowUnder(
	ctx context.Context, bh BackgroundExec, accountID uint32, tableID, logicalID uint64, dbName, tblName string,
) error {
	actx := defines.AttachAccountId(ctx, accountID)
	bh.ClearExecResultSet()
	err := bh.Exec(actx, fmt.Sprintf(
		"update mo_catalog.%s set %s = '%s', %s = '%s', %s = %d where %s = %d and %s = %d",
		catalog.MO_DATA_BRANCH_LINK_RETENTION,
		catalog.DataBranchLinkRetentionColDatabaseName, sqlquote.EscapeString(dbName),
		catalog.DataBranchLinkRetentionColTableName, sqlquote.EscapeString(tblName),
		catalog.DataBranchLinkRetentionColLogicalID, logicalID,
		catalog.DataBranchLinkRetentionColRowKind, catalog.DataBranchLinkRetentionRowKindReal,
		catalog.DataBranchLinkRetentionColTableID, tableID,
	))
	if err == nil {
		return nil
	}
	if moerr.IsMoErrCode(err, moerr.ErrNoSuchTable) {
		return nil
	}
	if !databranchutils.PkMoveLedgerMissingColumnErr(err) {
		return err
	}
	bh.ClearExecResultSet()
	err = bh.Exec(actx, fmt.Sprintf(
		"update mo_catalog.%s set %s = '%s', %s = '%s' where %s = %d",
		catalog.MO_DATA_BRANCH_LINK_RETENTION,
		catalog.DataBranchLinkRetentionColDatabaseName, sqlquote.EscapeString(dbName),
		catalog.DataBranchLinkRetentionColTableName, sqlquote.EscapeString(tblName),
		catalog.DataBranchLinkRetentionColTableID, tableID,
	))
	if err != nil && moerr.IsMoErrCode(err, moerr.ErrNoSuchTable) {
		return nil
	}
	return err
}

// selectDataBranchSnapshotOwnerRows reads mo_snapshots owner rows
// (kind != 'branch') of the given account's catalog. Branch-protect
// snapshots retire with the lineage metadata the descendant probe covers,
// so they are never owner coverage.
func selectDataBranchSnapshotOwnerRows(
	ctx context.Context, bh BackgroundExec, accountID uint32,
) ([]databranchutils.PkMoveSnapshotRow, error) {
	sql := "select level, ifnull(account_name, ''), ifnull(database_name, ''), " +
		"ifnull(table_name, ''), ifnull(obj_id, 0) " +
		"from mo_catalog.mo_snapshots where kind != 'branch'"
	actx := defines.AttachAccountId(ctx, accountID)
	bh.ClearExecResultSet()
	if err := bh.Exec(actx, sql); err != nil {
		if moerr.IsMoErrCode(err, moerr.ErrNoSuchTable) {
			return nil, nil
		}
		return nil, err
	}
	erArray, err := getResultSet(actx, bh)
	if err != nil {
		return nil, err
	}
	var rows []databranchutils.PkMoveSnapshotRow
	if execResultArrayHasData(erArray) {
		for i := uint64(0); i < erArray[0].GetRowCount(); i++ {
			var row databranchutils.PkMoveSnapshotRow
			if row.Level, err = erArray[0].GetString(ctx, i, 0); err != nil {
				return nil, err
			}
			if row.AccountName, err = erArray[0].GetString(ctx, i, 1); err != nil {
				return nil, err
			}
			if row.DatabaseName, err = erArray[0].GetString(ctx, i, 2); err != nil {
				return nil, err
			}
			if row.TableName, err = erArray[0].GetString(ctx, i, 3); err != nil {
				return nil, err
			}
			if row.ObjId, err = erArray[0].GetUint64(ctx, i, 4); err != nil {
				return nil, err
			}
			rows = append(rows, row)
		}
	}
	return rows, nil
}

// selectActiveDataBranchPitrOwnerRows reads every ACTIVE mo_pitr row
// (pitr_status = 1); the catalog lives only under SYS.
func selectActiveDataBranchPitrOwnerRows(
	ctx context.Context, bh BackgroundExec,
) ([]databranchutils.PkMovePitrRow, error) {
	sql := fmt.Sprintf(
		"select level, ifnull(account_id, 0), ifnull(account_name, ''), "+
			"ifnull(database_name, ''), ifnull(table_name, ''), ifnull(obj_id, 0) "+
			"from mo_catalog.%s where %s = 1",
		catalog.MO_PITR, catalog.MO_PITR_STATUS,
	)
	actx := defines.AttachAccountId(ctx, catalog.System_Account)
	bh.ClearExecResultSet()
	if err := bh.Exec(actx, sql); err != nil {
		if moerr.IsMoErrCode(err, moerr.ErrNoSuchTable) {
			return nil, nil
		}
		return nil, err
	}
	erArray, err := getResultSet(actx, bh)
	if err != nil {
		return nil, err
	}
	var rows []databranchutils.PkMovePitrRow
	if execResultArrayHasData(erArray) {
		for i := uint64(0); i < erArray[0].GetRowCount(); i++ {
			var row databranchutils.PkMovePitrRow
			if row.Level, err = erArray[0].GetString(ctx, i, 0); err != nil {
				return nil, err
			}
			if row.AccountId, err = erArray[0].GetUint64(ctx, i, 1); err != nil {
				return nil, err
			}
			if row.AccountName, err = erArray[0].GetString(ctx, i, 2); err != nil {
				return nil, err
			}
			if row.DatabaseName, err = erArray[0].GetString(ctx, i, 3); err != nil {
				return nil, err
			}
			if row.TableName, err = erArray[0].GetString(ctx, i, 4); err != nil {
				return nil, err
			}
			if row.ObjId, err = erArray[0].GetUint64(ctx, i, 5); err != nil {
				return nil, err
			}
			rows = append(rows, row)
		}
	}
	return rows, nil
}

// dataBranchLineageHasLiveDescendant mirrors the compile-side liveness
// probe for the frontend reap: mo_branch_metadata is a sys-global catalog,
// and a retained table's links survive while any transitive branch/clone
// descendant lives — diffs and merges through that descendant still
// resolve moves recorded on the retained table. The walk counts
// non-deleted children per node so no boolean column is ever decoded, is
// row-bounded, and fails ALIVE on fan-out pathologies (reclaim is
// permanent, so a pathological lineage keeps its links); query errors
// propagate to the fail-atomic caller (D155-C).
func dataBranchLineageHasLiveDescendant(
	ctx context.Context, bh BackgroundExec, tableID uint64,
) (bool, error) {
	const maxVisited = 512
	visited := map[uint64]struct{}{tableID: {}}
	queue := []uint64{tableID}
	actx := defines.AttachAccountId(ctx, catalog.System_Account)
	for len(queue) > 0 {
		if len(visited) > maxVisited {
			return true, nil
		}
		parent := queue[0]
		queue = queue[1:]
		bh.ClearExecResultSet()
		if err := bh.Exec(actx, fmt.Sprintf(
			"select count(table_id), ifnull(max(table_id), 0) from %s.%s where p_table_id = %d and table_deleted = false",
			catalog.MO_CATALOG, catalog.MO_BRANCH_METADATA, parent,
		)); err != nil {
			return false, err
		}
		erArray, err := getResultSet(actx, bh)
		if err != nil {
			return false, err
		}
		if !execResultArrayHasData(erArray) {
			continue
		}
		liveCount, err := erArray[0].GetUint64(ctx, 0, 0)
		if err != nil {
			return false, err
		}
		if liveCount > 0 {
			return true, nil
		}
		// No live children at this node: expand through deleted
		// intermediate generations — a dropped branch still names its
		// live children as lineage parents.
		bh.ClearExecResultSet()
		if err = bh.Exec(actx, fmt.Sprintf(
			"select table_id from %s.%s where p_table_id = %d",
			catalog.MO_CATALOG, catalog.MO_BRANCH_METADATA, parent,
		)); err != nil {
			return false, err
		}
		if erArray, err = getResultSet(actx, bh); err != nil {
			return false, err
		}
		if !execResultArrayHasData(erArray) {
			continue
		}
		for i := uint64(0); i < erArray[0].GetRowCount(); i++ {
			child, err := erArray[0].GetUint64(ctx, i, 0)
			if err != nil {
				return false, err
			}
			if _, ok := visited[child]; ok {
				continue
			}
			visited[child] = struct{}{}
			queue = append(queue, child)
		}
	}
	return false, nil
}

// selectMoAccountIDs enumerates every tenant account id under SYS; a
// cluster-level owner covers them all.
func selectMoAccountIDs(ctx context.Context, bh BackgroundExec) ([]uint32, error) {
	actx := defines.AttachAccountId(ctx, catalog.System_Account)
	bh.ClearExecResultSet()
	if err := bh.Exec(actx, "select account_id from mo_catalog.mo_account order by account_id"); err != nil {
		return nil, err
	}
	erArray, err := getResultSet(actx, bh)
	if err != nil {
		return nil, err
	}
	var ids []uint32
	if execResultArrayHasData(erArray) {
		for i := uint64(0); i < erArray[0].GetRowCount(); i++ {
			id, err := erArray[0].GetUint64(ctx, i, 0)
			if err != nil {
				return nil, err
			}
			ids = append(ids, uint32(id))
		}
	}
	return ids, nil
}

// moAccountExists reports whether the account is still alive in
// mo_account; account-level owner targets that vanished (the catalogs died
// with the account) are skipped by classification instead of failing the
// drop on a phantom scan.
func moAccountExists(ctx context.Context, bh BackgroundExec, accountID uint32) (bool, error) {
	actx := defines.AttachAccountId(ctx, catalog.System_Account)
	bh.ClearExecResultSet()
	if err := bh.Exec(actx, fmt.Sprintf(
		"select 1 from mo_catalog.mo_account where account_id = %d", accountID,
	)); err != nil {
		return false, err
	}
	erArray, err := getResultSet(actx, bh)
	if err != nil {
		return false, err
	}
	return execResultArrayHasData(erArray), nil
}

// lookupMoAccountIDByName resolves a legacy name-keyed account-level owner
// row; a miss reports id 0 (the account is gone) while any error aborts
// the classification atomically.
func lookupMoAccountIDByName(ctx context.Context, bh BackgroundExec, accountName string) (uint64, error) {
	actx := defines.AttachAccountId(ctx, catalog.System_Account)
	bh.ClearExecResultSet()
	if err := bh.Exec(actx, fmt.Sprintf(
		"select account_id from mo_catalog.mo_account where account_name = '%s'",
		accountName,
	)); err != nil {
		return 0, err
	}
	erArray, err := getResultSet(actx, bh)
	if err != nil {
		return 0, err
	}
	if !execResultArrayHasData(erArray) {
		return 0, nil
	}
	return erArray[0].GetUint64(ctx, 0, 0)
}

// lookupDataBranchAccountName resolves the probing tenant's name for the
// lazy name-fallback half of the sys-owned owner predicates.
func lookupDataBranchAccountName(ctx context.Context, bh BackgroundExec, accountID uint32) (string, error) {
	actx := defines.AttachAccountId(ctx, catalog.System_Account)
	bh.ClearExecResultSet()
	if err := bh.Exec(actx, fmt.Sprintf(
		"select account_name from mo_catalog.mo_account where account_id = %d", accountID,
	)); err != nil {
		return "", err
	}
	erArray, err := getResultSet(actx, bh)
	if err != nil {
		return "", err
	}
	if !execResultArrayHasData(erArray) {
		return "", nil
	}
	return erArray[0].GetString(ctx, 0, 0)
}

// deleteDataBranchUpdateLinksUnder retires one table's retained pk-move
// links in the account's own catalog, riding the same link_id half-open
// primary-key range the compile path uses. A missing catalog is tolerated.
func deleteDataBranchUpdateLinksUnder(
	ctx context.Context, bh BackgroundExec, accountID uint32, tableID uint64,
) error {
	if tableID+1 == 0 {
		return nil
	}
	actx := defines.AttachAccountId(ctx, accountID)
	bh.ClearExecResultSet()
	err := bh.Exec(actx, fmt.Sprintf(
		"delete from mo_catalog.%s where %s >= x'%016x' and %s < x'%016x'",
		catalog.MO_DATA_BRANCH_UPDATE_LINK,
		catalog.DataBranchUpdateLinkColLinkID, tableID,
		catalog.DataBranchUpdateLinkColLinkID, tableID+1,
	))
	if err != nil && moerr.IsMoErrCode(err, moerr.ErrNoSuchTable) {
		return nil
	}
	return err
}

// deletePkMoveRetentionLedgerRowUnder removes one account's ledger row for
// a table the reap just retired; ownership transfer says ledger rows die
// only with their links or the account. The delete pins row_kind = 0 so
// fencing scratch rows of any id stay untouched, and degrades to the
// legacy shape when the column does not exist.
func deletePkMoveRetentionLedgerRowUnder(
	ctx context.Context, bh BackgroundExec, accountID uint32, tableID uint64,
) error {
	actx := defines.AttachAccountId(ctx, accountID)
	bh.ClearExecResultSet()
	err := bh.Exec(actx, fmt.Sprintf(
		"delete from mo_catalog.%s where %s = %d and %s = %d",
		catalog.MO_DATA_BRANCH_LINK_RETENTION,
		catalog.DataBranchLinkRetentionColRowKind, catalog.DataBranchLinkRetentionRowKindReal,
		catalog.DataBranchLinkRetentionColTableID, tableID,
	))
	if err != nil {
		if moerr.IsMoErrCode(err, moerr.ErrNoSuchTable) {
			return nil
		}
		if !databranchutils.PkMoveLedgerMissingColumnErr(err) {
			return err
		}
		bh.ClearExecResultSet()
		err = bh.Exec(actx, fmt.Sprintf(
			"delete from mo_catalog.%s where %s = %d",
			catalog.MO_DATA_BRANCH_LINK_RETENTION,
			catalog.DataBranchLinkRetentionColTableID, tableID,
		))
		if err != nil && moerr.IsMoErrCode(err, moerr.ErrNoSuchTable) {
			return nil
		}
	}
	return err
}

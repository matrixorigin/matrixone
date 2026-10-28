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
	"fmt"
	"strings"
	"sync/atomic"
	"time"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/sqlquote"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/frontend/databranchutils"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae"
	"go.uber.org/zap"
)

// This file holds the compile-side helpers of the pk-move link retention
// ledger (mo_catalog.mo_data_branch_link_retention). Drop-lifecycle funnel
// KEEP verdicts persist one keyed, retry-idempotent row with the retained
// table's identity, inside the same dropping transaction; the frontend
// owner-removal reap later scans the ledger to rediscover retained dead
// tables. Ledger writes mirror POST-mutation keep verdicts — they never
// re-derive coverage, and a write failure never wedges a table drop
// (funnel-side fail-ALIVE), it only degrades the ledger to the WARN-visible
// under-report the O1 maintenance question owns. Tenants predating the
// catalog simply keep no rows.
//
// Capability is decided at every contact (no persistent cache): writers
// and readers attempt the current wide shape first and degrade to the
// legacy pre-upgrade shape on the missing-column signal, so a heal ALTER
// or a tenant upgrade takes effect on the very next contact in the same
// process and per-tenant schema generations coexist on one CN. Every
// statement pins row_kind = 0 on the wide shape so fencing scratch rows
// stay unreachable to the funnel/genesis contacts; the legacy shape drops
// the predicate because a catalog old enough to lack row_kind predates
// the fencing protocol.

// pkMoveRetentionLegacyWarnOnce bounds the legacy-shape degradation to one
// WARN per process (D160(D)).
var pkMoveRetentionLegacyWarnOnce atomic.Bool

// pkMoveRetentionLegacyShapeWarn logs the once-per-process legacy-shape
// degradation notice.
func pkMoveRetentionLegacyShapeWarn(tableID uint64) {
	if pkMoveRetentionLegacyWarnOnce.CompareAndSwap(false, true) {
		logutil.Warn(
			"DataBranch-PkMoveLink-RetentionLedgerLegacyShape",
			zap.Uint64("table-id", tableID),
		)
	}
}

// lookupPkMoveLinkRetention fetches the retained identity of tableID, if
// the ledger knows it. A missing catalog reports not-found (pre-upgrade
// tenants), a missing-column verdict retries the legacy projection with
// logicalID 0, and any other error is returned to the caller, which fails
// ALIVE onto keeping links.
func lookupPkMoveLinkRetention(
	c *Compile, accountID uint32, tableID uint64,
) (dbName, tblName string, logicalID uint64, found bool, err error) {
	dbName, tblName, logicalID, found, err = queryPkMoveLinkRetention(c, accountID, tableID, false)
	if err != nil && databranchutils.PkMoveLedgerMissingColumnErr(err) {
		dbName, tblName, logicalID, found, err = queryPkMoveLinkRetention(c, accountID, tableID, true)
	}
	if err != nil {
		if moerr.IsMoErrCode(err, moerr.ErrNoSuchTable) {
			return "", "", 0, false, nil
		}
		return "", "", 0, false, err
	}
	return dbName, tblName, logicalID, found, nil
}

// queryPkMoveLinkRetention runs one keyed lookup attempt; legacy=true
// drops the row_kind predicate and the logical_id projection.
func queryPkMoveLinkRetention(
	c *Compile, accountID uint32, tableID uint64, legacy bool,
) (dbName, tblName string, logicalID uint64, found bool, err error) {
	var stmt string
	if legacy {
		stmt = fmt.Sprintf(
			"select %s, %s from %s.%s where %s = %d",
			catalog.DataBranchLinkRetentionColDatabaseName,
			catalog.DataBranchLinkRetentionColTableName,
			catalog.MO_CATALOG, catalog.MO_DATA_BRANCH_LINK_RETENTION,
			catalog.DataBranchLinkRetentionColTableID, tableID,
		)
	} else {
		stmt = fmt.Sprintf(
			"select %s, %s, %s from %s.%s where %s = %d and %s = %d",
			catalog.DataBranchLinkRetentionColDatabaseName,
			catalog.DataBranchLinkRetentionColTableName,
			catalog.DataBranchLinkRetentionColLogicalID,
			catalog.MO_CATALOG, catalog.MO_DATA_BRANCH_LINK_RETENTION,
			catalog.DataBranchLinkRetentionColRowKind, catalog.DataBranchLinkRetentionRowKindReal,
			catalog.DataBranchLinkRetentionColTableID, tableID,
		)
	}
	res, err := c.runSqlWithResultAndOptions(
		stmt,
		int32(accountID),
		executor.StatementOption{}.WithDisableLog(),
	)
	if err != nil {
		return "", "", 0, false, err
	}
	defer res.Close()
	res.ReadRows(func(n int, cols []*vector.Vector) bool {
		if n > 0 && !found {
			found = true
			if !cols[0].IsNull(0) {
				dbName = string(cols[0].GetBytesAt(0))
			}
			if !cols[1].IsNull(0) {
				tblName = string(cols[1].GetBytesAt(0))
			}
			if !legacy && !cols[2].IsNull(0) {
				if v, castErr := pkMoveRetentionIDCell(cols[2], 0); castErr == nil {
					logicalID = v
				}
			}
		}
		return true
	})
	return dbName, tblName, logicalID, found, nil
}

// insertPkMoveLinkRetention persists one keyed, retry-idempotent ledger row
// for a retained dead table. First write wins; a later verdict carrying real
// names enriches an existing empty-name residue row (the identity capture
// that should have won the race), and a later contact holding the member's
// logical id backfills a stored 0 marker while the catalog row stays
// readable. created_ts is immutable on refresh. Errors are logged and
// swallowed: the keep verdict already committed logically, and dropping the
// write must not wedge the table drop.
func insertPkMoveLinkRetention(
	c *Compile, accountID uint32, tableID, logicalID uint64, dbName, tblName string,
) {
	// D206(B.1)/D211(B.2) SETTER SITE (5): the KEEP funnel's enrichment and
	// insert writes ride the drop statement's OWN transaction; one
	// idempotent mark at the funnel's entry covers all nine KEEP call
	// sites and every KEEP write in one drop. Deliberately BEFORE the
	// first enrichment/insert write. A nil observation fails closed in the
	// funnel's own keep shape: no mark, no write (the fail-ALIVE posture
	// and legacy degradations below are untouched).
	keepOp := c.proc.GetTxnOperator()
	if keepOp == nil {
		logutil.Warn(
			"DataBranch-PkMoveLink-RetentionMarkSkippedNoTxn",
			zap.Uint64("table-id", tableID),
		)
		return
	}
	client.MarkTxnExpectedConflictIfSupported(keepOp)
	oldDb, oldTbl, oldLogical, found, err := lookupPkMoveLinkRetention(c, accountID, tableID)
	if err != nil {
		logutil.Warn(
			"DataBranch-PkMoveLink-RetentionLedgerProbeFailed",
			zap.Uint64("table-id", tableID),
			zap.Error(err),
		)
		return
	}
	if found {
		setClauses := make([]string, 0, 3)
		if oldDb == "" && oldTbl == "" && dbName != "" && tblName != "" {
			setClauses = append(
				setClauses,
				fmt.Sprintf("%s = '%s'", catalog.DataBranchLinkRetentionColDatabaseName, sqlquote.EscapeString(dbName)),
				fmt.Sprintf("%s = '%s'", catalog.DataBranchLinkRetentionColTableName, sqlquote.EscapeString(tblName)),
			)
		}
		if oldLogical == 0 && logicalID != 0 {
			setClauses = append(
				setClauses,
				fmt.Sprintf("%s = %d", catalog.DataBranchLinkRetentionColLogicalID, logicalID),
			)
		}
		if len(setClauses) == 0 {
			return
		}
		refreshed := false
		stmt := fmt.Sprintf(
			"update %s.%s set %s where %s = %d and %s = %d",
			catalog.MO_CATALOG, catalog.MO_DATA_BRANCH_LINK_RETENTION,
			strings.Join(setClauses, ", "),
			catalog.DataBranchLinkRetentionColRowKind, catalog.DataBranchLinkRetentionRowKindReal,
			catalog.DataBranchLinkRetentionColTableID, tableID,
		)
		if uerr := c.runSqlWithAccountId(stmt, int32(accountID)); uerr != nil {
			switch {
			case moerr.IsMoErrCode(uerr, moerr.ErrNoSuchTable):
			case databranchutils.PkMoveLedgerMissingColumnErr(uerr):
				// Legacy catalog: row_kind/logical_id do not exist; retry
				// the names-only enrichment without either column.
				if oldDb == "" && oldTbl == "" && dbName != "" && tblName != "" {
					legacy := fmt.Sprintf(
						"update %s.%s set %s = '%s', %s = '%s' where %s = %d",
						catalog.MO_CATALOG, catalog.MO_DATA_BRANCH_LINK_RETENTION,
						catalog.DataBranchLinkRetentionColDatabaseName, sqlquote.EscapeString(dbName),
						catalog.DataBranchLinkRetentionColTableName, sqlquote.EscapeString(tblName),
						catalog.DataBranchLinkRetentionColTableID, tableID,
					)
					pkMoveRetentionLegacyShapeWarn(tableID)
					if lerr := c.runSqlWithAccountId(legacy, int32(accountID)); lerr != nil &&
						!moerr.IsMoErrCode(lerr, moerr.ErrNoSuchTable) {
						logutil.Warn(
							"DataBranch-PkMoveLink-RetentionLedgerEnrichFailed",
							zap.Uint64("table-id", tableID),
							zap.Error(lerr),
						)
					} else if lerr == nil {
						refreshed = true
					}
				}
			default:
				logutil.Warn(
					"DataBranch-PkMoveLink-RetentionLedgerEnrichFailed",
					zap.Uint64("table-id", tableID),
					zap.Error(uerr),
				)
			}
		} else {
			refreshed = true
		}
		if refreshed {
			// D169 REFRESH: the enrichment just re-staged this address's
			// retention identity as this transaction's own delete+insert;
			// if the transaction tracks the address (OWNED/EXTERNAL epoch),
			// the ownership transfer must be journaled in this statement's
			// segment so cancellation, rollback, and the forced-dump keep
			// lane follow the transferred entry. Untracked addresses stay
			// plain unowned funnel upserts (uniform no-op).
			if ws := pkMoveGenesisTxnWorkspace(c); ws != nil {
				ws.PkMoveNoteGenesisRefresh(accountID, []uint64{tableID})
			}
		}
		return
	}
	stmt := fmt.Sprintf(
		"insert into %s.%s(%s, %s, %s, %s, %s, %s, %s) values(%d, %d, '%s', '%s', %d, %d, %d)",
		catalog.MO_CATALOG, catalog.MO_DATA_BRANCH_LINK_RETENTION,
		catalog.DataBranchLinkRetentionColRowKind,
		catalog.DataBranchLinkRetentionColTableID,
		catalog.DataBranchLinkRetentionColDatabaseName,
		catalog.DataBranchLinkRetentionColTableName,
		catalog.DataBranchLinkRetentionColObjID,
		catalog.DataBranchLinkRetentionColCreatedTS,
		catalog.DataBranchLinkRetentionColLogicalID,
		catalog.DataBranchLinkRetentionRowKindReal,
		tableID, sqlquote.EscapeString(dbName), sqlquote.EscapeString(tblName),
		tableID, time.Now().UnixNano(), logicalID,
	)
	if ierr := c.runSqlWithAccountId(stmt, int32(accountID)); ierr != nil {
		switch {
		case moerr.IsMoErrCode(ierr, moerr.ErrNoSuchTable):
			logutil.Warn(
				"DataBranch-PkMoveLink-RetentionLedgerCatalogMissing",
				zap.Uint64("table-id", tableID),
			)
		case databranchutils.PkMoveLedgerMissingColumnErr(ierr):
			legacy := fmt.Sprintf(
				"insert into %s.%s(%s, %s, %s, %s, %s) values(%d, '%s', '%s', %d, %d)",
				catalog.MO_CATALOG, catalog.MO_DATA_BRANCH_LINK_RETENTION,
				catalog.DataBranchLinkRetentionColTableID,
				catalog.DataBranchLinkRetentionColDatabaseName,
				catalog.DataBranchLinkRetentionColTableName,
				catalog.DataBranchLinkRetentionColObjID,
				catalog.DataBranchLinkRetentionColCreatedTS,
				tableID, sqlquote.EscapeString(dbName), sqlquote.EscapeString(tblName),
				tableID, time.Now().UnixNano(),
			)
			pkMoveRetentionLegacyShapeWarn(tableID)
			if lerr := c.runSqlWithAccountId(legacy, int32(accountID)); lerr != nil &&
				!moerr.IsMoErrCode(lerr, moerr.ErrNoSuchTable) {
				logutil.Warn(
					"DataBranch-PkMoveLink-RetentionLedgerWriteFailed",
					zap.Uint64("table-id", tableID),
					zap.Error(lerr),
				)
			}
		default:
			logutil.Warn(
				"DataBranch-PkMoveLink-RetentionLedgerWriteFailed",
				zap.Uint64("table-id", tableID),
				zap.Error(ierr),
			)
		}
	}
}

// pkMoveRetentionLegacyDeleteForced is the D155-C-family legacy-arm
// fixture (D212(C.3)): it forces the names-only retirement DELETE of a
// legacy catalog (:314-322's shape) in a LIVE multi-service schedule
// without touching the wide catalog. Harness-only: never enabled on any
// production path, consulted only at this one site.
var pkMoveRetentionLegacyDeleteForced atomic.Bool

// ForcePkMoveRetentionLegacyDeleteForTest flips the legacy DELETE fixture
// and returns a restore func; harness-only.
func ForcePkMoveRetentionLegacyDeleteForTest(forced bool) (restore func()) {
	prev := pkMoveRetentionLegacyDeleteForced.Swap(forced)
	return func() { pkMoveRetentionLegacyDeleteForced.Store(prev) }
}

// deletePkMoveLinkRetention removes tableID's ledger row. Ownership
// transfer: every flow that deletes a table's retained links also removes
// the ledger row that named them, so ledger rows die only with their links
// or the account. The delete pins row_kind = 0 so fencing scratch rows of
// any id stay untouched. A missing catalog retries the legacy shape,
// and a missing row is not an error.
func deletePkMoveLinkRetention(c *Compile, accountID uint32, tableID uint64) error {
	if pkMoveRetentionLegacyDeleteForced.Load() {
		// D212(C.3) harness fixture: the legacy names-only shape verbatim.
		legacy := fmt.Sprintf(
			"delete from %s.%s where %s = %d",
			catalog.MO_CATALOG, catalog.MO_DATA_BRANCH_LINK_RETENTION,
			catalog.DataBranchLinkRetentionColTableID, tableID,
		)
		err := c.runSqlWithAccountId(legacy, int32(accountID))
		if err != nil && moerr.IsMoErrCode(err, moerr.ErrNoSuchTable) {
			return nil
		}
		return err
	}
	stmt := fmt.Sprintf(
		"delete from %s.%s where %s = %d and %s = %d",
		catalog.MO_CATALOG, catalog.MO_DATA_BRANCH_LINK_RETENTION,
		catalog.DataBranchLinkRetentionColRowKind, catalog.DataBranchLinkRetentionRowKindReal,
		catalog.DataBranchLinkRetentionColTableID, tableID,
	)
	err := c.runSqlWithAccountId(stmt, int32(accountID))
	if err != nil {
		if moerr.IsMoErrCode(err, moerr.ErrNoSuchTable) {
			return nil
		}
		if databranchutils.PkMoveLedgerMissingColumnErr(err) {
			legacy := fmt.Sprintf(
				"delete from %s.%s where %s = %d",
				catalog.MO_CATALOG, catalog.MO_DATA_BRANCH_LINK_RETENTION,
				catalog.DataBranchLinkRetentionColTableID, tableID,
			)
			err = c.runSqlWithAccountId(legacy, int32(accountID))
			if err != nil && moerr.IsMoErrCode(err, moerr.ErrNoSuchTable) {
				return nil
			}
		}
	}
	return err
}

// deletePkMoveLinkAndRetention retires a table's retained pk-move links
// together with the retention-ledger row that tracks them.
func deletePkMoveLinkAndRetention(c *Compile, accountID uint32, tableID uint64) error {
	// D206(B.1)/D212(B.3) SETTER SITE (6): the retiring statement's OWN
	// transaction carries the fenced grant-acquisition surface and both
	// skip-path DELETE surfaces for all six supported callers; one
	// idempotent mark at the funnel's entry - BEFORE the fencing CS, so no
	// guard or degrade arm escapes it - is idempotent against SITE (5) and
	// across the multi-id loop. A nil observation FAILS CLOSED (no mark,
	// no write, error to caller) per D210(B).
	retireOp := c.proc.GetTxnOperator()
	if retireOp == nil {
		return moerr.NewInternalErrorNoCtx(
			"pk-move retirement mark: txn operator absent")
	}
	client.MarkTxnExpectedConflictIfSupported(retireOp)
	// D168(B)/D176 retirement critical section: with a CN substrate the
	// paired deletes below execute only after the fencing witness round's
	// record (C_f, C_f1, B_f) is accepted against the retiring
	// transaction's EXCLUSIVE ledger-row grant (B_r == B_f, fence >= C_f),
	// the causal frontier is installed, and the locked re-probe has run.
	// Mock-proc and non-CN contexts degrade to the plain paired deletes
	// (done=false), as do pre-upgrade tenants without the wide ledger.
	done, _, err := runDataBranchPkMoveFencingRetire(c, accountID, tableID)
	if err != nil {
		return err
	}
	_ = done
	if err := deleteDataBranchUpdateLinks(c, accountID, tableID); err != nil {
		return err
	}
	if err := deletePkMoveLinkRetention(c, accountID, tableID); err != nil {
		return err
	}
	// D167 RETIRE: the paired deletes ran in this statement, so the genesis
	// epoch (OWNED or EXTERNAL) this transaction may hold for the address
	// must die in the same journaled segment — the staged identity row dies
	// with the positional workspace cut, the map entry is removed, and the
	// next touch re-probes as a NEW EPOCH first touch. Transaction rollback
	// restores paired delete, prior map state, and membership from the same
	// segment. Transactions that never tracked the address (background
	// reaps, ordinary drops) hit the uniform map-miss no-op.
	if ws := pkMoveGenesisTxnWorkspace(c); ws != nil {
		ws.PkMoveRetireAddresses(accountID, []uint64{tableID})
	}
	return nil
}

// pkMoveGenesisTxnWorkspace resolves the caller transaction's disttae
// workspace for the genesis journal transitions (D167 RETIRE, D169
// REFRESH). Contexts without a *disttae.Transaction workspace (mock procs,
// non-CN storage engines) degrade to the transitions' uniform no-op.
func pkMoveGenesisTxnWorkspace(c *Compile) *disttae.Transaction {
	if c == nil || c.proc == nil {
		return nil
	}
	txnOp := c.proc.GetTxnOperator()
	if txnOp == nil {
		return nil
	}
	ws, _ := txnOp.GetWorkspace().(*disttae.Transaction)
	return ws
}

// pkMoveRetentionIDCell defensively casts one ledger id cell to uint64,
// tolerating the int64 shape pre-upgrade decoders produced for the same
// unsigned catalog columns (D149).
func pkMoveRetentionIDCell(col *vector.Vector, i int) (uint64, error) {
	switch col.GetType().Oid {
	case types.T_uint64:
		return vector.MustFixedColNoTypeCheck[uint64](col)[i], nil
	case types.T_int64:
		return uint64(vector.MustFixedColNoTypeCheck[int64](col)[i]), nil
	default:
		return 0, moerr.NewInternalErrorNoCtx("invalid pk-move retention ledger id column")
	}
}

// pkMoveLinkPresenceProbe reports whether the link catalog still holds any
// row for tableID, riding the same link_id primary-key half-open range the
// delete path uses. A missing catalog reports absent (nothing to retain).
func pkMoveLinkPresenceProbe(c *Compile, accountID uint32, tblID uint64) (bool, error) {
	if tblID+1 == 0 {
		return false, nil
	}
	res, err := c.runSqlWithResultAndOptions(
		fmt.Sprintf(
			"select 1 from mo_catalog.%s where %s >= x'%016x' and %s < x'%016x' limit 1",
			catalog.MO_DATA_BRANCH_UPDATE_LINK,
			catalog.DataBranchUpdateLinkColLinkID, tblID,
			catalog.DataBranchUpdateLinkColLinkID, tblID+1,
		),
		int32(accountID),
		executor.StatementOption{}.WithDisableLog(),
	)
	if err != nil {
		if moerr.IsMoErrCode(err, moerr.ErrNoSuchTable) {
			return false, nil
		}
		return false, err
	}
	defer res.Close()
	has := false
	res.ReadRows(func(n int, _ []*vector.Vector) bool {
		if n > 0 {
			has = true
		}
		return true
	})
	return has, nil
}

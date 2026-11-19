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
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// sequenceCommitTableName quotes a sequence relation for internal commit SQL,
// mirroring the function package's escaping.
func sequenceCommitTableName(db, table string) string {
	return "`" + strings.ReplaceAll(db, "`", "``") + "`.`" + strings.ReplaceAll(table, "`", "``") + "`"
}

// isSeqLifecycleMiss reports that the obligation's generation no longer
// resolves in the committing transaction: the terminal lifecycle outcome
// (self-deleted or replaced identity) retires the obligation unstamped.
// Classification traverses wrappers: the commit path may join the miss with
// a rollback outcome, whose dynamic type defeats moerr.IsMoErrCode's direct
// assertion.
func isSeqLifecycleMiss(err error) bool {
	var me *moerr.Error
	if !errors.As(err, &me) {
		return false
	}
	code := me.ErrorCode()
	return code == moerr.OkExpectedEOB || code == moerr.ErrNoSuchTable
}

// formatSequenceObligationFloor renders the floor in the sequence's numeric
// domain.
func formatSequenceObligationFloor(ob process.SequenceObligation) string {
	if ob.Unsigned {
		return strconv.FormatUint(ob.Floor, 10)
	}
	return strconv.FormatInt(int64(ob.Floor), 10)
}

// applySequenceObligationsBeforeCommitUnsafe persists the issued-value
// obligations of transaction-local sequence generations before the physical
// commit. For every surviving generation exactly one conditional UPDATE runs:
// it restores (floor, is_called=true) precisely when the generation's
// surviving workspace position is unsafe for the final pass (behind the
// floor in allocation order, or equal to the floor while still uncalled —
// the rewind cases), and writes nothing otherwise, preserving natural
// positions and surviving advances (e.g. transaction-local merge
// reconciliation). Failure is terminal with the same contract as lineage
// validation: a partial stamp must not separate from the physical commit, so
// the transaction is rolled back on the cleanup context.
func (th *TxnHandler) applySequenceObligationsBeforeCommitUnsafe(execCtx *ExecCtx) error {
	sess, ok := execCtx.ses.(*Session)
	if !ok || sess == nil {
		return nil
	}
	set := sess.getSequenceObligationSet()
	if set == nil {
		return nil
	}
	entries := set.Snapshot()
	if len(entries) == 0 {
		return nil
	}
	if err := th.stampSequenceObligationsAtCommitUnsafe(execCtx, sess, entries); err != nil {
		return errors.Join(err, th.rollbackUnsafe(execCtx, nil))
	}
	// The stamp consumed the obligations. Even if the physical commit later
	// fails, the transaction is gone and its issued-value records must never
	// stamp a subsequent transaction.
	set.Clear()
	return nil
}

func (th *TxnHandler) stampSequenceObligationsAtCommitUnsafe(execCtx *ExecCtx, sess *Session, entries []process.SequenceObligation) error {
	storage := th.storage
	txnOp := th.txnOp
	if storage == nil || txnOp == nil {
		return moerr.NewInternalErrorNoCtx("sequence obligation stamp: missing storage or txn operator")
	}
	rt := runtime.ServiceRuntime(sess.GetService())
	if rt == nil {
		return moerr.NewInternalErrorNoCtx("sequence obligation stamp: missing runtime")
	}
	value, ok := rt.GetGlobalVariables(runtime.InternalSQLExecutor)
	if !ok {
		return moerr.NewInternalErrorNoCtx("sequence obligation stamp: missing internal SQL executor")
	}
	sqlExecutor, ok := value.(executor.SQLExecutor)
	if !ok {
		return moerr.NewInternalErrorNoCtx("sequence obligation stamp: invalid internal SQL executor")
	}
	var accountID uint32
	if tenant := sess.GetTenantInfo(); tenant != nil {
		accountID = tenant.GetTenantID()
	}
	lowerCaseTableNames := int64(1)
	if v, e := sess.GetSessionSysVar("lower_case_table_names"); e == nil && v != nil {
		if n, ok := v.(int64); ok {
			lowerCaseTableNames = n
		}
	}
	// The write joins the committing transaction; BgKey marks the internal
	// sequence-row write contract exactly as the sequenceSQL helper does.
	ctx := context.WithValue(execCtx.reqCtx, defines.BgKey{}, true)
	opts := executor.Options{}.
		WithDisableIncrStatement().
		WithTxn(txnOp).
		WithKeepTxnAlive().
		WithAccountID(accountID).
		WithLowerCaseTableNames(&lowerCaseTableNames).
		WithTimeZone(sess.GetTimeZone()).
		WithStatementOption(executor.StatementOption{}.WithAccountID(accountID))
	for _, ob := range entries {
		dbHandler, err := storage.Database(ctx, ob.Db, txnOp)
		if err != nil {
			if isSeqLifecycleMiss(err) {
				continue
			}
			return err
		}
		rel, err := dbHandler.Relation(ctx, ob.Table, nil)
		if err != nil {
			if isSeqLifecycleMiss(err) {
				continue
			}
			return err
		}
		if rel.GetTableID(ctx) != ob.RelID {
			// Replaced identity: the obligation's generation is gone.
			continue
		}
		floor := formatSequenceObligationFloor(ob)
		cmp := "<"
		if ob.Minus {
			cmp = ">"
		}
		// Preserve-or-restore in one conditional statement: rows already at
		// or beyond the obligation (called at the floor, natural positions,
		// surviving reconcile advances, safe older-pass rewinds) match no
		// guard arm and are preserved.
		sql := fmt.Sprintf(
			"update %s set last_seq_num = %s, is_called = true where last_seq_num %s %s or (last_seq_num = %s and is_called = false)",
			sequenceCommitTableName(ob.Db, ob.Table), floor, cmp, floor, floor)
		res, err := sqlExecutor.Exec(ctx, sql, opts)
		if err != nil {
			return err
		}
		res.Close()
	}
	return nil
}

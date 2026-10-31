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
	"strings"
	"sync/atomic"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/defines"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/util/fault"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

// Owner-removal reap fabric tests (D153/D154/D155): classification of the
// removed owner rows, the per-account scan order and attaches, the
// ONE-predicate coverage re-evaluation, the fail-atomic error contract,
// and the empty-ledger fast path. Everything here is SQL-shape driven
// against backgroundExecTest, with an account-aware overlay because the
// same mo_snapshots query text must answer differently under the tenant's
// attach and under SYS.

func newMrsForOwnerRows(cols int, rows [][]interface{}) *MysqlResultSet {
	mrs := &MysqlResultSet{}
	for i := 0; i < cols; i++ {
		col := &MysqlColumn{}
		col.SetColumnType(defines.MYSQL_TYPE_VARCHAR)
		mrs.AddColumn(col)
	}
	for _, row := range rows {
		mrs.AddRow(row)
	}
	return mrs
}

// accountAwareExec keys mock results by (attached account, sql) so the two
// attach worlds of a reap can return different rows for the same probe
// text; anything missing falls back to the plain sql2result map.
type accountAwareExec struct {
	*backgroundExecTest
	byAccount      map[string]ExecResult
	errByAccount   map[string]error
	currentAccount uint32
}

// Exec honors the account-keyed error injection first, recording the
// failed statement exactly like backgroundExecTest.Exec so ordering and
// attach assertions stay parallel, then delegates.
func (e *accountAwareExec) Exec(ctx context.Context, s string) error {
	e.currentAccount, _ = defines.GetAccountId(ctx)
	if err, ok := e.errByAccount[accountKey(e.currentAccount, s)]; ok {
		e.backgroundExecTest.currentSql = s
		e.backgroundExecTest.executedSQLs = append(e.backgroundExecTest.executedSQLs, s)
		e.backgroundExecTest.systemCTELimits = append(e.backgroundExecTest.systemCTELimits, process.HasSystemCTELimits(ctx))
		e.backgroundExecTest.lockWriterFair = append(e.backgroundExecTest.lockWriterFair, defines.IsLockWriterFair(ctx))
		e.backgroundExecTest.publicationViewBinding = append(e.backgroundExecTest.publicationViewBinding,
			defines.IsPublicationViewBinding(ctx))
		e.backgroundExecTest.executionAccountIDs = append(e.backgroundExecTest.executionAccountIDs, e.currentAccount)
		databaseType, _ := ctx.Value(defines.DatTypKey{}).(string)
		e.backgroundExecTest.executionDatabaseTypes = append(e.backgroundExecTest.executionDatabaseTypes, databaseType)
		return err
	}
	return e.backgroundExecTest.Exec(ctx, s)
}

func (e *accountAwareExec) GetExecResultSet() []interface{} {
	if e.backgroundExecTest.resultSets != nil {
		return e.backgroundExecTest.GetExecResultSet()
	}
	if result, ok := e.byAccount[accountKey(e.currentAccount, e.backgroundExecTest.currentSql)]; ok {
		return []interface{}{result}
	}
	return e.backgroundExecTest.GetExecResultSet()
}

func newAccountAwareExec() *accountAwareExec {
	bt := &backgroundExecTest{}
	bt.init()
	return &accountAwareExec{
		backgroundExecTest: bt,
		byAccount:          map[string]ExecResult{},
		errByAccount:       map[string]error{},
	}
}

func accountKey(accountID uint32, sql string) string {
	return fmt.Sprintf("%d\x00%s", accountID, sql)
}

// recordingRetireSink captures the D167 RETIRE transitions a reap journals
// into the drop transaction's genesis registry.
type retireSinkCall struct {
	accountID uint32
	tableIDs  []uint64
}

type recordingRetireSink struct {
	calls []retireSinkCall
}

func (s *recordingRetireSink) PkMoveRetireAddresses(accountID uint32, tableIDs []uint64) {
	s.calls = append(s.calls, retireSinkCall{
		accountID: accountID,
		tableIDs:  append([]uint64(nil), tableIDs...),
	})
}

const (
	ownerLedgerScanSQL       = "select table_id, ifnull(database_name, ''), ifnull(table_name, ''), obj_id, created_ts, logical_id from mo_catalog.mo_data_branch_link_retention where row_kind = 0 order by table_id"
	ownerLedgerScanLegacySQL = "select table_id, ifnull(database_name, ''), ifnull(table_name, ''), obj_id, created_ts from mo_catalog.mo_data_branch_link_retention order by table_id"
	ownerSnapshotsSQL        = "select level, ifnull(account_name, ''), ifnull(database_name, ''), ifnull(table_name, ''), ifnull(obj_id, 0) from mo_catalog.mo_snapshots where kind != 'branch'"
	ownerPitrsSQL            = "select level, ifnull(account_id, 0), ifnull(account_name, ''), ifnull(database_name, ''), ifnull(table_name, ''), ifnull(obj_id, 0) from mo_catalog.mo_pitr where pitr_status = 1"
	ownerAccountsSQL         = "select account_id from mo_catalog.mo_account order by account_id"
	ownerLinkDeleteSQL       = "delete from mo_catalog.mo_data_branch_update_link"
	ownerLedgerDelete        = "delete from mo_catalog.mo_data_branch_link_retention"
	ownerLiveCountSQL        = "select count(table_id), ifnull(max(table_id), 0) from mo_catalog.mo_branch_metadata where p_table_id = %d and table_deleted = false"
	ownerChildIdsSQL         = "select table_id from mo_catalog.mo_branch_metadata where p_table_id = %d"
	ownerAccountByName       = "select account_name from mo_catalog.mo_account where account_id = %d"
	// ownerSelfLiveFmt is the batched self-live probe (D159(D)) joining
	// the ledger row's durable logical id; tests probe the single id 800.
	ownerSelfLiveFmt = "select rel_id, reldatabase, relname, coalesce(nullif(rel_logical_id,0),rel_id) from mo_catalog.mo_tables where rel_id in (%s)"

	ownerPitrScopeSQL     = "select level, ifnull(account_id, 0), ifnull(account_name, ''), ifnull(obj_id, 0) from mo_catalog.mo_pitr where pitr_name = 'cp01' and create_account = 0"
	ownerSnapshotScopeSQL = "select level, ifnull(account_name, ''), ifnull(obj_id, 0) from mo_catalog.mo_snapshots where sname = 'own01' order by snapshot_id"
)

// The discovery readers parse the classification projection; a missing row
// simply yields no scope (no reap), and the statement's own error
// propagates untouched.
func TestReadDroppedOwnerScopes(t *testing.T) {
	ctx := context.Background()

	t.Run("pitr scope parses", func(t *testing.T) {
		bh := &backgroundExecTest{}
		bh.init()
		bh.sql2result[ownerPitrScopeSQL] = newMrsForOwnerRows(4, [][]interface{}{
			{"cluster", uint64(0), "", ^uint64(0)},
		})
		scopes, err := readDroppedPitrOwnerScopes(ctx, bh, "cp01", 0)
		require.NoError(t, err)
		require.Len(t, scopes, 1)
		require.Equal(t, "cluster", scopes[0].level)
		require.Equal(t, ^uint64(0), scopes[0].objID)
	})
	t.Run("pitr scope missing row yields no scope", func(t *testing.T) {
		bh := &backgroundExecTest{}
		bh.init()
		bh.sql2result[ownerPitrScopeSQL] = newMrsForOwnerRows(4, nil)
		scopes, err := readDroppedPitrOwnerScopes(ctx, bh, "cp01", 0)
		require.NoError(t, err)
		require.Empty(t, scopes)
	})
	t.Run("pitr scope read error propagates", func(t *testing.T) {
		bh := &backgroundExecTest{}
		bh.init()
		boom := moerr.NewInternalErrorNoCtx("boom")
		bh.sql2err[ownerPitrScopeSQL] = boom
		_, err := readDroppedPitrOwnerScopes(ctx, bh, "cp01", 0)
		require.ErrorIs(t, err, boom)
	})
	t.Run("snapshot scope rejects an invalid name without touching the catalog", func(t *testing.T) {
		bh := &backgroundExecTest{}
		bh.init()
		_, err := readDroppedSnapshotOwnerScopes(ctx, bh, "own01' or '1'='1")
		require.Error(t, err)
		require.Empty(t, bh.executedSQLs)
	})
}

// Classification maps each removed owner row to the exact tenant set the
// owner could have protected (D154-C/D155-B).
func TestClassifyRemovedDataBranchOwnerAccounts(t *testing.T) {
	ctx := context.Background()

	seedAccounts := func(bh *backgroundExecTest, ids []uint64) {
		rows := make([][]interface{}, 0, len(ids))
		for _, id := range ids {
			rows = append(rows, []interface{}{id})
		}
		bh.sql2result[ownerAccountsSQL] = newMrsForOwnerRows(1, rows)
	}

	t.Run("cluster owner covers every tenant in mo_account plus the caller", func(t *testing.T) {
		bh := &backgroundExecTest{}
		bh.init()
		seedAccounts(bh, []uint64{0, 7, 9})
		got, err := classifyRemovedDataBranchOwnerAccounts(ctx, bh,
			[]dataBranchRemovedOwnerScope{{level: "cluster", objID: ^uint64(0)}}, 0)
		require.NoError(t, err)
		require.Equal(t, []uint32{0, 7, 9}, got)
	})
	t.Run("account owner by recorded obj id", func(t *testing.T) {
		bh := &backgroundExecTest{}
		bh.init()
		bh.sql2result["select 1 from mo_catalog.mo_account where account_id = 9"] = newMrsForOwnerRows(1, [][]interface{}{{1}})
		got, err := classifyRemovedDataBranchOwnerAccounts(ctx, bh,
			[]dataBranchRemovedOwnerScope{{level: "account", objID: 9}}, 0)
		require.NoError(t, err)
		require.Equal(t, []uint32{0, 9}, got)
	})
	t.Run("account owner by recorded account_id when obj id is zero", func(t *testing.T) {
		bh := &backgroundExecTest{}
		bh.init()
		bh.sql2result["select 1 from mo_catalog.mo_account where account_id = 11"] = newMrsForOwnerRows(1, [][]interface{}{{1}})
		got, err := classifyRemovedDataBranchOwnerAccounts(ctx, bh,
			[]dataBranchRemovedOwnerScope{{level: "account", accountID: 11}}, 9)
		require.NoError(t, err)
		require.Equal(t, []uint32{9, 11}, got)
	})
	t.Run("account owner by recorded name resolves through mo_account", func(t *testing.T) {
		bh := &backgroundExecTest{}
		bh.init()
		bh.sql2result["select account_id from mo_catalog.mo_account where account_name = 'tenant6'"] =
			newMrsForOwnerRows(1, [][]interface{}{{uint64(6)}})
		bh.sql2result["select 1 from mo_catalog.mo_account where account_id = 6"] = newMrsForOwnerRows(1, [][]interface{}{{1}})
		got, err := classifyRemovedDataBranchOwnerAccounts(ctx, bh,
			[]dataBranchRemovedOwnerScope{{level: "account", accountName: "tenant6"}}, 0)
		require.NoError(t, err)
		require.Equal(t, []uint32{0, 6}, got)
	})
	t.Run("name-keyed owner of a deleted account is skipped", func(t *testing.T) {
		bh := &backgroundExecTest{}
		bh.init()
		bh.sql2result["select account_id from mo_catalog.mo_account where account_name = 'ghost'"] =
			newMrsForOwnerRows(1, nil)
		got, err := classifyRemovedDataBranchOwnerAccounts(ctx, bh,
			[]dataBranchRemovedOwnerScope{{level: "account", accountName: "ghost"}}, 4)
		require.NoError(t, err)
		require.Equal(t, []uint32{4}, got)
	})
	t.Run("id-keyed owner target that vanished is skipped", func(t *testing.T) {
		bh := &backgroundExecTest{}
		bh.init()
		bh.sql2result["select 1 from mo_catalog.mo_account where account_id = 42"] = newMrsForOwnerRows(1, nil)
		got, err := classifyRemovedDataBranchOwnerAccounts(ctx, bh,
			[]dataBranchRemovedOwnerScope{{level: "account", objID: 42}}, 0)
		require.NoError(t, err)
		require.Equal(t, []uint32{0}, got)
	})
	t.Run("database and table owners stay with their recorded account and the caller", func(t *testing.T) {
		bh := &backgroundExecTest{}
		bh.init()
		got, err := classifyRemovedDataBranchOwnerAccounts(ctx, bh,
			[]dataBranchRemovedOwnerScope{{level: "database", accountID: 8}}, 7)
		require.NoError(t, err)
		require.Equal(t, []uint32{7, 8}, got)
	})
	t.Run("mo_account enumeration failure aborts the drop atomically", func(t *testing.T) {
		bh := &backgroundExecTest{}
		bh.init()
		boom := moerr.NewInternalErrorNoCtx("enumeration failed")
		bh.sql2err[ownerAccountsSQL] = boom
		_, err := classifyRemovedDataBranchOwnerAccounts(ctx, bh,
			[]dataBranchRemovedOwnerScope{{level: "cluster"}}, 0)
		require.ErrorIs(t, err, boom)
	})
}

// The per-account scan: owner coverage re-evaluation, attach discipline,
// id-keyed residue, and the empty-ledger fast path.
func TestReapDataBranchLinksOfAccount(t *testing.T) {
	ctx := context.Background()

	seedReapBase := func(bh *accountAwareExec, accountID uint32, ledgerRows [][]interface{}) {
		bh.byAccount[accountKey(accountID, ownerLedgerScanSQL)] = newMrsForOwnerRows(6, ledgerRows)
		bh.byAccount[accountKey(accountID, ownerSnapshotsSQL)] = newMrsForOwnerRows(5, nil)
		bh.byAccount[accountKey(0, ownerSnapshotsSQL)] = newMrsForOwnerRows(5, nil)
		bh.byAccount[accountKey(0, ownerPitrsSQL)] = newMrsForOwnerRows(6, nil)
		// The self-live probe runs for any coverage miss before the
		// descendant walk; the base scenario is fully dead lineage.
		bh.byAccount[accountKey(accountID, fmt.Sprintf(ownerSelfLiveFmt, "800"))] = newMrsForOwnerRows(4, nil)
		bh.byAccount[accountKey(0, fmt.Sprintf(ownerLiveCountSQL, 800))] = newMrsForOwnerRows(2, [][]interface{}{{uint64(0), uint64(0)}})
		bh.byAccount[accountKey(0, fmt.Sprintf(ownerChildIdsSQL, 800))] = newMrsForOwnerRows(1, nil)
	}

	t.Run("empty ledger fast path skips every owner probe", func(t *testing.T) {
		bh := newAccountAwareExec()
		bh.byAccount[accountKey(7, ownerLedgerScanSQL)] = newMrsForOwnerRows(6, nil)
		require.NoError(t, reapDataBranchLinksOfAccount(ctx, bh, "sid-test", nil, 7))
		for _, sql := range bh.executedSQLs {
			require.NotContains(t, sql, "mo_snapshots")
			require.NotContains(t, sql, "mo_pitr")
			require.NotContains(t, sql, "mo_branch_metadata")
		}
	})

	t.Run("uncovered retained table reaps links and ledger under its own attach", func(t *testing.T) {
		bh := newAccountAwareExec()
		seedReapBase(bh, 9, [][]interface{}{{uint64(800), "db1", "t800", uint64(800), uint64(1700000000), uint64(0)}})
		require.NoError(t, reapDataBranchLinksOfAccount(ctx, bh, "sid-test", nil, 9))
		var linkDeletes, ledgerDeletes []int
		for i, sql := range bh.executedSQLs {
			if strings.HasPrefix(sql, ownerLinkDeleteSQL) {
				linkDeletes = append(linkDeletes, i)
			}
			if strings.HasPrefix(sql, ownerLedgerDelete) {
				ledgerDeletes = append(ledgerDeletes, i)
			}
		}
		require.Len(t, linkDeletes, 1)
		require.Len(t, ledgerDeletes, 1)
		require.Less(t, linkDeletes[0], ledgerDeletes[0], "links retire before their ledger row")
		require.Equal(t, uint32(9), bh.executionAccountIDs[linkDeletes[0]],
			"the link delete runs attached to the scanned account")
		require.Equal(t, uint32(9), bh.executionAccountIDs[ledgerDeletes[0]])
		require.Contains(t, bh.executedSQLs[linkDeletes[0]], "link_id >= x'0000000000000320'")
	})

	t.Run("retire sink fires with the exact address after the paired delete", func(t *testing.T) {
		bh := newAccountAwareExec()
		seedReapBase(bh, 9, [][]interface{}{{uint64(800), "db1", "t800", uint64(800), uint64(1700000000), uint64(0)}})
		sink := &recordingRetireSink{}
		require.NoError(t, reapDataBranchLinksOfAccount(ctx, bh, "sid-test", sink, 9))
		require.Equal(t, []retireSinkCall{{accountID: 9, tableIDs: []uint64{800}}}, sink.calls,
			"the D167 transition rides the same statement segment as the paired deletes")
	})

	t.Run("retire sink stays silent when no row retires", func(t *testing.T) {
		bh := newAccountAwareExec()
		seedReapBase(bh, 9, [][]interface{}{{uint64(800), "db1", "t800", uint64(800), uint64(1700000000), uint64(0)}})
		bh.byAccount[accountKey(0, ownerSnapshotsSQL)] = newMrsForOwnerRows(5, [][]interface{}{
			{"cluster", "", "", "", ^uint64(0)},
		})
		sink := &recordingRetireSink{}
		require.NoError(t, reapDataBranchLinksOfAccount(ctx, bh, "sid-test", sink, 9))
		require.Empty(t, sink.calls, "a covered row keeps links, ledger row, and epoch alike")
	})

	t.Run("retire sink stays silent when the paired delete fails", func(t *testing.T) {
		bh := newAccountAwareExec()
		seedReapBase(bh, 9, [][]interface{}{{uint64(800), "db1", "t800", uint64(800), uint64(1700000000), uint64(0)}})
		boom := moerr.NewInternalErrorNoCtx("ledger delete failed")
		bh.sql2err[ownerLedgerDelete+" where row_kind = 0 and table_id = 800"] = boom
		sink := &recordingRetireSink{}
		err := reapDataBranchLinksOfAccount(ctx, bh, "sid-test", sink, 9)
		require.ErrorIs(t, err, boom)
		require.Empty(t, sink.calls,
			"a failed paired delete never journals the retirement transition")
	})

	t.Run("sys-owned cluster owner still covers the tenant", func(t *testing.T) {
		bh := newAccountAwareExec()
		seedReapBase(bh, 9, [][]interface{}{{uint64(800), "db1", "t800", uint64(800), uint64(1700000000), uint64(0)}})
		bh.byAccount[accountKey(0, ownerSnapshotsSQL)] = newMrsForOwnerRows(5, [][]interface{}{
			{"cluster", "", "", "", ^uint64(0)},
		})
		require.NoError(t, reapDataBranchLinksOfAccount(ctx, bh, "sid-test", nil, 9))
		require.Zero(t, countExecuted(bh.executedSQLs, ownerLinkDeleteSQL))
	})

	t.Run("active account pitr recorded under sys still covers", func(t *testing.T) {
		bh := newAccountAwareExec()
		seedReapBase(bh, 9, [][]interface{}{{uint64(800), "db1", "t800", uint64(800), uint64(1700000000), uint64(0)}})
		bh.byAccount[accountKey(0, ownerPitrsSQL)] = newMrsForOwnerRows(6, [][]interface{}{
			{"account", uint64(9), "tenant9", "", "", uint64(9)},
		})
		require.NoError(t, reapDataBranchLinksOfAccount(ctx, bh, "sid-test", nil, 9))
		require.Zero(t, countExecuted(bh.executedSQLs, ownerLinkDeleteSQL))
	})

	t.Run("empty-name residue gets an id-keyed local snapshot chance", func(t *testing.T) {
		bh := newAccountAwareExec()
		// Residue row: no names, only the physical id. A tenant-local
		// table snapshot carrying the recorded obj id still grounds it.
		seedReapBase(bh, 9, [][]interface{}{{uint64(800), "", "", uint64(800), uint64(1700000000), uint64(0)}})
		bh.byAccount[accountKey(9, ownerSnapshotsSQL)] = newMrsForOwnerRows(5, [][]interface{}{
			{"table", "", "old_db", "old_t", uint64(800)},
		})
		require.NoError(t, reapDataBranchLinksOfAccount(ctx, bh, "sid-test", nil, 9))
		require.Zero(t, countExecuted(bh.executedSQLs, ownerLinkDeleteSQL))
	})

	t.Run("lazy account-name resolution covers a name-keyed account pitr", func(t *testing.T) {
		bh := newAccountAwareExec()
		seedReapBase(bh, 9, [][]interface{}{{uint64(800), "db1", "t800", uint64(800), uint64(1700000000), uint64(0)}})
		bh.byAccount[accountKey(0, ownerPitrsSQL)] = newMrsForOwnerRows(6, [][]interface{}{
			{"account", uint64(0), "tenant9", "", "", uint64(0)},
		})
		bh.byAccount[accountKey(0, fmt.Sprintf(ownerAccountByName, 9))] = newMrsForOwnerRows(1, [][]interface{}{{"tenant9"}})
		require.NoError(t, reapDataBranchLinksOfAccount(ctx, bh, "sid-test", nil, 9))
		require.Zero(t, countExecuted(bh.executedSQLs, ownerLinkDeleteSQL))
		require.Equal(t, 1, countExecuted(bh.executedSQLs, fmt.Sprintf(ownerAccountByName, 9)),
			"the name is resolved at most once per scanned account")
	})

	t.Run("account-name lookup failure fails the reap atomically", func(t *testing.T) {
		bh := newAccountAwareExec()
		seedReapBase(bh, 9, [][]interface{}{{uint64(800), "db1", "t800", uint64(800), uint64(1700000000), uint64(0)}})
		bh.byAccount[accountKey(0, ownerPitrsSQL)] = newMrsForOwnerRows(6, [][]interface{}{
			{"account", uint64(0), "tenant9", "", "", uint64(0)},
		})
		boom := moerr.NewInternalErrorNoCtx("mo_account read failed")
		bh.sql2err[fmt.Sprintf(ownerAccountByName, 9)] = boom
		err := reapDataBranchLinksOfAccount(ctx, bh, "sid-test", nil, 9)
		require.ErrorIs(t, err, boom)
		require.Zero(t, countExecuted(bh.executedSQLs, ownerLinkDeleteSQL),
			"an undecidable coverage probe never deletes")
	})

	t.Run("live lineage descendant keeps without any owner", func(t *testing.T) {
		bh := newAccountAwareExec()
		seedReapBase(bh, 9, [][]interface{}{{uint64(800), "db1", "t800", uint64(800), uint64(1700000000), uint64(0)}})
		bh.byAccount[accountKey(0, fmt.Sprintf(ownerLiveCountSQL, 800))] = newMrsForOwnerRows(2, [][]interface{}{{uint64(1), uint64(2)}})
		require.NoError(t, reapDataBranchLinksOfAccount(ctx, bh, "sid-test", nil, 9))
		require.Zero(t, countExecuted(bh.executedSQLs, ownerLinkDeleteSQL))
	})

	t.Run("empty-name residue is never auto-deleted (D154-D)", func(t *testing.T) {
		// Nothing grounds the residue row — no owner rows at all. The
		// O1 residue class still keeps links and ledger row and only
		// surfaces a WARN; auto-deleting it could erase provenance a
		// name-keyed owner was never able to vouch for.
		bh := newAccountAwareExec()
		seedReapBase(bh, 9, [][]interface{}{{uint64(800), "", "", uint64(800), uint64(1700000000), uint64(0)}})
		require.NoError(t, reapDataBranchLinksOfAccount(ctx, bh, "sid-test", nil, 9))
		require.Zero(t, countExecuted(bh.executedSQLs, ownerLinkDeleteSQL))
		require.Zero(t, countExecuted(bh.executedSQLs, ownerLedgerDelete))
	})

	t.Run("database owner cannot decide residue, which is still never deleted (D154-D)", func(t *testing.T) {
		// A surviving database-level owner would keep the links of a
		// NAMED row in db1 (proved by the coverage cases above). The
		// residue row cannot match that name — and the reap must still
		// not delete it: the pre-fix behavior deleted exactly this row,
		// reverting a later historical-clone MERGE/PICK to delete+insert
		// replay (r50 review P1).
		bh := newAccountAwareExec()
		seedReapBase(bh, 9, [][]interface{}{{uint64(800), "", "", uint64(800), uint64(1700000000), uint64(0)}})
		bh.byAccount[accountKey(9, ownerSnapshotsSQL)] = newMrsForOwnerRows(5, [][]interface{}{
			{"database", "", "db1", "", uint64(0)},
		})
		require.NoError(t, reapDataBranchLinksOfAccount(ctx, bh, "sid-test", nil, 9))
		require.Zero(t, countExecuted(bh.executedSQLs, ownerLinkDeleteSQL))
		require.Zero(t, countExecuted(bh.executedSQLs, ownerLedgerDelete))
	})

	t.Run("named row under a database owner is covered and kept", func(t *testing.T) {
		// The identity-carrying half of the fix: a KEEP record that
		// carries its resolved names matches the surviving
		// database-level owner, so the row neither degrades to residue
		// nor gets deleted while its owner lives.
		bh := newAccountAwareExec()
		seedReapBase(bh, 9, [][]interface{}{{uint64(800), "db1", "t800", uint64(800), uint64(1700000000), uint64(0)}})
		bh.byAccount[accountKey(9, ownerSnapshotsSQL)] = newMrsForOwnerRows(5, [][]interface{}{
			{"database", "", "db1", "", uint64(0)},
		})
		require.NoError(t, reapDataBranchLinksOfAccount(ctx, bh, "sid-test", nil, 9))
		require.Zero(t, countExecuted(bh.executedSQLs, ownerLinkDeleteSQL))
	})
}

func countExecuted(sqls []string, prefix string) int {
	n := 0
	for _, sql := range sqls {
		if strings.HasPrefix(sql, prefix) {
			n++
		}
	}
	return n
}

// stubResolveReapTxnOperator arms the D206(B.1) SETTER SITE (4) gate with a
// non-nil operator: these SQL-shape harnesses are not *backExec, so the
// production unwrap returns nil and the reap driver would fail closed
// before the behavior under test runs. The gomock operator does not
// implement MarkExpectedConflict, so the mark helper no-ops on it — the
// same shape the compile-funnel tests rely on.
func stubResolveReapTxnOperator(t *testing.T) {
	ctrl := gomock.NewController(t)
	stub := mock_frontend.NewMockTxnOperator(ctrl)
	prev := resolveReapTxnOperator
	resolveReapTxnOperator = func(BackgroundExec) client.TxnOperator { return stub }
	t.Cleanup(func() { resolveReapTxnOperator = prev })
}

// markWitnessOperator witnesses the D206(B.1) optional-interface mark on
// SITE (4)'s resolved background operator while delegating every other
// call to the gomock operator: MarkTxnExpectedConflictIfSupported folds to
// a no-op unless the resolved operator implements MarkExpectedConflict,
// and this wrapper is exactly that implementation.
type markWitnessOperator struct {
	*mock_frontend.MockTxnOperator
	marks atomic.Int32
}

func (m *markWitnessOperator) MarkExpectedConflict() { m.marks.Add(1) }

// D210(B)/D206(B.1) SITE (4) fail-closed gate: a nil background-operator
// resolution aborts the reap driver with the fail-closed error BEFORE any
// classification contact or verdict write issues.
func TestReapMarkFailsClosedWithoutOperator(t *testing.T) {
	ctx := context.Background()
	prev := resolveReapTxnOperator
	resolveReapTxnOperator = func(BackgroundExec) client.TxnOperator { return nil }
	t.Cleanup(func() { resolveReapTxnOperator = prev })
	bh := newAccountAwareExec()
	err := reapDataBranchLinksAfterOwnerRemoval(ctx, bh, "sid-test",
		nil, []dataBranchRemovedOwnerScope{{level: "account", objID: 9}}, 7)
	require.Error(t, err)
	require.Contains(t, err.Error(),
		"pk-move reap mark: background txn operator absent")
	require.Empty(t, bh.executedSQLs,
		"the fail-closed gate precedes the reap's first consult and write")
}

// D210(B)/D206(B.1) SITE (4) witness: the operator resolved at reap-driver
// entry is marked BEFORE the reap's first consult, once per driver
// invocation; an all-empty affected-account set issues no verdict work.
func TestReapMarkWitnessesBackgroundOperator(t *testing.T) {
	ctx := context.Background()
	ctrl := gomock.NewController(t)
	spy := &markWitnessOperator{MockTxnOperator: mock_frontend.NewMockTxnOperator(ctrl)}
	prev := resolveReapTxnOperator
	resolveReapTxnOperator = func(BackgroundExec) client.TxnOperator { return spy }
	t.Cleanup(func() { resolveReapTxnOperator = prev })
	bh := newAccountAwareExec()
	bh.sql2result["select 1 from mo_catalog.mo_account where account_id = 9"] =
		newMrsForOwnerRows(1, [][]interface{}{{1}})
	// Both candidate accounts hold empty retention ledgers: the reap
	// short-circuits every owner probe, so no verdict branch runs.
	bh.byAccount[accountKey(7, ownerLedgerScanSQL)] = newMrsForOwnerRows(6, nil)
	bh.byAccount[accountKey(9, ownerLedgerScanSQL)] = newMrsForOwnerRows(6, nil)
	err := reapDataBranchLinksAfterOwnerRemoval(ctx, bh, "sid-test",
		nil, []dataBranchRemovedOwnerScope{{level: "account", objID: 9}}, 7)
	require.NoError(t, err)
	require.Equal(t, int32(1), spy.marks.Load(),
		"the background operator is marked exactly once at reap-driver entry")
	require.Zero(t, countExecuted(bh.executedSQLs, ownerLinkDeleteSQL))
	require.Zero(t, countExecuted(bh.executedSQLs, ownerLedgerDelete))
}

// The multi-account scan aborts at the first failing account and never
// half-finishes the rest (D155-C); the owning statement rolls the whole
// drop back (proved at SQL level by the injection tests below and at
// engine level by the live battery).
func TestReapDataBranchLinksAfterOwnerRemovalFailAtomic(t *testing.T) {
	ctx := context.Background()
	stubResolveReapTxnOperator(t)
	bh := newAccountAwareExec()
	bh.sql2result["select 1 from mo_catalog.mo_account where account_id = 9"] = newMrsForOwnerRows(1, [][]interface{}{{1}})
	// Account 7: one uncovered, fully-dead retained table -> its pair of
	// deletes runs attached to 7.
	bh.byAccount[accountKey(7, ownerLedgerScanSQL)] = newMrsForOwnerRows(6, [][]interface{}{{uint64(800), "db1", "t800", uint64(800), uint64(1700000000), uint64(0)}})
	bh.byAccount[accountKey(7, ownerSnapshotsSQL)] = newMrsForOwnerRows(5, nil)
	bh.byAccount[accountKey(0, ownerSnapshotsSQL)] = newMrsForOwnerRows(5, nil)
	bh.byAccount[accountKey(0, ownerPitrsSQL)] = newMrsForOwnerRows(6, nil)
	bh.byAccount[accountKey(7, fmt.Sprintf(ownerSelfLiveFmt, "800"))] = newMrsForOwnerRows(4, nil)
	bh.byAccount[accountKey(0, fmt.Sprintf(ownerLiveCountSQL, 800))] = newMrsForOwnerRows(2, [][]interface{}{{uint64(0), uint64(0)}})
	bh.byAccount[accountKey(0, fmt.Sprintf(ownerChildIdsSQL, 800))] = newMrsForOwnerRows(1, nil)
	// Account 9's ledger scan fails. sql2err is text-keyed and would also
	// fail account 7's identical scan, so the failure goes through the
	// attach-aware dispatch; nothing of account 9 may be touched.
	boom := moerr.NewInternalErrorNoCtx("ledger scan of account 9 failed")
	bh.errByAccount[accountKey(9, ownerLedgerScanSQL)] = boom

	err := reapDataBranchLinksAfterOwnerRemoval(ctx, bh, "sid-test",
		nil, []dataBranchRemovedOwnerScope{{level: "account", objID: 9}}, 7)
	// Sorted scan order processes 7 before 9: 7's deletes are recorded,
	// then 9's scan errors and the owning statement rolls the whole drop
	// back atomically (proved at SQL level here and at engine level by
	// the live battery).
	require.Error(t, err)
	require.Contains(t, err.Error(), "pk-move link reap of account 9")
	require.Equal(t, 1, countExecuted(bh.executedSQLs, ownerLinkDeleteSQL),
		"the already-processed account's verdict issued before the failure; the owning statement rolls it back atomically")
}

// D155-C kill-switch consumption (r50 review P2): a name-armed fault
// fails exactly the later account's scan AFTER the earlier account's
// deletes already issued inside the drop's transaction; disarming lets
// the identical retry complete both accounts. SQL-shape companion of the
// transaction-backed rollback battery that proves the rollback restores
// every affected row byte-identically.
func TestReapDataBranchLinksAfterOwnerRemovalInjectedAccountFault(t *testing.T) {
	ctx := context.Background()
	stubResolveReapTxnOperator(t)
	fault.Enable()
	defer fault.Disable()

	newHarness := func() *accountAwareExec {
		bh := newAccountAwareExec()
		bh.sql2result["select 1 from mo_catalog.mo_account where account_id = 9"] = newMrsForOwnerRows(1, [][]interface{}{{1}})
		for _, account := range []uint32{7, 9} {
			bh.byAccount[accountKey(account, ownerLedgerScanSQL)] = newMrsForOwnerRows(6, [][]interface{}{{uint64(800), "db1", "t800", uint64(800), uint64(1700000000), uint64(0)}})
			bh.byAccount[accountKey(account, ownerSnapshotsSQL)] = newMrsForOwnerRows(5, nil)
			bh.byAccount[accountKey(account, fmt.Sprintf(ownerSelfLiveFmt, "800"))] = newMrsForOwnerRows(4, nil)
		}
		bh.byAccount[accountKey(0, ownerSnapshotsSQL)] = newMrsForOwnerRows(5, nil)
		bh.byAccount[accountKey(0, ownerPitrsSQL)] = newMrsForOwnerRows(6, nil)
		bh.byAccount[accountKey(0, fmt.Sprintf(ownerLiveCountSQL, 800))] = newMrsForOwnerRows(2, [][]interface{}{{uint64(0), uint64(0)}})
		bh.byAccount[accountKey(0, fmt.Sprintf(ownerChildIdsSQL, 800))] = newMrsForOwnerRows(1, nil)
		bh.byAccount[accountKey(0, fmt.Sprintf(ownerAccountByName, 7))] = newMrsForOwnerRows(1, [][]interface{}{{"tenant7"}})
		bh.byAccount[accountKey(0, fmt.Sprintf(ownerAccountByName, 9))] = newMrsForOwnerRows(1, [][]interface{}{{"tenant9"}})
		return bh
	}
	scopes := []dataBranchRemovedOwnerScope{{level: "account", objID: 9}}

	require.NoError(t, fault.AddFaultPoint(ctx,
		objectio.FJ_CNDataBranchReapAccountFailed, ":::", "echo", 0, "tenant9", false))
	bh := newHarness()
	err := reapDataBranchLinksAfterOwnerRemoval(ctx, bh, "sid-test", nil, scopes, 7)
	require.Error(t, err)
	require.Contains(t, err.Error(), "injected pk-move link reap failure of account tenant9")
	require.Equal(t, 1, countExecuted(bh.executedSQLs, ownerLinkDeleteSQL),
		"account 7's verdicts issued before the injected failure; the drop's transaction rolls them back atomically (proved transaction-backed by the BVT battery)")
	require.Equal(t, 1, countExecuted(bh.executedSQLs, ownerLedgerDelete))

	removed, rmErr := fault.RemoveFaultPoint(ctx, objectio.FJ_CNDataBranchReapAccountFailed)
	require.True(t, removed)
	require.NoError(t, rmErr)
	retry := newHarness()
	require.NoError(t, reapDataBranchLinksAfterOwnerRemoval(ctx, retry, "sid-test", nil, scopes, 7))
	require.Equal(t, 2, countExecuted(retry.executedSQLs, ownerLinkDeleteSQL),
		"the disarmed retry completes every account's reap")
	require.Equal(t, 2, countExecuted(retry.executedSQLs, ownerLedgerDelete))
}

// The lineage liveness walk: non-deleted children short-circuit as live,
// deleted intermediate generations are walked through, pathological
// fan-out fails ALIVE, and query errors propagate to the fail-atomic
// caller.
func TestDataBranchLineageHasLiveDescendant(t *testing.T) {
	ctx := context.Background()

	t.Run("direct live child", func(t *testing.T) {
		bh := newAccountAwareExec()
		bh.byAccount[accountKey(0, fmt.Sprintf(ownerLiveCountSQL, 800))] = newMrsForOwnerRows(2, [][]interface{}{{uint64(2), uint64(0)}})
		live, err := dataBranchLineageHasLiveDescendant(ctx, bh, 800)
		require.NoError(t, err)
		require.True(t, live)
	})
	t.Run("deleted intermediate with live grandchild", func(t *testing.T) {
		bh := newAccountAwareExec()
		bh.byAccount[accountKey(0, fmt.Sprintf(ownerLiveCountSQL, 800))] = newMrsForOwnerRows(2, [][]interface{}{{uint64(0), uint64(0)}})
		bh.byAccount[accountKey(0, fmt.Sprintf(ownerChildIdsSQL, 800))] = newMrsForOwnerRows(1, [][]interface{}{{uint64(801)}})
		bh.byAccount[accountKey(0, fmt.Sprintf(ownerLiveCountSQL, 801))] = newMrsForOwnerRows(2, [][]interface{}{{uint64(1), uint64(0)}})
		live, err := dataBranchLineageHasLiveDescendant(ctx, bh, 800)
		require.NoError(t, err)
		require.True(t, live)
	})
	t.Run("fully deleted subtree", func(t *testing.T) {
		bh := newAccountAwareExec()
		bh.byAccount[accountKey(0, fmt.Sprintf(ownerLiveCountSQL, 800))] = newMrsForOwnerRows(2, [][]interface{}{{uint64(0), uint64(0)}})
		bh.byAccount[accountKey(0, fmt.Sprintf(ownerChildIdsSQL, 800))] = newMrsForOwnerRows(1, [][]interface{}{{uint64(801)}})
		bh.byAccount[accountKey(0, fmt.Sprintf(ownerLiveCountSQL, 801))] = newMrsForOwnerRows(2, [][]interface{}{{uint64(0), uint64(0)}})
		bh.byAccount[accountKey(0, fmt.Sprintf(ownerChildIdsSQL, 801))] = newMrsForOwnerRows(1, nil)
		live, err := dataBranchLineageHasLiveDescendant(ctx, bh, 800)
		require.NoError(t, err)
		require.False(t, live)
	})
	t.Run("pathological fan-out fails ALIVE", func(t *testing.T) {
		bh := newAccountAwareExec()
		bh.byAccount[accountKey(0, fmt.Sprintf(ownerLiveCountSQL, 800))] = newMrsForOwnerRows(2, [][]interface{}{{uint64(0), uint64(0)}})
		var rows [][]interface{}
		for id := uint64(1); id <= 600; id++ {
			rows = append(rows, []interface{}{id})
		}
		bh.byAccount[accountKey(0, fmt.Sprintf(ownerChildIdsSQL, 800))] = newMrsForOwnerRows(1, rows)
		live, err := dataBranchLineageHasLiveDescendant(ctx, bh, 800)
		require.NoError(t, err)
		require.True(t, live,
			"the bounded walk keeps links rather than risking a wrong reclaim")
	})
	t.Run("probe error propagates to the fail-atomic caller", func(t *testing.T) {
		bh := newAccountAwareExec()
		boom := moerr.NewInternalErrorNoCtx("metadata catalog read failed")
		bh.sql2err[fmt.Sprintf(ownerLiveCountSQL, 800)] = boom
		_, err := dataBranchLineageHasLiveDescendant(ctx, bh, 800)
		require.ErrorIs(t, err, boom)
	})
}

// fakeReapTxnOperator is the minimal non-nil operator the D206(B.1)
// reap-entry resolve-and-mark step needs when the BackgroundExec is not a
// *backExec (D210(B): harness and funnel UTs supply the operator through
// the resolver). MarkExpectedConflict is deliberately absent from
// TxnOperator's method set, so the mark consult is a no-op here; the
// exercised paths call no other operator method.
type fakeReapTxnOperator struct{ client.TxnOperator }

// installReapTxnOperatorForTest points the reap driver's resolver at the
// fake operator for the calling test only. The package-level resolver is
// process-global (restored via Cleanup; this file's tests run serially);
// these tests' fake executor is not a *backExec, so production resolution
// would fail closed at the mark consult ("pk-move reap mark: background
// txn operator absent") before reaching the behavior under test.
func installReapTxnOperatorForTest(t *testing.T) {
	t.Helper()
	restore := SetReapTxnOperatorResolverForTest(
		func(_ BackgroundExec,
			_ func(BackgroundExec) client.TxnOperator,
		) client.TxnOperator {
			return &fakeReapTxnOperator{}
		})
	t.Cleanup(restore)
}

// doDropPitr end-to-end with an injected mid-enumeration failure (D155-C):
// the failing scan aborts the DROP, and finishTxn rolls back the owner-row
// delete that rode the same pessimistic transaction.
func TestDoDropPitrReapFailureRollsBackAtomically(t *testing.T) {
	installReapTxnOperatorForTest(t)
	ses, bh, ctx := newPitrLifecycleTestSession(t)
	stmt := &tree.DropPitr{Name: "cp01"}

	checkSQL, err := getSqlForCheckPitr(ctx, "cp01", sysAccountID)
	require.NoError(t, err)
	bh.sql2result[checkSQL] = newMrsForPitrRecord([][]interface{}{{"pitr-id"}})
	bh.sql2result[ownerPitrScopeSQL] = newMrsForOwnerRows(4, [][]interface{}{
		{"cluster", uint64(0), "", ^uint64(0)},
	})
	bh.sql2result[getSqlForDropPitr("cp01", sysAccountID)] = nil
	// Other pitrs survive: no SYSMOCATALOGPITR cascade.
	otherSQL := fmt.Sprintf(getPitrFormat+" where pitr_name != '%s';", SYSMOCATALOGPITR)
	bh.sql2result[otherSQL] = newMrsForPitrRecord([][]interface{}{{"other-id"}})
	// Classification: cluster owner covers [0, 7, 9]; 0/9 scans are empty,
	// 7's ledger scan errors mid-enumeration.
	bh.sql2result[ownerAccountsSQL] = newMrsForOwnerRows(1, [][]interface{}{{uint64(0)}, {uint64(7)}, {uint64(9)}})
	boom := moerr.NewInternalErrorNoCtx("injected ledger scan failure")
	bh.sql2err[ownerLedgerScanSQL] = boom

	err = doDropPitr(ctx, ses, stmt)
	require.ErrorIs(t, err, boom)
	require.Contains(t, bh.executedSQLs, getSqlForDropPitr("cp01", sysAccountID),
		"the owner row delete issued inside the still-open transaction...")
	require.Contains(t, bh.executedSQLs, "rollback;",
		"...and the failing reap rolls the whole drop back atomically")
	require.NotContains(t, bh.executedSQLs, "commit;")
	require.Zero(t, countExecuted(bh.executedSQLs, ownerLinkDeleteSQL),
		"account 0's scan failed before any account could reap")
}

// The twin happy path: dropping a cluster PITR reaps exactly the
// unprotected accounts' retained links, attached per account, before the
// drop commits.
func TestDoDropPitrOwnerRemovalReapsUnprotectedAccounts(t *testing.T) {
	installReapTxnOperatorForTest(t)
	ses, bh, ctx := newPitrLifecycleTestSession(t)
	stmt := &tree.DropPitr{Name: "cp01"}
	// Let the foreground flow also see the account-attached context: the
	// plain backgroundExecTest records attaches; identical mo_snapshots
	// texts under different accounts share the empty answer — adequate
	// here because everything stays uncovered.
	checkSQL, err := getSqlForCheckPitr(ctx, "cp01", sysAccountID)
	require.NoError(t, err)
	bh.sql2result[checkSQL] = newMrsForPitrRecord([][]interface{}{{"pitr-id"}})
	bh.sql2result[ownerPitrScopeSQL] = newMrsForOwnerRows(4, [][]interface{}{
		{"cluster", uint64(0), "", ^uint64(0)},
	})
	bh.sql2result[getSqlForDropPitr("cp01", sysAccountID)] = nil
	otherSQL := fmt.Sprintf(getPitrFormat+" where pitr_name != '%s';", SYSMOCATALOGPITR)
	bh.sql2result[otherSQL] = newMrsForPitrRecord([][]interface{}{{"other-id"}})
	bh.sql2result[ownerAccountsSQL] = newMrsForOwnerRows(1, [][]interface{}{{uint64(0)}, {uint64(7)}})
	bh.sql2result[ownerLedgerScanSQL] = newMrsForOwnerRows(6, nil)
	bh.sql2result[ownerSnapshotsSQL] = newMrsForOwnerRows(5, nil)
	bh.sql2result[ownerPitrsSQL] = newMrsForOwnerRows(6, nil)
	bh.sql2result[fmt.Sprintf(ownerLiveCountSQL, 800)] = newMrsForOwnerRows(2, [][]interface{}{{uint64(0), uint64(0)}})
	bh.sql2result[fmt.Sprintf(ownerChildIdsSQL, 800)] = newMrsForOwnerRows(1, nil)

	require.NoError(t, doDropPitr(ctx, ses, stmt))
	require.Contains(t, bh.executedSQLs, "commit;")
	// Account 7 had no ledger rows (both accounts share the empty ledger
	// result), so only the empty fast path ran: one ledger scan per
	// account, zero deletes.
	require.Equal(t, 2, countExecuted(bh.executedSQLs, ownerLedgerScanSQL))
	require.Zero(t, countExecuted(bh.executedSQLs, ownerLinkDeleteSQL))
}

// doDropSnapshot twin: the removed row's classification drives the reap,
// a failing scan rolls the drop back, and the owner row delete was issued
// inside the same doomed transaction.
func TestDoDropSnapshotReapFailureRollsBackAtomically(t *testing.T) {
	installReapTxnOperatorForTest(t)
	ses, bh, ctx := newPitrLifecycleTestSession(t)
	// doDropSnapshot derives the reap's caller account from the statement
	// context; production attaches the session tenant to every statement.
	ctx = defines.AttachAccountId(ctx, sysAccountID)
	stmt := &tree.DropSnapShot{Name: tree.Identifier("own01")}

	checkSQL, err := getSqlForCheckSnapshot(ctx, "own01")
	require.NoError(t, err)
	bh.sql2result[checkSQL] = newMrsForOwnerRows(1, [][]interface{}{{uint64(1)}})
	kindSQL := "select kind from mo_catalog.mo_snapshots where sname = 'own01' order by snapshot_id limit 1"
	bh.sql2result[kindSQL] = newMrsForOwnerRows(1, [][]interface{}{{"user"}})
	bh.sql2result[ownerSnapshotScopeSQL] = newMrsForOwnerRows(3, [][]interface{}{
		{"cluster", "", ^uint64(0)},
	})
	bh.sql2result[getSqlForDropSnapshot("own01")] = nil
	bh.sql2result[ownerAccountsSQL] = newMrsForOwnerRows(1, [][]interface{}{{uint64(0)}, {uint64(7)}})
	boom := moerr.NewInternalErrorNoCtx("injected ledger scan failure")
	bh.sql2err[ownerLedgerScanSQL] = boom

	err = doDropSnapshot(ctx, ses, stmt)
	require.ErrorIs(t, err, boom)
	require.Contains(t, bh.executedSQLs, getSqlForDropSnapshot("own01"))
	require.Contains(t, bh.executedSQLs, "rollback;")
	require.NotContains(t, bh.executedSQLs, "commit;")
}

// The IF EXISTS early-return still fires no discovery and no reap
// (D155-B): a missing owner is not a maintenance event.
func TestDoDropPitrIfExistsMissingFiresNoReap(t *testing.T) {
	ses, bh, ctx := newPitrLifecycleTestSession(t)
	stmt := &tree.DropPitr{Name: "ghost", IfExists: true}

	checkSQL, err := getSqlForCheckPitr(ctx, "ghost", sysAccountID)
	require.NoError(t, err)
	bh.sql2result[checkSQL] = newMrsForOwnerRows(1, nil)

	require.NoError(t, doDropPitr(ctx, ses, stmt))
	for _, sql := range bh.executedSQLs {
		require.NotContains(t, sql, "ifnull(account_id, 0), ifnull(account_name, '')")
		require.NotContains(t, sql, ownerAccountsSQL)
	}
}

// The D159(D) self-live probe: an uncovered ledger row whose mo_tables
// row still lives is its own surviving producer — it KEEPS links and
// ledger row and is refreshed in place from the live catalog (created_ts
// immutable). A probe failure fails the reap atomically (D155-C), and a
// wide-projection logical id selects the logical coverage lane.
func TestReapDataBranchLinksSelfLiveProbe(t *testing.T) {
	ctx := context.Background()

	newAccount9Harness := func() *accountAwareExec {
		bh := newAccountAwareExec()
		bh.byAccount[accountKey(9, ownerLedgerScanSQL)] = newMrsForOwnerRows(6,
			[][]interface{}{{uint64(800), "db1", "t800", uint64(800), uint64(1700000000), uint64(0)}})
		bh.byAccount[accountKey(9, ownerSnapshotsSQL)] = newMrsForOwnerRows(5, nil)
		bh.byAccount[accountKey(0, ownerSnapshotsSQL)] = newMrsForOwnerRows(5, nil)
		bh.byAccount[accountKey(0, ownerPitrsSQL)] = newMrsForOwnerRows(6, nil)
		return bh
	}

	t.Run("self-live uncovered row keeps links and refreshes the ledger row", func(t *testing.T) {
		bh := newAccount9Harness()
		probeSQL := fmt.Sprintf(ownerSelfLiveFmt, "800")
		bh.byAccount[accountKey(9, probeSQL)] = newMrsForOwnerRows(4,
			[][]interface{}{{uint64(800), "db_live", "t_live", uint64(900)}})
		require.NoError(t, reapDataBranchLinksOfAccount(ctx, bh, "sid-test", nil, 9))
		require.Zero(t, countExecuted(bh.executedSQLs, ownerLinkDeleteSQL))
		require.Zero(t, countExecuted(bh.executedSQLs, ownerLedgerDelete))
		require.Zero(t, countExecuted(bh.executedSQLs,
			"select count(table_id), ifnull(max(table_id), 0) from mo_catalog.mo_branch_metadata"),
			"a self-live row never reaches the descendant walk")
		require.Contains(t, bh.executedSQLs,
			"update mo_catalog.mo_data_branch_link_retention set database_name = 'db_live', table_name = 't_live', logical_id = 900 where row_kind = 0 and table_id = 800",
			"the kept row is refreshed in place from the live catalog identity")
	})

	t.Run("self-live probe failure fails the reap atomically", func(t *testing.T) {
		bh := newAccount9Harness()
		boom := moerr.NewInternalErrorNoCtx("mo_tables read failed")
		bh.errByAccount[accountKey(9, fmt.Sprintf(ownerSelfLiveFmt, "800"))] = boom
		err := reapDataBranchLinksOfAccount(ctx, bh, "sid-test", nil, 9)
		require.ErrorIs(t, err, boom)
		require.Zero(t, countExecuted(bh.executedSQLs, ownerLinkDeleteSQL))
		require.Zero(t, countExecuted(bh.executedSQLs, ownerLedgerDelete))
	})

	t.Run("ledger logical id selects the logical coverage lane", func(t *testing.T) {
		bh := newAccountAwareExec()
		// The ledger row's durable logical id (700) differs from the
		// physical table id; the retained table snapshot recorded the
		// logical id and grounds the row only in the logical lane.
		bh.byAccount[accountKey(9, ownerLedgerScanSQL)] = newMrsForOwnerRows(6,
			[][]interface{}{{uint64(800), "db1", "t_new", uint64(800), uint64(1700000000), uint64(700)}})
		bh.byAccount[accountKey(9, ownerSnapshotsSQL)] = newMrsForOwnerRows(5, [][]interface{}{
			{"table", "", "db1", "old_t", uint64(700)},
		})
		bh.byAccount[accountKey(0, ownerSnapshotsSQL)] = newMrsForOwnerRows(5, nil)
		bh.byAccount[accountKey(0, ownerPitrsSQL)] = newMrsForOwnerRows(6, nil)
		require.NoError(t, reapDataBranchLinksOfAccount(ctx, bh, "sid-test", nil, 9))
		require.Zero(t, countExecuted(bh.executedSQLs, ownerLinkDeleteSQL))
		require.Zero(t, countExecuted(bh.executedSQLs,
			"select rel_id, reldatabase, relname, coalesce(nullif(rel_logical_id,0),rel_id) from mo_catalog.mo_tables"),
			"a covered row never reaches the self-live probe")
	})
}

// The ledger's capability contract: every wide-first read degrades to the
// pre-upgrade legacy shape on the missing-column signal (and only on it),
// so a heal ALTER takes effect at the very next contact.
func TestReapDataBranchLinksLedgerShapeDegradation(t *testing.T) {
	ctx := context.Background()

	t.Run("wide scan falls back to the legacy projection on the missing-column verdict", func(t *testing.T) {
		bh := &backgroundExecTest{}
		bh.init()
		bh.sql2err[ownerLedgerScanSQL] = moerr.NewBadFieldError(
			context.Background(), "logical_id", "mo_data_branch_link_retention")
		// The legacy projection decodes rows with LogicalID 0 (legacy
		// lane); this uncovered, fully-dead row still retires normally.
		bh.sql2result[ownerLedgerScanLegacySQL] = newMrsForOwnerRows(5,
			[][]interface{}{{uint64(800), "db1", "t800", uint64(800), uint64(1700000000)}})
		bh.sql2result[ownerSnapshotsSQL] = newMrsForOwnerRows(5, nil)
		bh.sql2result[ownerPitrsSQL] = newMrsForOwnerRows(6, nil)
		bh.sql2result[fmt.Sprintf(ownerSelfLiveFmt, "800")] = newMrsForOwnerRows(4, nil)
		bh.sql2result[fmt.Sprintf(ownerLiveCountSQL, 800)] = newMrsForOwnerRows(2, [][]interface{}{{uint64(0), uint64(0)}})
		bh.sql2result[fmt.Sprintf(ownerChildIdsSQL, 800)] = newMrsForOwnerRows(1, nil)
		require.NoError(t, reapDataBranchLinksOfAccount(ctx, bh, "sid-test", nil, 9))
		require.Equal(t, 1, countExecuted(bh.executedSQLs, ownerLedgerScanSQL),
			"the wide attempt ran first")
		require.Equal(t, 1, countExecuted(bh.executedSQLs, ownerLedgerScanLegacySQL),
			"the legacy projection ran on the missing-column signal")
		require.Equal(t, 1, countExecuted(bh.executedSQLs, ownerLinkDeleteSQL))
		require.Equal(t, 1, countExecuted(bh.executedSQLs, ownerLedgerDelete))
	})

	t.Run("ledger row delete retries the legacy shape on the missing-column verdict", func(t *testing.T) {
		bh := &backgroundExecTest{}
		bh.init()
		bh.sql2result[ownerLedgerScanSQL] = newMrsForOwnerRows(6,
			[][]interface{}{{uint64(800), "db1", "t800", uint64(800), uint64(1700000000), uint64(0)}})
		bh.sql2result[ownerSnapshotsSQL] = newMrsForOwnerRows(5, nil)
		bh.sql2result[ownerPitrsSQL] = newMrsForOwnerRows(6, nil)
		bh.sql2result[fmt.Sprintf(ownerSelfLiveFmt, "800")] = newMrsForOwnerRows(4, nil)
		bh.sql2result[fmt.Sprintf(ownerLiveCountSQL, 800)] = newMrsForOwnerRows(2, [][]interface{}{{uint64(0), uint64(0)}})
		bh.sql2result[fmt.Sprintf(ownerChildIdsSQL, 800)] = newMrsForOwnerRows(1, nil)
		bh.sql2err["delete from mo_catalog.mo_data_branch_link_retention where row_kind = 0 and table_id = 800"] =
			moerr.NewBadFieldError(context.Background(), "row_kind", "mo_data_branch_link_retention")
		require.NoError(t, reapDataBranchLinksOfAccount(ctx, bh, "sid-test", nil, 9))
		require.Equal(t, 1, countExecuted(bh.executedSQLs, ownerLinkDeleteSQL))
		require.Contains(t, bh.executedSQLs,
			"delete from mo_catalog.mo_data_branch_link_retention where table_id = 800",
			"the ledger delete retried without the row_kind predicate")
	})
}

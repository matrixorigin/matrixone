// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
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
	"slices"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/defines"
	pbtxn "github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/stretchr/testify/require"
)

type accountDropTestTxn struct {
	TxnOperator
	id      []byte
	cleaned bool
}

func (op *accountDropTestTxn) Txn() pbtxn.TxnMeta {
	return pbtxn.TxnMeta{ID: op.id, Mode: pbtxn.TxnMode_Pessimistic, Isolation: pbtxn.TxnIsolation_RC}
}

func (op *accountDropTestTxn) IsAbortedTxnCleanupComplete(id []byte) bool {
	return op.cleaned && slices.Equal(id, op.id)
}

type accountDropTestExec struct {
	backgroundExecTest
	op *accountDropTestTxn
}

func (bh *accountDropTestExec) getTxn() TxnOperator {
	if bh.op == nil {
		return nil
	}
	return bh.op
}

func newAccountDropTest(t *testing.T) (context.Context, *Session, *accountDropTestExec, string) {
	t.Helper()
	ses := newSes(nil, gomock.NewController(t))
	t.Cleanup(ses.Close)
	ctx := defines.AttachAccountId(context.Background(), 0)
	bh := &accountDropTestExec{}
	bh.init()
	check, err := getSqlForCheckTenant(ctx, "acc")
	require.NoError(t, err)
	bh.sql2result[check] = newMrsForGetAllAccounts([][]interface{}{{uint64(1), "acc", "open", uint64(1), nil}})
	bh.sql2result[fmt.Sprintf(getPubInfoSql, 1)+" order by update_time desc, created_time desc"] = newMrsForSqlForGetPubs(nil)
	bh.sql2result["select 1 from mo_catalog.mo_columns where account_id = 0 and att_database = 'mo_catalog' and att_relname = 'mo_subs' and attname = 'sub_account_name'"] = newMrsForSqlForGetSubs([][]interface{}{{1}})
	bh.sql2result[getSubsSql+" and sub_account_id = 1"] = newMrsForSqlForGetSubs(nil)
	bh.sql2result["show databases;"] = newMrsForSqlForShowDatabases([][]interface{}{{"db1"}})
	bh.sql2result["show tables from mo_catalog;"] = newMrsForShowTables(nil)
	registerEmptyBranchMetadataResult(bh.sql2result)
	return ctx, ses, bh, check
}

func TestDropAccountRetryBoundary(t *testing.T) {
	conflict := moerr.NewLockConflictNoCtx()
	cleanupErr := errors.New("rollback failed")
	semanticErr := errors.New("invalid database")
	for _, tc := range []struct {
		name          string
		conflicts     int
		operationErr  error
		terminalErr   error
		unprovenAbort bool
		borrowed      bool
		replaced      bool
		missing       bool
		commit        bool
		attempts      int
		wantErr       error
	}{
		{name: "success", attempts: 1},
		{name: "transient", conflicts: 1, attempts: 2},
		{name: "bounded contention", conflicts: 10, attempts: 5, wantErr: conflict},
		{name: "semantic error", operationErr: semanticErr, attempts: 1, wantErr: semanticErr},
		{name: "rollback error", conflicts: 1, terminalErr: cleanupErr, attempts: 1, wantErr: cleanupErr},
		{name: "hidden failed or pending abort", conflicts: 1, unprovenAbort: true, attempts: 1, wantErr: conflict},
		{name: "borrowed transaction", conflicts: 1, borrowed: true, attempts: 0, wantErr: conflict},
		{name: "commit conflict", commit: true, terminalErr: conflict, attempts: 1, wantErr: conflict},
		{name: "commit unknown", commit: true, terminalErr: moerr.NewTxnUnknown(context.Background(), "test"), attempts: 1},
		{name: "replacement account", conflicts: 1, replaced: true, attempts: 2},
		{name: "account already removed", conflicts: 1, missing: true, attempts: 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				ctx, ses, bh, check := newAccountDropTest(t)
				begins, drops, commits, rollbacks := 0, 0, 0, 0
				bh.beforeExec = func(statement string) {
					switch {
					case statement == "begin;":
						begins++
						bh.op = &accountDropTestTxn{id: []byte{byte(begins)}}
						if begins > 1 {
							require.Empty(t, ses.rm.accountRoutine.deepCopyKillQueue(), "failed attempt must have no post-commit effects")
							// A new attempt must reload its database list, not append to the old one.
							bh.sql2result["show databases;"] = newMrsForSqlForShowDatabases([][]interface{}{{"db2"}})
							if tc.replaced {
								bh.sql2result[check] = newMrsForGetAllAccounts([][]interface{}{{uint64(2), "acc", "open", uint64(1), nil}})
							}
							if tc.missing {
								bh.sql2result[check] = newMrsForGetAllAccounts(nil)
							}
						}
					case strings.HasPrefix(statement, "drop database if exists"):
						drops++
						delete(bh.sql2err, statement)
						if drops <= tc.conflicts {
							bh.sql2err[statement] = conflict
						} else if tc.operationErr != nil {
							bh.sql2err[statement] = tc.operationErr
						}
					case statement == "commit;":
						commits++
						if tc.commit {
							bh.sql2err[statement] = tc.terminalErr
						}
					case statement == "rollback;":
						rollbacks++
						require.NoError(t, ctx.Err())
						bh.op.cleaned = !tc.unprovenAbort && (tc.terminalErr == nil || tc.commit)
						if !tc.commit {
							bh.sql2err[statement] = tc.terminalErr
						}
					}
				}
				err := doDropAccount(ctx, bh, ses, &dropAccount{Name: "acc", IfExists: tc.missing}, tc.borrowed)
				switch {
				case tc.replaced:
					require.ErrorContains(t, err, "was replaced")
					require.Equal(t, 1, drops)
				case tc.commit && tc.terminalErr != nil:
					require.Equal(t, tc.terminalErr.Error(), err.Error())
				case tc.wantErr != nil:
					require.ErrorIs(t, err, tc.wantErr)
				default:
					require.NoError(t, err)
				}
				require.Equal(t, tc.attempts, begins)
				if tc.borrowed {
					require.Zero(t, commits)
					require.Zero(t, rollbacks)
				} else if err == nil {
					require.Equal(t, 1, commits)
					require.Equal(t, begins-1, rollbacks)
				}
				if err != nil || tc.missing {
					require.Empty(t, ses.rm.accountRoutine.deepCopyKillQueue())
				} else {
					require.Len(t, ses.rm.accountRoutine.deepCopyKillQueue(), 1)
				}
				if tc.name == "transient" {
					require.Equal(t, 2, drops)
					require.Contains(t, bh.executedSQLs, "drop database if exists `db2`;")
				}
			})
		})
	}
}

func TestDropAccountRetryCancellation(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, ses, bh, _ := newAccountDropTest(t)
		ctx, cancel := context.WithCancel(ctx)
		defer cancel()
		begins := 0
		bh.beforeExec = func(statement string) {
			switch statement {
			case "begin;":
				begins++
				bh.op = &accountDropTestTxn{id: []byte{byte(begins)}}
			case "drop database if exists `db1`;":
				bh.sql2err[statement] = moerr.NewLockConflictNoCtx()
			case "rollback;":
				bh.op.cleaned = true
				go func() {
					time.Sleep(time.Millisecond) // virtual time: cancel during the retry wait
					cancel()
				}()
			}
		}
		require.ErrorIs(t, doDropAccount(ctx, bh, ses, &dropAccount{Name: "acc"}), context.Canceled)
		require.Equal(t, 1, begins)
		require.Empty(t, ses.rm.accountRoutine.deepCopyKillQueue())
		// An already-cancelled request cannot start another transaction.
		require.ErrorIs(t, doDropAccount(ctx, bh, ses, &dropAccount{Name: "acc"}), context.Canceled)
		require.Equal(t, 1, begins)
	})
}

// dataBranchCatalogDropSQLs are the teardown statements DROP ACCOUNT owes the
// per-account pk-move provenance catalogs: every tenant receives
// mo_data_branch_update_link and mo_data_branch_link_retention at creation
// (existing tenants from the v4.0.8 tenant upgrade), and both physical
// tables must vanish with the account or their mo_catalog.mo_tables rows
// outlive it (the async-drop integration test counts exactly those
// leftovers).
var dataBranchCatalogDropSQLs = []string{
	"drop table if exists mo_catalog.`mo_data_branch_update_link`;",
	"drop table if exists mo_catalog.`mo_data_branch_link_retention`;",
}

func TestDropAccountRetiresDataBranchCatalogs(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, ses, bh, _ := newAccountDropTest(t)
		bh.beforeExec = func(statement string) {
			switch statement {
			case "begin;":
				bh.op = &accountDropTestTxn{id: []byte{1}}
			case "rollback;":
				bh.op.cleaned = true
			}
		}
		require.NoError(t, doDropAccount(ctx, bh, ses, &dropAccount{Name: "acc"}))
		idx := make(map[string]int, len(bh.executedSQLs))
		for i, s := range bh.executedSQLs {
			if _, dup := idx[s]; !dup {
				idx[s] = i
			}
		}
		dbDrop := "drop database if exists `db1`;"
		icebergDrop := "drop table if exists mo_catalog.mo_iceberg_refs;"
		for _, want := range append([]string{dbDrop, icebergDrop}, dataBranchCatalogDropSQLs...) {
			require.Contains(t, idx, want, "drop account must execute %q", want)
		}
		// The per-database drops still consult both catalogs, and the
		// catalog drop must precede the remaining sink cleanups.
		for _, catalogDrop := range dataBranchCatalogDropSQLs {
			require.Less(t, idx[dbDrop], idx[catalogDrop])
			require.Less(t, idx[catalogDrop], idx[icebergDrop])
		}
	})
}

func TestDropAccountDataBranchCatalogDropFailureAborts(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, ses, bh, _ := newAccountDropTest(t)
		failure := errors.New("link drop failed")
		bh.beforeExec = func(statement string) {
			switch statement {
			case "begin;":
				bh.op = &accountDropTestTxn{id: []byte{1}}
			case "rollback;":
				bh.op.cleaned = true
			}
		}
		bh.sql2err[dataBranchCatalogDropSQLs[0]] = failure
		require.ErrorIs(t, doDropAccount(ctx, bh, ses, &dropAccount{Name: "acc"}), failure)
		// A failed link-catalog drop aborts the sequence: the retention
		// catalog and the sink cleanups must not run half-retired.
		require.NotContains(t, bh.executedSQLs, dataBranchCatalogDropSQLs[1])
		require.NotContains(t, bh.executedSQLs, "drop table if exists mo_catalog.mo_iceberg_refs;")
	})
}

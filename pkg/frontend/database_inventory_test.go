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
	"fmt"
	"math"
	"slices"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/prashantv/gostub"
	"github.com/stretchr/testify/require"
)

// Keep row-count behavior scoped to inventory tests; do not change the shared
// backgroundExecTest or its procedural ROW_COUNT consumers.
type inventoryBackgroundExecTest struct {
	*backgroundExecTest
	lastRows      int64
	affectedBySQL map[string]int64
}

func (b *inventoryBackgroundExecTest) Exec(ctx context.Context, query string) error {
	b.lastRows = -1
	if err := b.backgroundExecTest.Exec(ctx, query); err != nil {
		return err
	}
	if rows, ok := b.affectedBySQL[query]; ok {
		b.lastRows = rows
	}
	return nil
}

func (b *inventoryBackgroundExecTest) ExecWithSQLMode(ctx context.Context, query, mode string) error {
	b.parserSQLMode = mode
	return b.Exec(ctx, query)
}

func (b *inventoryBackgroundExecTest) GetLastAffectedRows() int64  { return b.lastRows }
func (b *inventoryBackgroundExecTest) SetLastAffectedRows(n int64) { b.lastRows = n }

func newInventoryBackgroundExec(account uint32, database string, id, epoch uint64) *inventoryBackgroundExecTest {
	base := &backgroundExecTest{}
	base.init()
	base.sql2result[databaseInventoryReadSQL(account, database)] = newMrsForRestoreStringRows(
		[]string{"dat_id", "epoch"}, [][]interface{}{{id, epoch}})
	query := catalog.DatabaseInventoryIncrementSQL(account, id) + fmt.Sprintf(" and epoch=%d", epoch)
	return &inventoryBackgroundExecTest{backgroundExecTest: base, affectedBySQL: map[string]int64{query: 1}}
}

func TestRoutineDatabaseInventoryToken(t *testing.T) {
	ctx := defines.AttachAccountId(t.Context(), 7)
	for _, tc := range []struct {
		name string
		rows [][]interface{}
		ok   bool
	}{
		{"one identity", [][]interface{}{{uint64(42), uint64(3)}}, true},
		{"absent database", nil, false},
		{"missing inventory", [][]interface{}{{uint64(42), nil}}, false},
		{"duplicate identity", [][]interface{}{{uint64(42), uint64(3)}, {uint64(42), uint64(4)}}, false},
		{"invalid database", [][]interface{}{{uint64(0), uint64(3)}}, false},
		{"zero epoch", [][]interface{}{{uint64(42), uint64(0)}}, false},
		{"exhausted epoch", [][]interface{}{{uint64(42), uint64(math.MaxUint64)}}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			bh := newInventoryBackgroundExec(7, "db", 42, 3)
			const query = "select d.dat_id,i.epoch from mo_catalog.mo_database d left join mo_catalog.mo_database_inventory i on i.account_id=d.account_id and i.database_id=d.dat_id where d.account_id=7 and d.datname='db'"
			bh.sql2result[query] = newMrsForRestoreStringRows([]string{"dat_id", "epoch"}, tc.rows)
			token, err := readDatabaseInventory(ctx, bh, "db")
			if tc.ok {
				require.NoError(t, err)
				require.Equal(t, databaseInventoryToken{accountID: 7, databaseID: 42, epoch: 3, tracked: true}, token)
			} else {
				require.Error(t, err)
				require.Zero(t, token)
			}
			require.Equal(t, []string{query}, bh.executedSQLs)
		})
	}
	t.Run("system exclusion and missing account", func(t *testing.T) {
		bh := newInventoryBackgroundExec(7, "db", 42, 3)
		token, err := readDatabaseInventory(ctx, bh, "MO_CATALOG")
		require.NoError(t, err)
		require.NoError(t, advanceRoutineDatabaseInventory(ctx, bh, token))
		_, err = readDatabaseInventory(context.Background(), bh, "db")
		require.Error(t, err)
		require.Empty(t, bh.executedSQLs)
	})
	t.Run("database name is escaped", func(t *testing.T) {
		require.Contains(t, databaseInventoryReadSQL(7, "a'b"), "d.datname='a''b'")
	})
}

func TestRoutineDatabaseInventoryAdvance(t *testing.T) {
	ctx := defines.AttachAccountId(t.Context(), 7)
	token := databaseInventoryToken{accountID: 7, databaseID: 42, epoch: 3, tracked: true}
	const query = "update mo_catalog.mo_database_inventory set epoch=epoch+1 where account_id=7 and database_id=42 and epoch>0 and epoch<18446744073709551615 and epoch=3"
	for _, rows := range []int64{-1, 0, 1, 2} {
		t.Run(fmt.Sprint(rows), func(t *testing.T) {
			bh := newInventoryBackgroundExec(7, "db", 42, 3)
			bh.lastRows = 1 // A previous successful statement is not evidence.
			bh.affectedBySQL[query] = rows
			err := advanceRoutineDatabaseInventory(ctx, bh, token)
			if rows == 1 {
				require.NoError(t, err)
			} else {
				require.True(t, moerr.IsMoErrCode(err, moerr.ErrTxnNeedRetryWithDefChanged))
			}
			require.Equal(t, []string{query}, bh.executedSQLs)
		})
	}
	t.Run("account mismatch", func(t *testing.T) {
		bh := newInventoryBackgroundExec(7, "db", 42, 3)
		err := advanceRoutineDatabaseInventory(defines.AttachAccountId(ctx, 8), bh, token)
		require.Error(t, err)
		require.Empty(t, bh.executedSQLs)
	})
	t.Run("row count must be observable", func(t *testing.T) {
		bh := &backgroundExecTest{}
		bh.init()
		require.Error(t, advanceRoutineDatabaseInventory(ctx, bh, token))
		require.Empty(t, bh.executedSQLs)
	})
}

func TestRoutineDatabaseInventoryDrop(t *testing.T) {
	for _, kind := range []string{"function", "procedure"} {
		for _, outcome := range []string{"commit", "missing", "inventory conflict", "delete failure", "commit failure"} {
			t.Run(kind+"/"+outcome, func(t *testing.T) {
				ctrl := gomock.NewController(t)
				ses := newSes(nil, ctrl)
				ctx := ses.GetTxnHandler().GetTxnCtx()
				bh := newInventoryBackgroundExec(0, "db", 42, 3)
				stub := gostub.StubFunc(&NewBackgroundExec, bh)
				defer stub.Reset()
				advance := catalog.DatabaseInventoryIncrementSQL(0, 42) + " and epoch=3"
				query, deletion := fmt.Sprintf(checkStoredProcedureArgs, "inventory_proc", "db"), fmt.Sprintf(deleteStoredProcedureFormat, 7)
				declaration := "drop procedure if exists db.inventory_proc"
				bh.sql2result[query] = newMrsForCheckDatabase([][]interface{}{{int64(7)}})
				if kind == "function" {
					declaration = "drop function if exists db.inventory_fn (int)"
					query, deletion = fmt.Sprintf(checkUdfArgs, "inventory_fn", "db"), fmt.Sprintf(deleteUserDefinedFunctionFormat, 7)
					bh.sql2result[query] = newMrsForCheckUdfArgs([][]interface{}{
						{`[{"name":"x","type":"int"}]`, int64(7), `{"import":true,"body":"inventory.py"}`},
					})
					checkDB, err := getSqlForCheckDatabaseByAccount(ctx, "db")
					require.NoError(t, err)
					bh.sql2result[checkDB] = newMrsForCheckDatabase([][]interface{}{{int64(1)}})
				}
				switch outcome {
				case "missing":
					bh.sql2result[query] = newMrsForCheckDatabase(nil)
				case "inventory conflict":
					bh.affectedBySQL[advance] = 0
				case "delete failure":
					bh.sql2err[deletion] = moerr.NewInternalError(ctx, "injected metadata delete failure")
				case "commit failure":
					bh.sql2err["commit;"] = moerr.NewInternalError(ctx, "injected unknown commit")
				}
				stmt, err := parsers.ParseOne(ctx, dialect.MYSQL, declaration, 1)
				require.NoError(t, err)
				defer stmt.Free()
				var removed []string
				if kind == "function" {
					err = doDropFunction(ctx, ses, stmt.(*tree.DropFunction), func(path string) error {
						require.Equal(t, "commit;", bh.executedSQLs[len(bh.executedSQLs)-1])
						removed = append(removed, path)
						return nil
					})
				} else {
					err = doDropProcedure(ctx, ses, stmt.(*tree.DropProcedure))
				}
				if outcome == "commit" || outcome == "missing" {
					require.NoError(t, err)
					require.Contains(t, bh.executedSQLs, "commit;")
				} else {
					require.Error(t, err)
					if outcome != "commit failure" {
						require.Contains(t, bh.executedSQLs, "rollback;")
					}
				}
				if outcome == "missing" {
					require.NotContains(t, bh.executedSQLs, advance)
					require.NotContains(t, bh.executedSQLs, deletion)
				} else if outcome == "inventory conflict" {
					require.Contains(t, bh.executedSQLs, advance)
					require.NotContains(t, bh.executedSQLs, deletion)
				} else {
					require.Contains(t, bh.executedSQLs, deletion)
					require.Less(t, slices.Index(bh.executedSQLs, databaseInventoryReadSQL(0, "db")), slices.Index(bh.executedSQLs, query))
					require.Less(t, slices.Index(bh.executedSQLs, advance), slices.Index(bh.executedSQLs, deletion))
				}
				if kind == "function" && outcome == "commit" {
					require.Equal(t, []string{"inventory.py"}, removed)
				} else {
					require.Empty(t, removed)
				}
			})
		}
	}
}

func TestCompatibilityConfigDatabaseInventoryOwner(t *testing.T) {
	for _, outcome := range []string{"commit", "owner denied", "identity changed", "missing inventory", "inventory conflict", "write failure", "commit unknown"} {
		t.Run(outcome, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			ses := newSes(nil, ctrl)
			defer ses.Close()
			ctx := ses.GetTxnHandler().GetTxnCtx()
			bh := newInventoryBackgroundExec(0, "db", 42, 3)
			stub := gostub.StubFunc(&NewBackgroundExec, bh)
			defer stub.Reset()
			ownerQuery, err := getSqlForCheckDatabaseWithOwner(ctx, "db", 0)
			require.NoError(t, err)
			owner := int64(0)
			if outcome == "owner denied" {
				owner = 1
			}
			bh.sql2result[ownerQuery] = newMrsForRestoreStringRows(
				[]string{"dat_id", "owner"}, [][]interface{}{{uint64(42), owner}})
			advance := catalog.DatabaseInventoryIncrementSQL(0, 42) + " and epoch=3"
			write, err := getSqlForupdateConfigurationByDbNameAndAccountName(ctx, "0.8", sysAccountName, "db", "version_compatibility")
			require.NoError(t, err)
			switch outcome {
			case "identity changed":
				bh.sql2result[databaseInventoryReadSQL(0, "db")] = newMrsForRestoreStringRows(
					[]string{"dat_id", "epoch"}, [][]interface{}{{uint64(43), uint64(3)}})
			case "missing inventory":
				bh.sql2result[databaseInventoryReadSQL(0, "db")] = newMrsForRestoreStringRows(
					[]string{"dat_id", "epoch"}, [][]interface{}{{uint64(42), nil}})
			case "inventory conflict":
				bh.affectedBySQL[advance] = 0
			case "write failure":
				bh.sql2err[write] = moerr.NewInternalError(ctx, "injected compatibility write failure")
			case "commit unknown":
				bh.sql2err["commit;"] = moerr.NewInternalError(ctx, "injected unconfirmed commit")
			}
			err = doAlterDatabaseConfig(ctx, ses, &tree.AlterDataBaseConfig{DbName: "db", UpdateConfig: "0.8", ConfigType: tree.MYSQL_COMPATIBILITY_MODE})
			if outcome == "commit" {
				require.NoError(t, err)
				require.Equal(t, "commit;", bh.executedSQLs[len(bh.executedSQLs)-1])
			} else {
				require.Error(t, err)
				if outcome != "commit unknown" {
					require.Contains(t, bh.executedSQLs, "rollback;")
				}
			}
			begins := 0
			for _, query := range bh.executedSQLs {
				if query == "begin" {
					begins++
				}
			}
			require.Equal(t, 1, begins, "keep the original single write owner")
			if outcome == "owner denied" || outcome == "identity changed" || outcome == "missing inventory" {
				require.NotContains(t, bh.executedSQLs, advance)
				require.NotContains(t, bh.executedSQLs, write)
			} else if outcome == "inventory conflict" {
				require.Contains(t, bh.executedSQLs, advance)
				require.NotContains(t, bh.executedSQLs, write)
			} else {
				require.Contains(t, bh.executedSQLs, write)
				require.Less(t, slices.Index(bh.executedSQLs, ownerQuery), slices.Index(bh.executedSQLs, databaseInventoryReadSQL(0, "db")))
				require.Less(t, slices.Index(bh.executedSQLs, advance), slices.Index(bh.executedSQLs, write))
				require.Equal(t, "", bh.parserSQLMode, "catalog UPDATE uses canonical literal interpretation")
			}
		})
	}
}

func TestCompatibilityConfigCatalogLiterals(t *testing.T) {
	for _, value := range []string{"0.8", "0.8' where 1=1; --\\suffix"} {
		query, err := getSqlForupdateConfigurationByDbNameAndAccountName(t.Context(), value, "acc'\\name", "db", "version_compatibility")
		require.NoError(t, err)
		stmt, err := parsers.ParseOne(t.Context(), dialect.MYSQL, query, 1)
		require.NoError(t, err)
		func() {
			defer stmt.Free()
			update := stmt.(*tree.Update)
			require.Len(t, update.Exprs, 1)
			require.Equal(t, value, update.Exprs[0].Expr.(*tree.NumVal).String())
			var literals []string
			var visit func(tree.Expr)
			visit = func(expr tree.Expr) {
				if and, ok := expr.(*tree.AndExpr); ok {
					visit(and.Left)
					visit(and.Right)
					return
				}
				comparison := expr.(*tree.ComparisonExpr)
				literals = append(literals, comparison.Right.(*tree.NumVal).String())
			}
			visit(update.Where.Expr)
			require.Equal(t, []string{"acc'\\name", "db", "version_compatibility"}, literals)
		}()
	}
}

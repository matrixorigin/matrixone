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
	"testing"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/stretchr/testify/require"
)

func TestCatalogRestoreMetadataOwnership(t *testing.T) {
	for _, family := range []struct {
		name    string
		restore func(context.Context, BackgroundExec) error
	}{
		{"snapshot", func(ctx context.Context, bh BackgroundExec) error {
			return restoreSystemDatabase(ctx, "", bh, "snap", 42, 123, 42)
		}},
		{"pitr", func(ctx context.Context, bh BackgroundExec) error {
			return restoreSystemDatabaseWithPitr(ctx, "", bh, "p", 123, 42)
		}},
		{"cross-account timestamp", func(ctx context.Context, bh BackgroundExec) error {
			return restoreSystemDatabaseFromTS(ctx, "", bh, 123, 42, 43)
		}},
	} {
		t.Run(family.name, func(t *testing.T) {
			for _, scenario := range []string{"enumeration failure", "canceled after enumeration", "clone failure", "sequence read failure", "ordinary and UDF"} {
				t.Run(scenario, func(t *testing.T) {
					ctx, cancel := context.WithCancel(defines.AttachAccountId(context.Background(), 42))
					defer cancel()
					bh := &backgroundExecTest{}
					bh.init()
					kind := catalog.SystemOrdinaryRel
					if scenario == "sequence read failure" {
						kind = catalog.SystemSequenceRel
					}
					list := buildTableInfoListSQL(moCatalog, "", 123, 42)
					bh.sql2result[list] = newMrsForRestoreStringRows([]string{"name", "type", "kind", "view"}, [][]interface{}{
						{"mo_user", "BASE TABLE", kind, ""},
						{"mo_user_defined_function", "BASE TABLE", catalog.SystemOrdinaryRel, ""},
						{"mo_tables", "BASE TABLE", catalog.SystemOrdinaryRel, ""},
					})
					if scenario == "enumeration failure" {
						failure := moerr.NewInternalErrorNoCtx("catalog enumeration failed")
						bh.sql2err[list] = failure
						require.ErrorIs(t, family.restore(ctx, bh), failure)
						require.Equal(t, []string{list}, bh.executedSQLs)
						return
					}
					if scenario == "canceled after enumeration" {
						// The shared result boundary covers both Exec and ExecRestore.
						canceling := &cancelRestoreResults{BackgroundExec: bh, cancel: cancel}
						require.ErrorIs(t, family.restore(ctx, canceling), context.Canceled)
						require.Equal(t, []string{list}, bh.executedSQLs)
						return
					}
					master := fmt.Sprintf(checkTableIsMasterFormat, quoteSQLStringLiteral(moCatalog), quoteSQLStringLiteral("mo_user"))
					bh.sql2result[master] = newMrsForRestoreStringRows([]string{"db", "table"}, nil)
					clone := restoreTableDataByTsSQL(moCatalog, "mo_user", 123)
					if scenario == "ordinary and UDF" {
						require.NoError(t, family.restore(ctx, bh))
						require.Contains(t, bh.executedSQLs, clone)
						require.Contains(t, bh.executedSQLs, MoCatalogMoUserDefinedFunctionDDL)
						require.NotContains(t, bh.executedSQLs, restoreTableDataByTsSQL(moCatalog, "mo_tables", 123))
					} else {
						failureSQL := clone
						failure := moerr.NewNoSuchTableNoCtx("missing_db", "dependency")
						if scenario == "sequence read failure" {
							failureSQL = showCreateTableSQL(moCatalog, "mo_user") + " {MO_TS = 123}"
						}
						bh.sql2err[failureSQL] = failure
						require.ErrorIs(t, family.restore(ctx, bh), failure)
						require.Equal(t, failureSQL, bh.executedSQLs[len(bh.executedSQLs)-1])
						require.NotContains(t, bh.executedSQLs, MoCatalogMoUserDefinedFunctionDDL)
						if scenario == "sequence read failure" {
							require.Equal(t, []string{list, failureSQL}, bh.executedSQLs)
						}
					}
					if scenario != "sequence read failure" {
						for _, sql := range bh.executedSQLs {
							require.NotContains(t, sql, "show create")
						}
					}
				})
			}
		})
	}
}

func TestSnapshotUserDependencyToleranceIsPreserved(t *testing.T) {
	ctx := defines.AttachAccountId(context.Background(), 42)
	bh := &backgroundExecTest{}
	bh.init()
	master := fmt.Sprintf(checkTableIsMasterFormat, quoteSQLStringLiteral("user_db"), quoteSQLStringLiteral("child"))
	bh.sql2result[master] = newMrsForRestoreStringRows([]string{"db", "table"}, nil)
	bh.sql2err[restoreTableDataByTsSQL("user_db", "child", 123)] = moerr.NewNoSuchTableNoCtx("user_db", "parent")
	require.NoError(t, recreateTable(ctx, "", bh, "snap", &tableInfo{dbName: "user_db", tblName: "child"}, 42, 123, false))
}

func TestClusterTableCleanup(t *testing.T) {
	deps := fmt.Sprintf("delete from %s.%s", moCatalog, catalog.MO_VIEW_DEPENDENCIES)
	refresh := fmt.Sprintf("delete from %s.%s", moCatalog, catalog.MO_VIEW_REFRESH)
	for _, tc := range []struct {
		name      string
		account   uint32
		failTable string
		mutations []string
	}{
		{"system", 0, "", []string{deps, refresh, dropTableIfExistsSQL(moCatalog, "custom_cluster")}},
		{"ordinary account", 42, "", nil},
		{"dependency delete failure", 0, catalog.MO_VIEW_DEPENDENCIES, []string{deps}},
		{"refresh delete failure", 0, catalog.MO_VIEW_REFRESH, []string{deps, refresh}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := defines.AttachAccountId(context.Background(), tc.account)
			bh := &backgroundExecTest{}
			bh.init()
			list := buildTableInfoListSQL(moCatalog, "", 0, tc.account)
			bh.sql2result[list] = newMrsForRestoreStringRows([]string{"name", "type", "kind", "view"}, [][]interface{}{
				{"mo_user", "BASE TABLE", catalog.SystemOrdinaryRel, ""},
				{catalog.MO_VIEW_DEPENDENCIES, "CLUSTER TABLE", catalog.SystemClusterRel, ""},
				{catalog.MO_VIEW_REFRESH, "CLUSTER TABLE", catalog.SystemClusterRel, ""},
				{"custom_cluster", "CLUSTER TABLE", catalog.SystemClusterRel, ""},
			})
			var failure error
			if tc.failTable != "" {
				failure = moerr.NewNoSuchTableNoCtx(moCatalog, tc.failTable)
				bh.sql2err[tc.mutations[len(tc.mutations)-1]] = failure
			}
			err := dropClusterTable(ctx, "", bh, "snap", tc.account)
			if failure != nil {
				require.ErrorIs(t, err, failure)
			} else {
				require.NoError(t, err)
			}
			require.Equal(t, append([]string{list}, tc.mutations...), bh.executedSQLs)
		})
	}
}

// Cancel after a successful enumeration returns its metadata, independently of
// whether the restore family reads through Exec or cross-account ExecRestore.
type cancelRestoreResults struct {
	BackgroundExec
	cancel context.CancelFunc
}

func (e *cancelRestoreResults) GetExecResultSet() []interface{} {
	result := e.BackgroundExec.GetExecResultSet()
	e.cancel()
	return result
}

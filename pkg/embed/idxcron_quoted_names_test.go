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

package embed

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/matrixorigin/matrixone/pkg/cnservice"
	"github.com/matrixorigin/matrixone/pkg/common/sqlquote"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/idxcron"
	"github.com/stretchr/testify/require"
)

func TestIdxcronQuotedTableRename(t *testing.T) {
	RunSingleCNBaseClusterTests(t, func(c Cluster) {
		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", cn.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		defer db.Close()
		ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
		defer cancel()
		conn, err := db.Conn(ctx)
		require.NoError(t, err)
		defer conn.Close()
		svc := cn.RawService().(cnservice.Service)
		service := cn.GetServiceConfig().CN.UUID
		catalogCtx := defines.AttachAccountId(ctx, 0)
		cleanup := func(t *testing.T, statement string) {
			t.Helper()
			cleanupCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			res, err := svc.GetSQLExecutor().Exec(defines.AttachAccountId(cleanupCtx, 0), statement, executor.Options{}.WithAccountID(0))
			if err != nil {
				t.Errorf("cleanup %s: %v", statement, err)
				return
			}
			res.Close()
		}
		_, err = conn.ExecContext(ctx, "create database idxcron_quoted_names")
		require.NoError(t, err)
		defer cleanup(t, "drop database if exists idxcron_quoted_names")
		for _, mode := range []string{"", "ANSI_QUOTES", "NO_BACKSLASH_ESCAPES", "ANSI_QUOTES,NO_BACKSLASH_ESCAPES"} {
			t.Run(mode, func(t *testing.T) {
				_, err := conn.ExecContext(ctx, "set sql_mode="+sqlquote.String(mode))
				require.NoError(t, err)
				for _, names := range [][2]string{
					{"合法'\\表", "改名'\\表"},
					{"old'quote", "plain"},
					{"plain", "new'quote"},
					{"old\\", "new\\"},
					{"old`tick", "x' OR 1=1 --"},
				} {
					t.Run(names[0], func(t *testing.T) {
						old := sqlquote.QualifiedIdent("idxcron_quoted_names", names[0])
						newName := sqlquote.QualifiedIdent("idxcron_quoted_names", names[1])
						stmt, err := conn.PrepareContext(ctx, "create table "+old+" (id int)")
						require.NoError(t, err)
						defer stmt.Close()
						_, err = stmt.ExecContext(ctx)
						require.NoError(t, err)
						defer cleanup(t, "drop table if exists "+old+", "+newName)
						_, err = conn.ExecContext(ctx, "insert into "+old+" values (7)")
						require.NoError(t, err)
						var tableID uint64
						seedJob := mode == "" && names[0] == "合法'\\表"
						if seedJob {
							require.NoError(t, conn.QueryRowContext(ctx, "select rel_id from mo_catalog.mo_tables where account_id=0 and reldatabase=? and relname=?", "idxcron_quoted_names", names[0]).Scan(&tableID))
							defer cleanup(t, "delete from mo_catalog.mo_index_update where db_name='idxcron_quoted_names'")
							for _, key := range [][2]uint64{{0, tableID}, {0, tableID + 1}, {42, tableID}} {
								require.NoError(t, idxcron.RegisterUpdate(defines.AttachAccountId(ctx, uint32(key[0])), service, nil, key[1], "idxcron_quoted_names", names[0], "probe", "probe", "{}"))
							}
							require.NoError(t, idxcron.RegisterUpdate(catalogCtx, service, nil, tableID, "idxcron_quoted_names", names[0], "second", "probe", "{}"))
							require.NoError(t, idxcron.RegisterUpdate(catalogCtx, service, nil, tableID, "idxcron_quoted_names", names[0]+"other", "other", "probe", "{}"))
						}
						_, err = conn.ExecContext(ctx, "rename table "+old+" to "+newName)
						require.NoError(t, err)
						var got int
						require.NoError(t, conn.QueryRowContext(ctx, "select id from "+newName).Scan(&got))
						require.Equal(t, 7, got)
						if seedJob {
							var name string
							require.NoError(t, conn.QueryRowContext(ctx, "select count(*) from mo_catalog.mo_index_update where account_id=0 and table_id=? and table_name=?", tableID, names[1]).Scan(&got))
							require.Equal(t, 2, got, "all matching jobs must be renamed")
							require.NoError(t, conn.QueryRowContext(ctx, "select count(*) from mo_catalog.mo_index_update where account_id=0 and table_id=? and table_name=?", tableID, names[0]+"other").Scan(&got))
							require.Equal(t, 1, got, "old-name predicate must protect nonmatching jobs")
							require.NoError(t, conn.QueryRowContext(ctx, "select table_name from mo_catalog.mo_index_update where account_id=0 and table_id=? and index_name='probe'", tableID).Scan(&name))
							require.Equal(t, names[1], name)
							require.NoError(t, conn.QueryRowContext(ctx, "select count(*) from mo_catalog.mo_index_update where db_name='idxcron_quoted_names' and table_name=?", names[0]).Scan(&got))
							require.Equal(t, 2, got, "other table and account rows must remain unchanged")
							_, err = conn.ExecContext(ctx, "create table "+old+" (id int)")
							require.NoError(t, err)
							_, err = conn.ExecContext(ctx, "rename table "+newName+" to "+old)
							require.Error(t, err, "rename collision must not mutate table or job")
							require.NoError(t, conn.QueryRowContext(ctx, "select id from "+newName).Scan(&got))
							require.Equal(t, 7, got)
							require.NoError(t, conn.QueryRowContext(ctx, "select table_name from mo_catalog.mo_index_update where account_id=0 and table_id=? and index_name='probe'", tableID).Scan(&name))
							require.Equal(t, names[1], name)
							rollback := errors.New("rollback quoted table rename")
							rolledBack := sqlquote.QualifiedIdent("idxcron_quoted_names", "rolled'\\back")
							err = svc.GetSQLExecutor().ExecTxn(catalogCtx, func(txn executor.TxnExecutor) error {
								res, err := txn.Exec("rename table "+newName+" to "+rolledBack, executor.StatementOption{})
								if err != nil {
									return err
								}
								res.Close()
								return rollback
							}, executor.Options{}.WithAccountID(0))
							require.ErrorIs(t, err, rollback)
							require.NoError(t, conn.QueryRowContext(ctx, "select id from "+newName).Scan(&got))
							require.Equal(t, 7, got)
							require.NoError(t, conn.QueryRowContext(ctx, "select table_name from mo_catalog.mo_index_update where account_id=0 and table_id=? and index_name='probe'", tableID).Scan(&name))
							require.Equal(t, names[1], name, "table and job rename must roll back together")
							_, err = conn.ExecContext(ctx, "select * from "+rolledBack)
							require.Error(t, err)
							_, err = conn.ExecContext(ctx, "drop table "+old)
							require.NoError(t, err)
						} else {
							require.NoError(t, conn.QueryRowContext(ctx, "select count(*) from mo_catalog.mo_index_update where db_name='idxcron_quoted_names'").Scan(&got))
							require.Zero(t, got, "ordinary rename must not register an index job")
						}
						_, err = conn.ExecContext(ctx, "select * from "+old)
						require.Error(t, err, "old table name must disappear")
					})
				}
			})
		}
		t.Run("catalogCommands", func(t *testing.T) {
			_, err := conn.ExecContext(ctx, "set sql_mode=''")
			require.NoError(t, err)
			otherCtx := defines.AttachAccountId(ctx, 42)
			const tableID = uint64(90129864)
			dbName, tableName, indexName, action := "db'\\name", "table'\\name", "index'\\name", "action'\\name"
			metadata := `{"cfg":{"path":"C:\\index's"}}`
			register := func(ctx context.Context, dbName, indexName, action string) {
				require.NoError(t, idxcron.RegisterUpdate(ctx, service, nil, tableID, dbName, tableName, indexName, action, metadata))
			}
			defer cleanup(t, fmt.Sprintf("delete from mo_catalog.mo_index_update where table_id=%d", tableID))
			register(catalogCtx, dbName, indexName, action)
			register(otherCtx, dbName, indexName, action)
			var gotDB, gotTable, gotIndex, gotAction, gotPath string
			require.NoError(t, conn.QueryRowContext(ctx, "select db_name, table_name, index_name, action, json_extract_string(metadata, '$.cfg.path') from mo_catalog.mo_index_update where account_id=0 and table_id=?", tableID).Scan(&gotDB, &gotTable, &gotIndex, &gotAction, &gotPath))
			require.Equal(t, []string{dbName, tableName, indexName, action, "C:\\index's"}, []string{gotDB, gotTable, gotIndex, gotAction, gotPath})
			factory := idxcron.IndexUpdateTaskExecutorFactory(service, svc.GetEngine(), svc.GetTxnClient(), nil)
			require.NoError(t, factory(catalogCtx, nil))
			var status, message string
			require.NoError(t, conn.QueryRowContext(ctx, "select json_extract_string(status, '$.status'), json_extract_string(status, '$.msg') from mo_catalog.mo_index_update where account_id=0 and table_id=?", tableID).Scan(&status, &message))
			require.Equal(t, idxcron.Status_Error, status)
			require.Contains(t, message, "no plugin registered for action")
			require.Contains(t, message, fmt.Sprintf("%q", action), "status error must preserve the quoted action")
			count := func(account int, want int) {
				var got int
				require.NoError(t, conn.QueryRowContext(ctx, "select count(*) from mo_catalog.mo_index_update where table_id=? and account_id=?", tableID, account).Scan(&got))
				require.Equal(t, want, got)
			}
			register(catalogCtx, dbName, indexName, "second")
			require.NoError(t, idxcron.UnregisterUpdate(catalogCtx, service, nil, tableID, indexName, action))
			count(0, 1)
			count(42, 1)
			require.NoError(t, idxcron.UnregisterUpdate(catalogCtx, service, nil, tableID, indexName, idxcron.Action_Wildcard))
			count(0, 0)
			register(catalogCtx, dbName, indexName, action)
			register(catalogCtx, "different_db", indexName, action)
			require.NoError(t, idxcron.UnregisterUpdateByDbName(catalogCtx, service, nil, dbName))
			count(0, 1)
			count(42, 1)
			require.NoError(t, idxcron.UnregisterUpdateByTableId(catalogCtx, service, nil, tableID))
			count(0, 0)
			count(42, 1)
		})
	})
}

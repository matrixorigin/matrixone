// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package issues

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/matrixorigin/matrixone/pkg/clusterservice"
	"github.com/matrixorigin/matrixone/pkg/cnservice"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/pb/query"
	queryclient "github.com/matrixorigin/matrixone/pkg/queryservice/client"
	"github.com/matrixorigin/matrixone/pkg/tests/testutils"
	"github.com/stretchr/testify/require"
)

// A WHERE clause on an ordinary table is not a CRC32 dependency. Exercise the
// actual source and destination routines on distinct same-build CNs, rather
// than treating successful SQL parsing or a synthetic plan as migration.
func TestCRC32FreePreparedWhereMigratesBetweenCNs(t *testing.T) {
	runAuthenticatedClusterTest(t, func(cluster embed.Cluster) {
		ctx, cancel := context.WithTimeout(t.Context(), 4*time.Minute)
		defer cancel()
		sourceCN, err := cluster.GetCNService(0)
		require.NoError(t, err)
		targetCN, err := cluster.GetCNService(1)
		require.NoError(t, err)
		require.NotEqual(t, sourceCN.ServiceID(), targetCN.ServiceID())

		dbName := "crc32_where_" + strings.ReplaceAll(uuid.NewString(), "-", "")
		openDB := func(cn embed.ServiceOperator, database string) *sql.DB {
			t.Helper()
			db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/%s", cn.GetServiceConfig().CN.Frontend.Port, database))
			require.NoError(t, err)
			db.SetMaxOpenConns(1)
			return db
		}
		admin := openDB(sourceCN, "")
		defer admin.Close()
		execSQLRequire(t, ctx, admin, "create database "+dbName)
		defer func() {
			cleanup, stop := context.WithTimeout(context.Background(), 30*time.Second)
			defer stop()
			_, err := admin.ExecContext(cleanup, "drop database if exists "+dbName)
			require.NoError(t, err)
		}()
		execSQLRequire(t, ctx, admin, "create table "+dbName+".t(id int primary key)")
		execSQLRequire(t, ctx, admin, "insert into "+dbName+".t values(1),(2)")
		commitTS := sourceCN.RawService().(cnservice.Service).GetTxnClient().GetLatestCommitTS()
		require.False(t, commitTS.IsEmpty())
		testutils.WaitLogtailApplied(t, commitTS, targetCN)

		sourceDB := openDB(sourceCN, dbName)
		defer sourceDB.Close()
		targetDB := openDB(targetCN, dbName)
		defer targetDB.Close()
		source, err := sourceDB.Conn(ctx)
		require.NoError(t, err)
		defer source.Close()
		target, err := targetDB.Conn(ctx)
		require.NoError(t, err)
		defer target.Close()
		for _, conn := range []*sql.Conn{source, target} {
			_, err = conn.ExecContext(ctx, "set enable_remap_hint = 0")
			require.NoError(t, err)
		}
		_, err = source.ExecContext(ctx, "prepare crc_where from 'select id from "+dbName+".t where id=1'")
		require.NoError(t, err)
		assertRow := func(conn *sql.Conn) {
			t.Helper()
			rows, err := conn.QueryContext(ctx, "execute crc_where")
			require.NoError(t, err)
			defer rows.Close()
			var got []int
			for rows.Next() {
				var id int
				require.NoError(t, rows.Scan(&id))
				got = append(got, id)
			}
			require.NoError(t, rows.Err())
			require.Equal(t, []int{1}, got)
		}
		assertRow(source)

		var sourceID, targetID uint32
		require.NoError(t, source.QueryRowContext(ctx, "select connection_id()").Scan(&sourceID))
		require.NoError(t, target.QueryRowContext(ctx, "select connection_id()").Scan(&targetID))
		inventory, err := clusterservice.GetMOClusterWithContext(ctx, sourceCN.ServiceID())
		require.NoError(t, err)
		refresher, ok := inventory.(clusterservice.AuthoritativeRefresher)
		require.True(t, ok)
		require.NoError(t, refresher.Refresh(ctx))
		address := func(cn embed.ServiceOperator) string {
			t.Helper()
			var result string
			require.NoError(t, clusterservice.GetCNServiceRawWithContext(ctx, inventory,
				clusterservice.NewServiceIDSelector(cn.ServiceID()), func(service metadata.CNService) bool {
					result = service.QueryAddress
					return false
				}))
			require.NotEmpty(t, result)
			return result
		}
		sourceAddress, targetAddress := address(sourceCN), address(targetCN)
		require.NotEqual(t, sourceAddress, targetAddress)
		client, err := queryclient.NewQueryClient(sourceCN.ServiceID(), sourceCN.GetServiceConfig().CN.RPC)
		require.NoError(t, err)
		defer client.Close()
		request := client.NewRequest(query.CmdMethod_MigrateConnFrom)
		request.MigrateConnFromRequest = &query.MigrateConnFromRequest{
			ConnID: sourceID, TempTableMigrationSupported: true, LastInsertIDMigrationSupported: true,
			PreparedStatementProtocolSupported: true,
		}
		response, err := client.SendMessage(ctx, sourceAddress, request)
		if response != nil {
			defer client.Release(response)
		}
		// A failed admission must not damage the original prepared handle.
		assertRow(source)
		require.NoError(t, err)
		require.NotNil(t, response)
		snapshot := response.MigrateConnFromResponse
		require.NotNil(t, snapshot)
		require.Len(t, snapshot.PrepareStmts, 1)
		require.Equal(t, "crc_where", snapshot.PrepareStmts[0].Name)
		require.True(t, snapshot.PreparedStmtLongDataChecked)
		require.True(t, snapshot.PreparedStmtCursorsChecked)
		require.True(t, snapshot.LastInsertIDExported)
		require.True(t, snapshot.UserDefinedVarsExported)
		require.True(t, snapshot.SystemVariablesExported)
		require.False(t, snapshot.UserDefinedVarsSnapshotTooLarge)
		require.False(t, snapshot.SystemVariablesSnapshotTooLarge)
		require.Empty(t, snapshot.UserLevelLocks)

		request = client.NewRequest(query.CmdMethod_MigrateConnTo)
		request.MigrateConnToRequest = &query.MigrateConnToRequest{
			ConnID: targetID, DB: snapshot.DB, PrepareStmts: snapshot.PrepareStmts,
			LastAffectedRows: snapshot.LastAffectedRows, LastInsertID: snapshot.LastInsertID,
			LastInsertIDExported: snapshot.LastInsertIDExported, FoundRows: snapshot.FoundRows,
			UserDefinedVars: snapshot.UserDefinedVars, UserDefinedVarsExported: snapshot.UserDefinedVarsExported,
			UserDefinedVarsReplayable: snapshot.UserDefinedVarsReplayable,
			SystemVariables:           snapshot.SystemVariables, SystemVariablesExported: snapshot.SystemVariablesExported,
			SystemVariablesReplayable: snapshot.SystemVariablesReplayable, TempTables: snapshot.TempTables,
		}
		response, err = client.SendMessage(ctx, targetAddress, request)
		if response != nil {
			defer client.Release(response)
		}
		require.NoError(t, err)
		require.NotNil(t, response)
		require.NotNil(t, response.MigrateConnToResponse)
		require.True(t, response.MigrateConnToResponse.Success)
		// No target PREPARE is issued by the test; replay must install the handle.
		assertRow(target)
		assertRow(source)
	})
}

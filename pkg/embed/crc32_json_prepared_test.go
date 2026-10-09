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
	"fmt"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/matrixorigin/matrixone/pkg/clusterservice"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/pb/query"
	qclient "github.com/matrixorigin/matrixone/pkg/queryservice/client"
	"github.com/stretchr/testify/require"
)

// Reuse the package's single-CN fixture; two rows distinguish runtime JSON
// evaluation from folding and one schema change exercises prepared reprepare.
func TestCRC32JSONBinaryPrepared(t *testing.T) {
	RunSingleCNBaseClusterTests(t, func(c Cluster) {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
		defer cancel()
		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		cfg := mysql.NewConfig()
		cfg.User = "dump"
		cfg.Passwd = "111"
		cfg.Net = "tcp"
		cfg.Addr = fmt.Sprintf("127.0.0.1:%d", cn.GetServiceConfig().CN.Frontend.Port)
		db, err := sql.Open("mysql", cfg.FormatDSN())
		require.NoError(t, err)
		defer db.Close()
		db.SetMaxOpenConns(1)
		conn, err := db.Conn(ctx)
		require.NoError(t, err)
		defer conn.Close()
		exec := func(query string) { t.Helper(); _, err := conn.ExecContext(ctx, query); require.NoError(t, err) }
		exec("create database crc32_prepared_contract")
		defer func() {
			cleanup, stop := context.WithTimeout(context.Background(), 5*time.Second)
			defer stop()
			_, _ = conn.ExecContext(cleanup, "drop database crc32_prepared_contract")
		}()
		exec("use crc32_prepared_contract")
		exec("create table t(id int primary key,j json)")
		exec(`insert into t values(1,'{"t1":"a"}'),(2,'{"t1":"b"}')`)
		stmt, err := conn.PrepareContext(ctx, "select crc32(j) from t where id=?")
		require.NoError(t, err)
		defer stmt.Close()
		check := func(id int, want uint64) {
			t.Helper()
			var got uint64
			require.NoError(t, stmt.QueryRowContext(ctx, id).Scan(&got))
			require.Equal(t, want, got)
		}
		check(1, 4012824821)
		check(2, 3983042220)
		exec("alter table t add column other int")
		check(1, 4012824821)

		// Exercise the added function BVT's generated value and index contract
		// through the same live SQL frontend, including every mutation form.
		// The embedded cluster becomes query-ready before the durable catalog
		// barrier finishes. Observe the real CN admission result; do not set a
		// runtime version or floor to manufacture authoring permission.
		require.Eventually(t, func() bool {
			rt := moruntime.ServiceRuntime(cn.ServiceID())
			if rt == nil {
				return false
			}
			value, ok := rt.GetGlobalVariables(moruntime.PersistedExpressionProtocolAuthoringFloor)
			floor, valid := value.(int64)
			return ok && valid && floor >= defines.MORPCVersion109
		}, 90*time.Second, 100*time.Millisecond, "durable CRC32 catalog authoring admission did not complete")
		exec("create table generated_crc(id int primary key,j json,c bigint unsigned generated always as (crc32(j)) stored,index idx_crc(c))")
		exec(`insert into generated_crc(id,j) values(1,'{"t1":"a"}')`)
		exec("insert into generated_crc(id,j) select 2,j from generated_crc where id=1")
		exec(`update generated_crc set j='{"t1":"b"}' where id=2`)
		exec(`replace into generated_crc(id,j) values(1,'{"t1":"c"}')`)
		exec(`insert into generated_crc(id,j) values(2,'{"t1":"a"}') on duplicate key update j=values(j)`)
		checkGenerated := func() {
			t.Helper()
			for id, want := range map[int]uint64{1: 3970567323, 2: 4012824821} {
				var stored, fresh uint64
				require.NoError(t, conn.QueryRowContext(ctx, "select c,crc32(j) from generated_crc where id=?", id).Scan(&stored, &fresh))
				require.Equal(t, want, stored)
				require.Equal(t, want, fresh)
			}
		}
		checkGenerated()
		exec("prepare crc32_read from 'select c,crc32(j) from generated_crc where id=?'")
		exec("set @crc32_id=2")
		var preparedStored, preparedFresh uint64
		require.NoError(t, conn.QueryRowContext(ctx, "execute crc32_read using @crc32_id").Scan(&preparedStored, &preparedFresh))
		require.Equal(t, uint64(4012824821), preparedStored)
		require.Equal(t, preparedStored, preparedFresh)
		exec("deallocate prepare crc32_read")
		exec("alter table generated_crc add column note int")
		checkGenerated()
		exec("create unique index uniq_crc on generated_crc(c)")
		exec(`insert ignore into generated_crc(id,j) values(3,'{"t1":"a"}')`)
		var count int
		require.NoError(t, conn.QueryRowContext(ctx, "select count(*) from generated_crc").Scan(&count))
		require.Equal(t, 2, count)

		// Real SQL/binary PREPARE -> CN query RPC -> frontend lifecycle entry.
		// This is one current binary, not evidence of a real old/new rollout.
		queryClient, err := qclient.NewQueryClient(cn.ServiceID(), cn.GetServiceConfig().CN.RPC)
		require.NoError(t, err)
		defer func() { require.NoError(t, queryClient.Close()) }()
		// PortBase configurations advertise the address allocated by the running
		// CN, not the legacy query-service config field. Use its admitted identity.
		cluster, err := clusterservice.GetMOClusterWithContext(ctx, cn.ServiceID())
		require.NoError(t, err)
		var queryAddress string
		require.NoError(t, clusterservice.GetCNServiceWithoutWorkingStateWithContext(
			ctx, cluster, clusterservice.NewServiceIDSelector(cn.ServiceID()), func(service metadata.CNService) bool {
				queryAddress = service.QueryAddress
				return false
			}))
		require.NotEmpty(t, queryAddress, "running CN query endpoint was not advertised")
		var sourceID uint32
		require.NoError(t, conn.QueryRowContext(ctx, "select connection_id()").Scan(&sourceID))
		export := func() (*query.Response, error) {
			request := queryClient.NewRequest(query.CmdMethod_MigrateConnFrom)
			request.MigrateConnFromRequest = &query.MigrateConnFromRequest{
				ConnID: sourceID, TempTableMigrationSupported: true, LastInsertIDMigrationSupported: true,
			}
			return queryClient.SendMessage(ctx, queryAddress, request)
		}
		_, err = export() // the live COM_STMT prepared crc32(j)
		require.True(t, moerr.IsMoErrCode(err, moerr.OkExpectedNotSafeToStartTransfer), "binary CRC32 migration rejection: %v", err)
		check(1, 4012824821) // rejection must leave the original handle executable
		require.NoError(t, stmt.Close())
		exec(`prepare crc32_migration from 'select crc32(cast(''{"t1":"a"}'' as json))'`)
		_, err = export()
		require.True(t, moerr.IsMoErrCode(err, moerr.OkExpectedNotSafeToStartTransfer), "SQL CRC32 migration rejection: %v", err)
		var checksum uint64
		require.NoError(t, conn.QueryRowContext(ctx, "execute crc32_migration").Scan(&checksum))
		require.Equal(t, uint64(4012824821), checksum)
		exec("deallocate prepare crc32_migration")
		plain, err := conn.PrepareContext(ctx, "select crc32('abc')")
		require.NoError(t, err)
		defer plain.Close()
		exec(`set @digest=crc32(cast('{"t1":"a"}' as json))`)
		snapshot, err := export()
		require.NoError(t, err)
		func() {
			defer queryClient.Release(snapshot)
			require.NotNil(t, snapshot.MigrateConnFromResponse)
			require.Len(t, snapshot.MigrateConnFromResponse.PrepareStmts, 1)
			foundDigest := false
			for _, variable := range snapshot.MigrateConnFromResponse.UserDefinedVars {
				if variable.Name == "digest" {
					foundDigest = true
					require.NotNil(t, variable.Value.GetLit())
					require.Equal(t, uint64(4012824821), variable.Value.GetLit().GetU64Val())
				}
			}
			require.True(t, foundDigest)
		}()
		require.NoError(t, plain.Close())

		// An identity-less old-source payload is sent to an actual target routine.
		// It must reject the later unsafe statement before even the earlier USE
		// or safe PREPARE. No old binary or protocol value is manufactured here.
		exec("create table migration_identity(id bigint unsigned auto_increment primary key, marker int)")
		targetDB, err := sql.Open("mysql", cfg.FormatDSN())
		require.NoError(t, err)
		defer targetDB.Close()
		targetDB.SetMaxOpenConns(1)
		target, err := targetDB.Conn(ctx)
		require.NoError(t, err)
		defer target.Close()
		_, err = target.ExecContext(ctx, "set @keep=42")
		require.NoError(t, err)
		// MatrixOne supports the zero-argument last_insert_id() reader. Seed the
		// real session counter through INSERT without selecting a target database.
		_, err = target.ExecContext(ctx, "insert into crc32_prepared_contract.migration_identity(marker) values(1)")
		require.NoError(t, err)
		var targetID uint32
		var initialLastInsertID uint64
		var initialDatabase sql.NullString
		require.NoError(t, target.QueryRowContext(ctx, "select database(),connection_id(),last_insert_id()").Scan(&initialDatabase, &targetID, &initialLastInsertID))
		// Keep USE observable even if an unselected database is represented by
		// a non-NULL empty string instead of SQL NULL.
		require.NotEqual(t, "crc32_prepared_contract", initialDatabase.String)
		require.Equal(t, uint64(1), initialLastInsertID)
		request := queryClient.NewRequest(query.CmdMethod_MigrateConnTo)
		request.MigrateConnToRequest = &query.MigrateConnToRequest{
			ConnID: targetID, DB: "crc32_prepared_contract", LastInsertIDExported: true, LastInsertID: 91,
			PrepareStmts: []*query.PrepareStmt{
				{Name: "safe_first", SQL: "select 1"}, {Name: "unsafe_later", SQL: "select crc32(cast(? as json))"},
			},
		}
		_, err = queryClient.SendMessage(ctx, queryAddress, request)
		require.True(t, moerr.IsMoErrCode(err, moerr.OkExpectedNotSafeToStartTransfer), "receiver CRC32 migration rejection: %v", err)
		var database sql.NullString
		var keep int64
		require.NoError(t, target.QueryRowContext(ctx, "select database(),@keep,last_insert_id()").Scan(&database, &keep, &checksum))
		require.Equal(t, initialDatabase, database, "rejection must precede USE")
		require.Equal(t, int64(42), keep)
		require.Equal(t, initialLastInsertID, checksum, "rejection must precede counter restoration")
		_, err = target.ExecContext(ctx, "deallocate prepare safe_first")
		var sqlErr *mysql.MySQLError
		require.ErrorAs(t, err, &sqlErr)
		require.Equal(t, uint16(1243), sqlErr.Number, "the earlier safe handle must not have been installed")
	})
}

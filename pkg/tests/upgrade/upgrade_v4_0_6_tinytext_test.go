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

package upgrade

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"testing"
	"time"

	mysqlDriver "github.com/go-sql-driver/mysql"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/cnservice"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/stretchr/testify/require"
)

func TestV406UpgradeRecoversLegacyTinyText(t *testing.T) {
	embed.RunBaseClusterTests(t, func(cluster embed.Cluster) {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
		defer cancel()

		cn, err := cluster.GetCNService(0)
		require.NoError(t, err)
		port := cn.GetServiceConfig().CN.Frontend.Port
		db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", port))
		require.NoError(t, err)
		defer db.Close()

		conn, err := db.Conn(ctx)
		require.NoError(t, err)
		defer conn.Close()

		const (
			databaseName = "tinytext_upgrade_26687"
			sourceTable  = "legacy_source"
			cloneTable   = "legacy_clone"
		)
		_, _ = conn.ExecContext(ctx, "drop database if exists "+databaseName)
		defer func() {
			cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cleanupCancel()
			_, _ = conn.ExecContext(cleanupCtx, "drop database if exists "+databaseName)
		}()

		mustExecTinyTextUpgradeSQL(t, ctx, conn, "set role moadmin")
		mustExecTinyTextUpgradeSQL(t, ctx, conn, "create database "+databaseName)
		mustExecTinyTextUpgradeSQL(t, ctx, conn,
			"create table "+databaseName+"."+sourceTable+" (id int primary key, payload tinytext)")

		// Recreate the durable state emitted by a pre-fix binary through the
		// engine's schema-change protocol: Createsql says TINYTEXT, atttyp says
		// T_text/Width=0, and an oversized row already exists. Direct catalog
		// UPDATE is deliberately avoided because system metadata changes must go
		// through the full delete/create protocol.
		writeLegacyTinyTextCatalogAndRow(
			t,
			ctx,
			cn.RawService().(cnservice.Service),
			databaseName,
			sourceTable,
		)

		require.Equal(t, int64(0), tinyTextCatalogWidth(t, ctx, conn, databaseName, sourceTable, "payload"))

		// The metadata-only recovery deliberately preserves legacy oversized
		// values. The recovered bound applies when a value is written again.
		var existingLength int
		require.NoError(t, conn.QueryRowContext(
			ctx,
			"select length(payload) from "+databaseName+"."+sourceTable+" where id = 1",
		).Scan(&existingLength))
		require.Equal(t, 1000, existingLength)

		sourceDDL := showCreateTableSQL(t, ctx, conn, databaseName, sourceTable)
		require.Contains(t, strings.ToUpper(sourceDDL), "TINYTEXT")

		mustExecTinyTextUpgradeSQL(t, ctx, conn,
			"create table "+databaseName+"."+cloneTable+" like "+databaseName+"."+sourceTable)
		cloneDDL := showCreateTableSQL(t, ctx, conn, databaseName, cloneTable)
		require.Contains(t, strings.ToUpper(cloneDDL), "TINYTEXT")
		require.Equal(t, int64(255), tinyTextCatalogWidth(t, ctx, conn, databaseName, cloneTable, "payload"))

		mustExecTinyTextUpgradeSQL(t, ctx, conn, "set session sql_mode = 'STRICT_TRANS_TABLES'")
		_, err = conn.ExecContext(
			ctx,
			"insert into "+databaseName+"."+sourceTable+" values (2, repeat('s', 256))",
		)
		var mysqlErr *mysqlDriver.MySQLError
		require.ErrorAs(t, err, &mysqlErr)
		require.Equal(t, uint16(1406), mysqlErr.Number)

		_, err = conn.ExecContext(
			ctx,
			"insert into "+databaseName+"."+cloneTable+" values (2, repeat('s', 256))",
		)
		require.ErrorAs(t, err, &mysqlErr)
		require.Equal(t, uint16(1406), mysqlErr.Number)
		_, err = conn.ExecContext(
			ctx,
			"update "+databaseName+"."+sourceTable+" set payload = repeat('u', 256) where id = 1",
		)
		require.ErrorAs(t, err, &mysqlErr)
		require.Equal(t, uint16(1406), mysqlErr.Number)
		require.NoError(t, conn.QueryRowContext(
			ctx,
			"select length(payload) from "+databaseName+"."+sourceTable+" where id = 1",
		).Scan(&existingLength))
		require.Equal(t, 1000, existingLength)

		mustExecTinyTextUpgradeSQL(t, ctx, conn, "set session sql_mode = ''")
		mustExecTinyTextUpgradeSQL(t, ctx, conn,
			"insert into "+databaseName+"."+sourceTable+" values (2, repeat('n', 1000))")
		mustExecTinyTextUpgradeSQL(t, ctx, conn,
			"insert into "+databaseName+"."+cloneTable+" values (2, repeat('n', 1000))")
		mustExecTinyTextUpgradeSQL(t, ctx, conn,
			"update "+databaseName+"."+sourceTable+" set payload = repeat('u', 1000) where id = 1")
		require.NoError(t, conn.QueryRowContext(
			ctx,
			"select length(payload) from "+databaseName+"."+sourceTable+" where id = 1",
		).Scan(&existingLength))
		require.Equal(t, 255, existingLength)

		for _, tableName := range []string{sourceTable, cloneTable} {
			var storedLength int
			require.NoError(t, conn.QueryRowContext(
				ctx,
				"select length(payload) from "+databaseName+"."+tableName+" where id = 2",
			).Scan(&storedLength))
			require.Equal(t, 255, storedLength)
		}
	})
}

func writeLegacyTinyTextCatalogAndRow(
	t *testing.T,
	ctx context.Context,
	service cnservice.Service,
	databaseName string,
	tableName string,
) {
	t.Helper()
	ctx = defines.AttachAccount(ctx, catalog.System_Account, catalog.System_User, catalog.System_Role)
	txn, err := service.GetTxnClient().New(ctx, timestamp.Timestamp{})
	require.NoError(t, err)
	committed := false
	defer func() {
		if !committed {
			_ = txn.Rollback(ctx)
		}
	}()

	eng := service.GetEngine()
	require.NoError(t, eng.New(ctx, txn))
	database, err := eng.Database(ctx, databaseName, txn)
	require.NoError(t, err)
	relation, err := database.Relation(ctx, tableName, nil)
	require.NoError(t, err)

	legacyDef := plan2.DeepCopyTableDef(relation.GetTableDef(ctx), true)
	payload := plan2.FindColumn(legacyDef.Cols, "payload")
	require.NotNil(t, payload)
	require.Equal(t, int32(types.MaxTinyTextLen), payload.Typ.Width)
	payload.Typ.Width = 0
	require.NoError(t, relation.AlterTable(
		ctx,
		nil,
		[]*api.AlterTableReq{
			api.NewReplaceDefReq(relation.GetDBID(ctx), relation.GetTableID(ctx), legacyDef),
		},
	))

	mp := mpool.MustNewZero()
	legacyRow := batch.NewWithSize(2)
	legacyRow.Attrs = []string{"id", "payload"}
	legacyRow.Vecs[0] = vector.NewVec(types.T_int32.ToType())
	legacyRow.Vecs[1] = vector.NewVec(types.T_text.ToType())
	require.NoError(t, vector.AppendFixed(legacyRow.Vecs[0], int32(1), false, mp))
	require.NoError(t, vector.AppendBytes(legacyRow.Vecs[1], []byte(strings.Repeat("x", 1000)), false, mp))
	legacyRow.SetRowCount(1)
	defer legacyRow.Clean(mp)
	require.NoError(t, relation.Write(ctx, legacyRow))

	require.NoError(t, txn.Commit(ctx))
	committed = true
}

func mustExecTinyTextUpgradeSQL(
	t *testing.T,
	ctx context.Context,
	conn *sql.Conn,
	statement string,
) {
	t.Helper()
	_, err := conn.ExecContext(ctx, statement)
	require.NoError(t, err, statement)
}

func tinyTextCatalogWidth(
	t *testing.T,
	ctx context.Context,
	conn *sql.Conn,
	databaseName string,
	tableName string,
	columnName string,
) int64 {
	t.Helper()
	var width int64
	require.NoError(t, conn.QueryRowContext(
		ctx,
		fmt.Sprintf(
			"select internal_char_length(atttyp) from %s.%s "+
				"where %s = '%s' and %s = '%s' and %s = '%s' and %s = 0",
			catalog.MO_CATALOG,
			catalog.MO_COLUMNS,
			catalog.SystemColAttr_DBName,
			databaseName,
			catalog.SystemColAttr_RelName,
			tableName,
			catalog.SystemColAttr_Name,
			columnName,
			catalog.SystemColAttr_AccID,
		),
	).Scan(&width))
	return width
}

func showCreateTableSQL(
	t *testing.T,
	ctx context.Context,
	conn *sql.Conn,
	databaseName string,
	tableName string,
) string {
	t.Helper()
	var name string
	var ddl string
	require.NoError(t, conn.QueryRowContext(
		ctx,
		"show create table "+databaseName+"."+tableName,
	).Scan(&name, &ddl))
	return ddl
}

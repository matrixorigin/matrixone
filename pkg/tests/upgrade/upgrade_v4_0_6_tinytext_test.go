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
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
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
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/stretchr/testify/require"
)

func TestV406UpgradeRecoversLegacyTinyText(t *testing.T) {
	embed.RunSingleCNBaseClusterTests(t, func(cluster embed.Cluster) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
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
			databaseName       = "tinytext_upgrade_26687"
			sourceTable        = "legacy_source"
			cloneTable         = "legacy_clone"
			renamedTable       = "legacy_renamed"
			modifiedTable      = "legacy_modified"
			readdedTable       = "legacy_readded"
			cloneCopyTable     = "legacy_clone_copy"
			renamedCopyTable   = "legacy_renamed_copy"
			modifiedCopyTable  = "legacy_modified_copy"
			readdedCopyTable   = "legacy_readded_copy"
			assignTarget       = "fresh_assign_target"
			ignoreTarget       = "fresh_ignore_target"
			updateTarget       = "fresh_update_target"
			alterCopyTable     = "legacy_alter_copy"
			strictCTASTable    = "strict_ctas_target"
			nonStrictCTASTable = "nonstrict_ctas_target"
			stageName          = "tinytext_upgrade_26687_stage"
			dumpPath           = "legacy-like"
			legacyCloneSQL     = "CREATE TABLE " + databaseName + "." + cloneTable + " LIKE " + databaseName + "." + sourceTable
			legacyRenamedSQL   = "CREATE TABLE " + databaseName + "." + renamedTable + " (id INT PRIMARY KEY, payload TINYTEXT)"
			legacyModifiedSQL  = "CREATE TABLE " + databaseName + "." + modifiedTable + " (id INT PRIMARY KEY, payload TINYTEXT)"
			legacyReaddedSQL   = "CREATE TABLE " + databaseName + "." + readdedTable + " (id INT PRIMARY KEY, payload TINYTEXT)"
			renamedColumnName  = "renamed_payload"
		)
		_, _ = conn.ExecContext(ctx, "drop database if exists "+databaseName)
		_, _ = conn.ExecContext(ctx, "drop stage if exists "+stageName)
		defer func() {
			cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cleanupCancel()
			_, _ = conn.ExecContext(cleanupCtx, "drop database if exists "+databaseName)
			_, _ = conn.ExecContext(cleanupCtx, "drop stage if exists "+stageName)
		}()

		mustExecTinyTextUpgradeSQL(t, ctx, conn, "set role moadmin")
		mustExecTinyTextUpgradeSQL(t, ctx, conn, "create database "+databaseName)
		mustExecTinyTextUpgradeSQL(t, ctx, conn, "use "+databaseName)
		mustExecTinyTextUpgradeSQL(t, ctx, conn,
			"create table "+databaseName+"."+sourceTable+" (id int primary key, payload tinytext)")
		mustExecTinyTextUpgradeSQL(t, ctx, conn, legacyCloneSQL)
		mustExecTinyTextUpgradeSQL(t, ctx, conn, legacyRenamedSQL)
		mustExecTinyTextUpgradeSQL(t, ctx, conn,
			legacyModifiedSQL)
		mustExecTinyTextUpgradeSQL(t, ctx, conn, legacyReaddedSQL)
		mustExecTinyTextUpgradeSQL(t, ctx, conn,
			"create table "+databaseName+"."+alterCopyTable+" (id int primary key, payload tinytext)")

		// Recreate version-zero definitions emitted by a pre-fix binary through
		// the engine's delete/create protocol: Createsql says TINYTEXT while
		// atttyp says T_text/Width=0. Direct catalog UPDATE is deliberately
		// avoided because system metadata changes must follow the full DDL path.
		writeLegacyTinyTextCatalogAndRow(
			t,
			ctx,
			cn.RawService().(cnservice.Service),
			databaseName,
			sourceTable,
		)
		writeLegacyTinyTextCatalogAndRow(
			t,
			ctx,
			cn.RawService().(cnservice.Service),
			databaseName,
			alterCopyTable,
		)
		replaceLegacyTinyTextCatalog(
			t,
			ctx,
			cn.RawService().(cnservice.Service),
			databaseName,
			cloneTable,
			legacyCloneSQL,
			"payload",
		)
		replaceLegacyTinyTextCatalog(
			t,
			ctx,
			cn.RawService().(cnservice.Service),
			databaseName,
			renamedTable,
			legacyRenamedSQL,
			"payload",
		)
		replaceLegacyTinyTextCatalog(
			t, ctx, cn.RawService().(cnservice.Service), databaseName,
			modifiedTable, legacyModifiedSQL, "payload",
		)
		replaceLegacyTinyTextCatalog(
			t, ctx, cn.RawService().(cnservice.Service), databaseName,
			readdedTable, legacyReaddedSQL, "payload",
		)

		// These three schema changes reproduce the exact ambiguous pre-fix
		// states. Rename preserves the lossy physical type, Seqnum, and physical
		// identity. MODIFY and DROP/ADD deliberately choose TEXT through a copy
		// ALTER whose new TblId retains the original LogicalId.
		renameLegacyTinyTextColumn(
			t, ctx, cn.RawService().(cnservice.Service), databaseName,
			renamedTable, "payload", renamedColumnName,
		)
		mustExecTinyTextUpgradeSQL(t, ctx, conn,
			"alter table "+databaseName+"."+modifiedTable+" modify payload text")
		mustExecTinyTextUpgradeSQL(t, ctx, conn,
			"alter table "+databaseName+"."+readdedTable+" drop column payload")
		mustExecTinyTextUpgradeSQL(t, ctx, conn,
			"alter table "+databaseName+"."+readdedTable+" add column payload text")
		for _, tableName := range []string{modifiedTable, readdedTable} {
			require.Equal(t, int64(0), tinyTextCatalogVersion(t, ctx, conn, databaseName, tableName))
			tableID, logicalID := tinyTextCatalogIdentity(t, ctx, conn, databaseName, tableName)
			require.NotEqual(t, logicalID, tableID)
		}
		restoreLegacyTinyTextCreateSQL(
			t, ctx, cn.RawService().(cnservice.Service), databaseName,
			modifiedTable, legacyModifiedSQL,
		)
		restoreLegacyTinyTextCreateSQL(
			t, ctx, cn.RawService().(cnservice.Service), databaseName,
			readdedTable, legacyReaddedSQL,
		)

		require.Equal(t, int64(types.MaxStringSize), tinyTextCatalogWidth(t, ctx, conn, databaseName, sourceTable, "payload"))
		require.Equal(t, int64(types.MaxStringSize), tinyTextCatalogWidth(t, ctx, conn, databaseName, alterCopyTable, "payload"))
		require.Equal(t, int64(types.MaxStringSize), tinyTextCatalogWidth(t, ctx, conn, databaseName, cloneTable, "payload"))
		require.Equal(t, int64(types.MaxStringSize), tinyTextCatalogWidth(t, ctx, conn, databaseName, renamedTable, renamedColumnName))
		require.Equal(t, int64(types.MaxStringSize), tinyTextCatalogWidth(t, ctx, conn, databaseName, modifiedTable, "payload"))
		require.Equal(t, int64(types.MaxStringSize), tinyTextCatalogWidth(t, ctx, conn, databaseName, readdedTable, "payload"))
		require.Equal(t, int64(0), tinyTextCatalogVersion(t, ctx, conn, databaseName, sourceTable))
		require.Equal(t, int64(0), tinyTextCatalogVersion(t, ctx, conn, databaseName, cloneTable))
		require.Equal(t, int64(1), tinyTextCatalogVersion(t, ctx, conn, databaseName, renamedTable))
		require.Equal(t, int64(1), tinyTextCatalogVersion(t, ctx, conn, databaseName, modifiedTable))
		require.Equal(t, int64(1), tinyTextCatalogVersion(t, ctx, conn, databaseName, readdedTable))
		for _, tableName := range []string{sourceTable, cloneTable, renamedTable} {
			tableID, logicalID := tinyTextCatalogIdentity(t, ctx, conn, databaseName, tableName)
			require.Equal(t, logicalID, tableID)
		}
		for _, tableName := range []string{modifiedTable, readdedTable} {
			tableID, logicalID := tinyTextCatalogIdentity(t, ctx, conn, databaseName, tableName)
			require.NotEqual(t, logicalID, tableID)
		}
		require.Equal(t, legacyModifiedSQL, tinyTextCatalogCreateSQL(t, ctx, conn, databaseName, modifiedTable))
		require.Equal(t, legacyReaddedSQL, tinyTextCatalogCreateSQL(t, ctx, conn, databaseName, readdedTable))

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
		cloneDDL := showCreateTableSQL(t, ctx, conn, databaseName, cloneTable)
		require.Contains(t, strings.ToUpper(cloneDDL), "TINYTEXT")
		renamedDDL := showCreateTableSQL(t, ctx, conn, databaseName, renamedTable)
		require.Contains(t, strings.ToUpper(renamedDDL), strings.ToUpper(renamedColumnName))
		require.Contains(t, strings.ToUpper(renamedDDL), "TINYTEXT")
		requireLegacyAlteredTextDDL(
			t, showCreateTableSQL(t, ctx, conn, databaseName, modifiedTable), "payload",
		)
		requireLegacyAlteredTextDDL(
			t, showCreateTableSQL(t, ctx, conn, databaseName, readdedTable), "payload",
		)
		// Recovery is planner-owned and deliberately does not mutate the old
		// catalog row in place. The metadata function maps its persisted TEXT
		// width marker to the MySQL-compatible bound.
		require.Equal(t, int64(types.MaxStringSize), tinyTextCatalogWidth(t, ctx, conn, databaseName, cloneTable, "payload"))
		require.Equal(t, int64(types.MaxStringSize), tinyTextCatalogWidth(t, ctx, conn, databaseName, renamedTable, renamedColumnName))

		mustExecTinyTextUpgradeSQL(t, ctx, conn, "set session sql_mode = 'STRICT_TRANS_TABLES'")
		for _, tableName := range []string{assignTarget, ignoreTarget, updateTarget} {
			mustExecTinyTextUpgradeSQL(t, ctx, conn,
				"create table "+databaseName+"."+tableName+" (id int primary key, payload tinytext)")
		}

		// A recovered source and a fresh target have the same normalized planner
		// type. The write boundary must still validate the source because its
		// physical rows predate the TINYTEXT width constraint.
		expectTinyTextStrictError(t, ctx, conn,
			"insert into "+databaseName+"."+assignTarget+
				" select id, payload from "+databaseName+"."+sourceTable)
		require.Equal(t, 0, tinyTextUpgradeRowCount(t, ctx, conn, databaseName, assignTarget))

		mustExecTinyTextUpgradeSQL(t, ctx, conn,
			"insert into "+databaseName+"."+updateTarget+" values (1, 'safe')")
		expectTinyTextStrictError(t, ctx, conn,
			"update "+databaseName+"."+updateTarget+
				" set payload = (select payload from "+databaseName+"."+sourceTable+" where id = 1) where id = 1")
		require.Equal(t, 4, tinyTextUpgradeValueLength(t, ctx, conn, databaseName, updateTarget, "payload", 1))

		expectTinyTextStrictError(t, ctx, conn,
			"create table "+databaseName+"."+strictCTASTable+
				" as select id, payload from "+databaseName+"."+sourceTable)
		mustExecTinyTextUpgradeSQL(t, ctx, conn,
			"drop table if exists "+databaseName+"."+strictCTASTable)

		// ALTER COPY executes an internal INSERT. It keeps its internal error
		// contract, but must not silently copy an oversized legacy row.
		_, alterCopyErr := conn.ExecContext(ctx,
			"alter table "+databaseName+"."+alterCopyTable+" algorithm=copy, modify column id bigint")
		require.Error(t, alterCopyErr)
		require.Equal(t, 1000, tinyTextUpgradeValueLength(
			t, ctx, conn, databaseName, alterCopyTable, "payload", 1,
		))

		mustExecTinyTextUpgradeSQL(t, ctx, conn,
			"insert ignore into "+databaseName+"."+ignoreTarget+
				" select id, payload from "+databaseName+"."+sourceTable)
		require.Equal(t, 255, tinyTextUpgradeValueLength(
			t, ctx, conn, databaseName, ignoreTarget, "payload", 1,
		))

		for _, tableName := range []string{sourceTable, cloneTable} {
			expectTinyTextStrictError(
				t,
				ctx,
				conn,
				"insert into "+databaseName+"."+tableName+" values (2, repeat('s', 256))",
			)
		}
		expectTinyTextStrictError(t, ctx, conn,
			"insert into "+databaseName+"."+renamedTable+" (id, "+renamedColumnName+") values (2, repeat('s', 256))")
		for _, table := range []struct {
			name   string
			column string
		}{
			{name: modifiedTable, column: "payload"},
			{name: readdedTable, column: "payload"},
		} {
			mustExecTinyTextUpgradeSQL(t, ctx, conn,
				"insert into "+databaseName+"."+table.name+" (id, "+table.column+") values (2, repeat('s', 1000))")
		}
		expectTinyTextStrictError(t, ctx, conn,
			"update "+databaseName+"."+sourceTable+" set payload = repeat('u', 256) where id = 1")
		require.NoError(t, conn.QueryRowContext(
			ctx,
			"select length(payload) from "+databaseName+"."+sourceTable+" where id = 1",
		).Scan(&existingLength))
		require.Equal(t, 1000, existingLength)

		mustExecTinyTextUpgradeSQL(t, ctx, conn, "set session sql_mode = ''")
		mustExecTinyTextUpgradeSQL(t, ctx, conn,
			"insert into "+databaseName+"."+assignTarget+
				" select id, payload from "+databaseName+"."+sourceTable)
		require.Equal(t, 255, tinyTextUpgradeValueLength(
			t, ctx, conn, databaseName, assignTarget, "payload", 1,
		))
		mustExecTinyTextUpgradeSQL(t, ctx, conn,
			"update "+databaseName+"."+updateTarget+
				" set payload = (select payload from "+databaseName+"."+sourceTable+" where id = 1) where id = 1")
		require.Equal(t, 255, tinyTextUpgradeValueLength(
			t, ctx, conn, databaseName, updateTarget, "payload", 1,
		))
		mustExecTinyTextUpgradeSQL(t, ctx, conn,
			"create table "+databaseName+"."+nonStrictCTASTable+
				" as select id, payload from "+databaseName+"."+sourceTable)
		require.Equal(t, int64(types.MaxTinyTextLen), tinyTextCatalogWidth(
			t, ctx, conn, databaseName, nonStrictCTASTable, "payload",
		))
		require.Equal(t, 255, tinyTextUpgradeValueLength(
			t, ctx, conn, databaseName, nonStrictCTASTable, "payload", 1,
		))
		mustExecTinyTextUpgradeSQL(t, ctx, conn,
			"alter table "+databaseName+"."+alterCopyTable+" algorithm=copy, modify column id bigint")
		require.Equal(t, 255, tinyTextUpgradeValueLength(
			t, ctx, conn, databaseName, alterCopyTable, "payload", 1,
		))

		mustExecTinyTextUpgradeSQL(t, ctx, conn,
			"insert into "+databaseName+"."+sourceTable+" values (2, repeat('n', 1000))")
		mustExecTinyTextUpgradeSQL(t, ctx, conn,
			"insert into "+databaseName+"."+cloneTable+" values (2, repeat('n', 1000))")
		mustExecTinyTextUpgradeSQL(t, ctx, conn,
			"insert into "+databaseName+"."+renamedTable+" (id, "+renamedColumnName+") values (2, repeat('n', 1000))")
		for _, table := range []struct {
			name   string
			column string
		}{
			{name: modifiedTable, column: "payload"},
			{name: readdedTable, column: "payload"},
		} {
			mustExecTinyTextUpgradeSQL(t, ctx, conn,
				"insert into "+databaseName+"."+table.name+" (id, "+table.column+") values (3, repeat('n', 1000))")
		}
		mustExecTinyTextUpgradeSQL(t, ctx, conn,
			"update "+databaseName+"."+sourceTable+" set payload = repeat('u', 1000) where id = 1")
		require.NoError(t, conn.QueryRowContext(
			ctx,
			"select length(payload) from "+databaseName+"."+sourceTable+" where id = 1",
		).Scan(&existingLength))
		require.Equal(t, 255, existingLength)

		for _, table := range []struct {
			name   string
			column string
		}{
			{name: sourceTable, column: "payload"},
			{name: cloneTable, column: "payload"},
			{name: renamedTable, column: renamedColumnName},
		} {
			var storedLength int
			require.NoError(t, conn.QueryRowContext(
				ctx,
				"select length("+table.column+") from "+databaseName+"."+table.name+" where id = 2",
			).Scan(&storedLength))
			require.Equal(t, 255, storedLength)
		}
		for _, table := range []struct {
			name   string
			column string
		}{
			{name: modifiedTable, column: "payload"},
			{name: readdedTable, column: "payload"},
		} {
			for _, id := range []int{2, 3} {
				var storedLength int
				require.NoError(t, conn.QueryRowContext(
					ctx,
					fmt.Sprintf("select length(%s) from %s.%s where id = %d", table.column, databaseName, table.name, id),
				).Scan(&storedLength))
				require.Equal(t, 1000, storedLength)
			}
		}

		// CREATE LIKE preserves recovered TINYTEXT through an in-place rename and
		// the explicit unbounded TEXT choice after copy-table type evolution.
		mustExecTinyTextUpgradeSQL(t, ctx, conn,
			"create table "+databaseName+"."+cloneCopyTable+" like "+databaseName+"."+cloneTable)
		mustExecTinyTextUpgradeSQL(t, ctx, conn,
			"create table "+databaseName+"."+renamedCopyTable+" like "+databaseName+"."+renamedTable)
		mustExecTinyTextUpgradeSQL(t, ctx, conn,
			"create table "+databaseName+"."+modifiedCopyTable+" like "+databaseName+"."+modifiedTable)
		mustExecTinyTextUpgradeSQL(t, ctx, conn,
			"create table "+databaseName+"."+readdedCopyTable+" like "+databaseName+"."+readdedTable)
		require.Equal(t, int64(types.MaxTinyTextLen), tinyTextCatalogWidth(
			t, ctx, conn, databaseName, cloneCopyTable, "payload",
		))
		require.Equal(t, int64(types.MaxTinyTextLen), tinyTextCatalogWidth(
			t, ctx, conn, databaseName, renamedCopyTable, renamedColumnName,
		))
		require.Equal(t, int64(types.MaxStringSize), tinyTextCatalogWidth(
			t, ctx, conn, databaseName, modifiedCopyTable, "payload",
		))
		require.Equal(t, int64(types.MaxStringSize), tinyTextCatalogWidth(
			t, ctx, conn, databaseName, readdedCopyTable, "payload",
		))
		for _, table := range []struct {
			name   string
			column string
		}{
			{name: modifiedCopyTable, column: "payload"},
			{name: readdedCopyTable, column: "payload"},
		} {
			requireLegacyAlteredTextDDL(
				t, showCreateTableSQL(t, ctx, conn, databaseName, table.name), table.column,
			)
			mustExecTinyTextUpgradeSQL(t, ctx, conn,
				"insert into "+databaseName+"."+table.name+" (id, "+table.column+") values (1, repeat('l', 1000))")
		}
		renamedCopyDDL := showCreateTableSQL(t, ctx, conn, databaseName, renamedCopyTable)
		require.Contains(t, strings.ToUpper(renamedCopyDDL), strings.ToUpper(renamedColumnName))
		require.Contains(t, strings.ToUpper(renamedCopyDDL), "TINYTEXT")
		mustExecTinyTextUpgradeSQL(t, ctx, conn,
			"insert into "+databaseName+"."+renamedCopyTable+" (id, "+renamedColumnName+") values (1, repeat('l', 1000))")
		var renamedCopyLength int
		require.NoError(t, conn.QueryRowContext(ctx,
			"select length("+renamedColumnName+") from "+databaseName+"."+renamedCopyTable+" where id = 1",
		).Scan(&renamedCopyLength))
		require.Equal(t, 255, renamedCopyLength)

		// DUMP rebuilds stale historical DDL for renamed and type-altered tables,
		// but preserves an unaltered legacy LIKE statement whose hash is normalized
		// through the source lineage. Every manifest must recreate a matching target.
		stageRoot := t.TempDir()
		mustExecTinyTextUpgradeSQL(t, ctx, conn,
			fmt.Sprintf("create stage %s url = 'file://%s'", stageName, stageRoot))
		readManifestCreateSQL := func(path string) string {
			manifestBytes, err := os.ReadFile(filepath.Join(stageRoot, path, "manifest.json"))
			require.NoError(t, err)
			var manifest struct {
				CreateSQL string `json:"create_sql"`
			}
			require.NoError(t, json.Unmarshal(manifestBytes, &manifest))
			return manifest.CreateSQL
		}

		const renamedDumpPath = "legacy-renamed"
		renamedStagePath := fmt.Sprintf("stage://%s/%s", stageName, renamedDumpPath)
		mustExecTinyTextUpgradeSQL(t, ctx, conn,
			fmt.Sprintf("select mo_ctl('dn', 'flush', '%s.%s')", databaseName, renamedTable))
		mustExecTinyTextUpgradeSQL(t, ctx, conn,
			"dump table "+databaseName+"."+renamedTable+" to '"+renamedStagePath+"' metadata only")
		renamedManifestSQL := readManifestCreateSQL(renamedDumpPath)
		require.Contains(t, strings.ToUpper(renamedManifestSQL), strings.ToUpper(renamedColumnName))
		require.Contains(t, strings.ToUpper(renamedManifestSQL), "TINYTEXT")
		mustExecTinyTextUpgradeSQL(t, ctx, conn, "drop table "+databaseName+"."+renamedTable)
		mustExecTinyTextUpgradeSQL(t, ctx, conn, renamedManifestSQL)
		require.Equal(t, int64(types.MaxTinyTextLen), tinyTextCatalogWidth(
			t, ctx, conn, databaseName, renamedTable, renamedColumnName,
		))
		mustExecTinyTextUpgradeSQL(t, ctx, conn,
			"load table "+databaseName+"."+renamedTable+" from '"+renamedStagePath+"'")

		const modifiedDumpPath = "legacy-modified"
		modifiedStagePath := fmt.Sprintf("stage://%s/%s", stageName, modifiedDumpPath)
		mustExecTinyTextUpgradeSQL(t, ctx, conn,
			fmt.Sprintf("select mo_ctl('dn', 'flush', '%s.%s')", databaseName, modifiedTable))
		mustExecTinyTextUpgradeSQL(t, ctx, conn,
			"dump table "+databaseName+"."+modifiedTable+" to '"+modifiedStagePath+"' metadata only")
		modifiedManifestSQL := readManifestCreateSQL(modifiedDumpPath)
		requireLegacyAlteredTextDDL(t, modifiedManifestSQL, "payload")
		mustExecTinyTextUpgradeSQL(t, ctx, conn, "drop table "+databaseName+"."+modifiedTable)
		mustExecTinyTextUpgradeSQL(t, ctx, conn, modifiedManifestSQL)
		require.Equal(t, int64(types.MaxStringSize), tinyTextCatalogWidth(
			t, ctx, conn, databaseName, modifiedTable, "payload",
		))
		mustExecTinyTextUpgradeSQL(t, ctx, conn,
			"load table "+databaseName+"."+modifiedTable+" from '"+modifiedStagePath+"'")

		stagePath := fmt.Sprintf("stage://%s/%s", stageName, dumpPath)
		mustExecTinyTextUpgradeSQL(t, ctx, conn,
			fmt.Sprintf("select mo_ctl('dn', 'flush', '%s.%s')", databaseName, cloneTable))
		mustExecTinyTextUpgradeSQL(t, ctx, conn,
			"dump table "+databaseName+"."+cloneTable+" to '"+stagePath+"' metadata only")
		cloneManifestSQL := readManifestCreateSQL(dumpPath)
		require.Equal(t, legacyCloneSQL, cloneManifestSQL)
		mustExecTinyTextUpgradeSQL(t, ctx, conn, "drop table "+databaseName+"."+cloneTable)
		mustExecTinyTextUpgradeSQL(t, ctx, conn, cloneManifestSQL)
		require.Equal(t, int64(types.MaxTinyTextLen), tinyTextCatalogWidth(
			t, ctx, conn, databaseName, cloneTable, "payload",
		))
		mustExecTinyTextUpgradeSQL(t, ctx, conn,
			"load table "+databaseName+"."+cloneTable+" from '"+stagePath+"'")
	})
}

func expectTinyTextStrictError(
	t *testing.T,
	ctx context.Context,
	conn *sql.Conn,
	statement string,
) {
	t.Helper()
	_, err := conn.ExecContext(ctx, statement)
	var mysqlErr *mysqlDriver.MySQLError
	require.ErrorAs(t, err, &mysqlErr, statement)
	require.Equal(t, uint16(1406), mysqlErr.Number, statement)
}

func tinyTextUpgradeRowCount(
	t *testing.T,
	ctx context.Context,
	conn *sql.Conn,
	databaseName string,
	tableName string,
) int {
	t.Helper()
	var count int
	require.NoError(t, conn.QueryRowContext(
		ctx, "select count(*) from "+databaseName+"."+tableName,
	).Scan(&count))
	return count
}

func tinyTextUpgradeValueLength(
	t *testing.T,
	ctx context.Context,
	conn *sql.Conn,
	databaseName string,
	tableName string,
	columnName string,
	id int,
) int {
	t.Helper()
	var length int
	require.NoError(t, conn.QueryRowContext(
		ctx,
		fmt.Sprintf("select length(%s) from %s.%s where id = %d", columnName, databaseName, tableName, id),
	).Scan(&length))
	return length
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
	legacyDefs := legacyTinyTextExecutionDefs(
		t, relation.GetTableDef(ctx), relation.GetTableDef(ctx).Createsql, "payload",
	)
	require.NoError(t, database.Delete(ctx, tableName))
	require.NoError(t, database.Create(ctx, tableName, legacyDefs))
	relation, err = database.Relation(ctx, tableName, nil)
	require.NoError(t, err)

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

func replaceLegacyTinyTextCatalog(
	t *testing.T,
	ctx context.Context,
	service cnservice.Service,
	databaseName string,
	tableName string,
	createSQL string,
	columnName string,
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
	legacyDefs := legacyTinyTextExecutionDefs(t, relation.GetTableDef(ctx), createSQL, columnName)
	require.NoError(t, database.Delete(ctx, tableName))
	require.NoError(t, database.Create(ctx, tableName, legacyDefs))
	require.NoError(t, txn.Commit(ctx))
	committed = true
}

func legacyTinyTextExecutionDefs(
	t *testing.T,
	tableDef *plan2.TableDef,
	createSQL string,
	columnName string,
) []engine.TableDef {
	t.Helper()
	require.Zero(t, tableDef.Version)
	legacyDef := plan2.DeepCopyTableDef(tableDef, true)
	column := plan2.FindColumn(legacyDef.Cols, columnName)
	require.NotNil(t, column)
	require.Equal(t, int32(types.MaxTinyTextLen), column.Typ.Width)
	column.Typ.Width = 0
	legacyDef.Createsql = createSQL
	foundCreateSQLProperty := false
	for _, definition := range legacyDef.Defs {
		properties := definition.GetProperties()
		if properties == nil {
			continue
		}
		for _, property := range properties.Properties {
			if property.Key == catalog.SystemRelAttr_CreateSQL {
				property.Value = createSQL
				foundCreateSQLProperty = true
			}
		}
	}
	require.True(t, foundCreateSQLProperty)
	defs, _, err := engine.PlanDefsToExeDefs(legacyDef)
	require.NoError(t, err)
	return append(defs, engine.PlanColsToExeCols(legacyDef.Cols)...)
}

func renameLegacyTinyTextColumn(
	t *testing.T,
	ctx context.Context,
	service cnservice.Service,
	databaseName string,
	tableName string,
	oldName string,
	newName string,
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
	current := relation.GetTableDef(ctx)
	require.Zero(t, current.Version)
	renamed := plan2.DeepCopyTableDef(current, true)
	column := plan2.FindColumn(renamed.Cols, oldName)
	require.NotNil(t, column)
	require.Zero(t, column.Typ.Width)
	column.Name = newName
	column.OriginName = newName
	require.NoError(t, relation.AlterTable(
		ctx,
		nil,
		[]*api.AlterTableReq{
			api.NewReplaceDefReq(relation.GetDBID(ctx), relation.GetTableID(ctx), renamed),
			api.NewRenameColumnReq(
				relation.GetDBID(ctx), relation.GetTableID(ctx), oldName, newName, column.Seqnum,
			),
		},
	))
	require.NoError(t, txn.Commit(ctx))
	committed = true
}

func restoreLegacyTinyTextCreateSQL(
	t *testing.T,
	ctx context.Context,
	service cnservice.Service,
	databaseName string,
	tableName string,
	createSQL string,
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
	legacyDef.Createsql = createSQL
	foundCreateSQLProperty := false
	for _, definition := range legacyDef.Defs {
		properties := definition.GetProperties()
		if properties == nil {
			continue
		}
		for _, property := range properties.Properties {
			if property.Key == catalog.SystemRelAttr_CreateSQL {
				property.Value = createSQL
				foundCreateSQLProperty = true
			}
		}
	}
	require.True(t, foundCreateSQLProperty)
	require.NoError(t, relation.AlterTable(
		ctx,
		nil,
		[]*api.AlterTableReq{
			api.NewReplaceDefReq(relation.GetDBID(ctx), relation.GetTableID(ctx), legacyDef),
		},
	))
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

func tinyTextCatalogVersion(
	t *testing.T,
	ctx context.Context,
	conn *sql.Conn,
	databaseName string,
	tableName string,
) int64 {
	t.Helper()
	var version int64
	require.NoError(t, conn.QueryRowContext(
		ctx,
		fmt.Sprintf(
			"select %s from %s.%s where %s = '%s' and %s = '%s' and %s = 0",
			catalog.SystemRelAttr_Version,
			catalog.MO_CATALOG,
			catalog.MO_TABLES,
			catalog.SystemRelAttr_DBName,
			databaseName,
			catalog.SystemRelAttr_Name,
			tableName,
			catalog.SystemRelAttr_AccID,
		),
	).Scan(&version))
	return version
}

func tinyTextCatalogIdentity(
	t *testing.T,
	ctx context.Context,
	conn *sql.Conn,
	databaseName string,
	tableName string,
) (uint64, uint64) {
	t.Helper()
	var tableID uint64
	var logicalID uint64
	require.NoError(t, conn.QueryRowContext(
		ctx,
		fmt.Sprintf(
			"select %s, %s from %s.%s where %s = '%s' and %s = '%s' and %s = 0",
			catalog.SystemRelAttr_ID,
			catalog.SystemRelAttr_LogicalID,
			catalog.MO_CATALOG,
			catalog.MO_TABLES,
			catalog.SystemRelAttr_DBName,
			databaseName,
			catalog.SystemRelAttr_Name,
			tableName,
			catalog.SystemRelAttr_AccID,
		),
	).Scan(&tableID, &logicalID))
	return tableID, logicalID
}

func tinyTextCatalogCreateSQL(
	t *testing.T,
	ctx context.Context,
	conn *sql.Conn,
	databaseName string,
	tableName string,
) string {
	t.Helper()
	var createSQL string
	require.NoError(t, conn.QueryRowContext(
		ctx,
		fmt.Sprintf(
			"select %s from %s.%s where %s = '%s' and %s = '%s' and %s = 0",
			catalog.SystemRelAttr_CreateSQL,
			catalog.MO_CATALOG,
			catalog.MO_TABLES,
			catalog.SystemRelAttr_DBName,
			databaseName,
			catalog.SystemRelAttr_Name,
			tableName,
			catalog.SystemRelAttr_AccID,
		),
	).Scan(&createSQL))
	return createSQL
}

func requireLegacyAlteredTextDDL(t *testing.T, ddl string, columnName string) {
	t.Helper()
	upperDDL := strings.ToUpper(ddl)
	require.NotContains(t, upperDDL, "TINYTEXT")
	require.Contains(t, upperDDL, "`"+strings.ToUpper(columnName)+"` TEXT")
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

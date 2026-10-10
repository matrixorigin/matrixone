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
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/tests/testutils"
	"github.com/stretchr/testify/require"
)

func TestIssue29723MediumIntSemantics(t *testing.T) {
	embed.RunBaseClusterTests(t, func(cluster embed.Cluster) {
		ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
		defer cancel()

		cn, err := cluster.GetCNService(0)
		require.NoError(t, err)
		port := cn.GetServiceConfig().CN.Frontend.Port
		db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/?interpolateParams=false", port))
		require.NoError(t, err)
		defer func() { require.NoError(t, db.Close()) }()
		db.SetMaxOpenConns(1)
		db.SetMaxIdleConns(1)
		require.NoError(t, db.PingContext(ctx))
		_, err = db.ExecContext(ctx, "SET SESSION sql_mode='STRICT_TRANS_TABLES'")
		require.NoError(t, err)

		dbName := testutils.GetDatabaseName(t)
		execSQLRequire(t, ctx, db, "CREATE DATABASE `"+dbName+"`")
		defer func() { execSQLMaybe(t, ctx, db, "DROP DATABASE IF EXISTS `"+dbName+"`") }()
		table := "`" + dbName + "`.types_x"
		execSQLRequire(t, ctx, db, "CREATE TABLE "+table+" (m MEDIUMINT, mu MEDIUMINT UNSIGNED, m3 INT3, m3u INT3 UNSIGNED, display_int INT(24))")

		var tableName, createSQL string
		require.NoError(t, db.QueryRowContext(ctx, "SHOW CREATE TABLE "+table).Scan(&tableName, &createSQL))
		lowerCreate := strings.ToLower(createSQL)
		require.Contains(t, lowerCreate, "`m` mediumint")
		require.Contains(t, lowerCreate, "`mu` mediumint unsigned")
		require.Contains(t, lowerCreate, "`m3` mediumint")
		require.Contains(t, lowerCreate, "`m3u` mediumint unsigned")

		defaultsTable := "`" + dbName + "`.mediumint_defaults"
		execSQLRequire(t, ctx, db, "CREATE TABLE "+defaultsTable+
			" (m MEDIUMINT DEFAULT 8388607, mu MEDIUMINT UNSIGNED DEFAULT 16777215)")
		execSQLRequire(t, ctx, db, "INSERT INTO "+defaultsTable+" () VALUES ()")
		var defaultSigned, defaultUnsigned int64
		require.NoError(t, db.QueryRowContext(ctx, "SELECT m, mu FROM "+defaultsTable).
			Scan(&defaultSigned, &defaultUnsigned))
		require.Equal(t, int64(8388607), defaultSigned)
		require.Equal(t, int64(16777215), defaultUnsigned)
		_, err = db.ExecContext(ctx, "CREATE TABLE `"+dbName+"`.invalid_mediumint_default (m MEDIUMINT DEFAULT 8388608)")
		require.Error(t, err, "out-of-range MEDIUMINT defaults must be rejected")

		generatedTable := "`" + dbName + "`.mediumint_generated"
		execSQLRequire(t, ctx, db, "CREATE TABLE "+generatedTable+
			" (source INT, m MEDIUMINT GENERATED ALWAYS AS (source) STORED)")
		execSQLRequire(t, ctx, db, "INSERT INTO "+generatedTable+" (source) VALUES (8388607)")
		_, err = db.ExecContext(ctx, "INSERT INTO "+generatedTable+" (source) VALUES (8388608)")
		require.Error(t, err, "stored generated MEDIUMINT values must enforce the declared domain")

		loadTable := "`" + dbName + "`.load_parallel"
		execSQLRequire(t, ctx, db, "CREATE TABLE "+loadTable+" (m MEDIUMINT)")
		csvPath := filepath.Join(t.TempDir(), "mediumint.csv")
		require.NoError(t, os.WriteFile(csvPath, []byte("8388607\n"), 0o600))
		loadSQL := fmt.Sprintf("LOAD DATA INFILE '%s' INTO TABLE %s FIELDS TERMINATED BY ',' PARALLEL 'true'", csvPath, loadTable)
		execSQLRequire(t, ctx, db, loadSQL)
		var loadedValue int64
		require.NoError(t, db.QueryRowContext(ctx, "SELECT m FROM "+loadTable).Scan(&loadedValue))
		require.Equal(t, int64(8388607), loadedValue)
		require.NoError(t, os.WriteFile(csvPath, []byte("8388608\n"), 0o600))
		_, err = db.ExecContext(ctx, loadSQL)
		require.Error(t, err, "parallel CSV LOAD must enforce the MEDIUMINT target range")
		var loadedRows int64
		require.NoError(t, db.QueryRowContext(ctx, "SELECT count(*) FROM "+loadTable).Scan(&loadedRows))
		require.Equal(t, int64(1), loadedRows, "failed parallel CSV LOAD must not commit an out-of-range row")

		execSQLRequire(t, ctx, db, "INSERT INTO "+table+" VALUES (-8388608, 0, -8388608, 0, 2147483647), (8388607, 16777215, 8388607, 16777215, -2147483648)")
		var signedMin, signedMax, unsignedMax int64
		require.NoError(t, db.QueryRowContext(ctx, "SELECT min(m), max(m), max(mu) FROM "+table).Scan(&signedMin, &signedMax, &unsignedMax))
		require.Equal(t, int64(-8388608), signedMin)
		require.Equal(t, int64(8388607), signedMax)
		require.Equal(t, int64(16777215), unsignedMax)

		for _, statement := range []string{
			"INSERT INTO " + table + " (m) VALUES (8388608)",
			"INSERT INTO " + table + " (m) VALUES (-8388609)",
			"INSERT INTO " + table + " (mu) VALUES (16777216)",
		} {
			_, err := db.ExecContext(ctx, statement)
			require.Error(t, err, statement)
		}

		prepared, err := db.PrepareContext(ctx, "INSERT INTO "+table+" (m) VALUES (?)")
		require.NoError(t, err)
		defer func() { require.NoError(t, prepared.Close()) }()
		_, err = prepared.ExecContext(ctx, int64(8388607))
		require.NoError(t, err, "prepared statement must accept MEDIUMINT maximum")
		_, err = prepared.ExecContext(ctx, int64(8388608))
		require.Error(t, err, "prepared statement must reject signed MEDIUMINT overflow")
		require.NoError(t, prepared.Close())

		execSQLRequire(t, ctx, db, "CREATE TABLE `"+dbName+"`.source_int (v INT)")
		execSQLRequire(t, ctx, db, "INSERT INTO `"+dbName+"`.source_int VALUES (8388608)")
		_, err = db.ExecContext(ctx, "ALTER TABLE `"+dbName+"`.source_int MODIFY COLUMN v MEDIUMINT")
		require.Error(t, err, "narrowing ALTER must reject a legacy INT value outside MEDIUMINT bounds")
		var sourceValue int64
		require.NoError(t, db.QueryRowContext(ctx, "SELECT v FROM `"+dbName+"`.source_int").Scan(&sourceValue))
		require.Equal(t, int64(8388608), sourceValue, "failed narrowing ALTER must preserve the source table")
		var sourceType string
		require.NoError(t, db.QueryRowContext(ctx, `
SELECT data_type FROM information_schema.columns
WHERE table_schema = ? AND table_name = 'source_int' AND column_name = 'v'`, strings.ToLower(dbName)).Scan(&sourceType))
		require.Equal(t, "int", strings.ToLower(sourceType), "failed narrowing ALTER must retain the source type")
		_, err = db.ExecContext(ctx, "INSERT INTO "+table+" (m) SELECT v FROM `"+dbName+"`.source_int")
		require.Error(t, err, "INSERT SELECT must enforce the target MEDIUMINT domain")
		_, err = db.ExecContext(ctx, "UPDATE "+table+" SET m=8388608 WHERE m=8388607")
		require.Error(t, err, "UPDATE must enforce the target MEDIUMINT domain")

		autoTable := "`" + dbName + "`.auto_mediumint"
		execSQLRequire(t, ctx, db, "CREATE TABLE "+autoTable+" (id MEDIUMINT NOT NULL AUTO_INCREMENT PRIMARY KEY)")
		execSQLRequire(t, ctx, db, "ALTER TABLE "+autoTable+" AUTO_INCREMENT=8388607")
		execSQLRequire(t, ctx, db, "INSERT INTO "+autoTable+" VALUES ()")
		var autoID int64
		require.NoError(t, db.QueryRowContext(ctx, "SELECT id FROM "+autoTable).Scan(&autoID))
		require.Equal(t, int64(8388607), autoID)
		_, err = db.ExecContext(ctx, "INSERT INTO "+autoTable+" VALUES ()")
		require.Error(t, err, "MEDIUMINT AUTO_INCREMENT must reject values beyond its signed range")

		rows, err := db.QueryContext(ctx, `
SELECT column_name, column_type, numeric_precision
FROM information_schema.columns
WHERE table_schema = ? AND table_name = 'types_x'
ORDER BY ordinal_position`, strings.ToLower(dbName))
		require.NoError(t, err)
		defer func() { require.NoError(t, rows.Close()) }()
		metadata := make(map[string]struct {
			columnType string
			precision  sql.NullInt64
		})
		for rows.Next() {
			var name string
			var info struct {
				columnType string
				precision  sql.NullInt64
			}
			require.NoError(t, rows.Scan(&name, &info.columnType, &info.precision))
			metadata[name] = info
		}
		require.NoError(t, rows.Err())
		require.NoError(t, rows.Close())
		for _, name := range []string{"m", "mu", "m3", "m3u"} {
			info, ok := metadata[name]
			require.True(t, ok, "missing MEDIUMINT information_schema row for %s", name)
			require.Contains(t, strings.ToLower(info.columnType), "mediumint")
			require.True(t, info.precision.Valid)
			require.Equal(t, int64(7), info.precision.Int64)
		}

		resultRows, err := db.QueryContext(ctx, "SELECT m, mu FROM "+table+" LIMIT 1")
		require.NoError(t, err)
		defer func() { require.NoError(t, resultRows.Close()) }()
		columnTypes, err := resultRows.ColumnTypes()
		require.NoError(t, err)
		require.Equal(t, "MEDIUMINT", columnTypes[0].DatabaseTypeName())
		require.Equal(t, "UNSIGNED MEDIUMINT", columnTypes[1].DatabaseTypeName())
		require.NoError(t, resultRows.Err())
		require.NoError(t, resultRows.Close())

		// The persisted catalog uses the existing int32/uint32 OIDs plus width
		// 24. Check that identity and its existing rows survive a full restart.
		require.NoError(t, db.Close())
		require.NoError(t, cluster.Close())
		require.NoError(t, cluster.Start())
		cn, err = cluster.GetCNService(0)
		require.NoError(t, err)
		port = cn.GetServiceConfig().CN.Frontend.Port
		db, err = sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/?interpolateParams=false", port))
		require.NoError(t, err)
		db.SetMaxOpenConns(1)
		db.SetMaxIdleConns(1)
		require.NoError(t, db.PingContext(ctx))
		var persistedMax int64
		require.NoError(t, db.QueryRowContext(ctx, "SELECT max(m) FROM "+table).Scan(&persistedMax))
		require.Equal(t, int64(8388607), persistedMax)
		var persistedCreate string
		require.NoError(t, db.QueryRowContext(ctx, "SHOW CREATE TABLE "+table).Scan(&tableName, &persistedCreate))
		require.Contains(t, strings.ToLower(persistedCreate), "`m` mediumint")
		_, err = db.ExecContext(ctx, "INSERT INTO "+table+" (m) VALUES (8388608)")
		require.Error(t, err, "restarted catalog must retain MEDIUMINT assignment semantics")
	})
}

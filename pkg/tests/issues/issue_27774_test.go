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

	mysqlDriver "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/tests/testutils"
)

func TestIssue27774PreparedTemporalCompositeRange(t *testing.T) {
	embed.RunBaseClusterTests(t, func(c embed.Cluster) {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
		defer cancel()

		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		port := cn.GetServiceConfig().CN.Frontend.Port
		binaryDSN := fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/?interpolateParams=false", port)
		db, err := sql.Open("mysql", binaryDSN)
		require.NoError(t, err)
		defer db.Close()

		dbName := strings.ToLower(testutils.GetDatabaseName(t))
		execSQLMaybe(t, ctx, db, fmt.Sprintf("drop database if exists `%s`", dbName))
		defer func() {
			cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cleanupCancel()
			execSQLMaybe(t, cleanupCtx, db, fmt.Sprintf("drop database if exists `%s`", dbName))
		}()
		execSQLRequire(t, ctx, db, fmt.Sprintf("create database `%s`", dbName))

		type temporalCase struct {
			name      string
			columnSQL string
			lower     string
			middle    string
			upper     string
			after     string
		}
		cases := []temporalCase{
			{
				name: "date", columnSQL: "date",
				lower: "2026-05-01", middle: "2026-05-10", upper: "2026-05-19", after: "2026-06-01",
			},
			{
				name: "datetime", columnSQL: "datetime",
				lower: "2026-05-01 08:55:23", middle: "2026-05-10 12:00:00",
				upper: "2026-05-19 04:14:00", after: "2026-06-01 00:00:00",
			},
			{
				name: "timestamp", columnSQL: "timestamp",
				lower: "2026-05-01 08:55:23", middle: "2026-05-10 12:00:00",
				upper: "2026-05-19 04:14:00", after: "2026-06-01 00:00:00",
			},
		}

		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				table := "temporal_" + tc.name
				execSQLRequire(t, ctx, db, fmt.Sprintf(
					"create table `%s`.`%s` (v %s not null, id int not null, primary key(v, id))",
					dbName, table, tc.columnSQL))
				execSQLRequire(t, ctx, db, fmt.Sprintf(
					"insert into `%s`.`%s` values ('%s',1),('%s',2),('%s',3),('%s',4)",
					dbName, table, tc.lower, tc.middle, tc.upper, tc.after))

				query := fmt.Sprintf("select count(*) from `%s`.`%s` where v > ? and v <= ?", dbName, table)
				stmt, prepareErr := db.PrepareContext(ctx, query)
				require.NoError(t, prepareErr)
				defer stmt.Close()

				var count int
				require.NoError(t, stmt.QueryRowContext(ctx, tc.lower, tc.upper).Scan(&count))
				require.Equal(t, 2, count)
				require.NoError(t, stmt.QueryRowContext(ctx, tc.lower, tc.middle).Scan(&count))
				require.Equal(t, 1, count, "a reused COM_STMT must retain the temporal range domain")

				require.NoError(t, stmt.QueryRowContext(ctx, nil, tc.upper).Scan(&count))
				require.Zero(t, count)
			})
		}

		datetimeQuery := fmt.Sprintf(
			"select count(*) from `%s`.`temporal_datetime` where v > ? and v <= ?", dbName)
		stmt, err := db.PrepareContext(ctx, datetimeQuery)
		require.NoError(t, err)
		defer stmt.Close()

		var count int
		err = stmt.QueryRowContext(ctx, "not-a-datetime", cases[1].upper).Scan(&count)
		require.Error(t, err)
		var mysqlErr *mysqlDriver.MySQLError
		require.ErrorAs(t, err, &mysqlErr)
		require.Equal(t, uint16(20301), mysqlErr.Number)
		require.Contains(t, mysqlErr.Message, "invalid datetime value")
		require.NotContains(t, mysqlErr.Message, "invalid numeric string")
		require.NoError(t, stmt.QueryRowContext(ctx, cases[1].lower, cases[1].upper).Scan(&count))
		require.Equal(t, 2, count, "a failed execute must not corrupt the prepared statement")

		literalQuery := fmt.Sprintf(
			"select count(*) from `%s`.`temporal_datetime` where v > '%s' and v <= '%s'",
			dbName, cases[1].lower, cases[1].upper)
		require.NoError(t, db.QueryRowContext(ctx, literalQuery).Scan(&count))
		require.Equal(t, 2, count)

		textDSN := fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/?interpolateParams=true", port)
		textDB, err := sql.Open("mysql", textDSN)
		require.NoError(t, err)
		defer textDB.Close()
		require.NoError(t, textDB.QueryRowContext(
			ctx, datetimeQuery, cases[1].lower, cases[1].upper).Scan(&count))
		require.Equal(t, 2, count)

		execSQLRequire(t, ctx, db, fmt.Sprintf("drop database `%s`", dbName))
		require.NoError(t, db.QueryRowContext(ctx,
			"select count(*) from mo_catalog.mo_database where account_id = 0 and datname = ?", dbName).
			Scan(&count))
		require.Zero(t, count, "the public regression must leave the shared cluster clean")
	})
}

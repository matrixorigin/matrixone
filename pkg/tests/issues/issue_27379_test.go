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
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/embed"
)

func TestIssue27379TimestampAddPreservesFSP(t *testing.T) {
	embed.RunBaseClusterTests(t, func(c embed.Cluster) {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
		defer cancel()

		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		port := cn.GetServiceConfig().CN.Frontend.Port
		db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/?interpolateParams=false", port))
		require.NoError(t, err)
		defer db.Close()

		const dbName = "issue_27379_timestampadd_fsp"
		execSQLMaybe(t, ctx, db, "drop database if exists "+dbName)
		defer execSQLMaybe(t, ctx, db, "drop database if exists "+dbName)
		execSQLRequire(t, ctx, db, "create database "+dbName)
		execSQLRequire(t, ctx, db, "create table "+dbName+`.t (
			dt0 datetime(0), dt3 datetime(3), dt6 datetime(6),
			ts0 timestamp(0), ts3 timestamp(3), ts6 timestamp(6))`)
		execSQLRequire(t, ctx, db, "insert into "+dbName+`.t values
			('2026-08-12 10:00:00', '2026-08-12 10:00:00.000100', '2026-08-12 10:00:00.000100',
			 '2026-08-12 10:00:00', '2026-08-12 10:00:00.000100', '2026-08-12 10:00:00.000100')`)

		rows, err := db.QueryContext(ctx, `select
			timestampadd(second, 1, dt0), timestampadd(second, 1, dt3), timestampadd(second, 1, dt6),
			timestampadd(second, 1, ts0), timestampadd(second, 1, ts3), timestampadd(second, 1, ts6),
			timestampadd(microsecond, 1, dt0), timestampadd(microsecond, 1, dt3), timestampadd(microsecond, 1, dt6),
			timestampadd(microsecond, 1, ts0), timestampadd(microsecond, 1, ts3), timestampadd(microsecond, 1, ts6)
			from `+dbName+`.t`)
		require.NoError(t, err)
		defer rows.Close()

		columnTypes, err := rows.ColumnTypes()
		require.NoError(t, err)
		require.Len(t, columnTypes, 12)
		wantScales := []int64{0, 3, 6, 0, 3, 6, 6, 6, 6, 6, 6, 6}
		for i, columnType := range columnTypes {
			_, scale, ok := columnType.DecimalSize()
			require.Truef(t, ok, "column %d has no decimal scale", i)
			require.Equal(t, wantScales[i], scale, "column %d metadata", i)
		}

		require.True(t, rows.Next())
		values := make([]string, len(columnTypes))
		dest := make([]any, len(values))
		for i := range values {
			dest[i] = &values[i]
		}
		require.NoError(t, rows.Scan(dest...))
		require.Equal(t, []string{
			"2026-08-12 10:00:01", "2026-08-12 10:00:01.000100", "2026-08-12 10:00:01.000100",
			"2026-08-12 10:00:01", "2026-08-12 10:00:01.000100", "2026-08-12 10:00:01.000100",
			"2026-08-12 10:00:00.000001", "2026-08-12 10:00:00.000101", "2026-08-12 10:00:00.000101",
			"2026-08-12 10:00:00.000001", "2026-08-12 10:00:00.000101", "2026-08-12 10:00:00.000101",
		}, values)
		require.False(t, rows.Next())
		require.NoError(t, rows.Err())

		var formatted, casted string
		var delta int64
		require.NoError(t, db.QueryRowContext(ctx, `select
			date_format(timestampadd(microsecond, 1, dt3), '%Y-%m-%d %H:%i:%s.%f'),
			cast(timestampadd(microsecond, 1, dt3) as char),
			timestampdiff(microsecond, dt3, timestampadd(microsecond, 1, dt3))
			from `+dbName+`.t`).Scan(&formatted, &casted, &delta))
		require.Equal(t, "2026-08-12 10:00:00.000101", formatted)
		require.Equal(t, "2026-08-12 10:00:00.000101", casted)
		require.Equal(t, int64(1), delta)

		windowRows, err := db.QueryContext(ctx, `select
			_wstart, timestampadd(second, 1, _wstart),
			date_format(timestampadd(microsecond, 1, _wstart), '%Y-%m-%d %H:%i:%s.%f')
			from `+dbName+`.t interval(dt3, 1000, microsecond)`)
		require.NoError(t, err)
		defer windowRows.Close()
		require.True(t, windowRows.Next())
		var wstart, shifted, formattedWindow string
		require.NoError(t, windowRows.Scan(&wstart, &shifted, &formattedWindow))
		require.NotEmpty(t, wstart)
		require.NotEmpty(t, shifted)
		require.Contains(t, formattedWindow, ".")
		require.NoError(t, windowRows.Err())
	})
}

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

package issues

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/stretchr/testify/require"
)

func TestIssue31913UnionDateAndTime(t *testing.T) {
	embed.RunSingleCNBaseClusterTests(t, func(cluster embed.Cluster) {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cancel()
		cn, err := cluster.GetCNService(0)
		require.NoError(t, err)
		db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", cn.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		defer db.Close()
		name := fmt.Sprintf("issue_31913_%d", time.Now().UnixNano())
		execSQLRequire(t, ctx, db, "create database "+name)
		defer execSQLMaybe(t, context.Background(), db, "drop database if exists "+name)
		execSQLRequire(t, ctx, db, "create table "+name+".dates (d date)")
		execSQLRequire(t, ctx, db, "create table "+name+".clocks (v time)")
		execSQLRequire(t, ctx, db, "insert into "+name+".dates values ('2024-01-01')")
		execSQLRequire(t, ctx, db, "insert into "+name+".clocks values ('10:30:00'),('23:00:00')")
		for _, query := range []string{
			"select d as x from " + name + ".dates union all select v from " + name + ".clocks",
			"select v as x from " + name + ".clocks union all select d from " + name + ".dates",
			"select d as x from " + name + ".dates union select v from " + name + ".clocks",
		} {
			rows, err := db.QueryContext(ctx, query)
			require.NoError(t, err, query)
			var values []string
			for rows.Next() {
				var value string
				require.NoError(t, rows.Scan(&value))
				values = append(values, value)
			}
			require.NoError(t, rows.Err())
			require.NoError(t, rows.Close())
			require.Len(t, values, 3, query)
			var morning, evening bool
			for _, value := range values {
				morning = morning || strings.HasSuffix(value, "10:30:00")
				evening = evening || strings.HasSuffix(value, "23:00:00")
			}
			require.True(t, morning, "%s: %v", query, values)
			require.True(t, evening, "%s: %v", query, values)
		}

		execSQLRequire(t, ctx, db, "create table "+name+".copy as select d as x from "+name+".dates union all select v from "+name+".clocks")
		var dataType string
		err = db.QueryRowContext(ctx, "select data_type from information_schema.columns where table_schema=? and table_name='copy' and column_name='x'", name).Scan(&dataType)
		require.NoError(t, err)
		require.Equal(t, "datetime", strings.ToLower(dataType))
		var count int
		err = db.QueryRowContext(ctx, "select count(distinct x) from "+name+".copy").Scan(&count)
		require.NoError(t, err)
		require.Equal(t, 3, count)

		rows, err := db.QueryContext(ctx, "select cast('2024-01-01' as date) as x union all select cast('10:30:00.123456' as time(6))")
		require.NoError(t, err)
		defer rows.Close()
		var fractionalTime bool
		for rows.Next() {
			var value string
			require.NoError(t, rows.Scan(&value))
			fractionalTime = fractionalTime || strings.HasSuffix(value, "10:30:00.123456")
		}
		require.NoError(t, rows.Err())
		require.True(t, fractionalTime)
	})
}

// Copyright 2021 Matrix Origin
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

package sqlintegration

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

func TestPreparedWindowReleasesInheritedAccounts(t *testing.T) {
	runSQLIntegration(t, func(c embed.Cluster) {
		ctx, cancel := context.WithTimeout(t.Context(), 90*time.Second)
		defer cancel()
		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		dsn := fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/?interpolateParams=false", cn.GetServiceConfig().CN.Frontend.Port)
		db, err := sql.Open("mysql", dsn)
		require.NoError(t, err)
		defer db.Close()
		db.SetMaxOpenConns(1)
		const database = "window_prepared_account"
		_, err = db.ExecContext(ctx, "create database "+database)
		require.NoError(t, err)
		defer cleanupSQLIntegration(t, cn, "drop database if exists "+database)
		conn, err := db.Conn(ctx)
		require.NoError(t, err)
		defer conn.Close()
		for _, statement := range []string{
			"use " + database,
			"create table t(id int primary key, g int, v int)",
			"insert into t values(1,1,10),(2,1,20),(3,1,30)",
		} {
			_, err := conn.ExecContext(ctx, statement)
			require.NoError(t, err)
		}
		for _, partition := range []string{"", "partition by x.g "} {
			query := "select x.id, sum(y.v) over (" + partition + "order by x.id rows unbounded preceding) from t x join t y on y.id=x.id where x.id>=? order by x.id"
			// The literal control and prepared execution must preserve the same sums.
			rows, err := conn.QueryContext(ctx, strings.Replace(query, "?", "1", 1))
			require.NoError(t, err)
			require.Equal(t, []string{"1:10", "2:30", "3:60"}, readWindowNullRows(t, rows))
			func() {
				stmt, err := conn.PrepareContext(ctx, query)
				require.NoError(t, err)
				defer func() { require.NoError(t, stmt.Close()) }()
				for _, trial := range []struct {
					min  int
					want []string
				}{
					{1, []string{"1:10", "2:30", "3:60"}},
					{2, []string{"2:20", "3:50"}},
					{4, nil},
					{1, []string{"1:10", "2:30", "3:60"}},
				} {
					rows, err := stmt.QueryContext(ctx, trial.min)
					require.NoError(t, err)
					require.Equal(t, trial.want, readWindowNullRows(t, rows))
				}
			}()
		}
		var count int
		require.NoError(t, conn.QueryRowContext(ctx, "select count(*) from t").Scan(&count))
		require.Equal(t, 3, count)
		// A separate pool has no idle connection: Ping must perform new authentication.
		next, err := sql.Open("mysql", dsn)
		require.NoError(t, err)
		defer next.Close()
		require.NoError(t, next.PingContext(ctx))
		require.NoError(t, next.QueryRowContext(ctx, "select count(*) from "+database+".t").Scan(&count))
		require.Equal(t, 3, count)
	})
}

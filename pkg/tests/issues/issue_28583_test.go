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
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/stretchr/testify/require"
)

// TestIssue28583PreparedSignNumericDomains covers both server-side SQL
// PREPARE/EXECUTE and the binary COM_STMT path. SIGN's result type is always
// BIGINT, but its input overload must follow the execute-time numeric domain.
func TestIssue28583PreparedSignNumericDomains(t *testing.T) {
	embed.RunBaseClusterTests(t, func(c embed.Cluster) {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cancel()

		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		port := cn.GetServiceConfig().CN.Frontend.Port
		db, err := sql.Open("mysql", fmt.Sprintf(
			"dump:111@tcp(127.0.0.1:%d)/?interpolateParams=false", port))
		require.NoError(t, err)
		defer db.Close()
		// SQL PREPARE state is connection-local. Keep all statements on one
		// connection so the repeated EXECUTE calls observe the same plan.
		db.SetMaxOpenConns(1)
		db.SetMaxIdleConns(1)

		_, err = db.ExecContext(ctx, "prepare issue_28583_sign from 'select sign(?)'")
		require.NoError(t, err)
		defer func() {
			_, _ = db.ExecContext(context.Background(), "deallocate prepare issue_28583_sign")
		}()

		for _, test := range []struct {
			assignment string
			want       int64
		}{
			{assignment: "-0.1", want: -1},
			{assignment: "cast(-0.1 as double)", want: -1},
			{assignment: "0.1", want: 1},
			{assignment: "cast(0.1 as double)", want: 1},
			{assignment: "0", want: 0},
		} {
			_, err = db.ExecContext(ctx, "set @issue_28583_sign = "+test.assignment)
			require.NoError(t, err, test.assignment)
			var got int64
			require.NoError(t, db.QueryRowContext(ctx,
				"execute issue_28583_sign using @issue_28583_sign").Scan(&got), test.assignment)
			require.Equal(t, test.want, got, test.assignment)
		}

		for _, test := range []struct {
			name string
			sql  string
		}{
			{name: "issue_28583_sign_decimal", sql: "select sign(cast(? as decimal(20,5)))"},
			{name: "issue_28583_sign_double", sql: "select sign(cast(? as double))"},
		} {
			_, err = db.ExecContext(ctx, "prepare "+test.name+" from '"+test.sql+"'")
			require.NoError(t, err, test.name)
			func() {
				defer func() {
					_, _ = db.ExecContext(context.Background(), "deallocate prepare "+test.name)
				}()
				_, err = db.ExecContext(ctx, "set @issue_28583_sign = -0.1")
				require.NoError(t, err, test.name)
				var got int64
				require.NoError(t, db.QueryRowContext(ctx,
					"execute "+test.name+" using @issue_28583_sign").Scan(&got), test.name)
				require.Equal(t, int64(-1), got, test.name)
			}()
		}

		stmt, err := db.PrepareContext(ctx, "select sign(?)")
		require.NoError(t, err)
		defer stmt.Close()
		for _, test := range []struct {
			value any
			want  int64
		}{
			{value: float64(-0.1), want: -1},
			{value: "-0.1", want: -1},
			{value: "0.1", want: 1},
			{value: int64(0), want: 0},
		} {
			var got int64
			require.NoError(t, stmt.QueryRowContext(ctx, test.value).Scan(&got),
				fmt.Sprintf("%v", test.value))
			require.Equal(t, test.want, got, fmt.Sprintf("%v", test.value))
		}
		var nullResult sql.NullInt64
		require.NoError(t, stmt.QueryRowContext(ctx, nil).Scan(&nullResult))
		require.False(t, nullResult.Valid)
	})
}

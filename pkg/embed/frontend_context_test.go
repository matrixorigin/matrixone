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

package embed

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestFrontendExpressionContextAcrossRequests(t *testing.T) {
	RunBaseClusterTests(t, func(c Cluster) {
		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/?multiStatements=true", cn.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		defer db.Close()
		ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
		defer cancel()
		conn, err := db.Conn(ctx)
		require.NoError(t, err)
		defer conn.Close()
		for _, tc := range []struct {
			name, query string
			want        [][][]string
		}{
			{"LIKE", "show variables where variable_name like 'version_comm%'", [][][]string{{{"version_comment", "MatrixOne"}}}},
			{"ILIKE", "show variables where value ilike '%matrixone%' and variable_name = 'version_comment'", [][][]string{{{"version_comment", "MatrixOne"}}}},
			{"collation", "show collation where Charset = 'utf8mb4' and Collation like '%bin%'", [][][]string{{
				{"utf8mb4_bin", "utf8mb4", "46", "", "Yes", "1", "PAD SPACE"},
			}}},
			{"empty result", "show variables where variable_name like 'system%' and variable_name != 'system_time_zone'", [][][]string{nil}},
			{"multi statement", "select 1; show variables where variable_name like 'version_comm%'; select 2; show variables where variable_name like 'version_comm%'", [][][]string{{{"1"}}, {{"version_comment", "MatrixOne"}}, {{"2"}}, {{"version_comment", "MatrixOne"}}}},
			{"nested SET", "set @context_probe = (select 'ok'); show variables where variable_name like 'version_comm%'", [][][]string{{{"version_comment", "MatrixOne"}}}},
			{"staged sql_mode", "set sql_mode=''; show variables where variable_name like 'version_comm%'; set sql_mode=default; show variables where variable_name like 'version_comm%'", [][][]string{{{"version_comment", "MatrixOne"}}, {{"version_comment", "MatrixOne"}}}},
			{"implicit commit", "begin; insert into frontend_context_epoch.seed values (1); truncate table frontend_context_epoch.cleared; show variables where variable_name like 'version_comm%'; rollback; select count(*) from frontend_context_epoch.seed; select count(*) from frontend_context_epoch.cleared", [][][]string{{{"version_comment", "MatrixOne"}}, {{"1"}}, {{"0"}}}},
		} {
			t.Run(tc.name, func(t *testing.T) {
				if tc.name == "implicit commit" {
					_, err := conn.ExecContext(ctx, "create database frontend_context_epoch")
					require.NoError(t, err)
					defer func() {
						cleanupCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
						defer cancel()
						_, err := conn.ExecContext(cleanupCtx, "rollback")
						require.NoError(t, err)
						_, err = conn.ExecContext(cleanupCtx, "drop database frontend_context_epoch")
						require.NoError(t, err)
					}()
					_, err = conn.ExecContext(ctx, "create table frontend_context_epoch.seed (v int)")
					require.NoError(t, err)
					_, err = conn.ExecContext(ctx, "create table frontend_context_epoch.cleared (v int)")
					require.NoError(t, err)
					_, err = conn.ExecContext(ctx, "insert into frontend_context_epoch.cleared values (1)")
					require.NoError(t, err)
				}
				// A completed engine request must not poison the next frontend request.
				var one int
				require.NoError(t, conn.QueryRowContext(ctx, "select 1").Scan(&one))
				require.Equal(t, 1, one)
				rows, err := conn.QueryContext(ctx, tc.query)
				require.NoError(t, err)
				defer rows.Close()
				resultSet := 0
				for {
					columns, err := rows.Columns()
					require.NoError(t, err)
					var got [][]string
					for rows.Next() {
						values := make([]string, len(columns))
						args := make([]any, len(values))
						for i := range values {
							args[i] = &values[i]
						}
						require.NoError(t, rows.Scan(args...))
						got = append(got, values)
					}
					require.NoError(t, rows.Err())
					require.Less(t, resultSet, len(tc.want))
					require.ElementsMatch(t, tc.want[resultSet], got)
					resultSet++
					if !rows.NextResultSet() {
						break
					}
				}
				require.NoError(t, rows.Err())
				require.Equal(t, len(tc.want), resultSet)
			})
		}
	})
}

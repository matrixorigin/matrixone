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
	"encoding/json"
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/common/sqlquote"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestFrontendExpressionContextAcrossRequests(t *testing.T) {
	RunSingleCNBaseClusterTests(t, func(c Cluster) {
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

		t.Run("compiler ownership", func(t *testing.T) {
			_, err := conn.ExecContext(ctx, "create database frontend_context_compile")
			require.NoError(t, err)
			defer func() {
				cleanupCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancel()
				_, rollbackErr := conn.ExecContext(cleanupCtx, "rollback")
				_, dropErr := conn.ExecContext(cleanupCtx, "drop database frontend_context_compile")
				require.NoError(t, rollbackErr)
				require.NoError(t, dropErr)
			}()
			path := filepath.Join(t.TempDir(), "context_rows.csv")
			require.NoError(t, os.WriteFile(path, []byte("first\nsecond\n"), 0600))
			_, err = conn.ExecContext(ctx, "create external table frontend_context_compile.ext(payload varchar(32)) infile{'filepath'="+sqlquote.String(path)+"} fields terminated by ',' lines terminated by '\n'")
			require.NoError(t, err)
			_, err = conn.ExecContext(ctx, "prepare frontend_context_stmt from select count(*) from frontend_context_compile.ext where __mo_filepath like ?")
			require.NoError(t, err)
			defer func() {
				_, err := conn.ExecContext(ctx, "deallocate prepare frontend_context_stmt")
				require.NoError(t, err)
			}()
			for _, tc := range []struct {
				pattern string
				want    int
			}{{"%context_rows.csv", 2}, {"%missing.csv", 0}, {"%context_rows.csv", 2}} {
				_, err = conn.ExecContext(ctx, "set @frontend_context_path="+sqlquote.String(tc.pattern))
				require.NoError(t, err)
				var got int
				require.NoError(t, conn.QueryRowContext(ctx, "execute frontend_context_stmt using @frontend_context_path").Scan(&got))
				require.Equal(t, tc.want, got)
			}
			var comment string
			require.NoError(t, conn.QueryRowContext(ctx, "show variables where variable_name like 'version_comm%'").Scan(new(string), &comment))
			require.Equal(t, "MatrixOne", comment)
			// CREATE VIEW must only describe both definitions; it must execute neither.
			type faultStatus struct {
				PodType string `json:"pod_type"`
				PodID   string `json:"pod_id"`
				Status  string `json:"return_str"`
			}
			readFaultStatus := func() []faultStatus {
				var raw string
				require.NoError(t, conn.QueryRowContext(ctx, "select fault_inject('all.','status_fault_point','')").Scan(&raw))
				var statuses []faultStatus
				require.NoError(t, json.Unmarshal([]byte(raw), &statuses))
				require.NotEmpty(t, statuses)
				return statuses
			}
			originalStatuses := readFaultStatus()
			for _, status := range originalStatuses {
				require.Contains(t, []string{"Fault injection is enabled", "Fault injection is disabled"}, status.Status)
				require.NotEmpty(t, status.PodID)
			}
			defer func() {
				cleanupCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancel()
				for _, status := range originalStatuses {
					command := "disable_fault_injection"
					if status.Status == "Fault injection is enabled" {
						command = "enable_fault_injection"
					}
					_, err := conn.ExecContext(cleanupCtx, "select fault_inject("+sqlquote.String(status.PodType+"."+status.PodID)+","+sqlquote.String(command)+",'')")
					if err != nil {
						t.Errorf("restore fault state for %s: %v", status.PodID, err)
					}
				}
			}()
			_, err = conn.ExecContext(ctx, "select fault_inject('all.','disable_fault_injection','')")
			require.NoError(t, err)
			for _, ddl := range []string{"create table frontend_context_compile.source(id int)", "create view frontend_context_compile.projected as select enable_fault_injection() as r,id from frontend_context_compile.source", "create view frontend_context_compile.filtered as select id from frontend_context_compile.source where enable_fault_injection()"} {
				_, err = conn.ExecContext(ctx, ddl)
				require.NoError(t, err)
				statuses := readFaultStatus()
				seen := map[string]bool{}
				for _, status := range statuses {
					seen[status.PodType] = true
					require.Equal(t, "Fault injection is disabled", status.Status, ddl)
				}
				require.True(t, seen["CN"])
				require.True(t, seen["DN"])
			}
		})

	})
}

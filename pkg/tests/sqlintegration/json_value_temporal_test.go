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

package sqlintegration

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/stretchr/testify/require"
)

// BVT owns SQL PREPARE; this shared one-CN fixture proves COM_STMT_PREPARE /
// COM_STMT_EXECUTE with unbounded document parameters and repeated execution.
func TestJSONValueBinaryPreparedTemporal(t *testing.T) {
	runSQLIntegration(t, func(c embed.Cluster) {
		ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
		defer cancel()
		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/?interpolateParams=false", cn.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		defer db.Close()
		conn, err := db.Conn(ctx)
		require.NoError(t, err)
		defer conn.Close()
		for _, tc := range []struct{ target, fallback, valid, want string }{
			{"time(6)", "01:02:03", "12:34:56", "12:34:56.000000"},
			{"date", "2000-01-01", "2024-01-02", "2024-01-02"},
		} {
			t.Run(tc.target, func(t *testing.T) {
				for _, policy := range []string{"null on error", "default '" + tc.fallback + "' on error", "error on error"} {
					t.Run(policy, func(t *testing.T) {
						stmt, err := conn.PrepareContext(ctx, "select json_value(?, '$.v' returning "+tc.target+" "+policy+")")
						require.NoError(t, err)
						defer stmt.Close()
						var got sql.NullString
						err = stmt.QueryRowContext(ctx, `{"v":"2024-01-02 12:34:56"}`).Scan(&got)
						switch policy {
						case "error on error":
							require.Error(t, err)
						case "null on error":
							require.NoError(t, err)
							require.False(t, got.Valid)
						default:
							require.NoError(t, err)
							want := tc.fallback
							if tc.target == "time(6)" {
								want += ".000000"
							}
							require.Equal(t, sql.NullString{String: want, Valid: true}, got)
						}
						require.NoError(t, stmt.QueryRowContext(ctx, `{"v":"`+tc.valid+`"}`).Scan(&got))
						require.Equal(t, sql.NullString{String: tc.want, Valid: true}, got)
					})
				}
				for _, policy := range []string{"empty", "error"} {
					stmt, err := conn.PrepareContext(ctx, "select json_value('\""+tc.valid+"\"', '$' returning "+tc.target+" default '2024-01-02 12:34:56' on "+policy+")")
					if stmt != nil {
						defer stmt.Close()
					}
					require.Error(t, err, "invalid unused DEFAULT must fail prepare")
				}
			})
		}
	})
}

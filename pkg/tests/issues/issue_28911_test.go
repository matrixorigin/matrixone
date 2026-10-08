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

func TestIssue28911BinaryUncompressedLengthWarnings(t *testing.T) {
	embed.RunBaseClusterTests(t, func(cluster embed.Cluster) {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cancel()
		cn, err := cluster.GetCNService(0)
		require.NoError(t, err)
		db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/?interpolateParams=false", cn.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		defer db.Close()
		conn, err := db.Conn(ctx)
		require.NoError(t, err)
		defer conn.Close()
		assertWarnings := func(t *testing.T, want int) {
			t.Helper()
			var count int
			require.NoError(t, conn.QueryRowContext(ctx, "show count(*) warnings").Scan(&count))
			require.Equal(t, want, count)
			rows, err := conn.QueryContext(ctx, "show warnings")
			require.NoError(t, err)
			defer rows.Close()
			seen := 0
			for rows.Next() {
				var level, message string
				var code int
				require.NoError(t, rows.Scan(&level, &code, &message))
				require.Equal(t, "Warning", level)
				require.Equal(t, 1259, code)
				require.Equal(t, "ZLIB: Input data corrupted", message)
				seen++
			}
			require.NoError(t, rows.Err())
			require.Equal(t, want, seen)
		}
		t.Run("implicit_prepare", func(t *testing.T) {
			var got int64
			require.NoError(t, conn.QueryRowContext(ctx, "select uncompressed_length(?)", []byte{0}).Scan(&got))
			require.Equal(t, int64(0), got)
			assertWarnings(t, 1)
		})
		stmt, err := conn.PrepareContext(ctx, "select uncompressed_length(?)")
		require.NoError(t, err)
		defer stmt.Close()
		for _, tc := range []struct {
			name     string
			input    any
			want     sql.NullInt64
			warnings int
		}{
			{"one_byte", []byte{0}, sql.NullInt64{Int64: 0, Valid: true}, 1},
			{"empty", []byte{}, sql.NullInt64{Int64: 0, Valid: true}, 0},
			{"three_bytes", []byte{0, 0, 0}, sql.NullInt64{Int64: 0, Valid: true}, 1},
			{"null", nil, sql.NullInt64{}, 0},
			{"four_bytes", []byte{1, 0, 0, 0}, sql.NullInt64{Int64: 0, Valid: true}, 1},
			{"complete_header", []byte{3, 0, 0, 0, 0}, sql.NullInt64{Int64: 3, Valid: true}, 0},
			{"string", "x", sql.NullInt64{Int64: 0, Valid: true}, 1},
			{"repeat", []byte{0}, sql.NullInt64{Int64: 0, Valid: true}, 1},
		} {
			t.Run(tc.name, func(t *testing.T) {
				var got sql.NullInt64
				require.NoError(t, stmt.QueryRowContext(ctx, tc.input).Scan(&got))
				require.Equal(t, tc.want, got)
				assertWarnings(t, tc.warnings)
			})
		}
		t.Run("explicit_close", func(t *testing.T) {
			var result int64
			require.NoError(t, stmt.QueryRowContext(ctx, []byte{0}).Scan(&result))
			require.Zero(t, result)
			assertWarnings(t, 1)
			require.NoError(t, stmt.Close())
			assertWarnings(t, 1)
			var got int
			require.NoError(t, conn.QueryRowContext(ctx, "select 1").Scan(&got))
			require.Equal(t, 1, got)
			assertWarnings(t, 0)
		})
	})
}

// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package sqlintegration

import (
	"bufio"
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/stretchr/testify/require"
)

// Execute the canonical BVT SQL on one actual protocol session. The handwritten
// terminal rows are independent of inventory implementation details; the BVT
// runner separately verifies the protocol's formatted .result representation.
func TestDatabaseInventoryBVTSQL(t *testing.T) {
	runSQLIntegration(t, func(c embed.Cluster) {
		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
		defer cancel()
		db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", cn.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		defer db.Close()
		defer cleanupSQLIntegration(t, cn, "drop database if exists inventory_bvt")
		conn, err := db.Conn(ctx)
		require.NoError(t, err)
		defer conn.Close()
		defer func() {
			cleanupCtx, stop := context.WithTimeout(context.Background(), 5*time.Second)
			defer stop()
			_, _ = conn.ExecContext(cleanupCtx, "rollback")
		}()
		fixture, err := os.Open(filepath.Join("..", "..", "..", "test", "distributed", "cases", "database", "database_inventory.sql"))
		require.NoError(t, err)
		defer fixture.Close()
		scanner := bufio.NewScanner(fixture)
		var outputs [][][2]int
		for scanner.Scan() {
			query := strings.TrimSpace(scanner.Text())
			if query == "" || strings.HasPrefix(query, "--") {
				continue
			}
			require.True(t, strings.HasSuffix(query, ";"), "BVT keeps one complete statement per line")
			if query != "select id,ok from assertions order by id;" {
				_, err := conn.ExecContext(ctx, query)
				require.NoError(t, err, query)
				continue
			}
			values := func() [][2]int {
				rows, err := conn.QueryContext(ctx, query)
				require.NoError(t, err)
				defer rows.Close()
				columns, err := rows.Columns()
				require.NoError(t, err)
				require.Equal(t, []string{"id", "ok"}, columns)
				var values [][2]int
				for rows.Next() {
					var value [2]int
					require.NoError(t, rows.Scan(&value[0], &value[1]))
					values = append(values, value)
				}
				require.NoError(t, rows.Err())
				return values
			}()
			outputs = append(outputs, values)
		}
		require.NoError(t, scanner.Err())
		require.Equal(t, [][][2]int{
			{{1, 1}, {2, 1}, {3, 1}, {4, 1}, {5, 1}, {6, 1}, {7, 1}, {8, 1}},
			{{9, 1}, {10, 1}},
		}, outputs)
	})
}

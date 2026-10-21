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

	_ "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"
)

func TestStringMathMixedCaseHexTextProvenance(t *testing.T) {
	RunSingleCNBaseClusterTests(t, func(c Cluster) {
		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		db, err := sql.Open("mysql", fmt.Sprintf(
			"dump:111@tcp(127.0.0.1:%d)/?interpolateParams=false",
			cn.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		defer db.Close()
		db.SetMaxOpenConns(1)
		ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
		defer cancel()
		conn, err := db.Conn(ctx)
		require.NoError(t, err)
		defer conn.Close()

		read := func(t *testing.T, rows *sql.Rows, err error) [][]string {
			t.Helper()
			require.NoError(t, err)
			defer rows.Close()
			var got [][]string
			for rows.Next() {
				var id, value sql.NullString
				require.NoError(t, rows.Scan(&id, &value))
				row := []sql.NullString{id, value}
				values := make([]string, len(row))
				for i, value := range row {
					values[i] = "NULL"
					if value.Valid {
						values[i] = value.String
					}
				}
				got = append(got, values)
			}
			require.NoError(t, rows.Err())
			return got
		}
		want := [][]string{{"1", "49"}, {"2", "1"}, {"3", "NULL"}}

		t.Run("direct", func(t *testing.T) {
			query := "SELECT id, " +
				"ABS(CASE WHEN id=1 THEN X'31' WHEN id=2 THEN '1' ELSE NULL END) " +
				"FROM (SELECT 1 AS id UNION ALL SELECT 2 UNION ALL SELECT 3) AS source ORDER BY id"
			rows, err := conn.QueryContext(ctx, query)
			require.Equal(t, want, read(t, rows, err))
			require.NoError(t, rows.Err())
		})

		t.Run("prepared_text_value", func(t *testing.T) {
			query := "SELECT id, " +
				"ABS(CASE WHEN id=1 THEN X'31' WHEN id=2 THEN ? ELSE NULL END) " +
				"FROM (SELECT 1 AS id UNION ALL SELECT 2 UNION ALL SELECT 3) AS source ORDER BY id"
			stmt, err := conn.PrepareContext(ctx, query)
			require.NoError(t, err)
			defer stmt.Close()
			rows, err := stmt.QueryContext(ctx, "1")
			require.Equal(t, want, read(t, rows, err))
			require.NoError(t, rows.Err())
		})
	})
}

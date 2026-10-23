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

		read := func(t *testing.T, rows *sql.Rows) [][]string {
			t.Helper()
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
			return got
		}
		want := [][]string{{"1", "49"}, {"2", "1"}, {"3", "NULL"}}

		t.Run("direct", func(t *testing.T) {
			query := "SELECT id, " +
				"ABS(CASE WHEN id=1 THEN X'31' WHEN id=2 THEN '1' ELSE NULL END) " +
				"FROM (SELECT 1 AS id UNION ALL SELECT 2 UNION ALL SELECT 3) AS source ORDER BY id"
			rows, err := conn.QueryContext(ctx, query)
			require.NoError(t, err)
			defer rows.Close()
			require.Equal(t, want, read(t, rows))
			require.NoError(t, rows.Err())
		})

		t.Run("table_materialization", func(t *testing.T) {
			database := fmt.Sprintf("pr30669_case_provenance_%d", time.Now().UnixNano())
			_, err := conn.ExecContext(ctx, "CREATE DATABASE "+database)
			require.NoError(t, err)
			defer func() {
				_, dropErr := conn.ExecContext(ctx, "DROP DATABASE IF EXISTS "+database)
				require.NoError(t, dropErr)
			}()
			_, err = conn.ExecContext(ctx, "USE "+database)
			require.NoError(t, err)

			exec := func(t *testing.T, statement string) {
				t.Helper()
				_, err := conn.ExecContext(ctx, statement)
				require.NoError(t, err, statement)
			}
			read := func(t *testing.T, query string) [][]string {
				t.Helper()
				rows, err := conn.QueryContext(ctx, query)
				require.NoError(t, err, query)
				defer func() { require.NoError(t, rows.Close()) }()
				var got [][]string
				for rows.Next() {
					var id, hex, absolute sql.NullString
					require.NoError(t, rows.Scan(&id, &hex, &absolute))
					values := []sql.NullString{id, hex, absolute}
					row := make([]string, len(values))
					for i, value := range values {
						row[i] = "NULL"
						if value.Valid {
							row[i] = value.String
						}
					}
					got = append(got, row)
				}
				require.NoError(t, rows.Err())
				return got
			}
			want := [][]string{{"1", "31", "1"}, {"2", "31", "1"}, {"3", "NULL", "NULL"}}
			caseExpression := "CASE WHEN id=1 THEN X'31' WHEN id=2 THEN '1' ELSE NULL END"
			source := "(SELECT 1 AS id UNION ALL SELECT 2 UNION ALL SELECT 3) AS s"

			exec(t, "BEGIN")
			exec(t, "CREATE TABLE ctas_mixed AS SELECT id, "+caseExpression+" AS v FROM "+source)
			require.Equal(t, want, read(t, "SELECT id, HEX(v), ABS(v) FROM ctas_mixed ORDER BY id"))
			exec(t, "COMMIT")
			require.Equal(t, want, read(t, "SELECT id, HEX(v), ABS(v) FROM ctas_mixed ORDER BY id"))

			exec(t, "CREATE TABLE default_binary (id INT, v VARCHAR(255) DEFAULT X'31')")
			exec(t, "BEGIN")
			exec(t, "INSERT INTO default_binary(id) VALUES(1)")
			defaultWant := [][]string{{"1", "31", "1"}}
			require.Equal(t, defaultWant, read(t, "SELECT id, HEX(v), ABS(v) FROM default_binary ORDER BY id"))
			exec(t, "COMMIT")
			require.Equal(t, defaultWant, read(t, "SELECT id, HEX(v), ABS(v) FROM default_binary ORDER BY id"))

			exec(t, "CREATE TABLE inserted (id INT, v VARCHAR(255))")
			exec(t, "BEGIN")
			exec(t, "INSERT INTO inserted SELECT id, "+caseExpression+" FROM "+source)
			require.Equal(t, want, read(t, "SELECT id, HEX(v), ABS(v) FROM inserted ORDER BY id"))
			exec(t, "COMMIT")
			require.Equal(t, want, read(t, "SELECT id, HEX(v), ABS(v) FROM inserted ORDER BY id"))

			exec(t, "BEGIN")
			exec(t, "UPDATE inserted SET v="+caseExpression+" WHERE id IN (1,2,3)")
			require.Equal(t, want, read(t, "SELECT id, HEX(v), ABS(v) FROM inserted ORDER BY id"))
			exec(t, "COMMIT")
			require.Equal(t, want, read(t, "SELECT id, HEX(v), ABS(v) FROM inserted ORDER BY id"))

			exec(t, "BEGIN")
			exec(t, "INSERT INTO inserted VALUES(4, X'31')")
			exec(t, "ROLLBACK")
			exec(t, "BEGIN")
			exec(t, "INSERT INTO inserted VALUES(4, '1')")
			require.Equal(t, [][]string{{"4", "31", "1"}}, read(t, "SELECT id, HEX(v), ABS(v) FROM inserted WHERE id=4"))
			exec(t, "COMMIT")
			require.Equal(t, [][]string{{"4", "31", "1"}}, read(t, "SELECT id, HEX(v), ABS(v) FROM inserted WHERE id=4"))
		})

		t.Run("prepared_text_value", func(t *testing.T) {
			query := "SELECT id, " +
				"ABS(CASE WHEN id=1 THEN X'31' WHEN id=2 THEN ? ELSE NULL END) " +
				"FROM (SELECT 1 AS id UNION ALL SELECT 2 UNION ALL SELECT 3) AS source ORDER BY id"
			stmt, err := conn.PrepareContext(ctx, query)
			require.NoError(t, err)
			defer stmt.Close()
			rows, err := stmt.QueryContext(ctx, "1")
			require.NoError(t, err)
			defer rows.Close()
			require.Equal(t, want, read(t, rows))
			require.NoError(t, rows.Err())
		})
	})
}

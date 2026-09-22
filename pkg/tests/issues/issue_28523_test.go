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
	"math"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/stretchr/testify/require"
)

// TestIssue28523NumericCompatibilityOverBinaryPreparedStatement checks the
// actual COM_STMT path: changing sql_mode must re-evaluate an already-prepared
// expression, and MATRIXONE_NATIVE must remain strict when both mode bits exist.
func TestIssue28523NumericCompatibilityOverBinaryPreparedStatement(t *testing.T) {
	embed.RunBaseClusterTests(t, func(c embed.Cluster) {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cancel()

		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		db, err := sql.Open("mysql", fmt.Sprintf(
			"dump:111@tcp(127.0.0.1:%d)/?interpolateParams=false",
			cn.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		defer db.Close()
		// sql_mode and prepared-statement state are connection-local.
		db.SetMaxOpenConns(1)
		db.SetMaxIdleConns(1)

		for _, tc := range []struct {
			name, query            string
			prefix, complete, zero float64
		}{
			{name: "ABS", query: "SELECT ABS(?)", prefix: 1.5, complete: 125, zero: 0},
			{name: "IF", query: "SELECT IF(?, 10, 20)", prefix: 10, complete: 10, zero: 20},
			{name: "IFF", query: "SELECT IFF(?, 10, 20)", prefix: 10, complete: 10, zero: 20},
		} {
			t.Run(tc.name, func(t *testing.T) {
				_, err := db.ExecContext(ctx, "SET SESSION sql_mode = ''")
				require.NoError(t, err)
				stmt, err := db.PrepareContext(ctx, tc.query)
				require.NoError(t, err)
				defer stmt.Close()
				query := func(value string) (float64, error) {
					var got float64
					err := stmt.QueryRowContext(ctx, value).Scan(&got)
					return got, err
				}
				// Toggle in both directions without replacing the COM_STMT handle.
				for _, mode := range []string{"", "MYSQL_NUMERIC_COMPATIBILITY", "", "MYSQL_NUMERIC_COMPATIBILITY", "MATRIXONE_NATIVE,MYSQL_NUMERIC_COMPATIBILITY"} {
					_, err := db.ExecContext(ctx, "SET SESSION sql_mode = '"+mode+"'")
					require.NoError(t, err)
					got, err := query("1.5tail")
					if mode == "MYSQL_NUMERIC_COMPATIBILITY" {
						require.NoError(t, err)
						require.Equal(t, tc.prefix, got)
					} else {
						require.ErrorContains(t, err, "invalid numeric string", mode)
					}
					got, err = query("  -1.25e2 ")
					require.NoError(t, err, "complete tokens must work after rejected input")
					require.Equal(t, tc.complete, got)
					got, err = query("0")
					require.NoError(t, err)
					require.Equal(t, tc.zero, got)
				}
			})
		}

		for _, name := range []string{"ROUND", "TRUNCATE", "CEIL", "CEILING", "FLOOR"} {
			t.Run(name+" precision", func(t *testing.T) {
				stmt, err := db.PrepareContext(ctx, "SELECT "+name+"(?, ?)")
				require.NoError(t, err)
				defer stmt.Close()
				for _, mode := range []string{"", "MYSQL_NUMERIC_COMPATIBILITY"} {
					_, err := db.ExecContext(ctx, "SET SESSION sql_mode = '"+mode+"'")
					require.NoError(t, err)
					var got float64
					err = stmt.QueryRowContext(ctx, 12.345, "2.5tail").Scan(&got)
					require.ErrorContains(t, err, "invalid argument cast to int", mode)
					for _, precision := range []float64{0x1p63, math.Nextafter(-0x1p63, math.Inf(-1))} {
						err = stmt.QueryRowContext(ctx, 12.345, precision).Scan(&got)
						require.ErrorContains(t, err, "out of range", mode)
					}
					err = stmt.QueryRowContext(ctx, 12.345, 2.5).Scan(&got)
					require.NoError(t, err, "ordinary INT64 rounding and post-error reuse")
					require.InDelta(t, 12.345, got, 1e-10)
				}
			})
		}

		t.Run("binary literal and flow control ownership", func(t *testing.T) {
			// A HEX literal in numeric context is 49. A binary string containing
			// "1" converts to 1. Mixed string-valued conditionals retain that
			// provenance per row: the HEX row is 49 and the text row is 1.
			// Ordinary binary-string values remain distinct from HEX/BIT literals.
			var literal, binaryString float64
			require.NoError(t, db.QueryRowContext(ctx,
				"SELECT ABS(X'31'), ABS(CAST('1' AS BINARY))").Scan(&literal, &binaryString))
			require.Equal(t, float64(49), literal)
			require.Equal(t, float64(1), binaryString)
			for _, expression := range []string{
				"CASE WHEN id=1 THEN X'31' WHEN id=2 THEN '1' ELSE NULL END",
				"IF(id=1, X'31', IF(id=2, '1', NULL))",
				"COALESCE(IF(id=1, X'31', NULL), IF(id=2, '1', NULL))",
			} {
				query := fmt.Sprintf("SELECT id, ABS(%[1]s), ROUND(%[1]s), MOD(%[1]s, 50) "+
					"FROM (SELECT 1 AS id UNION ALL SELECT 2 UNION ALL SELECT 3) AS src WHERE id <= ? ORDER BY id", expression)
				stmt, err := db.PrepareContext(ctx, query)
				require.NoError(t, err)
				func() {
					defer stmt.Close()
					for _, mode := range []string{"", "MYSQL_NUMERIC_COMPATIBILITY", "MATRIXONE_NATIVE"} {
						_, err := db.ExecContext(ctx, "SET SESSION sql_mode = '"+mode+"'")
						require.NoError(t, err)
						rows, err := stmt.QueryContext(ctx, 3)
						require.NoError(t, err)
						func() {
							defer rows.Close()
							count := 0
							for rows.Next() {
								var id int
								var abs, round, mod sql.NullFloat64
								require.NoError(t, rows.Scan(&id, &abs, &round, &mod))
								count++
								require.Equal(t, count, id)
								for _, got := range []sql.NullFloat64{abs, round, mod} {
									require.Equal(t, id != 3, got.Valid, expression)
									if id != 3 {
										want := float64(1)
										if id == 1 {
											want = 49
										}
										require.Equal(t, want, got.Float64, "mode=%q expression=%s", mode, expression)
									}
								}
							}
							require.NoError(t, rows.Err())
							require.Equal(t, 3, count)
						}()
					}
				}()
			}
		})
	})
}

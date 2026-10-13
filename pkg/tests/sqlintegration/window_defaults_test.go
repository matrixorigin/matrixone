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

// Issue #29925: explicit NULL defaults must survive argument-vector reuse
// when the last partition is smaller than the preceding partition.
func TestLagLeadNullDefaultsAcrossPartitions(t *testing.T) {
	runSQLIntegration(t, func(c embed.Cluster) {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cancel()
		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", cn.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		defer db.Close()
		db.SetMaxOpenConns(1)

		const input = ` FROM (
 SELECT 1 point, 1 eid, CAST(1 AS DOUBLE) value
 UNION ALL SELECT 2, 1, 2
 UNION ALL SELECT 3, 2, 3
) f ORDER BY point`
		for _, function := range []string{"LAG", "LEAD"} {
			for _, test := range []struct {
				name, defaultArg string
				zero             bool
			}{
				{name: "untyped_NULL", defaultArg: ", NULL"},
				{name: "typed_DOUBLE_NULL", defaultArg: ", CAST(NULL AS DOUBLE)"},
				{name: "omitted"},
				{name: "zero", defaultArg: ", CAST(0 AS DOUBLE)", zero: true},
				{name: "row_dependent", defaultArg: ", CASE WHEN point = 3 THEN CAST(NULL AS DOUBLE) ELSE CAST(-1 AS DOUBLE) END"},
			} {
				t.Run(function+"/"+test.name, func(t *testing.T) {
					query := "SELECT point, eid, " + function + "(value, 1" + test.defaultArg +
						") OVER (PARTITION BY eid ORDER BY point) AS result" + input
					rows, err := db.QueryContext(ctx, query)
					require.NoError(t, err, query)
					defer rows.Close()
					want := []float64{0, 1, 0}
					nulls := []bool{true, false, true}
					if function == "LEAD" {
						want = []float64{2, 0, 0}
						nulls = []bool{false, true, true}
					}
					if test.zero {
						nulls = []bool{false, false, false}
					} else if test.name == "row_dependent" {
						if function == "LAG" {
							want[0], nulls[0] = -1, false
						} else {
							want[1], nulls[1] = -1, false
						}
					}
					row := 0
					for rows.Next() {
						require.Less(t, row, 3)
						var point, eid int
						var value sql.NullFloat64
						require.NoError(t, rows.Scan(&point, &eid, &value))
						require.Equal(t, row+1, point)
						wantEID := 1
						if row == 2 {
							wantEID = 2
						}
						require.Equal(t, wantEID, eid)
						require.Equal(t, !nulls[row], value.Valid, "point %d NULL bit", point)
						if value.Valid {
							require.Equal(t, want[row], value.Float64, "point %d value", point)
						}
						row++
					}
					require.NoError(t, rows.Err())
					require.Equal(t, 3, row, "must include the final singleton partition")
				})
			}
		}
		var one int
		require.NoError(t, db.QueryRowContext(ctx, "SELECT 1").Scan(&one))
		require.Equal(t, 1, one)
	})
}

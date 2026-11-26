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

package isolated

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/stretchr/testify/require"
)

// TestMoTableColExtremaDecimalScale checks that mo_table_col_min and
// mo_table_col_max render DECIMAL extrema with the column's declared scale,
// matching the ordinary MIN/MAX aggregates over real persisted zonemap
// ranges. Decimal64 (decimal(10,3)) and Decimal128 (decimal(19,4)) columns
// carry negative extrema with trailing zeros; int columns around them keep
// column positions observable; decimal(6,0) is a scale-zero control; and a
// never-populated table plus an absent column keep SQL NULL observable.
func TestMoTableColExtremaDecimalScale(t *testing.T) {
	cluster, err := embed.StartTestCluster(embed.WithCNCount(1))
	if cluster != nil {
		t.Cleanup(func() { require.NoError(t, cluster.Close()) })
	}
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Minute)
	defer cancel()
	cn, err := cluster.GetCNService(0)
	require.NoError(t, err)
	db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/decimal_extrema", cn.GetServiceConfig().CN.Frontend.Port))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	admin, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", cn.GetServiceConfig().CN.Frontend.Port))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, admin.Close()) })
	require.NoError(t, waitSystemBootstrap(ctx, admin))
	execSQLRequire(t, ctx, admin, "create database decimal_extrema")
	const schema = "(a int, d64 decimal(10,3), b int, d128 decimal(19,4), c int, dzero decimal(6,0))"
	execSQLRequire(t, ctx, db, "create table extrema "+schema)
	execSQLRequire(t, ctx, db, "create table never_inserted "+schema)

	// Two flushes leave two persisted objects, so the extrema functions read
	// persisted zonemap ranges instead of a single in-memory block.
	execSQLRequire(t, ctx, db, "insert into extrema values (1, -12.500, 10, -0.5000, 100, -5)")
	execSQLRequire(t, ctx, db, "select mo_ctl('dn','flush','decimal_extrema.extrema')")
	execSQLRequire(t, ctx, db, "insert into extrema values (2, 7.250, 20, 7.2500, 200, 42)")
	execSQLRequire(t, ctx, db, "insert into extrema values (3, 12345.678, 30, 99999999999999.9999, 300, 999999)")
	execSQLRequire(t, ctx, db, "select mo_ctl('dn','flush','decimal_extrema.extrema')")

	want := []struct {
		column   string
		min, max string
	}{
		{"d64", "-12.500", "12345.678"},            // Decimal64, negative min with trailing zeros
		{"d128", "-0.5000", "99999999999999.9999"}, // Decimal128, scale 4 extrema on both sides
		{"a", "1", "3"},                            // int anchors around decimal positions
		{"b", "10", "30"},
		{"c", "100", "300"},
		{"dzero", "-5", "999999"}, // scale-zero DECIMAL control
	}
	for _, w := range want {
		t.Run(w.column, func(t *testing.T) {
			// "where a > 0" matches every row, so the ordinary aggregate oracle
			// covers exactly the rows whose extrema must match the metadata.
			var aggLo, aggHi sql.NullString
			require.NoError(t, db.QueryRowContext(ctx,
				"select min("+w.column+"), max("+w.column+") from extrema where a > 0").Scan(&aggLo, &aggHi))
			require.True(t, aggLo.Valid, "aggregate min for %s", w.column)
			require.True(t, aggHi.Valid, "aggregate max for %s", w.column)
			require.Equal(t, w.min, aggLo.String, "aggregate min for %s", w.column)
			require.Equal(t, w.max, aggHi.String, "aggregate max for %s", w.column)

			var lo, hi sql.NullString
			require.NoError(t, db.QueryRowContext(ctx,
				"select mo_table_col_min('decimal_extrema','extrema','"+w.column+"'), "+
					"mo_table_col_max('decimal_extrema','extrema','"+w.column+"') from dual").
				Scan(&lo, &hi))
			require.True(t, lo.Valid, "mo_table_col_min for %s", w.column)
			require.True(t, hi.Valid, "mo_table_col_max for %s", w.column)
			require.Equal(t, w.min, lo.String, "mo_table_col_min for %s", w.column)
			require.Equal(t, w.max, hi.String, "mo_table_col_max for %s", w.column)
		})
	}

	t.Run("never_inserted is null", func(t *testing.T) {
		var lo, hi sql.NullString
		require.NoError(t, db.QueryRowContext(ctx,
			"select mo_table_col_min('decimal_extrema','never_inserted','d128'), "+
				"mo_table_col_max('decimal_extrema','never_inserted','d128') from dual").Scan(&lo, &hi))
		require.False(t, lo.Valid)
		require.False(t, hi.Valid)
	})
	t.Run("absent column is null", func(t *testing.T) {
		var value sql.NullString
		require.NoError(t, db.QueryRowContext(ctx,
			"select mo_table_col_max('decimal_extrema','extrema','absent') from dual").Scan(&value))
		require.False(t, value.Valid)
	})
}

// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package embed

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestCatalogDecimalMigrationUsesCurrentStatementContext(t *testing.T) {
	// Share the canonical SQL fixture; starting a separate two-CN fixture
	// prevents later single-CN catalog regressions from obtaining admission.
	RunSingleCNBaseClusterTests(t, func(c Cluster) {
		db := openIcebergTestDB(t, c)
		defer db.Close()
		ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
		defer cancel()
		conn, err := db.Conn(ctx)
		require.NoError(t, err)
		defer conn.Close()
		exec := func(query string) {
			t.Helper()
			_, err := conn.ExecContext(ctx, query)
			require.NoError(t, err, query)
		}
		exec("create database catalog_decimal_context")
		defer func() {
			_, err := conn.ExecContext(t.Context(), "drop database catalog_decimal_context")
			require.NoError(t, err)
		}()
		for i := range 3 {
			var one int
			require.NoError(t, conn.QueryRowContext(ctx, "select 1").Scan(&one))
			require.Equal(t, 1, one)
			table := fmt.Sprintf("catalog_decimal_context.t%d", i)
			exec(fmt.Sprintf("create table %s(id int auto_increment primary key, i bigint, tm time(0), constraint ck_%d check(i>tm))", table, i))
			exec("insert into " + table + "(i,tm) values(1000000000000000000,'00:00:00')")
			var value int64
			require.NoError(t, conn.QueryRowContext(ctx, "select i from "+table).Scan(&value))
			require.Equal(t, int64(1000000000000000000), value)
		}
		for _, value := range []string{"999999999999999999", "1000000000000000000", "-1000000000000000000", "9223372036854775807", "-9223372036854775808"} {
			for _, reverse := range []bool{false, true} {
				left, right := "cast("+value+" as signed)", "1.0"
				if reverse {
					left, right = right, left
				}
				query := "select " + left + " * " + right
				var got string
				require.NoError(t, conn.QueryRowContext(ctx, query).Scan(&got), query)
				require.Equal(t, value+".0", got, query)
			}
		}
	})
}

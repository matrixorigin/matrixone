// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
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
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/stretchr/testify/require"
	"math/big"
	"strings"
	"testing"
	"time"
)

func TestMixedIntegerComparisons(t *testing.T) {
	runSQLIntegration(t, func(c embed.Cluster) {
		ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
		defer cancel()
		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", cn.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		defer db.Close()
		conn, err := db.Conn(ctx)
		require.NoError(t, err)
		defer conn.Close()
		defer cleanupSQLIntegration(t, cn, "drop database if exists mixed_integer_comparison")
		exec := func(q string) { t.Helper(); _, err := conn.ExecContext(ctx, q); require.NoError(t, err, q) }
		exec("create database mixed_integer_comparison")
		exec("use mixed_integer_comparison")
		domains := []string{"tinyint", "smallint", "int", "bigint", "tinyint unsigned", "smallint unsigned", "int unsigned", "bigint unsigned"}
		values := []string{"-128", "-32768", "-2147483648", "-9223372036854775808", "255", "65535", "4294967295", "18446744073709551615"}
		cols := make([]string, len(domains))
		for i, d := range domains {
			cols[i] = fmt.Sprintf("v%d %s", i, d)
		}
		exec("create table extrema(" + strings.Join(cols, ",") + ")")
		exec("insert into extrema values(" + strings.Join(values, ",") + ")")
		for i, a := range values {
			for j, b := range values {
				av, ok := new(big.Int).SetString(a, 10)
				require.True(t, ok)
				bv, ok := new(big.Int).SetString(b, 10)
				require.True(t, ok)
				cmp := av.Cmp(bv)
				for _, op := range []string{"=", "!=", "<", ">", "<=", ">="} {
					want := map[string]bool{"=": cmp == 0, "!=": cmp != 0, "<": cmp < 0, ">": cmp > 0, "<=": cmp <= 0, ">=": cmp >= 0}[op]
					q := fmt.Sprintf("select v%d %s v%d from extrema", i, op, j)
					var got bool
					require.NoError(t, conn.QueryRowContext(ctx, q).Scan(&got), q)
					require.Equal(t, want, got, q)
				}
			}
		}
		for _, q := range []string{"select cast(-1 as smallint) between cast(0 as tinyint unsigned) and cast(65535 as smallint unsigned)", "select cast(-1 as tinyint) = cast(18446744073709551615 as bigint unsigned)", "select cast(9007199254740993 as bigint) = cast(9007199254740992 as bigint unsigned)"} {
			var got bool
			require.NoError(t, conn.QueryRowContext(ctx, q).Scan(&got), q)
			require.False(t, got, q)
		}
		exec("create table s(v bigint)")
		exec("create table u(v bigint unsigned primary key)")
		exec("insert into s values(-1),(0),(9007199254740993),(null)")
		exec("insert into u values(0),(9007199254740992),(9007199254740993),(18446744073709551615)")
		var count int
		require.NoError(t, conn.QueryRowContext(ctx, "select count(*) from s join u on s.v=u.v").Scan(&count))
		require.Equal(t, 2, count)
		require.NoError(t, conn.QueryRowContext(ctx, "select count(*) from u where v < cast(-1 as tinyint)").Scan(&count))
		require.Zero(t, count)
		var null sql.NullBool
		require.NoError(t, conn.QueryRowContext(ctx, "select v < cast(1 as bigint unsigned) from s where v is null").Scan(&null))
		require.False(t, null.Valid)
		stmt, err := conn.PrepareContext(ctx, "select ? < cast(18446744073709551615 as bigint unsigned)")
		require.NoError(t, err)
		defer stmt.Close()
		for _, v := range []int64{-1, 0, 9223372036854775807, -2} {
			var got bool
			require.NoError(t, stmt.QueryRowContext(ctx, v).Scan(&got))
			require.True(t, got)
		}
	})
}

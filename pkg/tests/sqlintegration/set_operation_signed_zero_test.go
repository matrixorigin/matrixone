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

// Set operations use SQL equality, which identifies -0 and +0 (issue #30155).
// MatrixOne intentionally does not follow MySQL's signed-zero set membership.
func TestSetOperationSignedZeroNormalized(t *testing.T) {
	runSQLIntegration(t, func(c embed.Cluster) {
		ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
		defer cancel()
		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", cn.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		defer db.Close()

		const schema = "set_operation_signed_zero_30155"
		execSQLRequire(t, ctx, db, "drop database if exists "+schema)
		defer cleanupSQLIntegration(t, cn, "drop database if exists "+schema)
		execSQLRequire(t, ctx, db, "create database "+schema)

		for _, typ := range []string{"float", "double"} {
			t.Run(typ, func(t *testing.T) {
				table := schema + ".z_" + typ
				execSQLRequire(t, ctx, db, "create table "+table+"(id int primary key, x "+typ+")")
				execSQLRequire(t, ctx, db, "insert into "+table+" values (1, -0e0), (2, 0e0)")
				neg := "select x from " + table + " where id = 1"
				pos := "select x from " + table + " where id = 2"
				for _, tc := range []struct {
					op   string
					want int64
				}{
					{"union", 1},
					{"union all", 2},
					{"intersect", 1},
					{"intersect all", 1},
					{"except", 0},
					{"except all", 0},
				} {
					t.Run(tc.op, func(t *testing.T) {
						for _, q := range []string{neg + " " + tc.op + " " + pos, pos + " " + tc.op + " " + neg} {
							var got int64
							require.NoError(t, db.QueryRowContext(ctx, "select count(*) from ("+q+") q").Scan(&got), q)
							require.Equal(t, tc.want, got, q)
						}
					})
				}
			})
		}

		var count int64
		require.NoError(t, db.QueryRowContext(ctx, "select count(*) from (select -0e0 intersect select 0e0) q").Scan(&count))
		require.Equal(t, int64(1), count)
		require.NoError(t, db.QueryRowContext(ctx, "select count(*) from (select -0e0 except select 0e0) q").Scan(&count))
		require.Equal(t, int64(0), count)
	})
}

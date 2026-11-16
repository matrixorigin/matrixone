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

package enumcollation

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

func TestCollatedEnumOverflowRejectsWithoutPartialWrite(t *testing.T) {
	cluster, err := embed.StartTestCluster(embed.WithCNCount(1))
	if cluster != nil {
		t.Cleanup(func() { require.NoError(t, cluster.Close()) })
	}
	require.NoError(t, err)
	cn, err := cluster.GetCNService(0)
	require.NoError(t, err)
	db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", cn.GetServiceConfig().CN.Frontend.Port))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	_, err = db.ExecContext(ctx, "create database enum_overflow_32814")
	require.NoError(t, err)
	for _, tc := range []struct{ name, collation string }{
		{"t_bin", "utf8mb4_bin"}, {"t_ci", "utf8mb4_general_ci"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			table := "enum_overflow_32814." + tc.name
			_, err := db.ExecContext(ctx, "create table "+table+"(e enum('a','b') collate "+tc.collation+")")
			require.NoError(t, err)
			_, err = db.ExecContext(ctx, "insert into "+table+" values('b')")
			require.NoError(t, err)
			for _, input := range []string{"65535", "65535 ", "65536", "65536 ", "65537", "65537 "} {
				_, err = db.ExecContext(ctx, "insert into "+table+" values ('a'), ('"+input+"')")
				require.Error(t, err, "input %q", input)
				var count int
				require.NoError(t, db.QueryRowContext(ctx, "select count(*) from "+table).Scan(&count))
				require.Equal(t, 1, count, "the valid row in the failed INSERT must roll back")
			}
			_, err = db.ExecContext(ctx, "insert ignore into "+table+" values ('65537 ')")
			require.NoError(t, err)
			var bad, first int
			require.NoError(t, db.QueryRowContext(ctx, "select count(*) from "+table+" where e+0=0").Scan(&bad))
			require.NoError(t, db.QueryRowContext(ctx, "select count(*) from "+table+" where e+0=1").Scan(&first))
			require.Equal(t, 1, bad)
			require.Zero(t, first, "IGNORE must not turn an overflow into a valid first member")
			_, err = db.ExecContext(ctx, "insert into "+table+" values ('2 ')")
			require.NoError(t, err)
			var count int
			require.NoError(t, db.QueryRowContext(ctx, "select count(*) from "+table+" where e+0=2").Scan(&count))
			require.Equal(t, 2, count)
		})
	}
}

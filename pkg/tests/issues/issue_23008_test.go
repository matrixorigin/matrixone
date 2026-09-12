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

package issues

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

// Use an actual frontend session: SET/EXECUTE must carry the assignment's SQL
// domain, and the ENUM label must come from the real catalog/binder, not from a
// text vector whose expected label has already been installed by the test.
func TestIssue23008MemberOfScalarDomains(t *testing.T) {
	embed.RunBaseClusterTests(t, func(c embed.Cluster) {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
		defer cancel()
		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/?interpolateParams=false", cn.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		defer db.Close()
		conn, err := db.Conn(ctx)
		require.NoError(t, err)
		defer conn.Close()
		exec := func(query string) {
			t.Helper()
			_, err := conn.ExecContext(ctx, query)
			require.NoError(t, err, query)
		}
		value := func(query string, want int64) {
			t.Helper()
			var got int64
			require.NoError(t, conn.QueryRowContext(ctx, query).Scan(&got), query)
			require.Equal(t, want, got, query)
		}
		exec("create database issue23008_scalar")
		defer func() { _, _ = conn.ExecContext(ctx, "drop database issue23008_scalar") }()
		exec("use issue23008_scalar")
		exec("create table scalar_values (e enum('red','blue'), g geometry, g32 geometry32, b bit(9))")
		exec("insert into scalar_values values ('red', cast('POINT(1 2)' as geometry), cast('POINT(1 2)' as geometry32), 266), ('blue', cast('POINT(3 4)' as geometry), cast('POINT(3 4)' as geometry32), 1)")
		value(`select count(*) from scalar_values where e member of ('["red"]')`, 1)
		value(`select sum(e member of ('[1,2]')) from scalar_values`, 0)
		value(`select count(*) from scalar_values where g member of ('[{"type":"Point","coordinates":[1,2]}]')`, 1)
		value(`select count(*) from scalar_values where g32 member of ('[{"type":"Point","coordinates":[1,2]}]')`, 1)
		value("select sum(b member of (json_array(b))) from scalar_values", 2)
		exec("prepare membership from 'select ? member of (?)'")
		defer func() { _, _ = conn.ExecContext(ctx, "deallocate prepare membership") }()
		// Reuse one plan across failures and valid values, on both operands.
		for i := 0; i < 3; i++ {
			exec("set @left_value = cast('[1,2]' as vecf32(2))")
			exec(`set @right_value = '["[1 2]"]'`)
			var ignored int64
			err := conn.QueryRowContext(ctx, "execute membership using @left_value, @right_value").Scan(&ignored)
			require.ErrorContains(t, err, "argument 1")
			exec("set @left_value = 1")
			exec("set @right_value = cast('[1]' as vecf32(1))")
			err = conn.QueryRowContext(ctx, "execute membership using @left_value, @right_value").Scan(&ignored)
			require.ErrorContains(t, err, "argument 2")
			exec("set @left_value = null")
			var nullable sql.NullInt64
			require.NoError(t, conn.QueryRowContext(ctx, "execute membership using @left_value, @right_value").Scan(&nullable))
			require.False(t, nullable.Valid)
			exec("set @left_value = 1")
			exec("set @right_value = '[1]'")
			value("execute membership using @left_value, @right_value", 1)
		}
		exec("set @left_value = (select e from scalar_values where e = 'red')")
		exec(`set @right_value = '["red"]'`)
		value("execute membership using @left_value, @right_value", 1)
		// SET currently rejects native geometry user variables before MEMBER
		// OF is reached. Do not invent a supported assignment in the fixture.
		_, err = conn.ExecContext(ctx, "set @left_value = (select g from scalar_values where e = 'red')")
		require.ErrorContains(t, err, "invalid argument variable type")
		// The binary driver's PREPARE/EXECUTE path also reaches the real filter.
		stmt, err := conn.PrepareContext(ctx, "select count(*) from scalar_values where e member of (?)")
		require.NoError(t, err)
		defer stmt.Close()
		var count int64
		require.NoError(t, stmt.QueryRowContext(ctx, `["blue"]`).Scan(&count))
		require.Equal(t, int64(1), count)
		value("select 1", 1)
	})
}

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

func TestCountNullabilityAfterAddSQL(t *testing.T) {
	runSQLIntegration(t, func(c embed.Cluster) {
		ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
		defer cancel()
		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", cn.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		defer db.Close()
		const schema = "count_nullability_add"
		const table = schema + ".customer"
		defer cleanupSQLIntegration(t, cn, "drop database if exists "+schema)
		for _, statement := range []string{
			"create database " + schema,
			"create table " + table + "(id int not null,tenant int not null,v int,primary key(tenant,id))",
			"insert into " + table + " values (1,1,7),(2,1,null),(3,1,7)",
			"select mo_ctl('dn','flush','" + table + "')",
			"alter table " + table + " add column extra int",
		} {
			execSQLRequire(t, ctx, db, statement)
		}
		for _, test := range []struct {
			query string
			want  int
		}{
			{"select count(id) from " + table, 3},
			{"select count(v) from " + table, 2},
			{"select count(null) from " + table, 0},
			{"select count(distinct v) from " + table, 1},
			{"select count(extra) from " + table, 0},
			{"select count(q.id) from " + table + " p left join " + table + " q on p.id=q.id and q.id=1", 1},
		} {
			t.Run(test.query, func(t *testing.T) {
				var got int
				require.NoError(t, db.QueryRowContext(ctx, test.query).Scan(&got))
				require.Equal(t, test.want, got)
			})
		}
		rows, err := db.QueryContext(ctx, "select id,v,extra from "+table+" order by id")
		require.NoError(t, err)
		defer rows.Close()
		seen := 0
		for rows.Next() {
			var id int
			var value, extra sql.NullInt64
			require.NoError(t, rows.Scan(&id, &value, &extra))
			seen++
			require.Equal(t, seen, id)
			require.Equal(t, id != 2, value.Valid)
			if value.Valid {
				require.Equal(t, int64(7), value.Int64)
			}
			require.False(t, extra.Valid)
		}
		require.NoError(t, rows.Err())
		require.Equal(t, 3, seen)
	})
}

// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
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

	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/stretchr/testify/require"
)

func TestIssue28909DirectFunctionWireMetadata(t *testing.T) {
	embed.RunBaseClusterTests(t, func(c embed.Cluster) {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
		defer cancel()
		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/?interpolateParams=false", cn.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		defer db.Close()
		for _, prepared := range []bool{false, true} {
			t.Run(fmt.Sprintf("prepared=%v", prepared), func(t *testing.T) {
				const query = "select find_in_set('b','a,b') as pos, strcmp('a','b') as cmp"
				var rows *sql.Rows
				if prepared {
					stmt, err := db.PrepareContext(ctx, query)
					require.NoError(t, err)
					defer stmt.Close()
					rows, err = stmt.QueryContext(ctx)
					require.NoError(t, err)
				} else {
					rows, err = db.QueryContext(ctx, query)
					require.NoError(t, err)
				}
				defer rows.Close()
				cols, err := rows.ColumnTypes()
				require.NoError(t, err)
				require.Len(t, cols, 2)
				for _, col := range cols {
					require.Equal(t, "BIGINT", col.DatabaseTypeName(), col.Name())
				}
				require.True(t, rows.Next())
				var pos, cmp int64
				require.NoError(t, rows.Scan(&pos, &cmp))
				require.Equal(t, int64(2), pos)
				require.Equal(t, int64(-1), cmp)
				require.False(t, rows.Next())
				require.NoError(t, rows.Err())
			})
		}
		const schema = "issue_28909_wire_metadata"
		execSQLRequire(t, ctx, db, "create database "+schema)
		defer execSQLMaybe(t, ctx, db, "drop database if exists "+schema)
		execSQLRequire(t, ctx, db, "create table "+schema+".src(id int primary key, s varchar(20))")
		execSQLRequire(t, ctx, db, "insert into "+schema+".src values(1,'a'),(2,'b'),(3,'中'),(4,NULL)")
		check := func(rows *sql.Rows, wantTypes []string, want [][]sql.NullInt64) {
			t.Helper()
			defer rows.Close()
			cols, err := rows.ColumnTypes()
			require.NoError(t, err)
			require.Len(t, cols, len(wantTypes))
			for i, typ := range wantTypes {
				require.Equal(t, typ, cols[i].DatabaseTypeName(), cols[i].Name())
			}
			for _, expected := range want {
				require.True(t, rows.Next())
				actual := make([]sql.NullInt64, len(cols))
				dest := make([]any, len(cols))
				for i := range dest {
					dest[i] = &actual[i]
				}
				require.NoError(t, rows.Scan(dest...))
				require.Equal(t, expected, actual)
			}
			require.False(t, rows.Next())
			require.NoError(t, rows.Err())
		}
		n := func(v int64) sql.NullInt64 { return sql.NullInt64{Int64: v, Valid: true} }
		want := [][]sql.NullInt64{{n(1), n(-1), n(1)}, {n(2), n(0), n(2)}, {n(3), n(1), n(3)}, {{}, {}, n(4)}}
		parameterStmt, err := db.PrepareContext(ctx, "select find_in_set(?,'a,b,中'), strcmp(?,'b'), cast(73 as signed)")
		require.NoError(t, err)
		defer parameterStmt.Close()
		for i, value := range []any{"a", "b", "中", nil, "a"} {
			rows, err := parameterStmt.QueryContext(ctx, value, value)
			require.NoError(t, err)
			defer rows.Close()
			expected := want[i%len(want)]
			check(rows, []string{"BIGINT", "BIGINT", "BIGINT"}, [][]sql.NullInt64{{expected[0], expected[1], n(73)}})
			require.NoError(t, rows.Err())
		}
		const projection = "select find_in_set(s,'a,b,中') pos, strcmp(s,'b') cmp, id from " + schema + ".src"
		rows, err := db.QueryContext(ctx, projection+" order by id")
		require.NoError(t, err)
		defer rows.Close()
		check(rows, []string{"BIGINT", "BIGINT", "INT"}, want)
		require.NoError(t, rows.Err())
		stmt, err := db.PrepareContext(ctx, projection+" where id=?")
		require.NoError(t, err)
		defer stmt.Close()
		for pass := 0; pass < 2; pass++ {
			if pass == 1 {
				execSQLRequire(t, ctx, db, "alter table "+schema+".src add column extra int")
			}
			for i, expected := range want {
				rows, err := stmt.QueryContext(ctx, i+1)
				require.NoError(t, err)
				defer rows.Close()
				check(rows, []string{"BIGINT", "BIGINT", "INT"}, [][]sql.NullInt64{expected})
				require.NoError(t, rows.Err())
			}
		}
		// Cluster availability precedes the two-phase catalog admission fence.
		// Wait for its actual authoring readiness; never override the safety gate.
		require.Eventually(t, func() bool {
			value, ok := moruntime.ServiceRuntime(cn.GetServiceConfig().CN.UUID).GetGlobalVariables(moruntime.PersistedExpressionProtocolAuthoringFloor)
			floor, valid := value.(int64)
			return ok && valid && floor >= int64(defines.MORPCLatestVersion)
		}, time.Minute, 100*time.Millisecond, "catalog expression admission must be ready")
		execSQLRequire(t, ctx, db, "create view "+schema+".v as "+projection)
		execSQLRequire(t, ctx, db, "create table "+schema+".stored as "+projection)
		metadata, err := db.QueryContext(ctx, "select table_name,column_name,data_type from information_schema.columns where table_schema='"+schema+"' and table_name in ('v','stored') order by table_name,ordinal_position")
		require.NoError(t, err)
		defer metadata.Close()
		count := 0
		for metadata.Next() {
			var table, column, typ string
			require.NoError(t, metadata.Scan(&table, &column, &typ))
			require.Equal(t, "int", typ, table+"."+column)
			count++
		}
		require.NoError(t, metadata.Err())
		require.NoError(t, metadata.Close())
		require.Equal(t, 6, count)
		rows, err = db.QueryContext(ctx, "select pos,cmp,id from "+schema+".stored order by id")
		require.NoError(t, err)
		defer rows.Close()
		check(rows, []string{"INT", "INT", "INT"}, want)
		require.NoError(t, rows.Err())
	})
}

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
	"github.com/stretchr/testify/require"

	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/tests/testutils"
)

// TestIssue28469BinaryPreparedIntegerAssignment verifies that FLOAT parameters
// retain ties-to-even semantics beneath numeric expressions and that negative
// FLOAT values reach the unsigned assignment boundary without wrapping.
func TestIssue28469BinaryPreparedIntegerAssignment(t *testing.T) {
	embed.RunBaseClusterTests(t, func(c embed.Cluster) {
		ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
		defer cancel()
		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/?interpolateParams=false",
			cn.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		defer db.Close()
		conn, err := db.Conn(ctx)
		require.NoError(t, err)
		defer conn.Close()

		dbName := testutils.GetDatabaseName(t)
		mustExec(t, ctx, conn, fmt.Sprintf("create database `%s`", dbName))
		mustExec(t, ctx, conn, fmt.Sprintf("use `%s`", dbName))
		defer func() {
			cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cleanupCancel()
			_, _ = db.ExecContext(cleanupCtx, fmt.Sprintf("drop database if exists `%s`", dbName))
		}()

		mustExec(t, ctx, conn, "create table src (x bigint)")
		mustExec(t, ctx, conn, "insert into src values (5)")
		mustExec(t, ctx, conn, "create table dst (v bigint)")
		for _, tc := range []struct {
			name, query string
			want        int64
		}{
			{"constant_division", "insert into dst values (5 / 2)", 3},
			{"projection", "insert into dst select x / 2 from src", 3},
			{"abs_wrapper", "insert into dst select abs(x / 2) from src", 3},
			{"negation_wrapper", "insert into dst select -(x / 2) from src", -3},
			{"addition_wrapper", "insert into dst select x / 2 + 0 from src", 3},
			{"update_wrapper", "update dst set v = abs(5 / 2)", 3},
			{"approximate_control", "insert into dst select x / 2E0 from src", 2},
			{"folded_approximate_control", "insert into dst values (abs(5E0 / 2) + 0)", 2},
			{"explicit_float_boundary", "insert into dst select cast(x / 2 as double) from src", 2},
			{"derived_projection", "insert into dst select abs(q) from (select x / 2 as q from src) s", 3},
			{"negative_constant", "insert into dst values (-(5 / 2))", -3},
		} {
			t.Run(tc.name, func(t *testing.T) {
				mustExec(t, ctx, conn, "delete from dst")
				if tc.name == "update_wrapper" {
					mustExec(t, ctx, conn, "insert into dst values (0)")
				}
				mustExec(t, ctx, conn, tc.query)
				var got int64
				require.NoError(t, conn.QueryRowContext(ctx, "select v from dst").Scan(&got))
				require.Equal(t, tc.want, got)
			})
		}

		t.Run("division_unique_key", func(t *testing.T) {
			mustExec(t, ctx, conn, "create table quotient_source (result bigint)")
			mustExec(t, ctx, conn, "insert into quotient_source values (150), (250)")
			mustExec(t, ctx, conn, "create table quotient_target (a bigint, b bigint, primary key(a,b))")
			mustExec(t, ctx, conn, "insert into quotient_target select result/100, result%100 from quotient_source")
			var count int
			require.NoError(t, conn.QueryRowContext(ctx, "select count(*) from quotient_target where (a=2 or a=3) and b=50").Scan(&count))
			require.Equal(t, 2, count)
		})

		t.Run("prepared_approximate_division", func(t *testing.T) {
			mustExec(t, ctx, conn, "delete from dst")
			stmt, err := conn.PrepareContext(ctx, "insert into dst values (? / 2)")
			require.NoError(t, err)
			defer stmt.Close()
			_, err = stmt.ExecContext(ctx, float64(5))
			require.NoError(t, err)
			var got int64
			require.NoError(t, conn.QueryRowContext(ctx, "select v from dst").Scan(&got))
			require.Equal(t, int64(2), got)
		})

		mustExec(t, ctx, conn, "create table ignore_unsigned (id int primary key, v bigint unsigned)")
		for _, protocol := range []string{"sql_prepare", "com_stmt"} {
			t.Run(protocol+"_ignore_latest", func(t *testing.T) {
				rt := moruntime.ServiceRuntime(cn.GetServiceConfig().CN.UUID)
				old, exists := rt.GetGlobalVariables(moruntime.MOProtocolVersion)
				require.True(t, exists)
				rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCLatestVersion)
				defer rt.SetGlobalVariables(moruntime.MOProtocolVersion, old)
				mustExec(t, ctx, conn, "delete from ignore_unsigned")
				if protocol == "sql_prepare" {
					mustExec(t, ctx, conn, "prepare ignore_p from 'insert ignore into ignore_unsigned values (?, ?)'")
					defer func() { _, _ = conn.ExecContext(ctx, "deallocate prepare ignore_p") }()
					mustExec(t, ctx, conn, "set @ignore_id=1, @ignore_v=cast(-1 as double)")
					mustExec(t, ctx, conn, "execute ignore_p using @ignore_id, @ignore_v")
					mustExec(t, ctx, conn, "set @ignore_id=2, @ignore_v=cast(3.5 as double)")
					mustExec(t, ctx, conn, "execute ignore_p using @ignore_id, @ignore_v")
				} else {
					stmt, err := conn.PrepareContext(ctx, "insert ignore into ignore_unsigned values (?, ?)")
					require.NoError(t, err)
					defer stmt.Close()
					_, err = stmt.ExecContext(ctx, 1, float64(-1))
					require.NoError(t, err)
					_, err = stmt.ExecContext(ctx, 2, float64(3.5))
					require.NoError(t, err)
				}
				var got uint64
				require.NoError(t, conn.QueryRowContext(ctx, "select v from ignore_unsigned where id=1").Scan(&got))
				require.Zero(t, got)
				require.NoError(t, conn.QueryRowContext(ctx, "select v from ignore_unsigned where id=2").Scan(&got))
				require.Equal(t, uint64(4), got)
			})
		}

		mustExec(t, ctx, conn, "create table t_abs (value bigint)")
		mustExec(t, ctx, conn, "create table t_add (value bigint)")
		for _, tc := range []struct {
			query string
			value int64
		}{
			{query: "insert into t_abs values (abs(?))", value: 2},
			{query: "insert into t_add values (? + 0)", value: -2},
		} {
			stmt, prepareErr := conn.PrepareContext(ctx, tc.query)
			require.NoError(t, prepareErr)
			defer stmt.Close()
			_, execErr := stmt.ExecContext(ctx, float64(-2.5))
			require.NoError(t, execErr)
		}
		var value int64
		require.NoError(t, conn.QueryRowContext(ctx, "select value from t_abs").Scan(&value))
		require.Equal(t, int64(2), value)
		require.NoError(t, conn.QueryRowContext(ctx, "select value from t_add").Scan(&value))
		require.Equal(t, int64(-2), value)

		mustExec(t, ctx, conn, "set sql_mode='STRICT_TRANS_TABLES'")
		mustExec(t, ctx, conn, "create table u (value bigint unsigned)")
		strictStmt, err := conn.PrepareContext(ctx, "insert into u values (?)")
		require.NoError(t, err)
		defer strictStmt.Close()
		_, err = strictStmt.ExecContext(ctx, float64(-1))
		require.Error(t, err)
		var count int64
		require.NoError(t, conn.QueryRowContext(ctx, "select count(*) from u").Scan(&count))
		require.Zero(t, count)
	})
}

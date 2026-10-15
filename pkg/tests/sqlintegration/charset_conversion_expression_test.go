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

func TestCharsetConversionPreservesExpressionSemantics(t *testing.T) {
	runSQLIntegration(t, func(cluster embed.Cluster) {
		ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
		defer cancel()
		cn, err := cluster.GetCNService(0)
		require.NoError(t, err)
		db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", cn.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		defer db.Close()
		conn, err := db.Conn(ctx)
		require.NoError(t, err)
		defer conn.Close()
		defer cleanupSQLIntegration(t, cn, "drop database if exists conversion_expressions")
		exec := func(statement string) {
			t.Helper()
			_, err := conn.ExecContext(ctx, statement)
			require.NoError(t, err, statement)
		}
		exec("create database conversion_expressions")
		exec("use conversion_expressions")
		for _, tc := range []struct {
			name, alter, collation string
			precision              int
			expression, input      string
		}{
			{"unchanged_session", "convert to character set utf8mb4 collate utf8mb4_bin", "utf8mb4_bin", 10, "a/b", "1"},
			{"ordinary_copy", "add column extra int, algorithm=copy", "utf8mb4_bin", 0, "a/b", "1"},
			{"same_domain", "convert to character set utf8mb4 collate utf8mb4_bin", "utf8mb4_bin", 0, "a/b", "1"},
			{"changed_domain", "convert to character set utf8mb4 collate utf8mb4_general_ci", "utf8mb4_general_ci", 0, "a/b", "1"},
			{"dependent_same_domain", "convert to character set utf8mb4 collate utf8mb4_bin", "utf8mb4_bin", 0, "concat(v,a/b)", "1"},
			{"dependent_changed_domain", "convert to character set utf8mb4 collate utf8mb4_general_ci", "utf8mb4_general_ci", 0, "concat(v,a/b)", "1"},
			{"dependent_authored_cast", "convert to character set utf8mb4 collate utf8mb4_general_ci", "utf8mb4_general_ci", 0, "concat(v,cast(a/b as char(32)))", "1"},
			{"dependent_numeric_result", "convert to character set utf8mb4 collate utf8mb4_general_ci", "utf8mb4_general_ci", 0, "cast(v as decimal(10,2))/b", "1"},
			{"authored_collate", "convert to character set utf8mb4 collate utf8mb4_general_ci", "utf8mb4_general_ci", 0, "concat(v collate utf8mb4_bin='a',a/b)", "A"},
		} {
			t.Run(tc.name, func(t *testing.T) {
				exec("set div_precision_increment=10")
				exec("create table " + tc.name + "(v varchar(8), a decimal(10,2), b decimal(10,2), g varchar(64) generated always as(" + tc.expression + ") stored) collate utf8mb4_bin")
				exec(fmt.Sprintf("insert into %s(v,a,b) values('%s',1,3)", tc.name, tc.input))
				read := func() string {
					t.Helper()
					var value string
					require.NoError(t, conn.QueryRowContext(ctx, "select g from "+tc.name).Scan(&value))
					return value
				}
				before := read()
				expected := "0.333333333333"
				if tc.expression == "concat(v,a/b)" || tc.expression == "concat(v,cast(a/b as char(32)))" {
					expected = "1" + expected
				} else if tc.name == "authored_collate" {
					expected = "0" + expected
				}
				require.Equal(t, expected, before)
				exec(fmt.Sprintf("set div_precision_increment=%d", tc.precision))
				exec("alter table " + tc.name + " " + tc.alter)
				require.Equal(t, before, read(), "DDL must not re-author unaffected arithmetic")
				var collation string
				require.NoError(t, conn.QueryRowContext(ctx, "select collation_name from information_schema.columns where table_schema='conversion_expressions' and table_name=? and column_name='g'", tc.name).Scan(&collation))
				require.Equal(t, tc.collation, collation)
				exec(fmt.Sprintf("insert into %s(v,a,b) values('%s',1,3)", tc.name, tc.input))
				var mismatches int
				require.NoError(t, conn.QueryRowContext(ctx, "select count(*) from "+tc.name+" where g<>?", before).Scan(&mismatches))
				require.Zero(t, mismatches, "future writes must retain the persisted expression as well")
				var rows int
				require.NoError(t, conn.QueryRowContext(ctx, "select count(*) from "+tc.name).Scan(&rows))
				require.Equal(t, 2, rows, "the future-write oracle must observe both inserted rows")
			})
		}
	})
}

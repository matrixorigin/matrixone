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
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/tests/testutils"
	"github.com/stretchr/testify/require"
)

func TestIssue30018RuntimeFilterDisableHint(t *testing.T) {
	embed.RunBaseClusterTests(t, func(cluster embed.Cluster) {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cancel()
		cn, err := cluster.GetCNService(0)
		require.NoError(t, err)
		rt := moruntime.ServiceRuntime(cn.ServiceID())
		version, ok := rt.GetGlobalVariables(moruntime.MOProtocolVersion)
		require.True(t, ok)
		protocol, ok := version.(int64)
		require.True(t, ok)
		require.GreaterOrEqual(t, protocol, defines.MORPCVersion43, "scalar enabled control requires admitted protocol")
		oldHints, hadHints := rt.GetGlobalVariables("optimizer_hints")
		defer func() {
			if hadHints {
				rt.SetGlobalVariables("optimizer_hints", oldHints)
			} else if current, exists := rt.GetGlobalVariables("optimizer_hints"); exists {
				rt.CompareAndDeleteGlobalVariables("optimizer_hints", current)
			}
		}()
		db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", cn.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		defer func() { require.NoError(t, db.Close()) }()
		conn, err := db.Conn(ctx)
		require.NoError(t, err)
		defer func() { require.NoError(t, conn.Close()) }()
		exec := func(statement string) {
			t.Helper()
			_, err := conn.ExecContext(ctx, statement)
			require.NoError(t, err, statement)
		}
		name := strings.ToLower(testutils.GetDatabaseName(t))
		exec("set session optimizer_hints=''")
		exec("create database `" + name + "`")
		defer func() {
			cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cleanupCancel()
			_, err := conn.ExecContext(cleanupCtx, "set session optimizer_hints=''")
			require.NoError(t, err)
			_, err = conn.ExecContext(cleanupCtx, "drop database `"+name+"`")
			require.NoError(t, err)
		}()
		exec("use `" + name + "`")
		exec("create table rf_probe(k1 bigint not null,k2 bigint not null,payload bigint,primary key(k1,k2))")
		exec("create table rf_build(k1 bigint not null,k2 bigint not null,payload bigint,primary key(k1,k2))")
		exec("insert into rf_probe values (1,1,10),(1,2,20),(2,1,30),(2,2,40),(3,1,50),(3,2,60),(4,1,70),(4,2,80)")
		exec("insert into rf_build values (1,1,100),(2,2,200)")
		exec("create table rf_scalar(k bigint)")
		exec("insert into rf_scalar values (1)")

		compositeSQL := "select p.payload from rf_probe p join rf_build b on p.k1=b.k1 and p.k2=b.k2 order by p.k1,p.k2"
		scalarSQL := "select p.payload from rf_probe p where p.k1=(select k from rf_scalar) order by p.k1,p.k2"
		for _, tc := range []struct {
			query   string
			enabled string
			needle  string
			result  []int64
		}{
			{query: compositeSQL, enabled: "joinOrdering=1", needle: "serial(", result: []int64{10, 40}},
			{query: scalarSQL, needle: "Join Type: SINGLE", result: []int64{10, 20}},
		} {
			// Replan on the same session after each hint change; enabled absence
			// would make this a vacuous on/off comparison and must fail.
			for _, disabled := range []bool{false, true, false} {
				hint := tc.enabled
				if disabled {
					if hint != "" {
						hint += ","
					}
					hint += "runtimeFilter=1"
				}
				exec("set session optimizer_hints='" + hint + "'")
				plan := issue30018Explain(t, ctx, conn, tc.query)
				if disabled {
					require.NotContains(t, plan, "Runtime Filter Build:", plan)
					require.NotContains(t, plan, "Runtime Filter Probe:", plan)
				} else {
					require.Contains(t, plan, "Runtime Filter Build:", plan)
					require.Contains(t, plan, "Runtime Filter Probe:", plan)
					require.Contains(t, plan, tc.needle, plan)
				}
				if tc.query == scalarSQL {
					require.Contains(t, plan, "Join Type: SINGLE", plan)
					require.Contains(t, plan, "Filter Cond:", plan)
				} else {
					require.Contains(t, plan, "Join Cond:", plan)
				}
				values, err := issue30018Values(ctx, conn, tc.query)
				require.NoError(t, err)
				require.Equal(t, tc.result, values)
			}
		}

		exec("delete from rf_build")
		for _, hint := range []string{"joinOrdering=1", "joinOrdering=1,runtimeFilter=1"} {
			exec("set session optimizer_hints='" + hint + "'")
			values, err := issue30018Values(ctx, conn, compositeSQL)
			require.NoError(t, err)
			require.Empty(t, values)
		}
		for _, scalarInput := range []string{"", "(null)", "(1),(2)"} {
			exec("delete from rf_scalar")
			if scalarInput != "" {
				exec("insert into rf_scalar values " + scalarInput)
			}
			var enabledError *mysql.MySQLError
			for _, hint := range []string{"", "runtimeFilter=1"} {
				exec("set session optimizer_hints='" + hint + "'")
				values, err := issue30018Values(ctx, conn, scalarSQL)
				if scalarInput != "(1),(2)" {
					require.NoError(t, err)
					require.Empty(t, values)
					continue
				}
				var sqlError *mysql.MySQLError
				require.Error(t, err)
				require.True(t, errors.As(err, &sqlError))
				require.Contains(t, sqlError.Message, "Subquery returns more than 1 row")
				if enabledError == nil {
					enabledError = sqlError
				} else {
					require.Equal(t, enabledError.Number, sqlError.Number)
					require.Equal(t, enabledError.SQLState, sqlError.SQLState)
				}
			}
		}
	})
}

func issue30018Explain(t *testing.T, ctx context.Context, conn *sql.Conn, query string) string {
	t.Helper()
	rows, err := conn.QueryContext(ctx, "explain "+query)
	require.NoError(t, err)
	defer func() { require.NoError(t, rows.Close()) }()
	var lines []string
	for rows.Next() {
		var line string
		require.NoError(t, rows.Scan(&line))
		lines = append(lines, line)
	}
	require.NoError(t, rows.Err())
	return strings.Join(lines, "\n")
}

func issue30018Values(ctx context.Context, conn *sql.Conn, query string) (values []int64, err error) {
	rows, err := conn.QueryContext(ctx, query)
	if err != nil {
		return nil, err
	}
	defer func() {
		if closeErr := rows.Close(); err == nil {
			err = closeErr
		}
	}()
	for rows.Next() {
		var value int64
		if err := rows.Scan(&value); err != nil {
			return nil, err
		}
		values = append(values, value)
	}
	return values, rows.Err()
}

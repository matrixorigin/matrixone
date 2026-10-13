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

package issues

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/tests/testutils"
	"github.com/stretchr/testify/require"
)

func TestIssue28243SetvalRejectsNarrowIntegerOverflow(t *testing.T) {
	embed.RunBaseClusterTests(t, func(c embed.Cluster) {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
		defer cancel()

		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", cn.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		defer db.Close()
		db.SetMaxOpenConns(1)

		conn, err := db.Conn(ctx)
		require.NoError(t, err)
		defer conn.Close()

		exec := func(tb testing.TB, statement string) {
			tb.Helper()
			_, err := conn.ExecContext(ctx, statement)
			require.NoErrorf(tb, err, "exec failed: %s", statement)
		}
		queryInt64 := func(tb testing.TB, statement string) int64 {
			tb.Helper()
			var value int64
			require.NoErrorf(tb, conn.QueryRowContext(ctx, statement).Scan(&value), "query failed: %s", statement)
			return value
		}
		queryState := func(tb testing.TB, statement string) (int64, bool) {
			tb.Helper()
			var last int64
			var called bool
			require.NoErrorf(tb, conn.QueryRowContext(ctx, statement).Scan(&last, &called), "query failed: %s", statement)
			return last, called
		}
		queryString := func(tb testing.TB, statement string) string {
			tb.Helper()
			var value string
			require.NoErrorf(tb, conn.QueryRowContext(ctx, statement).Scan(&value), "query failed: %s", statement)
			return value
		}

		dbName := testutils.GetDatabaseName(t)
		exec(t, fmt.Sprintf("create database `%s`", dbName))
		defer func() {
			cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cleanupCancel()
			_, err := conn.ExecContext(cleanupCtx, fmt.Sprintf("drop database if exists `%s`", dbName))
			require.NoError(t, err)
		}()
		exec(t, fmt.Sprintf("use `%s`", dbName))

		type validSetvalValue struct {
			value    string
			expected int64
		}
		type invalidSetvalValue struct {
			value     string
			errorText string
		}
		tests := []struct {
			name        string
			typ         string
			minValue    string
			maxValue    string
			startValue  string
			validValue  string
			validValues []validSetvalValue
			invalid     []invalidSetvalValue
		}{
			{
				name:        "signed_smallint_configured_range",
				typ:         "smallint",
				minValue:    "1",
				maxValue:    "100",
				startValue:  "10",
				validValue:  "20",
				validValues: []validSetvalValue{{"1", 1}, {"100", 100}},
				invalid:     []invalidSetvalValue{{"65537", "Set value is not in range"}, {"-65535", "Set value is not in range"}},
			},
			{
				name:        "unsigned_smallint_type_range",
				typ:         "smallint unsigned",
				minValue:    "0",
				maxValue:    "65535",
				startValue:  "10",
				validValue:  "20",
				validValues: []validSetvalValue{{"0", 0}, {"65535", 65535}},
				invalid:     []invalidSetvalValue{{"65536", "Set value is not in range"}, {"65537", "Set value is not in range"}, {"-1", "invalid syntax"}},
			},
			{
				name:        "signed_int_configured_range",
				typ:         "int",
				minValue:    "1",
				maxValue:    "100",
				startValue:  "10",
				validValue:  "20",
				validValues: []validSetvalValue{{"1", 1}, {"100", 100}},
				invalid:     []invalidSetvalValue{{"4294967297", "Set value is not in range"}, {"-4294967295", "Set value is not in range"}},
			},
			{
				name:        "unsigned_int_type_range",
				typ:         "int unsigned",
				minValue:    "0",
				maxValue:    "4294967295",
				startValue:  "10",
				validValue:  "20",
				validValues: []validSetvalValue{{"0", 0}, {"4294967295", 4294967295}},
				invalid:     []invalidSetvalValue{{"4294967296", "Set value is not in range"}, {"4294967297", "Set value is not in range"}, {"-1", "invalid syntax"}},
			},
			{
				name:        "signed_smallint_type_range",
				typ:         "smallint",
				minValue:    "-32768",
				maxValue:    "32767",
				startValue:  "10",
				validValue:  "20",
				validValues: []validSetvalValue{{"-32768", -32768}, {"32767", 32767}},
				invalid:     []invalidSetvalValue{{"32768", "Set value is not in range"}, {"-32769", "Set value is not in range"}},
			},
		}

		for _, tc := range tests {
			t.Run(tc.name, func(t *testing.T) {
				sequence := "s_" + tc.name
				exec(t, fmt.Sprintf(
					"create sequence `%s`.`%s` as %s minvalue %s maxvalue %s start with %s no cycle",
					dbName, sequence, tc.typ, tc.minValue, tc.maxValue, tc.startValue))
				defer func() {
					cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
					defer cleanupCancel()
					_, err := conn.ExecContext(cleanupCtx, fmt.Sprintf(
						"drop sequence if exists `%s`.`%s`", dbName, sequence))
					require.NoError(t, err)
				}()

				require.Equal(t, int64(10), queryInt64(t, fmt.Sprintf("select nextval('%s')", sequence)))
				last, called := queryState(t, fmt.Sprintf(
					"select last_seq_num, is_called from `%s`.`%s`", dbName, sequence))
				require.Equal(t, int64(10), last)
				require.True(t, called)
				require.Equal(t, "10", queryString(t, "select lastval()"))

				for _, invalid := range tc.invalid {
					for _, isCalled := range []string{"true", "false"} {
						statement := fmt.Sprintf("select setval('%s', %s, %s)", sequence, invalid.value, isCalled)
						_, err := conn.ExecContext(ctx, statement)
						require.Error(t, err, "out-of-range SETVAL must fail: %s", statement)
						if invalid.errorText != "" {
							require.ErrorContains(t, err, invalid.errorText, "unexpected SETVAL error: %s", statement)
						}
						last, called = queryState(t, fmt.Sprintf(
							"select last_seq_num, is_called from `%s`.`%s`", dbName, sequence))
						require.Equal(t, int64(10), last)
						require.True(t, called)
						require.Equal(t, int64(10), queryInt64(t, fmt.Sprintf("select currval('%s')", sequence)))
						require.Equal(t, "10", queryString(t, "select lastval()"))
					}
				}

				for _, valid := range tc.validValues {
					var result string
					require.NoError(t, conn.QueryRowContext(ctx,
						fmt.Sprintf("select setval('%s', %s, true)", sequence, valid.value)).Scan(&result))
					require.Equal(t, valid.value, result)
					last, called = queryState(t, fmt.Sprintf(
						"select last_seq_num, is_called from `%s`.`%s`", dbName, sequence))
					require.Equal(t, valid.expected, last)
					require.True(t, called)
					require.Equal(t, valid.value, queryString(t, fmt.Sprintf("select currval('%s')", sequence)))
					require.Equal(t, valid.value, queryString(t, "select lastval()"))
				}

				var result string
				require.NoError(t, conn.QueryRowContext(ctx,
					fmt.Sprintf("select setval('%s', %s, true)", sequence, tc.validValue)).Scan(&result))
				require.Equal(t, tc.validValue, result)
				require.Equal(t, int64(20), queryInt64(t, fmt.Sprintf(
					"select last_seq_num from `%s`.`%s`", dbName, sequence)))
				require.Equal(t, int64(20), queryInt64(t, fmt.Sprintf("select currval('%s')", sequence)))
				require.Equal(t, int64(21), queryInt64(t, fmt.Sprintf("select nextval('%s')", sequence)))
			})
		}
	})
}

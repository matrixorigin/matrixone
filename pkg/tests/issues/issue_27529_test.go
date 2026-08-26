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
	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/embed"
)

func TestIssue27529JSONStringsDoNotCompareAsBooleans(t *testing.T) {
	embed.RunBaseClusterTests(t, func(c embed.Cluster) {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
		defer cancel()

		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		port := cn.GetServiceConfig().CN.Frontend.Port
		db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", port))
		require.NoError(t, err)
		defer db.Close()

		const dbName = "issue_27529_json_string_bool"
		execSQLMaybe(t, ctx, db, "drop database if exists "+dbName)
		defer func() {
			cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cleanupCancel()
			execSQLMaybe(t, cleanupCtx, db, "drop database if exists "+dbName)
		}()

		execSQLRequire(t, ctx, db, "create database "+dbName)
		execSQLRequire(t, ctx, db, "create table "+dbName+`.docs(id int primary key, meta json)`)
		execSQLRequire(t, ctx, db, "insert into "+dbName+`.docs values
			(1, '{\"active\":true}'),
			(2, '{\"active\":false}'),
			(3, '{\"active\":\"true\"}'),
			(4, '{\"active\":\"false\"}'),
			(5, '{\"active\":null}'),
			(6, '{}')`)

		rows, err := db.QueryContext(ctx, "select id, json_extract(meta, '$.active') = true, "+
			"json_extract(meta, '$.active') = false from "+dbName+".docs order by id")
		require.NoError(t, err)
		defer rows.Close()
		expectedComparisons := []struct {
			id         int
			equalTrue  sql.NullBool
			equalFalse sql.NullBool
		}{
			{id: 1, equalTrue: sql.NullBool{Bool: true, Valid: true}, equalFalse: sql.NullBool{Valid: true}},
			{id: 2, equalTrue: sql.NullBool{Valid: true}, equalFalse: sql.NullBool{Bool: true, Valid: true}},
			{id: 3},
			{id: 4},
			{id: 5},
			{id: 6},
		}
		for _, expected := range expectedComparisons {
			require.True(t, rows.Next())
			var id int
			var equalTrue, equalFalse sql.NullBool
			require.NoError(t, rows.Scan(&id, &equalTrue, &equalFalse))
			require.Equal(t, expected.id, id)
			require.Equal(t, expected.equalTrue, equalTrue)
			require.Equal(t, expected.equalFalse, equalFalse)
		}
		require.False(t, rows.Next())
		require.NoError(t, rows.Err())

		assertIDs := func(query string, expected ...int) {
			t.Helper()
			rows, queryErr := db.QueryContext(ctx, query)
			require.NoError(t, queryErr)
			defer rows.Close()
			var actual []int
			for rows.Next() {
				var id int
				require.NoError(t, rows.Scan(&id))
				actual = append(actual, id)
			}
			require.NoError(t, rows.Err())
			require.Equal(t, expected, actual)
		}

		assertIDs("select id from "+dbName+`.docs where json_extract(meta, '$.active') = true order by id`, 1)
		assertIDs("select id from "+dbName+`.docs where json_extract(meta, '$.active') = false order by id`, 2)

		queryPreparedBool := func(query string, arg any) (bool, bool) {
			t.Helper()
			stmt, prepareErr := db.PrepareContext(ctx, query)
			require.NoError(t, prepareErr)
			defer stmt.Close()
			var got sql.NullBool
			require.NoError(t, stmt.QueryRowContext(ctx, arg).Scan(&got))
			return got.Bool, got.Valid
		}
		for _, tc := range []struct {
			name       string
			query      string
			arg        any
			expected   bool
			expectNull bool
		}{
			{name: "null-safe equality JSON left boolean", query: `select json_extract(json_object('v', true), '$.v') <=> ?`, arg: true, expected: true},
			{name: "equality preserves JSON number to boolean coercion", query: `select json_extract(json_array(1), '$[0]') = ?`, arg: true, expected: true},
			{name: "null-safe equality preserves JSON number to boolean coercion", query: `select ? <=> json_extract(json_array(1), '$[0]')`, arg: true, expected: true},
			{name: "IN preserves JSON number to boolean coercion", query: `select json_extract(json_array(1), '$[0]') in (?)`, arg: true, expected: true},
			{name: "NOT IN preserves JSON number to boolean coercion", query: `select json_extract(json_array(1), '$[0]') not in (?)`, arg: true, expected: false},
			{name: "equality preserves adjacent int64 precision", query: `select json_extract(json_array(9007199254740992), '$[0]') = ?`, arg: int64(9007199254740993), expected: false},
			{name: "reversed equality preserves adjacent int64 precision", query: `select ? = json_extract(json_array(9007199254740992), '$[0]')`, arg: int64(9007199254740993), expected: false},
			{name: "null-safe equality preserves adjacent int64 precision", query: `select json_extract(json_array(9007199254740992), '$[0]') <=> ?`, arg: int64(9007199254740993), expected: false},
			{name: "IN preserves adjacent int64 precision", query: `select json_extract(json_array(9007199254740992), '$[0]') in (?)`, arg: int64(9007199254740993), expected: false},
			{name: "NOT IN preserves adjacent int64 precision", query: `select json_extract(json_array(9007199254740992), '$[0]') not in (?)`, arg: int64(9007199254740993), expected: true},
			{name: "equality preserves max int64", query: `select json_extract(json_array(9223372036854775807), '$[0]') = ?`, arg: int64(9223372036854775807), expected: true},
			{name: "integer parameter keeps numeric JSON string coercion", query: `select json_extract(json_array('7'), '$[0]') = ?`, arg: int64(7), expected: true},
			{name: "float parameter preserves typed literal parity", query: `select json_extract(json_array(1.25), '$[0]') = ?`, arg: float64(1.25), expected: true},
			{name: "null-safe equality JSON right boolean", query: `select ? <=> json_extract(json_object('v', true), '$.v')`, arg: true, expected: true},
			{name: "null-safe equality boolean coerces to string parameter", query: `select json_extract(json_object('v', true), '$.v') <=> ?`, arg: "true", expected: true},
			{name: "null-safe equality string matches string parameter", query: `select ? <=> json_extract(json_object('v', 'true'), '$.v')`, arg: "true", expected: true},
			{name: "null-safe equality distinguishes JSON string", query: `select json_extract(json_object('v', 'true'), '$.v') <=> ?`, arg: true, expected: false},
			{name: "null-safe equality missing JSON and SQL NULL", query: `select json_extract(json_object('v', true), '$.missing') <=> ?`, arg: nil, expected: true},
			{name: "null-safe equality SQL NULL left and missing JSON", query: `select ? <=> json_extract(json_object('v', true), '$.missing')`, arg: nil, expected: true},
			{name: "IN matches JSON boolean", query: `select json_extract(json_object('v', true), '$.v') in (?)`, arg: true, expected: true},
			{name: "IN matches JSON string", query: `select json_extract(json_object('v', 'true'), '$.v') in (?)`, arg: "true", expected: true},
			{name: "IN keeps JSON string distinct", query: `select json_extract(json_object('v', 'true'), '$.v') in (?)`, arg: true, expectNull: true},
			{name: "NOT IN rejects matching JSON boolean", query: `select json_extract(json_object('v', true), '$.v') not in (?)`, arg: true, expected: false},
			{name: "NOT IN rejects matching JSON string", query: `select json_extract(json_object('v', 'true'), '$.v') not in (?)`, arg: "true", expected: false},
			{name: "NOT IN keeps JSON string distinct", query: `select json_extract(json_object('v', 'true'), '$.v') not in (?)`, arg: true, expectNull: true},
		} {
			t.Run(tc.name, func(t *testing.T) {
				actual, valid := queryPreparedBool(tc.query, tc.arg)
				if tc.expectNull {
					require.False(t, valid)
				} else {
					require.True(t, valid)
					require.Equal(t, tc.expected, actual)
				}
			})
		}

		stmt, err := db.PrepareContext(ctx, `select
			json_extract(json_array(9007199254740992), '$[0]') = cast(9007199254740993 as signed),
			json_extract(json_array(9007199254740992), '$[0]') = ?`)
		require.NoError(t, err)
		var direct, prepared bool
		require.NoError(t, stmt.QueryRowContext(ctx, int64(9007199254740993)).Scan(&direct, &prepared))
		require.Equal(t, direct, prepared)
		require.False(t, prepared)
		require.NoError(t, stmt.Close())

		stmt, err = db.PrepareContext(ctx,
			`select json_extract('18446744073709551615', '$') = ?`)
		require.NoError(t, err)
		require.Error(t, db.QueryRowContext(ctx,
			`select json_extract('18446744073709551615', '$') = cast(9223372036854775807 as signed)`).Scan(new(bool)))
		require.Error(t, stmt.QueryRowContext(ctx, int64(9223372036854775807)).Scan(new(bool)),
			"prepared BIGINT must retain the direct JSON-to-BIGINT overflow error")
		require.NoError(t, stmt.Close())

		for _, query := range []string{
			`select json_extract(json_object('v', json_object('nested', true)), '$.v') = ?`,
			`select json_extract(json_object('v', json_array(true)), '$.v') = ?`,
		} {
			stmt, err = db.PrepareContext(ctx, query)
			require.NoError(t, err)
			require.Error(t, stmt.QueryRowContext(ctx, true).Scan(new(bool)))
			require.NoError(t, stmt.Close())
		}
		var health int
		require.NoError(t, db.QueryRowContext(ctx, "select 1").Scan(&health))
		require.Equal(t, 1, health)

		execSQLRequire(t, ctx, db, `prepare issue_27529_p from "select id from `+dbName+`.docs where json_extract(meta, '$.active') = ? order by id"`)
		defer execSQLMaybe(t, context.Background(), db, "deallocate prepare issue_27529_p")
		execSQLRequire(t, ctx, db, "set @issue_27529_b = true")
		assertIDs("execute issue_27529_p using @issue_27529_b", 1)
		execSQLRequire(t, ctx, db, "set @issue_27529_b = false")
		assertIDs("execute issue_27529_p using @issue_27529_b", 2)

		var directRounded bool
		require.NoError(t, db.QueryRowContext(ctx,
			`select json_extract('16777217', '$') = cast(16777216 as float)`).Scan(&directRounded))
		require.True(t, directRounded)

		execSQLRequire(t, ctx, db,
			`prepare issue_27529_float from "select json_extract('16777217', '$') = ?"`)
		defer execSQLMaybe(t, context.Background(), db, "deallocate prepare issue_27529_float")
		execSQLRequire(t, ctx, db, "set @issue_27529_float = cast(16777216 as float)")
		var rounded bool
		require.NoError(t,
			db.QueryRowContext(ctx, "execute issue_27529_float using @issue_27529_float").Scan(&rounded))
		require.True(t, rounded)

		execSQLRequire(t, ctx, db,
			`prepare issue_27529_signed from "select json_extract('18446744073709551615', '$') = ?"`)
		defer execSQLMaybe(t, context.Background(), db, "deallocate prepare issue_27529_signed")
		execSQLRequire(t, ctx, db,
			"set @issue_27529_signed = cast(9223372036854775807 as signed)")
		_, err = db.ExecContext(ctx, "execute issue_27529_signed using @issue_27529_signed")
		require.Error(t, err)
	})
}

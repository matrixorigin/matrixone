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

package embed

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestJSONLineOutfileLoadPreservesTextAndRejectsInvalidUTF8(t *testing.T) {
	RunSingleCNBaseClusterTests(t, func(c Cluster) {
		ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
		defer cancel()
		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", cn.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		defer db.Close()
		conn, err := db.Conn(ctx)
		require.NoError(t, err)
		defer conn.Close()
		exec := func(stmt string) {
			t.Helper()
			_, err := conn.ExecContext(ctx, stmt)
			require.NoError(t, err, stmt)
		}
		query := func(stmt string) *sql.Rows {
			t.Helper()
			rows, err := conn.QueryContext(ctx, stmt)
			require.NoError(t, err, stmt)
			return rows
		}

		const database = "issue_30704_30705_text_fidelity"
		exec("create database " + database)
		defer func() { _, _ = conn.ExecContext(ctx, "drop database if exists "+database) }()
		exec("create table " + database + ".src (id int primary key, s varchar(32))")
		exec("insert into " + database + ".src values (1, cast(x'5C4E' as char)), (2, null), (3, 'plain'), (4, ''), (5, cast(x'5C5C4E' as char)), (6, '中文'), (7, cast(x'EFBFBD' as char))")

		type textRow struct {
			ID     int
			IsNull bool
			Hex    string
		}
		readRows := func(table string) []textRow {
			t.Helper()
			rows := query("select id, s is null, coalesce(hex(s), '#NULL') from " + database + "." + table + " order by id")
			defer rows.Close()
			var got []textRow
			for rows.Next() {
				var row textRow
				require.NoError(t, rows.Scan(&row.ID, &row.IsNull, &row.Hex))
				got = append(got, row)
			}
			require.NoError(t, rows.Err())
			return got
		}
		wantRows := []textRow{
			{ID: 1, Hex: "5C4E"},
			{ID: 2, IsNull: true, Hex: "#NULL"},
			{ID: 3, Hex: "706C61696E"},
			{ID: 4, Hex: ""},
			{ID: 5, Hex: "5C5C4E"},
			{ID: 6, Hex: "E4B8ADE69687"},
			{ID: 7, Hex: "EFBFBD"},
		}
		require.Equal(t, wantRows, readRows("src"))

		dir := t.TempDir()
		jsonPath := filepath.Join(dir, "roundtrip.jsonl")
		exec(fmt.Sprintf("select id, s from %s.src order by id into outfile '%s' format 'jsonline'", database, jsonPath))
		jsonBytes, err := os.ReadFile(jsonPath)
		require.NoError(t, err)
		t.Logf("JSONLINE OUTFILE bytes=%x text=%q", jsonBytes, string(jsonBytes))
		require.Equal(t, []byte("{\"id\":1,\"s\":\"\\\\N\"}\n{\"id\":2,\"s\":null}\n{\"id\":3,\"s\":\"plain\"}\n{\"id\":4,\"s\":\"\"}\n{\"id\":5,\"s\":\"\\\\\\\\N\"}\n{\"id\":6,\"s\":\"中文\"}\n{\"id\":7,\"s\":\"�\"}\n"), jsonBytes)
		exec("create table " + database + ".json_dst (id int, s varchar(32))")
		exec(fmt.Sprintf("load data infile {'filepath'='%s','format'='jsonline','jsondata'='object'} into table %s.json_dst", jsonPath, database))
		require.Equal(t, wantRows, readRows("json_dst"))
		var sourceCount, loadedCount int
		require.NoError(t, conn.QueryRowContext(ctx, "select count(s) from "+database+".src").Scan(&sourceCount))
		require.NoError(t, conn.QueryRowContext(ctx, "select count(s) from "+database+".json_dst").Scan(&loadedCount))
		require.Equal(t, 6, sourceCount)
		require.Equal(t, sourceCount, loadedCount)

		arrayPath := filepath.Join(dir, "array.jsonl")
		arrayBytes := []byte("[\"\\\\N\"]\n[null]\n[\"\"]\n")
		require.NoError(t, os.WriteFile(arrayPath, arrayBytes, 0o600))
		exec("create table " + database + ".array_dst (s varchar(32))")
		exec(fmt.Sprintf("load data infile {'filepath'='%s','format'='jsonline','jsondata'='array'} into table %s.array_dst", arrayPath, database))
		arrayRows := query("select s is null, coalesce(hex(s), '#NULL') from " + database + ".array_dst order by s is null desc, hex(s)")
		defer arrayRows.Close()
		var gotArray [][2]string
		for arrayRows.Next() {
			var isNull bool
			var value string
			require.NoError(t, arrayRows.Scan(&isNull, &value))
			gotArray = append(gotArray, [2]string{fmt.Sprint(isNull), value})
		}
		require.NoError(t, arrayRows.Err())
		require.NoError(t, arrayRows.Close())
		require.Equal(t, [][2]string{{"true", "#NULL"}, {"false", ""}, {"false", "5C4E"}}, gotArray)

		csvPath := filepath.Join(dir, "roundtrip.csv")
		exec(fmt.Sprintf("select id, s from %s.src order by id into outfile '%s' format 'csv' header 'false'", database, csvPath))
		csvBytes, err := os.ReadFile(csvPath)
		require.NoError(t, err)
		t.Logf("CSV OUTFILE bytes=%x text=%q", csvBytes, string(csvBytes))
		require.Equal(t, []byte("1,\"\\\\N\"\n2,\\N\n3,\"plain\"\n4,\"\"\n5,\"\\\\\\\\N\"\n6,\"中文\"\n7,\"�\"\n"), csvBytes)
		exec("create table " + database + ".csv_dst (id int, s varchar(32))")
		exec(fmt.Sprintf("load data infile '%s' into table %s.csv_dst fields terminated by ','", csvPath, database))
		require.Equal(t, wantRows, readRows("csv_dst"))

		exec("create table " + database + ".bad_utf8 (s varchar(32))")
		exec("insert into " + database + ".bad_utf8 values (cast(x'61FF62' as char))")
		var sourceHex string
		require.NoError(t, conn.QueryRowContext(ctx, "select hex(s) from "+database+".bad_utf8").Scan(&sourceHex))
		require.Equal(t, "61FF62", sourceHex)
		badPath := filepath.Join(dir, "bad-utf8.jsonl")
		failCtx, failCancel := context.WithTimeout(ctx, 15*time.Second)
		started := time.Now()
		_, err = conn.ExecContext(failCtx, fmt.Sprintf("select s from %s.bad_utf8 into outfile '%s' format 'jsonline'", database, badPath))
		failCancel()
		require.Error(t, err)
		require.Contains(t, strings.ToLower(err.Error()), "invalid utf-8")
		t.Logf("invalid UTF-8 OUTFILE error after %s: %v", time.Since(started), err)
		badBytes, readErr := os.ReadFile(badPath)
		if readErr == nil {
			t.Logf("failed invalid UTF-8 target bytes=%x text=%q", badBytes, string(badBytes))
			require.NotContains(t, string(badBytes), "�", "invalid input must not be reported as a successful replacement-character export")
		} else {
			require.True(t, errors.Is(readErr, os.ErrNotExist), "unexpected failed-target read error: %v", readErr)
		}

		var sessionCheck int
		require.NoError(t, conn.QueryRowContext(ctx, "select 1").Scan(&sessionCheck))
		require.Equal(t, 1, sessionCheck)
		afterPath := filepath.Join(dir, "after-error.jsonl")
		exec(fmt.Sprintf("select s from %s.src where id=3 into outfile '%s' format 'jsonline'", database, afterPath))
		afterBytes, err := os.ReadFile(afterPath)
		require.NoError(t, err)
		require.Equal(t, []byte("{\"s\":\"plain\"}\n"), afterBytes)
	})
}

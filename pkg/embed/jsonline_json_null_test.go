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

// TestJSONLineOutfileRejectsTopLevelJSONNullRoundTrip pins the #31277
// contract at SQL level. A JSON column can hold both a top-level JSON literal
// null and a SQL NULL, but a JSONLINE line carries both as the same bare
// null, which the loader reads back as SQL NULL. The export must therefore
// fail loudly for a top-level JSON null instead of silently changing the
// value, keep every representable value (SQL NULL, nested null, the JSON
// string "null", ordinary JSON) exactly round-trippable in both reader
// modes, and leave CSV as the lossless path for the full value space.
func TestJSONLineOutfileRejectsTopLevelJSONNullRoundTrip(t *testing.T) {
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

		const database = "issue_31277_json_null"
		exec("create database " + database)
		defer func() { _, _ = conn.ExecContext(ctx, "drop database if exists "+database) }()
		exec("create table " + database + ".src (id int primary key, j json)")
		exec("insert into " + database + ".src values " +
			"(1, 'null'), (2, null), (3, '{\"a\":null}'), (4, '\"null\"'), (5, '{\"a\":1}')")

		type jsonRow struct {
			ID     int
			IsNull bool
			Text   string
		}
		readRows := func(table string) []jsonRow {
			t.Helper()
			rows := query("select id, j is null, coalesce(cast(j as char), '#NULL') from " + database + "." + table + " order by id")
			defer rows.Close()
			var got []jsonRow
			for rows.Next() {
				var row jsonRow
				require.NoError(t, rows.Scan(&row.ID, &row.IsNull, &row.Text))
				got = append(got, row)
			}
			require.NoError(t, rows.Err())
			return got
		}
		// MO renders JSON objects MySQL-style with spaces after ':'; the
		// value identity and the IS NULL flag are what this test pins.
		wantSrc := []jsonRow{
			{ID: 1, IsNull: false, Text: "null"},
			{ID: 2, IsNull: true, Text: "#NULL"},
			{ID: 3, IsNull: false, Text: `{"a": null}`},
			{ID: 4, IsNull: false, Text: `"null"`},
			{ID: 5, IsNull: false, Text: `{"a": 1}`},
		}
		require.Equal(t, wantSrc, readRows("src"))

		dir := t.TempDir()

		// Exporting the whole table must fail on the top-level JSON null
		// instead of writing a line that loads back as SQL NULL.
		rejectPath := filepath.Join(dir, "reject.jsonl")
		failCtx, failCancel := context.WithTimeout(ctx, 15*time.Second)
		started := time.Now()
		_, err = conn.ExecContext(failCtx, fmt.Sprintf(
			"select id, j from %s.src order by id into outfile '%s' format 'jsonline'", database, rejectPath))
		failCancel()
		require.Error(t, err)
		require.Contains(t, strings.ToLower(err.Error()), "jsonline")
		require.Contains(t, strings.ToLower(err.Error()), "json null")
		t.Logf("top-level JSON null OUTFILE error after %s: %v", time.Since(started), err)
		if rejectBytes, readErr := os.ReadFile(rejectPath); readErr == nil {
			t.Logf("failed top-level JSON null target bytes=%x text=%q", rejectBytes, string(rejectBytes))
			require.NotContains(t, string(rejectBytes), `"id":1`,
				"the ambiguous row must not be published by a failed export")
		} else {
			require.True(t, errors.Is(readErr, os.ErrNotExist), "unexpected failed-target read error: %v", readErr)
		}

		// The session stays usable after the rejected export.
		var sessionCheck int
		require.NoError(t, conn.QueryRowContext(ctx, "select 1").Scan(&sessionCheck))
		require.Equal(t, 1, sessionCheck)

		// Every representable value exports with its exact encoding.
		jsonPath := filepath.Join(dir, "roundtrip.jsonl")
		exec(fmt.Sprintf(
			"select id, j from %s.src where id <> 1 order by id into outfile '%s' format 'jsonline'", database, jsonPath))
		jsonBytes, err := os.ReadFile(jsonPath)
		require.NoError(t, err)
		t.Logf("JSONLINE OUTFILE bytes=%x text=%q", jsonBytes, string(jsonBytes))
		require.Equal(t, []byte("{\"id\":2,\"j\":null}\n{\"id\":3,\"j\":{\"a\":null}}\n{\"id\":4,\"j\":\"null\"}\n{\"id\":5,\"j\":{\"a\":1}}\n"), jsonBytes)

		// Object mode loads every line back with the source semantics.
		exec("create table " + database + ".object_dst (id int primary key, j json)")
		exec(fmt.Sprintf(
			"load data infile {'filepath'='%s','format'='jsonline','jsondata'='object'} into table %s.object_dst", jsonPath, database))
		require.Equal(t, wantSrc[1:], readRows("object_dst"))

		// The array-mode reader keeps the same bare-null -> SQL NULL contract.
		arrayPath := filepath.Join(dir, "array.jsonl")
		arrayBytes := []byte("[2,null]\n[3,{\"a\":null}]\n[4,\"null\"]\n[5,{\"a\":1}]\n")
		require.NoError(t, os.WriteFile(arrayPath, arrayBytes, 0o600))
		exec("create table " + database + ".array_dst (id int primary key, j json)")
		exec(fmt.Sprintf(
			"load data infile {'filepath'='%s','format'='jsonline','jsondata'='array'} into table %s.array_dst", arrayPath, database))
		require.Equal(t, wantSrc[1:], readRows("array_dst"))

		// A bare null into a NOT NULL JSON column is rejected and leaves the
		// table empty.
		exec("create table " + database + ".not_null_dst (j json not null)")
		nullOnlyPath := filepath.Join(dir, "null-only.jsonl")
		require.NoError(t, os.WriteFile(nullOnlyPath, []byte("{\"j\":null}\n"), 0o600))
		_, err = conn.ExecContext(ctx, fmt.Sprintf(
			"load data infile {'filepath'='%s','format'='jsonline','jsondata'='object'} into table %s.not_null_dst", nullOnlyPath, database))
		require.Error(t, err, "a bare null must not load into a NOT NULL JSON column")
		var notNullCount int
		require.NoError(t, conn.QueryRowContext(ctx, "select count(*) from "+database+".not_null_dst").Scan(&notNullCount))
		require.Zero(t, notNullCount, "the rejected load must not leave partial rows")

		// CSV is the documented lossless path for this issue's value space:
		// it carries the top-level JSON null and the SQL NULL as distinct
		// tokens and round-trips objects exactly. A top-level JSON *string*
		// scalar is excluded: the pre-existing CSV JSON loader trims
		// surrounding quotes from every field (external.go), so JSON string
		// scalars cannot survive CSV load; that is a separate contract
		// outside this issue and outside this diff.
		csvPath := filepath.Join(dir, "roundtrip.csv")
		exec(fmt.Sprintf(
			"select id, j from %s.src where id <> 4 order by id into outfile '%s' format 'csv' header 'false'", database, csvPath))
		csvBytes, err := os.ReadFile(csvPath)
		require.NoError(t, err)
		t.Logf("CSV OUTFILE bytes=%x text=%q", csvBytes, string(csvBytes))
		require.Contains(t, string(csvBytes), "1,\"null\"\n", "CSV writes the JSON literal null as text")
		require.Contains(t, string(csvBytes), "2,\\N\n", "CSV writes the SQL NULL as the NULL token")
		exec("create table " + database + ".csv_dst (id int primary key, j json)")
		exec(fmt.Sprintf(
			"load data infile '%s' into table %s.csv_dst fields terminated by ','", csvPath, database))
		require.Equal(t, []jsonRow{wantSrc[0], wantSrc[1], wantSrc[2], wantSrc[4]}, readRows("csv_dst"))
	})
}

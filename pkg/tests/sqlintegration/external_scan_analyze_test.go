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
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/parquet-go/parquet-go"
	"github.com/stretchr/testify/require"
)

func TestExternalScanAnalyzeOutputRows(t *testing.T) {
	runSQLIntegration(t, func(c embed.Cluster) {
		ctx, cancel := context.WithTimeout(t.Context(), 90*time.Second)
		defer cancel()
		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		defer cleanupSQLIntegration(t, cn, "drop database if exists external_scan_analyze")
		db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", cn.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		defer db.Close()
		conn, err := db.Conn(ctx)
		require.NoError(t, err)
		defer conn.Close()
		exec := func(t *testing.T, q string) {
			t.Helper()
			_, err := conn.ExecContext(ctx, q)
			require.NoError(t, err, q)
		}
		explain := func(t *testing.T, q string) string {
			t.Helper()
			rows, err := conn.QueryContext(ctx, q)
			require.NoError(t, err, q)
			defer rows.Close()
			var lines []string
			for rows.Next() {
				var line string
				require.NoError(t, rows.Scan(&line))
				lines = append(lines, line)
			}
			require.NoError(t, rows.Err())
			return strings.Join(lines, "\n")
		}
		type sourceRow struct {
			ID int32  `parquet:"id"`
			S  string `parquet:"s"`
		}
		want := []sourceRow{{1, "alpha"}, {2, "beta"}, {3, "gamma"}}
		checkRows := func(t *testing.T, table string, expected []sourceRow) {
			t.Helper()
			rows, err := conn.QueryContext(ctx, "select id,s from "+table+" order by id")
			require.NoError(t, err)
			defer rows.Close()
			var got []sourceRow
			for rows.Next() {
				var row sourceRow
				require.NoError(t, rows.Scan(&row.ID, &row.S))
				got = append(got, row)
			}
			require.NoError(t, rows.Err())
			require.Equal(t, expected, got)
		}
		checkScan := func(t *testing.T, text, scan string, count int) {
			t.Helper()
			// Match only stable execution fields, not costs, timing or node IDs.
			pattern := fmt.Sprintf(`%s[^\n]*\n[^\n]*outputRows=%d\b`, scan, count)
			require.Regexp(t, regexp.MustCompile(pattern), text)
		}
		exec(t, "drop database if exists external_scan_analyze")
		exec(t, "create database external_scan_analyze")
		exec(t, "use external_scan_analyze")
		exec(t, "create table dst(id int,s varchar(20))")
		var parquetData bytes.Buffer
		writer := parquet.NewGenericWriter[sourceRow](&parquetData)
		_, err = writer.Write(want)
		require.NoError(t, err)
		require.NoError(t, writer.Close())
		dir := t.TempDir()
		for _, tc := range []struct {
			name, format string
			data         []byte
			want         []sourceRow
		}{
			{"csv", "csv", []byte("1,alpha\n2,beta\n3,gamma\n"), want},
			{"parquet", "parquet", parquetData.Bytes(), want},
			{"empty", "csv", nil, nil},
		} {
			t.Run(tc.name, func(t *testing.T) {
				path := filepath.Join(dir, tc.name+"."+tc.format)
				require.NoError(t, os.WriteFile(path, tc.data, 0600))
				exec(t, fmt.Sprintf("create external table %s(id int,s varchar(20)) infile{'filepath'='%s','format'='%s'} fields terminated by ','", tc.name, strings.ReplaceAll(path, "'", "''"), tc.format))
				checkRows(t, tc.name, tc.want)
				t.Run("select", func(t *testing.T) {
					checkScan(t, explain(t, "explain analyze select id,s from "+tc.name), "External Scan", len(tc.want))
				})
				t.Run("insert", func(t *testing.T) {
					exec(t, "truncate table dst")
					text := explain(t, "explain analyze insert into dst select id,s from "+tc.name)
					checkRows(t, "dst", tc.want)
					checkScan(t, text, "External Scan", len(tc.want))
					if len(tc.want) > 0 {
						checkScan(t, text, "Project", len(tc.want))
						checkScan(t, text, "PreInsert", len(tc.want))
					}
				})
			})
		}
		t.Run("internal-table-control", func(t *testing.T) {
			exec(t, "create table src(id int,s varchar(20))")
			exec(t, "insert into src values(1,'alpha'),(2,'beta'),(3,'gamma')")
			exec(t, "truncate table dst")
			checkScan(t, explain(t, "explain analyze insert into dst select id,s from src"), "Table Scan", len(want))
			checkRows(t, "dst", want)
		})
	})
}

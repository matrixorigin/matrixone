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
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	mysql "github.com/go-sql-driver/mysql"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/stretchr/testify/require"
)

// #31326: speculative strict boundary selection must abandon an illegal tail
// without changing the execution error/rollback or disabling ordinary fanout.
func TestLoadCSVStrictProbeFallback(t *testing.T) {
	runSQLIntegration(t, func(c embed.Cluster) {
		ctx, cancel := context.WithTimeout(t.Context(), 90*time.Second)
		defer cancel()
		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		defer cleanupSQLIntegration(t, cn, "drop database if exists strict_probe_load")
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
		exec(t, "create database strict_probe_load")
		exec(t, "use strict_probe_load")
		exec(t, "set max_dop=2")
		exec(t, "set sql_mode='STRICT_TRANS_TABLES'")
		exec(t, "create table mapped(c1 int,c2 varchar(64),c3 decimal(12,2))")
		exec(t, "create table matched(id int,c1 int,c2 varchar(64),c3 decimal(12,2))")
		testingContext := moruntime.MustGetTestingContext(cn.ServiceID())
		const rows = 601 // The nominal split leaves more than 256 illegal candidates.
		for _, tc := range []struct {
			name, table, columns, decimal string
			valid                         bool
		}{
			{"misaligned columns", "mapped", "c1,c2,c3", "1.25", false},
			{"invalid decimal", "matched", "id,c1,c2,c3", "bad", false},
			{"valid parallel control", "matched", "id,c1,c2,c3", "1.25", true},
		} {
			t.Run(tc.name, func(t *testing.T) {
				var content strings.Builder
				content.WriteString("id,c1,c2,c3\n")
				for i := 0; i < rows; i++ {
					fmt.Fprintf(&content, "%04d,%02d,st%020d,%s\n", i, i%100, i, tc.decimal)
				}
				path := filepath.Join(t.TempDir(), "probe.csv")
				require.NoError(t, os.WriteFile(path, []byte(content.String()), 0600))
				var observed struct {
					sync.Mutex
					readers int
					offsets []int64
				}
				previous := testingContext.SetLoadDataTestConfig(moruntime.LoadDataTestConfig{Filepath: path, MinSize: int64(content.Len() / 2), OnRanges: func(n int, o []int64) {
					observed.Lock()
					defer observed.Unlock()
					observed.readers = n
					observed.offsets = append([]int64(nil), o...)
				}})
				defer testingContext.SetLoadDataTestConfig(previous)
				load := "load data infile '" + strings.ReplaceAll(path, "'", "''") + "' into table " + tc.table + " fields terminated by ',' lines terminated by '\\n' ignore 1 lines (" + tc.columns + ") parallel '"
				var serialCode uint16
				for _, parallel := range []string{"false", "true"} {
					exec(t, "truncate table "+tc.table)
					_, err := conn.ExecContext(ctx, load+parallel+"'")
					if tc.valid {
						require.NoError(t, err)
						func() {
							got, err := conn.QueryContext(ctx, "select id,c1,c2,c3 from matched order by id")
							require.NoError(t, err)
							defer got.Close()
							i := 0
							for got.Next() {
								var id, n int
								var text, decimal string
								require.NoError(t, got.Scan(&id, &n, &text, &decimal))
								require.Equal(t, i, id)
								require.Equal(t, i%100, n)
								require.Equal(t, fmt.Sprintf("st%020d", i), text)
								require.Equal(t, "1.25", decimal)
								i++
							}
							require.NoError(t, got.Err())
							require.Equal(t, rows, i)
						}()
					} else {
						var me *mysql.MySQLError
						require.ErrorAs(t, err, &me)
						if parallel == "false" {
							serialCode = me.Number
						} else {
							require.Equal(t, serialCode, me.Number)
						}
						if tc.name == "misaligned columns" {
							require.Equal(t, uint16(20301), me.Number)
							require.Contains(t, me.Message, "row 1")
						}
						var count int
						require.NoError(t, conn.QueryRowContext(ctx, "select count(*) from "+tc.table).Scan(&count))
						require.Zero(t, count)
					}
				}
				observed.Lock()
				readers, offsets := observed.readers, append([]int64(nil), observed.offsets...)
				observed.Unlock()
				require.Equal(t, 2, readers)
				if tc.valid {
					require.Len(t, offsets, 4)
				} else {
					require.Equal(t, []int64{12, -1}, offsets)
				}
				t.Logf("requested_readers=%d ranges=%v rows=%d valid=%t", readers, offsets, rows, tc.valid)
			})
		}
		exec(t, "create table long_records(payload longtext)")
		for _, tc := range []struct {
			name                  string
			recordBytes           int
			multiline, restartBOM bool
			atSoftLimit           bool
			wantRanges            int
		}{
			{name: "short record control", recordBytes: 400 * 1024, wantRanges: 2},
			{name: "long record parallel", recordBytes: 600 * 1024, wantRanges: 2},
			{name: "quoted multiline fallback", multiline: true, wantRanges: 1},
			{name: "BOM restart fallback", recordBytes: 600 * 1024, restartBOM: true, wantRanges: 1},
			{name: "soft limit plain control", atSoftLimit: true, wantRanges: 2},
			{name: "soft limit BOM fallback", atSoftLimit: true, restartBOM: true, wantRanges: 1},
			{name: "soft limit multiline fallback", atSoftLimit: true, multiline: true, wantRanges: 1},
		} {
			t.Run(tc.name, func(t *testing.T) {
				var values []string
				if tc.atSoftLimit {
					// IGNORE removes the header before computing the half-file guess.
					// The suffix is exactly the former 256 KiB physical input cap.
					const suffixBytes, lastBytes = 256 * 1024, 6144
					const firstBytes = 2*suffixBytes - lastBytes
					if tc.multiline {
						values = []string{strings.Repeat("x", firstBytes-2) + "\n" + strings.Repeat("y", lastBytes-2)}
					} else {
						last := strings.Repeat("b", lastBytes-1)
						if tc.restartBOM {
							last = "\xef\xbb\xbf" + strings.Repeat("b", lastBytes-4)
						}
						values = []string{strings.Repeat("x", firstBytes-1), last}
					}
				} else {
					values = make([]string, 5)
					for i := range values {
						if tc.multiline {
							values[i] = strings.Repeat("x", 900*1024) + "\n" + strings.Repeat("y", 300*1024)
						} else {
							prefix := fmt.Sprintf("%d:", i)
							if tc.restartBOM && i == 3 {
								prefix = "\xef\xbb\xbf" + prefix
							}
							values[i] = prefix + strings.Repeat("x", tc.recordBytes-len(prefix)-1)
						}
					}
				}
				var content strings.Builder
				content.WriteString("payload\n")
				for _, value := range values {
					if tc.multiline {
						content.WriteByte('"')
					}
					content.WriteString(value)
					if tc.multiline {
						content.WriteByte('"')
					}
					content.WriteByte('\n')
				}
				rows := len(values)
				sort.Strings(values)
				path := filepath.Join(t.TempDir(), "long-records.csv")
				require.NoError(t, os.WriteFile(path, []byte(content.String()), 0600))
				var observed struct {
					sync.Mutex
					readers int
					offsets []int64
				}
				previous := testingContext.SetLoadDataTestConfig(moruntime.LoadDataTestConfig{
					Filepath: path, MinSize: int64(content.Len() / 2), OnRanges: func(n int, o []int64) {
						observed.Lock()
						defer observed.Unlock()
						observed.readers = n
						observed.offsets = append([]int64(nil), o...)
					},
				})
				defer testingContext.SetLoadDataTestConfig(previous)
				load := "load data infile '" + strings.ReplaceAll(path, "'", "''") + "' into table long_records fields terminated by ',' lines terminated by '\\n' ignore 1 lines parallel '"
				for _, parallel := range []string{"false", "true"} {
					exec(t, "truncate table long_records")
					exec(t, load+parallel+"'")
					func() {
						got, err := conn.QueryContext(ctx, "select payload from long_records")
						require.NoError(t, err)
						defer got.Close()
						var actual []string
						for got.Next() {
							var payload string
							require.NoError(t, got.Scan(&payload))
							actual = append(actual, payload)
						}
						require.NoError(t, got.Err())
						require.Len(t, actual, rows)
						sort.Strings(actual)
						for i := range values {
							require.True(t, values[i] == actual[i], "parallel=%s payload %d differs, bytes=%d/%d", parallel, i, len(values[i]), len(actual[i]))
						}
					}()
				}
				observed.Lock()
				readers, offsets := observed.readers, append([]int64(nil), observed.offsets...)
				observed.Unlock()
				require.Equal(t, 2, readers)
				require.Len(t, offsets, 2*tc.wantRanges)
				require.Equal(t, int64(len("payload\n")), offsets[0])
				t.Logf("requested_readers=%d ranges=%v rows=%d exact_serial_parallel_values=true", readers, offsets, rows)
			})
		}
	})
}

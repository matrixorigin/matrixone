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
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/stretchr/testify/require"
)

func TestLoadCSVParallelNonStrictMultiline(t *testing.T) {
	runSQLIntegration(t, func(c embed.Cluster) {
		ctx, cancel := context.WithTimeout(t.Context(), 90*time.Second)
		defer cancel()
		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		defer cleanupSQLIntegration(t, cn, "drop database if exists csv_multiline_load")
		db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", cn.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		defer db.Close()
		conn, err := db.Conn(ctx)
		require.NoError(t, err)
		defer conn.Close()
		exec := func(query string) {
			t.Helper()
			_, err := conn.ExecContext(ctx, query)
			require.NoError(t, err, query)
		}
		exec("drop database if exists csv_multiline_load")
		exec("create database csv_multiline_load")
		exec("use csv_multiline_load")
		exec("set max_dop=2")
		// No unique constraint: it must not conceal duplicated row identities.
		exec("create table dst(id int, s varchar(100))")
		data := "1,\"" + strings.Repeat("a", 32) + "\nb\"\n2,\"x\"\"y\\\\z\"\n3,tail\n"
		path := filepath.Join(t.TempDir(), "multiline.csv")
		require.NoError(t, os.WriteFile(path, []byte(data), 0600))
		nominalSplit := int64(len(data) / 2)
		require.Greater(t, nominalSplit, int64(strings.IndexByte(data, '"')))
		require.Less(t, nominalSplit, int64(strings.IndexByte(data, '\n')))
		firstRecordEnd := int64(strings.Index(data, "\nb\"\n") + len("\nb\"\n"))
		var observed struct {
			sync.Mutex
			readers int
			offsets [][]int64
		}
		testingContext := moruntime.MustGetTestingContext(cn.ServiceID())
		previous := testingContext.SetLoadDataTestConfig(moruntime.LoadDataTestConfig{
			Filepath: path, MinSize: nominalSplit,
			OnRanges: func(readers int, offsets []int64) {
				observed.Lock()
				defer observed.Unlock()
				observed.readers = readers
				observed.offsets = append(observed.offsets, append([]int64(nil), offsets...))
				// The observer owns a private copy, never the execution ranges.
				if len(offsets) > 0 {
					offsets[0] = -1
				}
			},
		})
		defer testingContext.SetLoadDataTestConfig(previous)
		load := "load data infile '" + strings.ReplaceAll(path, "'", "''") +
			"' into table dst fields terminated by ',' enclosed by '\"' escaped by '\\\\' lines terminated by '\\n'"
		type row struct {
			id int
			s  string
		}
		want := []row{{1, strings.Repeat("a", 32) + "\nb"}, {2, "x\"y\\z"}, {3, "tail"}}
		checkRows := func() {
			t.Helper()
			rows, err := conn.QueryContext(ctx, "select id,s from dst order by id")
			require.NoError(t, err)
			defer rows.Close()
			var got []row
			for rows.Next() {
				var r row
				require.NoError(t, rows.Scan(&r.id, &r.s))
				got = append(got, r)
			}
			require.NoError(t, rows.Err())
			require.Equal(t, want, got, "complete values and exact identities, without omissions or duplicates")
		}
		exec(load + " parallel 'true' strict 'false'")
		checkRows()
		observed.Lock()
		readers, offsets := observed.readers, observed.offsets
		observed.Unlock()
		require.Equal(t, 2, readers, "SQL must admit multiple parallel readers")
		require.Equal(t, [][]int64{{0, firstRecordEnd, firstRecordEnd, -1}}, offsets)
		t.Logf("SQL parallel/non-strict: bytes=%d nominal_split=%d readers=%d actual_ranges=%v", len(data), nominalSplit, readers, offsets)
		// The same public SQL and data through serial execution is the control.
		exec("truncate table dst")
		exec(load + " parallel 'false' strict 'false'")
		checkRows()
	})
}

func TestLoadJSONLineIgnoreWhitespace(t *testing.T) {
	runSQLIntegration(t, func(c embed.Cluster) {
		ctx, cancel := context.WithTimeout(t.Context(), 90*time.Second)
		defer cancel()
		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		defer cleanupSQLIntegration(t, cn, "drop database if exists jsonline_ignore_whitespace")
		db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", cn.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		defer db.Close()
		conn, err := db.Conn(ctx)
		require.NoError(t, err)
		defer conn.Close()
		exec := func(t *testing.T, query string) {
			t.Helper()
			_, err := conn.ExecContext(ctx, query)
			require.NoError(t, err, query)
		}
		exec(t, "create database jsonline_ignore_whitespace")
		exec(t, "use jsonline_ignore_whitespace")
		exec(t, "set max_dop=2")
		exec(t, "create table dst(a int)")
		testingContext := moruntime.MustGetTestingContext(cn.ServiceID())
		for _, shape := range []string{"object", "array"} {
			t.Run(shape, func(t *testing.T) {
				row := func(value int) string {
					if shape == "object" {
						return fmt.Sprintf(`{"a":%d}`, value)
					}
					return fmt.Sprintf("[%d]", value)
				}
				for _, split := range []bool{false, true} {
					data := "\n \n" + row(2) + "\n" + row(3) + "\n"
					want := "3"
					if split {
						data = "\n \n" + row(1) + "\n \n" + row(2) + "\n" + row(3) + "\n" + row(4) + "\n" + row(5) + "\n "
						want = "2,3,4,5"
					}
					path := filepath.Join(t.TempDir(), "records.jsonl")
					require.NoError(t, os.WriteFile(path, []byte(data), 0600))
					var observed struct {
						sync.Mutex
						readers int
						offsets []int64
					}
					previous := testingContext.SetLoadDataTestConfig(moruntime.LoadDataTestConfig{
						Filepath: path, MinSize: 1,
						OnRanges: func(readers int, offsets []int64) {
							observed.Lock()
							defer observed.Unlock()
							observed.readers = readers
							observed.offsets = append([]int64(nil), offsets...)
						},
					})
					func() {
						defer testingContext.SetLoadDataTestConfig(previous)
						for _, parallel := range []bool{false, true} {
							exec(t, "truncate table dst")
							exec(t, fmt.Sprintf("load data infile{'filepath'='%s','format'='jsonline','jsondata'='%s'} into table dst ignore 1 lines parallel '%t' strict 'false'", strings.ReplaceAll(path, "'", "''"), shape, parallel))
							var got string
							require.NoError(t, conn.QueryRowContext(ctx, "select group_concat(a order by a) from dst").Scan(&got))
							require.Equal(t, want, got, "shape=%s split=%t parallel=%t", shape, split, parallel)
							if split && parallel {
								observed.Lock()
								readers, offsets := observed.readers, append([]int64(nil), observed.offsets...)
								observed.Unlock()
								require.Equal(t, 2, readers, "must exercise actual multiple readers")
								require.NotEmpty(t, offsets)
								require.Equal(t, int64(3+len(row(1))+1), offsets[0], "prescan skips exactly the first visible JSON record")
							}
						}
					}()
				}
			})
		}
	})
}

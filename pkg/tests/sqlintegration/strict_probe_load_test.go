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
	})
}

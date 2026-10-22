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
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/embed"
)

// TestIssue30552MODUMPSplit exercises MODUMP's public SQL path. The query ID
// is dynamic and the MODUMP grammar requires a string literal, so this cannot
// be represented as a stable SQL-only BVT/MOTR golden file.
func TestIssue30552MODUMPSplit(t *testing.T) {
	embed.RunBaseClusterTests(t, func(c embed.Cluster) {
		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		port := cn.GetServiceConfig().CN.Frontend.Port
		db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", port))
		require.NoError(t, err)
		defer db.Close()
		ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
		defer cancel()
		conn, err := db.Conn(ctx)
		require.NoError(t, err)
		defer conn.Close()
		exec := func(q string) {
			t.Helper()
			_, e := conn.ExecContext(ctx, q)
			require.NoError(t, e, q)
		}
		exec("drop database if exists qa_issue_30552")
		exec("create database qa_issue_30552")
		defer func() {
			cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cleanupCancel()
			_, _ = conn.ExecContext(cleanupCtx, "drop database if exists qa_issue_30552")
		}()
		exec("use qa_issue_30552")
		exec("set save_query_result=on")
		defer func() { _, _ = conn.ExecContext(context.Background(), "set save_query_result=off") }()
		exec("create table src(id int primary key, v varchar(300), d date)")
		for i := 1; i <= 16; i++ {
			exec(fmt.Sprintf("insert into src values(%d,repeat('x',200),'2024-01-01')", i))
		}
		rows, err := conn.QueryContext(ctx, "/* save_result */ select id,v,d from src order by id")
		require.NoError(t, err)
		for rows.Next() {
		}
		require.NoError(t, rows.Err())
		require.NoError(t, rows.Close())
		exec("set @qa_saved_result_id = last_query_id()")
		var id string
		require.NoError(t, conn.QueryRowContext(ctx, "select @qa_saved_result_id").Scan(&id))
		require.NotEmpty(t, id)

		t.Run("split_and_data_integrity", func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "split.csv")
			exec(fmt.Sprintf("modump query_result '%s' into '%s' max_file_size 1", id, path))
			files, globErr := filepath.Glob(path + "*")
			require.NoError(t, globErr)
			require.GreaterOrEqual(t, len(files), 2, "the 1 KiB limit must rotate a 3 KiB+ result")
			seen := make(map[int]int)
			var totalBytes int64
			for _, name := range files {
				f, openErr := os.Open(name)
				require.NoError(t, openErr)
				st, statErr := f.Stat()
				require.NoError(t, statErr)
				totalBytes += st.Size()
				r := csv.NewReader(f)
				for {
					rec, readErr := r.Read()
					if readErr == io.EOF {
						break
					}
					require.NoError(t, readErr)
					require.Len(t, rec, 3)
					if rec[0] == "id" {
						continue
					}
					n, parseErr := strconv.Atoi(rec[0])
					require.NoError(t, parseErr)
					require.Equal(t, strings.Repeat("x", 200), rec[1])
					require.Equal(t, "2024-01-01", rec[2])
					seen[n]++
				}
				require.NoError(t, f.Close())
			}
			require.Greater(t, totalBytes, int64(1024))
			require.Len(t, seen, 16)
			for i := 1; i <= 16; i++ {
				require.Equal(t, 1, seen[i], "row %d must occur exactly once", i)
			}
		})

		t.Run("non_split_control", func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "whole.csv")
			exec(fmt.Sprintf("modump query_result '%s' into '%s' max_file_size 100000", id, path))
			files, globErr := filepath.Glob(path + "*")
			require.NoError(t, globErr)
			require.Len(t, files, 1)
			content, readErr := os.ReadFile(files[0])
			require.NoError(t, readErr)
			require.Contains(t, string(content), "2024-01-01")
		})

		t.Run("unknown_query_id", func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "invalid.csv")
			_, e := conn.ExecContext(ctx, fmt.Sprintf("modump query_result '%s' into '%s' max_file_size 1", "00000000-0000-0000-0000-000000000000", path))
			require.Error(t, e)
			files, globErr := filepath.Glob(path + "*")
			require.NoError(t, globErr)
			require.Empty(t, files, "invalid ID must not leave a partial export")
		})
	})
}

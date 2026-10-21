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
	"encoding/csv"
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/clusterservice"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/tests/testutils"
	"github.com/stretchr/testify/require"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"
)

// The frontend whitebox case injects slow CSV I/O. This complementary public
// SQL case verifies completion, file contents and continued session use through
// the actual parser, compiler, frontend and file service.
func TestCSVDumpTerminalPublicSQL(t *testing.T) {
	embed.RunBaseClusterTests(t, func(c embed.Cluster) {
		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()
		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", cn.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		defer db.Close()
		db.SetMaxOpenConns(2)
		conn, err := db.Conn(ctx)
		require.NoError(t, err)
		defer conn.Close()
		name := strings.ToLower(testutils.GetDatabaseName(t))
		mustExec(t, ctx, conn, fmt.Sprintf("create database `%s`", name))
		defer func() {
			cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cleanupCancel()
			_, err := db.ExecContext(cleanupCtx, fmt.Sprintf("drop database if exists `%s`", name))
			require.NoError(t, err)
		}()
		mustExec(t, ctx, conn, fmt.Sprintf("use `%s`", name))
		mustExec(t, ctx, conn, "create table t(id int primary key, note varchar(16))")
		mustExec(t, ctx, conn, "insert into t values (1,'a'),(2,'b')")
		path := filepath.Join(t.TempDir(), "dump.csv")
		mustExec(t, ctx, conn, fmt.Sprintf("select id,note from t order by id into outfile '%s' fields terminated by ',' enclosed by '' lines terminated by '\\n' header 'false'", path))
		file, err := os.Open(path)
		require.NoError(t, err)
		defer file.Close()
		rows, err := csv.NewReader(file).ReadAll()
		require.NoError(t, err)
		require.Equal(t, [][]string{{"1", "a"}, {"2", "b"}}, rows)
		// A regular file used as a parent gives a deterministic destination
		// failure without relying on permissions, disk capacity or timing.
		blockedParent := filepath.Join(t.TempDir(), "regular-file")
		require.NoError(t, os.WriteFile(blockedParent, []byte("occupied"), 0600))
		badPath := filepath.Join(blockedParent, "dump.csv")
		for range 2 {
			_, err = conn.ExecContext(ctx, fmt.Sprintf("select id,note from t into outfile '%s' header 'false'", badPath))
			require.ErrorContains(t, err, "not a directory")
		}
		var count int
		require.NoError(t, conn.QueryRowContext(ctx, "select count(*) from t").Scan(&count))
		require.Equal(t, 2, count)
	})
}

// Three storage blocks give the three-CN fixture actual scan work to distribute.
// EXPLAIN PHYPLAN ANALYZE proves the scan executes remote scopes; the separate
// export checks every row through the public SQL path under the same setting.
func TestCSVDumpTerminalMultiCNSQL(t *testing.T) {
	embed.RunBaseClusterTests(t, func(c embed.Cluster) {
		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()
		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		_, err = c.GetCNService(2)
		require.NoError(t, err)
		// Service startup and compiler membership refresh have separate owners.
		// Wait for the actual working topology, rather than a timing delay or
		// retrying the export to obtain a convenient plan.
		membership := clusterservice.GetMOCluster(cn.ServiceID())
		tick := time.NewTicker(100 * time.Millisecond)
		defer tick.Stop()
		for {
			membership.ForceRefresh(true)
			working := 0
			membership.GetCNService(clusterservice.NewSelector(), func(metadata.CNService) bool { working++; return true })
			if working == 3 {
				break
			}
			select {
			case <-ctx.Done():
				t.Fatalf("three working CNs not visible: count=%d err=%v", working, ctx.Err())
			case <-tick.C:
			}
		}

		db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", cn.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		defer db.Close()
		conn, err := db.Conn(ctx)
		require.NoError(t, err)
		defer conn.Close()
		name := strings.ToLower(testutils.GetDatabaseName(t))
		mustExec(t, ctx, conn, fmt.Sprintf("create database `%s`", name))
		defer func() {
			cleanup, stop := context.WithTimeout(context.Background(), 10*time.Second)
			defer stop()
			_, err := db.ExecContext(cleanup, fmt.Sprintf("drop database if exists `%s`", name))
			require.NoError(t, err)
		}()
		mustExec(t, ctx, conn, fmt.Sprintf("use `%s`", name))
		mustExec(t, ctx, conn, "create table t(id int primary key)")
		const rowCount = 3 * 8192
		mustExec(t, ctx, conn, fmt.Sprintf("insert into t select result from generate_series(1,%d)g", rowCount))
		previous := plan.GetForceScanOnMultiCN()
		plan.SetForceScanOnMultiCN(true)
		defer plan.SetForceScanOnMultiCN(previous)
		path := filepath.Join(t.TempDir(), "multicn.csv")
		exportSQL := func(target string) string {
			return fmt.Sprintf("select id from t order by id into outfile '%s' fields terminated by ',' enclosed by '' lines terminated by '\\n' header 'false'", target)
		}
		rows, err := conn.QueryContext(ctx, "explain phyplan analyze "+exportSQL(filepath.Join(t.TempDir(), "analyze.csv")))
		require.NoError(t, err)
		defer rows.Close()
		var physical strings.Builder
		for rows.Next() {
			var line string
			require.NoError(t, rows.Scan(&line))
			physical.WriteString(line)
			physical.WriteByte('\n')
		}
		require.NoError(t, rows.Err())
		require.NoError(t, rows.Close())
		// The fixture's raw config is populated from TOML on service startup;
		// use the executed plan's endpoints rather than its empty raw fields.
		addresses := make(map[string]struct{})
		for _, match := range regexp.MustCompile(`Magic: Remote, addr:([^,]+),`).FindAllStringSubmatch(physical.String(), -1) {
			addresses[match[1]] = struct{}{}
		}
		require.Len(t, addresses, 3, "scan must execute on three distinct CN endpoints: %s", physical.String())
		mustExec(t, ctx, conn, exportSQL(path))
		file, err := os.Open(path)
		require.NoError(t, err)
		defer file.Close()
		reader := csv.NewReader(file)
		values, err := reader.ReadAll()
		require.NoError(t, err)
		require.Len(t, values, rowCount)
		for i, row := range values {
			require.Equal(t, []string{strconv.Itoa(i + 1)}, row)
		}
		var first int
		require.NoError(t, conn.QueryRowContext(ctx, "select id from t order by id limit 1").Scan(&first))
		require.Equal(t, 1, first)
		var count int
		require.NoError(t, conn.QueryRowContext(ctx, "select count(*) from t").Scan(&count))
		require.Equal(t, rowCount, count)
	})
}

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
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/tests/testutils"
	"github.com/stretchr/testify/require"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// Public SQL control for successful content, destination failure and session reuse.
// Cancellation phase synchronization is covered in frontend's real statement tests.
func TestExportLifecyclePublicSQL(t *testing.T) {
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

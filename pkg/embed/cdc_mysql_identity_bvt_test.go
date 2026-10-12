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
	"fmt"
	"net/url"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/matrixorigin/matrixone/pkg/cdc"
	"github.com/stretchr/testify/require"
)

// Opt-in real MySQL 8 BVT. The URI user needs PROCESS and SELECT/DDL/DML rights on
// cdc_rev7_sink; its target must use InnoDB.
func TestCDCMySQL8TargetIdentityBVT(t *testing.T) {
	targetURI := os.Getenv("MO_CDC_MYSQL8_TEST_URI")
	if targetURI == "" {
		t.Skip("set MO_CDC_MYSQL8_TEST_URI to run the real MySQL 8 CDC BVT")
	}
	parsed, err := url.Parse(targetURI)
	require.NoError(t, err)
	require.Equal(t, "mysql", parsed.Scheme)
	require.NotNil(t, parsed.User)
	require.NotEmpty(t, parsed.Host)
	password, _ := parsed.User.Password()
	config := mysql.NewConfig()
	config.User, config.Passwd = parsed.User.Username(), password
	config.Net, config.Addr = "tcp", parsed.Host
	targetDB, err := sql.Open("mysql", config.FormatDSN())
	require.NoError(t, err)
	defer targetDB.Close()
	RunSingleCNBaseClusterTests(t, func(cluster Cluster) {
		cn, cnErr := cluster.GetCNService(0)
		require.NoError(t, cnErr)
		port := cn.GetServiceConfig().CN.Frontend.Port
		ctx, cancel := context.WithTimeout(t.Context(), 4*time.Minute)
		defer cancel()
		root, openErr := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", port))
		require.NoError(t, openErr)
		defer root.Close()
		require.NoError(t, execSQL(ctx, root, "CREATE ACCOUNT cdc_mysql_probe ADMIN_NAME 'admin' IDENTIFIED BY '111'"))
		defer func() {
			cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cleanupCancel()
			_, _ = root.ExecContext(cleanupCtx, "DROP ACCOUNT IF EXISTS cdc_mysql_probe")
		}()
		account, openErr := sql.Open("mysql", fmt.Sprintf("cdc_mysql_probe#admin:111@tcp(127.0.0.1:%d)/", port))
		require.NoError(t, openErr)
		defer account.Close()
		require.NoError(t, execSQL(ctx, account, "CREATE DATABASE cdc_mysql_src"))
		require.NoError(t, execSQL(ctx, account, "CREATE TABLE cdc_mysql_src.t (id INT PRIMARY KEY, v INT)"))
		require.NoError(t, execSQL(ctx, account, "INSERT INTO cdc_mysql_src.t VALUES (1,10),(2,20)"))
		require.NoError(t, execSQL(ctx, account, "CREATE PITR cdc_mysql_pitr FOR DATABASE cdc_mysql_src RANGE 2 'h'"))
		defer func() {
			cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cleanupCancel()
			_, _ = account.ExecContext(cleanupCtx, "DROP CDC TASK cdc_mysql_task")
			_, _ = account.ExecContext(cleanupCtx, "DROP PITR cdc_mysql_pitr")
		}()
		require.NoError(t, execSQL(ctx, targetDB, "DROP TABLE IF EXISTS cdc_rev7_sink.t"))
		sourceURI := fmt.Sprintf("mysql://cdc_mysql_probe#admin:111@127.0.0.1:%d", port)
		_, err = account.ExecContext(ctx, fmt.Sprintf(
			"CREATE CDC cdc_mysql_task '%s' 'mysql' '%s' 'cdc_mysql_src:cdc_rev7_sink' {'Level'='database'}",
			sourceURI, targetURI))
		require.NoError(t, err)
		readRows := func() []int {
			rows, readErr := targetDB.QueryContext(ctx, "SELECT id FROM cdc_rev7_sink.t ORDER BY id")
			if readErr != nil {
				return nil
			}
			defer rows.Close()
			var ids []int
			for rows.Next() {
				var id int
				if rows.Scan(&id) != nil {
					return nil
				}
				ids = append(ids, id)
			}
			if rows.Err() != nil {
				return nil
			}
			return ids
		}
		require.Eventually(t, func() bool { return reflect.DeepEqual(readRows(), []int{1, 2}) }, 90*time.Second, 250*time.Millisecond)
		require.NoError(t, execSQL(ctx, account, "PAUSE CDC TASK cdc_mysql_task"))
		require.Eventually(t, func() bool {
			var state string
			return root.QueryRowContext(ctx,
				"SELECT state FROM mo_catalog.mo_cdc_task WHERE task_name = 'cdc_mysql_task'").Scan(&state) == nil && state == cdc.CDCState_Paused
		}, 30*time.Second, 250*time.Millisecond)
		var identity string
		require.NoError(t, root.QueryRowContext(ctx,
			"SELECT w.target_identity FROM mo_catalog.mo_cdc_watermark AS w JOIN mo_catalog.mo_cdc_task AS t "+
				"ON t.account_id = w.account_id AND t.task_id = w.task_id "+
				"WHERE t.task_name = 'cdc_mysql_task' AND w.db_name = 'cdc_mysql_src' AND w.table_name = 't'").Scan(&identity))
		require.True(t, strings.HasPrefix(identity, "mysql:"))
		require.NoError(t, execSQL(ctx, targetDB, "DROP TABLE cdc_rev7_sink.t"))
		require.NoError(t, execSQL(ctx, targetDB, "CREATE TABLE cdc_rev7_sink.t (id INT PRIMARY KEY, v INT) ENGINE=InnoDB"))
		require.NoError(t, execSQL(ctx, targetDB, "INSERT INTO cdc_rev7_sink.t VALUES (1,10)"))
		require.NoError(t, execSQL(ctx, account, "INSERT INTO cdc_mysql_src.t VALUES (3,30)"))
		require.NoError(t, execSQL(ctx, account, "RESUME CDC TASK cdc_mysql_task"))
		require.Eventually(t, func() bool {
			var state string
			return root.QueryRowContext(ctx,
				"SELECT state FROM mo_catalog.mo_cdc_task WHERE task_name = 'cdc_mysql_task'").Scan(&state) == nil && state == cdc.CDCState_Failed
		}, 90*time.Second, 250*time.Millisecond)
		require.Equal(t, []int{1}, readRows())
	})
}

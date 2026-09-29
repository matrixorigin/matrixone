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
	"fmt"
	"testing"
	"time"

	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/stretchr/testify/require"
)

// A cost hint set by an earlier connection also reaches background executors
// through the CN runtime. It must not remove the local-delivery constraint of
// the DedupJoin used by CREATE DATABASE's compatibility-mode INSERT.
func TestIssue29068CreateDatabaseWithExecutionHint(t *testing.T) {
	embed.RunBaseClusterTests(t, func(c embed.Cluster) {
		for _, cnIndex := range []int{0, 1} {
			cn, err := c.GetCNService(cnIndex)
			require.NoError(t, err)
			cfg := cn.GetServiceConfig()
			for _, hint := range []int{2, 3} {
				t.Run(fmt.Sprintf("cn-%d/execType-%d", cnIndex, hint), func(t *testing.T) {
					db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", cfg.CN.Frontend.Port))
					require.NoError(t, err)
					defer func() { require.NoError(t, db.Close()) }()
					// Do not reuse the hint-setting connection for the DDL. This
					// reproduces the independent-new-connections failure mode.
					db.SetMaxIdleConns(0)
					rt := moruntime.ServiceRuntime(cn.ServiceID())
					oldHints, ok := rt.GetGlobalVariables("optimizer_hints")
					if !ok {
						oldHints = ""
					}
					name := fmt.Sprintf("create_db_hint_cn%d_type%d", cnIndex, hint)
					defer func() {
						rt.SetGlobalVariables("optimizer_hints", oldHints)
						ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
						defer cancel()
						_, err := db.ExecContext(ctx, "drop database if exists "+name)
						require.NoError(t, err)
						var remaining int
						require.NoError(t, db.QueryRowContext(ctx,
							"select count(*) from mo_catalog.mo_mysql_compatibility_mode where dat_name = ?", name).Scan(&remaining))
						require.Zero(t, remaining)
					}()

					ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
					defer cancel()
					_, err = db.ExecContext(ctx, fmt.Sprintf("set session optimizer_hints='execType=%d'", hint))
					require.NoError(t, err)
					conn, err := db.Conn(ctx)
					require.NoError(t, err)
					defer func() { require.NoError(t, conn.Close()) }()
					_, err = conn.ExecContext(ctx, "create database "+name)
					require.NoError(t, err)

					var count int
					require.NoError(t, conn.QueryRowContext(ctx,
						"select count(*) from mo_catalog.mo_database where datname = ?", name).Scan(&count))
					require.Equal(t, 1, count)
					require.NoError(t, conn.QueryRowContext(ctx,
						"select count(*) from mo_catalog.mo_mysql_compatibility_mode where dat_name = ? and variable_name = 'version_compatibility'", name).Scan(&count))
					require.Equal(t, 1, count)
					// A no-op must neither hang nor append a second metadata row.
					_, err = conn.ExecContext(ctx, "create database if not exists "+name)
					require.NoError(t, err)
					require.NoError(t, conn.QueryRowContext(ctx,
						"select count(*) from mo_catalog.mo_mysql_compatibility_mode where dat_name = ?", name).Scan(&count))
					require.Equal(t, 1, count)
					require.NoError(t, conn.QueryRowContext(ctx, "select 1").Scan(&count))
					require.Equal(t, 1, count)
				})
			}
		}
	})
}

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

	"github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/embed"
)

// 同一连接在 prepare/execute 间切换角色；实际快照由管理员回读，
// 不将展示层过滤或 prepare 时保存的宽权限误认为执行时授权。
func TestPreparedCTASRowPolicies(t *testing.T) {
	// 共享 base cluster 跳过真实用户鉴权，不能证明行策略安全边界。
	// 使用单 CN、保留真实登录鉴权的独立 fixture。
	c, err := embed.NewCluster(embed.WithCNCount(1), embed.WithTesting())
	require.NoError(t, err)
	defer func() { require.NoError(t, c.Close()) }()
	require.NoError(t, c.Start())
	{
		ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
		defer cancel()
		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		addr := fmt.Sprintf("127.0.0.1:%d", cn.GetServiceConfig().CN.Frontend.Port)
		admin, err := sql.Open("mysql", "dump:111@tcp("+addr+")/")
		require.NoError(t, err)
		defer admin.Close()
		adminConn, err := admin.Conn(ctx)
		require.NoError(t, err)
		defer adminConn.Close()
		mustExec(t, ctx, adminConn, "create database prepared_ctas_policy")
		defer func() {
			cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cleanupCancel()
			for _, sql := range []string{
				"drop user if exists prepared_ctas_user",
				"drop role if exists prepared_ctas_narrow",
				"drop role if exists prepared_ctas_wide",
				"drop database if exists prepared_ctas_policy",
			} {
				_, err := adminConn.ExecContext(cleanupCtx, sql)
				require.NoError(t, err)
			}
		}()
		for _, sql := range []string{
			"create table prepared_ctas_policy.source(k int, amount int)",
			"insert into prepared_ctas_policy.source values (1,10),(2,20)",
			"create role prepared_ctas_narrow",
			"create role prepared_ctas_wide",
			`alter role prepared_ctas_narrow add rule "select amount from prepared_ctas_policy.source where k = 1" on table prepared_ctas_policy.source`,
			"create user prepared_ctas_user identified by '123456' default role prepared_ctas_narrow",
			"grant prepared_ctas_wide to prepared_ctas_user",
		} {
			mustExec(t, ctx, adminConn, sql)
		}
		for _, role := range []string{"prepared_ctas_narrow", "prepared_ctas_wide"} {
			mustExec(t, ctx, adminConn, "grant connect on account * to "+role)
			mustExec(t, ctx, adminConn, "grant select on table *.* to "+role)
			mustExec(t, ctx, adminConn, "grant create table, drop table on database prepared_ctas_policy to "+role)
		}
		for _, protocol := range []string{"sql", "binary"} {
			t.Run(protocol, func(t *testing.T) {
				cfg := mysql.NewConfig()
				cfg.User = "sys:prepared_ctas_user:prepared_ctas_narrow"
				cfg.Passwd, cfg.Net, cfg.Addr = "123456", "tcp", addr
				connector, err := mysql.NewConnector(cfg)
				require.NoError(t, err)
				db := sql.OpenDB(connector)
				defer db.Close()
				conn, err := db.Conn(ctx)
				require.NoError(t, err)
				defer conn.Close()
				mustExec(t, ctx, conn, "set enable_remap_hint = 1")
				for _, prepareRole := range []string{"prepared_ctas_narrow", "prepared_ctas_wide"} {
					t.Run(prepareRole, func(t *testing.T) {
						mustExec(t, ctx, conn, "set role "+prepareRole)
						const query = "create table prepared_ctas_policy.snapshot as (select * from prepared_ctas_policy.source)"
						var execute func() error
						if protocol == "binary" {
							stmt, err := conn.PrepareContext(ctx, query)
							require.NoError(t, err)
							defer stmt.Close()
							execute = func() error { _, err := stmt.ExecContext(ctx); return err }
						} else {
							mustExec(t, ctx, conn, "prepare ctas_stmt from '"+query+"'")
							defer mustExec(t, ctx, conn, "deallocate prepare ctas_stmt")
							execute = func() error { _, err := conn.ExecContext(ctx, "execute ctas_stmt"); return err }
						}
						for _, role := range []string{"prepared_ctas_narrow", "prepared_ctas_wide", "prepared_ctas_narrow"} {
							mustExec(t, ctx, conn, "set role "+role)
							wantN, wantTotal := 1, 10
							if role == "prepared_ctas_wide" {
								wantN, wantTotal = 2, 30
							}
							var n, total int
							require.NoError(t, conn.QueryRowContext(ctx, "select count(*), sum(amount) from prepared_ctas_policy.source").Scan(&n, &total))
							require.Equal(t, wantN, n)
							require.Equal(t, wantTotal, total)
							require.NoError(t, execute())
							func() {
								defer mustExec(t, ctx, adminConn, "drop table prepared_ctas_policy.snapshot")
								func() {
									rows, err := adminConn.QueryContext(ctx, "select * from prepared_ctas_policy.snapshot limit 0")
									require.NoError(t, err)
									defer rows.Close()
									cols, err := rows.Columns()
									require.NoError(t, err)
									wantCols := 1
									if role == "prepared_ctas_wide" {
										wantCols = 2
									}
									require.Len(t, cols, wantCols, "列推导也必须使用执行时角色")
									require.NoError(t, rows.Err())
								}()
								require.NoError(t, adminConn.QueryRowContext(ctx, "select count(*), sum(amount) from prepared_ctas_policy.snapshot").Scan(&n, &total))
								require.Equal(t, wantN, n, "执行角色 %s，prepare 角色 %s", role, prepareRole)
								require.Equal(t, wantTotal, total)
								// 目标已存在失败后，同一 prepared handle 仍能按新角色执行。
								require.Error(t, execute())
							}()
						}
					})
				}
				t.Run("explicit hint", func(t *testing.T) {
					// 无角色规则时，用户显式 hint 也必须保留，不能被刷新角色策略吞掉。
					mustExec(t, ctx, conn, "set role prepared_ctas_wide")
					const query = `/*+ {"rewrites":{"prepared_ctas_policy.source":"select amount from prepared_ctas_policy.source where k = 1"}} */ create table prepared_ctas_policy.explicit_snapshot as select * from prepared_ctas_policy.source`
					if protocol == "binary" {
						stmt, err := conn.PrepareContext(ctx, query)
						require.NoError(t, err)
						defer stmt.Close()
						_, err = stmt.ExecContext(ctx)
						require.NoError(t, err)
					} else {
						mustExec(t, ctx, conn, "prepare explicit_stmt from '"+query+"'")
						defer mustExec(t, ctx, conn, "deallocate prepare explicit_stmt")
						mustExec(t, ctx, conn, "execute explicit_stmt")
					}
					defer mustExec(t, ctx, adminConn, "drop table prepared_ctas_policy.explicit_snapshot")
					var n, total int
					require.NoError(t, adminConn.QueryRowContext(ctx, "select count(*), sum(amount) from prepared_ctas_policy.explicit_snapshot").Scan(&n, &total))
					require.Equal(t, 1, n)
					require.Equal(t, 10, total)
				})
			})
		}
	}
}

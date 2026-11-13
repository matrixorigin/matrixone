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
				"drop database if exists prepared_ctas_other",
				"drop database if exists prepared_ctas_policy",
			} {
				_, err := adminConn.ExecContext(cleanupCtx, sql)
				require.NoError(t, err)
			}
		}()
		for _, sql := range []string{
			"create table prepared_ctas_policy.source(k int, amount int)",
			"insert into prepared_ctas_policy.source values (1,10),(2,20)",
			"create database prepared_ctas_other",
			"create table prepared_ctas_other.source(k int, amount int)",
			"insert into prepared_ctas_other.source values (9,900)",
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
		mustExec(t, ctx, adminConn, "grant create table, drop table on database prepared_ctas_other to prepared_ctas_wide")
		for _, protocol := range []string{"sql", "direct", "binary"} {
			t.Run(protocol, func(t *testing.T) {
				cfg := mysql.NewConfig()
				cfg.User = "sys:prepared_ctas_user:prepared_ctas_narrow"
				cfg.Passwd, cfg.Net, cfg.Addr = "123456", "tcp", addr
				cfg.MultiStatements = protocol == "direct"
				connector, err := mysql.NewConnector(cfg)
				require.NoError(t, err)
				db := sql.OpenDB(connector)
				defer db.Close()
				conn, err := db.Conn(ctx)
				require.NoError(t, err)
				defer conn.Close()
				mustExec(t, ctx, conn, "set enable_remap_hint = 1")
				prepare := func(t *testing.T, name, query string) func() error {
					t.Helper()
					if protocol == "binary" {
						stmt, err := conn.PrepareContext(ctx, query)
						require.NoError(t, err)
						t.Cleanup(func() { require.NoError(t, stmt.Close()) })
						return func() error { _, err := stmt.ExecContext(ctx); return err }
					}
					text := "prepare " + name + " from '" + query + "'"
					if protocol == "direct" {
						text = "/* from misleading prefix */ prepare `" + name + "` from " + query
					}
					mustExec(t, ctx, conn, text)
					t.Cleanup(func() { mustExec(t, ctx, conn, "deallocate prepare "+name) })
					return func() error { _, err := conn.ExecContext(ctx, "execute "+name); return err }
				}
				for _, prepareRole := range []string{"prepared_ctas_narrow", "prepared_ctas_wide"} {
					t.Run(prepareRole, func(t *testing.T) {
						mustExec(t, ctx, conn, "set role "+prepareRole)
						const query = "create table prepared_ctas_policy.snapshot as (select * from prepared_ctas_policy.source)"
						execute := prepare(t, "ctas_stmt", query)
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
					execute := prepare(t, "explicit_stmt", query)
					require.NoError(t, execute())
					defer mustExec(t, ctx, adminConn, "drop table prepared_ctas_policy.explicit_snapshot")
					var n, total int
					require.NoError(t, adminConn.QueryRowContext(ctx, "select count(*), sum(amount) from prepared_ctas_policy.explicit_snapshot").Scan(&n, &total))
					require.Equal(t, 1, n)
					require.Equal(t, 10, total)
				})
				t.Run("prepare database", func(t *testing.T) {
					mustExec(t, ctx, conn, "set role prepared_ctas_wide")
					mustExec(t, ctx, conn, "use prepared_ctas_policy")
					execute := prepare(t, "db_stmt", "create table snapshot as select * from source")
					mustExec(t, ctx, conn, "use prepared_ctas_other")
					defer mustExec(t, ctx, adminConn, "drop table if exists prepared_ctas_policy.snapshot")
					defer mustExec(t, ctx, adminConn, "drop table if exists prepared_ctas_other.snapshot")
					for _, role := range []string{"prepared_ctas_wide", "prepared_ctas_narrow"} {
						mustExec(t, ctx, conn, "set role "+role)
						require.NoError(t, execute())
						var n, total int
						require.NoError(t, adminConn.QueryRowContext(ctx, "select count(*), sum(amount) from prepared_ctas_policy.snapshot").Scan(&n, &total))
						wantN, wantTotal := 2, 30
						if role == "prepared_ctas_narrow" {
							// 窄角色没有 other 库的 CREATE 权限，鉴权也必须绑定原库。
							wantN, wantTotal = 1, 10
						}
						require.Equal(t, wantN, n)
						require.Equal(t, wantTotal, total, "未限定源表也必须从 prepare 库读取")
						require.Error(t, execute(), "已有目标报错后仍保持当前库")
						var db string
						require.NoError(t, conn.QueryRowContext(ctx, "select database()").Scan(&db))
						require.Equal(t, "prepared_ctas_other", db)
						var currentTotal int
						require.NoError(t, conn.QueryRowContext(ctx, "select sum(amount) from source").Scan(&currentTotal))
						require.Equal(t, 900, currentTotal, "普通查询的 compiler context 也必须恢复当前库")
						mustExec(t, ctx, adminConn, "drop table prepared_ctas_policy.snapshot")
					}
					var misplaced int
					require.NoError(t, adminConn.QueryRowContext(ctx, "select count(*) from mo_catalog.mo_tables where reldatabase='prepared_ctas_other' and relname='snapshot'").Scan(&misplaced))
					require.Zero(t, misplaced)
					func() {
						mustExec(t, ctx, adminConn, "rename table prepared_ctas_policy.source to prepared_ctas_policy.saved_source")
						defer mustExec(t, ctx, adminConn, "rename table prepared_ctas_policy.saved_source to prepared_ctas_policy.source")
						require.Error(t, execute(), "原库缺少源表时不能回退读取当前库")
						var currentTotal int
						require.NoError(t, conn.QueryRowContext(ctx, "select sum(amount)+1 from source").Scan(&currentTotal))
						require.Equal(t, 901, currentTotal, "重新规划报错也必须恢复 compiler context；新查询避免复用旧 plan")
					}()
					// 显式跨库目标不改变源表的 prepare-time 默认库。
					mustExec(t, ctx, conn, "set role prepared_ctas_wide")
					mustExec(t, ctx, conn, "use prepared_ctas_policy")
					qualified := prepare(t, "qualified_stmt", "create table prepared_ctas_other.qualified_snapshot as select * from source")
					mustExec(t, ctx, conn, "use prepared_ctas_other")
					require.NoError(t, qualified())
					defer mustExec(t, ctx, adminConn, "drop table prepared_ctas_other.qualified_snapshot")
					var total int
					require.NoError(t, adminConn.QueryRowContext(ctx, "select sum(amount) from prepared_ctas_other.qualified_snapshot").Scan(&total))
					require.Equal(t, 30, total)
				})
				if protocol == "direct" {
					t.Run("batch raw source", func(t *testing.T) {
						mustExec(t, ctx, conn, "set role prepared_ctas_narrow")
						defer mustExec(t, ctx, adminConn, "drop table if exists prepared_ctas_policy.batch_snapshot")
						mustExec(t, ctx, conn, "select 1; /* from misleading */ prepare batch_stmt from create table prepared_ctas_policy.batch_snapshot as select * from prepared_ctas_policy.source; execute batch_stmt")
						t.Cleanup(func() { mustExec(t, ctx, conn, "deallocate prepare batch_stmt") })
						var n, total int
						require.NoError(t, adminConn.QueryRowContext(ctx, "select count(*), sum(amount) from prepared_ctas_policy.batch_snapshot").Scan(&n, &total))
						require.Equal(t, 1, n)
						require.Equal(t, 10, total)
					})
				}
			})
		}
	}
}

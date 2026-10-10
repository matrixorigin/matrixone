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

package multicn

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/clusterservice"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/stretchr/testify/require"
)

func TestDatabaseDefaultsTwoCNPreparedFreshness(t *testing.T) {
	cluster, err := embed.StartTestCluster(embed.WithCNCount(2), embed.WithPreStart(func(op embed.ServiceOperator) {
		if op.ServiceType() == metadata.ServiceType_CN {
			op.Adjust(func(cfg *embed.ServiceConfig) { cfg.CN.Txn.Mode = txn.TxnMode_Pessimistic.String() })
		}
	}))
	if cluster != nil {
		t.Cleanup(func() { require.NoError(t, cluster.Close()) })
	}
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(t.Context(), 120*time.Second)
	t.Cleanup(cancel)
	open := func(index int) *sql.Conn {
		t.Helper()
		cn, err := cluster.GetCNService(index)
		require.NoError(t, err)
		db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", cn.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, db.Close()) })
		conn, err := db.Conn(ctx)
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, conn.Close()) })
		return conn
	}
	writer, reader, competingWriter := open(0), open(1), open(1)
	exec := func(conn *sql.Conn, statement string) {
		t.Helper()
		_, err := conn.ExecContext(ctx, statement)
		require.NoError(t, err, statement)
	}
	// 使用公开 RPC 控制路径模拟混合升级期的协商协议下限；尚未提升
	// 下限时两个 CN 都必须拒绝 C03 写入，普通历史 DDL 仍可执行。
	func() {
		firstCN, err := cluster.GetCNService(0)
		require.NoError(t, err)
		secondCN, err := cluster.GetCNService(1)
		require.NoError(t, err)
		targets := firstCN.ServiceID() + "," + secondCN.ServiceID()
		// SQL ready 不等于周期刷新的服务清单已收录第二个 CN。先通过
		// 清单所有者取得当前拓扑，再使用不重试的公开版本控制 RPC。
		clusterservice.GetMOCluster(firstCN.ServiceID()).ForceRefresh(true)
		setProtocol := func(version int) {
			exec(writer, fmt.Sprintf("select mo_ctl('cn','setprotocolversion','%s:%d')", targets, version))
		}
		setProtocol(109)
		defer setProtocol(110)
		for _, conn := range []*sql.Conn{writer, reader} {
			_, err := conn.ExecContext(ctx, "create database rejected_rollout collate utf8mb4_bin")
			require.ErrorContains(t, err, "protocol version 110")
		}
		exec(writer, "create database legacy_rollout")
		defer exec(writer, "drop database legacy_rollout")
		exec(writer, "create table legacy_rollout.t(v varchar(8))")
		_, err = reader.ExecContext(ctx, "alter table legacy_rollout.t convert to character set binary")
		require.ErrorContains(t, err, "protocol version 110")
	}()
	exec(writer, "create database defaults_two_cn collate utf8mb4_bin")
	t.Cleanup(func() {
		cleanup, stop := context.WithTimeout(context.Background(), 30*time.Second)
		defer stop()
		_, err := writer.ExecContext(cleanup, "drop database if exists defaults_two_cn")
		require.NoError(t, err)
	})
	checkColumn := func(conn *sql.Conn, table, want string) {
		t.Helper()
		var column, tableDefault string
		require.NoError(t, conn.QueryRowContext(ctx,
			"select collation_name from information_schema.columns where table_schema='defaults_two_cn' and table_name=? and column_name='v'", table).Scan(&column))
		require.NoError(t, conn.QueryRowContext(ctx,
			"select table_collation from information_schema.tables where table_schema='defaults_two_cn' and table_name=?", table).Scan(&tableDefault))
		require.Equal(t, want, column)
		require.Equal(t, want, tableDefault)
	}
	for _, protocol := range []string{"text", "binary"} {
		t.Logf("prepared protocol=%s", protocol)
		func() {
			exec(writer, "alter database defaults_two_cn collate utf8mb4_bin")
			table := "prepared_" + protocol
			statement := "create table defaults_two_cn." + table + " (v varchar(8))"
			var prepared *sql.Stmt
			if protocol == "text" {
				exec(reader, "prepare inherited_default from "+statement)
				defer exec(reader, "deallocate prepare inherited_default")
			} else {
				prepared, err = reader.PrepareContext(ctx, statement)
				require.NoError(t, err)
				defer func() { require.NoError(t, prepared.Close()) }()
			}
			exec(writer, "alter database defaults_two_cn collate utf8mb4_unicode_ci")
			if prepared == nil {
				exec(reader, "execute inherited_default")
			} else {
				_, err = prepared.ExecContext(ctx)
				require.NoError(t, err)
			}
			for _, conn := range []*sql.Conn{writer, reader} {
				checkColumn(conn, table, "utf8mb4_unicode_ci")
			}
			// 实际比较必须来自执行时依赖，而非仅改展示。
			exec(reader, "insert into defaults_two_cn."+table+" values ('a'),('A')")
			var matched int
			require.NoError(t, writer.QueryRowContext(ctx,
				"select count(*) from defaults_two_cn."+table+" where v='a'").Scan(&matched))
			require.Equal(t, 2, matched)
		}()
	}

	// 两个真实连接同时提交不同域；代际必须连续更新，不能丢失一次成功写入。
	var before uint64
	versionSQL := "select version from mo_catalog.mo_database_defaults where account_id=0 and database_id=" +
		"(select dat_id from mo_catalog.mo_database where account_id=0 and datname='defaults_two_cn')"
	require.NoError(t, writer.QueryRowContext(ctx, versionSQL).Scan(&before))
	ready := make(chan struct{}, 2)
	start := make(chan struct{})
	done := make(chan error, 2)
	for index, conn := range []*sql.Conn{writer, competingWriter} {
		collation := []string{"utf8mb4_bin", "utf8mb4_general_ci"}[index]
		go func() {
			ready <- struct{}{}
			select {
			case <-start:
				_, err := conn.ExecContext(ctx, "alter database defaults_two_cn collate "+collation)
				done <- err
			case <-ctx.Done():
				done <- ctx.Err()
			}
		}()
	}
	for range 2 {
		select {
		case <-ready:
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}
	close(start)
	var writerErrors []error
	for range 2 {
		select {
		case err := <-done:
			writerErrors = append(writerErrors, err)
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}
	for _, err := range writerErrors {
		require.NoError(t, err)
	}
	var after uint64
	require.NoError(t, reader.QueryRowContext(ctx, versionSQL).Scan(&after))
	require.Equal(t, before+2, after)
	exec(reader, "create table defaults_two_cn.after_writers (v varchar(8))")
	var finalDefault string
	require.NoError(t, reader.QueryRowContext(ctx,
		"select default_collation_name from information_schema.schemata where schema_name='defaults_two_cn'").Scan(&finalDefault))
	require.Contains(t, []string{"utf8mb4_bin", "utf8mb4_general_ci"}, finalDefault)
	checkColumn(writer, "after_writers", finalDefault)

	// 已持有数据库锁是精确阶段边界；观察真实 waiter 后取消，不靠 sleep
	// 猜测 SQL 是否阻塞。取消后锁必须撤离且代际/默认值不发生变化。
	func() {
		waiter := open(1)
		var waiterID uint64
		require.NoError(t, waiter.QueryRowContext(ctx, "select connection_id()").Scan(&waiterID))
		exec(writer, "begin")
		defer exec(writer, "rollback")
		exec(writer, "alter database defaults_two_cn collate utf8mb4_unicode_ci")
		waitCtx, stopWait := context.WithCancel(ctx)
		defer stopWait()
		finished := make(chan error, 1)
		go func() {
			_, err := waiter.ExecContext(waitCtx, "alter database defaults_two_cn collate utf8mb4_bin")
			finished <- err
		}()
		waiters := func() int {
			var count int
			err := reader.QueryRowContext(ctx, fmt.Sprintf("select count(*) from mo_locks() l where l.table_id='%d' and l.lock_wait<>''", catalog.MO_DATABASE_ID)).Scan(&count)
			if err != nil {
				t.Logf("lock observation: %v", err)
				return -1
			}
			return count
		}
		require.Eventually(t, func() bool { return waiters() > 0 }, 10*time.Second, 20*time.Millisecond)
		// KILL QUERY 取消服务器持有的 statement context。仅关闭客户端 TCP
		// 不构成服务器已观察取消的阶段证明（响应前服务器未必在读连接）。
		exec(reader, fmt.Sprintf("kill query %d", waiterID))
		select {
		case err := <-finished:
			require.Error(t, err)
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
		require.Eventually(t, func() bool { return waiters() == 0 }, 10*time.Second, 20*time.Millisecond)
	}()
	var unchanged uint64
	require.NoError(t, reader.QueryRowContext(ctx, versionSQL).Scan(&unchanged))
	require.Equal(t, after, unchanged)
	var unchangedDefault string
	require.NoError(t, reader.QueryRowContext(ctx, "select default_collation_name from information_schema.schemata where schema_name='defaults_two_cn'").Scan(&unchangedDefault))
	require.Equal(t, finalDefault, unchangedDefault)
	exec(reader, "alter database defaults_two_cn collate utf8mb4_unicode_ci")

	// 同名替换改变数据库 identity；旧 prepared 计划不得沿用旧对象/默认值。
	exec(reader, "prepare recreated_default from create table defaults_two_cn.after_recreate (v varchar(8))")
	defer exec(reader, "deallocate prepare recreated_default")
	exec(writer, "drop database defaults_two_cn")
	exec(writer, "create database defaults_two_cn collate utf8mb4_unicode_ci")
	exec(reader, "execute recreated_default")
	checkColumn(writer, "after_recreate", "utf8mb4_unicode_ci")
}

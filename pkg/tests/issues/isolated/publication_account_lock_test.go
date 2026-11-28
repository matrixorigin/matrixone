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

package isolated

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/google/uuid"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/cnservice"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/lockservice"
	"github.com/matrixorigin/matrixone/pkg/pb/lock"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/tests/testutils"
	"github.com/stretchr/testify/require"
)

func startAccountLockCluster(t *testing.T, cnCount int, configs ...lockservice.Config) embed.Cluster {
	t.Helper()
	nextCN := 0
	cluster, err := embed.StartTestCluster(embed.WithCNCount(cnCount), embed.WithPreStart(func(s embed.ServiceOperator) {
		if s.ServiceType() == metadata.ServiceType_CN {
			locks := configs[nextCN]
			nextCN++
			s.Adjust(func(cfg *embed.ServiceConfig) {
				cfg.CN.LockService.MaxLockRowCount = locks.MaxLockRowCount
				cfg.CN.LockService.MaxFixedSliceSize = locks.MaxFixedSliceSize
				cfg.CN.LockService.EnableRemoteLocalProxy = locks.EnableRemoteLocalProxy
				cfg.CN.SQLAddress = fmt.Sprintf("127.0.0.1:%d", cfg.CN.Frontend.Port)
				cfg.CN.Frontend.SkipCheckUser = false
			})
		}
	}))
	if cluster != nil {
		t.Cleanup(func() { require.NoError(t, cluster.Close()) })
	}
	require.NoError(t, err)
	return cluster
}

func openAccountLockSQL(t *testing.T, cluster embed.Cluster, index int) (*sql.DB, string) {
	t.Helper()
	cn, err := cluster.GetCNService(index)
	require.NoError(t, err)
	db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", cn.GetServiceConfig().CN.Frontend.Port))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	return db, cn.ServiceID()
}

// A different CN can pick a snapshot before the writer's commit is applied.
func waitAccountLockCommit(t *testing.T, cluster embed.Cluster, writerIndex, readerIndex int) {
	t.Helper()
	writer, err := cluster.GetCNService(writerIndex)
	require.NoError(t, err)
	reader, err := cluster.GetCNService(readerIndex)
	require.NoError(t, err)

	commit := writer.RawService().(cnservice.Service).GetTxnClient().GetLatestCommitTS()
	require.False(t, commit.IsEmpty())
	testutils.WaitLogtailApplied(t, commit, reader)
}

func TestDropAccountWithMinimumRowLockCount(t *testing.T) {
	// Force accumulated metadata keys to span multiple lock-service segments.
	cluster := startAccountLockCluster(t, 1, lockservice.Config{MaxLockRowCount: 1, MaxFixedSliceSize: 4})
	db, cn := openAccountLockSQL(t, cluster, 0)
	ctx := t.Context()
	exec := func(stmt string) {
		t.Helper()
		_, err := db.ExecContext(ctx, stmt)
		require.NoError(t, err, stmt)
	}
	count := func(query string) int {
		t.Helper()
		var n int
		require.NoError(t, db.QueryRowContext(ctx, query).Scan(&n))
		return n
	}

	exec("create account direct_drop admin_name 'admin' identified by '111'")
	func() {
		// Pin the catalog entry for mo_account in another transaction. DROP
		// first shares this point, then its final DELETE also locks the
		// metadata for the unique index.
		packer := types.NewPacker()
		defer packer.Close()
		packer.EncodeUint32(0)
		packer.EncodeStringType([]byte(catalog.MO_CATALOG))
		packer.EncodeStringType([]byte("mo_account"))
		serial := packer.Bytes()
		packer.Reset()
		packer.EncodeStringType(serial)
		service := lockservice.GetLockServiceByServiceID(cn)
		holder := uuid.New()
		defer func() { require.NoError(t, service.Unlock(context.Background(), holder[:], timestamp.Timestamp{})) }()
		_, err := service.Lock(ctx, catalog.MO_TABLES_ID, [][]byte{packer.Bytes()}, holder[:], lock.LockOptions{
			Mode: lock.LockMode_Shared, Granularity: lock.Granularity_Row,
		})
		require.NoError(t, err)
		exec("drop account direct_drop")
	}()
	require.Zero(t, count("select count(*) from mo_catalog.mo_account where account_name='direct_drop'"))

	// Cluster restore drops accounts created after the snapshot inside its own transaction.
	exec("create database restore_scope")
	exec("create table restore_scope.t (id int primary key)")
	exec("insert into restore_scope.t values (1)")
	exec("create snapshot before_account for cluster")
	exec("create account restore_drop admin_name 'admin' identified by '111'")
	exec("update restore_scope.t set id=2 where id=1")
	exec("restore cluster {snapshot='before_account'}")
	require.Zero(t, count("select count(*) from mo_catalog.mo_account where account_name='restore_drop'"))
	require.Equal(t, 1, count("select id from restore_scope.t"))
}

func TestAlterPublicationExplicitTargetLockScope(t *testing.T) {
	const budget = 16
	locks := lockservice.Config{MaxLockRowCount: budget}
	cluster := startAccountLockCluster(t, 2, locks, locks)
	left, leftID := openAccountLockSQL(t, cluster, 0)
	right, rightID := openAccountLockSQL(t, cluster, 1)
	ctx := t.Context()
	exec := func(t *testing.T, stmt string) {
		t.Helper()
		_, err := left.ExecContext(ctx, stmt)
		require.NoError(t, err, stmt)
	}
	number := func(t *testing.T, query string, args ...any) int {
		t.Helper()
		var n int
		require.NoError(t, left.QueryRowContext(ctx, query, args...).Scan(&n))
		return n
	}
	account := func(name string) {
		exec(t, "create account "+name+" admin_name 'admin' identified by '111'")
	}
	exec(t, "create database scope_db")
	targets := []string{"scope_first"}
	account(targets[0])
	account("scope_gap")
	for i := range budget {
		name := fmt.Sprintf("scope_target_%d", i)
		account(name)
		targets = append(targets, name)
	}
	account("scope_other")
	const accountID = "select account_id from mo_catalog.mo_account where account_name=?"
	gapID := number(t, accountID, "scope_gap")
	require.Less(t, number(t, accountID, targets[0]), gapID)
	require.Less(t, gapID, number(t, accountID, targets[budget]))

	for _, tc := range []struct {
		name      string
		targets   []string
		unrelated string
	}{
		{"wide", targets, "scope_gap"},
		{"singleton", targets[:1], "scope_other"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			pub := "scope_" + tc.name
			target := tc.targets[len(tc.targets)-1]
			deletedID := number(t, accountID, target)
			exec(t, "create publication "+pub+" database scope_db account scope_first")
			// ALTER runs on the other CN and must see the committed publication.
			var synced string
			require.NoError(t, left.QueryRowContext(t.Context(), "select mo_ctl('cn','syncCommit','')").Scan(&synced))
			require.Contains(t, synced, "sync 2 cn services")
			ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
			var wg sync.WaitGroup
			defer wg.Wait()
			defer cancel()
			start := func(db *sql.DB, stmt string) <-chan error {
				result := make(chan error, 1)
				wg.Go(func() {
					_, err := db.ExecContext(ctx, stmt)
					result <- err
				})
				return result
			}
			// Hold ALTER at its publication write, after it validated the recipients.
			gate, err := left.BeginTx(ctx, nil)
			require.NoError(t, err)
			defer gate.Rollback()
			var locked string
			require.NoError(t, gate.QueryRowContext(ctx, "select pub_name from mo_catalog.mo_pubs where account_id=0 and pub_name=? for update", pub).Scan(&locked))
			require.Equal(t, pub, locked)
			waiting := func(op <-chan error, cnID, table string) {
				t.Helper()
				waitCtx, stop := context.WithTimeout(ctx, 10*time.Second)
				defer stop()
				tick := time.NewTicker(30 * time.Millisecond)
				defer tick.Stop()
				for {
					var n int
					err := left.QueryRowContext(waitCtx, `select count(*) from mo_transactions() t where cn_id=? and table_id in
						(select cast(rel_id as char) from mo_catalog.mo_tables where account_id=0 and reldatabase='mo_catalog' and relname=?)`, cnID, table).Scan(&n)
					require.NoError(t, err)
					if n > 0 {
						return
					}
					select {
					case err := <-op:
						t.Fatalf("DDL returned %v before waiting on %s", err, table)
					case <-waitCtx.Done():
						t.Fatalf("%s did not wait on %s: %v", cnID, table, waitCtx.Err())
					case <-tick.C:
					}
				}
			}
			// An ordinary shared range that predates ALTER must disappear with its
			// holder. Otherwise ALTER can inherit it and block the gap account.
			var covering *sql.Tx
			if tc.name == "wide" {
				covering, err = left.BeginTx(ctx, nil)
				require.NoError(t, err)
				defer covering.Rollback()
				rows, err := covering.QueryContext(ctx, "select account_id from mo_catalog.mo_account for share")
				require.NoError(t, err)
				defer rows.Close()
				var count int
				for rows.Next() {
					var id int
					require.NoError(t, rows.Scan(&id))
					count++
				}
				require.NoError(t, rows.Err())
				require.NoError(t, rows.Close())
				require.Greater(t, count, budget)
				require.Positive(t, number(t, `select count(*) from mo_locks() l where l.lock_key='range' and lower(l.lock_mode)='shared' and l.table_id in
					(select cast(rel_id as char) from mo_catalog.mo_tables where account_id=0 and reldatabase='mo_catalog' and relname='mo_account')`))
			}
			alter := start(right, "alter publication "+pub+" account "+strings.Join(tc.targets, ","))
			if covering != nil {
				waiting(alter, rightID, "mo_account")
				require.NoError(t, covering.Rollback())
			}
			waiting(alter, rightID, "mo_pubs")
			_, err = left.ExecContext(ctx, "drop account "+tc.unrelated)
			require.NoError(t, err, "unrelated DROP must commit before releasing ALTER")

			drop := start(left, "drop account "+target)
			waiting(drop, leftID, "mo_account")
			require.NoError(t, gate.Rollback())
			require.NoError(t, <-alter)
			require.NoError(t, <-drop)
			const grants = "select count(*) from mo_catalog.mo_subs where pub_account_id=0 and pub_name=?"
			require.Zero(t, number(t, grants+" and sub_account_id=?", pub, deletedID))
			require.Equal(t, len(tc.targets)-1, number(t, grants+" and status=0", pub))
		})
	}
}

func TestCreatePublicationBeyondLockSliceCapacity(t *testing.T) {
	const maxSlice = 16
	locks := lockservice.Config{MaxLockRowCount: 4, MaxFixedSliceSize: maxSlice}
	// Constrain the publishing CN so the named grant crosses a lock-key segment.
	// DROP ACCOUNT also cleans up system tables using ordinary range locks.
	cluster := startAccountLockCluster(t, 1, lockservice.Config{}, locks)
	owner, _ := openAccountLockSQL(t, cluster, 0)
	ctx := t.Context()
	_, err := owner.ExecContext(ctx, "create database wide_db")
	require.NoError(t, err)
	names := make([]string, maxSlice+1)
	for i := range names {
		names[i] = fmt.Sprintf("recipient_%03d", i)
		_, err := owner.ExecContext(ctx, "create account "+names[i]+" admin_name 'admin' identified by '111'")
		require.NoError(t, err)
	}
	// Creating the accounts before starting the publisher puts mo_account on
	// the first CN and exercises remote lock bookkeeping on the publisher.
	require.NoError(t, cluster.StartNewCNService(1))
	publisher, _ := openAccountLockSQL(t, cluster, 1)
	_, err = publisher.ExecContext(ctx, "create publication wide_pub database wide_db account "+strings.Join(names, ","))
	require.NoError(t, err)
	waitAccountLockCommit(t, cluster, 1, 0)
	rows, err := owner.QueryContext(ctx, `select sub_account_name from mo_catalog.mo_subs where pub_account_id=0 and pub_name='wide_pub' and status=0 order by sub_account_name`)
	require.NoError(t, err)
	defer rows.Close()
	var granted []string
	for rows.Next() {
		var name string
		require.NoError(t, rows.Scan(&name))
		granted = append(granted, name)
	}
	require.NoError(t, rows.Err())
	require.Equal(t, names, granted)
	// The other CN must be able to delete keys from both held-key segments.
	dropCtx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	for _, name := range []string{names[0], names[maxSlice]} {
		_, err = owner.ExecContext(dropCtx, "drop account "+name)
		require.NoError(t, err)
	}
	var remaining int
	require.NoError(t, owner.QueryRowContext(ctx, "select count(*) from mo_catalog.mo_subs where pub_account_id=0 and pub_name='wide_pub' and status=0").Scan(&remaining))
	require.Equal(t, len(names)-2, remaining)
}

func TestOrdinaryUpdateSharedMetadataRange(t *testing.T) {
	cluster := startAccountLockCluster(t, 1, lockservice.Config{MaxLockRowCount: 4, MaxFixedSliceSize: 16})
	db, _ := openAccountLockSQL(t, cluster, 0)
	ctx := t.Context()
	_, err := db.ExecContext(ctx, "create database meta_range")
	require.NoError(t, err)
	tables := make([]string, 17)
	for i := range tables {
		name := fmt.Sprintf("meta_range.t%02d", i)
		tables[i] = name
		_, err = db.ExecContext(ctx, "create table "+name+" (id int primary key, v int)")
		require.NoError(t, err)
		_, err = db.ExecContext(ctx, "insert into "+name+" values (1,1), (2,1)")
		require.NoError(t, err)
	}
	update := func(target string, sources []string, id int) string {
		parts := make([]string, len(sources))
		for i, source := range sources {
			parts[i] = "select v from " + source
		}
		return fmt.Sprintf("update %s set v=v+1 where id=%d and (select sum(v) from (%s) s)>0",
			target, id, strings.Join(parts, " union all "))
	}
	first, err := db.BeginTx(ctx, nil)
	require.NoError(t, err)
	defer first.Rollback()
	// Both requests have the same metadata endpoints. Their writes target
	// different tables so only shared metadata admission is under test.
	_, err = first.ExecContext(ctx, update(tables[0], []string{tables[1], tables[2], tables[3], tables[16]}, 1))
	require.NoError(t, err)
	second, err := db.BeginTx(ctx, nil)
	require.NoError(t, err)
	defer second.Rollback()
	updateCtx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	_, err = second.ExecContext(updateCtx, update(tables[16], tables[:16], 2))
	require.NoError(t, err)
	require.NoError(t, second.Commit())
	require.NoError(t, first.Commit())
	var a, b int
	require.NoError(t, db.QueryRowContext(ctx, "select a.v,b.v from meta_range.t00 a,meta_range.t16 b where a.id=1 and b.id=2").Scan(&a, &b))
	require.Equal(t, 2, a)
	require.Equal(t, 2, b)
}

func TestRestoreClusterRemotePublicationLocks(t *testing.T) {
	cluster := startAccountLockCluster(t, 1, lockservice.Config{}, lockservice.Config{EnableRemoteLocalProxy: true})
	owner, ownerCN := openAccountLockSQL(t, cluster, 0)
	ctx := t.Context()
	exec := func(db *sql.DB, statement string) {
		t.Helper()
		_, err := db.ExecContext(ctx, statement)
		require.NoError(t, err, statement)
	}
	exec(owner, "create account restore_sub admin_name 'admin' identified by '111'")
	exec(owner, "create database restore_pub_db")
	exec(owner, "create table restore_pub_db.t (id int primary key)")
	exec(owner, "create publication a_all database restore_pub_db account all")
	exec(owner, "create publication b_named database restore_pub_db account restore_sub")
	exec(owner, "create snapshot pub_lock_snapshot for cluster")
	exec(owner, "drop publication a_all")
	exec(owner, "drop publication b_named")

	var tableID uint64
	require.NoError(t, owner.QueryRowContext(ctx, "select rel_id from mo_catalog.mo_tables where account_id=0 and reldatabase='mo_catalog' and relname='mo_account'").Scan(&tableID))
	ls := lockservice.GetLockServiceByServiceID(ownerCN)
	bind, err := ls.GetLockTableBind(0, tableID)
	require.NoError(t, err)
	require.Equal(t, ls.GetServiceID(), bind.ServiceID)
	// Start the proxy CN after the first CN owns mo_account. Starting them
	// together lets either CN win the catalog binding during bootstrap.
	require.NoError(t, cluster.StartNewCNService(1))
	restore, restoreCN := openAccountLockSQL(t, cluster, 1)
	require.True(t, lockservice.GetLockServiceByServiceID(restoreCN).GetConfig().EnableRemoteLocalProxy)
	exec(restore, "restore cluster {snapshot='pub_lock_snapshot'}")
	waitAccountLockCommit(t, cluster, 1, 0)
	var pubs, grants int
	require.NoError(t, owner.QueryRowContext(ctx, "select count(*) from mo_catalog.mo_pubs where account_id=0 and database_name='restore_pub_db' and pub_name in ('a_all','b_named')").Scan(&pubs))
	require.NoError(t, owner.QueryRowContext(ctx, "select count(distinct pub_name) from mo_catalog.mo_subs where pub_account_id=0 and pub_name in ('a_all','b_named') and sub_account_name='restore_sub' and status=0").Scan(&grants))
	require.Equal(t, 2, pubs)
	require.Equal(t, 2, grants)

	exec(restore, "drop account restore_sub")
	var accounts int
	require.NoError(t, restore.QueryRowContext(ctx, "select count(*) from mo_catalog.mo_account where account_name='restore_sub'").Scan(&accounts))
	require.Zero(t, accounts)
}

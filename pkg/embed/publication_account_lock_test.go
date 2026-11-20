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

package embed

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/stretchr/testify/require"
)

func TestCreatePublicationExplicitTargetLockScope(t *testing.T) {
	const lockBudget = 16
	// This fixture needs a small row-lock threshold. Release any idle shared
	// fixture before starting a cluster with that configuration.
	require.NoError(t, CloseBaseClusterTests())
	require.NoError(t, CloseSingleCNBaseClusterTests())
	c, err := StartTestCluster(WithCNCount(2), WithTestAdmissionContext(t.Context()), WithPreStart(func(s ServiceOperator) {
		adjustBasicClusterService(s)
		if s.ServiceType() == metadata.ServiceType_CN {
			s.Adjust(func(cfg *ServiceConfig) {
				cfg.CN.LockService.MaxLockRowCount = lockBudget
			})
		}
	}))
	if c != nil {
		t.Cleanup(func() { require.NoError(t, c.Close()) })
	}
	require.NoError(t, err)

	fixture := newPublicationAccountSQL(t, c)
	defer fixture.stopStatements()
	fixture.exec(fixture.left, "create database pub_lock_src")
	fixture.createAccount("pub_lock_first")
	// The unrelated account falls inside the span of the wide list, so a
	// covering range and a full table lock both fail the DROP oracle.
	fixture.createAccount("pub_lock_unrelated_wide")
	targets := []string{"pub_lock_first"}
	for i := 1; i <= lockBudget; i++ {
		name := fmt.Sprintf("pub_lock_target_%02d", i)
		fixture.createAccount(name)
		targets = append(targets, name)
	}
	fixture.createAccount("pub_lock_unrelated_single")
	accountID := func(name string) int {
		return fixture.number(fixture.left, "select account_id from mo_catalog.mo_account where account_name=?", name)
	}
	unrelatedID := accountID("pub_lock_unrelated_wide")
	require.Less(t, accountID(targets[0]), unrelatedID)
	require.Less(t, unrelatedID, accountID(targets[len(targets)-1]))

	t.Run("existing publication during recipient drop", func(t *testing.T) {
		f := newPublicationAccountSQL(t, c)
		defer f.stopStatements()
		f.exec(f.left, "create publication pub_lock_existing database pub_lock_src account pub_lock_first")
		f.createAccount("pub_lock_unused")
		id := f.number(f.left, "select account_id from mo_catalog.mo_account where account_name='pub_lock_unused'")
		gate, err := f.left.BeginTx(f.ctx, nil)
		require.NoError(t, err)
		defer gate.Rollback()
		var locked int
		require.NoError(t, gate.QueryRowContext(f.ctx, "select account_id from mo_catalog.mo_account where account_id=? for update", id).Scan(&locked))
		require.Equal(t, id, locked)
		drop := f.start(f.left, "drop account pub_lock_unused")
		waited, err := f.waitForLockOrResult(drop, 0, "mo_account")
		require.NoError(t, err)
		require.True(t, waited)

		noop := f.start(f.right, "create publication if not exists pub_lock_existing database pub_lock_src account pub_lock_unused")
		waited, err = f.waitForLockOrResult(noop, 1, "mo_account")
		require.NoError(t, err)
		require.False(t, waited, "an existing publication must return before the recipient DROP is unblocked")
		require.NoError(t, gate.Rollback())
		require.NoError(t, <-drop)
		require.Equal(t, 1, f.number(f.left, "select count(*) from mo_catalog.mo_subs where pub_name='pub_lock_existing' and sub_account_name='pub_lock_first' and status=0"))
		require.Zero(t, f.number(f.left, "select count(*) from mo_catalog.mo_subs where pub_name='pub_lock_existing' and sub_account_id=?", id))
	})

	for _, tc := range []struct {
		name      string
		create    string
		targets   []string
		unrelated string
	}{
		{name: "wide", create: "create publication", targets: targets, unrelated: "pub_lock_unrelated_wide"},
		{name: "singleton", create: "create publication if not exists", targets: targets[:1], unrelated: "pub_lock_unrelated_single"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newPublicationAccountSQL(t, c)
			defer f.stopStatements()

			// Drop the last target in each case. The first also belongs to the
			// singleton case, so keep it alive through the wide case.
			target := tc.targets[len(tc.targets)-1]
			targetID := f.number(f.left, "select account_id from mo_catalog.mo_account where account_name=?", target)
			gate := f.holdSourceDatabase("pub_lock_src")
			defer gate.Rollback()
			pub := "pub_lock_" + tc.name
			publishing := f.start(f.right, tc.create+" "+pub+" database pub_lock_src account "+strings.Join(tc.targets, ","))
			waited, err := f.waitForLockOrResult(publishing, 1, "mo_database")
			require.NoError(t, err)
			require.True(t, waited, "publication must retain its target locks while waiting on the source database")

			waited, err = f.waitForLockOrResult(f.start(f.left, "drop account "+tc.unrelated), 0, "mo_account")
			t.Logf("target count: %d; unrelated DROP waited on mo_account: %v", len(tc.targets), waited)
			require.NoError(t, err)
			require.False(t, waited, "unrelated DROP must commit while the source database remains locked")
			require.Zero(t, f.number(f.left, "select count(*) from mo_catalog.mo_account where account_name=?", tc.unrelated))

			dropping := f.start(f.left, "drop account "+target)
			waited, err = f.waitForLockOrResult(dropping, 0, "mo_account")
			require.NoError(t, err)
			require.True(t, waited, "target DROP must wait for the publication")
			require.NoError(t, gate.Rollback())
			require.NoError(t, <-publishing)
			require.NoError(t, <-dropping)
			require.Zero(t, f.number(f.left, "select count(*) from mo_catalog.mo_account where account_id=?", targetID))
			require.Zero(t, f.number(f.left, "select count(*) from mo_catalog.mo_subs where pub_account_id=0 and pub_name=? and sub_account_id=?", pub, targetID))
			require.Equal(t, len(tc.targets)-1, f.number(f.left, "select count(*) from mo_catalog.mo_subs where pub_account_id=0 and pub_name=? and status=0", pub))
			t.Logf("target DROP waited and committed after publication; no grants remain for its former account ID")
		})
	}
}

type publicationAccountSQL struct {
	t           *testing.T
	ctx         context.Context
	cancel      context.CancelFunc
	cluster     Cluster
	left, right *sql.DB
	running     sync.WaitGroup
}

func newPublicationAccountSQL(t *testing.T, cluster Cluster) *publicationAccountSQL {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 90*time.Second)
	t.Cleanup(cancel)
	f := &publicationAccountSQL{t: t, ctx: ctx, cancel: cancel, cluster: cluster}
	f.left, f.right = f.open(0, "dump"), f.open(1, "dump")
	return f
}

func (f *publicationAccountSQL) open(index int, user string) *sql.DB {
	f.t.Helper()
	cn, err := f.cluster.GetCNService(index)
	require.NoError(f.t, err)
	cfg := mysql.NewConfig()
	cfg.User = user
	cfg.Passwd = "111"
	cfg.Net = "tcp"
	cfg.Addr = fmt.Sprintf("127.0.0.1:%d", cn.GetServiceConfig().CN.Frontend.Port)
	db, err := sql.Open("mysql", cfg.FormatDSN())
	require.NoError(f.t, err)
	f.t.Cleanup(func() { require.NoError(f.t, db.Close()) })
	return db
}

func (f *publicationAccountSQL) stopStatements() {
	f.cancel()
	f.running.Wait()
}

func (f *publicationAccountSQL) exec(db *sql.DB, stmt string) {
	f.t.Helper()
	_, err := db.ExecContext(f.ctx, stmt)
	require.NoError(f.t, err, stmt)
}

func (f *publicationAccountSQL) createAccount(name string) {
	f.t.Helper()
	f.exec(f.left, "create account "+name+" admin_name 'admin' identified by '111'")
}

func (f *publicationAccountSQL) number(db *sql.DB, query string, args ...any) int {
	f.t.Helper()
	var number int
	require.NoError(f.t, db.QueryRowContext(f.ctx, query, args...).Scan(&number), query)
	return number
}

func (f *publicationAccountSQL) holdSourceDatabase(name string) *sql.Tx {
	f.t.Helper()
	tx, err := f.left.BeginTx(f.ctx, nil)
	require.NoError(f.t, err)
	f.t.Cleanup(func() { _ = tx.Rollback() })
	var id uint64
	require.NoError(f.t, tx.QueryRowContext(f.ctx, `select dat_id from mo_catalog.mo_database
		where account_id=0 and datname=? for update`, name).Scan(&id))
	return tx
}

func (f *publicationAccountSQL) start(db *sql.DB, stmt string) <-chan error {
	result := make(chan error, 1)
	f.running.Go(func() {
		_, err := db.ExecContext(f.ctx, stmt)
		result <- err
	})
	return result
}

// waitForLockOrResult observes a real lock wait on the DDL's CN or collects its
// terminal result. Only the DDL runs on that CN against the given catalog tables.
func (f *publicationAccountSQL) waitForLockOrResult(result <-chan error, index int, tables ...string) (bool, error) {
	f.t.Helper()
	cn, err := f.cluster.GetCNService(index)
	require.NoError(f.t, err)
	ctx, cancel := context.WithTimeout(f.ctx, 15*time.Second)
	defer cancel()
	args := make([]any, 1+len(tables))
	args[0] = cn.ServiceID()
	for i, table := range tables {
		args[i+1] = table
	}
	placeholders := strings.TrimSuffix(strings.Repeat("?,", len(tables)), ",")
	query := `select count(*) from mo_transactions() t where cn_id=? and table_id in
		(select cast(rel_id as char) from mo_catalog.mo_tables where account_id=0 and
		 reldatabase='mo_catalog' and relname in (` + placeholders + "))"
	ticker := time.NewTicker(30 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case err := <-result:
			return false, err
		default:
		}
		var count int
		err := f.left.QueryRowContext(ctx, query, args...).Scan(&count)
		require.NoError(f.t, err)
		if count > 0 {
			return true, nil
		}
		select {
		case err := <-result:
			return false, err
		case <-ticker.C:
		case <-ctx.Done():
			f.t.Fatalf("CN %d neither finished nor waited on %v: %v", index, tables, ctx.Err())
		}
	}
}

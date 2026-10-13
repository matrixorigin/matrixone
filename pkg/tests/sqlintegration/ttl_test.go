// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package sqlintegration

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math/rand"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/matrixorigin/matrixone/pkg/common/sqlquote"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/sql/ttl"
	"github.com/matrixorigin/matrixone/pkg/tests/testutils"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/stretchr/testify/require"
)

const ttlTestSeconds = uint64(36400 * 86400)

var ttlTestCutoff = time.Date(2027, 1, 1, 0, 0, 0, 0, time.UTC)

func ttlTestNow() time.Time { return ttlTestCutoff.Add(time.Duration(ttlTestSeconds) * time.Second) }
func ttlExec(t *testing.T, ctx context.Context, exec executor.SQLExecutor, db, statement string, account uint32) {
	t.Helper()
	res, err := exec.Exec(ctx, statement, executor.Options{}.WithAccountID(account).WithDatabase(db).WithTimeZone(time.UTC).WithWaitCommittedLogApplied())
	res.Close()
	require.NoError(t, err, statement)
}
func ttlIDs(t *testing.T, ctx context.Context, exec executor.SQLExecutor, db, table string, account uint32) []int32 {
	t.Helper()
	res, err := exec.Exec(ctx, "select id from "+sqlquote.Ident(table)+" order by id", executor.Options{}.WithDatabase(db).WithAccountID(account))
	require.NoError(t, err)
	defer res.Close()
	ids := []int32{}
	res.ReadRows(func(rows int, cols []*vector.Vector) bool {
		ids = append(ids, vector.MustFixedColNoTypeCheck[int32](cols[0])[:rows]...)
		return true
	})
	return ids
}
func ttlCandidate(t *testing.T, ctx context.Context, exec executor.SQLExecutor, db, table string, account uint32) ttl.Candidate {
	t.Helper()
	candidates, err := ttl.Discover(ctx, exec)
	require.NoError(t, err)
	for _, c := range candidates {
		if c.AccountID == account && c.Database == db && c.Table == table {
			return c
		}
	}
	t.Fatalf("TTL candidate missing for %d %s.%s", account, db, table)
	return ttl.Candidate{}
}
func TestTTLTemporalMatrix(t *testing.T) {
	runSQLIntegration(t, func(cluster embed.Cluster) {
		cn, err := cluster.GetCNService(0)
		require.NoError(t, err)
		exec := testutils.GetSQLExecutor(cn)
		ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
		defer cancel()
		db := strings.ToLower(testutils.GetDatabaseName(t))
		ttlExec(t, ctx, exec, "", "create database "+db, 0)
		defer cleanupSQLIntegration(t, cn, "drop database if exists "+db)
		units := []struct {
			name   string
			factor uint64
		}{{"second", 1}, {"minute", 60}, {"hour", 3600}, {"day", 86400}, {"week", 604800}}
		types := []string{"date", "datetime", "datetime(6)", "timestamp", "timestamp(6)"}
		for ti, typ := range types {
			for ui, unit := range units {
				table := fmt.Sprintf("t_%d_%d", ti, ui)
				t.Run(typ+"_"+unit.name, func(t *testing.T) {
					ttlExec(t, ctx, exec, db, fmt.Sprintf("create table %s(id int primary key, EventTime %s, value int, unique key(value)) ttl EventTime + interval %d %s delete", table, typ, ttlTestSeconds/unit.factor, unit.name), 0)
					ttlExec(t, ctx, exec, db, fmt.Sprintf("insert into %s values(1,'2026-12-31',11),(2,'2027-01-01',12),(3,'2027-01-02',13),(4,null,14)", table), 0)
					c := ttlCandidate(t, ctx, exec, db, table, 0)
					require.Equal(t, uint32(1), c.Policy.Seqnum)
					deleted, err := ttl.DeleteBatch(ctx, exec, c, ttlTestNow(), 1)
					require.NoError(t, err)
					require.Equal(t, uint64(1), deleted)
					require.Len(t, ttlIDs(t, ctx, exec, db, table, 0), 3)
					deleted, err = ttl.DeleteBatch(ctx, exec, c, ttlTestNow(), 16)
					require.NoError(t, err)
					require.Equal(t, uint64(1), deleted)
					require.Equal(t, []int32{3, 4}, ttlIDs(t, ctx, exec, db, table, 0))
					// Reuse deleted unique keys: stale index entries would reject this insert.
					ttlExec(t, ctx, exec, db, fmt.Sprintf("insert into %s values(5,'2027-01-03',11),(6,'2027-01-04',12)", table), 0)
					deleted, err = ttl.DeleteBatch(ctx, exec, c, ttlTestNow(), 16)
					require.NoError(t, err)
					require.Zero(t, deleted)
				})
			}
		}
	})
}
func TestTTLPolicyLifecycleAndStaleDiscovery(t *testing.T) {
	runSQLIntegration(t, func(cluster embed.Cluster) {
		cn, err := cluster.GetCNService(0)
		require.NoError(t, err)
		exec := testutils.GetSQLExecutor(cn)
		ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
		defer cancel()
		db := strings.ToLower(testutils.GetDatabaseName(t))
		ttlExec(t, ctx, exec, "", "create database "+db, 0)
		defer cleanupSQLIntegration(t, cn, "drop database if exists "+db)
		create := fmt.Sprintf("create table events(id int primary key, ts datetime(6), payload int) ttl ts + interval %d second", ttlTestSeconds)
		ttlExec(t, ctx, exec, db, create, 0)
		ttlExec(t, ctx, exec, db, "insert into events values(1,'2026-12-31',1)", 0)
		stale := ttlCandidate(t, ctx, exec, db, "events", 0)
		ttlExec(t, ctx, exec, db, fmt.Sprintf("alter table events modify ttl ts + interval %d second", ttlTestSeconds-86400), 0)
		n, err := ttl.DeleteBatch(ctx, exec, stale, ttlTestNow(), 10)
		require.NoError(t, err)
		require.Zero(t, n)
		require.Equal(t, []int32{1}, ttlIDs(t, ctx, exec, db, "events", 0))
		current := ttlCandidate(t, ctx, exec, db, "events", 0)
		require.Equal(t, ttlTestSeconds-86400, current.Policy.Seconds)
		ttlExec(t, ctx, exec, db, "alter table events remove ttl", 0)
		n, err = ttl.DeleteBatch(ctx, exec, current, ttlTestNow(), 10)
		require.NoError(t, err)
		require.Zero(t, n)
		for _, statement := range []string{"alter table events rename column ts to renamed", "alter table events change renamed ts datetime(6)"} {
			ttlExec(t, ctx, exec, db, statement, 0)
		}
		ttlExec(t, ctx, exec, db, fmt.Sprintf("alter table events ttl ts + interval %d second", ttlTestSeconds), 0)
		current = ttlCandidate(t, ctx, exec, db, "events", 0)
		ttlExec(t, ctx, exec, db, "alter table events add column extra int", 0)
		current = ttlCandidate(t, ctx, exec, db, "events", 0)
		ttlExec(t, ctx, exec, db, "create table cloned like events", 0)
		cloned := ttlCandidate(t, ctx, exec, db, "cloned", 0)
		require.Equal(t, current.Policy, cloned.Policy)
		for _, statement := range []string{"alter table events drop column ts", "alter table events rename column ts to changed", "alter table events modify ts varchar(30)"} {
			res, err := exec.Exec(ctx, statement, executor.Options{}.WithDatabase(db))
			res.Close()
			require.Error(t, err, statement)
		}
		ttlExec(t, ctx, exec, db, "drop table events", 0)
		ttlExec(t, ctx, exec, db, "create table events(id int primary key, ts datetime)", 0)
		ttlExec(t, ctx, exec, db, "insert into events values(1,'2026-12-31')", 0)
		n, err = ttl.DeleteBatch(ctx, exec, current, ttlTestNow(), 10)
		require.NoError(t, err)
		require.Zero(t, n)
		require.Equal(t, []int32{1}, ttlIDs(t, ctx, exec, db, "events", 0))
	})
}

// Fail after DELETE executes so ExecTxn must roll back the whole batch.
type ttlRollbackExecutor struct{ executor.SQLExecutor }

var ttlForcedRollback = errors.New("forced TTL rollback")

func (e ttlRollbackExecutor) ExecTxn(ctx context.Context, fn func(executor.TxnExecutor) error, opts executor.Options) error {
	return e.SQLExecutor.ExecTxn(ctx, func(tx executor.TxnExecutor) error {
		if err := fn(tx); err != nil {
			return err
		}
		return ttlForcedRollback
	}, opts)
}
func TestTTLRandomizedRowsRollbackAndConcurrentWorkers(t *testing.T) {
	runSQLIntegration(t, func(cluster embed.Cluster) {
		cn, err := cluster.GetCNService(0)
		require.NoError(t, err)
		exec := testutils.GetSQLExecutor(cn)
		ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
		defer cancel()
		db := strings.ToLower(testutils.GetDatabaseName(t))
		ttlExec(t, ctx, exec, "", "create database "+db, 0)
		defer cleanupSQLIntegration(t, cn, "drop database if exists "+db)
		ttlExec(t, ctx, exec, db, fmt.Sprintf("create table events(id int primary key, ts datetime(6)) ttl ts + interval %d second", ttlTestSeconds), 0)
		rng := rand.New(rand.NewSource(101))
		values := []string{}
		expected := []int32{}
		all := []int32{}
		for i := int32(1); i <= 1500; i++ {
			all = append(all, i)
			if i%13 == 0 {
				values = append(values, fmt.Sprintf("(%d,null)", i))
				expected = append(expected, i)
				continue
			}
			// Include exact boundary and +/- 1 microsecond alongside random row ages.
			delta := time.Duration(rng.Int63n(2000001)-1000000) * time.Microsecond
			if i <= 3 {
				delta = time.Duration(i-2) * time.Microsecond
			}
			row := ttlTestCutoff.Add(delta)
			values = append(values, fmt.Sprintf("(%d,'%s')", i, row.Format("2006-01-02 15:04:05.000000")))
			if row.After(ttlTestCutoff) {
				expected = append(expected, i)
			}
		}
		ttlExec(t, ctx, exec, db, "insert into events values "+strings.Join(values, ","), 0)
		c := ttlCandidate(t, ctx, exec, db, "events", 0)
		n, err := ttl.DeleteBatch(ctx, ttlRollbackExecutor{exec}, c, ttlTestNow(), 127)
		require.ErrorIs(t, err, ttlForcedRollback)
		require.Zero(t, n)
		require.Equal(t, all, ttlIDs(t, ctx, exec, db, "events", 0))
		var wg sync.WaitGroup
		errs := make(chan error, 8)
		for i := 0; i < 8; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for j := 0; j < 8; j++ {
					_, err := ttl.DeleteBatch(ctx, exec, c, ttlTestNow(), 127)
					if err != nil {
						errs <- err
						return
					}
				}
			}()
		}
		wg.Wait()
		close(errs)
		for err := range errs {
			require.NoError(t, err)
		}
		sort.Slice(expected, func(i, j int) bool { return expected[i] < expected[j] })
		require.Equal(t, expected, ttlIDs(t, ctx, exec, db, "events", 0))
		// Verify timestamp conversion under a non-UTC client insert.
		ttlExec(t, ctx, exec, db, fmt.Sprintf("create table zoned(id int primary key, ts timestamp(6)) ttl ts + interval %d second", ttlTestSeconds), 0)
		port := cn.GetServiceConfig().CN.Frontend.Port
		client, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/%s", port, db))
		require.NoError(t, err)
		defer client.Close()
		client.SetMaxOpenConns(1)
		execSQLRequire(t, ctx, client, "set time_zone='+08:00'")
		execSQLRequire(t, ctx, client, "insert into zoned values(1,'2027-01-01 07:59:59.999999'),(2,'2027-01-01 08:00:00'),(3,'2027-01-01 08:00:00.000001')")
		z := ttlCandidate(t, ctx, exec, db, "zoned", 0)
		n, err = ttl.DeleteBatch(ctx, exec, z, ttlTestNow(), 10)
		require.NoError(t, err)
		require.Equal(t, uint64(2), n)
		require.Equal(t, []int32{3}, ttlIDs(t, ctx, exec, db, "zoned", 0))
	})
}

func TestTTLTenantIsolation(t *testing.T) {
	runSQLIntegration(t, func(cluster embed.Cluster) {
		cn, err := cluster.GetCNService(0)
		require.NoError(t, err)
		exec := testutils.GetSQLExecutor(cn)
		ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
		defer cancel()
		port := cn.GetServiceConfig().CN.Frontend.Port
		admin, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", port))
		require.NoError(t, err)
		defer admin.Close()
		account := "ttl_isolation_tenant"
		execSQLRequire(t, ctx, admin, "create account "+account+" admin_name='admin' identified by 'pass123'")
		defer func() {
			_, err := admin.ExecContext(context.Background(), "drop account if exists "+account)
			require.NoError(t, err)
		}()
		var accountID uint32
		require.NoError(t, admin.QueryRowContext(ctx, "select account_id from mo_catalog.mo_account where account_name=?", account).Scan(&accountID))
		db := strings.ToLower(testutils.GetDatabaseName(t))
		ttlExec(t, ctx, exec, "", "create database "+db, 0)
		defer cleanupSQLIntegration(t, cn, "drop database if exists "+db)
		ttlExec(t, ctx, exec, "", "create database "+db, accountID)
		for _, id := range []uint32{0, accountID} {
			ttlExec(t, ctx, exec, db, fmt.Sprintf("create table same_name(id int primary key, ts datetime) ttl ts + interval %d second", ttlTestSeconds), id)
			ttlExec(t, ctx, exec, db, "insert into same_name values(1,'2026-12-31'),(2,'2027-01-02')", id)
		}
		tenant := ttlCandidate(t, ctx, exec, db, "same_name", accountID)
		n, err := ttl.DeleteBatch(ctx, exec, tenant, ttlTestNow(), 10)
		require.NoError(t, err)
		require.Equal(t, uint64(1), n)
		require.Equal(t, []int32{2}, ttlIDs(t, ctx, exec, db, "same_name", accountID))
		require.Equal(t, []int32{1, 2}, ttlIDs(t, ctx, exec, db, "same_name", 0))
		system := ttlCandidate(t, ctx, exec, db, "same_name", 0)
		n, err = ttl.DeleteBatch(ctx, exec, system, ttlTestNow(), 10)
		require.NoError(t, err)
		require.Equal(t, uint64(1), n)
		require.Equal(t, []int32{2}, ttlIDs(t, ctx, exec, db, "same_name", accountID))
	})
}
func TestTTLBackgroundCronAndRestart(t *testing.T) {
	runSQLIntegration(t, func(cluster embed.Cluster) {
		cn, err := cluster.GetCNService(0)
		require.NoError(t, err)
		exec := testutils.GetSQLExecutor(cn)
		ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
		defer cancel()
		db := strings.ToLower(testutils.GetDatabaseName(t))
		ttlExec(t, ctx, exec, "", "create database "+db, 0)
		defer cleanupSQLIntegration(t, cn, "drop database if exists "+db)
		ttlExec(t, ctx, exec, db, "create table events(id int primary key, ts datetime) ttl ts + interval 1 second", 0)
		before := ttlCandidate(t, ctx, exec, db, "events", 0)
		require.NoError(t, cn.Close())
		require.NoError(t, cn.Start())
		exec = testutils.GetSQLExecutor(cn)
		after := ttlCandidate(t, ctx, exec, db, "events", 0)
		require.Equal(t, before.Policy, after.Policy)
		require.Equal(t, before.TableID, after.TableID)
		ttlExec(t, ctx, exec, db, "insert into events values(1,'2020-01-01'),(2,'2099-01-01'),(3,null)", 0)
		deadline := time.NewTimer(95 * time.Second)
		defer deadline.Stop()
		tick := time.NewTicker(time.Second)
		defer tick.Stop()
		for {
			res, err := exec.Exec(ctx, "select id from events order by id", executor.Options{}.WithDatabase(db))
			require.NoError(t, err)
			ids := []int32{}
			res.ReadRows(func(rows int, cols []*vector.Vector) bool {
				ids = append(ids, vector.MustFixedColNoTypeCheck[int32](cols[0])[:rows]...)
				return true
			})
			res.Close()
			if len(ids) == 2 {
				require.Equal(t, []int32{2, 3}, ids)
				break
			}
			select {
			case <-deadline.C:
				t.Fatal("scheduled TTL did not remove expired row after restart")
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			case <-tick.C:
			}
		}
	})
}

func TestTTLConcurrentForegroundWrites(t *testing.T) {
	runSQLIntegration(t, func(cluster embed.Cluster) {
		cn, err := cluster.GetCNService(0)
		require.NoError(t, err)
		exec := testutils.GetSQLExecutor(cn)
		ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
		defer cancel()
		db := strings.ToLower(testutils.GetDatabaseName(t))
		ttlExec(t, ctx, exec, "", "create database "+db, 0)
		defer cleanupSQLIntegration(t, cn, "drop database if exists "+db)
		ttlExec(t, ctx, exec, db, fmt.Sprintf("create table events(id int primary key, ts datetime(6), value int, unique key(value)) ttl ts + interval %d second", ttlTestSeconds), 0)
		c := ttlCandidate(t, ctx, exec, db, "events", 0)
		port := cn.GetServiceConfig().CN.Frontend.Port
		client, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/%s", port, db))
		require.NoError(t, err)
		defer client.Close()
		client.SetMaxOpenConns(4)
		var wg sync.WaitGroup
		errs := make(chan error, 5)
		for writer := 0; writer < 4; writer++ {
			wg.Add(1)
			go func(writer int) {
				defer wg.Done()
				for batch := 0; batch < 20; batch++ {
					values := []string{}
					for row := 0; row < 10; row++ {
						id := writer*1000 + batch*10 + row
						date := "2027-01-02"
						if id%2 == 0 {
							date = "2026-12-31"
						}
						values = append(values, fmt.Sprintf("(%d,'%s',%d)", id, date, id))
					}
					_, err := client.ExecContext(ctx, "insert into events values "+strings.Join(values, ","))
					if err != nil {
						errs <- err
						return
					}
				}
			}(writer)
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 20; i++ {
				_, err := ttl.DeleteBatch(ctx, exec, c, ttlTestNow(), 37)
				if err != nil {
					errs <- err
					return
				}
			}
		}()
		wg.Wait()
		close(errs)
		for err := range errs {
			require.NoError(t, err)
		}
		for {
			n, err := ttl.DeleteBatch(ctx, exec, c, ttlTestNow(), 37)
			require.NoError(t, err)
			if n == 0 {
				break
			}
		}
		ids := ttlIDs(t, ctx, exec, db, "events", 0)
		require.Len(t, ids, 400)
		for _, id := range ids {
			require.Equal(t, int32(1), id%2)
		}
	})
}

// Signal immediately before acquiring the TTL table lock, so the test can
// commit a policy removal while TTL waits behind the foreground DDL owner.
type ttlLockSignalExecutor struct {
	executor.SQLExecutor
	entered chan struct{}
}
type ttlLockSignalTxn struct {
	executor.TxnExecutor
	entered chan struct{}
}

func (e ttlLockSignalExecutor) ExecTxn(ctx context.Context, fn func(executor.TxnExecutor) error, opts executor.Options) error {
	return e.SQLExecutor.ExecTxn(ctx, func(tx executor.TxnExecutor) error { return fn(ttlLockSignalTxn{tx, e.entered}) }, opts)
}
func (tx ttlLockSignalTxn) LockTable(table string) error {
	select {
	case <-tx.entered:
	default:
		close(tx.entered)
	}
	return tx.TxnExecutor.LockTable(table)
}
func TestTTLPolicyRemovalWhileWorkerWaits(t *testing.T) {
	runSQLIntegration(t, func(cluster embed.Cluster) {
		cn, err := cluster.GetCNService(0)
		require.NoError(t, err)
		exec := testutils.GetSQLExecutor(cn)
		ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
		defer cancel()
		db := strings.ToLower(testutils.GetDatabaseName(t))
		ttlExec(t, ctx, exec, "", "create database "+db, 0)
		defer cleanupSQLIntegration(t, cn, "drop database if exists "+db)
		ttlExec(t, ctx, exec, db, fmt.Sprintf("create table events(id int primary key, ts datetime) ttl ts + interval %d second", ttlTestSeconds), 0)
		ttlExec(t, ctx, exec, db, "insert into events values(1,'2026-12-31')", 0)
		c := ttlCandidate(t, ctx, exec, db, "events", 0)
		locked := make(chan struct{})
		release := make(chan struct{})
		writerDone := make(chan error, 1)
		go func() {
			writerDone <- exec.ExecTxn(ctx, func(tx executor.TxnExecutor) error {
				if err := tx.LockTable("events"); err != nil {
					return err
				}
				close(locked)
				select {
				case <-release:
				case <-ctx.Done():
					return ctx.Err()
				}
				res, err := tx.Exec("alter table events remove ttl", executor.StatementOption{})
				res.Close()
				return err
			}, executor.Options{}.WithDatabase(db))
		}()
		select {
		case <-locked:
		case err := <-writerDone:
			require.NoError(t, err)
			t.Fatal("DDL did not acquire its lock")
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
		entered := make(chan struct{})
		workerDone := make(chan error, 1)
		go func() {
			n, err := ttl.DeleteBatch(ctx, ttlLockSignalExecutor{exec, entered}, c, ttlTestNow(), 10)
			if err == nil && n != 0 {
				err = fmt.Errorf("stale worker deleted %d rows", n)
			}
			workerDone <- err
		}()
		select {
		case <-entered:
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
		close(release)
		require.NoError(t, <-writerDone)
		require.NoError(t, <-workerDone)
		require.Equal(t, []int32{1}, ttlIDs(t, ctx, exec, db, "events", 0))
	})
}

func TestTTLPrivilegesAndReferencedParent(t *testing.T) {
	runSQLIntegration(t, func(cluster embed.Cluster) {
		cn, err := cluster.GetCNService(0)
		require.NoError(t, err)
		exec := testutils.GetSQLExecutor(cn)
		ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
		defer cancel()
		db := strings.ToLower(testutils.GetDatabaseName(t))
		ttlExec(t, ctx, exec, "", "create database "+db, 0)
		defer cleanupSQLIntegration(t, cn, "drop database if exists "+db)
		ttlExec(t, ctx, exec, db, fmt.Sprintf("create table events(id int primary key, ts datetime) ttl ts + interval %d second", ttlTestSeconds), 0)
		ttlExec(t, ctx, exec, db, "insert into events values(1,'2026-12-31')", 0)
		port := cn.GetServiceConfig().CN.Frontend.Port
		admin, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/%s", port, db))
		require.NoError(t, err)
		defer admin.Close()
		execSQLRequire(t, ctx, admin, "create role ttl_reader_role")
		defer func() {
			_, err := admin.ExecContext(context.Background(), "drop role if exists ttl_reader_role")
			require.NoError(t, err)
		}()
		execSQLRequire(t, ctx, admin, "create user ttl_reader identified by 'reader111'")
		defer func() {
			_, err := admin.ExecContext(context.Background(), "drop user if exists ttl_reader")
			require.NoError(t, err)
		}()
		execSQLRequire(t, ctx, admin, "grant connect on account * to ttl_reader_role")
		execSQLRequire(t, ctx, admin, "grant select on table "+db+".events to ttl_reader_role")
		execSQLRequire(t, ctx, admin, "grant ttl_reader_role to ttl_reader")
		connector, err := mysql.NewConnector(&mysql.Config{User: "sys:ttl_reader:ttl_reader_role", Passwd: "reader111", Net: "tcp", Addr: fmt.Sprintf("127.0.0.1:%d", port), DBName: db, AllowNativePasswords: true})
		require.NoError(t, err)
		reader := sql.OpenDB(connector)
		defer reader.Close()
		var count int
		require.NoError(t, reader.QueryRowContext(ctx, "select count(*) from events").Scan(&count))
		require.Equal(t, 1, count)
		for _, statement := range []string{"alter table events remove ttl", "alter table events modify ttl ts + interval 1 second"} {
			_, err := reader.ExecContext(ctx, statement)
			require.Error(t, err, statement)
		}
		require.Equal(t, ttlTestSeconds, ttlCandidate(t, ctx, exec, db, "events", 0).Policy.Seconds)
		ttlExec(t, ctx, exec, db, "create table parent(id int primary key, ts datetime)", 0)
		ttlExec(t, ctx, exec, db, "create table child(id int primary key, parent_id int, foreign key(parent_id) references parent(id))", 0)
		res, err := exec.Exec(ctx, "alter table parent ttl ts + interval 1 day", executor.Options{}.WithDatabase(db))
		res.Close()
		require.Error(t, err)
	})
}

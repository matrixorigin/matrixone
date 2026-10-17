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
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/google/uuid"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/cnservice"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/lockservice"
	"github.com/matrixorigin/matrixone/pkg/pb/lock"
	"github.com/matrixorigin/matrixone/pkg/queryservice"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/stretchr/testify/require"
)

func TestCheckConstraintNamespace(t *testing.T) {
	defer func() { require.NoError(t, CloseBaseClusterTests()) }()
	RunBaseClusterTests(t, func(cluster Cluster) {
		cn, err := cluster.GetCNService(0)
		require.NoError(t, err)
		db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", cn.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		defer db.Close()
		ctx, cancel := context.WithTimeout(t.Context(), 5*time.Minute)
		defer cancel()
		conn, err := db.Conn(ctx)
		require.NoError(t, err)
		defer conn.Close()
		exec := func(t *testing.T, statement string) {
			t.Helper()
			_, err := conn.ExecContext(ctx, statement)
			require.NoError(t, err, statement)
		}
		expectError := func(t *testing.T, statement string, code uint16) {
			t.Helper()
			_, err := conn.ExecContext(ctx, statement)
			var serverError *mysql.MySQLError
			require.True(t, errors.As(err, &serverError), "%s: %v", statement, err)
			require.Equal(t, code, serverError.Number, "%s: %v", statement, err)
		}
		count := func(t *testing.T, query string, want int) {
			t.Helper()
			var actual int
			require.NoError(t, conn.QueryRowContext(ctx, query).Scan(&actual), query)
			require.Equal(t, want, actual, query)
		}
		const database = "check_namespace_review"
		exec(t, "create database "+database)
		exec(t, "use "+database)
		defer func() {
			cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), time.Minute)
			defer cleanupCancel()
			_, err := conn.ExecContext(cleanupCtx, "drop snapshot if exists check_namespace_legacy")
			require.NoError(t, err)
			_, err = conn.ExecContext(cleanupCtx, "drop database if exists "+database)
			require.NoError(t, err)
		}()

		t.Run("copy alter validates only newly introduced names", func(t *testing.T) {
			exec(t, "create table a(x int, constraint ck check(x>0))")
			exec(t, "create table b(x int)")
			exec(t, "insert into b values(5)")
			for _, alter := range []string{
				"alter table b add constraint ck check(x<10)",
				"alter table b add constraint CK check(x<10), algorithm=copy",
			} {
				expectError(t, alter, 3822)
				count(t, "select count(*) from b where x=5", 1)
				count(t, "select count(*) from information_schema.table_constraints where table_schema='"+database+"' and table_name='b' and constraint_type='CHECK'", 0)
			}
			exec(t, "alter table b add constraint ck_b check(x<10)")
			exec(t, "alter table b add column y int, algorithm=copy")
			count(t, "select count(*) from information_schema.table_constraints where table_schema='"+database+"' and table_name='b' and constraint_name='ck_b'", 1)
		})

		t.Run("partition backing tables borrow parent identity", func(t *testing.T) {
			exec(t, "create table p(x int, constraint ck_part check(x>0)) partition by hash(x) partitions 2")
			exec(t, "insert into p values(1),(2)")
			exec(t, "rename table p to q")
			count(t, "select count(*) from q", 2)
			expectError(t, "insert into q values(-1)", 3819)
			expectError(t, "create table part_conflict(x int, constraint ck_part check(x>0))", 3822)
			exec(t, "alter table q add column y int, algorithm=copy")
			count(t, "select count(*) from q", 2)
		})

		t.Run("generated identifier boundaries", func(t *testing.T) {
			exec(t, "create table long_rename(x int check(x>0))")
			exec(t, "create table length_source(x int check(x>0))")
			for _, length := range []int{58, 59, 64} {
				name := strings.Repeat("t", length)
				if length > 58 {
					expectError(t, "create table "+name+"(x int check(x>0))", 1059)
					expectError(t, "rename table long_rename to "+name, 1059)
					expectError(t, "create table "+name+" like length_source", 1059)
					expectError(t, "create table "+name+" clone length_source", 1059)
					continue
				}
				exec(t, "create table "+name+"(x int check(x>0))")
				exec(t, "alter table "+name+" add column y int, algorithm=copy")
				exec(t, "drop table "+name)
				exec(t, "create table "+name+" like length_source")
				exec(t, "drop table "+name)
				exec(t, "create table "+name+" clone length_source")
				exec(t, "drop table "+name)
			}
		})

		t.Run("legacy snapshot restore preserves duplicate historical identities", func(t *testing.T) {
			svc := cn.RawService().(cnservice.Service)
			for _, name := range []string{"legacy_a", "legacy_b"} {
				// Emulate names persisted by an old CN; this internal fixture is
				// deliberately unavailable to ordinary user-authored DDL.
				res, err := svc.GetSQLExecutor().Exec(defines.AttachAccountId(ctx, 0),
					"create table "+database+"."+name+"(x int, constraint __mo_chk_1 check(x>0))",
					executor.Options{}.WithWaitCommittedLogApplied().WithStatementOption(executor.StatementOption{}.WithTrustedCheckConstraintNames()))
				require.NoError(t, err)
				res.Close()
				exec(t, "insert into "+name+" values(1)")
			}
			exec(t, "create snapshot check_namespace_legacy for database "+database)
			exec(t, "insert into legacy_a values(2)")
			exec(t, "restore database "+database+" {snapshot='check_namespace_legacy'}")
			exec(t, "use "+database)
			for _, name := range []string{"legacy_a", "legacy_b"} {
				count(t, "select count(*) from "+name, 1)
				count(t, "select count(*) from information_schema.table_constraints where table_schema='"+database+"' and table_name='"+name+"' and constraint_name='__mo_chk_1'", 1)
				expectError(t, "insert into "+name+" values(-1)", 3819)
			}
			// Ordinary clones remain authors of new identities, even though
			// the clone executor also carries the frontend IsRestore marker.
			exec(t, "create table ordinary_clone clone legacy_a")
			count(t, "select count(*) from information_schema.table_constraints where table_schema='"+database+"' and table_name='ordinary_clone' and constraint_name='ordinary_clone_chk_1'", 1)
			exec(t, "alter table legacy_a add column y int, algorithm=copy")
			exec(t, "rename table legacy_a to legacy_renamed")
			count(t, "select count(*) from legacy_renamed", 1)
		})

		otherCN, err := cluster.GetCNService(1)
		require.NoError(t, err)
		otherDB, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/%s", otherCN.GetServiceConfig().CN.Frontend.Port, database))
		require.NoError(t, err)
		defer otherDB.Close()
		for _, commit := range []bool{true, false} {
			t.Run(fmt.Sprintf("concurrent namespace owner commit=%t", commit), func(t *testing.T) {
				checkConcurrentConstraintClaim(t, ctx, conn, otherDB, cn.ServiceID(), otherCN, commit)
			})
		}
	})
}

func checkConcurrentConstraintClaim(t *testing.T, parent context.Context, owner *sql.Conn, db *sql.DB, ownerServiceID string, cn ServiceOperator, commit bool) {
	t.Helper()
	ctx, cancel := context.WithTimeout(parent, time.Minute)
	defer cancel()
	writer, err := db.Conn(ctx)
	require.NoError(t, err)
	defer writer.Close()
	exec := func(conn *sql.Conn, query string) {
		t.Helper()
		_, err := conn.ExecContext(ctx, query)
		require.NoError(t, err, query)
	}
	for _, table := range []string{"concurrent_a", "concurrent_b"} {
		exec(owner, "drop table if exists "+table)
	}
	exec(owner, "begin")
	defer func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), time.Minute)
		defer cleanupCancel()
		_, err := owner.ExecContext(cleanupCtx, "rollback")
		require.NoError(t, err)
	}()
	exec(owner, "create table concurrent_a(x int, constraint shared_ck check(x>0))")
	exec(writer, "begin")
	// Establish B's older snapshot before A publishes its CHECK identity.
	var connectionID uint32
	require.NoError(t, writer.QueryRowContext(ctx, "select connection_id()").Scan(&connectionID))
	provider := cn.RawService().(interface {
		SessionMgr() *queryservice.SessionManager
	})
	var writerTxn []byte
	for _, session := range provider.SessionMgr().GetAllSessions() {
		if identity, ok := session.(interface {
			GetTxnId() uuid.UUID
			GetConnectionID() uint32
		}); ok {
			id := identity.GetTxnId()
			if id != (uuid.UUID{}) && identity.GetConnectionID() == connectionID {
				writerTxn = append([]byte(nil), id[:]...)
			}
		}
	}
	require.NotEmpty(t, writerTxn)
	packer := types.NewPacker()
	defer packer.Close()
	packer.EncodeUint32(0)
	for _, name := range []string{"check_namespace_review", "CHECK_CONSTRAINT", "shared_ck"} {
		packer.EncodeStringType([]byte(name))
	}
	// lockop encodes the serialized VARCHAR primary key once more as a
	// string before handing it to the lock service.
	tuple := packer.Bytes()
	packer.Reset()
	packer.EncodeStringType(tuple)
	key := packer.GetBuf()
	services := []lockservice.LockService{
		lockservice.GetLockServiceByServiceID(ownerServiceID),
		lockservice.GetLockServiceByServiceID(cn.ServiceID()),
	}
	for _, service := range services {
		require.NotNil(t, service)
	}
	done := make(chan struct{})
	var writerErr error
	go func() {
		_, writerErr = writer.ExecContext(ctx, "create table concurrent_b(x int, constraint SHARED_CK check(x<10))")
		close(done)
	}()
	defer func() {
		cancel()
		// Join the SQL actor before replacing callbacks or returning its session.
		<-done
	}()
	// Observe the exact normalized CHECK key in the real lock-service queue,
	// not a timing window or an assumed ordinal of catalog lock callbacks.
	require.Eventually(t, func() bool {
		found := false
		for _, service := range services {
			service.IterLocks(func(tableID uint64, keys [][]byte, held lockservice.Lock) bool {
				if tableID == catalog.MO_TABLES_ID && len(keys) == 1 && bytes.Equal(keys[0], key) {
					held.IterWaiters(func(waiter lock.WaitTxn) bool {
						found = found || bytes.Equal(waiter.TxnID, writerTxn)
						return !found
					})
				}
				return !found
			})
		}
		return found
	}, 10*time.Second, 5*time.Millisecond, "second CN must wait on the CHECK identity held by the first transaction")
	if commit {
		exec(owner, "commit")
	} else {
		exec(owner, "rollback")
	}
	select {
	case <-done:
		if commit {
			var serverError *mysql.MySQLError
			require.True(t, errors.As(writerErr, &serverError), "%v", writerErr)
			require.Equal(t, uint16(3822), serverError.Number)
			exec(writer, "rollback")
		} else {
			require.NoError(t, writerErr)
			exec(writer, "commit")
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
}

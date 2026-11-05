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

package embed

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/frontend"
	"github.com/stretchr/testify/require"
)

func init() {
	nativeRebaseCrashPhase = runNativeRebaseCrashPhase
}

// The child commits one replay/history/registry transaction, then is killed
// after applying the next generation but before its COMMIT.
func runNativeRebaseCrashPhase(t *testing.T, phase, root string, db *sql.DB, ctx context.Context, identity rebaseCrashIdentity) {
	t.Helper()
	execute := func(query string) {
		t.Helper()
		_, err := db.ExecContext(ctx, query)
		require.NoError(t, err, query)
	}
	count := func(query string) int {
		t.Helper()
		var n int
		require.NoError(t, db.QueryRowContext(ctx, query).Scan(&n), query)
		return n
	}
	const edgeQuery = "select count(*) from mo_catalog.mo_branch_metadata where p_table_id in (select rel_id from mo_catalog.mo_tables where account_id=0 and reldatabase='rebase_crash_source')"
	const pinQuery = "select count(*) from mo_catalog.mo_snapshots where kind='branch' and database_name='rebase_crash_source'"
	if phase == "verify" {
		require.Equal(t, 11, count("select v from rebase_crash_committed.a where id=1"))
		require.Equal(t, 12, count("select v from rebase_crash_committed.b where id=1"))
		require.Zero(t, count("select count(*) from mo_catalog.mo_database where datname='rebase_crash_uncommitted'"))
		require.Equal(t, identity.EdgeCount, count(edgeQuery))
		require.Equal(t, identity.PinCount, count(pinQuery))
		for i, table := range []string{"a", "b"} {
			require.Equal(t, 30, count(fmt.Sprintf("select sum(v) from rebase_crash_source.%s{MO_TS=%d}", table, identity.BaseTS)))
			require.Equal(t, 1, count(fmt.Sprintf("select count(*) from mo_catalog.mo_branch_metadata where table_id=%d and clone_ts=%d", identity.Committed[i], identity.BaseTS)))
			require.Equal(t, 1, count(fmt.Sprintf("select count(*) from mo_catalog.mo_snapshots where kind='branch' and sname='__mo_branch_%d'", identity.Committed[i])))
		}
		execute("create role rebase_crash_rule_role")
		execute("alter role rebase_crash_rule_role add rule \"select * from rebase_crash_feature.a where id=1\" on table rebase_crash_feature.a")
		execute("drop role rebase_crash_rule_role")
		return
	}

	execute("create database rebase_crash_source")
	for _, table := range []string{"a", "b"} {
		execute("create table rebase_crash_source." + table + "(id int primary key,v int)")
		execute("insert into rebase_crash_source." + table + " values(1,10),(2,20)")
	}
	execute("create snapshot rebase_crash_bu for database rebase_crash_source")
	execute("data branch create database rebase_crash_feature from rebase_crash_source{snapshot='rebase_crash_bu'}")
	execute("create snapshot rebase_crash_bf for database rebase_crash_feature")
	execute("update rebase_crash_feature.a set v=11 where id=1")
	execute("update rebase_crash_feature.b set v=12 where id=1")
	execute("create snapshot rebase_crash_fh for database rebase_crash_feature")
	execute("create snapshot rebase_crash_uh for database rebase_crash_source")
	query := func(target string) string {
		return "data branch rebase database " + target + " from rebase_crash_feature{snapshot='rebase_crash_fh'} against rebase_crash_source{snapshot='rebase_crash_uh'} between rebase_crash_feature{snapshot='rebase_crash_bf'} and rebase_crash_source{snapshot='rebase_crash_bu'}"
	}
	var database, base string
	var id, tables uint64
	var counts [6]uint64
	require.NoError(t, db.QueryRowContext(ctx, query("rebase_crash_committed")).Scan(
		&database, &id, &base, &identity.BaseTS, &tables,
		&counts[0], &counts[1], &counts[2], &counts[3], &counts[4], &counts[5]))
	require.Equal(t, "rebase_crash_committed", database)
	require.Equal(t, uint64(2), tables)
	require.Equal(t, [6]uint64{2, 0, 0, 0, 2, 0}, counts)
	for i, table := range []string{"a", "b"} {
		require.NoError(t, db.QueryRowContext(ctx, "select rel_id from mo_catalog.mo_tables where account_id=0 and reldatabase='rebase_crash_committed' and relname=?", table).Scan(&identity.Committed[i]))
	}
	identity.EdgeCount, identity.PinCount = count(edgeQuery), count(pinQuery)
	data, err := json.Marshal(identity)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(root, "identity.json"), data, 0600))
	ready := os.NewFile(3, "ready")
	require.NotNil(t, ready)
	defer ready.Close()
	release := make(chan struct{})
	var once sync.Once
	reached := make(chan error, 1)
	queryDone := make(chan error, 1)
	joined := false
	restore := frontend.SetRebaseTableAppliedHookForTest(func(hookCtx context.Context, database, table string) {
		if database != "rebase_crash_uncommitted" || table != "a" {
			return
		}
		once.Do(func() { reached <- json.NewEncoder(ready).Encode(identity) })
		select {
		case <-release:
		case <-hookCtx.Done():
		}
	})
	defer func() {
		close(release)
		restore()
		if !joined {
			select {
			case <-queryDone:
			case <-time.After(30 * time.Second):
				t.Error("native Rebase query did not terminate during cleanup")
			}
		}
	}()
	go func() {
		rows, err := db.QueryContext(ctx, query("rebase_crash_uncommitted"))
		if err == nil {
			for rows.Next() {
			}
			err = rows.Err()
			if closeErr := rows.Close(); err == nil {
				err = closeErr
			}
		}
		queryDone <- err
	}()
	select {
	case err := <-reached:
		require.NoError(t, err)
	case err := <-queryDone:
		joined = true
		t.Fatalf("native Rebase terminated before the crash barrier: %v", err)
	case <-ctx.Done():
		t.Fatal("native Rebase did not reach the crash barrier")
	}
	select {
	case err := <-queryDone:
		joined = true
		t.Fatalf("native Rebase passed the crash barrier unexpectedly: %v", err)
	case <-ctx.Done():
		t.Fatal("native Rebase was not killed at the crash barrier")
	}
}

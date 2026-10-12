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

package issues

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/lockservice"
	pblock "github.com/matrixorigin/matrixone/pkg/pb/lock"
	"github.com/matrixorigin/matrixone/pkg/tests/testutils"
	"github.com/stretchr/testify/require"
)

func TestIssue29602IndexBackfillLocksStayScopedToPrivateTargets(t *testing.T) {
	runAuthenticatedClusterTest(t, func(cluster embed.Cluster) {
		ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
		defer cancel()
		open := func(index int) *sql.DB {
			cn, err := cluster.GetCNService(index)
			require.NoError(t, err)
			db, err := sql.Open("mysql", issue27487DSN(cn.GetServiceConfig().CN.Frontend.Port))
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, db.Close()) })
			return db
		}
		creatorDB, otherDB := open(0), open(1)
		database := strings.ToLower(testutils.GetDatabaseName(t))
		execSQLRequire(t, ctx, creatorDB, "create database `"+database+"`")
		t.Cleanup(func() {
			cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cleanupCancel()
			execSQLRequire(t, cleanupCtx, creatorDB, "drop database if exists `"+database+"`")
		})
		creator, err := creatorDB.Conn(ctx)
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, creator.Close()) })
		services := issue27487LockServices(cluster)
		require.NotEmpty(t, services)

		// All targets use a composite varlen key, including the regular index.
		// These are the public keyspace endpoints, independent of lockop's fetcher.
		packer := types.NewPacker()
		defer packer.Close()
		packer.EncodeStringType(nil)
		minKey := packer.Bytes()
		packer.Reset()
		packer.EncodeStringTypeMax()
		maxKey := packer.Bytes()

		for _, tc := range []struct {
			name       string
			seed       string
			unique     bool
			rollback   bool
			hiddenRows int
			matches    int
		}{
			{"regular", "(1,10,100),(2,10,100)", false, false, 2, 2},
			{"composite_unique", "(1,10,100),(2,10,101)", true, false, 2, 1},
			{"nullable_unique", "(1,10,100),(2,NULL,100),(3,NULL,100)", true, false, 1, 1},
			{"rollback", "(1,10,100),(2,20,200)", false, true, 2, 1},
		} {
			t.Run(tc.name, func(t *testing.T) {
				table := fmt.Sprintf("`%s`.`%s`", database, tc.name)
				execSQLRequire(t, ctx, creatorDB, "create table "+table+" (id int primary key, a int, b int)")
				execSQLRequire(t, ctx, creatorDB, "insert into "+table+" values "+tc.seed)
				txn, err := creator.BeginTx(ctx, nil)
				require.NoError(t, err)
				defer txn.Rollback()
				kind := ""
				if tc.unique {
					kind = "unique "
				}
				_, err = txn.ExecContext(ctx, "create "+kind+"index ix_backfill on "+table+" (a,b)")
				require.NoError(t, err)
				var hiddenName string
				var hiddenID uint64
				require.NoError(t, txn.QueryRowContext(ctx, `select distinct i.index_table_name, h.rel_id
from mo_catalog.mo_indexes i
join mo_catalog.mo_tables b on i.table_id = b.rel_id
join mo_catalog.mo_tables h on h.reldatabase_id = b.reldatabase_id and h.relname = i.index_table_name
where b.reldatabase = ? and b.relname = ? and i.name = 'ix_backfill'`, database, tc.name).Scan(&hiddenName, &hiddenID))
				require.NotZero(t, hiddenID)
				locks := issue29602TargetLocks(services, hiddenID)
				require.Len(t, locks, 1, "backfill must hold one full target range until transaction completion")
				require.True(t, locks[0].rangeLock)
				require.Equal(t, pblock.LockMode_Exclusive, locks[0].mode)
				require.Equal(t, [][]byte{minKey, maxKey}, locks[0].keys)

				var hiddenRows int
				require.NoError(t, txn.QueryRowContext(ctx,
					fmt.Sprintf("select count(*) from `%s`.`%s`", database, hiddenName)).Scan(&hiddenRows))
				require.Equal(t, tc.hiddenRows, hiddenRows)
				if tc.rollback {
					require.NoError(t, txn.Rollback())
					var published int
					require.NoError(t, creator.QueryRowContext(ctx,
						"select count(*) from mo_catalog.mo_tables where rel_id = ?", hiddenID).Scan(&published))
					require.Zero(t, published, "rollback must remove the private index table")
					require.NoError(t, creator.QueryRowContext(ctx,
						"select count(*) from information_schema.statistics where table_schema = ? and table_name = ? and index_name = 'ix_backfill'",
						database, tc.name).Scan(&published))
					require.Zero(t, published)
					var rows int
					require.NoError(t, creator.QueryRowContext(ctx, "select count(*) from "+table).Scan(&rows))
					require.Equal(t, 2, rows, "index rollback must preserve the committed source rows")
				} else {
					require.NoError(t, txn.Commit())
					for _, hint := range []string{"force", "ignore"} {
						var matches int
						require.NoError(t, creator.QueryRowContext(ctx,
							"select count(*) from "+table+" "+hint+" index(ix_backfill) where a=10 and b=100").Scan(&matches))
						require.Equal(t, tc.matches, matches)
					}
				}
				issue29602RequireUnlocked(t, services, hiddenID)
			})
		}

		t.Run("duplicate composite key rejects publication", func(t *testing.T) {
			table := "`" + database + "`.`duplicate_keys`"
			execSQLRequire(t, ctx, creatorDB, "create table "+table+" (id int primary key, a int, b int)")
			execSQLRequire(t, ctx, creatorDB, "insert into "+table+" values (1,10,100),(2,10,100)")
			_, err := creator.ExecContext(ctx, "create unique index ix_backfill on "+table+" (a,b)")
			issue289RequireMySQLError(t, err, 1062)
			var published, rows int
			require.NoError(t, creator.QueryRowContext(ctx,
				"select count(*) from information_schema.statistics where table_schema = ? and table_name = 'duplicate_keys' and index_name = 'ix_backfill'",
				database).Scan(&published))
			require.Zero(t, published)
			require.NoError(t, creator.QueryRowContext(ctx, "select count(*) from "+table).Scan(&rows))
			require.Equal(t, 2, rows)
		})

		t.Run("ordinary inserts keep distinct row locks after DDL", func(t *testing.T) {
			table := "`" + database + "`.`composite_unique`"
			var baseID, hiddenID uint64
			require.NoError(t, creator.QueryRowContext(ctx, `select distinct b.rel_id, h.rel_id
from mo_catalog.mo_tables b
join mo_catalog.mo_indexes i on i.table_id = b.rel_id and i.name = 'ix_backfill'
join mo_catalog.mo_tables h on h.reldatabase_id = b.reldatabase_id and h.relname = i.index_table_name
where b.reldatabase = ? and b.relname = 'composite_unique'`, database).Scan(&baseID, &hiddenID))
			first, err := creator.BeginTx(ctx, nil)
			require.NoError(t, err)
			defer first.Rollback()
			_, err = first.ExecContext(ctx, "insert into "+table+" values (3,30,300)")
			require.NoError(t, err)
			second, err := otherDB.BeginTx(ctx, nil)
			require.NoError(t, err)
			defer second.Rollback()
			// The first insert remains uncommitted while a different CN inserts a
			// distinct key. An accidentally retained table lock would block this.
			insertCtx, cancelInsert := context.WithTimeout(ctx, 20*time.Second)
			defer cancelInsert()
			_, err = second.ExecContext(insertCtx, "insert into "+table+" values (4,40,400)")
			require.NoError(t, err)
			for _, tableID := range []uint64{baseID, hiddenID} {
				locks := issue29602TargetLocks(services, tableID)
				require.NotEmpty(t, locks)
				for _, held := range locks {
					require.False(t, held.rangeLock, "ordinary INSERT must retain #26706 row-lock semantics")
					require.Len(t, held.keys, 1)
				}
			}
			require.NoError(t, second.Rollback())
			require.NoError(t, first.Rollback())
			issue29602RequireUnlocked(t, services, baseID)
			issue29602RequireUnlocked(t, services, hiddenID)
			var rows int
			require.NoError(t, creator.QueryRowContext(ctx, "select count(*) from "+table).Scan(&rows))
			require.Equal(t, 2, rows)
		})
	})
}

type issue29602HeldLock struct {
	keys      [][]byte
	mode      pblock.LockMode
	rangeLock bool
}

func issue29602TargetLocks(services []lockservice.LockService, target uint64) []issue29602HeldLock {
	var result []issue29602HeldLock
	for _, service := range services {
		service.IterLocks(func(tableID uint64, keys [][]byte, held lockservice.Lock) bool {
			if tableID != target {
				return true
			}
			active := false
			held.IterHolders(func(pblock.WaitTxn) bool { active = true; return false })
			held.IterWaiters(func(pblock.WaitTxn) bool { active = true; return false })
			if active {
				result = append(result, issue29602HeldLock{cloneIssue27487Keys(keys), held.GetLockMode(), held.IsRangeLock()})
			}
			return true
		})
	}
	return result
}

func issue29602RequireUnlocked(t *testing.T, services []lockservice.LockService, tableID uint64) {
	t.Helper()
	// Remote unlock delivery is asynchronous; observe actual holder/waiter
	// disappearance rather than relying on a scheduler sleep after COMMIT.
	require.Eventually(t, func() bool {
		return len(issue29602TargetLocks(services, tableID)) == 0
	}, 10*time.Second, 10*time.Millisecond, "transaction left locks on table %d", tableID)
}

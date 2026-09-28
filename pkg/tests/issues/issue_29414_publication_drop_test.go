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
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/lockservice"
	lockpb "github.com/matrixorigin/matrixone/pkg/pb/lock"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/stretchr/testify/require"
)

// A sys publication can name another account's database. Observe the exact
// mo_database lock wait before choosing the DROP outcome; a scheduler race alone
// cannot prove which operation won.
func TestIssue29414PublicationWaitsForPhysicalDatabaseDrop(t *testing.T) {
	runAuthenticatedClusterTest(t, func(cluster embed.Cluster) {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
		defer cancel()

		cn0, err := cluster.GetCNService(0)
		require.NoError(t, err)
		cn1, err := cluster.GetCNService(1)
		require.NoError(t, err)
		openDB := func(t *testing.T, port int64, user string) *sql.DB {
			t.Helper()
			db, openErr := sql.Open("mysql", fmt.Sprintf("%s:111@tcp(127.0.0.1:%d)/", user, port))
			require.NoError(t, openErr)
			t.Cleanup(func() { require.NoError(t, db.Close()) })
			return db
		}
		sys0 := openDB(t, cn0.GetServiceConfig().CN.Frontend.Port, "sys#root#moadmin")
		sys1 := openDB(t, cn1.GetServiceConfig().CN.Frontend.Port, "sys#root#moadmin")
		const accountName = "issue_29414_pub_tenant"
		cleanupCtx := func() (context.Context, context.CancelFunc) {
			return context.WithTimeout(context.Background(), 20*time.Second)
		}
		t.Cleanup(func() {
			cleanCtx, cleanCancel := cleanupCtx()
			defer cleanCancel()
			for _, arm := range []string{"rollback", "commit", "replace", "cancel"} {
				execSQLMaybe(t, cleanCtx, sys0, "drop publication if exists issue_29414_pub_"+arm)
			}
			execSQLMaybe(t, cleanCtx, sys0, "drop account if exists `"+accountName+"`")
		})
		for _, arm := range []string{"rollback", "commit", "replace", "cancel"} {
			execSQLMaybe(t, ctx, sys0, "drop publication if exists issue_29414_pub_"+arm)
		}
		execSQLMaybe(t, ctx, sys0, "drop account if exists `"+accountName+"`")
		execSQLRequire(t, ctx, sys0,
			"create account `"+accountName+"` admin_name = 'admin' identified by '111'")
		var accountID uint32
		require.NoError(t, sys0.QueryRowContext(ctx,
			"select account_id from mo_catalog.mo_account where account_name = ?", accountName,
		).Scan(&accountID))
		tenant := openDB(t, cn0.GetServiceConfig().CN.Frontend.Port, accountName+"#admin#accountadmin")

		for _, arm := range []string{"rollback", "commit", "replace", "cancel"} {
			t.Run(arm, func(t *testing.T) {
				dbName := "issue_29414_db_" + arm
				pubName := "issue_29414_pub_" + arm
				t.Cleanup(func() {
					cleanCtx, cleanCancel := cleanupCtx()
					defer cleanCancel()
					execSQLMaybe(t, cleanCtx, sys0, "drop publication if exists "+pubName)
					execSQLMaybe(t, cleanCtx, tenant, "drop database if exists `"+dbName+"`")
				})
				execSQLRequire(t, ctx, tenant, "create database `"+dbName+"`")
				var oldID uint64
				require.NoError(t, tenant.QueryRowContext(ctx,
					"select dat_id from mo_catalog.mo_database where datname = ?", dbName,
				).Scan(&oldID))
				// The publisher runs on the second CN. Make the fixture visible there
				// before starting the race, so its initial lookup reaches the lock.
				require.Eventually(t, func() bool {
					var seen int
					err := sys1.QueryRowContext(ctx,
						"select count(*) from mo_catalog.mo_database where account_id = ? and datname = ?",
						accountID, dbName,
					).Scan(&seen)
					return err == nil && seen == 1
				}, 20*time.Second, 10*time.Millisecond)

				publisher, err := sys1.Conn(ctx)
				require.NoError(t, err)
				defer func() { require.NoError(t, publisher.Close()) }()
				var publisherConnectionID uint64
				require.NoError(t, publisher.QueryRowContext(ctx,
					"select connection_id()").Scan(&publisherConnectionID))

				holder, err := tenant.Conn(ctx)
				require.NoError(t, err)
				holderOpen := false
				var pubDone chan error
				var pubCancel context.CancelFunc
				pubPending := false
				defer func() {
					if pubCancel != nil {
						pubCancel()
					}
					if pubPending {
						cleanCtx, cleanCancel := cleanupCtx()
						_, _ = sys1.ExecContext(cleanCtx, fmt.Sprintf("kill query %d", publisherConnectionID))
						cleanCancel()
					}
					if holderOpen {
						cleanCtx, cleanCancel := cleanupCtx()
						_, _ = holder.ExecContext(cleanCtx, "rollback")
						cleanCancel()
					}
					if pubPending {
						select {
						case <-pubDone:
						case <-time.After(20 * time.Second):
							t.Error("publication did not finish during cleanup")
						}
					}
					require.NoError(t, holder.Close())
				}()
				_, err = holder.ExecContext(ctx, "begin")
				require.NoError(t, err)
				holderOpen = true
				_, err = holder.ExecContext(ctx, "drop database `"+dbName+"`")
				require.NoError(t, err)

				pubCtx, cancelPub := context.WithTimeout(ctx, 45*time.Second)
				pubCancel = cancelPub
				pubDone = make(chan error, 1)
				pubPending = true
				go func() {
					_, createErr := publisher.ExecContext(pubCtx,
						"create publication "+pubName+" database `"+dbName+"` account `"+accountName+"`")
					pubDone <- createErr
				}()
				require.Eventually(t, func() bool {
					select {
					case createErr := <-pubDone:
						pubPending = false
						t.Errorf("publication finished before the held DROP was released: %v", createErr)
						return true
					default:
					}
					return issue29414HasDatabaseWaiter(cluster, dbName)
				}, 20*time.Second, 10*time.Millisecond,
					"publication did not wait on the physical source database")
				require.True(t, pubPending, "publication must still be waiting")

				if arm == "cancel" {
					_, err = sys1.ExecContext(ctx, fmt.Sprintf("kill query %d", publisherConnectionID))
					require.NoError(t, err)
				} else {
					command := "commit"
					if arm == "rollback" {
						command = "rollback"
					}
					_, err = holder.ExecContext(ctx, command)
					require.NoError(t, err)
					holderOpen = false
					if arm == "replace" {
						execSQLRequire(t, ctx, tenant, "create database `"+dbName+"`")
					}
				}
				select {
				case err = <-pubDone:
					pubPending = false
				case <-time.After(20 * time.Second):
					t.Fatal("publication did not finish after DROP resolution")
				}
				if arm == "rollback" {
					require.NoError(t, err)
				} else {
					require.Error(t, err)
				}
				var pubRows int
				var storedID uint64
				require.NoError(t, sys0.QueryRowContext(ctx,
					"select count(*), coalesce(max(database_id), 0) from mo_catalog.mo_pubs where account_id = 0 and pub_name = ?",
					pubName,
				).Scan(&pubRows, &storedID))
				if arm == "rollback" {
					require.Equal(t, 1, pubRows)
					require.Equal(t, oldID, storedID)
					_, err = tenant.ExecContext(ctx, "drop database `"+dbName+"`")
					require.Error(t, err, "committed publication must protect its source")
				} else {
					require.Zero(t, pubRows, "failed or canceled publication must leave no catalog reference")
				}
				if arm == "replace" {
					var newID uint64
					require.NoError(t, tenant.QueryRowContext(ctx,
						"select dat_id from mo_catalog.mo_database where datname = ?", dbName,
					).Scan(&newID))
					require.NotEqual(t, oldID, newID)
				}
				if arm == "cancel" {
					_, err = holder.ExecContext(ctx, "rollback")
					require.NoError(t, err)
					holderOpen = false
					// The lock service retires a canceled waiter when its holder
					// releases. Check that it cannot affect subsequent operations.
					require.Eventually(t, func() bool {
						return !issue29414HasDatabaseWaiter(cluster, dbName)
					}, 20*time.Second, 10*time.Millisecond)
					execSQLRequire(t, ctx, sys0,
						"create publication "+pubName+" database `"+dbName+"` account `"+accountName+"`")
					execSQLRequire(t, ctx, sys0, "drop publication "+pubName)
				}
			})
		}
	})
}

func issue29414HasDatabaseWaiter(cluster embed.Cluster, dbName string) bool {
	waiting := false
	cluster.ForeachServices(func(svc embed.ServiceOperator) bool {
		if svc.ServiceType() != metadata.ServiceType_CN {
			return true
		}
		lockService := lockservice.GetLockServiceByServiceID(svc.ServiceID())
		lockService.IterLocks(func(tableID uint64, keys [][]byte, lock lockservice.Lock) bool {
			if tableID != catalog.MO_DATABASE_ID {
				return true
			}
			matched := false
			for _, key := range keys {
				if bytes.Contains(key, []byte(dbName)) {
					matched = true
					break
				}
			}
			if !matched {
				return true
			}
			lock.IterWaiters(func(lockpb.WaitTxn) bool {
				waiting = true
				return false
			})
			return !waiting
		})
		return !waiting
	})
	return waiting
}

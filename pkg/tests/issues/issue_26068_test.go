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
	"encoding/hex"
	"fmt"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/lockservice"
	"github.com/matrixorigin/matrixone/pkg/pb/status"
	"github.com/matrixorigin/matrixone/pkg/queryservice"
	"github.com/stretchr/testify/require"
)

func TestIssue26068DataBranchDatabaseIdentityLifecycle(t *testing.T) {
	runAuthenticatedClusterTest(t, func(c embed.Cluster) {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
		defer cancel()

		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		dsn := fmt.Sprintf(
			"dump:111@tcp(127.0.0.1:%d)/?multiStatements=true",
			cn.GetServiceConfig().CN.Frontend.Port,
		)
		db, err := sql.Open("mysql", dsn)
		require.NoError(t, err)
		defer db.Close()
		db.SetMaxOpenConns(4)

		const (
			sourceDB       = "issue_26068_identity_source"
			branchDB       = "issue_26068_identity_branch"
			ordinaryDB     = "issue_26068_identity_ordinary"
			lockedDB       = "issue_26068_identity_locked"
			branchSnapshot = "issue_26068_identity_snapshot"
		)
		cleanup := func(cleanupCtx context.Context) {
			for _, name := range []string{branchDB, ordinaryDB, lockedDB, sourceDB} {
				execSQLMaybe(t, cleanupCtx, db, "drop database if exists `"+name+"`")
			}
			execSQLMaybe(t, cleanupCtx, db, "drop snapshot if exists "+branchSnapshot)
		}
		cleanup(ctx)
		defer func() {
			cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cleanupCancel()
			cleanup(cleanupCtx)
		}()

		execSQLRequire(t, ctx, db, "create database `"+sourceDB+"`")

		t.Run("marker is statement scoped", func(t *testing.T) {
			// Both statements must share one COM_QUERY. Separate Exec calls would
			// not exercise reuse of the frontend ExecCtx statement wrapper.
			execSQLRequire(t, ctx, db,
				"data branch create database `"+branchDB+"` from `"+sourceDB+"`; "+
					"create database `"+ordinaryDB+"`")

			var branchType, ordinaryType string
			require.NoError(t, db.QueryRowContext(ctx,
				"select coalesce(dat_type, '') from mo_catalog.mo_database where account_id=0 and datname=?",
				branchDB,
			).Scan(&branchType))
			require.NoError(t, db.QueryRowContext(ctx,
				"select coalesce(dat_type, '') from mo_catalog.mo_database where account_id=0 and datname=?",
				ordinaryDB,
			).Scan(&ordinaryType))
			require.Equal(t, "data-branch", branchType)
			require.Empty(t, ordinaryType)
		})

		t.Run("snapshot restore preserves marker", func(t *testing.T) {
			execSQLRequire(t, ctx, db, "create snapshot "+branchSnapshot+" for database `"+branchDB+"`")
			execSQLRequire(t, ctx, db, "drop database `"+branchDB+"`")
			execSQLRequire(t, ctx, db, "restore database `"+branchDB+"` {snapshot=\""+branchSnapshot+"\"}")

			var databaseType string
			require.NoError(t, db.QueryRowContext(ctx,
				"select coalesce(dat_type, '') from mo_catalog.mo_database where account_id=0 and datname=?",
				branchDB,
			).Scan(&databaseType))
			require.Equal(t, "data-branch", databaseType)

			execSQLRequire(t, ctx, db, "data branch delete database `"+branchDB+"`")
		})

		t.Run("delete validates after exclusive database lock", func(t *testing.T) {
			execSQLRequire(t, ctx, db,
				"data branch create database `"+lockedDB+"` from `"+sourceDB+"`")

			creator, err := db.Conn(ctx)
			require.NoError(t, err)
			defer creator.Close()
			deleter, err := db.Conn(ctx)
			require.NoError(t, err)
			defer deleter.Close()

			_, err = creator.ExecContext(ctx, "begin")
			require.NoError(t, err)
			creatorOpen := true
			defer func() {
				if creatorOpen {
					_, _ = creator.ExecContext(context.Background(), "rollback")
				}
			}()
			_, err = creator.ExecContext(ctx,
				"create table `"+lockedDB+"`.local_t(id int primary key)")
			require.NoError(t, err)

			var creatorConnectionID uint32
			require.NoError(t, creator.QueryRowContext(ctx,
				"select connection_id()",
			).Scan(&creatorConnectionID))
			sessionProvider, ok := cn.RawService().(interface {
				SessionMgr() *queryservice.SessionManager
			})
			require.True(t, ok, "CN service does not expose its session manager")
			sessionManager := sessionProvider.SessionMgr()
			require.NotNil(t, sessionManager)
			creatorTxnID, err := issue26068SessionTxnID(
				sessionManager.GetAllStatusSessions(),
				cn.ServiceID(),
				creatorConnectionID,
			)
			require.NoError(t, err)
			require.NotEmpty(t, creatorTxnID)
			creatorLockService := lockservice.GetLockServiceByServiceID(cn.ServiceID())
			require.NotNil(t, creatorLockService)

			deleteDone := make(chan error, 1)
			go func() {
				_, deleteErr := deleter.ExecContext(ctx,
					"data branch delete database `"+lockedDB+"`")
				deleteDone <- deleteErr
			}()
			var waitingListErr error
			require.Eventually(t, func() bool {
				found, waiting, queryErr := creatorLockService.GetWaitingList(ctx, creatorTxnID)
				waitingListErr = queryErr
				// DATA BRANCH DELETE uses a private background transaction, so its
				// lock-service transaction ID is intentionally not the outer SQL
				// session's ID. This isolated fixture has one contender for the
				// unique database key; require that exact cardinality instead.
				return queryErr == nil && found && len(waiting) == 1
			}, 30*time.Second, 10*time.Millisecond,
				"DATA BRANCH DELETE did not wait for the database lock: %v", waitingListErr)

			select {
			case deleteErr := <-deleteDone:
				t.Fatalf("DATA BRANCH DELETE returned before CREATE TABLE committed: %v", deleteErr)
			default:
			}

			_, err = creator.ExecContext(ctx, "commit")
			require.NoError(t, err)
			creatorOpen = false

			select {
			case deleteErr := <-deleteDone:
				require.ErrorContains(t, deleteErr,
					"DATA BRANCH DELETE target "+lockedDB+".local_t is not an active branch table")
			case <-time.After(30 * time.Second):
				t.Fatal("DATA BRANCH DELETE did not return after CREATE TABLE committed")
			}

			var tableCount int
			require.NoError(t, db.QueryRowContext(ctx,
				"select count(*) from mo_catalog.mo_tables where account_id=0 and reldatabase=? and relname='local_t'",
				lockedDB,
			).Scan(&tableCount))
			require.Equal(t, 1, tableCount)
		})
	})
}

func issue26068SessionTxnID(
	sessions []*status.Session,
	nodeID string,
	connectionID uint32,
) ([]byte, error) {
	var matched *status.Session
	for _, session := range sessions {
		if session == nil || session.NodeID != nodeID || session.ConnID != connectionID {
			continue
		}
		if matched != nil {
			return nil, fmt.Errorf(
				"found multiple sessions for node %q and connection %d",
				nodeID,
				connectionID,
			)
		}
		matched = session
	}
	if matched == nil {
		return nil, fmt.Errorf(
			"session for node %q and connection %d was not found",
			nodeID,
			connectionID,
		)
	}
	if matched.TxnID == "" {
		return nil, fmt.Errorf(
			"session for node %q and connection %d has an empty transaction ID",
			nodeID,
			connectionID,
		)
	}
	txnID, err := hex.DecodeString(matched.TxnID)
	if err != nil {
		return nil, fmt.Errorf("decode transaction ID %q: %w", matched.TxnID, err)
	}
	if len(txnID) != 16 {
		return nil, fmt.Errorf(
			"transaction ID for node %q and connection %d has length %d, want 16",
			nodeID,
			connectionID,
			len(txnID),
		)
	}
	allZero := true
	for _, b := range txnID {
		if b != 0 {
			allZero = false
			break
		}
	}
	if allZero {
		return nil, fmt.Errorf(
			"transaction ID for node %q and connection %d is zero",
			nodeID,
			connectionID,
		)
	}
	return txnID, nil
}

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
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/lockservice"
	lockpb "github.com/matrixorigin/matrixone/pkg/pb/lock"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/stretchr/testify/require"
)

func TestIssue27947FloatTableLockCoversSpecialKeys(t *testing.T) {
	runAuthenticatedClusterTest(t, func(c embed.Cluster) {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
		defer cancel()

		cn0, err := c.GetCNService(0)
		require.NoError(t, err)
		cn1, err := c.GetCNService(1)
		require.NoError(t, err)
		openDB := func(port int64) *sql.DB {
			db, openErr := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", port))
			require.NoError(t, openErr)
			db.SetMaxOpenConns(8)
			t.Cleanup(func() { require.NoError(t, db.Close()) })
			return db
		}
		ownerDB := openDB(cn0.GetServiceConfig().CN.Frontend.Port)
		remoteDB := openDB(cn1.GetServiceConfig().CN.Frontend.Port)

		const database = "issue_27947_float_lock"
		execSQLMaybe(t, ctx, ownerDB, "drop database if exists `"+database+"`")
		defer func() {
			cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cleanupCancel()
			execSQLMaybe(t, cleanupCtx, ownerDB, "drop database if exists `"+database+"`")
		}()
		execSQLRequire(t, ctx, ownerDB, "create database `"+database+"`")

		tests := []struct {
			name       string
			table      string
			sqlType    string
			typ        types.Type
			holderDB   *sql.DB
			encodeZero func(*types.Packer)
		}{
			{
				name:     "float32 local owner",
				table:    "f32_local",
				sqlType:  "float",
				typ:      types.T_float32.ToType(),
				holderDB: ownerDB,
				encodeZero: func(packer *types.Packer) {
					packer.EncodeFloat32(0)
				},
			},
			{
				name:     "float64 remote owner",
				table:    "f64_remote",
				sqlType:  "double",
				typ:      types.T_float64.ToType(),
				holderDB: remoteDB,
				encodeZero: func(packer *types.Packer) {
					packer.EncodeFloat64(0)
				},
			},
		}

		for _, test := range tests {
			t.Run(test.name, func(t *testing.T) {
				runIssue27947FloatLockScenario(t, ctx, c, ownerDB, test.holderDB,
					database, test.table, test.sqlType, test.typ, test.encodeZero)
			})
		}
	})
}

func runIssue27947FloatLockScenario(
	t *testing.T,
	ctx context.Context,
	c embed.Cluster,
	ownerDB *sql.DB,
	holderDB *sql.DB,
	database string,
	table string,
	sqlType string,
	typ types.Type,
	encodeZero func(*types.Packer),
) {
	t.Helper()
	qualified := "`" + database + "`.`" + table + "`"
	execSQLRequire(t, ctx, ownerDB,
		fmt.Sprintf("create table %s (k %s primary key, marker int)", qualified, sqlType))
	execSQLRequire(t, ctx, ownerDB, fmt.Sprintf(
		"insert into %s select cast(result as %s), result from generate_series(1, 10002) g",
		qualified, sqlType))
	execSQLRequire(t, ctx, ownerDB, fmt.Sprintf(
		"insert into %s values (cast('-Inf' as %s), -1), (cast('Inf' as %s), -2), (cast('NaN' as %s), -3)",
		qualified, sqlType, sqlType, sqlType))
	execSQLRequire(t, ctx, ownerDB, "analyze table "+qualified)

	var tableID uint64
	require.NoError(t, ownerDB.QueryRowContext(ctx,
		"select rel_id from mo_catalog.mo_tables where account_id = 0 and reldatabase = ? and relname = ?",
		database, table).Scan(&tableID))
	require.NotZero(t, tableID)

	// Establish the physical lock-table owner on CN0. The second scenario then
	// acquires the table range through CN1's remote proxy.
	warmup, err := ownerDB.BeginTx(ctx, nil)
	require.NoError(t, err)
	_, err = warmup.ExecContext(ctx, "select marker from "+qualified+" where marker = 1 for update")
	require.NoError(t, err)
	require.NoError(t, warmup.Rollback())

	minKey, maxKey := issue27947FullFloatRange(t, typ, encodeZero)
	specialKeys := []struct {
		name      string
		marker    int
		predicate string
	}{
		{name: "negative infinity", marker: -1, predicate: "k = cast('-Inf' as " + sqlType + ")"},
		{name: "positive infinity", marker: -2, predicate: "k = cast('Inf' as " + sqlType + ")"},
		// MatrixOne follows IEEE comparison semantics, so NaN is selected by
		// k != k rather than equality with another NaN value.
		{name: "NaN", marker: -3, predicate: "k != k"},
	}
	for _, special := range specialKeys {
		t.Run(special.name, func(t *testing.T) {
			holder, err := holderDB.BeginTx(ctx, nil)
			require.NoError(t, err)
			holderOpen := true
			defer func() {
				if holderOpen {
					_ = holder.Rollback()
				}
			}()
			rows, err := holder.QueryContext(ctx, "select marker from "+qualified+" for share")
			require.NoError(t, err)
			defer rows.Close()
			rowCount := 0
			for rows.Next() {
				var marker int
				require.NoError(t, rows.Scan(&marker))
				rowCount++
			}
			require.NoError(t, rows.Err())
			require.NoError(t, rows.Close())
			require.Equal(t, 10005, rowCount)

			require.Eventually(t, func() bool {
				return issue27947HasRange(c, tableID, minKey, maxKey)
			}, 10*time.Second, 10*time.Millisecond,
				"SELECT FOR SHARE did not acquire the complete FLOAT/DOUBLE table range")

			type updateResult struct {
				affected int64
				err      error
			}
			done := make(chan updateResult, 1)
			go func() {
				result, updateErr := ownerDB.ExecContext(ctx, fmt.Sprintf(
					"update %s set marker = marker - 10 where %s", qualified, special.predicate))
				if updateErr != nil {
					done <- updateResult{err: updateErr}
					return
				}
				affected, rowsErr := result.RowsAffected()
				done <- updateResult{affected: affected, err: rowsErr}
			}()

			require.Eventually(t, func() bool {
				return issue27947WaiterCount(c, tableID) > 0
			}, 10*time.Second, 10*time.Millisecond,
				"update of %s did not wait for the shared table range", special.name)
			select {
			case update := <-done:
				t.Fatalf("update of %s returned before the shared holder committed: %+v", special.name, update)
			default:
			}

			require.NoError(t, holder.Commit())
			holderOpen = false
			select {
			case update := <-done:
				require.NoError(t, update.err)
				require.Equal(t, int64(1), update.affected)
			case <-ctx.Done():
				t.Fatalf("update of %s did not resume after holder commit: %v", special.name, ctx.Err())
			}

			var updated int
			require.NoError(t, ownerDB.QueryRowContext(ctx,
				fmt.Sprintf("select count(*) from %s where marker = %d", qualified, special.marker-10)).Scan(&updated))
			require.Equal(t, 1, updated)
		})
	}
}

func issue27947FullFloatRange(
	t *testing.T,
	typ types.Type,
	encodeZero func(*types.Packer),
) ([]byte, []byte) {
	t.Helper()
	packer := types.NewPacker()
	defer packer.Close()
	encodeZero(packer)
	zeroKey := packer.Bytes()
	require.Equal(t, typ.Oid.TypeLen()+1, len(zeroKey))
	minKey := make([]byte, len(zeroKey))
	minKey[0] = zeroKey[0]
	maxKey := bytes.Repeat([]byte{0xff}, len(zeroKey))
	maxKey[0] = zeroKey[0]
	return minKey, maxKey
}

func issue27947HasRange(c embed.Cluster, tableID uint64, minKey, maxKey []byte) bool {
	found := false
	c.ForeachServices(func(svc embed.ServiceOperator) bool {
		if svc.ServiceType() != metadata.ServiceType_CN {
			return true
		}
		lockService := lockservice.GetLockServiceByServiceID(svc.ServiceID())
		lockService.IterLocks(func(lockedTableID uint64, keys [][]byte, _ lockservice.Lock) bool {
			if lockedTableID == tableID && len(keys) == 2 &&
				bytes.Equal(keys[0], minKey) && bytes.Equal(keys[1], maxKey) {
				found = true
				return false
			}
			return true
		})
		return !found
	})
	return found
}

func issue27947WaiterCount(c embed.Cluster, tableID uint64) int {
	waiters := 0
	c.ForeachServices(func(svc embed.ServiceOperator) bool {
		if svc.ServiceType() != metadata.ServiceType_CN {
			return true
		}
		lockService := lockservice.GetLockServiceByServiceID(svc.ServiceID())
		lockService.IterLocks(func(lockedTableID uint64, _ [][]byte, lock lockservice.Lock) bool {
			if lockedTableID != tableID {
				return true
			}
			lock.IterWaiters(func(_ lockpb.WaitTxn) bool {
				waiters++
				return true
			})
			return true
		})
		return true
	})
	return waiters
}

// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package v4_0_6

import (
	"encoding/hex"
	"fmt"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/stretchr/testify/require"
)

func TestMaintainOrphanObjectPrivilegesPage(t *testing.T) {
	const accountID = uint32(9)

	t.Run("candidate first classification and exact delete", func(t *testing.T) {
		candidates := []OrphanPrivilegeKey{
			{RoleID: 1, ObjectType: "database", ObjectID: 10, PrivilegeID: 1, PrivilegeLevel: "d"},
			{RoleID: 1, ObjectType: "database", ObjectID: 11, PrivilegeID: 1, PrivilegeLevel: "d"},
			{RoleID: 1, ObjectType: "table", ObjectID: 20, PrivilegeID: 2, PrivilegeLevel: "d.t"},
			{RoleID: 1, ObjectType: "table", ObjectID: 21, PrivilegeID: 2, PrivilegeLevel: "future"},
			{RoleID: 1, ObjectType: "view", ObjectID: 0, PrivilegeID: 3, PrivilegeLevel: "t"},
		}
		var candidateSQL, databaseSQL, relationSQL, deleteSQL string
		exec := executor.NewMemTxnExecutor(func(sql string) (executor.Result, error) {
			switch {
			case strings.Contains(sql, "order by "+orphanPrivilegePhysicalKeyColumn+" desc"):
				return newOrphanPrivilegeKeyResult(t, candidates[len(candidates)-1:]), nil
			case strings.HasPrefix(sql, "select "+orphanPrivilegeCandidateColumns+" from mo_catalog.mo_role_privs where"):
				candidateSQL = sql
				return newOrphanPrivilegeKeyResult(t, candidates), nil
			case strings.HasPrefix(sql, "select dat_id from mo_catalog.mo_database"):
				databaseSQL = sql
				return newOrphanPrivilegeObjectIDResult(t, 10), nil
			case strings.HasPrefix(sql, "select rel_logical_id from mo_catalog.mo_tables"):
				relationSQL = sql
				return executor.Result{}, nil
			case strings.HasPrefix(sql, "delete from mo_catalog.mo_role_privs"):
				deleteSQL = sql
				return executor.Result{AffectedRows: 2}, nil
			default:
				return executor.Result{}, fmt.Errorf("unexpected sql: %s", sql)
			}
		}, nil)

		next, complete, err := MaintainOrphanObjectPrivilegesPage(exec, accountID, OrphanPrivilegeScan{})
		require.NoError(t, err)
		require.True(t, complete)
		require.Zero(t, next)
		require.Contains(t, candidateSQL, "order by __mo_cpkey_col limit 1000")
		require.Contains(t, candidateSQL,
			"__mo_cpkey_col <= unhex('"+orphanPrivilegePhysicalKey(candidates[len(candidates)-1])+"')")
		require.NotContains(t, candidateSQL, "order by "+orphanPrivilegeKeyColumns)
		require.Contains(t, databaseSQL, "account_id = current_account_id()")
		require.Contains(t, databaseSQL, "dat_id in (10,11)")
		require.Contains(t, relationSQL, "rel_logical_id in (20)")
		require.Contains(t, deleteSQL, "(1,'database',11,1,'d')")
		require.Contains(t, deleteSQL, "(1,'table',20,2,'d.t')")
		require.NotContains(t, deleteSQL, "(1,'database',10,1,'d')")
		require.NotContains(t, deleteSQL, "future")
		require.NotContains(t, deleteSQL, "'view',0")
		require.Contains(t, deleteSQL, "limit 1000")
	})

	t.Run("full page advances examined physical cursor", func(t *testing.T) {
		candidates := make([]OrphanPrivilegeKey, orphanPrivilegePageSize)
		liveIDs := make([]uint64, orphanPrivilegePageSize)
		for i := range candidates {
			candidates[i] = OrphanPrivilegeKey{
				RoleID: 1, ObjectType: "database", ObjectID: uint64(i + 1),
				PrivilegeID: 1, PrivilegeLevel: "d",
			}
			liveIDs[i] = uint64(i + 1)
		}
		highWater := OrphanPrivilegeKey{
			RoleID: 2, ObjectType: "database", ObjectID: 2000, PrivilegeID: 1, PrivilegeLevel: "d",
		}
		cursor := OrphanPrivilegeKey{RoleID: 0}
		initial := OrphanPrivilegeScan{
			Initialized: true,
			CursorValid: true,
			Cursor:      orphanPrivilegePhysicalKey(cursor),
			HighWater:   orphanPrivilegePhysicalKey(highWater),
		}
		var candidateSQL string
		exec := executor.NewMemTxnExecutor(func(sql string) (executor.Result, error) {
			switch {
			case strings.HasPrefix(sql, "select "+orphanPrivilegeCandidateColumns+" from mo_catalog.mo_role_privs where"):
				candidateSQL = sql
				return newOrphanPrivilegeKeyResult(t, candidates), nil
			case strings.HasPrefix(sql, "select dat_id from mo_catalog.mo_database"):
				return newOrphanPrivilegeObjectIDResult(t, liveIDs...), nil
			default:
				return executor.Result{}, fmt.Errorf("unexpected sql: %s", sql)
			}
		}, nil)

		next, complete, err := MaintainOrphanObjectPrivilegesPage(exec, accountID, initial)
		require.NoError(t, err)
		require.False(t, complete)
		require.Equal(t, orphanPrivilegePhysicalKey(candidates[len(candidates)-1]), next.Cursor)
		require.True(t, next.CursorValid)
		require.Equal(t, orphanPrivilegePhysicalKey(highWater), next.HighWater)
		require.Contains(t, candidateSQL, "__mo_cpkey_col > unhex('"+orphanPrivilegePhysicalKey(cursor)+"')")
		require.Contains(t, candidateSQL, "__mo_cpkey_col <= unhex('"+orphanPrivilegePhysicalKey(highWater)+"')")
		require.Contains(t, candidateSQL, "order by __mo_cpkey_col limit 1000")
	})

	t.Run("inserts ahead of cursor can extend the high-water-bounded scan", func(t *testing.T) {
		highWater := OrphanPrivilegeKey{
			RoleID: 2, ObjectType: "account", ObjectID: 0, PrivilegeID: 1, PrivilegeLevel: "*",
		}
		var scan OrphanPrivilegeScan
		page := 0
		highWaterReads := 0
		var liveIDs []uint64
		exec := executor.NewMemTxnExecutor(func(sql string) (executor.Result, error) {
			switch {
			case strings.Contains(sql, "order by "+orphanPrivilegePhysicalKeyColumn+" desc"):
				highWaterReads++
				return newOrphanPrivilegeKeyResult(t, []OrphanPrivilegeKey{highWater}), nil
			case strings.HasPrefix(sql, "select "+orphanPrivilegeCandidateColumns):
				page++
				if page > 3 {
					return newOrphanPrivilegeKeyResult(t, []OrphanPrivilegeKey{highWater}), nil
				}
				// Model a fresh transaction snapshot receiving another full page
				// strictly after the committed cursor but below the fixed key bound.
				candidates := make([]OrphanPrivilegeKey, orphanPrivilegePageSize)
				liveIDs = make([]uint64, orphanPrivilegePageSize)
				for i := range candidates {
					objectID := uint64((page-1)*orphanPrivilegePageSize + i + 1)
					candidates[i] = OrphanPrivilegeKey{
						RoleID: 1, ObjectType: "database", ObjectID: objectID,
						PrivilegeID: 1, PrivilegeLevel: "d",
					}
					liveIDs[i] = objectID
				}
				firstKey := orphanPrivilegePhysicalKey(candidates[0])
				lastKey := orphanPrivilegePhysicalKey(candidates[len(candidates)-1])
				require.True(t, scan.Cursor == "" || firstKey > scan.Cursor,
					"each new snapshot page must be strictly ahead of the committed cursor")
				require.Less(t, strings.Compare(lastKey, orphanPrivilegePhysicalKey(highWater)), 0,
					"the injected page must remain below the fixed high-water")
				return newOrphanPrivilegeKeyResult(t, candidates), nil
			case strings.HasPrefix(sql, "select dat_id from mo_catalog.mo_database"):
				return newOrphanPrivilegeObjectIDResult(t, liveIDs...), nil
			default:
				return executor.Result{}, fmt.Errorf("unexpected sql: %s", sql)
			}
		}, nil)

		for i := 0; i < 3; i++ {
			next, complete, err := MaintainOrphanObjectPrivilegesPage(exec, accountID, scan)
			require.NoError(t, err)
			require.False(t, complete,
				"a fresh full page ahead of the cursor can keep this tenant selected")
			require.Equal(t, orphanPrivilegePhysicalKey(highWater), next.HighWater)
			require.NotEqual(t, scan.Cursor, next.Cursor)
			scan = next
		}
		require.Equal(t, 1, highWaterReads,
			"later page snapshots must reuse the key bound without reloading its snapshot")

		// Once inserts within the remaining key range stop, the fixed high-water
		// is reached and the same process-local traversal can complete.
		next, complete, err := MaintainOrphanObjectPrivilegesPage(exec, accountID, scan)
		require.NoError(t, err)
		require.True(t, complete)
		require.Zero(t, next)
	})

	t.Run("ring scans randomized suffix before wrapping to prefix", func(t *testing.T) {
		orphan := OrphanPrivilegeKey{
			RoleID: 2, ObjectType: "database", ObjectID: 2001, PrivilegeID: 1, PrivilegeLevel: "d",
		}
		prefix := OrphanPrivilegeKey{RoleID: 1, ObjectType: "account", PrivilegeLevel: "*"}
		start := orphanPrivilegePhysicalKey(orphan)
		reads := 0
		var statements []string
		exec := executor.NewMemTxnExecutor(func(sql string) (executor.Result, error) {
			statements = append(statements, sql)
			switch {
			case strings.Contains(sql, "order by "+orphanPrivilegePhysicalKeyColumn+" desc"):
				return newOrphanPrivilegeKeyResult(t, []OrphanPrivilegeKey{orphan}), nil
			case strings.HasPrefix(sql, "select "+orphanPrivilegeCandidateColumns):
				reads++
				if reads == 1 {
					return newOrphanPrivilegeKeyResult(t, []OrphanPrivilegeKey{orphan}), nil
				}
				return newOrphanPrivilegeKeyResult(t, []OrphanPrivilegeKey{prefix}), nil
			case strings.HasPrefix(sql, "select dat_id from mo_catalog.mo_database"):
				return executor.Result{}, nil
			case strings.HasPrefix(sql, "delete from mo_catalog.mo_role_privs"):
				return executor.Result{AffectedRows: 1}, nil
			default:
				return executor.Result{}, fmt.Errorf("unexpected sql: %s", sql)
			}
		}, nil)

		next, complete, err := MaintainOrphanObjectPrivilegesPage(exec, accountID, OrphanPrivilegeScan{Start: start})
		require.NoError(t, err)
		require.False(t, complete)
		require.True(t, next.Wrapped)
		require.False(t, next.CursorValid)
		require.Contains(t, statements[1], "__mo_cpkey_col >= unhex('"+start+"')")

		next, complete, err = MaintainOrphanObjectPrivilegesPage(exec, accountID, next)
		require.NoError(t, err)
		require.True(t, complete)
		require.Zero(t, next)
		require.Contains(t, statements[len(statements)-1], "__mo_cpkey_col < unhex('"+start+"')")
	})

	t.Run("empty table completes without delete", func(t *testing.T) {
		exec := executor.NewMemTxnExecutor(func(sql string) (executor.Result, error) {
			require.Contains(t, sql, "order by "+orphanPrivilegePhysicalKeyColumn+" desc limit 1")
			return executor.Result{}, nil
		}, nil)
		next, complete, err := MaintainOrphanObjectPrivilegesPage(exec, accountID, OrphanPrivilegeScan{})
		require.NoError(t, err)
		require.True(t, complete)
		require.Zero(t, next)
	})

	t.Run("rejects oversized candidate result", func(t *testing.T) {
		keys := make([]OrphanPrivilegeKey, orphanPrivilegePageSize+1)
		for i := range keys {
			keys[i] = OrphanPrivilegeKey{RoleID: int32(i)}
		}
		exec := executor.NewMemTxnExecutor(func(sql string) (executor.Result, error) {
			return newOrphanPrivilegeKeyResult(t, keys), nil
		}, nil)
		_, _, err := MaintainOrphanObjectPrivilegesPage(exec, accountID, OrphanPrivilegeScan{
			Initialized: true,
			HighWater:   orphanPrivilegePhysicalKey(OrphanPrivilegeKey{RoleID: 2000}),
		})
		require.ErrorContains(t, err, "candidate page has too many rows")
	})

	t.Run("rejects malformed physical cursor", func(t *testing.T) {
		_, _, err := MaintainOrphanObjectPrivilegesPage(nil, accountID, OrphanPrivilegeScan{Start: "not-hex"})
		require.ErrorContains(t, err, "invalid orphan privilege physical key")
	})
}

func newOrphanPrivilegeKeyResult(t *testing.T, keys []OrphanPrivilegeKey) executor.Result {
	t.Helper()
	mp := mpool.MustNewZeroNoFixed()
	t.Cleanup(func() { mpool.DeleteMPool(mp) })
	result := executor.NewMemResult([]types.Type{
		types.T_int32.ToType(),
		types.T_varchar.ToType(),
		types.T_uint64.ToType(),
		types.T_int32.ToType(),
		types.T_varchar.ToType(),
		types.T_varchar.ToType(),
	}, mp)
	result.NewBatchWithRowCount(len(keys))
	roleIDs := make([]int32, len(keys))
	objectTypes := make([]string, len(keys))
	objectIDs := make([]uint64, len(keys))
	privilegeIDs := make([]int32, len(keys))
	privilegeLevels := make([]string, len(keys))
	physicalKeys := make([]string, len(keys))
	for i, key := range keys {
		roleIDs[i] = key.RoleID
		objectTypes[i] = key.ObjectType
		objectIDs[i] = key.ObjectID
		privilegeIDs[i] = key.PrivilegeID
		privilegeLevels[i] = key.PrivilegeLevel
		physicalKeys[i] = orphanPrivilegePhysicalKey(key)
	}
	executor.AppendFixedRows(result, 0, roleIDs)
	require.NoError(t, executor.AppendStringRows(result, 1, objectTypes))
	executor.AppendFixedRows(result, 2, objectIDs)
	executor.AppendFixedRows(result, 3, privilegeIDs)
	require.NoError(t, executor.AppendStringRows(result, 4, privilegeLevels))
	require.NoError(t, executor.AppendStringRows(result, 5, physicalKeys))
	return result.GetResult()
}

func orphanPrivilegePhysicalKey(key OrphanPrivilegeKey) string {
	var buffer [OrphanPrivilegePhysicalKeyMaxSize]byte
	packer := types.NewPackerWithFixedBuffer(buffer[:])
	packer.EncodeInt32(key.RoleID)
	packer.EncodeStringType([]byte(key.ObjectType))
	packer.EncodeUint64(key.ObjectID)
	packer.EncodeInt32(key.PrivilegeID)
	packer.EncodeStringType([]byte(key.PrivilegeLevel))
	if packer.Err() != nil {
		panic(packer.Err())
	}
	return hex.EncodeToString(packer.GetBuf())
}

func newOrphanPrivilegeObjectIDResult(t *testing.T, ids ...uint64) executor.Result {
	t.Helper()
	mp := mpool.MustNewZeroNoFixed()
	t.Cleanup(func() { mpool.DeleteMPool(mp) })
	result := executor.NewMemResult([]types.Type{types.T_uint64.ToType()}, mp)
	result.NewBatchWithRowCount(len(ids))
	executor.AppendFixedRows(result, 0, ids)
	return result.GetResult()
}

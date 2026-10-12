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

package frontend

import (
	"errors"
	"slices"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/matrixorigin/matrixone/pkg/defines"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	"github.com/stretchr/testify/require"
)

func TestPrivilegeLevelProbeOrderAndScope(t *testing.T) {
	ses := newTestSession(t, gomock.NewController(t))
	t.Cleanup(ses.Close)
	ses.SetTenantInfo(getDefaultAccount())
	ses.SetDatabaseName("a'pp")
	ctx := defines.AttachAccountId(t.Context(), 0)
	for _, obj := range []objectType{objectTypeTable, objectTypeView, objectTypeDatabase, objectTypeAccount} {
		t.Run(obj.String(), func(t *testing.T) {
			entry := privilegeEntriesMap[PrivilegeTypeSelect]
			entry.objType, entry.databaseName, entry.tableName = obj, "", "t'able"
			if obj == objectTypeDatabase {
				entry.privilegeId = PrivilegeTypeShowTables
			} else if obj == objectTypeAccount {
				entry.privilegeId = PrivilegeTypeCreateDatabase
			}
			levels, err := getPrivilegeLevelsOfObjectType(ctx, obj)
			require.NoError(t, err)
			var queries []string
			var uniqueLevels []privilegeLevelType
			for _, pl := range levels {
				q, err := getSqlForPrivilege2(ctx, ses, 7, entry, pl)
				require.NoError(t, err)
				if !slices.Contains(queries, q) {
					queries = append(queries, q)
					uniqueLevels = append(uniqueLevels, pl)
				}
			}
			want := 3
			if obj == objectTypeAccount {
				want = 1
			}
			require.Len(t, queries, want)
			if obj == objectTypeTable || obj == objectTypeView {
				require.Contains(t, queries[2], "rel_logical_id")
				require.Contains(t, queries[1], "a\\'pp")
				require.Contains(t, queries[2], "t\\'able")
			}
			for hit := -1; hit < len(queries); hit++ {
				bh := &backgroundExecTest{}
				bh.init()
				for i, q := range queries {
					var rows [][]interface{}
					if i == hit {
						rows = [][]interface{}{{int64(entry.privilegeId), false}}
					}
					bh.sql2result[q] = newMrsForWithGrantOptionPrivilege(rows)
				}
				cache := &privilegeCache{}
				yes, err := verifyPrivilegeEntryInMultiPrivilegeLevels(ctx, bh, ses, cache, 7, entry, levels, true)
				require.NoError(t, err)
				require.Equal(t, hit >= 0, yes)
				count := len(queries)
				if hit >= 0 {
					count = hit + 1
				}
				require.Equal(t, queries[:count], bh.executedSQLs)
				if hit >= 0 {
					require.True(t, cache.has(obj, uniqueLevels[hit], "a'pp", entry.tableName, entry.privilegeId))
					if obj == objectTypeTable || obj == objectTypeView {
						otherObj := objectTypeTable
						if obj == objectTypeTable {
							otherObj = objectTypeView
						}
						require.False(t, cache.has(otherObj, uniqueLevels[hit], "a'pp", entry.tableName, entry.privilegeId))
						if hit > 0 {
							require.False(t, cache.has(obj, uniqueLevels[hit], "sibling", entry.tableName, entry.privilegeId))
						}
						if hit == 2 {
							require.False(t, cache.has(obj, uniqueLevels[hit], "a'pp", "sibling", entry.privilegeId))
						}
					}
				} else {
					// Misses are invocation-local: a later call must see a new grant.
					bh.sql2result[queries[0]] = newMrsForWithGrantOptionPrivilege([][]interface{}{{int64(entry.privilegeId), false}})
					bh.executedSQLs = nil
					yes, err = verifyPrivilegeEntryInMultiPrivilegeLevels(ctx, bh, ses, cache, 7, entry, levels, true)
					require.NoError(t, err)
					require.True(t, yes)
					require.Equal(t, queries[:1], bh.executedSQLs)
				}
				// Disabled and nil caches still deduplicate reads but publish no hits.
				for _, offCache := range []*privilegeCache{nil, {}} {
					bh.executedSQLs = nil
					yes, err = verifyPrivilegeEntryInMultiPrivilegeLevels(ctx, bh, ses, offCache, 7, entry, levels, false)
					require.NoError(t, err)
					require.True(t, yes)
					wantCount := count
					if hit < 0 {
						wantCount = 1
					}
					require.Equal(t, queries[:wantCount], bh.executedSQLs)
					if offCache != nil {
						for _, level := range uniqueLevels {
							require.False(t, offCache.has(obj, level, "a'pp", entry.tableName, entry.privilegeId))
						}
					}
				}
			}
		})
	}
}

func TestPrivilegeLevelProbeErrorsRemainOrdered(t *testing.T) {
	ses := newTestSession(t, gomock.NewController(t))
	t.Cleanup(ses.Close)
	ses.SetTenantInfo(getDefaultAccount())
	ses.SetDatabaseName("app")
	ctx := defines.AttachAccountId(t.Context(), 0)
	entry := privilegeEntriesMap[PrivilegeTypeSelect]
	entry.databaseName, entry.tableName = "app", "t"
	levels, _ := getPrivilegeLevelsOfObjectType(ctx, objectTypeTable)
	first, err := getSqlForPrivilege2(ctx, ses, 7, entry, levels[0])
	require.NoError(t, err)
	second, err := getSqlForPrivilege2(ctx, ses, 7, entry, levels[1])
	require.NoError(t, err)
	bh := &backgroundExecTest{}
	bh.init()
	bh.sql2result[first] = newMrsForWithGrantOptionPrivilege(nil)
	bh.sql2err[second] = errors.New("catalog read failed")
	cache := &privilegeCache{}
	cache.add(objectTypeTable, levels[3], "app", "t", entry.privilegeId)
	yes, err := verifyPrivilegeEntryInMultiPrivilegeLevels(ctx, bh, ses, cache, 7, entry, levels, true)
	require.ErrorContains(t, err, "catalog read failed")
	require.False(t, yes)
	require.Equal(t, []string{first, second}, bh.executedSQLs)
	// An early hit suppresses both later catalog errors and invalid levels.
	bh.executedSQLs = nil
	bh.sql2result[first] = newMrsForWithGrantOptionPrivilege([][]interface{}{{int64(entry.privilegeId), false}})
	yes, err = verifyPrivilegeEntryInMultiPrivilegeLevels(ctx, bh, ses, nil, 7, entry, append(levels[:1:1], privilegeLevelEnd), false)
	require.NoError(t, err)
	require.True(t, yes)
	require.Equal(t, []string{first}, bh.executedSQLs)
	bh.sql2result[first] = newMrsForWithGrantOptionPrivilege(nil)
	yes, err = verifyPrivilegeEntryInMultiPrivilegeLevels(ctx, bh, ses, nil, 7, entry, append(levels[:1:1], privilegeLevelEnd), false)
	require.Error(t, err)
	require.False(t, yes)
	// Malformed background results fail closed without publishing a cache hit.
	bad := mock_frontend.NewMockBackgroundExec(gomock.NewController(t))
	bad.EXPECT().ClearExecResultSet()
	bad.EXPECT().Exec(ctx, first).Return(nil)
	bad.EXPECT().GetExecResultSet().Return([]interface{}{"invalid"})
	cache.invalidate()
	yes, err = verifyPrivilegeEntryInMultiPrivilegeLevels(ctx, bad, ses, cache, 7, entry, levels, true)
	require.Error(t, err)
	require.False(t, yes)
	require.False(t, cache.has(objectTypeTable, levels[0], "app", "t", entry.privilegeId))
}

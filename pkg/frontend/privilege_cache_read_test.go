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
	"testing"

	"github.com/stretchr/testify/require"
)

// Each authorization starts cold; unsuccessful probes must not retain empty
// database/table trees for objects on which no privilege was found.
func TestPrivilegeCacheMissDoesNotAllocate(t *testing.T) {
	for _, typ := range []objectType{objectTypeTable, objectTypeView, objectTypeDatabase} {
		for _, level := range []privilegeLevelType{privilegeLevelDatabase, privilegeLevelDatabaseStar, privilegeLevelDatabaseTable, privilegeLevelTable} {
			var cache privilegeCache
			allocs := testing.AllocsPerRun(100, func() {
				cache.invalidate()
				if cache.has(typ, level, "db", "t", PrivilegeTypeSelect) {
					t.Fatal("empty cache authorized SELECT")
				}
			})
			if allocs != 0 {
				t.Errorf("object=%v level=%v: miss allocated %g times", typ, level, allocs)
			}
		}
	}
}

func BenchmarkPrivilegeCacheColdLookup(b *testing.B) {
	for _, typ := range []objectType{objectTypeTable, objectTypeView, objectTypeDatabase} {
		b.Run(typ.String(), func(b *testing.B) {
			var cache privilegeCache
			b.ReportAllocs()
			for b.Loop() {
				cache.invalidate()
				cache.has(typ, privilegeLevelDatabase, "db", "t", PrivilegeTypeSelect)
				cache.has(typ, privilegeLevelDatabaseStar, "db", "t", PrivilegeTypeSelect)
				cache.has(typ, privilegeLevelDatabaseTable, "db", "t", PrivilegeTypeSelect)
			}
		})
	}
}

func TestPrivilegeCacheDatabaseWildcardIsolation(t *testing.T) {
	for _, typ := range []objectType{objectTypeTable, objectTypeView} {
		for _, level := range []privilegeLevelType{privilegeLevelStar, privilegeLevelDatabaseStar} {
			var cache privilegeCache
			cache.add(typ, level, "allowed", "", PrivilegeTypeSelect)
			if !cache.has(typ, level, "allowed", "t", PrivilegeTypeSelect) {
				t.Fatal("lost authorized database wildcard")
			}
			if cache.has(typ, level, "denied", "t", PrivilegeTypeSelect) {
				t.Errorf("%v %v crossed database boundary", typ, level)
			}
		}
	}
}

func TestPrivilegeScopeAliasesDoNotRepeatCatalogQueries(t *testing.T) {
	for _, typ := range []objectType{objectTypeTable, objectTypeView} {
		t.Run(typ.String(), func(t *testing.T) {
			bh := &backgroundExecTest{}
			bh.init()
			ses := new(Session)
			entry := privilegeEntriesMap[PrivilegeTypeSelect]
			entry.objType = typ
			entry.databaseName = "app"
			entry.tableName = "t"
			levels, err := getPrivilegeLevelsOfObjectType(t.Context(), typ)
			require.NoError(t, err)
			var queries []string
			for _, level := range levels {
				q, err := getSqlForPrivilege2(t.Context(), ses, 3, entry, level)
				require.NoError(t, err)
				queries = append(queries, q)
				var rows [][]interface{}
				if level == privilegeLevelDatabaseTable {
					rows = [][]interface{}{{int64(PrivilegeTypeSelect)}}
				}
				bh.sql2result[q] = newMrsForRestoreStringRows([]string{"privilege"}, rows)
			}
			valid, err := verifyPrivilegeEntryInMultiPrivilegeLevels(t.Context(), bh, ses, nil, 3, entry, levels, false)
			require.NoError(t, err)
			require.True(t, valid)
			require.Len(t, bh.executedSQLs, 3, "direct grant must probe each distinct scope only once")
			require.Equal(t, queries, bh.executedSQLs)
		})
	}
}

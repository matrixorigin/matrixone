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

package frontend

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/stretchr/testify/require"
)

func TestBuildCatalogRestoreIdentityMap(t *testing.T) {
	identityMap, err := buildCatalogRestoreIdentityMap(
		[][]string{{"10", "app"}, {"11", "source_only"}},
		[][]string{{"20", "app"}, {"21", "target_only"}},
		[][]string{
			{"100", "app", "orders", catalog.SystemOrdinaryRel},
			{"101", "app", "report", catalog.SystemViewRel},
			{"102", "source_only", "omitted", catalog.SystemOrdinaryRel},
		},
		[][]string{
			{"200", "app", "orders", catalog.SystemOrdinaryRel},
			{"201", "app", "report", catalog.SystemViewRel},
			// The same name with a different object kind is not the same
			// authorization object.
			{"202", "source_only", "omitted", catalog.SystemViewRel},
		},
	)
	require.NoError(t, err)
	require.Equal(t, map[uint64]uint64{10: 20}, identityMap.databaseIDs)
	require.Equal(t, map[uint64]uint64{100: 200, 101: 201}, identityMap.objectIDs)
}

func TestRemapRolePrivilegeObjectID(t *testing.T) {
	identityMap := &catalogRestoreIdentityMap{
		databaseIDs: map[uint64]uint64{10: 20},
		objectIDs:   map[uint64]uint64{100: 200, 101: 201},
	}

	tests := []struct {
		name       string
		objectType string
		level      string
		objectID   uint64
		wantID     uint64
		wantFound  bool
		wantErr    bool
	}{
		{name: "account wildcard sentinel", objectType: objectTypeAccount.String(), level: privilegeLevelStar.String(), objectID: 0, wantFound: true},
		{name: "table all wildcard sentinel", objectType: objectTypeTable.String(), level: privilegeLevelStarStar.String(), objectID: 0, wantFound: true},
		{name: "database direct", objectType: objectTypeDatabase.String(), level: privilegeLevelDatabase.String(), objectID: 10, wantID: 20, wantFound: true},
		{name: "table current database", objectType: objectTypeTable.String(), level: privilegeLevelStar.String(), objectID: 10, wantID: 20, wantFound: true},
		{name: "view database wildcard", objectType: objectTypeView.String(), level: privilegeLevelDatabaseStar.String(), objectID: 10, wantID: 20, wantFound: true},
		{name: "table direct logical ID", objectType: objectTypeTable.String(), level: privilegeLevelDatabaseTable.String(), objectID: 100, wantID: 200, wantFound: true},
		{name: "view direct logical ID", objectType: objectTypeView.String(), level: privilegeLevelTable.String(), objectID: 101, wantID: 201, wantFound: true},
		{name: "omitted database", objectType: objectTypeDatabase.String(), level: privilegeLevelDatabase.String(), objectID: 11},
		{name: "omitted table", objectType: objectTypeTable.String(), level: privilegeLevelTable.String(), objectID: 102},
		{name: "invalid database level", objectType: objectTypeDatabase.String(), level: privilegeLevelDatabaseStar.String(), objectID: 10, wantErr: true},
		{name: "copied function identity", objectType: objectTypeFunction.String(), level: privilegeLevelRoutine.String(), objectID: 99, wantID: 99, wantFound: true},
		{name: "unsupported nonzero identity", objectType: objectTypeAccount.String(), level: privilegeLevelStar.String(), objectID: 99, wantErr: true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			gotID, gotFound, err := remapRolePrivilegeObjectID(rolePrivilegeRestoreRow{
				objectType:     test.objectType,
				objectID:       test.objectID,
				privilegeLevel: test.level,
			}, identityMap)
			if test.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, test.wantFound, gotFound)
			require.Equal(t, test.wantID, gotID)
		})
	}
}

func TestSystemCatalogTransformPoliciesHaveHandlers(t *testing.T) {
	require.NoError(t, validateSystemCatalogRestoreHandlers(t.Context()))
	handlers := make(map[string]struct{}, len(systemCatalogPostRestoreHandlers))
	for _, entry := range systemCatalogPostRestoreHandlers {
		require.Equal(t, systemCatalogRestoreCopyThenTransform, systemCatalogRestorePolicies[entry.tableName])
		_, duplicate := handlers[entry.tableName]
		require.False(t, duplicate)
		handlers[entry.tableName] = struct{}{}
	}
	for tableName, policy := range systemCatalogRestorePolicies {
		if policy == systemCatalogRestoreCopyThenTransform {
			_, ok := handlers[tableName]
			require.Truef(t, ok, "missing restore handler for %s", tableName)
		}
	}
}

func TestSystemCatalogRestorePoliciesCoverPredefinedTables(t *testing.T) {
	for tableName := range predefinedTables {
		_, ok := systemCatalogRestorePolicies[tableName]
		require.Truef(t, ok, "missing restore policy for predefined table %s", tableName)
	}
	for tableName := range sysAccountTables {
		_, ok := systemCatalogRestorePolicies[tableName]
		require.Truef(t, ok, "missing restore policy for sys table %s", tableName)
	}
}

func TestUnregisteredSystemCatalogRestorePolicyPreservesExistingDefaults(t *testing.T) {
	const futureCatalogTable = "mo_future_catalog_table"
	info := &tableInfo{dbName: moCatalog, tblName: futureCatalogTable, typ: "BASE TABLE"}

	// Preserve the historical runtime default for a catalog table introduced
	// before this registry is updated: system-database restore copies an
	// unregistered non-cluster table. The completeness test above makes every
	// table known at build time choose an explicit policy.
	require.False(t, needSkipTable(sysAccountID, moCatalog, futureCatalogTable))
	require.False(t, needSkipSystemTable(sysAccountID, info))
	require.True(t, needSkipTable(7, moCatalog, futureCatalogTable))
	require.False(t, needSkipSystemTable(7, info))
}

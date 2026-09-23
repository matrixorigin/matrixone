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
	"context"
	"errors"
	"fmt"
	"slices"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/pubsub"
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

func TestBuildCatalogRestoreIdentityMapRejectsMalformedRows(t *testing.T) {
	tests := []struct {
		name            string
		sourceDatabases [][]string
		targetDatabases [][]string
		sourceObjects   [][]string
		targetObjects   [][]string
	}{
		{name: "target database", targetDatabases: [][]string{{"invalid"}}},
		{name: "source database", sourceDatabases: [][]string{{"invalid"}}},
		{name: "target object", targetObjects: [][]string{{"invalid"}}},
		{name: "source object", sourceObjects: [][]string{{"invalid"}}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := buildCatalogRestoreIdentityMap(
				test.sourceDatabases,
				test.targetDatabases,
				test.sourceObjects,
				test.targetObjects,
			)
			require.Error(t, err)
		})
	}
}

func TestBuildCatalogRestoreNamedIdentityMap(t *testing.T) {
	identityMap, err := buildCatalogRestoreNamedIdentityMap(
		[][]string{{"7", "publisher_user"}, {"8", "source_only"}},
		[][]string{{"17", "publisher_user"}, {"18", "target_only"}},
	)
	require.NoError(t, err)
	require.Equal(t, map[uint32]uint32{7: 17}, identityMap)
}

func TestBuildCatalogRestoreNamedIdentityMapRejectsInvalidIDs(t *testing.T) {
	_, err := buildCatalogRestoreNamedIdentityMap(nil, [][]string{{"invalid", "user"}})
	require.Error(t, err)

	_, err = buildCatalogRestoreNamedIdentityMap([][]string{{"4294967296", "user"}}, nil)
	require.ErrorContains(t, err, "exceeds uint32")
}

func TestResolvePublicationRestoreIdentity(t *testing.T) {
	pubInfo := &pubsub.PubInfo{
		PubAccountName: "publisher",
		PubName:        "orders_pub",
		Creator:        7,
		Owner:          8,
	}
	targetAccounts := map[string]*pubsub.AccountInfo{
		"publisher": {Id: 20, Name: "publisher"},
	}
	principalMap := &catalogRestorePrincipalIdentityMap{
		userIDs: map[uint32]uint32{7: 17},
		roleIDs: map[uint32]uint32{8: 18},
	}

	identity, err := resolvePublicationRestoreIdentity(
		t.Context(), pubInfo, targetAccounts, principalMap,
	)
	require.NoError(t, err)
	require.Equal(t, publicationRestoreIdentity{accountID: 20, userID: 17, roleID: 18}, identity)

	delete(targetAccounts, "publisher")
	_, err = resolvePublicationRestoreIdentity(t.Context(), pubInfo, targetAccounts, principalMap)
	require.ErrorContains(t, err, "target account publisher does not exist")

	targetAccounts["publisher"] = &pubsub.AccountInfo{Id: 20, Name: "publisher"}
	delete(principalMap.userIDs, 7)
	_, err = resolvePublicationRestoreIdentity(t.Context(), pubInfo, targetAccounts, principalMap)
	require.ErrorContains(t, err, "creator user 7 does not exist")

	principalMap.userIDs[7] = 17
	delete(principalMap.roleIDs, 8)
	_, err = resolvePublicationRestoreIdentity(t.Context(), pubInfo, targetAccounts, principalMap)
	require.ErrorContains(t, err, "owner role 8 does not exist")
}

func TestLoadCatalogRestorePrincipalIdentityMap(t *testing.T) {
	const (
		snapshotTS    = int64(42)
		sourceAccount = uint32(10)
		targetAccount = uint32(20)
	)
	sourceUserSQL := fmt.Sprintf(
		"select cast(user_id as char), user_name from mo_catalog.mo_user {MO_TS = %d} order by user_id",
		snapshotTS,
	)
	targetUserSQL := "select cast(user_id as char), user_name from mo_catalog.mo_user order by user_id"
	sourceRoleSQL := fmt.Sprintf(
		"select cast(role_id as char), role_name from mo_catalog.mo_role {MO_TS = %d} order by role_id",
		snapshotTS,
	)
	targetRoleSQL := "select cast(role_id as char), role_name from mo_catalog.mo_role order by role_id"

	newBackgroundExec := func() *backgroundExecTest {
		bh := &backgroundExecTest{}
		bh.init()
		bh.sql2result[sourceUserSQL] = newMrsForRestoreStringRows(
			[]string{"user_id", "user_name"},
			[][]interface{}{{"7", "publisher_user"}},
		)
		bh.sql2result[targetUserSQL] = newMrsForRestoreStringRows(
			[]string{"user_id", "user_name"},
			[][]interface{}{{"17", "publisher_user"}},
		)
		bh.sql2result[sourceRoleSQL] = newMrsForRestoreStringRows(
			[]string{"role_id", "role_name"},
			[][]interface{}{{"8", "publisher_role"}},
		)
		bh.sql2result[targetRoleSQL] = newMrsForRestoreStringRows(
			[]string{"role_id", "role_name"},
			[][]interface{}{{"18", "publisher_role"}},
		)
		return bh
	}
	restoreContext := func(bh BackgroundExec) *systemCatalogRestoreContext {
		return &systemCatalogRestoreContext{
			ctx:           t.Context(),
			bh:            bh,
			snapshotTS:    snapshotTS,
			sourceAccount: sourceAccount,
			targetAccount: targetAccount,
		}
	}

	principalMap, err := loadCatalogRestorePrincipalIdentityMap(restoreContext(newBackgroundExec()))
	require.NoError(t, err)
	require.Equal(t, map[uint32]uint32{7: 17}, principalMap.userIDs)
	require.Equal(t, map[uint32]uint32{8: 18}, principalMap.roleIDs)

	for _, query := range []string{sourceUserSQL, targetUserSQL, sourceRoleSQL, targetRoleSQL} {
		t.Run(query, func(t *testing.T) {
			bh := newBackgroundExec()
			queryErr := errors.New("principal query failed")
			bh.sql2err[query] = queryErr
			_, err := loadCatalogRestorePrincipalIdentityMap(restoreContext(bh))
			require.ErrorIs(t, err, queryErr)
		})
	}
}

func TestCreatePubsValidatesTargetAccount(t *testing.T) {
	require.NoError(t, createPubs(t.Context(), "", nil, "snapshot", 42, nil))

	pubInfo := &pubsub.PubInfo{PubAccountName: "publisher", PubName: "orders_pub"}
	bh := &backgroundExecTest{}
	bh.init()
	bh.sql2result[getAccountIdNamesSql] = newMrsForGetAllAccounts(
		[][]interface{}{{uint64(1), "another_account", "open", uint64(1), nil}},
	)
	err := createPubs(t.Context(), "", bh, "snapshot", 42, []*pubsub.PubInfo{pubInfo})
	require.ErrorContains(t, err, "target account publisher does not exist")

	bh = &backgroundExecTest{}
	bh.init()
	accountErr := errors.New("account lookup failed")
	bh.sql2err[getAccountIdNamesSql] = accountErr
	err = createPubs(t.Context(), "", bh, "snapshot", 42, []*pubsub.PubInfo{pubInfo})
	require.ErrorIs(t, err, accountErr)
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
		{name: "invalid table level", objectType: objectTypeTable.String(), level: privilegeLevelRoutine.String(), objectID: 100, wantErr: true},
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

func TestSystemCatalogRebuildPoliciesHaveHandlers(t *testing.T) {
	require.NoError(t, validateSystemCatalogRestoreHandlers(t.Context()))
	handlers := make(map[string]struct{}, len(systemCatalogPostRestoreHandlers))
	for _, entry := range systemCatalogPostRestoreHandlers {
		require.Equal(t, systemCatalogRestoreRebuild, systemCatalogRestorePolicies[entry.tableName])
		require.True(t, needSkipTable(sysAccountID, moCatalog, entry.tableName))
		require.True(t, needSkipSystemTable(sysAccountID, &tableInfo{
			dbName: moCatalog, tblName: entry.tableName, typ: "BASE TABLE",
		}))
		_, duplicate := handlers[entry.tableName]
		require.False(t, duplicate)
		handlers[entry.tableName] = struct{}{}
	}
	for tableName, policy := range systemCatalogRestorePolicies {
		if policy == systemCatalogRestoreRebuild {
			_, ok := handlers[tableName]
			require.Truef(t, ok, "missing restore handler for %s", tableName)
		}
	}
}

func TestValidateSystemCatalogRestoreHandlersRejectsIncompleteRegistry(t *testing.T) {
	originalHandlers := systemCatalogPostRestoreHandlers
	t.Cleanup(func() {
		systemCatalogPostRestoreHandlers = originalHandlers
	})

	systemCatalogPostRestoreHandlers = append(
		slices.Clone(originalHandlers),
		systemCatalogPostRestoreHandler{tableName: "mo_user"},
	)
	require.ErrorContains(t, validateSystemCatalogRestoreHandlers(t.Context()), "has no rebuild policy")
	require.ErrorContains(t,
		restoreSystemCatalogsAfterObjects(t.Context(), "", nil, 0, 0, 0),
		"has no rebuild policy",
	)

	systemCatalogPostRestoreHandlers = append(slices.Clone(originalHandlers), originalHandlers[0])
	require.ErrorContains(t, validateSystemCatalogRestoreHandlers(t.Context()), "has multiple handlers")

	handlerErr := errors.New("handler failed")
	systemCatalogPostRestoreHandlers = []systemCatalogPostRestoreHandler{{
		tableName: "mo_role_privs",
		handler: func(*systemCatalogRestoreContext) error {
			return handlerErr
		},
	}}
	require.ErrorIs(t, restoreSystemCatalogsAfterObjects(t.Context(), "", nil, 0, 0, 0), handlerErr)

	systemCatalogPostRestoreHandlers = originalHandlers
	originalPolicy := systemCatalogRestorePolicies["mo_user"]
	systemCatalogRestorePolicies["mo_user"] = systemCatalogRestoreRebuild
	t.Cleanup(func() {
		systemCatalogRestorePolicies["mo_user"] = originalPolicy
	})
	require.ErrorContains(t, validateSystemCatalogRestoreHandlers(t.Context()), "has no handler")
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

func TestCatalogRestoreIdentityParsingRejectsInvalidRows(t *testing.T) {
	_, err := parseCatalogID([]string{"1"}, 2)
	require.ErrorContains(t, err, "invalid catalog identity row")

	_, err = parseCatalogID([]string{"not-an-id", "db"}, 2)
	require.Error(t, err)

	_, err = parseCatalogObjectIdentity([]string{"1", "db", "table"})
	require.ErrorContains(t, err, "invalid catalog identity row")
}

func TestLoadRolePrivilegesAtSnapshotValidatesCatalogRows(t *testing.T) {
	const snapshotTS = int64(42)
	query := fmt.Sprintf(
		"select cast(role_id as char), role_name, obj_type, cast(obj_id as char), "+
			"cast(privilege_id as char), privilege_name, privilege_level, "+
			"cast(coalesce(operation_user_id, 0) as char), cast(granted_time as char), "+
			"cast(with_grant_option as char) from mo_catalog.mo_role_privs {MO_TS = %d} "+
			"order by role_id, obj_type, obj_id, privilege_id, privilege_level",
		snapshotTS,
	)
	validRow := []interface{}{
		"1", "reader", objectTypeTable.String(), "100", "2", "select",
		privilegeLevelTable.String(), "3", "2026-08-04 12:00:00", "true",
	}
	tests := []struct {
		name            string
		column          int
		value           string
		wantErr         bool
		wantGrantOption bool
	}{
		{name: "invalid role ID", column: 0, value: "invalid", wantErr: true},
		{name: "invalid object ID", column: 3, value: "invalid", wantErr: true},
		{name: "invalid privilege ID", column: 4, value: "invalid", wantErr: true},
		{name: "invalid operation user ID", column: 7, value: "invalid", wantErr: true},
		{name: "invalid grant option", column: 9, value: "invalid", wantErr: true},
		{name: "numeric false grant option", column: 9, value: "0"},
		{name: "numeric true grant option", column: 9, value: "1", wantGrantOption: true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			row := slices.Clone(validRow)
			row[test.column] = test.value
			bh := &backgroundExecTest{}
			bh.init()
			bh.sql2result[query] = newMrsForRestoreStringRows(
				[]string{
					"role_id", "role_name", "obj_type", "obj_id", "privilege_id",
					"privilege_name", "privilege_level", "operation_user_id", "granted_time",
					"with_grant_option",
				},
				[][]interface{}{row},
			)
			rows, err := loadRolePrivilegesAtSnapshot(&systemCatalogRestoreContext{
				ctx:           context.Background(),
				bh:            bh,
				snapshotTS:    snapshotTS,
				sourceAccount: 1,
				targetAccount: 2,
			})
			if test.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Len(t, rows, 1)
			require.Equal(t, test.wantGrantOption, rows[0].withGrantOption)
		})
	}
}

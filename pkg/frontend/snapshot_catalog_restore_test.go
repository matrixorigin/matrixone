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
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/defines"
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

func TestCatalogRestoreIdentityMapUsesQualifiedNames(t *testing.T) {
	identityMap, err := buildCatalogRestoreIdentityMap(
		[][]string{{"10", "app"}, {"11", "other"}},
		[][]string{{"20", "other"}, {"21", "app"}},
		[][]string{
			{"100", "app", "t", catalog.SystemOrdinaryRel},
			{"101", "other", "t", catalog.SystemOrdinaryRel},
			{"102", "app", "missing", catalog.SystemOrdinaryRel},
		},
		[][]string{
			{"100", "other", "t", catalog.SystemOrdinaryRel},
			{"201", "app", "t", catalog.SystemOrdinaryRel},
			// A coincident numeric ID cannot rescue a missing qualified name.
			{"102", "other", "missing", catalog.SystemOrdinaryRel},
		},
	)
	require.NoError(t, err)
	require.Equal(t, map[uint64]uint64{10: 21, 11: 20}, identityMap.databaseIDs)
	require.Equal(t, map[uint64]uint64{100: 201, 101: 100}, identityMap.objectIDs)
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

func TestRolePrivilegesSkipBulkRestore(t *testing.T) {
	for _, account := range []uint32{sysAccountID, 7} {
		require.True(t, needSkipTable(account, moCatalog, "mo_role_privs"))
		require.True(t, needSkipSystemTable(account, &tableInfo{dbName: moCatalog, tblName: "mo_role_privs", typ: "BASE TABLE"}))
	}
}

type privilegeRestoreExec struct {
	*backgroundExecTest
	t       *testing.T
	calls   int
	failAt  int
	failure error
}

func (b *privilegeRestoreExec) Exec(ctx context.Context, sql string) error {
	b.t.Helper()
	account, err := defines.GetAccountId(ctx)
	require.NoError(b.t, err)
	require.Equal(b.t, uint32(20), account)
	b.calls++
	if b.calls == b.failAt {
		return b.failure
	}
	return b.backgroundExecTest.Exec(ctx, sql)
}

func (b *privilegeRestoreExec) ExecRestore(ctx context.Context, sql string, from, to uint32) error {
	b.t.Helper()
	account, err := defines.GetAccountId(ctx)
	require.NoError(b.t, err)
	require.Equal(b.t, uint32(10), account)
	require.Equal(b.t, uint32(10), from)
	require.Equal(b.t, uint32(20), to)
	b.calls++
	if b.calls == b.failAt {
		return b.failure
	}
	return b.backgroundExecTest.ExecRestore(ctx, sql, from, to)
}

func TestRestoreAccountPrivileges(t *testing.T) {
	newExec := func(t *testing.T, rows [][]interface{}) *privilegeRestoreExec {
		bh := &backgroundExecTest{}
		bh.init()
		bh.sql2result["select cast(dat_id as char), datname from mo_catalog.mo_database {MO_TS = 42} where account_id = 10 order by dat_id"] =
			newMrsForRestoreStringRows([]string{"id", "name"}, [][]interface{}{{"11", "app"}})
		bh.sql2result["select cast(dat_id as char), datname from mo_catalog.mo_database where account_id = 20 order by dat_id"] =
			newMrsForRestoreStringRows([]string{"id", "name"}, [][]interface{}{{"22", "app"}})
		bh.sql2result["select cast(coalesce(rel_logical_id, rel_id) as char), reldatabase, relname, relkind from mo_catalog.mo_tables {MO_TS = 42} where account_id = 10 order by rel_id"] =
			newMrsForRestoreStringRows([]string{"id", "db", "name", "kind"}, [][]interface{}{{"111", "app", "t", catalog.SystemOrdinaryRel}})
		bh.sql2result["select cast(coalesce(rel_logical_id, rel_id) as char), reldatabase, relname, relkind from mo_catalog.mo_tables where account_id = 20 order by rel_id"] =
			newMrsForRestoreStringRows([]string{"id", "db", "name", "kind"}, [][]interface{}{{"222", "app", "t", catalog.SystemOrdinaryRel}})
		bh.sql2result["select cast(role_id as char), role_name, obj_type, cast(obj_id as char), cast(privilege_id as char), privilege_name, privilege_level, cast(coalesce(operation_user_id, 0) as char), cast(granted_time as char), cast(with_grant_option as char) from mo_catalog.mo_role_privs {MO_TS = 42} order by role_id, obj_type, obj_id, privilege_id, privilege_level"] =
			newMrsForRestoreStringRows([]string{"role", "name", "type", "obj", "priv", "privname", "level", "user", "time", "grant"}, rows)
		return &privilegeRestoreExec{backgroundExecTest: bh, t: t, failure: errors.New("restore query failed")}
	}
	row := []interface{}{"7", "reader", "table", "111", "2", "select", "d.t", "3", "2026-09-28 00:00:00", "true"}
	const deleteSQL = "delete from mo_catalog.mo_role_privs"
	t.Run("remap and preserve grant metadata", func(t *testing.T) {
		bh := newExec(t, [][]interface{}{row})
		require.NoError(t, restoreAccountPrivileges(t.Context(), bh, 42, 10, 20))
		require.Equal(t, deleteSQL, bh.executedSQLs[5])
		require.Contains(t, bh.executedSQLs[6], "(7,'reader','table',222,2,'select','d.t',3,'2026-09-28 00:00:00',true)")
	})
	t.Run("empty replaces target grants", func(t *testing.T) {
		bh := newExec(t, nil)
		require.NoError(t, restoreAccountPrivileges(t.Context(), bh, 42, 10, 20))
		require.Len(t, bh.executedSQLs, 6)
		require.Equal(t, deleteSQL, bh.executedSQLs[5])
	})
	t.Run("omitted source object does not retain stale ID", func(t *testing.T) {
		missing := slices.Clone(row)
		missing[3] = "999"
		bh := newExec(t, [][]interface{}{missing})
		require.NoError(t, restoreAccountPrivileges(t.Context(), bh, 42, 10, 20))
		require.Len(t, bh.executedSQLs, 6)
	})
	t.Run("validate before replacing target grants", func(t *testing.T) {
		invalid := slices.Clone(row)
		invalid[6] = "invalid"
		bh := newExec(t, [][]interface{}{row, invalid})
		require.Error(t, restoreAccountPrivileges(t.Context(), bh, 42, 10, 20))
		require.NotContains(t, bh.executedSQLs, deleteSQL)
	})
	for _, count := range []int{255, 256, 257, 512} {
		t.Run(fmt.Sprintf("batch boundary %d and escaped names", count), func(t *testing.T) {
			rows := make([][]interface{}, count)
			for i := range rows {
				rows[i] = slices.Clone(row)
				rows[i][0] = fmt.Sprint(i + 1)
				rows[i][1] = "read'er"
			}
			bh := newExec(t, rows)
			require.NoError(t, restoreAccountPrivileges(t.Context(), bh, 42, 10, 20))
			require.Len(t, bh.executedSQLs, 6+(count+255)/256)
			for i, sql := range bh.executedSQLs[6:] {
				require.Equal(t, min(256, count-i*256), strings.Count(sql, "'read''er'"))
			}
		})
	}
	t.Run("omitted grants do not skip neighboring valid rows", func(t *testing.T) {
		missing := slices.Clone(row)
		missing[3] = "999"
		withoutGrant := slices.Clone(row)
		withoutGrant[0] = "8"
		withoutGrant[9] = "false"
		bh := newExec(t, [][]interface{}{missing, row, missing, withoutGrant, missing})
		require.NoError(t, restoreAccountPrivileges(t.Context(), bh, 42, 10, 20))
		require.Len(t, bh.executedSQLs, 7)
		require.Contains(t, bh.executedSQLs[6], "(7,'reader','table',222,2,'select','d.t',3,'2026-09-28 00:00:00',true)")
		require.Contains(t, bh.executedSQLs[6], "(8,'reader','table',222,2,'select','d.t',3,'2026-09-28 00:00:00',false)")
		require.NotContains(t, bh.executedSQLs[6], "999")
	})
	for _, failure := range []error{errors.New("restore query failed"), context.Canceled, context.DeadlineExceeded} {
		for failAt := 1; failAt <= 8; failAt++ {
			t.Run(fmt.Sprintf("query %d propagates %v", failAt, failure), func(t *testing.T) {
				rows := make([][]interface{}, 257)
				for i := range rows {
					rows[i] = slices.Clone(row)
					rows[i][0] = fmt.Sprint(i + 1)
				}
				bh := newExec(t, rows)
				bh.failAt = failAt
				bh.failure = failure
				require.ErrorIs(t, restoreAccountPrivileges(t.Context(), bh, 42, 10, 20), bh.failure)
				require.Equal(t, failAt, bh.calls)
			})
		}
	}
}

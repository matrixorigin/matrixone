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
	"context"
	"errors"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/prashantv/gostub"
	"github.com/stretchr/testify/require"
)

func TestDatabaseDefaultsRestoreMetadata(t *testing.T) {
	setProtocolVersionForTest(t, "", defines.MORPCVersion95)
	const checkSQL = "select relname from mo_catalog.mo_tables {MO_TS = 42} where account_id = 7 and reldatabase = 'mo_catalog' and relname = 'mo_database_defaults'"
	const readSQL = "select dd.character_set, dd.collation_name from mo_catalog.mo_database {MO_TS = 42} db join mo_catalog.mo_database_defaults {MO_TS = 42} dd on dd.account_id=db.account_id and dd.database_id=db.dat_id where db.account_id=7 and db.datname='source'"
	injected := errors.New("snapshot read unavailable")
	for _, tc := range []struct {
		name              string
		legacy            bool
		rows              [][]interface{}
		checkErr, readErr error
		wantErr           string
	}{
		{name: "bin survives recovery", rows: [][]interface{}{{"utf8mb4", "utf8mb4_bin"}}},
		{name: "legacy snapshot", legacy: true},
		{name: "legacy database"},
		{name: "existence query error", checkErr: injected, wantErr: "snapshot read unavailable"},
		{name: "catalog read error", readErr: injected, wantErr: "snapshot read unavailable"},
		{name: "invalid metadata", rows: [][]interface{}{{"utf8mb4", "bogus"}}, wantErr: "invalid database default metadata"},
		{name: "duplicate metadata", rows: [][]interface{}{{"utf8mb4", "utf8mb4_bin"}, {"utf8mb4", "utf8mb4_bin"}}, wantErr: "invalid database default metadata"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			base := &backgroundExecTest{}
			base.init()
			bh := &accountRecordingBackgroundExec{backgroundExecTest: base}
			exists := [][]interface{}{{"mo_database_defaults"}}
			if tc.legacy {
				exists = nil
			}
			base.sql2result[checkSQL] = newMrsForSqlForShowDatabases(exists)
			base.sql2result[readSQL] = defaultsRestoreRows(tc.rows)
			base.sql2err[checkSQL] = tc.checkErr
			base.sql2err[readSQL] = tc.readErr
			got, err := readDatabaseDefaultsForRestore(context.Background(), bh, "source", 7, 42)
			require.Equal(t, uint32(7), bh.accountID)
			if tc.wantErr != "" {
				require.ErrorContains(t, err, tc.wantErr)
				require.Nil(t, got)
				return
			}
			require.NoError(t, err)
			if tc.legacy || len(tc.rows) == 0 {
				require.Nil(t, got)
			} else {
				require.Equal(t, "CREATE DATABASE IF NOT EXISTS `target` character set utf8mb4 collate utf8mb4_bin", appendDatabaseDefaultsSQL(createDatabaseIfNotExistsSQL("target"), got))
				require.Zero(t, got.DatabaseId, "source IDs must not be copied to target")
			}
			if tc.legacy {
				require.Equal(t, []string{checkSQL}, base.executedSQLs)
			} else {
				require.Equal(t, []string{checkSQL, readSQL}, base.executedSQLs)
			}
		})
	}
	require.Equal(t, systemCatalogRestoreSkip, systemCatalogRestorePolicies[catalog.MODatabaseDefaults])
}

func defaultsRestoreRows(rows [][]interface{}) *MysqlResultSet {
	m := &MysqlResultSet{}
	for _, name := range []string{"character_set", "collation_name"} {
		c := &MysqlColumn{}
		c.SetName(name)
		c.SetColumnType(defines.MYSQL_TYPE_VARCHAR)
		m.AddColumn(c)
	}
	for _, r := range rows {
		m.AddRow(r)
	}
	return m
}

func TestDatabaseDefaultsRestorePreflight(t *testing.T) {
	definition := logicalRestoreDatabaseDefinition{defaults: &plan.DatabaseDefaults{CharacterSet: "utf8mb4", Collation: "utf8mb4_bin", Version: 1}}
	for _, version := range []int64{defines.MORPCVersion94, defines.MORPCVersion95} {
		_, err := prepareLogicalRestoreDatabase(t.Context(), "d", definition, version)
		if version < defines.MORPCVersion95 {
			require.ErrorContains(t, err, "protocol version 95")
		} else {
			require.NoError(t, err)
		}
	}
	visited := false
	err := preflightLogicalRestoreDatabases(t.Context(), []string{"mo_catalog", "d"}, defines.MORPCVersion94, func(name string) (logicalRestoreDatabaseDefinition, error) {
		visited = true
		require.Equal(t, "d", name)
		return definition, nil
	})
	require.True(t, visited)
	require.ErrorContains(t, err, "protocol version 95")
	require.Equal(t, "create database d", appendDatabaseDefaultsSQL("create database d", nil))
}

func TestDatabaseDefaultsStatementAdmission(t *testing.T) {
	for _, sql := range []string{"alter database target collate utf8mb4_bin", "alter database collate utf8mb4_bin"} {
		stmt, err := parsers.ParseOne(t.Context(), dialect.MYSQL, sql, 1)
		require.NoError(t, err)
		p := determinePrivilegeSetOfStatement(stmt)
		require.Equal(t, objectTypeDatabase, p.objType)
		require.True(t, p.writeDatabaseAndTableDirectly)
		require.Len(t, p.entries, 3)
		require.Equal(t, PrivilegeTypeAlterObject, p.entries[0].privilegeId)
		require.True(t, requiresPessimisticObjectLifecycleTxn(nil, stmt, ""))
		stmt.Free()
	}
	for _, p := range []*plan.Plan{
		{Plan: &plan.Plan_Ddl{Ddl: &plan.DataDefinition{Definition: &plan.DataDefinition_CreateTable{CreateTable: &plan.CreateTable{DatabaseDefaults: &plan.DatabaseDefaults{DatabaseId: 42}}}}}},
		{Plan: &plan.Plan_Ddl{Ddl: &plan.DataDefinition{Definition: &plan.DataDefinition_AlterDatabase{AlterDatabase: &plan.AlterDatabase{Database: "d"}}}}},
	} {
		require.True(t, shouldRebuildPreparePlan(false, p))
	}
}

func TestAlterDatabasePreparedRechecksCachedPrivilege(t *testing.T) {
	cacheStub := gostub.Stub(&privilegeCacheIsEnabled, func(context.Context, *Session) (bool, error) { return true, nil })
	defer cacheStub.Reset()
	for _, binary := range []bool{false, true} {
		t.Run(map[bool]string{false: "text execute", true: "binary execute"}[binary], func(t *testing.T) {
			ses, prepared, _, bh := newPreparedAuthorizationFixture(t, "alter database db1 collate utf8mb4_bin")
			defer prepared.Close()
			inner := &plan.Plan{Plan: &plan.Plan_Ddl{Ddl: &plan.DataDefinition{DdlType: plan.DataDefinition_ALTER_DATABASE, Definition: &plan.DataDefinition_AlterDatabase{AlterDatabase: &plan.AlterDatabase{Database: "db1"}}}}}
			installPreparedTableScanPlan(t, ses, prepared, inner)
			cw, execCtx := newPreparedAuthorizationWrapper(ses, prepared, inner, binary)
			configurePreparedAuthorizationSession(t, ses, execCtx)
			// A previous successful statement left permission in the session cache;
			// the committed catalog now contains no database grant.
			ses.cache.add(objectTypeDatabase, privilegeLevelDatabase, "db1", "", PrivilegeTypeDatabaseAll)
			bh.sql2result[getSqlForCheckUserGrantForAuthorization(3, 2)] = newMrsForCheckUserGrant([][]interface{}{{int64(3), int64(2), false}})
			stub := gostub.StubFunc(&NewBackgroundExec, bh)
			defer stub.Reset()
			_, err := cw.Compile(execCtx, nil)
			require.ErrorContains(t, err, "privilege")
			require.NotEmpty(t, bh.executedSQLs, "execute must consult current grants")
		})
	}
}

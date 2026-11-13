// Copyright 2021 - 2024 Matrix Origin
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
	"fmt"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/stretchr/testify/require"
)

func TestMarkedDatabaseRestorePreservesIdentity(t *testing.T) {
	ctx := defines.AttachAccountId(context.Background(), uint32(sysAccountID))
	const (
		dbName       = "issue26068_restore"
		snapshotName = "issue26068_snapshot"
		pitrName     = "issue26068_pitr"
		ts           = int64(100)
	)
	createSQL := createDatabaseIfNotExistsSQL(dbName)
	masterSQL := fmt.Sprintf(
		checkDatabaseIsMasterFormat,
		quoteSQLStringLiteral(dbName), quoteSQLStringLiteral(dbName),
	)
	publicationSQL := fmt.Sprintf(getPubInfoSql, uint32(sysAccountID)) +
		" and database_name = '" + dbName + "'"

	tests := []struct {
		name      string
		query     string
		tablesSQL string
		prepare   func(*backgroundExecTest)
		run       func(BackgroundExec) error
	}{
		{
			name: "snapshot",
			query: fmt.Sprintf(
				"select datname, dat_createsql, dat_type from mo_catalog.mo_database {snapshot = '%s'} where datname = '%s' and account_id = 0",
				snapshotName, dbName,
			),
			tablesSQL: buildTableInfoListSQL(dbName, "", ts, uint32(sysAccountID)),
			run: func(bh BackgroundExec) error {
				return restoreToDatabaseOrTable(
					ctx, "", bh, snapshotName, dbName, "", uint32(sysAccountID),
					map[string]*tableInfo{}, map[string]*tableInfo{}, ts,
					uint32(sysAccountID), false, nil,
				)
			},
		},
		{
			name: "timestamp restore",
			query: fmt.Sprintf(
				"select datname, dat_createsql, dat_type from mo_catalog.mo_database {MO_TS = %d } where datname = '%s' and account_id = 0",
				ts, dbName,
			),
			tablesSQL: buildTableInfoListSQL(dbName, "", ts, uint32(sysAccountID)),
			run: func(bh BackgroundExec) error {
				return restoreDatabaseFromTS(
					ctx, "", bh, dbName, ts, uint32(sysAccountID), uint32(sysAccountID),
					map[string]*tableInfo{}, map[string]*tableInfo{}, false, nil,
				)
			},
		},
		{
			name: "pitr",
			query: fmt.Sprintf(
				"select datname, dat_createsql, dat_type from mo_catalog.mo_database {MO_TS = %d} where datname = '%s' and account_id = 0",
				ts, dbName,
			),
			tablesSQL: buildTableInfoListSQL(dbName, "", ts, uint32(sysAccountID)),
			prepare: func(bh *backgroundExecTest) {
				bh.sql2result[getPubInfoWithPitr(ts, uint32(sysAccountID), dbName)] =
					newMrsForRestoreStringRows([]string{"account_id"}, nil)
			},
			run: func(bh BackgroundExec) error {
				return restoreToDatabaseOrTableWithPitr(
					ctx, "", bh, pitrName, ts, dbName, "",
					map[string]*tableInfo{}, map[string]*tableInfo{}, uint32(sysAccountID),
				)
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			bh := &backgroundExecTest{}
			bh.init()
			bh.sql2result[test.query] = newMrsForRestoreStringRows(
				[]string{"datname", "dat_createsql", "dat_type"},
				[][]interface{}{{dbName, "create database " + dbName, catalog.SystemDBTypeDataBranch}},
			)
			bh.sql2result[masterSQL] = newMrsForRestoreStringRows([]string{"db_name"}, nil)
			bh.sql2result[publicationSQL] = newMrsForRestoreStringRows([]string{"account_id"}, nil)
			bh.sql2result[test.tablesSQL] = newMrsForRestoreStringRows(
				[]string{"relname", "table_type", "relkind", "viewdef"}, nil,
			)
			if test.prepare != nil {
				test.prepare(bh)
			}

			require.NoError(t, test.run(bh))
			createIndex := -1
			for i, sql := range bh.executedSQLs {
				if sql == createSQL {
					createIndex = i
					break
				}
			}
			require.NotEqual(t, -1, createIndex)
			require.Equal(t, catalog.SystemDBTypeDataBranch, bh.executionDatabaseTypes[createIndex])

			t.Run("source read failure precedes target DDL", func(t *testing.T) {
				failed := &backgroundExecTest{}
				failed.init()
				primary := fmt.Errorf("source database identity read failed")
				failed.sql2err[test.query] = primary
				require.ErrorIs(t, test.run(failed), primary)
				require.Equal(t, []string{test.query}, failed.executedSQLs)
			})
		})
	}
}

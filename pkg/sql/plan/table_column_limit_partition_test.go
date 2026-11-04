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

package plan

import (
	"context"
	"errors"
	"fmt"
	"github.com/golang/mock/gomock"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/partitionservice"
	"github.com/matrixorigin/matrixone/pkg/pb/partition"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	executortest "github.com/matrixorigin/matrixone/pkg/util/executor/test"
	"github.com/stretchr/testify/require"
	"strings"
	"testing"
)

func TestTableColumnLimitPartitionCopyStorage(t *testing.T) {
	for _, tc := range []struct {
		source     int
		action     string
		target     int
		names      []string
		failSecond bool
	}{
		{4098, "DROP c4097", 4097, []string{"p0", "p1"}, false},
		{4098, "ADD extra int, DROP c4097", 4098, []string{"west", "east"}, false},
		{4097, "DROP c4096", 4096, []string{"p0", "p1"}, false},
		{4098, "DROP c4097", 4097, []string{"p0", "p1"}, true},
	} {
		t.Run(fmt.Sprintf("%d_%s_fail_%v", tc.source, tc.names[0], tc.failSecond), func(t *testing.T) {
			mock := columnLimitFixture(t, tc.source)
			p, err := buildSingleStmt(mock, t, "ALTER TABLE wide "+tc.action)
			require.NoError(t, err)
			alter := p.GetDdl().GetAlterTable()
			db := mock.ctxt.DefaultDatabase()
			caller := context.WithValue(t.Context(), defines.TenantIDKey{}, uint32(7))
			parent, err := WithAlterTableDDLReplay(caller, db, alter.TableDef, alter.CopyTableDef)
			require.NoError(t, err)
			mock.ctxt.SetContext(parent)
			p, err = buildSingleStmt(mock, t, alter.CreateTmpTableSql)
			require.NoError(t, err)
			def := p.GetDdl().GetCreateTable().TableDef
			def.DbName = db
			def.TblId = 90001
			stmt, err := parsers.ParseOne(parent, dialect.MYSQL, alter.CreateTmpTableSql, 1)
			require.NoError(t, err)
			defer stmt.Free()
			create := stmt.(*tree.CreateTable)
			metadata := partition.PartitionMetadata{TableID: def.TblId, TableName: def.Name, DatabaseName: db, Method: partition.PartitionMethod_Hash}
			for i, name := range tc.names {
				metadata.Partitions = append(metadata.Partitions, partition.Partition{Name: name, PartitionTableName: partitionservice.GetPartitionTableName(def.Name, name), Position: uint32(i), Expr: &Expr{}})
			}
			ctrl := gomock.NewController(t)
			sqlExec := executortest.NewMockSQLExecutor(ctrl)
			creates := 0
			injected := errors.New("second partition failure")
			sqlExec.EXPECT().ExecTxn(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
				func(childCtx context.Context, fn func(executor.TxnExecutor) error, _ executor.Options) error {
					require.Equal(t, uint32(7), childCtx.Value(defines.TenantIDKey{}))
					mock.ctxt.SetContext(childCtx)
					defer mock.ctxt.SetContext(parent)
					txn := executor.NewMemTxnExecutor(func(sql string) (executor.Result, error) {
						if strings.HasPrefix(strings.ToLower(strings.TrimSpace(sql)), "create table") {
							creates++
							if tc.failSecond && creates == 2 {
								return executor.Result{}, injected
							}
							_, e := buildSingleStmt(mock, t, sql)
							if e != nil {
								return executor.Result{}, e
							}
							// Unrelated and prefix-similar names cannot borrow child authority.
							_, e = buildSingleStmt(mock, t, columnLimitCreateSQL(4097, ""))
							requireColumnLimitError(t, e)
							child := metadata.Partitions[creates-1].PartitionTableName
							excess := strings.Replace(columnLimitCreateSQL(tc.target+1, ""), "wide", "\x60"+child+"\x60", 1)
							_, e = buildSingleStmt(mock, t, excess)
							requireColumnLimitError(t, e)
						}
						return executor.Result{}, nil
					}, nil)
					return fn(txn)
				})
			storage := partitionservice.NewStorage("column-limit-test", sqlExec, nil)
			err = storage.Create(parent, def, create, metadata, nil)
			if tc.failSecond {
				require.ErrorIs(t, err, injected)
			} else {
				require.NoError(t, err)
			}
			require.Equal(t, 2, creates)
			require.Zero(t, defines.AlterCopyColumnLimit(caller, db, def.Name))
			for _, part := range metadata.Partitions {
				require.Zero(t, defines.AlterCopyColumnLimit(parent, db, part.PartitionTableName))
			}
			// LIKE/expression-only replay must mask the parent's width grant.
			mock.ctxt.SetContext(WithPersistedDDLReplay(parent, alter.TableDef, alter.CopyTableDef))
			_, err = buildSingleStmt(mock, t, alter.CreateTmpTableSql)
			if tc.target > TableColumnCountLimit {
				requireColumnLimitError(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

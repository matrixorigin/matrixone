// Copyright 2021 - 2022 Matrix Origin
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

package plan

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/stretchr/testify/assert"
)

func TestBuildAlterView(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	type arg struct {
		obj   *ObjectRef
		table *TableDef
	}

	sql1 := "alter view v as select a from a"
	sql2 := "alter view v as select a from v"
	sql3 := "alter view v as select a from vx"

	store := make(map[string]arg)

	vData, err := json.Marshal(ViewData{
		"create view v as select a from a",
		"db",
	})
	assert.NoError(t, err)

	store["db.v"] = arg{nil,
		&plan.TableDef{
			TableType: catalog.SystemViewRel,
			ViewSql: &plan.ViewDef{
				View: string(vData),
			}},
	}

	vxData, err := json.Marshal(ViewData{
		"create view vx as select a from v",
		"db",
	})
	assert.NoError(t, err)
	store["db.vx"] = arg{nil,
		&plan.TableDef{
			TableType: catalog.SystemViewRel,
			ViewSql: &plan.ViewDef{
				View: string(vxData),
			}},
	}

	store["db.a"] = arg{
		&plan.ObjectRef{},
		&plan.TableDef{
			TableType: catalog.SystemOrdinaryRel,
			Cols: []*ColDef{
				{
					Name: "a",
					Typ: &plan.Type{
						Id:    int32(types.T_varchar),
						Width: types.MaxVarcharLen,
						Table: "a",
					},
				},
			},
		}}

	store["db.verror"] = arg{nil,
		&plan.TableDef{
			TableType: catalog.SystemViewRel},
	}

	ctx := NewMockCompilerContext2(ctrl)
	ctx.EXPECT().DefaultDatabase().Return("db").AnyTimes()
	ctx.EXPECT().Resolve(gomock.Any(), gomock.Any()).DoAndReturn(
		func(schemaName string, tableName string) (*ObjectRef, *TableDef) {
			if schemaName == "" {
				schemaName = "db"
			}
			x := store[schemaName+"."+tableName]
			return x.obj, x.table
		}).AnyTimes()
	ctx.EXPECT().SetBuildingAlterView(gomock.Any(), gomock.Any(), gomock.Any()).AnyTimes()
	ctx.EXPECT().ResolveVariable(gomock.Any(), gomock.Any(), gomock.Any()).Return("", nil).AnyTimes()
	ctx.EXPECT().GetAccountId().Return(catalog.System_Account).AnyTimes()
	ctx.EXPECT().GetContext().Return(context.Background()).AnyTimes()
	ctx.EXPECT().GetProcess().Return(nil).AnyTimes()
	ctx.EXPECT().Stats(gomock.Any(), gomock.Any()).Return(&plan.Stats{}).AnyTimes()

	ctx.EXPECT().GetRootSql().Return(sql1).AnyTimes()
	stmt1, err := parsers.ParseOne(context.Background(), dialect.MYSQL, sql1, 1)
	assert.NoError(t, err)
	_, err = buildAlterView(stmt1.(*tree.AlterView), ctx)
	assert.NoError(t, err)

	//direct recursive refrence
	ctx.EXPECT().GetRootSql().Return(sql2).AnyTimes()
	ctx.EXPECT().GetBuildingAlterView().Return(true, "db", "v").AnyTimes()
	stmt2, err := parsers.ParseOne(context.Background(), dialect.MYSQL, sql2, 1)
	assert.NoError(t, err)
	_, err = buildAlterView(stmt2.(*tree.AlterView), ctx)
	assert.Error(t, err)
	assert.EqualError(t, err, "internal error: there is a recursive reference to the view v")

	//indirect recursive refrence
	stmt3, err := parsers.ParseOne(context.Background(), dialect.MYSQL, sql3, 1)
	ctx.EXPECT().GetBuildingAlterView().Return(true, "db", "vx").AnyTimes()
	assert.NoError(t, err)
	_, err = buildAlterView(stmt3.(*tree.AlterView), ctx)
	assert.Error(t, err)
	assert.EqualError(t, err, "internal error: there is a recursive reference to the view v")

	sql4 := "alter view noexists as select a from a"
	stmt4, err := parsers.ParseOne(context.Background(), dialect.MYSQL, sql4, 1)
	assert.NoError(t, err)
	_, err = buildAlterView(stmt4.(*tree.AlterView), ctx)
	assert.Error(t, err)

	sql5 := "alter view verror as select a from a"
	stmt5, err := parsers.ParseOne(context.Background(), dialect.MYSQL, sql5, 1)
	assert.NoError(t, err)
	_, err = buildAlterView(stmt5.(*tree.AlterView), ctx)
	assert.Error(t, err)
}

func TestBuildLockTables(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	type arg struct {
		obj   *ObjectRef
		table *TableDef
	}

	store := make(map[string]arg)

	sql1 := "lock tables t1 read"
	sql2 := "lock tables t1 read, t2 write"
	sql3 := "lock tables t1 read, t1 write"

	store["db.t1"] = arg{
		&plan.ObjectRef{},
		&plan.TableDef{
			TableType: catalog.SystemOrdinaryRel,
			Cols: []*ColDef{
				{
					Name: "a",
					Typ: &plan.Type{
						Id:    int32(types.T_varchar),
						Width: types.MaxVarcharLen,
						Table: "t1",
					},
				},
			},
		}}

	ctx := NewMockCompilerContext2(ctrl)
	ctx.EXPECT().DefaultDatabase().Return("db").AnyTimes()
	ctx.EXPECT().Resolve(gomock.Any(), gomock.Any()).DoAndReturn(
		func(schemaName string, tableName string) (*ObjectRef, *TableDef) {
			if schemaName == "" {
				schemaName = "db"
			}
			x := store[schemaName+"."+tableName]
			return x.obj, x.table
		}).AnyTimes()
	ctx.EXPECT().ResolveVariable(gomock.Any(), gomock.Any(), gomock.Any()).Return("", nil).AnyTimes()
	ctx.EXPECT().GetAccountId().Return(catalog.System_Account).AnyTimes()
	ctx.EXPECT().GetContext().Return(context.Background()).AnyTimes()
	ctx.EXPECT().GetProcess().Return(nil).AnyTimes()
	ctx.EXPECT().Stats(gomock.Any(), gomock.Any()).Return(&plan.Stats{}).AnyTimes()

	ctx.EXPECT().GetRootSql().Return(sql1).AnyTimes()
	stmt1, err := parsers.ParseOne(context.Background(), dialect.MYSQL, sql1, 1)
	assert.NoError(t, err)
	_, err = buildLockTables(stmt1.(*tree.LockTableStmt), ctx)
	assert.NoError(t, err)

	ctx.EXPECT().GetRootSql().Return(sql2).AnyTimes()
	stmt2, err := parsers.ParseOne(context.Background(), dialect.MYSQL, sql2, 1)
	assert.NoError(t, err)
	_, err = buildLockTables(stmt2.(*tree.LockTableStmt), ctx)
	assert.Error(t, err)

	store["db.t2"] = arg{
		&plan.ObjectRef{},
		&plan.TableDef{
			TableType: catalog.SystemOrdinaryRel,
			Cols: []*ColDef{
				{
					Name: "a",
					Typ: &plan.Type{
						Id:    int32(types.T_varchar),
						Width: types.MaxVarcharLen,
						Table: "t2",
					},
				},
			},
		}}

	_, err = buildLockTables(stmt2.(*tree.LockTableStmt), ctx)
	assert.NoError(t, err)

	ctx.EXPECT().GetRootSql().Return(sql3).AnyTimes()
	stmt3, err := parsers.ParseOne(context.Background(), dialect.MYSQL, sql3, 1)
	assert.NoError(t, err)
	_, err = buildLockTables(stmt3.(*tree.LockTableStmt), ctx)
	assert.Error(t, err)
}

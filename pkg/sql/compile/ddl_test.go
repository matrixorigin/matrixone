// Copyright 2024 Matrix Origin
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

package compile

import (
	"context"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"

	"github.com/golang/mock/gomock"
	"github.com/prashantv/gostub"
	"github.com/smartystreets/goconvey/convey"
	"github.com/stretchr/testify/assert"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/buffer"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/defines"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/pb/lock"
	plan2 "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func Test_lockIndexTable(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	txnOperator := mock_frontend.NewMockTxnOperator(ctrl)
	proc := testutil.NewProc()
	proc.Base.TxnOperator = txnOperator

	mockEngine := mock_frontend.NewMockEngine(ctrl)
	mockEngine.EXPECT().New(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
	mockEngine.EXPECT().AllocateIDByKey(gomock.Any(), gomock.Any()).Return(uint64(272510), nil).AnyTimes()

	mock_db1_database := mock_frontend.NewMockDatabase(ctrl)
	mock_db1_database.EXPECT().Relation(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, moerr.NewLockTableNotFound(context.Background())).AnyTimes()

	type args struct {
		ctx        context.Context
		dbSource   engine.Database
		eng        engine.Engine
		proc       *process.Process
		tableName  string
		defChanged bool
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			name: "test",
			args: args{
				ctx:        context.Background(),
				dbSource:   mock_db1_database,
				eng:        mockEngine,
				proc:       proc,
				tableName:  "__mo_index_unique_0192aea0-8e78-76a7-b3ea-10862b69c51c",
				defChanged: true,
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := lockIndexTable(tt.args.ctx, tt.args.dbSource, tt.args.eng, tt.args.proc, tt.args.tableName, tt.args.defChanged); (err != nil) != tt.wantErr {
				t.Errorf("lockIndexTable() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestScope_CreateTable(t *testing.T) {
	tableDef := &plan.TableDef{
		Name: "dept",
		Cols: []*plan.ColDef{
			{
				ColId: 0,
				Name:  "deptno",
				Alg:   plan2.CompressType_Lz4,
				Typ: plan.Type{
					Id:          27,
					NotNullable: false,
					AutoIncr:    true,
					Width:       32,
					Scale:       -1,
				},
				Default: &plan2.Default{},
				NotNull: true,
				Primary: true,
				Pkidx:   0,
			},
			{
				ColId: 1,
				Name:  "dname",
				Alg:   plan2.CompressType_Lz4,
				Typ: plan.Type{
					Id:          61,
					NotNullable: false,
					AutoIncr:    false,
					Width:       15,
					Scale:       0,
				},
				Default: &plan2.Default{},
				NotNull: false,
				Primary: false,
				Pkidx:   0,
			},
			{
				ColId: 2,
				Name:  "loc",
				Alg:   plan2.CompressType_Lz4,
				Typ: plan.Type{
					Id:          61,
					NotNullable: false,
					AutoIncr:    false,
					Width:       50,
					Scale:       0,
				},
				Default: &plan2.Default{},
				NotNull: false,
				Primary: false,
				Pkidx:   0,
			},
		},
		Pkey: &plan.PrimaryKeyDef{
			Cols:        nil,
			PkeyColId:   0,
			PkeyColName: "deptno",
			Names:       []string{"deptno"},
		},
		Defs: []*plan2.TableDef_DefType{
			{
				Def: &plan.TableDef_DefType_Properties{
					Properties: &plan.PropertiesDef{
						Properties: []*plan.Property{
							{
								Key:   "relkind",
								Value: "r",
							},
						},
					},
				},
			},
		},
	}

	createTableDef := &plan2.CreateTable{
		IfNotExists: false,
		Database:    "test",
		Replace:     false,
		TableDef:    tableDef,
	}

	cplan := &plan.Plan{
		Plan: &plan2.Plan_Ddl{
			Ddl: &plan2.DataDefinition{
				DdlType: plan2.DataDefinition_CREATE_TABLE,
				Definition: &plan2.DataDefinition_CreateTable{
					CreateTable: createTableDef,
				},
			},
		},
	}

	s := &Scope{
		Magic:     CreateTable,
		Plan:      cplan,
		TxnOffset: 0,
	}

	sql := `create table dept(
		deptno int unsigned auto_increment COMMENT '部门编号',
		dname varchar(15) COMMENT '部门名称',
		loc varchar(50)  COMMENT '部门所在位置',
		primary key(deptno)
	) COMMENT='部门表'`

	convey.Convey("create table FaultTolerance1", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		proc := testutil.NewProcess()
		proc.Base.SessionInfo.Buf = buffer.New()

		ctx := defines.AttachAccountId(context.Background(), sysAccountId)
		proc.Ctx = defines.AttachAccountId(context.Background(), sysAccountId)
		txnCli, txnOp := newTestTxnClientAndOp(ctrl)
		proc.Base.TxnClient = txnCli
		proc.Base.TxnOperator = txnOp
		proc.ReplaceTopCtx(ctx)

		relation := mock_frontend.NewMockRelation(ctrl)
		relation.EXPECT().GetTableID(gomock.Any()).Return(uint64(1)).AnyTimes()

		eng := mock_frontend.NewMockEngine(ctrl)
		mockDbMeta := mock_frontend.NewMockDatabase(ctrl)
		eng.EXPECT().HasTempEngine().Return(false).AnyTimes()
		eng.EXPECT().Database(gomock.Any(), gomock.Any(), gomock.Any()).Return(mockDbMeta, nil).AnyTimes()

		mockDbMeta.EXPECT().RelationExists(gomock.Any(), "dept", gomock.Any()).Return(false, moerr.NewInternalErrorNoCtx("test"))

		mockDbMeta.EXPECT().Relation(gomock.Any(), catalog.MO_DATABASE, gomock.Any()).Return(relation, nil).AnyTimes()

		c := NewCompile("test", "test", sql, "", "", eng, proc, nil, false, nil, time.Now())
		assert.Error(t, s.CreateTable(c))
	})

	convey.Convey("create table FaultTolerance2", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		proc := testutil.NewProcess()
		txnCli, txnOp := newTestTxnClientAndOp(ctrl)
		proc.Base.TxnClient = txnCli
		proc.Base.TxnOperator = txnOp
		proc.Base.SessionInfo.Buf = buffer.New()

		ctx := defines.AttachAccountId(context.Background(), sysAccountId)
		proc.Ctx = defines.AttachAccountId(context.Background(), sysAccountId)
		proc.ReplaceTopCtx(ctx)

		relation := mock_frontend.NewMockRelation(ctrl)

		meta_relation := mock_frontend.NewMockRelation(ctrl)
		meta_relation.EXPECT().GetTableID(gomock.Any()).Return(uint64(1)).AnyTimes()

		mockDbMeta := mock_frontend.NewMockDatabase(ctrl)
		mockDbMeta.EXPECT().Relation(gomock.Any(), "dept", gomock.Any()).Return(relation, nil).AnyTimes()
		mockDbMeta.EXPECT().RelationExists(gomock.Any(), "dept", gomock.Any()).Return(false, nil).AnyTimes()
		mockDbMeta.EXPECT().Relation(gomock.Any(), catalog.MO_DATABASE, gomock.Any()).Return(meta_relation, nil).AnyTimes()

		mockDbMeta2 := mock_frontend.NewMockDatabase(ctrl)
		mockDbMeta2.EXPECT().RelationExists(gomock.Any(), gomock.Any(), gomock.Any()).Return(false, moerr.NewInternalErrorNoCtx("test"))

		eng := mock_frontend.NewMockEngine(ctrl)
		eng.EXPECT().HasTempEngine().Return(true).AnyTimes()
		eng.EXPECT().Database(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(func(ctx context.Context, name string, arg any) (engine.Database, error) {
			if name == defines.TEMPORARY_DBNAME {
				return mockDbMeta2, nil
			}
			return mockDbMeta, nil
		}).AnyTimes()

		c := NewCompile("test", "test", sql, "", "", eng, proc, nil, false, nil, time.Now())
		assert.Error(t, s.CreateTable(c))
	})

	convey.Convey("create table FaultTolerance3", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		proc := testutil.NewProcess()
		proc.Base.SessionInfo.Buf = buffer.New()

		ctx := defines.AttachAccountId(context.Background(), sysAccountId)
		proc.Ctx = defines.AttachAccountId(context.Background(), sysAccountId)
		txnCli, txnOp := newTestTxnClientAndOp(ctrl)
		proc.Base.TxnClient = txnCli
		proc.Base.TxnOperator = txnOp
		proc.ReplaceTopCtx(ctx)

		relation := mock_frontend.NewMockRelation(ctrl)
		relation.EXPECT().GetTableID(gomock.Any()).Return(uint64(1)).AnyTimes()

		mockDbMeta := mock_frontend.NewMockDatabase(ctrl)
		mockDbMeta.EXPECT().Relation(gomock.Any(), catalog.MO_DATABASE, gomock.Any()).Return(relation, nil).AnyTimes()

		eng := mock_frontend.NewMockEngine(ctrl)
		eng.EXPECT().HasTempEngine().Return(false).AnyTimes()
		eng.EXPECT().Database(gomock.Any(), gomock.Any(), gomock.Any()).Return(mockDbMeta, nil).AnyTimes()

		planDef2ExecDef := gostub.Stub(&engine.PlanDefsToExeDefs, func(_ *plan.TableDef) ([]engine.TableDef, *api.SchemaExtra, error) {
			return nil, nil, moerr.NewInternalErrorNoCtx("test error")
		})
		defer planDef2ExecDef.Reset()

		c := NewCompile("test", "test", sql, "", "", eng, proc, nil, false, nil, time.Now())
		assert.Error(t, s.CreateTable(c))
	})

	convey.Convey("create table FaultTolerance4", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		proc := testutil.NewProcess()
		proc.Base.SessionInfo.Buf = buffer.New()

		ctx := defines.AttachAccountId(context.Background(), sysAccountId)
		proc.Ctx = defines.AttachAccountId(context.Background(), sysAccountId)
		txnCli, txnOp := newTestTxnClientAndOp(ctrl)
		proc.Base.TxnClient = txnCli
		proc.Base.TxnOperator = txnOp
		proc.ReplaceTopCtx(ctx)

		relation := mock_frontend.NewMockRelation(ctrl)
		relation.EXPECT().GetTableID(gomock.Any()).Return(uint64(1)).AnyTimes()

		mockDbMeta := mock_frontend.NewMockDatabase(ctrl)
		mockDbMeta.EXPECT().Relation(gomock.Any(), catalog.MO_DATABASE, gomock.Any()).Return(relation, nil).AnyTimes()
		mockDbMeta.EXPECT().RelationExists(gomock.Any(), gomock.Any(), gomock.Any()).Return(false, nil).AnyTimes()

		eng := mock_frontend.NewMockEngine(ctrl)
		eng.EXPECT().HasTempEngine().Return(false).AnyTimes()
		eng.EXPECT().Database(gomock.Any(), gomock.Any(), gomock.Any()).Return(mockDbMeta, nil).AnyTimes()

		lockMoDb := gostub.Stub(&lockMoDatabase, func(_ *Compile, _ string, _ lock.LockMode) error {
			return nil
		})
		defer lockMoDb.Reset()

		lockMoTbl := gostub.Stub(&lockMoTable, func(_ *Compile, _ string, _ string, _ lock.LockMode) error {
			return moerr.NewTxnNeedRetryNoCtx()
		})
		defer lockMoTbl.Reset()

		c := NewCompile("test", "test", sql, "", "", eng, proc, nil, false, nil, time.Now())
		assert.Error(t, s.CreateTable(c))
	})

	convey.Convey("create table FaultTolerance5", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		proc := testutil.NewProcess()
		proc.Base.SessionInfo.Buf = buffer.New()

		ctx := defines.AttachAccountId(context.Background(), sysAccountId)
		proc.Ctx = defines.AttachAccountId(context.Background(), sysAccountId)
		txnCli, txnOp := newTestTxnClientAndOp(ctrl)
		proc.Base.TxnClient = txnCli
		proc.Base.TxnOperator = txnOp
		proc.ReplaceTopCtx(ctx)

		relation := mock_frontend.NewMockRelation(ctrl)
		relation.EXPECT().GetTableID(gomock.Any()).Return(uint64(1)).AnyTimes()

		mockDbMeta := mock_frontend.NewMockDatabase(ctrl)
		mockDbMeta.EXPECT().Relation(gomock.Any(), catalog.MO_DATABASE, gomock.Any()).Return(relation, nil).AnyTimes()
		mockDbMeta.EXPECT().RelationExists(gomock.Any(), gomock.Any(), gomock.Any()).Return(false, nil).AnyTimes()
		mockDbMeta.EXPECT().Create(gomock.Any(), gomock.Any(), gomock.Any()).Return(moerr.NewInternalErrorNoCtx("test err")).AnyTimes()

		eng := mock_frontend.NewMockEngine(ctrl)
		eng.EXPECT().HasTempEngine().Return(false).AnyTimes()
		eng.EXPECT().Database(gomock.Any(), gomock.Any(), gomock.Any()).Return(mockDbMeta, nil).AnyTimes()

		lockMoDb := gostub.Stub(&lockMoDatabase, func(_ *Compile, _ string, _ lock.LockMode) error {
			return nil
		})
		defer lockMoDb.Reset()

		lockMoTbl := gostub.Stub(&lockMoTable, func(_ *Compile, _ string, _ string, _ lock.LockMode) error {
			return nil
		})
		defer lockMoTbl.Reset()

		c := NewCompile("test", "test", sql, "", "", eng, proc, nil, false, nil, time.Now())
		assert.Error(t, s.CreateTable(c))
	})

	convey.Convey("create table FaultTolerance10", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		proc := testutil.NewProcess()
		proc.Base.SessionInfo.Buf = buffer.New()

		ctx := defines.AttachAccountId(context.Background(), sysAccountId)
		proc.Ctx = defines.AttachAccountId(context.Background(), sysAccountId)
		txnCli, txnOp := newTestTxnClientAndOp(ctrl)
		proc.Base.TxnClient = txnCli
		proc.Base.TxnOperator = txnOp
		proc.ReplaceTopCtx(ctx)

		relation := mock_frontend.NewMockRelation(ctrl)
		relation.EXPECT().GetTableID(gomock.Any()).Return(uint64(1)).AnyTimes()

		mockDbMeta := mock_frontend.NewMockDatabase(ctrl)
		mockDbMeta.EXPECT().Relation(gomock.Any(), gomock.Any(), gomock.Any()).Return(relation, nil).AnyTimes()
		mockDbMeta.EXPECT().RelationExists(gomock.Any(), gomock.Any(), gomock.Any()).Return(false, nil).AnyTimes()
		mockDbMeta.EXPECT().Create(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, tblName string, _ []engine.TableDef) error {
			if tblName == "dept" {
				return nil
			} else if tblName == "%!%p0%!%dept" || tblName == "%!%p1%!%dept" {
				return nil
			} else if tblName == "__mo_index_secondary_0193d918-3e7b-7506-9f70-64fbcf055c19" {
				return nil
			}
			return nil
		}).AnyTimes()

		eng := mock_frontend.NewMockEngine(ctrl)
		eng.EXPECT().HasTempEngine().Return(false).AnyTimes()
		eng.EXPECT().Database(gomock.Any(), gomock.Any(), gomock.Any()).Return(mockDbMeta, nil).AnyTimes()

		planDef2ExecDef := gostub.Stub(&engine.PlanDefsToExeDefs, func(tbl *plan.TableDef) ([]engine.TableDef, *api.SchemaExtra, error) {
			if tbl.Name == "dept" {
				return nil, nil, nil
			} else if tbl.Name == "%!%p0%!%dept" || tbl.Name == "%!%p1%!%dept" {
				return nil, nil, nil
			} else if tbl.Name == "__mo_index_secondary_0193d918-3e7b-7506-9f70-64fbcf055c19" {
				return nil, nil, nil
			}
			return nil, nil, nil
		})
		defer planDef2ExecDef.Reset()

		lockMoDb := gostub.Stub(&lockMoDatabase, func(_ *Compile, _ string, _ lock.LockMode) error {
			return nil
		})
		defer lockMoDb.Reset()

		lockMoTbl := gostub.Stub(&lockMoTable, func(_ *Compile, _ string, _ string, _ lock.LockMode) error {
			return nil
		})
		defer lockMoTbl.Reset()

		checkIndexInit := gostub.Stub(&checkIndexInitializable, func(_ string, _ string) bool {
			return false
		})
		defer checkIndexInit.Reset()

		createAutoIncrement := gostub.Stub(&maybeCreateAutoIncrement, func(_ context.Context, _ string, _ engine.Database, _ *plan.TableDef, _ client.TxnOperator, _ func() string) error {
			return moerr.NewInternalErrorNoCtx("test err")
		})
		defer createAutoIncrement.Reset()

		c := NewCompile("test", "test", sql, "", "", eng, proc, nil, false, nil, time.Now())
		assert.Error(t, s.CreateTable(c))
	})
}

func TestScope_CreateView(t *testing.T) {
	tableDef := &plan.TableDef{
		Name: "v1",
		Cols: []*plan.ColDef{
			{
				Name: "deptno",
				Alg:  plan2.CompressType_Lz4,
				Typ: plan.Type{
					Id:          27,
					NotNullable: true,
					AutoIncr:    true,
					Width:       32,
					Scale:       -1,
				},
				Default: &plan2.Default{},
				NotNull: false,
				Primary: false,
			},
			{
				Name: "dname",
				Alg:  plan2.CompressType_Lz4,
				Typ: plan.Type{
					Id:          61,
					NotNullable: false,
					AutoIncr:    false,
					Width:       15,
					Scale:       0,
				},
				Default: &plan2.Default{},
				NotNull: false,
				Primary: false,
			},
			{
				Name: "loc",
				Alg:  plan2.CompressType_Lz4,
				Typ: plan.Type{
					Id:          61,
					NotNullable: false,
					AutoIncr:    false,
					Width:       50,
					Scale:       0,
				},
				Default: &plan2.Default{},
				NotNull: false,
				Primary: false,
			},
		},
		ViewSql: &plan2.ViewDef{
			View: `{"Stmt":"create view v1 as select * from dept","DefaultDatabase":"db1"}`,
		},
		Defs: []*plan2.TableDef_DefType{
			{
				Def: &plan.TableDef_DefType_Properties{
					Properties: &plan.PropertiesDef{
						Properties: []*plan.Property{
							{
								Key:   "relkind",
								Value: "v",
							},
						},
					},
				},
			},
		},
	}

	createViewDef := &plan2.CreateView{
		IfNotExists: false,
		Database:    "test",
		Replace:     false,
		TableDef:    tableDef,
	}

	cplan := &plan.Plan{
		Plan: &plan2.Plan_Ddl{
			Ddl: &plan2.DataDefinition{
				DdlType: plan2.DataDefinition_CREATE_VIEW,
				Definition: &plan2.DataDefinition_CreateView{
					CreateView: createViewDef,
				},
			},
		},
	}

	s := &Scope{
		Magic:     CreateView,
		Plan:      cplan,
		TxnOffset: 0,
	}

	convey.Convey("create table FaultTolerance1", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		proc := testutil.NewProcess()
		proc.Base.SessionInfo.Buf = buffer.New()

		ctx := defines.AttachAccountId(context.Background(), sysAccountId)
		proc.Ctx = defines.AttachAccountId(context.Background(), sysAccountId)
		txnCli, txnOp := newTestTxnClientAndOp(ctrl)
		proc.Base.TxnClient = txnCli
		proc.Base.TxnOperator = txnOp
		proc.ReplaceTopCtx(ctx)

		meta_relation := mock_frontend.NewMockRelation(ctrl)
		meta_relation.EXPECT().GetTableID(gomock.Any()).Return(uint64(1)).AnyTimes()

		eng := mock_frontend.NewMockEngine(ctrl)
		mockDbMeta := mock_frontend.NewMockDatabase(ctrl)
		eng.EXPECT().Database(gomock.Any(), gomock.Any(), gomock.Any()).Return(mockDbMeta, nil).AnyTimes()

		mockDbMeta.EXPECT().RelationExists(gomock.Any(), "v1", gomock.Any()).Return(false, moerr.NewInternalErrorNoCtx("test"))
		mockDbMeta.EXPECT().Relation(gomock.Any(), catalog.MO_DATABASE, gomock.Any()).Return(meta_relation, nil).AnyTimes()

		sql := `create view v1 as select * from dept`
		c := NewCompile("test", "test", sql, "", "", eng, proc, nil, false, nil, time.Now())
		assert.Error(t, s.CreateView(c))
	})

	convey.Convey("create table FaultTolerance1", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		proc := testutil.NewProcess()
		proc.Base.SessionInfo.Buf = buffer.New()

		ctx := context.Background()
		proc.Ctx = context.Background()
		txnCli, txnOp := newTestTxnClientAndOp(ctrl)
		proc.Base.TxnClient = txnCli
		proc.Base.TxnOperator = txnOp
		proc.ReplaceTopCtx(ctx)

		relation := mock_frontend.NewMockRelation(ctrl)

		meta_relation := mock_frontend.NewMockRelation(ctrl)
		meta_relation.EXPECT().GetTableID(gomock.Any()).Return(uint64(1)).AnyTimes()

		mockDbMeta := mock_frontend.NewMockDatabase(ctrl)
		mockDbMeta.EXPECT().Relation(gomock.Any(), "v1", gomock.Any()).Return(relation, nil).AnyTimes()
		mockDbMeta.EXPECT().RelationExists(gomock.Any(), "v1", gomock.Any()).Return(false, nil).AnyTimes()
		mockDbMeta.EXPECT().Relation(gomock.Any(), catalog.MO_DATABASE, gomock.Any()).Return(meta_relation, nil).AnyTimes()

		mockDbMeta2 := mock_frontend.NewMockDatabase(ctrl)
		mockDbMeta2.EXPECT().RelationExists(gomock.Any(), gomock.Any(), gomock.Any()).Return(false, moerr.NewInternalErrorNoCtx("test")).AnyTimes()

		eng := mock_frontend.NewMockEngine(ctrl)
		eng.EXPECT().Database(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(func(ctx context.Context, name string, arg any) (engine.Database, error) {
			if name == defines.TEMPORARY_DBNAME {
				return mockDbMeta2, nil
			}
			return mockDbMeta, nil
		}).AnyTimes()

		sql := `create view v1 as select * from dept`
		c := NewCompile("test", "test", sql, "", "", eng, proc, nil, false, nil, time.Now())
		assert.Error(t, s.CreateView(c))
	})

}

func TestScope_Database(t *testing.T) {
	dropDbDef := &plan2.DropDatabase{
		IfExists: false,
		Database: "test",
	}

	cplan := &plan.Plan{
		Plan: &plan2.Plan_Ddl{
			Ddl: &plan2.DataDefinition{
				DdlType: plan2.DataDefinition_DROP_DATABASE,
				Definition: &plan2.DataDefinition_DropDatabase{
					DropDatabase: dropDbDef,
				},
			},
		},
	}

	s := &Scope{
		Magic:     DropDatabase,
		Plan:      cplan,
		TxnOffset: 0,
	}

	sql := `create database test;`

	convey.Convey("create table FaultTolerance1", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		proc := testutil.NewProcess()
		proc.Base.SessionInfo.Buf = buffer.New()

		proc.Ctx = context.Background()
		txnCli, txnOp := newTestTxnClientAndOp(ctrl)
		proc.Base.TxnClient = txnCli
		proc.Base.TxnOperator = txnOp
		proc.ReplaceTopCtx(context.Background())

		eng := mock_frontend.NewMockEngine(ctrl)

		c := NewCompile("test", "test", sql, "", "", eng, proc, nil, false, nil, time.Now())
		assert.Error(t, s.DropDatabase(c))
	})
}

func Test_addTimeSpan(t *testing.T) {
	cases := []struct {
		name    string
		len     int
		unit    string
		wantOk  bool
		wantMsg string
	}{
		{"hour", 1, "h", true, ""},
		{"day", 2, "d", true, ""},
		{"month", 3, "mo", true, ""},
		{"year", 4, "y", true, ""},
		{"invalid", 5, "xx", false, "unknown unit"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			_, err := addTimeSpan(c.len, c.unit)
			if c.wantOk {
				assert.NoError(t, err)
			} else {
				assert.Error(t, err)
				assert.Contains(t, err.Error(), c.wantMsg)
			}
		})
	}
}

func Test_getSqlForCheckPitrDup(t *testing.T) {
	mk := func(level int32, origin bool) *plan2.CreatePitr {
		return &plan2.CreatePitr{
			Level:             level,
			CurrentAccountId:  1,
			AccountName:       "acc",
			CurrentAccount:    "curacc",
			DatabaseName:      "db",
			TableName:         "tb",
			OriginAccountName: origin,
		}
	}
	assert.Contains(t, getSqlForCheckPitrDup(mk(int32(tree.PITRLEVELCLUSTER), false)), "obj_id")
	assert.Contains(t, getSqlForCheckPitrDup(mk(int32(tree.PITRLEVELACCOUNT), true)), "account_name = 'acc'")
	assert.Contains(t, getSqlForCheckPitrDup(mk(int32(tree.PITRLEVELACCOUNT), false)), "account_name = 'curacc'")
	assert.Contains(t, getSqlForCheckPitrDup(mk(int32(tree.PITRLEVELDATABASE), false)), "database_name = 'db'")
	assert.Contains(t, getSqlForCheckPitrDup(mk(int32(tree.PITRLEVELTABLE), false)), "table_name = 'tb'")
}

func TestCheckSysMoCatalogPitrResult(t *testing.T) {
	mp := mpool.MustNewZero()
	ctx := context.Background()

	t.Run("empty vecs", func(t *testing.T) {
		needInsert, needUpdate, err := CheckSysMoCatalogPitrResult(ctx, []*vector.Vector{}, 10, "d")
		assert.Error(t, err)
		assert.False(t, needInsert)
		assert.False(t, needUpdate)
	})

	t.Run("insert needed", func(t *testing.T) {
		v1 := vector.NewVec(types.T_uint64.ToType())
		v2 := vector.NewVec(types.T_varchar.ToType())
		// no data in vectors
		needInsert, needUpdate, err := CheckSysMoCatalogPitrResult(ctx, []*vector.Vector{v1, v2}, 10, "d")
		assert.NoError(t, err)
		assert.True(t, needInsert)
		assert.False(t, needUpdate)
	})

	t.Run("update needed", func(t *testing.T) {
		v1 := vector.NewVec(types.T_uint64.ToType())
		_ = vector.AppendFixed(v1, uint64(5), false, mp)
		v2 := vector.NewVec(types.T_varchar.ToType())
		_ = vector.AppendBytes(v2, []byte("d"), false, mp)
		needInsert, needUpdate, err := CheckSysMoCatalogPitrResult(ctx, []*vector.Vector{v1, v2}, 10, "d")
		assert.NoError(t, err)
		assert.False(t, needInsert)
		assert.True(t, needUpdate)
	})

	t.Run("no update needed", func(t *testing.T) {
		v1 := vector.NewVec(types.T_uint64.ToType())
		_ = vector.AppendFixed(v1, uint64(20), false, mp)
		v2 := vector.NewVec(types.T_varchar.ToType())
		_ = vector.AppendBytes(v2, []byte("d"), false, mp)
		needInsert, needUpdate, err := CheckSysMoCatalogPitrResult(ctx, []*vector.Vector{v1, v2}, 10, "d")
		assert.NoError(t, err)
		assert.False(t, needInsert)
		assert.False(t, needUpdate)
	})
}

func TestPitrDupError(t *testing.T) {
	compile := &Compile{proc: testutil.NewProc()}
	cases := []struct {
		level       int32
		accountName string
		dbName      string
		tableName   string
		expect      string
	}{
		{int32(tree.PITRLEVELCLUSTER), "", "", "", "cluster level pitr already exists"},
		{int32(tree.PITRLEVELACCOUNT), "acc", "", "", "account acc does not exist"},
		{int32(tree.PITRLEVELDATABASE), "", "db", "", "database `db` already has a pitr"},
		{int32(tree.PITRLEVELTABLE), "", "db", "tb", "table db.tb does not exist"},
	}
	for _, c := range cases {
		p := &plan2.CreatePitr{
			Level:        c.level,
			AccountName:  c.accountName,
			DatabaseName: c.dbName,
			TableName:    c.tableName,
		}
		err := pitrDupError(compile, p)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), c.expect)
	}
}

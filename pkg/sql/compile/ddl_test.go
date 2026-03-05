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

	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/pb/lock"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/prashantv/gostub"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"

	"github.com/golang/mock/gomock"
	"github.com/smartystreets/goconvey/convey"
	"github.com/stretchr/testify/assert"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/buffer"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/defines"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	plan2 "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func Test_lockIndexTable(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	txnOperator := mock_frontend.NewMockTxnOperator(ctrl)
	proc := testutil.NewProc(t)
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
	stubs := gostub.New()
	defer stubs.Reset()

	stubs.Stub(&engine.PlanDefsToExeDefs, func(_ *plan.TableDef) ([]engine.TableDef, *api.SchemaExtra, error) {
		return nil, nil, nil
	})
	stubs.Stub(&lockMoDatabase, func(c *Compile, dbName string, lockMode lock.LockMode) error {
		return nil
	})
	stubs.Stub(&lockMoTable, func(c *Compile, dbName string, tblName string, lockMode lock.LockMode) error {
		return nil
	})
	stubs.Stub(&checkIndexInitializable, func(dbName string, tblName string) bool {
		return true
	})
	stubs.Stub(&maybeCreateAutoIncrement, func(
		ctx context.Context,
		sid string,
		db engine.Database,
		def *plan.TableDef,
		txnOp client.TxnOperator,
		nameResolver func() string) error {
		return nil
	})

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
		proc := testutil.NewProcess(t)
		proc.Base.SessionInfo.Buf = buffer.New()
		proc.Ctx = defines.AttachAccountId(context.Background(), sysAccountId)

		eng := newStubEngine()
		db := newStubDatabase("test")
		eng.dbs["test"] = db
		db.relExistsErr = moerr.NewInternalErrorNoCtx("test error")

		c := NewCompile("test", "test", sql, "", "", eng, proc, nil, false, nil, time.Now())
		assert.Error(t, s.CreateTable(c))
	})

	convey.Convey("create table FaultTolerance2", t, func() {
		proc := testutil.NewProcess(t)
		proc.Base.SessionInfo.Buf = buffer.New()
		proc.Ctx = defines.AttachAccountId(context.Background(), sysAccountId)

		eng := newStubEngine()
		db := newStubDatabase("test")
		eng.dbs["test"] = db
		// To simulate "table exists" error when IfNotExists=false
		db.rels["dept"] = newStubRelation("dept")

		c := NewCompile("test", "test", sql, "", "", eng, proc, nil, false, nil, time.Now())
		assert.Error(t, s.CreateTable(c))
	})

	convey.Convey("create table FaultTolerance3", t, func() {
		proc := testutil.NewProcess(t)
		proc.Base.SessionInfo.Buf = buffer.New()
		proc.Ctx = defines.AttachAccountId(context.Background(), sysAccountId)

		eng := newStubEngine()
		eng.dbs["test"] = newStubDatabase("test")

		planDef2ExecDef := gostub.Stub(&engine.PlanDefsToExeDefs, func(_ *plan.TableDef) ([]engine.TableDef, *api.SchemaExtra, error) {
			return nil, nil, moerr.NewInternalErrorNoCtx("test error")
		})
		defer planDef2ExecDef.Reset()

		c := NewCompile("test", "test", sql, "", "", eng, proc, nil, false, nil, time.Now())
		assert.Error(t, s.CreateTable(c))
	})

	convey.Convey("create table FaultTolerance4", t, func() {
		proc := testutil.NewProcess(t)
		proc.Base.SessionInfo.Buf = buffer.New()
		proc.Ctx = defines.AttachAccountId(context.Background(), sysAccountId)

		eng := newStubEngine()
		eng.dbs["test"] = newStubDatabase("test")

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
		proc := testutil.NewProcess(t)
		proc.Base.SessionInfo.Buf = buffer.New()
		proc.Ctx = defines.AttachAccountId(context.Background(), sysAccountId)

		eng := newStubEngine()
		db := newStubDatabase("test")
		db.createErr = moerr.NewInternalErrorNoCtx("test err")
		eng.dbs["test"] = db

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
		proc := testutil.NewProcess(t)
		proc.Base.SessionInfo.Buf = buffer.New()
		proc.Ctx = defines.AttachAccountId(context.Background(), sysAccountId)

		eng := newStubEngine()
		eng.dbs["test"] = newStubDatabase("test")

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

		proc := testutil.NewProcess(t)
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

		proc := testutil.NewProcess(t)
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

		eng := mock_frontend.NewMockEngine(ctrl)
		eng.EXPECT().Database(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(func(ctx context.Context, name string, arg any) (engine.Database, error) {
			return mockDbMeta, nil
		}).AnyTimes()

		sql := `create view v1 as select * from dept`
		c := NewCompile("test", "test", sql, "", "", eng, proc, nil, false, nil, time.Now())
		assert.Error(t, s.CreateView(c))
	})

}

func TestScope_CreateTableIfNotExistsAsSelectWhenTableExists(t *testing.T) {
	stubs := gostub.New()
	defer stubs.Reset()

	stubs.Stub(&engine.PlanDefsToExeDefs, func(_ *plan.TableDef) ([]engine.TableDef, *api.SchemaExtra, error) {
		return nil, nil, nil
	})
	stubs.Stub(&lockMoDatabase, func(c *Compile, dbName string, lockMode lock.LockMode) error {
		return nil
	})

	tableDef := &plan2.TableDef{
		Name: "dept",
	}

	createTableDef := &plan2.CreateTable{
		IfNotExists:       true,
		Database:          "test",
		TableDef:          tableDef,
		CreateAsSelectSql: "insert into `test`.`dept` select 1",
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

	proc := testutil.NewProcess(t)
	proc.Base.SessionInfo.Buf = buffer.New()
	proc.Ctx = defines.AttachAccountId(context.Background(), sysAccountId)

	eng := newStubEngine()
	db := newStubDatabase("test")
	db.rels["dept"] = newStubRelation("dept")
	eng.dbs["test"] = db

	c := NewCompile(
		"test",
		"test",
		"create table if not exists dept as select 1",
		"",
		"",
		eng,
		proc,
		nil,
		false,
		nil,
		time.Now(),
	)

	assert.NoError(t, s.CreateTable(c))
	assert.Equal(t, uint64(0), c.getAffectedRows())
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

		proc := testutil.NewProcess(t)
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
	compile := &Compile{proc: testutil.NewProc(t)}
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

func TestIsExperimentalEnabled(t *testing.T) {
	s := newScope(TableClone)

	enabled, err := s.isExperimentalEnabled(nil, hnswIndexFlag)
	assert.NoError(t, err)
	assert.True(t, enabled)
}

func Test_isValidFrequency(t *testing.T) {
	cases := []struct {
		in string
		ok bool
	}{
		{"1h", true},
		{"2m", true},
		{"60m", true},
		{"0h", false},
		{"00m", false},
		{"-1h", false},
		{"1d", false},
		{"h", false},
		{"", false},
	}
	for _, c := range cases {
		if got := isValidFrequency(c.in); got != c.ok {
			t.Fatalf("isValidFrequency(%q)=%v, want %v", c.in, got, c.ok)
		}
	}
}

func Test_transformIntoHours(t *testing.T) {
	cases := []struct {
		in    string
		hours int64
	}{
		{"1h", 1},
		{"2h", 2},
		{"60m", 1},
		{"61m", 2},
		{"120m", 2},
		{"", 0},
	}
	for _, c := range cases {
		if got := transformIntoHours(c.in); got != c.hours {
			t.Fatalf("transformIntoHours(%q)=%d, want %d", c.in, got, c.hours)
		}
	}
}

func Test_CDCStrToTime(t *testing.T) {
	// valid RFC3339
	if _, err := CDCStrToTime("2025-01-02T03:04:05Z", time.UTC); err != nil {
		t.Fatalf("CDCStrToTime valid RFC3339 failed: %v", err)
	}
	// valid time.DateTime in local tz
	if _, err := CDCStrToTime("2025-01-02 03:04:05", time.Local); err != nil {
		t.Fatalf("CDCStrToTime valid time.DateTime failed: %v", err)
	}
	// empty string -> zero, nil error
	if ts, err := CDCStrToTime("", time.UTC); err != nil || !ts.IsZero() {
		t.Fatalf("CDCStrToTime empty got ts=%v err=%v", ts, err)
	}
}

func Test_toHours(t *testing.T) {
	cases := []struct {
		val  int64
		unit string
		want int64
	}{
		{1, "h", 1},
		{2, "d", 48},
		{1, "mo", 24 * 30},
		{1, "y", 24 * 365},
		{5, "unknown", 5},
	}
	for _, c := range cases {
		if got := toHours(c.val, c.unit); got != c.want {
			t.Fatalf("toHours(%d,%q)=%d, want %d", c.val, c.unit, got, c.want)
		}
	}
}

// TestDropDatabase_SnapshotRefreshAfterExclusiveLock verifies that DropDatabase
// refreshes the transaction snapshot after acquiring the exclusive lock on
// mo_database. This prevents the race condition where a concurrent CLONE
// (CREATE TABLE) commits between the snapshot and the lock acquisition,
// leaving orphan records in mo_tables.
func TestDropDatabase_SnapshotRefreshAfterExclusiveLock(t *testing.T) {
	dropDbDef := &plan2.DropDatabase{
		IfExists: false,
		Database: "test_db",
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

	snapshotTS := timestamp.Timestamp{PhysicalTime: 100}
	latestCommitTS := timestamp.Timestamp{PhysicalTime: 200}
	appliedTS := timestamp.Timestamp{PhysicalTime: 200}

	// Test 1: When snapshotTS < latestCommitTS, UpdateSnapshot MUST be called.
	t.Run("snapshot_refreshed_when_stale", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		proc := testutil.NewProcess(t)
		proc.Base.SessionInfo.Buf = buffer.New()
		ctx := defines.AttachAccountId(context.Background(), sysAccountId)
		proc.Ctx = ctx
		proc.ReplaceTopCtx(ctx)

		txnOp := mock_frontend.NewMockTxnOperator(ctrl)
		txnOp.EXPECT().Commit(gomock.Any()).Return(nil).AnyTimes()
		txnOp.EXPECT().Rollback(gomock.Any()).Return(nil).AnyTimes()
		txnOp.EXPECT().GetWorkspace().Return(&Ws{}).AnyTimes()
		txnOp.EXPECT().Txn().Return(txn.TxnMeta{
			Mode:       txn.TxnMode_Pessimistic,
			Isolation:  txn.TxnIsolation_RC,
			SnapshotTS: snapshotTS,
		}).AnyTimes()
		txnOp.EXPECT().TxnOptions().Return(txn.TxnOptions{}).AnyTimes()
		txnOp.EXPECT().NextSequence().Return(uint64(0)).AnyTimes()
		txnOp.EXPECT().EnterRunSqlWithTokenAndSQL(gomock.Any(), gomock.Any()).Return(uint64(0)).AnyTimes()
		txnOp.EXPECT().ExitRunSqlWithToken(gomock.Any()).Return().AnyTimes()
		txnOp.EXPECT().Snapshot().Return(txn.CNTxnSnapshot{}, nil).AnyTimes()
		txnOp.EXPECT().Status().Return(txn.TxnStatus_Active).AnyTimes()

		// Key assertion: UpdateSnapshot must be called with the applied timestamp.
		txnOp.EXPECT().UpdateSnapshot(gomock.Any(), appliedTS).Return(nil).Times(1)

		txnCli := mock_frontend.NewMockTxnClient(ctrl)
		txnCli.EXPECT().New(gomock.Any(), gomock.Any()).Return(txnOp, nil).AnyTimes()
		txnCli.EXPECT().GetLatestCommitTS().Return(latestCommitTS).Times(1)
		txnCli.EXPECT().WaitLogTailAppliedAt(gomock.Any(), latestCommitTS).Return(appliedTS, nil).Times(1)

		proc.Base.TxnClient = txnCli
		proc.Base.TxnOperator = txnOp

		mockDb := mock_frontend.NewMockDatabase(ctrl)
		mockDb.EXPECT().IsSubscription(gomock.Any()).Return(false).AnyTimes()
		// Relations returns an error to stop execution after the snapshot refresh.
		// The important thing is that UpdateSnapshot was called before we get here.
		mockDb.EXPECT().Relations(gomock.Any()).Return(nil, moerr.NewInternalErrorNoCtx("stop here")).AnyTimes()

		eng := mock_frontend.NewMockEngine(ctrl)
		eng.EXPECT().Database(gomock.Any(), "test_db", gomock.Any()).Return(mockDb, nil).AnyTimes()

		lockMoDb := gostub.Stub(&doLockMoDatabase, func(_ *Compile, _ string, _ lock.LockMode) error {
			return nil
		})
		defer lockMoDb.Reset()

		c := NewCompile("test", "test", "drop database test_db", "", "", eng, proc, nil, false, nil, time.Now())
		err := s.DropDatabase(c)
		// The test will error at Relations(), but the key assertion is that
		// UpdateSnapshot was called (enforced by Times(1) on the mock).
		assert.Error(t, err)
	})

	// Test 2: When snapshotTS >= latestCommitTS, UpdateSnapshot must NOT be called.
	t.Run("snapshot_not_refreshed_when_fresh", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		proc := testutil.NewProcess(t)
		proc.Base.SessionInfo.Buf = buffer.New()
		ctx := defines.AttachAccountId(context.Background(), sysAccountId)
		proc.Ctx = ctx
		proc.ReplaceTopCtx(ctx)

		freshSnapshotTS := timestamp.Timestamp{PhysicalTime: 300}
		staleCommitTS := timestamp.Timestamp{PhysicalTime: 200}

		txnOp := mock_frontend.NewMockTxnOperator(ctrl)
		txnOp.EXPECT().Commit(gomock.Any()).Return(nil).AnyTimes()
		txnOp.EXPECT().Rollback(gomock.Any()).Return(nil).AnyTimes()
		txnOp.EXPECT().GetWorkspace().Return(&Ws{}).AnyTimes()
		txnOp.EXPECT().Txn().Return(txn.TxnMeta{
			Mode:       txn.TxnMode_Pessimistic,
			Isolation:  txn.TxnIsolation_RC,
			SnapshotTS: freshSnapshotTS,
		}).AnyTimes()
		txnOp.EXPECT().TxnOptions().Return(txn.TxnOptions{}).AnyTimes()
		txnOp.EXPECT().NextSequence().Return(uint64(0)).AnyTimes()
		txnOp.EXPECT().EnterRunSqlWithTokenAndSQL(gomock.Any(), gomock.Any()).Return(uint64(0)).AnyTimes()
		txnOp.EXPECT().ExitRunSqlWithToken(gomock.Any()).Return().AnyTimes()
		txnOp.EXPECT().Snapshot().Return(txn.CNTxnSnapshot{}, nil).AnyTimes()
		txnOp.EXPECT().Status().Return(txn.TxnStatus_Active).AnyTimes()

		// Key assertion: UpdateSnapshot must NOT be called.
		txnOp.EXPECT().UpdateSnapshot(gomock.Any(), gomock.Any()).Times(0)

		txnCli := mock_frontend.NewMockTxnClient(ctrl)
		txnCli.EXPECT().New(gomock.Any(), gomock.Any()).Return(txnOp, nil).AnyTimes()
		txnCli.EXPECT().GetLatestCommitTS().Return(staleCommitTS).Times(1)
		// WaitLogTailAppliedAt should NOT be called either.
		txnCli.EXPECT().WaitLogTailAppliedAt(gomock.Any(), gomock.Any()).Times(0)

		proc.Base.TxnClient = txnCli
		proc.Base.TxnOperator = txnOp

		mockDb := mock_frontend.NewMockDatabase(ctrl)
		mockDb.EXPECT().IsSubscription(gomock.Any()).Return(false).AnyTimes()
		mockDb.EXPECT().Relations(gomock.Any()).Return(nil, moerr.NewInternalErrorNoCtx("stop here")).AnyTimes()

		eng := mock_frontend.NewMockEngine(ctrl)
		eng.EXPECT().Database(gomock.Any(), "test_db", gomock.Any()).Return(mockDb, nil).AnyTimes()

		lockMoDb := gostub.Stub(&doLockMoDatabase, func(_ *Compile, _ string, _ lock.LockMode) error {
			return nil
		})
		defer lockMoDb.Reset()

		c := NewCompile("test", "test", "drop database test_db", "", "", eng, proc, nil, false, nil, time.Now())
		err := s.DropDatabase(c)
		assert.Error(t, err)
	})
}

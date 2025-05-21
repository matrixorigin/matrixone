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

	"github.com/golang/mock/gomock"
	"github.com/prashantv/gostub"
	"github.com/smartystreets/goconvey/convey"
	"github.com/stretchr/testify/assert"

	"github.com/matrixorigin/matrixone/pkg/common/buffer"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	"github.com/matrixorigin/matrixone/pkg/pb/lock"
	plan2 "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func TestScope_AlterTableInplace(t *testing.T) {
	tableDef := &plan.TableDef{
		TblId: 282826,
		Name:  "dept",
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
		Indexes: []*plan.IndexDef{
			{
				IndexName:      "idxloc",
				Parts:          []string{"loc", "__mo_alias_deptno"},
				Unique:         false,
				IndexTableName: "__mo_index_secondary_0193dc98-4148-74f4-808a",
				TableExist:     true,
			},
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

	alterTable := &plan2.AlterTable{
		Database: "test",
		TableDef: tableDef,
		Actions: []*plan2.AlterTable_Action{
			{
				Action: &plan2.AlterTable_Action_AddIndex{
					AddIndex: &plan2.AlterTableAddIndex{
						DbName:                "test",
						TableName:             "dept",
						OriginTablePrimaryKey: "deptno",
						IndexTableExist:       true,
						IndexInfo: &plan2.CreateTable{
							TableDef: &plan.TableDef{
								Indexes: []*plan.IndexDef{
									{
										IndexName:      "idx",
										Parts:          []string{"dname", "__mo_alias_deptno"},
										Unique:         false,
										IndexTableName: "__mo_index_secondary_0193d918",
										TableExist:     true,
									},
								},
							},
							IndexTables: []*plan.TableDef{
								{
									Name: "__mo_index_secondary_0193d918-3e7b",
									Cols: []*plan.ColDef{
										{
											Name: "__mo_index_idx_col",
											Alg:  plan2.CompressType_Lz4,
											Typ: plan.Type{
												Id:          61,
												NotNullable: false,
												AutoIncr:    false,
												Width:       65535,
												Scale:       0,
											},
											NotNull: false,
											Default: &plan2.Default{
												NullAbility: false,
											},
											Pkidx: 0,
										},
										{
											Name: "__mo_index_pri_col",
											Alg:  plan2.CompressType_Lz4,
											Typ: plan.Type{
												Id:          27,
												NotNullable: false,
												AutoIncr:    false,
												Width:       32,
												Scale:       -1,
											},
											NotNull: false,
											Default: &plan2.Default{
												NullAbility: false,
											},
											Pkidx: 0,
										},
									},
									Pkey: &plan2.PrimaryKeyDef{
										PkeyColName: "__mo_index_idx_col",
										Names:       []string{"__mo_index_idx_col"},
									},
								},
							},
						},
					},
				},
			},
		},
	}

	cplan := &plan.Plan{
		Plan: &plan2.Plan_Ddl{
			Ddl: &plan2.DataDefinition{
				DdlType: plan2.DataDefinition_ALTER_TABLE,
				Definition: &plan2.DataDefinition_AlterTable{
					AlterTable: alterTable,
				},
			},
		},
	}

	s := &Scope{
		Magic:     AlterTable,
		Plan:      cplan,
		TxnOffset: 0,
	}

	sql := `alter table dept add index idx(dname)`

	convey.Convey("create table lock mo_database", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		proc := testutil.NewProcess()
		proc.Base.SessionInfo.Buf = buffer.New()

		ctx := context.Background()
		proc.Ctx = context.Background()
		txnCli, txnOp := newTestTxnClientAndOpWithPessimistic(ctrl)
		proc.Base.TxnClient = txnCli
		proc.Base.TxnOperator = txnOp
		proc.ReplaceTopCtx(ctx)

		relation := mock_frontend.NewMockRelation(ctrl)
		relation.EXPECT().GetTableID(gomock.Any()).Return(uint64(1)).AnyTimes()

		mockDb := mock_frontend.NewMockDatabase(ctrl)
		mockDb.EXPECT().GetDatabaseId(gomock.Any()).Return("12").AnyTimes()
		mockDb.EXPECT().Relation(gomock.Any(), gomock.Any(), gomock.Any()).Return(relation, nil).AnyTimes()

		eng := mock_frontend.NewMockEngine(ctrl)
		eng.EXPECT().Database(gomock.Any(), gomock.Any(), gomock.Any()).Return(mockDb, nil).AnyTimes()

		getConstraintDef := gostub.Stub(&GetConstraintDef, func(_ context.Context, _ engine.Relation) (*engine.ConstraintDef, error) {
			cstrDef := &engine.ConstraintDef{}
			cstrDef.Cts = make([]engine.Constraint, 0)
			return cstrDef, nil
		})
		defer getConstraintDef.Reset()

		lockMoDb := gostub.Stub(&lockMoDatabase, func(_ *Compile, _ string, _ lock.LockMode) error {
			return moerr.NewTxnNeedRetryWithDefChangedNoCtx()
		})
		defer lockMoDb.Reset()

		c := NewCompile("test", "test", sql, "", "", eng, proc, nil, false, nil, time.Now())
		assert.Error(t, s.AlterTableInplace(c))
	})

	convey.Convey("create table lock mo_tables", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		proc := testutil.NewProcess()
		proc.Base.SessionInfo.Buf = buffer.New()

		ctx := context.Background()
		proc.Ctx = context.Background()
		txnCli, txnOp := newTestTxnClientAndOpWithPessimistic(ctrl)
		proc.Base.TxnClient = txnCli
		proc.Base.TxnOperator = txnOp
		proc.ReplaceTopCtx(ctx)

		relation := mock_frontend.NewMockRelation(ctrl)
		relation.EXPECT().GetTableID(gomock.Any()).Return(uint64(1)).AnyTimes()

		mockDb := mock_frontend.NewMockDatabase(ctrl)
		mockDb.EXPECT().GetDatabaseId(gomock.Any()).Return("12").AnyTimes()
		mockDb.EXPECT().Relation(gomock.Any(), gomock.Any(), gomock.Any()).Return(relation, nil).AnyTimes()

		eng := mock_frontend.NewMockEngine(ctrl)
		eng.EXPECT().Database(gomock.Any(), gomock.Any(), gomock.Any()).Return(mockDb, nil).AnyTimes()

		getConstraintDef := gostub.Stub(&GetConstraintDef, func(_ context.Context, _ engine.Relation) (*engine.ConstraintDef, error) {
			cstrDef := &engine.ConstraintDef{}
			cstrDef.Cts = make([]engine.Constraint, 0)
			return cstrDef, nil
		})
		defer getConstraintDef.Reset()

		lockMoDb := gostub.Stub(&lockMoDatabase, func(_ *Compile, _ string, _ lock.LockMode) error {
			return nil
		})
		defer lockMoDb.Reset()

		lockMoTbl := gostub.Stub(&lockMoTable, func(_ *Compile, _ string, _ string, _ lock.LockMode) error {
			return moerr.NewTxnNeedRetryNoCtx()
		})
		defer lockMoTbl.Reset()

		lockTbl := gostub.Stub(&lockTable, func(_ context.Context, _ engine.Engine, _ *process.Process, _ engine.Relation, _ string, _ bool) error {
			return moerr.NewTxnNeedRetryNoCtx()
		})
		defer lockTbl.Reset()

		lockIdxTbl := gostub.Stub(&lockIndexTable, func(_ context.Context, _ engine.Database, _ engine.Engine, _ *process.Process, _ string, _ bool) error {
			return moerr.NewParseErrorNoCtx("table \"__mo_index_unique_0192748f-6868-7182-a6de-2e457c2975c6\" does not exist")
		})
		defer lockIdxTbl.Reset()

		c := NewCompile("test", "test", sql, "", "", eng, proc, nil, false, nil, time.Now())
		assert.Error(t, s.AlterTableInplace(c))
	})

	convey.Convey("create table lock index table1", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		proc := testutil.NewProcess()
		proc.Base.SessionInfo.Buf = buffer.New()

		ctx := context.Background()
		proc.Ctx = context.Background()
		txnCli, txnOp := newTestTxnClientAndOpWithPessimistic(ctrl)
		proc.Base.TxnClient = txnCli
		proc.Base.TxnOperator = txnOp
		proc.ReplaceTopCtx(ctx)

		relation := mock_frontend.NewMockRelation(ctrl)
		relation.EXPECT().GetTableID(gomock.Any()).Return(uint64(1)).AnyTimes()

		mockDb := mock_frontend.NewMockDatabase(ctrl)
		mockDb.EXPECT().GetDatabaseId(gomock.Any()).Return("12").AnyTimes()
		mockDb.EXPECT().Relation(gomock.Any(), gomock.Any(), gomock.Any()).Return(relation, nil).AnyTimes()

		eng := mock_frontend.NewMockEngine(ctrl)
		eng.EXPECT().Database(gomock.Any(), gomock.Any(), gomock.Any()).Return(mockDb, nil).AnyTimes()

		getConstraintDef := gostub.Stub(&GetConstraintDef, func(_ context.Context, _ engine.Relation) (*engine.ConstraintDef, error) {
			cstrDef := &engine.ConstraintDef{}
			cstrDef.Cts = make([]engine.Constraint, 0)
			return cstrDef, nil
		})
		defer getConstraintDef.Reset()

		lockMoDb := gostub.Stub(&lockMoDatabase, func(_ *Compile, _ string, _ lock.LockMode) error {
			return nil
		})
		defer lockMoDb.Reset()

		lockMoTbl := gostub.Stub(&lockMoTable, func(_ *Compile, _ string, _ string, _ lock.LockMode) error {
			return moerr.NewTxnNeedRetryNoCtx()
		})
		defer lockMoTbl.Reset()

		lockTbl := gostub.Stub(&lockTable, func(_ context.Context, _ engine.Engine, _ *process.Process, _ engine.Relation, _ string, _ bool) error {
			return moerr.NewTxnNeedRetryNoCtx()
		})
		defer lockTbl.Reset()

		lockIdxTbl := gostub.Stub(&lockIndexTable, func(_ context.Context, _ engine.Database, _ engine.Engine, _ *process.Process, _ string, _ bool) error {
			return moerr.NewParseErrorNoCtx("table \"__mo_index_unique_0192748f-6868-7182-a6de-2e457c2975c6\" does not exist")
		})
		defer lockIdxTbl.Reset()

		c := NewCompile("test", "test", sql, "", "", eng, proc, nil, false, nil, time.Now())
		assert.Error(t, s.AlterTableCopy(c))
	})

	convey.Convey("create table lock index table2", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		proc := testutil.NewProcess()
		proc.Base.SessionInfo.Buf = buffer.New()

		ctx := context.Background()
		proc.Ctx = context.Background()
		txnCli, txnOp := newTestTxnClientAndOpWithPessimistic(ctrl)
		proc.Base.TxnClient = txnCli
		proc.Base.TxnOperator = txnOp
		proc.ReplaceTopCtx(ctx)

		relation := mock_frontend.NewMockRelation(ctrl)
		relation.EXPECT().GetTableID(gomock.Any()).Return(uint64(1)).AnyTimes()

		mockDb := mock_frontend.NewMockDatabase(ctrl)
		mockDb.EXPECT().GetDatabaseId(gomock.Any()).Return("12").AnyTimes()
		mockDb.EXPECT().Relation(gomock.Any(), gomock.Any(), gomock.Any()).Return(relation, nil).AnyTimes()

		eng := mock_frontend.NewMockEngine(ctrl)
		eng.EXPECT().Database(gomock.Any(), gomock.Any(), gomock.Any()).Return(mockDb, nil).AnyTimes()

		getConstraintDef := gostub.Stub(&GetConstraintDef, func(_ context.Context, _ engine.Relation) (*engine.ConstraintDef, error) {
			cstrDef := &engine.ConstraintDef{}
			cstrDef.Cts = make([]engine.Constraint, 0)
			return cstrDef, nil
		})
		defer getConstraintDef.Reset()

		lockMoDb := gostub.Stub(&lockMoDatabase, func(_ *Compile, _ string, _ lock.LockMode) error {
			return nil
		})
		defer lockMoDb.Reset()

		lockMoTbl := gostub.Stub(&lockMoTable, func(_ *Compile, _ string, _ string, _ lock.LockMode) error {
			return moerr.NewTxnNeedRetryNoCtx()
		})
		defer lockMoTbl.Reset()

		lockTbl := gostub.Stub(&lockTable, func(_ context.Context, _ engine.Engine, _ *process.Process, _ engine.Relation, _ string, _ bool) error {
			return moerr.NewTxnNeedRetryNoCtx()
		})
		defer lockTbl.Reset()

		lockIdxTbl := gostub.Stub(&lockIndexTable, func(_ context.Context, _ engine.Database, _ engine.Engine, _ *process.Process, _ string, _ bool) error {
			return moerr.NewTxnNeedRetryNoCtx()
		})
		defer lockIdxTbl.Reset()

		c := NewCompile("test", "test", sql, "", "", eng, proc, nil, false, nil, time.Now())
		assert.Error(t, s.AlterTableInplace(c))
	})
}

func TestScope_AlterTableCopy(t *testing.T) {
	tableDef := &plan.TableDef{
		TblId: 282826,
		Name:  "dept",
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
		Indexes: []*plan.IndexDef{
			{
				IndexName:      "idxloc",
				Parts:          []string{"loc", "__mo_alias_deptno"},
				Unique:         false,
				IndexTableName: "__mo_index_secondary_0193dc98-4148-74f4-808a",
				TableExist:     true,
			},
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

	copyTableDef := &plan.TableDef{
		TblId: 282826,
		Name:  "dept_copy_0193dcb4-4c07-77d8",
		Cols: []*plan.ColDef{
			{
				ColId: 1,
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
				ColId: 2,
				Name:  "dname",
				Alg:   plan2.CompressType_Lz4,
				Typ: plan.Type{
					Id:          61,
					NotNullable: false,
					AutoIncr:    false,
					Width:       20,
					Scale:       0,
				},
				Default: &plan2.Default{},
				NotNull: false,
				Primary: false,
				Pkidx:   0,
			},
			{
				ColId: 3,
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
			{
				ColId:  4,
				Name:   "__mo_rowid",
				Hidden: true,
				Alg:    plan2.CompressType_Lz4,
				Typ: plan.Type{
					Id:          101,
					NotNullable: true,
					AutoIncr:    false,
					Width:       0,
					Scale:       0,
					Table:       "dept",
				},
				Default: &plan2.Default{},
				NotNull: false,
				Primary: false,
				Pkidx:   0,
			},
		},
		TableType: "r",
		Createsql: `create table dept (deptno int unsigned auto_increment comment "部门编号", dname varchar(15) comment "部门名称", loc varchar(50) comment "部门所在位置", index idxloc (loc), primary key (deptno)) comment = '部门表'`,
		Pkey: &plan.PrimaryKeyDef{
			Cols:        nil,
			PkeyColId:   0,
			PkeyColName: "deptno",
			Names:       []string{"deptno"},
		},
		Indexes: []*plan.IndexDef{
			{
				IndexName:      "idxloc",
				Parts:          []string{"loc", "__mo_alias_deptno"},
				Unique:         false,
				IndexTableName: "__mo_index_secondary_0193dc98-4148-74f4-808a",
				TableExist:     true,
			},
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

	alterTable := &plan2.AlterTable{
		Database:     "test",
		TableDef:     tableDef,
		CopyTableDef: copyTableDef,
	}

	cplan := &plan.Plan{
		Plan: &plan2.Plan_Ddl{
			Ddl: &plan2.DataDefinition{
				DdlType: plan2.DataDefinition_ALTER_TABLE,
				Definition: &plan2.DataDefinition_AlterTable{
					AlterTable: alterTable,
				},
			},
		},
	}

	s := &Scope{
		Magic:     AlterTable,
		Plan:      cplan,
		TxnOffset: 0,
	}

	sql := `alter table dept add index idx(dname)`

	convey.Convey("create table lock mo_database", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		proc := testutil.NewProcess()
		proc.Base.SessionInfo.Buf = buffer.New()

		ctx := context.Background()
		proc.Ctx = context.Background()
		txnCli, txnOp := newTestTxnClientAndOpWithPessimistic(ctrl)
		proc.Base.TxnClient = txnCli
		proc.Base.TxnOperator = txnOp
		proc.ReplaceTopCtx(ctx)

		relation := mock_frontend.NewMockRelation(ctrl)
		relation.EXPECT().GetTableID(gomock.Any()).Return(uint64(1)).AnyTimes()

		mockDb := mock_frontend.NewMockDatabase(ctrl)
		mockDb.EXPECT().GetDatabaseId(gomock.Any()).Return("12").AnyTimes()
		mockDb.EXPECT().Relation(gomock.Any(), gomock.Any(), gomock.Any()).Return(relation, nil).AnyTimes()

		eng := mock_frontend.NewMockEngine(ctrl)
		eng.EXPECT().Database(gomock.Any(), gomock.Any(), gomock.Any()).Return(mockDb, nil).AnyTimes()

		getConstraintDef := gostub.Stub(&GetConstraintDef, func(_ context.Context, _ engine.Relation) (*engine.ConstraintDef, error) {
			return nil, nil
		})
		defer getConstraintDef.Reset()

		lockMoDb := gostub.Stub(&lockMoDatabase, func(_ *Compile, _ string, _ lock.LockMode) error {
			return moerr.NewTxnNeedRetryWithDefChangedNoCtx()
		})
		defer lockMoDb.Reset()

		c := NewCompile("test", "test", sql, "", "", eng, proc, nil, false, nil, time.Now())
		assert.Error(t, s.AlterTableCopy(c))
	})

	convey.Convey("create table lock index table1", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		proc := testutil.NewProcess()
		proc.Base.SessionInfo.Buf = buffer.New()

		ctx := context.Background()
		proc.Ctx = context.Background()
		txnCli, txnOp := newTestTxnClientAndOpWithPessimistic(ctrl)
		proc.Base.TxnClient = txnCli
		proc.Base.TxnOperator = txnOp
		proc.ReplaceTopCtx(ctx)

		relation := mock_frontend.NewMockRelation(ctrl)
		relation.EXPECT().GetTableID(gomock.Any()).Return(uint64(1)).AnyTimes()

		mockDb := mock_frontend.NewMockDatabase(ctrl)
		mockDb.EXPECT().GetDatabaseId(gomock.Any()).Return("12").AnyTimes()
		mockDb.EXPECT().Relation(gomock.Any(), gomock.Any(), gomock.Any()).Return(relation, nil).AnyTimes()

		eng := mock_frontend.NewMockEngine(ctrl)
		eng.EXPECT().Database(gomock.Any(), gomock.Any(), gomock.Any()).Return(mockDb, nil).AnyTimes()

		getConstraintDef := gostub.Stub(&GetConstraintDef, func(_ context.Context, _ engine.Relation) (*engine.ConstraintDef, error) {
			return nil, nil
		})
		defer getConstraintDef.Reset()

		lockMoDb := gostub.Stub(&lockMoDatabase, func(_ *Compile, _ string, _ lock.LockMode) error {
			return nil
		})
		defer lockMoDb.Reset()

		lockMoTbl := gostub.Stub(&lockMoTable, func(_ *Compile, _ string, _ string, _ lock.LockMode) error {
			return moerr.NewTxnNeedRetryNoCtx()
		})
		defer lockMoTbl.Reset()

		lockTbl := gostub.Stub(&lockTable, func(_ context.Context, _ engine.Engine, _ *process.Process, _ engine.Relation, _ string, _ bool) error {
			return moerr.NewTxnNeedRetryNoCtx()
		})
		defer lockTbl.Reset()

		lockIdxTbl := gostub.Stub(&lockIndexTable, func(_ context.Context, _ engine.Database, _ engine.Engine, _ *process.Process, _ string, _ bool) error {
			return moerr.NewParseErrorNoCtx("table \"__mo_index_unique_0192748f-6868-7182-a6de-2e457c2975c6\" does not exist")
		})
		defer lockIdxTbl.Reset()

		c := NewCompile("test", "test", sql, "", "", eng, proc, nil, false, nil, time.Now())
		assert.Error(t, s.AlterTableCopy(c))
	})

	convey.Convey("create table lock index table2", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		proc := testutil.NewProcess()
		proc.Base.SessionInfo.Buf = buffer.New()

		ctx := context.Background()
		proc.Ctx = context.Background()
		txnCli, txnOp := newTestTxnClientAndOpWithPessimistic(ctrl)
		proc.Base.TxnClient = txnCli
		proc.Base.TxnOperator = txnOp
		proc.ReplaceTopCtx(ctx)

		relation := mock_frontend.NewMockRelation(ctrl)
		relation.EXPECT().GetTableID(gomock.Any()).Return(uint64(1)).AnyTimes()

		mockDb := mock_frontend.NewMockDatabase(ctrl)
		mockDb.EXPECT().GetDatabaseId(gomock.Any()).Return("12").AnyTimes()
		mockDb.EXPECT().Relation(gomock.Any(), gomock.Any(), gomock.Any()).Return(relation, nil).AnyTimes()

		eng := mock_frontend.NewMockEngine(ctrl)
		eng.EXPECT().Database(gomock.Any(), gomock.Any(), gomock.Any()).Return(mockDb, nil).AnyTimes()

		getConstraintDef := gostub.Stub(&GetConstraintDef, func(_ context.Context, _ engine.Relation) (*engine.ConstraintDef, error) {
			return nil, nil
		})
		defer getConstraintDef.Reset()

		lockMoDb := gostub.Stub(&lockMoDatabase, func(_ *Compile, _ string, _ lock.LockMode) error {
			return nil
		})
		defer lockMoDb.Reset()

		lockMoTbl := gostub.Stub(&lockMoTable, func(_ *Compile, _ string, _ string, _ lock.LockMode) error {
			return moerr.NewTxnNeedRetryNoCtx()
		})
		defer lockMoTbl.Reset()

		lockTbl := gostub.Stub(&lockTable, func(_ context.Context, _ engine.Engine, _ *process.Process, _ engine.Relation, _ string, _ bool) error {
			return moerr.NewTxnNeedRetryNoCtx()
		})
		defer lockTbl.Reset()

		lockIdxTbl := gostub.Stub(&lockIndexTable, func(_ context.Context, _ engine.Database, _ engine.Engine, _ *process.Process, _ string, _ bool) error {
			return moerr.NewTxnNeedRetryNoCtx()
		})
		defer lockIdxTbl.Reset()

		c := NewCompile("test", "test", sql, "", "", eng, proc, nil, false, nil, time.Now())
		assert.Error(t, s.AlterTableCopy(c))
	})
}

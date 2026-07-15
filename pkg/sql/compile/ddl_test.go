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
	"errors"
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
	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/buffer"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/defines"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	"github.com/matrixorigin/matrixone/pkg/incrservice"
	plan2 "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	hnswruntime "github.com/matrixorigin/matrixone/pkg/vectorindex/hnsw/plugin/runtime"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

type fixedResultSQLExecutor struct {
	result executor.Result
	err    error
	calls  int
}

func (e *fixedResultSQLExecutor) Exec(ctx context.Context, _ string, _ executor.Options) (executor.Result, error) {
	e.calls++
	if err := ctx.Err(); err != nil {
		return executor.Result{}, err
	}
	return e.result, e.err
}

func (e *fixedResultSQLExecutor) ExecTxn(context.Context, func(executor.TxnExecutor) error, executor.Options) error {
	return e.err
}

func TestConvertDBEOBToNoSuchTable(t *testing.T) {
	err := convertDBEOBToNoSuchTable(context.Background(), moerr.GetOkExpectedEOB(), "db1", "t2")
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrNoSuchTable))
	require.Contains(t, err.Error(), "no such table db1.t2")
}

func TestConvertDBEOBToNoSuchTablePassThrough(t *testing.T) {
	want := moerr.NewBadDB(context.Background(), "db1")
	got := convertDBEOBToNoSuchTable(context.Background(), want, "db1", "t2")
	require.Same(t, want, got)
}

func TestAlterTableInplaceAppendsAutoIncrementSchemaMutation(t *testing.T) {
	const requestedOffset = uint64(99)
	const effectiveOffset = uint64(120)

	proc := testutil.NewProcess(t)
	proc.Base.SessionInfo.Buf = buffer.New()
	proc.Ctx = defines.AttachAccountId(context.Background(), sysAccountId)
	txnOp, closeTxn := client.NewTestTxnOperator(proc.Ctx)
	t.Cleanup(closeTxn)
	proc.Base.TxnOperator = txnOp

	result := newAlterCopyFixedResult(t, proc.Mp(), types.T_uint64.ToType(), []uint64{effectiveOffset})
	moruntime.ServiceRuntime(proc.GetService()).SetGlobalVariables(
		moruntime.InternalSQLExecutor,
		&fixedResultSQLExecutor{result: result},
	)

	ctrl := gomock.NewController(t)
	autoSvc := mock_frontend.NewMockAutoIncrementService(ctrl)
	autoSvc.EXPECT().SetOffset(proc.Ctx, uint64(1), "id", effectiveOffset, txnOp).Return(nil)
	incrservice.SetAutoIncrementServiceByID(proc.GetService(), autoSvc)

	rel := newStubRelation("t")
	rel.dbID = 1
	rel.extra = &api.SchemaExtra{}
	db := newStubDatabase("db")
	db.rels["t"] = rel
	eng := newStubEngine()
	eng.dbs["db"] = db
	lockMoDb := gostub.Stub(&lockMoDatabase, func(*Compile, string, lock.LockMode) error { return nil })
	t.Cleanup(lockMoDb.Reset)
	lockMoTbl := gostub.Stub(&lockMoTable, func(*Compile, string, string, lock.LockMode) error { return nil })
	t.Cleanup(lockMoTbl.Reset)
	lockTbl := gostub.Stub(&lockTable, func(context.Context, engine.Engine, *process.Process, engine.Relation, string, bool) error {
		return nil
	})
	t.Cleanup(lockTbl.Reset)

	tableDef := &plan2.TableDef{
		Name: "t",
		Cols: []*plan2.ColDef{{
			Name: "id",
			Typ:  plan2.Type{Id: int32(types.T_uint64), AutoIncr: true},
		}},
	}
	s := &Scope{Plan: &plan2.Plan{Plan: &plan2.Plan_Ddl{Ddl: &plan2.DataDefinition{
		Definition: &plan2.DataDefinition_AlterTable{AlterTable: &plan2.AlterTable{
			Database: "db",
			TableDef: tableDef,
			Actions: []*plan2.AlterTable_Action{{Action: &plan2.AlterTable_Action_AlterAutoIncrement{
				AlterAutoIncrement: &plan2.AlterTableAutoIncrement{NewOffset: requestedOffset},
			}}},
		}},
	}}}}

	c := &Compile{e: eng, proc: proc, db: "db", pn: s.Plan}
	require.NoError(t, s.AlterTableInplace(c))
	require.Len(t, rel.alterReqs, 1)
	require.Equal(t, api.NewUpdateAutoIncrementReq(1, 1, effectiveOffset), rel.alterReqs[0])
}

func TestAlterTableInplaceSetOffsetErrorBeforeSchemaMutation(t *testing.T) {
	const requestedOffset = uint64(99)
	const effectiveOffset = uint64(120)

	proc := testutil.NewProcess(t)
	proc.Base.SessionInfo.Buf = buffer.New()
	proc.Ctx = defines.AttachAccountId(context.Background(), sysAccountId)
	txnOp, closeTxn := client.NewTestTxnOperator(proc.Ctx)
	t.Cleanup(closeTxn)
	proc.Base.TxnOperator = txnOp

	result := newAlterCopyFixedResult(t, proc.Mp(), types.T_uint64.ToType(), []uint64{effectiveOffset})
	moruntime.ServiceRuntime(proc.GetService()).SetGlobalVariables(
		moruntime.InternalSQLExecutor,
		&fixedResultSQLExecutor{result: result},
	)

	ctrl := gomock.NewController(t)
	autoSvc := mock_frontend.NewMockAutoIncrementService(ctrl)
	autoSvc.EXPECT().SetOffset(proc.Ctx, uint64(1), "id", effectiveOffset, txnOp).Return(context.Canceled)
	incrservice.SetAutoIncrementServiceByID(proc.GetService(), autoSvc)

	rel := newStubRelation("t")
	rel.dbID = 1
	rel.extra = &api.SchemaExtra{}
	db := newStubDatabase("db")
	db.rels["t"] = rel
	eng := newStubEngine()
	eng.dbs["db"] = db
	lockMoDb := gostub.Stub(&lockMoDatabase, func(*Compile, string, lock.LockMode) error { return nil })
	t.Cleanup(lockMoDb.Reset)
	lockMoTbl := gostub.Stub(&lockMoTable, func(*Compile, string, string, lock.LockMode) error { return nil })
	t.Cleanup(lockMoTbl.Reset)
	lockTbl := gostub.Stub(&lockTable, func(context.Context, engine.Engine, *process.Process, engine.Relation, string, bool) error {
		return nil
	})
	t.Cleanup(lockTbl.Reset)

	tableDef := &plan2.TableDef{
		Name: "t",
		Cols: []*plan2.ColDef{{
			Name: "id",
			Typ:  plan2.Type{Id: int32(types.T_uint64), AutoIncr: true},
		}},
	}
	s := &Scope{Plan: &plan2.Plan{Plan: &plan2.Plan_Ddl{Ddl: &plan2.DataDefinition{
		Definition: &plan2.DataDefinition_AlterTable{AlterTable: &plan2.AlterTable{
			Database: "db",
			TableDef: tableDef,
			Actions: []*plan2.AlterTable_Action{{Action: &plan2.AlterTable_Action_AlterAutoIncrement{
				AlterAutoIncrement: &plan2.AlterTableAutoIncrement{NewOffset: requestedOffset},
			}}},
		}},
	}}}}

	c := &Compile{e: eng, proc: proc, db: "db", pn: s.Plan}
	require.ErrorIs(t, s.AlterTableInplace(c), context.Canceled)
	require.Empty(t, rel.alterReqs)
}

func TestAlterTableInplaceDiscardsOffsetResetWhenSchemaMutationFails(t *testing.T) {
	const (
		requestedOffset = uint64(99)
		effectiveOffset = uint64(120)
	)
	alterErr := errors.New("alter table failed after offset reset")

	proc := testutil.NewProcess(t)
	proc.Base.SessionInfo.Buf = buffer.New()
	proc.Ctx = defines.AttachAccountId(context.Background(), sysAccountId)
	txnOp, closeTxn := client.NewTestTxnOperator(proc.Ctx)
	t.Cleanup(closeTxn)
	proc.Base.TxnOperator = txnOp

	result := newAlterCopyFixedResult(t, proc.Mp(), types.T_uint64.ToType(), []uint64{effectiveOffset})
	moruntime.ServiceRuntime(proc.GetService()).SetGlobalVariables(
		moruntime.InternalSQLExecutor,
		&fixedResultSQLExecutor{result: result},
	)

	ctrl := gomock.NewController(t)
	autoSvc := mock_frontend.NewMockAutoIncrementService(ctrl)
	autoSvc.EXPECT().SetOffset(proc.Ctx, uint64(1), "id", effectiveOffset, txnOp).Return(nil)
	autoSvc.EXPECT().DiscardOffsetReset(gomock.Any(), uint64(1), txnOp).Return(nil)
	incrservice.SetAutoIncrementServiceByID(proc.GetService(), autoSvc)

	rel := newStubRelation("t")
	rel.dbID = 1
	rel.extra = &api.SchemaExtra{}
	rel.alterErr = alterErr
	db := newStubDatabase("db")
	db.rels["t"] = rel
	eng := newStubEngine()
	eng.dbs["db"] = db
	lockMoDb := gostub.Stub(&lockMoDatabase, func(*Compile, string, lock.LockMode) error { return nil })
	t.Cleanup(lockMoDb.Reset)
	lockMoTbl := gostub.Stub(&lockMoTable, func(*Compile, string, string, lock.LockMode) error { return nil })
	t.Cleanup(lockMoTbl.Reset)
	lockTbl := gostub.Stub(&lockTable, func(context.Context, engine.Engine, *process.Process, engine.Relation, string, bool) error {
		return nil
	})
	t.Cleanup(lockTbl.Reset)

	tableDef := &plan2.TableDef{
		Name: "t",
		Cols: []*plan2.ColDef{{
			Name: "id",
			Typ:  plan2.Type{Id: int32(types.T_uint64), AutoIncr: true},
		}},
	}
	s := &Scope{Plan: &plan2.Plan{Plan: &plan2.Plan_Ddl{Ddl: &plan2.DataDefinition{
		Definition: &plan2.DataDefinition_AlterTable{AlterTable: &plan2.AlterTable{
			Database: "db",
			TableDef: tableDef,
			Actions: []*plan2.AlterTable_Action{{Action: &plan2.AlterTable_Action_AlterAutoIncrement{
				AlterAutoIncrement: &plan2.AlterTableAutoIncrement{NewOffset: requestedOffset},
			}}},
		}},
	}}}}

	c := &Compile{e: eng, proc: proc, db: "db", pn: s.Plan}
	require.ErrorIs(t, s.AlterTableInplace(c), alterErr)
}

func TestAlterTableInplaceDiscardsOffsetResetWhenCanceledAfterSetOffset(t *testing.T) {
	const (
		requestedOffset = uint64(99)
		effectiveOffset = uint64(120)
	)

	proc := testutil.NewProcess(t)
	proc.Base.SessionInfo.Buf = buffer.New()
	baseCtx := defines.AttachAccountId(context.Background(), sysAccountId)
	ctx, cancel := context.WithCancel(baseCtx)
	t.Cleanup(cancel)
	proc.Ctx = ctx
	txnOp, closeTxn := client.NewTestTxnOperator(baseCtx)
	t.Cleanup(closeTxn)
	proc.Base.TxnOperator = txnOp

	result := newAlterCopyFixedResult(t, proc.Mp(), types.T_uint64.ToType(), []uint64{effectiveOffset})
	moruntime.ServiceRuntime(proc.GetService()).SetGlobalVariables(
		moruntime.InternalSQLExecutor,
		&fixedResultSQLExecutor{result: result},
	)

	ctrl := gomock.NewController(t)
	autoSvc := mock_frontend.NewMockAutoIncrementService(ctrl)
	autoSvc.EXPECT().SetOffset(ctx, uint64(1), "id", effectiveOffset, txnOp).DoAndReturn(
		func(context.Context, uint64, string, uint64, client.TxnOperator) error {
			cancel()
			return nil
		},
	)
	autoSvc.EXPECT().DiscardOffsetReset(gomock.Any(), uint64(1), txnOp).Return(nil)
	incrservice.SetAutoIncrementServiceByID(proc.GetService(), autoSvc)

	rel := newStubRelation("t")
	rel.dbID = 1
	rel.extra = &api.SchemaExtra{}
	db := newStubDatabase("db")
	db.rels["t"] = rel
	eng := newStubEngine()
	eng.dbs["db"] = db
	lockMoDb := gostub.Stub(&lockMoDatabase, func(*Compile, string, lock.LockMode) error { return nil })
	t.Cleanup(lockMoDb.Reset)
	lockMoTbl := gostub.Stub(&lockMoTable, func(*Compile, string, string, lock.LockMode) error { return nil })
	t.Cleanup(lockMoTbl.Reset)
	lockTbl := gostub.Stub(&lockTable, func(context.Context, engine.Engine, *process.Process, engine.Relation, string, bool) error {
		return nil
	})
	t.Cleanup(lockTbl.Reset)

	tableDef := &plan2.TableDef{
		Name: "t",
		Cols: []*plan2.ColDef{{
			Name: "id",
			Typ:  plan2.Type{Id: int32(types.T_uint64), AutoIncr: true},
		}},
	}
	s := &Scope{Plan: &plan2.Plan{Plan: &plan2.Plan_Ddl{Ddl: &plan2.DataDefinition{
		Definition: &plan2.DataDefinition_AlterTable{AlterTable: &plan2.AlterTable{
			Database: "db",
			TableDef: tableDef,
			Actions: []*plan2.AlterTable_Action{{Action: &plan2.AlterTable_Action_AlterAutoIncrement{
				AlterAutoIncrement: &plan2.AlterTableAutoIncrement{NewOffset: requestedOffset},
			}}},
		}},
	}}}}

	c := &Compile{e: eng, proc: proc, db: "db", pn: s.Plan}
	require.ErrorIs(t, s.AlterTableInplace(c), context.Canceled)
	require.Empty(t, rel.alterReqs)
}

func TestAlterTableInplacePreCanceledBeforeAutoIncrementSideEffects(t *testing.T) {
	const requestedOffset = uint64(99)
	const effectiveOffset = uint64(120)

	proc := testutil.NewProcess(t)
	proc.Base.SessionInfo.Buf = buffer.New()
	baseCtx := defines.AttachAccountId(context.Background(), sysAccountId)
	txnOp, closeTxn := client.NewTestTxnOperator(baseCtx)
	t.Cleanup(closeTxn)
	proc.Base.TxnOperator = txnOp
	ctx, cancel := context.WithCancel(baseCtx)
	cancel()
	proc.Ctx = ctx

	result := newAlterCopyFixedResult(t, proc.Mp(), types.T_uint64.ToType(), []uint64{effectiveOffset})
	exec := &fixedResultSQLExecutor{result: result}
	moruntime.ServiceRuntime(proc.GetService()).SetGlobalVariables(
		moruntime.InternalSQLExecutor,
		exec,
	)

	ctrl := gomock.NewController(t)
	autoSvc := mock_frontend.NewMockAutoIncrementService(ctrl)
	autoSvc.EXPECT().SetOffset(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Times(0)
	incrservice.SetAutoIncrementServiceByID(proc.GetService(), autoSvc)

	rel := newStubRelation("t")
	rel.dbID = 1
	rel.extra = &api.SchemaExtra{}
	db := newStubDatabase("db")
	db.rels["t"] = rel
	eng := newStubEngine()
	eng.dbs["db"] = db
	lockMoDb := gostub.Stub(&lockMoDatabase, func(*Compile, string, lock.LockMode) error { return nil })
	t.Cleanup(lockMoDb.Reset)
	lockMoTbl := gostub.Stub(&lockMoTable, func(*Compile, string, string, lock.LockMode) error { return nil })
	t.Cleanup(lockMoTbl.Reset)
	lockTbl := gostub.Stub(&lockTable, func(context.Context, engine.Engine, *process.Process, engine.Relation, string, bool) error {
		return nil
	})
	t.Cleanup(lockTbl.Reset)

	tableDef := &plan2.TableDef{
		Name: "t",
		Cols: []*plan2.ColDef{{
			Name: "id",
			Typ:  plan2.Type{Id: int32(types.T_uint64), AutoIncr: true},
		}},
	}
	s := &Scope{Plan: &plan2.Plan{Plan: &plan2.Plan_Ddl{Ddl: &plan2.DataDefinition{
		Definition: &plan2.DataDefinition_AlterTable{AlterTable: &plan2.AlterTable{
			Database: "db",
			TableDef: tableDef,
			Actions: []*plan2.AlterTable_Action{{Action: &plan2.AlterTable_Action_AlterAutoIncrement{
				AlterAutoIncrement: &plan2.AlterTableAutoIncrement{NewOffset: requestedOffset},
			}}},
		}},
	}}}}

	c := &Compile{e: eng, proc: proc, db: "db", pn: s.Plan}
	require.ErrorIs(t, s.AlterTableInplace(c), context.Canceled)
	require.Zero(t, exec.calls)
	require.Empty(t, rel.alterReqs)
}

func TestTableScopedDDLDatabaseEOBMapsToNoSuchTable(t *testing.T) {
	newCompileWithStubEngine := func(t *testing.T, eng *stubEngine, sql string) *Compile {
		t.Helper()
		proc := testutil.NewProcess(t)
		proc.Base.SessionInfo.Buf = buffer.New()
		proc.Ctx = defines.AttachAccountId(context.Background(), sysAccountId)
		return NewCompile("test", "db1", sql, "", "", eng, proc, nil, false, nil, time.Now())
	}

	t.Run("AlterTableInplace", func(t *testing.T) {
		eng := newStubEngine()
		eng.dbErr = moerr.GetOkExpectedEOB()
		s := &Scope{Plan: &plan2.Plan{Plan: &plan2.Plan_Ddl{Ddl: &plan2.DataDefinition{
			Definition: &plan2.DataDefinition_AlterTable{
				AlterTable: &plan2.AlterTable{
					Database: "db1",
					TableDef: &plan2.TableDef{Name: "t2"},
				},
			},
		}}}}

		err := s.AlterTableInplace(newCompileWithStubEngine(t, eng, "alter table t2 add b int"))
		require.True(t, moerr.IsMoErrCode(err, moerr.ErrNoSuchTable))
	})

	t.Run("AlterTableCopy", func(t *testing.T) {
		eng := newStubEngine()
		eng.dbErr = moerr.GetOkExpectedEOB()
		s := &Scope{Plan: &plan2.Plan{Plan: &plan2.Plan_Ddl{Ddl: &plan2.DataDefinition{
			Definition: &plan2.DataDefinition_AlterTable{
				AlterTable: &plan2.AlterTable{
					Database: "db1",
					TableDef: &plan2.TableDef{Name: "t2"},
				},
			},
		}}}}

		err := s.AlterTableCopy(newCompileWithStubEngine(t, eng, "alter table t2 add b int"))
		require.True(t, moerr.IsMoErrCode(err, moerr.ErrNoSuchTable))
	})

	t.Run("CreateIndexDatabaseLookup", func(t *testing.T) {
		eng := newStubEngine()
		eng.dbErr = moerr.GetOkExpectedEOB()
		s := &Scope{Plan: &plan2.Plan{Plan: &plan2.Plan_Ddl{Ddl: &plan2.DataDefinition{
			Definition: &plan2.DataDefinition_CreateIndex{
				CreateIndex: &plan2.CreateIndex{
					Database: "db1",
					Table:    "t2",
					TableDef: &plan2.TableDef{Name: "t2"},
				},
			},
		}}}}

		lockMoDb := gostub.Stub(&lockMoDatabase, func(_ *Compile, _ string, _ lock.LockMode) error { return nil })
		defer lockMoDb.Reset()
		lockMoTbl := gostub.Stub(&lockMoTable, func(_ *Compile, _ string, _ string, _ lock.LockMode) error { return nil })
		defer lockMoTbl.Reset()

		err := s.CreateIndex(newCompileWithStubEngine(t, eng, "create index t2_idx on t2(a)"))
		require.True(t, moerr.IsMoErrCode(err, moerr.ErrNoSuchTable))
	})

	t.Run("CreateIndexLockDatabase", func(t *testing.T) {
		eng := newStubEngine()
		s := &Scope{Plan: &plan2.Plan{Plan: &plan2.Plan_Ddl{Ddl: &plan2.DataDefinition{
			Definition: &plan2.DataDefinition_CreateIndex{
				CreateIndex: &plan2.CreateIndex{
					Database: "db1",
					Table:    "t2",
					TableDef: &plan2.TableDef{Name: "t2"},
				},
			},
		}}}}

		lockMoDb := gostub.Stub(&lockMoDatabase, func(_ *Compile, _ string, _ lock.LockMode) error {
			return moerr.GetOkExpectedEOB()
		})
		defer lockMoDb.Reset()

		err := s.CreateIndex(newCompileWithStubEngine(t, eng, "create index t2_idx on t2(a)"))
		require.True(t, moerr.IsMoErrCode(err, moerr.ErrNoSuchTable))
	})

	t.Run("DropIndex", func(t *testing.T) {
		eng := newStubEngine()
		eng.dbErr = moerr.GetOkExpectedEOB()
		s := &Scope{Plan: &plan2.Plan{Plan: &plan2.Plan_Ddl{Ddl: &plan2.DataDefinition{
			Definition: &plan2.DataDefinition_DropIndex{
				DropIndex: &plan2.DropIndex{
					Database:  "db1",
					Table:     "t2",
					IndexName: "t2_idx",
				},
			},
		}}}}

		lockMoDb := gostub.Stub(&lockMoDatabase, func(_ *Compile, _ string, _ lock.LockMode) error { return nil })
		defer lockMoDb.Reset()

		err := s.DropIndex(newCompileWithStubEngine(t, eng, "drop index t2_idx on t2"))
		require.True(t, moerr.IsMoErrCode(err, moerr.ErrNoSuchTable))
	})

	t.Run("DropIndexIfExistsNoop", func(t *testing.T) {
		s := &Scope{Plan: &plan2.Plan{Plan: &plan2.Plan_Ddl{Ddl: &plan2.DataDefinition{
			Definition: &plan2.DataDefinition_DropIndex{
				DropIndex: &plan2.DropIndex{
					Database: "db1",
					Table:    "t2",
				},
			},
		}}}}

		err := s.DropIndex(newCompileWithStubEngine(t, newStubEngine(), "drop index if exists t2_idx on t2"))
		require.NoError(t, err)
	})

	t.Run("DropTableSingle", func(t *testing.T) {
		eng := newStubEngine()
		eng.dbErr = moerr.GetOkExpectedEOB()
		c := newCompileWithStubEngine(t, eng, "drop table t2")
		s := &Scope{}
		lockMoDb := gostub.Stub(&lockMoDatabase, func(_ *Compile, _ string, _ lock.LockMode) error { return nil })
		defer lockMoDb.Reset()
		err := s.dropTableSingle(c, &plan2.DropTable{
			Database: "db1",
			Table:    "t2",
		})
		require.True(t, moerr.IsMoErrCode(err, moerr.ErrNoSuchTable))
	})
}
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

	enabled, err := s.isExperimentalEnabled(nil, hnswruntime.HnswIndexFlag)
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

// TestDropDatabase_SnapshotAdvance verifies that DropDatabase safely advances
// the workspace snapshot after acquiring the exclusive lock. This prevents a
// concurrent CLONE from leaving orphan records and keeps transaction-local
// tombstones valid at the new snapshot.
func TestDropDatabase_SnapshotAdvance(t *testing.T) {
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

	origSnapshotTS := timestamp.Timestamp{PhysicalTime: 100}

	// Test 1: Pessimistic + RC advances the workspace with HLC Now and keeps
	// the advanced snapshot after DropDatabase returns.
	t.Run("advance_rc_workspace", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		proc := testutil.NewProcess(t)
		proc.Base.SessionInfo.Buf = buffer.New()
		ctx := defines.AttachAccountId(context.Background(), sysAccountId)
		proc.Ctx = ctx
		proc.ReplaceTopCtx(ctx)

		// Use a real TxnMeta so the workspace can simulate snapshot advancement.
		txnMeta := txn.TxnMeta{
			Mode:       txn.TxnMode_Pessimistic,
			Isolation:  txn.TxnIsolation_RC,
			SnapshotTS: origSnapshotTS,
		}

		txnOp := mock_frontend.NewMockTxnOperator(ctrl)
		txnOp.EXPECT().Commit(gomock.Any()).Return(nil).AnyTimes()
		txnOp.EXPECT().Rollback(gomock.Any()).Return(nil).AnyTimes()
		var advancedSnapshotTS timestamp.Timestamp
		ws := &Ws{advanceSnapshot: func(_ context.Context, ts timestamp.Timestamp) error {
			assert.True(t, origSnapshotTS.Less(ts),
				"AdvanceSnapshot should be called with HLC Now > origSnapshotTS")
			txnMeta.SnapshotTS = ts
			advancedSnapshotTS = ts
			return nil
		}}
		txnOp.EXPECT().GetWorkspace().Return(ws).AnyTimes()
		txnOp.EXPECT().Txn().Return(txnMeta).AnyTimes()
		txnOp.EXPECT().TxnOptions().Return(txn.TxnOptions{}).AnyTimes()
		txnOp.EXPECT().NextSequence().Return(uint64(0)).AnyTimes()
		txnOp.EXPECT().EnterRunSqlWithTokenAndSQL(gomock.Any(), gomock.Any()).Return(uint64(0)).AnyTimes()
		txnOp.EXPECT().ExitRunSqlWithToken(gomock.Any()).Return().AnyTimes()
		txnOp.EXPECT().Snapshot().Return(txn.CNTxnSnapshot{}, nil).AnyTimes()
		txnOp.EXPECT().Status().Return(txn.TxnStatus_Active).AnyTimes()
		proc.Base.TxnOperator = txnOp

		mockDb := mock_frontend.NewMockDatabase(ctrl)
		mockDb.EXPECT().IsSubscription(gomock.Any()).Return(false).AnyTimes()
		// Return non-numeric string to skip CCPR check (strconv.ParseUint will fail)
		mockDb.EXPECT().GetDatabaseId(gomock.Any()).Return("invalid").AnyTimes()
		// Relations returns an error to stop execution after the snapshot advance.
		mockDb.EXPECT().Relations(gomock.Any()).DoAndReturn(
			func(_ context.Context) ([]string, error) {
				assert.Equal(t, advancedSnapshotTS, txnMeta.SnapshotTS,
					"Relations must run while DropDatabase is using the advanced SnapshotTS")
				return nil, moerr.NewInternalErrorNoCtx("stop here")
			}).AnyTimes()

		eng := mock_frontend.NewMockEngine(ctrl)
		eng.EXPECT().Database(gomock.Any(), "test_db", gomock.Any()).Return(mockDb, nil).AnyTimes()

		lockMoDb := gostub.Stub(&lockMoDatabase, func(_ *Compile, _ string, _ lock.LockMode) error {
			return nil
		})
		defer lockMoDb.Reset()

		c := NewCompile("test", "test", "drop database test_db", "", "", eng, proc, nil, false, nil, time.Now())
		err := s.DropDatabase(c)
		// DropDatabase errors at Relations(), after the snapshot behavior has
		// already been observed.
		assert.Error(t, err)
		assert.Equal(t, advancedSnapshotTS, txnMeta.SnapshotTS,
			"SnapshotTS must remain advanced after DropDatabase returns")
	})

	// Test 2: non-RC transactions must not advance the workspace snapshot.
	t.Run("skip_advance_for_non_rc", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		proc := testutil.NewProcess(t)
		proc.Base.SessionInfo.Buf = buffer.New()
		ctx := defines.AttachAccountId(context.Background(), sysAccountId)
		proc.Ctx = ctx
		proc.ReplaceTopCtx(ctx)

		txnMeta := txn.TxnMeta{
			Mode:       txn.TxnMode_Optimistic,
			Isolation:  txn.TxnIsolation_SI,
			SnapshotTS: origSnapshotTS,
		}

		txnOp := mock_frontend.NewMockTxnOperator(ctrl)
		txnOp.EXPECT().Commit(gomock.Any()).Return(nil).AnyTimes()
		txnOp.EXPECT().Rollback(gomock.Any()).Return(nil).AnyTimes()
		ws := &Ws{advanceSnapshot: func(context.Context, timestamp.Timestamp) error {
			t.Fatal("non-RC DropDatabase must not advance the workspace snapshot")
			return nil
		}}
		txnOp.EXPECT().GetWorkspace().Return(ws).AnyTimes()
		txnOp.EXPECT().Txn().Return(txnMeta).AnyTimes()
		txnOp.EXPECT().TxnOptions().Return(txn.TxnOptions{}).AnyTimes()
		txnOp.EXPECT().NextSequence().Return(uint64(0)).AnyTimes()
		txnOp.EXPECT().EnterRunSqlWithTokenAndSQL(gomock.Any(), gomock.Any()).Return(uint64(0)).AnyTimes()
		txnOp.EXPECT().ExitRunSqlWithToken(gomock.Any()).Return().AnyTimes()
		txnOp.EXPECT().Snapshot().Return(txn.CNTxnSnapshot{}, nil).AnyTimes()
		txnOp.EXPECT().Status().Return(txn.TxnStatus_Active).AnyTimes()
		proc.Base.TxnOperator = txnOp

		mockDb := mock_frontend.NewMockDatabase(ctrl)
		mockDb.EXPECT().IsSubscription(gomock.Any()).Return(false).AnyTimes()
		// Return non-numeric string to skip CCPR check (strconv.ParseUint will fail)
		mockDb.EXPECT().GetDatabaseId(gomock.Any()).Return("invalid").AnyTimes()
		mockDb.EXPECT().Relations(gomock.Any()).DoAndReturn(
			func(_ context.Context) ([]string, error) {
				assert.Equal(t, origSnapshotTS, txnMeta.SnapshotTS,
					"Non-RC DropDatabase must not advance SnapshotTS before Relations")
				return nil, moerr.NewInternalErrorNoCtx("stop here")
			}).AnyTimes()

		eng := mock_frontend.NewMockEngine(ctrl)
		eng.EXPECT().Database(gomock.Any(), "test_db", gomock.Any()).Return(mockDb, nil).AnyTimes()

		lockMoDb := gostub.Stub(&lockMoDatabase, func(_ *Compile, _ string, _ lock.LockMode) error {
			return nil
		})
		defer lockMoDb.Reset()

		c := NewCompile("test", "test", "drop database test_db", "", "", eng, proc, nil, false, nil, time.Now())
		err := s.DropDatabase(c)
		assert.Error(t, err)
		assert.Equal(t, origSnapshotTS, txnMeta.SnapshotTS,
			"SnapshotTS must remain unchanged for non-RC txn")
	})
}

func TestRemoveFkeysRelationshipsSkipsDeletedRelationsDuringDropDatabase(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	proc := testutil.NewProcess(t)
	proc.Base.SessionInfo.Buf = buffer.New()
	ctx := defines.AttachAccountId(context.Background(), sysAccountId)
	proc.Ctx = ctx
	proc.ReplaceTopCtx(ctx)

	txnMeta := txn.TxnMeta{}
	txnOp := mock_frontend.NewMockTxnOperator(ctrl)
	txnOp.EXPECT().Txn().Return(txnMeta).AnyTimes()
	txnOp.EXPECT().GetWorkspace().Return(&Ws{}).AnyTimes()
	proc.Base.TxnOperator = txnOp

	deletedRelErr := moerr.NewNoSuchTable(ctx, "acc_test02", "aff01")
	parentRel := mock_frontend.NewMockRelation(ctrl)
	parentRel.EXPECT().GetTableID(gomock.Any()).Return(uint64(11)).AnyTimes()

	mockDb := mock_frontend.NewMockDatabase(ctrl)
	mockDb.EXPECT().Relations(gomock.Any()).Return([]string{"aff01", "pri01"}, nil).Times(1)
	mockDb.EXPECT().Relation(gomock.Any(), "aff01", gomock.Any()).Return(nil, deletedRelErr).Times(1)
	mockDb.EXPECT().Relation(gomock.Any(), "pri01", gomock.Any()).Return(parentRel, nil).Times(1)

	eng := mock_frontend.NewMockEngine(ctrl)
	eng.EXPECT().Database(gomock.Any(), "acc_test02", gomock.Any()).Return(mockDb, nil).Times(1)

	getConstraintDef := gostub.Stub(&GetConstraintDef, func(_ context.Context, rel engine.Relation) (*engine.ConstraintDef, error) {
		if rel == parentRel {
			return &engine.ConstraintDef{Cts: []engine.Constraint{}}, nil
		}
		t.Fatalf("unexpected relation passed to GetConstraintDef")
		return nil, nil
	})
	defer getConstraintDef.Reset()

	c := NewCompile("test", "test", "drop database acc_test02", "", "", eng, proc, nil, false, nil, time.Now())
	s := &Scope{}
	require.NoError(t, s.removeFkeysRelationships(c, "acc_test02"))
}

func TestDropDatabaseSkipsDeletedRelationsWhenCollectingTables(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	proc := testutil.NewProcess(t)
	proc.Base.SessionInfo.Buf = buffer.New()
	ctx := defines.AttachAccountId(context.Background(), sysAccountId)
	proc.Ctx = ctx
	proc.ReplaceTopCtx(ctx)

	txnMeta := txn.TxnMeta{}
	txnOp := mock_frontend.NewMockTxnOperator(ctrl)
	txnOp.EXPECT().Txn().Return(txnMeta).AnyTimes()
	txnOp.EXPECT().SnapshotTS().Return(timestamp.Timestamp{}).AnyTimes()
	txnOp.EXPECT().SetSnapshotTS(gomock.Any()).AnyTimes()
	txnOp.EXPECT().TxnRef().Return(&txnMeta).AnyTimes()
	txnOp.EXPECT().GetWorkspace().Return(&Ws{}).AnyTimes()
	proc.Base.TxnOperator = txnOp

	deletedRelErr := moerr.NewNoSuchTable(ctx, "acc_test02", "aff01")
	deleteStopErr := moerr.NewInternalError(ctx, "stop after database delete")

	parentRel := mock_frontend.NewMockRelation(ctrl)
	parentRel.EXPECT().GetTableID(gomock.Any()).Return(uint64(11)).AnyTimes()

	mockDb := mock_frontend.NewMockDatabase(ctrl)
	mockDb.EXPECT().IsSubscription(gomock.Any()).Return(false).AnyTimes()
	mockDb.EXPECT().GetDatabaseId(gomock.Any()).Return("invalid").AnyTimes()
	mockDb.EXPECT().Relations(gomock.Any()).Return([]string{"aff01", "pri01"}, nil).Times(1)
	mockDb.EXPECT().Relation(gomock.Any(), "aff01", gomock.Any()).Return(nil, deletedRelErr).Times(1)
	mockDb.EXPECT().Relation(gomock.Any(), "pri01", gomock.Any()).Return(parentRel, nil).Times(1)
	mockDb.EXPECT().Relations(gomock.Any()).Return([]string{"aff01"}, nil).Times(1)
	mockDb.EXPECT().Relation(gomock.Any(), "aff01", gomock.Any()).Return(nil, deletedRelErr).Times(1)

	eng := mock_frontend.NewMockEngine(ctrl)
	eng.EXPECT().Database(gomock.Any(), "acc_test02", gomock.Any()).Return(mockDb, nil).Times(3)
	eng.EXPECT().Delete(gomock.Any(), "acc_test02", gomock.Any()).Return(deleteStopErr).Times(1)

	getConstraintDef := gostub.Stub(&GetConstraintDef, func(_ context.Context, rel engine.Relation) (*engine.ConstraintDef, error) {
		if rel == parentRel {
			return &engine.ConstraintDef{Cts: []engine.Constraint{}}, nil
		}
		t.Fatalf("unexpected relation passed to GetConstraintDef")
		return nil, nil
	})
	defer getConstraintDef.Reset()

	lockMoDb := gostub.Stub(&lockMoDatabase, func(_ *Compile, _ string, _ lock.LockMode) error {
		return nil
	})
	defer lockMoDb.Reset()

	dropDbDef := &plan2.DropDatabase{
		IfExists: false,
		Database: "acc_test02",
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
		Magic: DropDatabase,
		Plan:  cplan,
	}

	c := NewCompile("test", "test", "drop database acc_test02", "", "", eng, proc, nil, false, nil, time.Now())
	require.ErrorIs(t, s.DropDatabase(c), deleteStopErr)
}

func TestDropDatabaseSkipsForeignKeyCleanupWhenIgnored(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	proc := testutil.NewProcess(t)
	proc.Base.SessionInfo.Buf = buffer.New()
	ctx := defines.AttachAccountId(context.Background(), sysAccountId)
	ctx = context.WithValue(ctx, defines.IgnoreForeignKey{}, true)
	proc.Ctx = ctx
	proc.ReplaceTopCtx(ctx)

	txnMeta := txn.TxnMeta{}
	txnOp := mock_frontend.NewMockTxnOperator(ctrl)
	txnOp.EXPECT().Txn().Return(txnMeta).AnyTimes()
	txnOp.EXPECT().SnapshotTS().Return(timestamp.Timestamp{}).AnyTimes()
	txnOp.EXPECT().SetSnapshotTS(gomock.Any()).AnyTimes()
	txnOp.EXPECT().GetWorkspace().Return(&Ws{}).AnyTimes()
	proc.Base.TxnOperator = txnOp

	stopErr := moerr.NewInternalError(ctx, "stop after FK cleanup check")
	mockDb := mock_frontend.NewMockDatabase(ctrl)
	mockDb.EXPECT().IsSubscription(gomock.Any()).Return(false).AnyTimes()
	mockDb.EXPECT().GetDatabaseId(gomock.Any()).Return("invalid").AnyTimes()
	mockDb.EXPECT().Relations(gomock.Any()).Return(nil, stopErr).Times(1)

	eng := mock_frontend.NewMockEngine(ctrl)
	// The first lookup opens the database; the second collects its tables.
	// A third lookup would mean removeFkeysRelationships was not skipped.
	eng.EXPECT().Database(gomock.Any(), "db1", gomock.Any()).Return(mockDb, nil).Times(2)

	lockMoDb := gostub.Stub(&lockMoDatabase, func(_ *Compile, _ string, _ lock.LockMode) error {
		return nil
	})
	defer lockMoDb.Reset()

	dropDbDef := &plan2.DropDatabase{Database: "db1"}
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
	s := &Scope{Magic: DropDatabase, Plan: cplan}
	c := NewCompile("test", "test", "drop database db1", "", "", eng, proc, nil, false, nil, time.Now())

	require.ErrorIs(t, s.DropDatabase(c), stopErr)
}

func TestDropDatabaseReturnsInternalRelationErrorWhenCollectingTables(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	proc := testutil.NewProcess(t)
	proc.Base.SessionInfo.Buf = buffer.New()
	ctx := defines.AttachAccountId(context.Background(), sysAccountId)
	proc.Ctx = ctx
	proc.ReplaceTopCtx(ctx)

	txnMeta := txn.TxnMeta{}
	txnOp := mock_frontend.NewMockTxnOperator(ctrl)
	txnOp.EXPECT().Txn().Return(txnMeta).AnyTimes()
	txnOp.EXPECT().SnapshotTS().Return(timestamp.Timestamp{}).AnyTimes()
	txnOp.EXPECT().SetSnapshotTS(gomock.Any()).AnyTimes()
	txnOp.EXPECT().TxnRef().Return(&txnMeta).AnyTimes()
	txnOp.EXPECT().GetWorkspace().Return(&Ws{}).AnyTimes()
	proc.Base.TxnOperator = txnOp

	relationErr := moerr.NewInternalErrorf(ctx, "can not find table by id %d", 99)

	parentRel := mock_frontend.NewMockRelation(ctrl)
	parentRel.EXPECT().GetTableID(gomock.Any()).Return(uint64(11)).AnyTimes()

	mockDb := mock_frontend.NewMockDatabase(ctrl)
	mockDb.EXPECT().IsSubscription(gomock.Any()).Return(false).AnyTimes()
	mockDb.EXPECT().GetDatabaseId(gomock.Any()).Return("invalid").AnyTimes()
	mockDb.EXPECT().Relations(gomock.Any()).Return([]string{"pri01"}, nil).Times(1)
	mockDb.EXPECT().Relation(gomock.Any(), "pri01", gomock.Any()).Return(parentRel, nil).Times(1)
	mockDb.EXPECT().Relations(gomock.Any()).Return([]string{"aff01"}, nil).Times(1)
	mockDb.EXPECT().Relation(gomock.Any(), "aff01", gomock.Any()).Return(nil, relationErr).Times(1)

	eng := mock_frontend.NewMockEngine(ctrl)
	eng.EXPECT().Database(gomock.Any(), "acc_test02", gomock.Any()).Return(mockDb, nil).Times(3)

	getConstraintDef := gostub.Stub(&GetConstraintDef, func(_ context.Context, rel engine.Relation) (*engine.ConstraintDef, error) {
		if rel == parentRel {
			return &engine.ConstraintDef{Cts: []engine.Constraint{}}, nil
		}
		t.Fatalf("unexpected relation passed to GetConstraintDef")
		return nil, nil
	})
	defer getConstraintDef.Reset()

	lockMoDb := gostub.Stub(&lockMoDatabase, func(_ *Compile, _ string, _ lock.LockMode) error {
		return nil
	})
	defer lockMoDb.Reset()

	dropDbDef := &plan2.DropDatabase{
		IfExists: false,
		Database: "acc_test02",
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
		Magic: DropDatabase,
		Plan:  cplan,
	}

	c := NewCompile("test", "test", "drop database acc_test02", "", "", eng, proc, nil, false, nil, time.Now())
	require.ErrorIs(t, s.DropDatabase(c), relationErr)
}

func TestRemoveFkeysRelationshipsSkipsDeletedChildTableIds(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	proc := testutil.NewProcess(t)
	proc.Base.SessionInfo.Buf = buffer.New()
	ctx := defines.AttachAccountId(context.Background(), sysAccountId)
	proc.Ctx = ctx
	proc.ReplaceTopCtx(ctx)

	txnMeta := txn.TxnMeta{}
	txnOp := mock_frontend.NewMockTxnOperator(ctrl)
	txnOp.EXPECT().Txn().Return(txnMeta).AnyTimes()
	txnOp.EXPECT().GetWorkspace().Return(&Ws{}).AnyTimes()
	proc.Base.TxnOperator = txnOp

	parentRel := mock_frontend.NewMockRelation(ctrl)
	parentRel.EXPECT().GetTableID(gomock.Any()).Return(uint64(11)).AnyTimes()

	mockDb := mock_frontend.NewMockDatabase(ctrl)
	mockDb.EXPECT().Relations(gomock.Any()).Return([]string{"pri01"}, nil).Times(1)
	mockDb.EXPECT().Relation(gomock.Any(), "pri01", gomock.Any()).Return(parentRel, nil).Times(1)

	eng := mock_frontend.NewMockEngine(ctrl)
	eng.EXPECT().Database(gomock.Any(), "acc_test02", gomock.Any()).Return(mockDb, nil).Times(1)
	eng.EXPECT().GetRelationById(gomock.Any(), gomock.Any(), uint64(22)).
		Return("", "", nil, moerr.NewInternalErrorf(ctx, "can not find table by id %d: accountId: %d. Deleted in txn", 22, sysAccountId)).
		Times(1)

	getConstraintDef := gostub.Stub(&GetConstraintDef, func(_ context.Context, rel engine.Relation) (*engine.ConstraintDef, error) {
		if rel == parentRel {
			return &engine.ConstraintDef{
				Cts: []engine.Constraint{
					&engine.RefChildTableDef{Tables: []uint64{22}},
				},
			}, nil
		}
		t.Fatalf("unexpected relation passed to GetConstraintDef")
		return nil, nil
	})
	defer getConstraintDef.Reset()

	c := NewCompile("test", "test", "drop database acc_test02", "", "", eng, proc, nil, false, nil, time.Now())
	s := &Scope{}
	require.NoError(t, s.removeFkeysRelationships(c, "acc_test02"))
}

func TestRemoveFkeysRelationshipsSkipsDeletedParentTableIds(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	proc := testutil.NewProcess(t)
	proc.Base.SessionInfo.Buf = buffer.New()
	ctx := defines.AttachAccountId(context.Background(), sysAccountId)
	proc.Ctx = ctx
	proc.ReplaceTopCtx(ctx)

	txnMeta := txn.TxnMeta{}
	txnOp := mock_frontend.NewMockTxnOperator(ctrl)
	txnOp.EXPECT().Txn().Return(txnMeta).AnyTimes()
	txnOp.EXPECT().GetWorkspace().Return(&Ws{}).AnyTimes()
	proc.Base.TxnOperator = txnOp

	childRel := mock_frontend.NewMockRelation(ctrl)
	childRel.EXPECT().GetTableID(gomock.Any()).Return(uint64(11)).AnyTimes()

	mockDb := mock_frontend.NewMockDatabase(ctrl)
	mockDb.EXPECT().Relations(gomock.Any()).Return([]string{"aff01"}, nil).Times(1)
	mockDb.EXPECT().Relation(gomock.Any(), "aff01", gomock.Any()).Return(childRel, nil).Times(1)

	eng := mock_frontend.NewMockEngine(ctrl)
	eng.EXPECT().Database(gomock.Any(), "acc_test02", gomock.Any()).Return(mockDb, nil).Times(1)
	eng.EXPECT().GetRelationById(gomock.Any(), gomock.Any(), uint64(33)).
		Return("", "", nil, moerr.NewNoSuchTable(ctx, "acc_test02", "pri01")).
		Times(1)

	getConstraintDef := gostub.Stub(&GetConstraintDef, func(_ context.Context, rel engine.Relation) (*engine.ConstraintDef, error) {
		if rel == childRel {
			return &engine.ConstraintDef{
				Cts: []engine.Constraint{
					&engine.ForeignKeyDef{
						Fkeys: []*plan.ForeignKeyDef{
							{ForeignTbl: uint64(33)},
						},
					},
				},
			}, nil
		}
		t.Fatalf("unexpected relation passed to GetConstraintDef")
		return nil, nil
	})
	defer getConstraintDef.Reset()

	c := NewCompile("test", "test", "drop database acc_test02", "", "", eng, proc, nil, false, nil, time.Now())
	s := &Scope{}
	require.NoError(t, s.removeFkeysRelationships(c, "acc_test02"))
}

func TestMissingTablePredicates(t *testing.T) {
	ctx := context.Background()

	noSuchTableErr := moerr.NewNoSuchTable(ctx, "db1", "t1")
	canNotFindTableByIDErr := moerr.NewInternalError(ctx, "can not find table by id : accountId: 0")
	otherInternalErr := moerr.NewInternalError(ctx, "some other internal error")
	otherErr := moerr.NewBadDB(ctx, "db1")

	require.True(t, isMissingTableByNameForDropDatabase(noSuchTableErr))
	require.False(t, isMissingTableByNameForDropDatabase(canNotFindTableByIDErr))
	require.False(t, isMissingTableByNameForDropDatabase(otherInternalErr))
	require.False(t, isMissingTableByNameForDropDatabase(otherErr))

	require.True(t, isMissingTableByIdForFkCleanup(noSuchTableErr))
	require.True(t, isMissingTableByIdForFkCleanup(canNotFindTableByIDErr))
	require.False(t, isMissingTableByIdForFkCleanup(otherInternalErr))
	require.False(t, isMissingTableByIdForFkCleanup(otherErr))
}

func TestDropTableSingleSkipsMissingFkTables(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	proc := testutil.NewProcess(t)
	proc.Base.SessionInfo.Buf = buffer.New()
	ctx := defines.AttachAccountId(context.Background(), sysAccountId)
	proc.Ctx = ctx
	proc.ReplaceTopCtx(ctx)

	txnMeta := txn.TxnMeta{}
	txnOp := mock_frontend.NewMockTxnOperator(ctrl)
	txnOp.EXPECT().Txn().Return(txnMeta).AnyTimes()
	txnOp.EXPECT().GetWorkspace().Return(&Ws{}).AnyTimes()
	proc.Base.TxnOperator = txnOp

	mockRel := mock_frontend.NewMockRelation(ctrl)
	mockRel.EXPECT().GetTableID(gomock.Any()).Return(uint64(1)).AnyTimes()
	mockRel.EXPECT().GetTableDef(gomock.Any()).Return(&plan2.TableDef{}).AnyTimes()

	mockDb := mock_frontend.NewMockDatabase(ctrl)
	mockDb.EXPECT().Relation(gomock.Any(), "test_tbl", gomock.Any()).Return(mockRel, nil).AnyTimes()
	mockDb.EXPECT().Delete(gomock.Any(), "test_tbl").Return(nil).AnyTimes()

	eng := mock_frontend.NewMockEngine(ctrl)
	eng.EXPECT().Database(gomock.Any(), catalog.MO_CATALOG, gomock.Any()).Return(mockDb, nil).AnyTimes()
	// FK parent table not found in ForeignTbl loop.
	eng.EXPECT().GetRelationById(gomock.Any(), gomock.Any(), uint64(42)).
		Return("", "", nil, moerr.NewNoSuchTable(ctx, "db", "parent_tbl")).
		Times(1)
	// FK child table not found in FkChildTblsReferToMe loop.
	eng.EXPECT().GetRelationById(gomock.Any(), gomock.Any(), uint64(43)).
		Return("", "", nil, moerr.NewInternalErrorf(ctx, "can not find table by id %d", 43)).
		Times(1)

	c := NewCompile("test", "test", "drop table test_tbl", "", "", eng, proc, nil, false, nil, time.Now())
	c.disableLock = true
	s := &Scope{}
	err := s.dropTableSingle(c, &plan2.DropTable{
		Database:             catalog.MO_CATALOG,
		Table:                "test_tbl",
		TableId:              1,
		IsView:               true,
		TableDef:             &plan2.TableDef{},
		ForeignTbl:           []uint64{42},
		FkChildTblsReferToMe: []uint64{43},
	})
	require.NoError(t, err)
}

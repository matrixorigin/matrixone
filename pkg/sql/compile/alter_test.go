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

	"github.com/golang/mock/gomock"
	"github.com/prashantv/gostub"
	"github.com/smartystreets/goconvey/convey"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/buffer"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	mock_lock "github.com/matrixorigin/matrixone/pkg/frontend/test/mock_lock"
	"github.com/matrixorigin/matrixone/pkg/lockservice"
	"github.com/matrixorigin/matrixone/pkg/objectio/ioutil"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/pb/lock"
	plan2 "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func TestShouldEnableAlterCopyPipelineFlush(t *testing.T) {
	assert.False(t, shouldEnableAlterCopyPipelineFlush(nil))
	assert.False(t, shouldEnableAlterCopyPipelineFlush(&plan2.AlterCopyOpt{SkipPkDedup: false}))
	assert.True(t, shouldEnableAlterCopyPipelineFlush(&plan2.AlterCopyOpt{SkipPkDedup: true}))
}

type alterCopyInsertSpyExecutor struct {
	insertSQL    string
	insertErr    error
	insertCtx    context.Context
	insertOption executor.StatementOption
	results      map[string]executor.Result
	errs         map[string]error
	executedSQLs []string
}

const (
	alterCopyTestPkNullCheckSQL      = "SELECT `col4` FROM `test`.`dept` WHERE `col4` IS NULL LIMIT 1"
	alterCopyTestPkDuplicateCheckSQL = "SELECT `col4` FROM `test`.`dept` GROUP BY `col4` HAVING count(*) > 1 LIMIT 1"
)

func (e *alterCopyInsertSpyExecutor) Exec(
	ctx context.Context,
	sql string,
	opts executor.Options,
) (executor.Result, error) {
	e.executedSQLs = append(e.executedSQLs, sql)
	if sql == e.insertSQL {
		e.insertCtx = ctx
		e.insertOption = opts.StatementOption()
		return executor.Result{}, e.insertErr
	}
	if e.errs != nil {
		if err, ok := e.errs[sql]; ok {
			return executor.Result{}, err
		}
	}
	if e.results != nil {
		if res, ok := e.results[sql]; ok {
			return res, nil
		}
	}
	return executor.Result{}, nil
}

func (e *alterCopyInsertSpyExecutor) ExecTxn(
	ctx context.Context,
	execFunc func(executor.TxnExecutor) error,
	opts executor.Options,
) error {
	return nil
}

func TestScopeAlterTableCopyInsertTmpDataPipelineFlush(t *testing.T) {
	insertErr := errors.New("stop after insert-copy")

	for _, tc := range []struct {
		name               string
		skipPkDedup        bool
		nilCtxBeforeInsert bool
		wantPipelineFlush  bool
	}{
		{
			name:               "skip pk dedup false",
			skipPkDedup:        false,
			nilCtxBeforeInsert: false,
			wantPipelineFlush:  false,
		},
		{
			name:               "skip pk dedup true",
			skipPkDedup:        true,
			nilCtxBeforeInsert: false,
			wantPipelineFlush:  true,
		},
		{
			name:               "skip pk dedup true with nil proc ctx",
			skipPkDedup:        true,
			nilCtxBeforeInsert: true,
			wantPipelineFlush:  true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			proc := testutil.NewProcess(t)
			proc.Base.SessionInfo.Buf = buffer.New()
			proc.Base.SessionInfo.TimeZone = time.Local

			serviceID := "alter-copy-pipeline-flush-" + tc.name
			lockSvc := mock_lock.NewMockLockService(ctrl)
			lockSvc.EXPECT().GetConfig().Return(lockservice.Config{ServiceID: serviceID}).AnyTimes()
			proc.Base.LockService = lockSvc
			require.Equal(t, serviceID, proc.GetService())

			const accountID = catalog.System_Account
			ctx := defines.AttachAccountId(context.Background(), accountID)
			proc.Ctx = ctx
			proc.ReplaceTopCtx(ctx)

			txnCli, txnOp := newTestTxnClientAndOp(ctrl)
			proc.Base.TxnClient = txnCli
			proc.Base.TxnOperator = txnOp

			tableDef := &plan.TableDef{
				TblId: 1,
				Name:  "dept",
			}
			copyTableDef := &plan.TableDef{
				TblId: 2,
				Name:  "dept_copy",
			}
			alterTable := &plan2.AlterTable{
				Database:          "test",
				TableDef:          tableDef,
				CopyTableDef:      copyTableDef,
				CreateTmpTableSql: "create table dept_copy",
				InsertTmpDataSql:  "insert into dept_copy select * from dept",
				Options:           &plan2.AlterCopyOpt{SkipPkDedup: tc.skipPkDedup},
			}
			s := &Scope{
				Magic: AlterTable,
				Plan: &plan.Plan{
					Plan: &plan2.Plan_Ddl{
						Ddl: &plan2.DataDefinition{
							DdlType: plan2.DataDefinition_ALTER_TABLE,
							Definition: &plan2.DataDefinition_AlterTable{
								AlterTable: alterTable,
							},
						},
					},
				},
			}

			originRel := mock_frontend.NewMockRelation(ctrl)
			originRel.EXPECT().GetTableID(gomock.Any()).Return(uint64(1)).AnyTimes()

			copyRel := mock_frontend.NewMockRelation(ctrl)
			if tc.nilCtxBeforeInsert {
				copyRel.EXPECT().CopyTableDef(gomock.Any()).DoAndReturn(func(context.Context) *plan.TableDef {
					proc.Ctx = nil
					return &plan.TableDef{
						TblId: 2,
						Name:  "dept_copy",
					}
				})
			} else {
				copyRel.EXPECT().CopyTableDef(gomock.Any()).Return(&plan.TableDef{
					TblId: 2,
					Name:  "dept_copy",
				}).AnyTimes()
			}

			mockDb := mock_frontend.NewMockDatabase(ctrl)
			mockDb.EXPECT().Relation(gomock.Any(), "dept", gomock.Any()).Return(originRel, nil).AnyTimes()
			mockDb.EXPECT().Relation(gomock.Any(), "dept_copy", gomock.Any()).Return(copyRel, nil).AnyTimes()

			eng := mock_frontend.NewMockEngine(ctrl)
			eng.EXPECT().Database(gomock.Any(), "test", gomock.Any()).Return(mockDb, nil).AnyTimes()

			spyExec := &alterCopyInsertSpyExecutor{
				insertSQL: alterTable.InsertTmpDataSql,
				insertErr: insertErr,
			}
			rt := moruntime.DefaultRuntime()
			rt.SetGlobalVariables(moruntime.InternalSQLExecutor, spyExec)
			moruntime.SetupServiceBasedRuntime(proc.GetService(), rt)

			c := NewCompile("test", "test", "alter table dept", "", "", eng, proc, nil, false, nil, time.Now())
			c.pn = s.Plan
			origCtx := proc.Ctx

			err := s.AlterTableCopy(c)
			require.ErrorIs(t, err, insertErr)
			require.NotNil(t, spyExec.insertCtx)
			assert.Equal(t, tc.wantPipelineFlush, spyExec.insertCtx.Value(ioutil.PipelineFlushKey) == true)

			insertAccountID, err := defines.GetAccountId(spyExec.insertCtx)
			require.NoError(t, err)
			assert.Equal(t, accountID, insertAccountID)

			if tc.nilCtxBeforeInsert {
				require.NotNil(t, proc.Ctx)
				require.NotSame(t, spyExec.insertCtx, proc.Ctx)
				require.Same(t, proc.GetTopContext(), proc.Ctx)

				restoredAccountID, err := defines.GetAccountId(proc.Ctx)
				require.NoError(t, err)
				assert.Equal(t, accountID, restoredAccountID)
			} else {
				require.Same(t, origCtx, proc.Ctx)
			}
			assert.NotEqual(t, true, proc.Ctx.Value(ioutil.PipelineFlushKey))

			if tc.skipPkDedup {
				require.Same(t, alterTable.Options, spyExec.insertOption.AlterCopyDedupOpt())
			} else {
				require.Nil(t, spyExec.insertOption.AlterCopyDedupOpt())
			}
		})
	}
}

func TestGetAlterCopyPkPrecheck(t *testing.T) {
	for _, tc := range []struct {
		name             string
		tableDef         *plan.TableDef
		copyTableDef     *plan.TableDef
		skipPkDedup      bool
		wantCols         []string
		wantCheckNotNull bool
	}{
		{
			name: "add pk on nullable original column",
			tableDef: &plan.TableDef{
				Cols: []*plan.ColDef{{Name: "col4", Typ: plan.Type{Id: int32(types.T_int32)}}},
				Pkey: &plan.PrimaryKeyDef{PkeyColName: catalog.FakePrimaryKeyColName},
			},
			copyTableDef: &plan.TableDef{
				Cols: []*plan.ColDef{{Name: "col4", NotNull: true, Primary: true, Typ: plan.Type{Id: int32(types.T_int32)}}},
				Pkey: &plan.PrimaryKeyDef{PkeyColName: "col4", Names: []string{"col4"}},
			},
			wantCols:         []string{"col4"},
			wantCheckNotNull: true,
		},
		{
			name: "add pk on not null original column",
			tableDef: &plan.TableDef{
				Cols: []*plan.ColDef{{Name: "col4", NotNull: true, Typ: plan.Type{Id: int32(types.T_int32)}}},
				Pkey: &plan.PrimaryKeyDef{PkeyColName: catalog.FakePrimaryKeyColName},
			},
			copyTableDef: &plan.TableDef{
				Cols: []*plan.ColDef{{Name: "col4", NotNull: true, Typ: plan.Type{Id: int32(types.T_int32)}}},
				Pkey: &plan.PrimaryKeyDef{PkeyColName: "col4", Names: []string{"col4"}},
			},
			wantCols: []string{"col4"},
		},
		{
			name: "static skip pk dedup needs no precheck",
			tableDef: &plan.TableDef{
				Cols: []*plan.ColDef{{Name: "col4", Typ: plan.Type{Id: int32(types.T_int32)}}},
				Pkey: &plan.PrimaryKeyDef{PkeyColName: "col4", Names: []string{"col4"}},
			},
			copyTableDef: &plan.TableDef{
				Cols: []*plan.ColDef{{Name: "col4", Typ: plan.Type{Id: int32(types.T_int32)}}},
				Pkey: &plan.PrimaryKeyDef{PkeyColName: "col4", Names: []string{"col4"}},
			},
			skipPkDedup: true,
		},
		{
			name: "pk column is not copied from original table",
			tableDef: &plan.TableDef{
				Cols: []*plan.ColDef{{Name: "col4", Typ: plan.Type{Id: int32(types.T_int32)}}},
				Pkey: &plan.PrimaryKeyDef{PkeyColName: catalog.FakePrimaryKeyColName},
			},
			copyTableDef: &plan.TableDef{
				Cols: []*plan.ColDef{{Name: "new_col", Typ: plan.Type{Id: int32(types.T_int32)}}},
				Pkey: &plan.PrimaryKeyDef{PkeyColName: "new_col", Names: []string{"new_col"}},
			},
		},
		{
			name: "pk column type change can change dedup key value",
			tableDef: &plan.TableDef{
				Cols: []*plan.ColDef{{Name: "col4", Typ: plan.Type{Id: int32(types.T_varchar), Width: 16}}},
				Pkey: &plan.PrimaryKeyDef{PkeyColName: catalog.FakePrimaryKeyColName},
			},
			copyTableDef: &plan.TableDef{
				Cols: []*plan.ColDef{{Name: "col4", NotNull: true, Primary: true, Typ: plan.Type{Id: int32(types.T_int32)}}},
				Pkey: &plan.PrimaryKeyDef{PkeyColName: "col4", Names: []string{"col4"}},
			},
		},
		{
			name: "pk column width change can change dedup key value",
			tableDef: &plan.TableDef{
				Cols: []*plan.ColDef{{Name: "col4", Typ: plan.Type{Id: int32(types.T_varchar), Width: 32}}},
				Pkey: &plan.PrimaryKeyDef{PkeyColName: catalog.FakePrimaryKeyColName},
			},
			copyTableDef: &plan.TableDef{
				Cols: []*plan.ColDef{{Name: "col4", NotNull: true, Primary: true, Typ: plan.Type{Id: int32(types.T_varchar), Width: 8}}},
				Pkey: &plan.PrimaryKeyDef{PkeyColName: "col4", Names: []string{"col4"}},
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			qry := &plan2.AlterTable{
				TableDef:     tc.tableDef,
				CopyTableDef: tc.copyTableDef,
				Options: &plan2.AlterCopyOpt{
					SkipPkDedup:     tc.skipPkDedup,
					TargetTableName: "dept_copy",
				},
			}
			pkCols, checkNotNull := getAlterCopyPkPrecheck(qry)
			assert.Equal(t, tc.wantCols, pkCols)
			assert.Equal(t, tc.wantCheckNotNull, checkNotNull)
		})
	}
}

func TestScopeAlterTableCopyPrecheckPrimaryKeyThenSkipDedup(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	proc := testutil.NewProcess(t)
	proc.Base.SessionInfo.Buf = buffer.New()
	proc.Base.SessionInfo.TimeZone = time.Local

	serviceID := "alter-copy-pk-precheck"
	lockSvc := mock_lock.NewMockLockService(ctrl)
	lockSvc.EXPECT().GetConfig().Return(lockservice.Config{ServiceID: serviceID}).AnyTimes()
	proc.Base.LockService = lockSvc
	require.Equal(t, serviceID, proc.GetService())

	const accountID = catalog.System_Account
	ctx := defines.AttachAccountId(context.Background(), accountID)
	proc.Ctx = ctx
	proc.ReplaceTopCtx(ctx)

	txnCli, txnOp := newTestTxnClientAndOp(ctrl)
	proc.Base.TxnClient = txnCli
	proc.Base.TxnOperator = txnOp

	tableDef := &plan.TableDef{
		TblId: 1,
		Name:  "dept",
		Cols: []*plan.ColDef{
			{Name: "col4", Typ: plan.Type{Id: int32(types.T_int32)}},
		},
		Pkey: &plan.PrimaryKeyDef{PkeyColName: catalog.FakePrimaryKeyColName},
	}
	copyTableDef := &plan.TableDef{
		TblId: 2,
		Name:  "dept_copy",
		Cols: []*plan.ColDef{
			{Name: "col4", Typ: plan.Type{Id: int32(types.T_int32)}},
		},
		Pkey: &plan.PrimaryKeyDef{PkeyColName: "col4", Names: []string{"col4"}},
	}
	alterTable := &plan2.AlterTable{
		Database:          "test",
		TableDef:          tableDef,
		CopyTableDef:      copyTableDef,
		CreateTmpTableSql: "create table dept_copy",
		InsertTmpDataSql:  "insert into dept_copy select * from dept",
		Options: &plan2.AlterCopyOpt{
			SkipPkDedup:     false,
			TargetTableName: "dept_copy",
		},
	}
	s := &Scope{
		Magic: AlterTable,
		Plan: &plan.Plan{
			Plan: &plan2.Plan_Ddl{
				Ddl: &plan2.DataDefinition{
					DdlType: plan2.DataDefinition_ALTER_TABLE,
					Definition: &plan2.DataDefinition_AlterTable{
						AlterTable: alterTable,
					},
				},
			},
		},
	}

	originRel := mock_frontend.NewMockRelation(ctrl)
	originRel.EXPECT().GetTableID(gomock.Any()).Return(uint64(1)).AnyTimes()

	copyRel := mock_frontend.NewMockRelation(ctrl)
	copyRel.EXPECT().CopyTableDef(gomock.Any()).Return(copyTableDef).AnyTimes()

	mockDb := mock_frontend.NewMockDatabase(ctrl)
	mockDb.EXPECT().Relation(gomock.Any(), "dept", gomock.Any()).Return(originRel, nil).AnyTimes()
	mockDb.EXPECT().Relation(gomock.Any(), "dept_copy", gomock.Any()).Return(copyRel, nil).AnyTimes()

	eng := mock_frontend.NewMockEngine(ctrl)
	eng.EXPECT().Database(gomock.Any(), "test", gomock.Any()).Return(mockDb, nil).AnyTimes()

	insertErr := errors.New("stop after insert-copy")
	spyExec := &alterCopyInsertSpyExecutor{
		insertSQL: alterTable.InsertTmpDataSql,
		insertErr: insertErr,
	}
	rt := moruntime.DefaultRuntime()
	rt.SetGlobalVariables(moruntime.InternalSQLExecutor, spyExec)
	moruntime.SetupServiceBasedRuntime(proc.GetService(), rt)

	c := NewCompile("test", "test", "alter table dept", "", "", eng, proc, nil, false, nil, time.Now())
	c.pn = s.Plan

	err := s.AlterTableCopy(c)
	require.ErrorIs(t, err, insertErr)
	assert.False(t, alterTable.Options.SkipPkDedup)
	require.NotNil(t, spyExec.insertCtx)
	assert.Equal(t, true, spyExec.insertCtx.Value(ioutil.PipelineFlushKey) == true)
	require.NotSame(t, alterTable.Options, spyExec.insertOption.AlterCopyDedupOpt())
	require.True(t, spyExec.insertOption.AlterCopyDedupOpt().SkipPkDedup)
	require.Equal(t, alterTable.Options.TargetTableName, spyExec.insertOption.AlterCopyDedupOpt().TargetTableName)
	assert.Equal(t, []string{
		alterTable.CreateTmpTableSql,
		alterCopyTestPkNullCheckSQL,
		alterCopyTestPkDuplicateCheckSQL,
		alterTable.InsertTmpDataSql,
	}, spyExec.executedSQLs)
}

func TestPrecheckAlterCopyPkDedupRejectsNull(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	alterTable := testAlterCopyAddPrimaryKeyPlan()
	spyExec := &alterCopyInsertSpyExecutor{}
	c := newAlterCopyPrecheckCompile(t, ctrl, spyExec)
	spyExec.results = map[string]executor.Result{
		alterCopyTestPkNullCheckSQL: newAlterCopyConstNullResult(c.proc.Mp(), types.T_int32.ToType()),
	}

	opt, err := c.precheckAlterCopyPkDedup("test", "dept", alterTable)
	require.Error(t, err)
	require.Nil(t, opt)
	assert.True(t, moerr.IsMoErrCode(err, moerr.ErrConstraintViolation))
	assert.False(t, alterTable.Options.SkipPkDedup)
	assert.Equal(t, []string{alterCopyTestPkNullCheckSQL}, spyExec.executedSQLs)
}

func TestPrecheckAlterCopyPkDedupRejectsDuplicate(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	alterTable := testAlterCopyAddPrimaryKeyPlan()
	spyExec := &alterCopyInsertSpyExecutor{}
	c := newAlterCopyPrecheckCompile(t, ctrl, spyExec)
	spyExec.results = map[string]executor.Result{
		alterCopyTestPkDuplicateCheckSQL: newAlterCopyFixedResult(t, c.proc.Mp(), types.T_int32.ToType(), []int32{7}),
	}

	opt, err := c.precheckAlterCopyPkDedup("test", "dept", alterTable)
	require.Error(t, err)
	require.Nil(t, opt)
	assert.True(t, moerr.IsMoErrCode(err, moerr.ErrDuplicateEntry))
	assert.False(t, alterTable.Options.SkipPkDedup)
	assert.Equal(t, []string{alterCopyTestPkNullCheckSQL, alterCopyTestPkDuplicateCheckSQL}, spyExec.executedSQLs)
}

func TestPrecheckAlterCopyPkDedupCanSkipNullCheck(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	alterTable := testAlterCopyAddPrimaryKeyPlan()
	alterTable.TableDef.Cols[0].NotNull = true
	spyExec := &alterCopyInsertSpyExecutor{}
	c := newAlterCopyPrecheckCompile(t, ctrl, spyExec)

	opt, err := c.precheckAlterCopyPkDedup("test", "dept", alterTable)
	require.NoError(t, err)
	require.NotNil(t, opt)
	assert.True(t, opt.SkipPkDedup)
	assert.False(t, alterTable.Options.SkipPkDedup)
	require.NotSame(t, alterTable.Options, opt)
	assert.Equal(t, []string{alterCopyTestPkDuplicateCheckSQL}, spyExec.executedSQLs)
}

func TestPrecheckAlterCopyPkDedupDoesNotMutatePlanOption(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	alterTable := testAlterCopyAddPrimaryKeyPlan()
	spyExec := &alterCopyInsertSpyExecutor{}
	c := newAlterCopyPrecheckCompile(t, ctrl, spyExec)

	firstOpt, err := c.precheckAlterCopyPkDedup("test", "dept", alterTable)
	require.NoError(t, err)
	require.NotNil(t, firstOpt)
	require.True(t, firstOpt.SkipPkDedup)
	require.False(t, alterTable.Options.SkipPkDedup)

	secondOpt, err := c.precheckAlterCopyPkDedup("test", "dept", alterTable)
	require.NoError(t, err)
	require.NotNil(t, secondOpt)
	require.True(t, secondOpt.SkipPkDedup)
	require.False(t, alterTable.Options.SkipPkDedup)
	require.NotSame(t, firstOpt, secondOpt)

	assert.Equal(t, []string{
		alterCopyTestPkNullCheckSQL,
		alterCopyTestPkDuplicateCheckSQL,
		alterCopyTestPkNullCheckSQL,
		alterCopyTestPkDuplicateCheckSQL,
	}, spyExec.executedSQLs)
}

func testAlterCopyAddPrimaryKeyPlan() *plan2.AlterTable {
	return &plan2.AlterTable{
		Database: "test",
		TableDef: &plan.TableDef{
			Name: "dept",
			Cols: []*plan.ColDef{
				{Name: "col4", Typ: plan.Type{Id: int32(types.T_int32)}},
			},
			Pkey: &plan.PrimaryKeyDef{PkeyColName: catalog.FakePrimaryKeyColName},
		},
		CopyTableDef: &plan.TableDef{
			Name: "dept_copy",
			Cols: []*plan.ColDef{
				{Name: "col4", Typ: plan.Type{Id: int32(types.T_int32)}},
			},
			Pkey: &plan.PrimaryKeyDef{PkeyColName: "col4", Names: []string{"col4"}},
		},
		Options: &plan2.AlterCopyOpt{
			SkipPkDedup:     false,
			TargetTableName: "dept_copy",
		},
	}
}

func newAlterCopyPrecheckCompile(t *testing.T, ctrl *gomock.Controller, exec executor.SQLExecutor) *Compile {
	proc := testutil.NewProcess(t)
	proc.Base.SessionInfo.Buf = buffer.New()
	proc.Base.SessionInfo.TimeZone = time.Local

	serviceID := "alter-copy-precheck-" + t.Name()
	lockSvc := mock_lock.NewMockLockService(ctrl)
	lockSvc.EXPECT().GetConfig().Return(lockservice.Config{ServiceID: serviceID}).AnyTimes()
	proc.Base.LockService = lockSvc

	ctx := defines.AttachAccountId(context.Background(), catalog.System_Account)
	proc.Ctx = ctx
	proc.ReplaceTopCtx(ctx)

	txnCli, txnOp := newTestTxnClientAndOp(ctrl)
	proc.Base.TxnClient = txnCli
	proc.Base.TxnOperator = txnOp

	rt := moruntime.DefaultRuntime()
	rt.SetGlobalVariables(moruntime.InternalSQLExecutor, exec)
	moruntime.SetupServiceBasedRuntime(proc.GetService(), rt)

	eng := mock_frontend.NewMockEngine(ctrl)
	c := NewCompile("test", "test", "alter table dept", "", "", eng, proc, nil, false, nil, time.Now())
	c.pn = &plan.Plan{
		Plan: &plan2.Plan_Ddl{
			Ddl: &plan2.DataDefinition{
				DdlType: plan2.DataDefinition_ALTER_TABLE,
			},
		},
	}
	return c
}

func newAlterCopyConstNullResult(mp *mpool.MPool, typ types.Type) executor.Result {
	bat := batch.NewWithSize(1)
	bat.SetRowCount(1)
	bat.Vecs[0] = vector.NewConstNull(typ, 1, mp)
	return executor.Result{Mp: mp, Batches: []*batch.Batch{bat}}
}

func newAlterCopyFixedResult[T any](t *testing.T, mp *mpool.MPool, typ types.Type, values []T) executor.Result {
	memRes := executor.NewMemResult([]types.Type{typ}, mp)
	memRes.NewBatchWithRowCount(len(values))
	require.NoError(t, executor.AppendFixedRows(memRes, 0, values))
	return memRes.GetResult()
}

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

		proc := testutil.NewProcess(t)
		proc.Base.SessionInfo.Buf = buffer.New()

		ctx := context.Background()
		proc.Ctx = context.Background()
		txnCli, txnOp := newTestTxnClientAndOpWithPessimistic(ctrl)
		proc.Base.TxnClient = txnCli
		proc.Base.TxnOperator = txnOp
		proc.ReplaceTopCtx(ctx)

		relation := mock_frontend.NewMockRelation(ctrl)
		relation.EXPECT().GetTableID(gomock.Any()).Return(uint64(1)).AnyTimes()
		relation.EXPECT().GetExtraInfo().Return(&api.SchemaExtra{}).AnyTimes()

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

		proc := testutil.NewProcess(t)
		proc.Base.SessionInfo.Buf = buffer.New()

		ctx := context.Background()
		proc.Ctx = context.Background()
		txnCli, txnOp := newTestTxnClientAndOpWithPessimistic(ctrl)
		proc.Base.TxnClient = txnCli
		proc.Base.TxnOperator = txnOp
		proc.ReplaceTopCtx(ctx)

		relation := mock_frontend.NewMockRelation(ctrl)
		relation.EXPECT().GetTableID(gomock.Any()).Return(uint64(1)).AnyTimes()
		relation.EXPECT().GetExtraInfo().Return(&api.SchemaExtra{}).AnyTimes()

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

		proc := testutil.NewProcess(t)
		proc.Base.SessionInfo.Buf = buffer.New()

		ctx := context.Background()
		proc.Ctx = context.Background()
		txnCli, txnOp := newTestTxnClientAndOpWithPessimistic(ctrl)
		proc.Base.TxnClient = txnCli
		proc.Base.TxnOperator = txnOp
		proc.ReplaceTopCtx(ctx)

		relation := mock_frontend.NewMockRelation(ctrl)
		relation.EXPECT().GetTableID(gomock.Any()).Return(uint64(1)).AnyTimes()
		relation.EXPECT().GetExtraInfo().Return(&api.SchemaExtra{}).AnyTimes()

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

		proc := testutil.NewProcess(t)
		proc.Base.SessionInfo.Buf = buffer.New()

		ctx := context.Background()
		proc.Ctx = context.Background()
		txnCli, txnOp := newTestTxnClientAndOpWithPessimistic(ctrl)
		proc.Base.TxnClient = txnCli
		proc.Base.TxnOperator = txnOp
		proc.ReplaceTopCtx(ctx)

		relation := mock_frontend.NewMockRelation(ctrl)
		relation.EXPECT().GetTableID(gomock.Any()).Return(uint64(1)).AnyTimes()
		relation.EXPECT().GetExtraInfo().Return(&api.SchemaExtra{}).AnyTimes()

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

		proc := testutil.NewProcess(t)
		proc.Base.SessionInfo.Buf = buffer.New()

		ctx := context.Background()
		proc.Ctx = context.Background()
		txnCli, txnOp := newTestTxnClientAndOpWithPessimistic(ctrl)
		proc.Base.TxnClient = txnCli
		proc.Base.TxnOperator = txnOp
		proc.ReplaceTopCtx(ctx)

		relation := mock_frontend.NewMockRelation(ctrl)
		relation.EXPECT().GetTableID(gomock.Any()).Return(uint64(1)).AnyTimes()
		relation.EXPECT().GetExtraInfo().Return(&api.SchemaExtra{}).AnyTimes()

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

		proc := testutil.NewProcess(t)
		proc.Base.SessionInfo.Buf = buffer.New()

		ctx := context.Background()
		proc.Ctx = context.Background()
		txnCli, txnOp := newTestTxnClientAndOpWithPessimistic(ctrl)
		proc.Base.TxnClient = txnCli
		proc.Base.TxnOperator = txnOp
		proc.ReplaceTopCtx(ctx)

		relation := mock_frontend.NewMockRelation(ctrl)
		relation.EXPECT().GetTableID(gomock.Any()).Return(uint64(1)).AnyTimes()
		relation.EXPECT().GetExtraInfo().Return(&api.SchemaExtra{}).AnyTimes()

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

		proc := testutil.NewProcess(t)
		proc.Base.SessionInfo.Buf = buffer.New()

		ctx := context.Background()
		proc.Ctx = context.Background()
		txnCli, txnOp := newTestTxnClientAndOpWithPessimistic(ctrl)
		proc.Base.TxnClient = txnCli
		proc.Base.TxnOperator = txnOp
		proc.ReplaceTopCtx(ctx)

		relation := mock_frontend.NewMockRelation(ctrl)
		relation.EXPECT().GetTableID(gomock.Any()).Return(uint64(1)).AnyTimes()
		relation.EXPECT().GetExtraInfo().Return(&api.SchemaExtra{}).AnyTimes()

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

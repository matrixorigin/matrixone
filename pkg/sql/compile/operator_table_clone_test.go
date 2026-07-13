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

package compile

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/common/sqlquote"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
)

type tableCloneRecordingExecutor struct {
	result executor.Result
	err    error
	sql    string
	opts   executor.Options
}

func (e *tableCloneRecordingExecutor) Exec(
	ctx context.Context,
	sql string,
	opts executor.Options,
) (executor.Result, error) {
	if err := ctx.Err(); err != nil {
		return executor.Result{}, err
	}
	e.sql = sql
	e.opts = opts
	return e.result, e.err
}

func (e *tableCloneRecordingExecutor) ExecTxn(
	ctx context.Context,
	_ func(executor.TxnExecutor) error,
	_ executor.Options,
) error {
	return ctx.Err()
}

func newTableCloneResult(t *testing.T, mp *mpool.MPool, value uint64) executor.Result {
	t.Helper()
	return newAlterCopyFixedResult(t, mp, types.T_uint64.ToType(), []uint64{value})
}

func TestConstructTableCloneReadsMaximumFromCloneSnapshot(t *testing.T) {
	proc := testutil.NewProcess(t)
	exec := &tableCloneRecordingExecutor{result: newTableCloneResult(t, proc.Mp(), 40)}
	runtime.ServiceRuntime(proc.GetService()).SetGlobalVariables(runtime.InternalSQLExecutor, exec)

	srcDef := &plan.TableDef{
		TblId:          7,
		DbName:         "db-name",
		Name:           "t`name",
		AutoIncrOffset: 999,
		Cols: []*plan.ColDef{{
			Name: "id`col",
			Typ:  plan.Type{Id: int32(types.T_uint64), AutoIncr: true},
		}},
	}
	dstDef := &plan.TableDef{AutoIncrOffset: 99}
	snapshot := &plan.Snapshot{
		TS:     &timestamp.Timestamp{PhysicalTime: 123},
		Tenant: &plan.SnapshotTenant{TenantID: 17},
	}
	clonePlan := &plan.CloneTable{
		SrcTableDef:  srcDef,
		SrcObjDef:    &plan.ObjectRef{},
		ScanSnapshot: snapshot,
		CreateTable: &plan.Plan{Plan: &plan.Plan_Ddl{Ddl: &plan.DataDefinition{
			Definition: &plan.DataDefinition_CreateTable{CreateTable: &plan.CreateTable{TableDef: dstDef}},
		}}},
	}

	tc, err := constructTableClone(&Compile{proc: proc, pn: &plan.Plan{}}, clonePlan)
	require.NoError(t, err)
	t.Cleanup(tc.Release)
	require.Equal(t, uint64(99), tc.Ctx.RequestedAutoIncrOffset)
	require.Equal(t, map[int32]uint64{0: 40}, tc.Ctx.SrcAutoIncrMaxValues)
	require.True(t, exec.opts.HasAccountID())
	require.Equal(t, uint32(17), exec.opts.AccountID())

	col := sqlquote.Ident(srcDef.Cols[0].Name)
	table := sqlquote.QualifiedIdent(srcDef.DbName, srcDef.Name)
	require.Equal(t,
		"select cast(coalesce(max(case when "+col+" > 0 then "+col+" else 0 end), 0) as unsigned) from "+
			table+" {MO_TS = 123}",
		exec.sql,
	)
}

func TestConstructTableCloneHonorsCanceledContextBeforeMaximumRead(t *testing.T) {
	proc := testutil.NewProcess(t)
	ctx, cancel := context.WithCancel(proc.Ctx)
	cancel()
	proc.Ctx = ctx
	exec := &tableCloneRecordingExecutor{result: newTableCloneResult(t, proc.Mp(), 40)}
	runtime.ServiceRuntime(proc.GetService()).SetGlobalVariables(runtime.InternalSQLExecutor, exec)

	_, err := constructTableClone(&Compile{proc: proc, pn: &plan.Plan{}}, &plan.CloneTable{
		SrcTableDef: &plan.TableDef{
			DbName: "db",
			Name:   "src",
			Cols: []*plan.ColDef{{
				Name: "id",
				Typ:  plan.Type{Id: int32(types.T_uint64), AutoIncr: true},
			}},
		},
		SrcObjDef: &plan.ObjectRef{},
	})
	require.ErrorIs(t, err, context.Canceled)
	require.Empty(t, exec.sql)
}

func TestConstructTableCloneClosesResultReturnedWithError(t *testing.T) {
	proc := testutil.NewProcess(t)
	result := newTableCloneResult(t, proc.Mp(), 40)
	resultBatch := result.Batches[0]
	wantErr := errors.New("maximum read failed")
	exec := &tableCloneRecordingExecutor{result: result, err: wantErr}
	runtime.ServiceRuntime(proc.GetService()).SetGlobalVariables(runtime.InternalSQLExecutor, exec)

	_, err := constructTableClone(&Compile{proc: proc, pn: &plan.Plan{}}, &plan.CloneTable{
		SrcTableDef: &plan.TableDef{
			DbName: "db",
			Name:   "src",
			Cols: []*plan.ColDef{{
				Name: "id",
				Typ:  plan.Type{Id: int32(types.T_uint64), AutoIncr: true},
			}},
		},
		SrcObjDef: &plan.ObjectRef{},
	})
	require.ErrorIs(t, err, wantErr)
	require.Nil(t, resultBatch.Vecs)
}

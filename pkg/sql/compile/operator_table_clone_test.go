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
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/common/sqlquote"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/table_clone"
	"github.com/matrixorigin/matrixone/pkg/sql/features"
	mysqlparser "github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
)

type tableCloneRecordingExecutor struct {
	result executor.Result
	err    error
	run    func(string) (executor.Result, error)
	sql    string
	sqls   []string
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
	e.sqls = append(e.sqls, sql)
	e.opts = opts
	if e.run != nil {
		return e.run(sql)
	}
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

func newTableCloneOffsetResult(t *testing.T, mp *mpool.MPool, colIdx int32, offset uint64) executor.Result {
	t.Helper()
	memRes := executor.NewMemResult([]types.Type{types.T_int32.ToType(), types.T_uint64.ToType()}, mp)
	memRes.NewBatchWithRowCount(1)
	require.NoError(t, executor.AppendFixedRows(memRes, 0, []int32{colIdx}))
	require.NoError(t, executor.AppendFixedRows(memRes, 1, []uint64{offset}))
	return memRes.GetResult()
}

func newTableCloneNamedOffsetResult(t *testing.T, mp *mpool.MPool, colName string, offset uint64) executor.Result {
	t.Helper()
	memRes := executor.NewMemResult([]types.Type{types.T_varchar.ToType(), types.T_uint64.ToType()}, mp)
	memRes.NewBatchWithRowCount(1)
	require.NoError(t, executor.AppendStringRows(memRes, 0, []string{colName}))
	require.NoError(t, executor.AppendFixedRows(memRes, 1, []uint64{offset}))
	return memRes.GetResult()
}

func newTableClonePartitionIndexResult(
	t *testing.T,
	mp *mpool.MPool,
	partitionNames, indexNames, tableTypes, tableNames []string,
) executor.Result {
	t.Helper()
	memRes := executor.NewMemResult([]types.Type{
		types.T_varchar.ToType(),
		types.T_varchar.ToType(),
		types.T_varchar.ToType(),
		types.T_varchar.ToType(),
	}, mp)
	memRes.NewBatchWithRowCount(len(partitionNames))
	require.NoError(t, executor.AppendStringRows(memRes, 0, partitionNames))
	require.NoError(t, executor.AppendStringRows(memRes, 1, indexNames))
	require.NoError(t, executor.AppendStringRows(memRes, 2, tableTypes))
	require.NoError(t, executor.AppendStringRows(memRes, 3, tableNames))
	return memRes.GetResult()
}

func cloneCreatePlan(dstDef *plan.TableDef) *plan.Plan {
	return &plan.Plan{Plan: &plan.Plan_Ddl{Ddl: &plan.DataDefinition{
		Definition: &plan.DataDefinition_CreateTable{
			CreateTable: &plan.CreateTable{TableDef: dstDef},
		},
	}}}
}

func TestConstructTableCloneUsesPhysicalTemporaryDestination(t *testing.T) {
	proc := testutil.NewProcess(t)
	proc.Base.SessionInfo.SessionId = uuid.MustParse("11111111-2222-3333-4444-555555555555")

	createPlan := cloneCreatePlan(&plan.TableDef{Name: "temp_dst"})
	createPlan.GetDdl().GetCreateTable().Temporary = true
	tc, err := constructTableClone(&Compile{proc: proc, pn: &plan.Plan{}}, &plan.CloneTable{
		SrcTableDef:     &plan.TableDef{},
		SrcObjDef:       &plan.ObjectRef{},
		DstDatabaseName: "clone_db",
		DstTableName:    "temp_dst",
		CreateTable:     createPlan,
	})
	require.NoError(t, err)
	t.Cleanup(tc.Release)
	require.Equal(t,
		defines.GenTempTableName(proc.Base.SessionInfo.SessionId, "clone_db", "temp_dst"),
		tc.Ctx.DstTblName,
	)
}

func TestConstructTableCloneUsesCopiedStateForFreshClone(t *testing.T) {
	proc := testutil.NewProcess(t)
	exec := &tableCloneRecordingExecutor{}
	exec.run = func(sql string) (executor.Result, error) {
		if strings.Contains(sql, "mo_catalog.mo_increment_columns") {
			return newTableCloneOffsetResult(t, proc.Mp(), 1, 50), nil
		}
		return newTableCloneResult(t, proc.Mp(), 40), nil
	}
	runtime.ServiceRuntime(proc.GetService()).SetGlobalVariables(runtime.InternalSQLExecutor, exec)

	srcDef := &plan.TableDef{
		TblId:          7,
		DbName:         "db-name",
		Name:           "t`name",
		AutoIncrOffset: 1999,
		Cols: []*plan.ColDef{
			{ColId: 1, Name: "payload", Typ: plan.Type{Id: int32(types.T_int64)}},
			{ColId: 11, Name: "id`col", Typ: plan.Type{Id: int32(types.T_uint64), AutoIncr: true}},
		},
	}
	dstDef := &plan.TableDef{
		AutoIncrOffset: 999,
		Cols: []*plan.ColDef{
			{ColId: 0, Name: "id`col", Typ: plan.Type{Id: int32(types.T_uint64), AutoIncr: true}},
			{ColId: 10, Name: "payload", Typ: plan.Type{Id: int32(types.T_int64)}},
		},
	}
	snapshot := &plan.Snapshot{
		TS:     &timestamp.Timestamp{PhysicalTime: 123},
		Tenant: &plan.SnapshotTenant{TenantID: 17},
	}
	tc, err := constructTableClone(&Compile{proc: proc, pn: &plan.Plan{}}, &plan.CloneTable{
		SrcTableDef:  srcDef,
		SrcObjDef:    &plan.ObjectRef{},
		ScanSnapshot: snapshot,
		CreateTable:  cloneCreatePlan(dstDef),
	})
	require.NoError(t, err)
	t.Cleanup(tc.Release)
	require.Equal(t, uint64(1999), tc.Ctx.RequestedAutoIncrOffset)
	require.Equal(t, map[string]uint64{"id`col": 40}, tc.Ctx.SrcAutoIncrMaxValues)
	require.Empty(t, tc.Ctx.SrcAutoIncrOffsets)
	require.True(t, exec.opts.HasAccountID())
	require.Equal(t, uint32(17), exec.opts.AccountID())

	col := sqlquote.Ident(srcDef.Cols[1].Name)
	table := sqlquote.QualifiedIdent(srcDef.DbName, srcDef.Name)
	require.Equal(t,
		"select cast(coalesce(max(case when "+col+" > 0 then "+col+" else 0 end), 0) as unsigned) from "+
			table+" {MO_TS = 123}",
		exec.sql,
	)
}

func TestConstructTableClonePreservesAllocatorForAlterCopy(t *testing.T) {
	proc := testutil.NewProcess(t)
	exec := &tableCloneRecordingExecutor{}
	exec.run = func(sql string) (executor.Result, error) {
		if strings.Contains(sql, "mo_catalog.mo_increment_columns") {
			return newTableCloneOffsetResult(t, proc.Mp(), 0, 50), nil
		}
		return newTableCloneResult(t, proc.Mp(), 40), nil
	}
	runtime.ServiceRuntime(proc.GetService()).SetGlobalVariables(runtime.InternalSQLExecutor, exec)

	tc, err := constructTableClone(&Compile{proc: proc, pn: &plan.Plan{}}, &plan.CloneTable{
		SrcTableDef: &plan.TableDef{
			TblId:  7,
			DbName: "db",
			Name:   "src",
			Cols: []*plan.ColDef{{
				ColId: 11,
				Name:  "id",
				Typ:   plan.Type{Id: int32(types.T_uint64), AutoIncr: true},
			}},
		},
		SrcObjDef: &plan.ObjectRef{},
	})
	require.NoError(t, err)
	t.Cleanup(tc.Release)
	require.Equal(t, map[string]uint64{"id": 40}, tc.Ctx.SrcAutoIncrMaxValues)
	require.Equal(t, map[string]uint64{"id": 50}, tc.Ctx.SrcAutoIncrOffsets)
}

func TestMapCloneAutoIncrColumnsAcrossSchemaChanges(t *testing.T) {
	autoType := plan.Type{Id: int32(types.T_uint64), AutoIncr: true}
	tests := []struct {
		name        string
		src         *plan.TableDef
		dst         *plan.TableDef
		sameIDSpace bool
		want        map[int32]string
	}{
		{
			name: "fresh clone maps by name despite numeric id collision",
			src: &plan.TableDef{Cols: []*plan.ColDef{
				{ColId: 1, Name: "id", Typ: autoType},
				{ColId: 2, Name: "payload", Typ: plan.Type{Id: int32(types.T_int64)}},
			}},
			dst: &plan.TableDef{Cols: []*plan.ColDef{
				{ColId: 1, Name: "payload", Typ: plan.Type{Id: int32(types.T_int64)}},
				{ColId: 0, Name: "id", Typ: autoType},
			}},
			want: map[int32]string{0: "id"},
		},
		{
			name: "copy alter maps rename and reorder by stable id",
			src: &plan.TableDef{Cols: []*plan.ColDef{
				{ColId: 10, Name: "dropped", Typ: plan.Type{Id: int32(types.T_int64)}},
				{ColId: 11, Name: "id", Typ: autoType},
				{ColId: 12, Name: "__mo_fake_pk_col", Hidden: true, Typ: autoType},
			}},
			dst: &plan.TableDef{Cols: []*plan.ColDef{
				{ColId: 13, Name: "added", Typ: plan.Type{Id: int32(types.T_int64)}},
				{ColId: 12, Name: "__mo_fake_pk_col", Hidden: true, Typ: autoType},
				{ColId: 11, Name: "renamed_id", Typ: autoType},
			}},
			sameIDSpace: true,
			want:        map[int32]string{1: "renamed_id", 2: "__mo_fake_pk_col"},
		},
		{
			name: "dropped source does not transfer allocator to reused name",
			src: &plan.TableDef{Cols: []*plan.ColDef{
				{ColId: 10, Name: "id", Typ: autoType},
			}},
			dst: &plan.TableDef{Cols: []*plan.ColDef{
				{ColId: 12, Name: "id", Typ: autoType},
			}},
			sameIDSpace: true,
			want:        map[int32]string{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, mapCloneAutoIncrColumns(tt.src, tt.dst, tt.sameIDSpace))
		})
	}
}

func TestConstructTableCloneDoesNotReadHiddenMaximum(t *testing.T) {
	proc := testutil.NewProcess(t)
	wantErr := errors.New("hidden auto-increment column must not be queried")
	exec := &tableCloneRecordingExecutor{}
	exec.run = func(sql string) (executor.Result, error) {
		if strings.Contains(sql, "__mo_fake_pk_col") {
			return executor.Result{}, wantErr
		}
		return newTableCloneOffsetResult(t, proc.Mp(), 0, 40), nil
	}
	runtime.ServiceRuntime(proc.GetService()).SetGlobalVariables(runtime.InternalSQLExecutor, exec)

	tc, err := constructTableClone(&Compile{proc: proc, pn: &plan.Plan{}}, &plan.CloneTable{
		SrcTableDef: &plan.TableDef{
			TblId: 7,
			Cols: []*plan.ColDef{{
				Name:   "__mo_fake_pk_col",
				Hidden: true,
				Typ:    plan.Type{Id: int32(types.T_uint64), AutoIncr: true},
			}},
		},
		SrcObjDef: &plan.ObjectRef{},
		CreateTable: cloneCreatePlan(&plan.TableDef{Cols: []*plan.ColDef{{
			Name:   "__mo_fake_pk_col",
			Hidden: true,
			Typ:    plan.Type{Id: int32(types.T_uint64), AutoIncr: true},
		}}}),
	})
	require.NoError(t, err)
	t.Cleanup(tc.Release)
	require.Len(t, exec.sqls, 1)
	require.Equal(t, map[string]uint64{"__mo_fake_pk_col": 40}, tc.Ctx.SrcAutoIncrOffsets)
}

func TestConstructTableCloneCapturesHiddenIndexAllocator(t *testing.T) {
	proc := testutil.NewProcess(t)
	const (
		srcIndexTable = "src'fulltext\\table"
		dstIndexTable = "__mo_index_fulltext_target"
		fakePK        = "__mo_fake_pk_col"
	)
	exec := &tableCloneRecordingExecutor{}
	exec.run = func(sql string) (executor.Result, error) {
		if strings.Contains(sql, "mo_catalog.mo_increment_columns") {
			return newTableCloneNamedOffsetResult(t, proc.Mp(), fakePK, 200), nil
		}
		return newTableCloneResult(t, proc.Mp(), 120), nil
	}
	runtime.ServiceRuntime(proc.GetService()).SetGlobalVariables(runtime.InternalSQLExecutor, exec)

	srcDef := &plan.TableDef{
		TblId:  7,
		DbName: "db'name\\path",
		Name:   "src",
		Indexes: []*plan.IndexDef{{
			IndexName:      "ftidx",
			IndexAlgo:      "fulltext",
			IndexTableName: srcIndexTable,
		}},
	}
	dstDef := &plan.TableDef{
		Name: "dst",
		Indexes: []*plan.IndexDef{{
			IndexName:      "ftidx",
			IndexAlgo:      "fulltext",
			IndexTableName: dstIndexTable,
		}},
	}
	dstIndexDef := &plan.TableDef{
		Name: dstIndexTable,
		Cols: []*plan.ColDef{{
			Name:   fakePK,
			Hidden: true,
			Typ:    plan.Type{Id: int32(types.T_uint64), AutoIncr: true},
		}},
		Pkey: &plan.PrimaryKeyDef{PkeyColName: fakePK},
	}
	createPlan := cloneCreatePlan(dstDef)
	createPlan.GetDdl().GetCreateTable().IndexTables = []*plan.TableDef{dstIndexDef}

	tc, err := constructTableClone(&Compile{proc: proc, pn: &plan.Plan{}}, &plan.CloneTable{
		SrcTableDef: srcDef,
		SrcObjDef:   &plan.ObjectRef{},
		CreateTable: createPlan,
		ScanSnapshot: &plan.Snapshot{
			TS: &timestamp.Timestamp{PhysicalTime: 123},
		},
	})
	require.NoError(t, err)
	t.Cleanup(tc.Release)
	require.Equal(t, table_clone.AutoIncrementState{
		MaxValues: map[string]uint64{fakePK: 120},
		Offsets:   map[string]uint64{fakePK: 200},
	}, tc.Ctx.IndexAutoIncrStates["ftidx."])
	require.Len(t, exec.sqls, 2)
	require.Contains(t, exec.sqls[0], "reldatabase = "+sqlquote.String(srcDef.DbName))
	require.Contains(t, exec.sqls[0], "relname = "+sqlquote.String(srcIndexTable))
	require.Contains(t, exec.sqls[1], "from "+sqlquote.QualifiedIdent(srcDef.DbName, srcIndexTable))
	for _, sql := range exec.sqls {
		_, err := mysqlparser.ParseOne(context.Background(), sql, 1)
		require.NoError(t, err, sql)
	}
}

func TestConstructTableCloneCapturesPartitionHiddenIndexAllocators(t *testing.T) {
	proc := testutil.NewProcess(t)
	const fakePK = "__mo_fake_pk_col"
	exec := &tableCloneRecordingExecutor{}
	exec.run = func(sql string) (executor.Result, error) {
		switch {
		case strings.Contains(sql, "mo_partition_tables"):
			return newTableClonePartitionIndexResult(
				t,
				proc.Mp(),
				[]string{"p0", "p1"},
				[]string{"ftidx", "ftidx"},
				[]string{"", ""},
				[]string{"__mo_index_fulltext_src_p0", "__mo_index_fulltext_src_p1"},
			), nil
		case strings.Contains(sql, "mo_increment_columns"):
			return newTableCloneNamedOffsetResult(t, proc.Mp(), fakePK, 200), nil
		default:
			return newTableCloneResult(t, proc.Mp(), 120), nil
		}
	}
	runtime.ServiceRuntime(proc.GetService()).SetGlobalVariables(runtime.InternalSQLExecutor, exec)

	srcDef := &plan.TableDef{
		TblId:       7,
		DbName:      "db",
		Name:        "src",
		FeatureFlag: features.Partitioned,
		Indexes: []*plan.IndexDef{{
			IndexName:      "ftidx",
			IndexAlgo:      "fulltext",
			IndexTableName: "__mo_index_fulltext_src",
		}},
	}
	dstDef := &plan.TableDef{
		Name: "dst",
		Indexes: []*plan.IndexDef{{
			IndexName:      "ftidx",
			IndexAlgo:      "fulltext",
			IndexTableName: "__mo_index_fulltext_dst",
		}},
	}
	dstIndexDef := &plan.TableDef{
		Name: "__mo_index_fulltext_dst",
		Cols: []*plan.ColDef{{
			Name:   fakePK,
			Hidden: true,
			Typ:    plan.Type{Id: int32(types.T_uint64), AutoIncr: true},
		}},
		Pkey: &plan.PrimaryKeyDef{PkeyColName: fakePK},
	}
	createPlan := cloneCreatePlan(dstDef)
	createPlan.GetDdl().GetCreateTable().IndexTables = []*plan.TableDef{dstIndexDef}

	tc, err := constructTableClone(&Compile{proc: proc, pn: &plan.Plan{}}, &plan.CloneTable{
		SrcTableDef: srcDef,
		SrcObjDef:   &plan.ObjectRef{},
		CreateTable: createPlan,
	})
	require.NoError(t, err)
	t.Cleanup(tc.Release)
	want := table_clone.AutoIncrementState{
		MaxValues: map[string]uint64{fakePK: 120},
		Offsets:   map[string]uint64{fakePK: 200},
	}
	require.Equal(t, want, tc.Ctx.IndexAutoIncrStates["p0.ftidx."])
	require.Equal(t, want, tc.Ctx.IndexAutoIncrStates["p1.ftidx."])
	require.Equal(t, want, tc.Ctx.IndexAutoIncrStates["ftidx."])
	require.Len(t, exec.sqls, 7)
	for _, sql := range exec.sqls {
		_, err := mysqlparser.ParseOne(context.Background(), sql, 1)
		require.NoError(t, err, sql)
	}
}

func TestConstructTableCloneHonorsCancellationAndClosesErrorResult(t *testing.T) {
	t.Run("canceled before read", func(t *testing.T) {
		proc := testutil.NewProcess(t)
		ctx, cancel := context.WithCancel(proc.Ctx)
		cancel()
		proc.Ctx = ctx
		exec := &tableCloneRecordingExecutor{result: newTableCloneResult(t, proc.Mp(), 40)}
		runtime.ServiceRuntime(proc.GetService()).SetGlobalVariables(runtime.InternalSQLExecutor, exec)

		_, err := constructTableClone(&Compile{proc: proc, pn: &plan.Plan{}}, &plan.CloneTable{
			SrcTableDef: &plan.TableDef{Cols: []*plan.ColDef{{
				Name: "id", Typ: plan.Type{Id: int32(types.T_uint64), AutoIncr: true},
			}}},
			SrcObjDef: &plan.ObjectRef{},
		})
		require.ErrorIs(t, err, context.Canceled)
		require.Empty(t, exec.sql)
	})

	t.Run("result returned with error", func(t *testing.T) {
		proc := testutil.NewProcess(t)
		result := newTableCloneResult(t, proc.Mp(), 40)
		resultBatch := result.Batches[0]
		wantErr := errors.New("allocator read failed")
		exec := &tableCloneRecordingExecutor{result: result, err: wantErr}
		runtime.ServiceRuntime(proc.GetService()).SetGlobalVariables(runtime.InternalSQLExecutor, exec)

		_, err := constructTableClone(&Compile{proc: proc, pn: &plan.Plan{}}, &plan.CloneTable{
			SrcTableDef: &plan.TableDef{Cols: []*plan.ColDef{{
				Name: "id", Typ: plan.Type{Id: int32(types.T_uint64), AutoIncr: true},
			}}},
			SrcObjDef: &plan.ObjectRef{},
		})
		require.ErrorIs(t, err, wantErr)
		require.Nil(t, resultBatch.Vecs)
	})
}

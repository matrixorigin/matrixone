// Copyright 2021-2024 Matrix Origin
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

package multi_update

import (
	"context"
	"math"
	"strconv"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/hashmap"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/common/rscthrottler"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	pbPlan "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/perfcounter"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/util/toml"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

var (
	i64typ     = plan.Type{Id: int32(types.T_int64)}
	i32typ     = plan.Type{Id: int32(types.T_int32)}
	varcharTyp = plan.Type{Id: int32(types.T_varchar), Width: 8192}
	rowIdTyp   = plan.Type{Id: int32(types.T_Rowid)}
)

const (
	// Two unequal, non-empty batches cover accumulation followed by a
	// threshold-triggered flush without using volume as the trigger.
	testS3BatchCount = 2
	testS3BatchRows  = 4
)

type testCase struct {
	op                *MultiUpdate
	inputBatchs       []*batch.Batch
	expectErr         bool
	relResetExpectErr bool
	affectedRows      uint64
}

func runTestCase(t *testing.T, proc *process.Process, tc *testCase) {
	t.Helper()
	runTestCaseWithLifecycle(t, proc, tc, false)
}

// Reset/reuse is an orthogonal lifecycle dimension. Call this from the
// maximal indexed schema for each action/sink pair instead of multiplying it
// across every semantic case.
func runTestCaseAfterReset(t *testing.T, proc *process.Process, tc *testCase) {
	t.Helper()
	runTestCaseWithLifecycle(t, proc, tc, true)
}

func runTestCaseWithLifecycle(
	t *testing.T,
	proc *process.Process,
	tc *testCase,
	checkResetAndReuse bool,
) {
	t.Helper()
	var err error
	var res vm.CallResult

	child := colexec.NewMockOperator().WithBatchs(tc.inputBatchs)
	tc.op.AppendChild(child)
	cleaned := false
	pipelineFailed := false
	var cleanupErr error
	cleanup := func() {
		if cleaned {
			return
		}
		cleaned = true
		child.Free(proc, pipelineFailed, cleanupErr)
		tc.op.Free(proc, pipelineFailed, cleanupErr)
		proc.Free()
	}
	t.Cleanup(cleanup)

	err = tc.op.Prepare(proc)
	require.NoError(t, err)
	setTestS3FlushThreshold(t, tc)
	for {
		res, err = vm.Exec(tc.op, proc)
		if tc.expectErr {
			require.Error(t, err)
			break
		}
		if res.Batch == nil || res.Status == vm.ExecStop {
			break
		}
	}

	// Error cases terminate on their first execution and do not have a valid
	// reset/reuse transition to exercise.
	if tc.expectErr {
		for _, bat := range tc.inputBatchs {
			bat.Clean(proc.GetMPool())
		}
		pipelineFailed = true
		cleanupErr = err
		cleanup()
		require.Equal(t, int64(0), proc.GetMPool().CurrNB())
		return
	}
	require.NoError(t, err)
	require.Equal(t, tc.affectedRows, tc.op.GetAffectedRows())

	if checkResetAndReuse {
		child.ResetBatchs()
		tc.op.Children[0].Reset(proc, false, nil)
		tc.op.Reset(proc, false, nil)

		child.WithBatchs(tc.inputBatchs)
		err = tc.op.Prepare(proc)
		if tc.relResetExpectErr {
			require.Error(t, err)
			for _, bat := range tc.inputBatchs {
				bat.Clean(proc.GetMPool())
			}
			pipelineFailed = true
			cleanupErr = err
			cleanup()
			require.Equal(t, int64(0), proc.GetMPool().CurrNB())
			return
		}
		require.NoError(t, err)
		setTestS3FlushThreshold(t, tc)
		if tc.op.Action == UpdateWriteS3 {
			sentinel, mapErr := hashmap.NewStrHashMap(false, proc.Mp())
			require.NoError(t, mapErr)
			tc.op.ctr.seenTargetRows[math.MaxUint64] = sentinel
			require.Same(t, sentinel, tc.op.ctr.s3Writer.seenTargetRows[math.MaxUint64])
			delete(tc.op.ctr.seenTargetRows, math.MaxUint64)
			sentinel.Free()
		}
		if tc.op.Action != UpdateWriteS3 {
			for _, info := range tc.op.ctr.updateCtxInfos {
				require.NotNil(t, info.Source)
			}
		}

		for {
			res, err = vm.Exec(tc.op, proc)
			if res.Batch == nil || res.Status == vm.ExecStop {
				break
			}
		}
		require.NoError(t, err)
		require.Equal(t, tc.op.GetAffectedRows(), tc.affectedRows)
	}

	cleanup()
	require.Equal(t, int64(0), proc.GetMPool().CurrNB())
}

func setTestS3FlushThreshold(t *testing.T, tc *testCase) {
	t.Helper()
	writer := tc.op.ctr.s3Writer
	if writer == nil {
		return
	}
	require.NotEmpty(t, tc.inputBatchs)

	// Keep the threshold boundary deterministic with minimum data: the first
	// batch stays buffered and the next non-empty batch triggers a flush.
	var firstBatchSize uint64
	for _, idx := range writer.checkSizeCols {
		firstBatchSize += uint64(tc.inputBatchs[0].Vecs[idx].Size())
	}
	require.Positive(t, firstBatchSize)
	writer.flushThreshold = firstBatchSize + 1
}

func ptrTo[T any](v T) *T {
	return &v
}

func prepareTestCtx(t *testing.T, withFs bool) (context.Context, *gomock.Controller, *process.Process) {
	ctrl := gomock.NewController(t)

	ctx := context.TODO()
	txnOperator := mock_frontend.NewMockTxnOperator(ctrl)
	txnOperator.EXPECT().Commit(gomock.Any()).Return(nil).AnyTimes()
	txnOperator.EXPECT().Rollback(ctx).Return(nil).AnyTimes()

	txnClient := mock_frontend.NewMockTxnClient(ctrl)
	txnClient.EXPECT().New(gomock.Any(), gomock.Any()).Return(txnOperator, nil).AnyTimes()

	var proc *process.Process
	if withFs {
		counterSet := new(perfcounter.CounterSet)
		dir := t.TempDir()
		cacheDir := t.TempDir()
		fs, _ := fileservice.NewFileService(
			context.Background(),
			fileservice.Config{
				Name:    defines.SharedFileServiceName,
				Backend: "S3",
				S3: fileservice.ObjectStorageArguments{
					Endpoint: "disk",
					Bucket:   dir,
				},
				Cache: fileservice.CacheConfig{
					MemoryCapacity: ptrTo(toml.ByteSize(1 << 20)),
					DiskPath:       ptrTo(cacheDir),
					DiskCapacity:   ptrTo(toml.ByteSize(10 * (1 << 20))),
					CheckOverlaps:  false,
				},
			},
			[]*perfcounter.CounterSet{
				counterSet,
			},
		)
		optFs := testutil.WithFileService(fs)
		proc = testutil.NewProc(t, optFs)
	} else {
		proc = testutil.NewProc(t)
	}
	fileService := proc.GetFileService()
	t.Cleanup(func() {
		fileService.Close(context.Background())
	})

	proc.Base.TxnClient = txnClient
	proc.Ctx = ctx

	_ = colexec.NewServer(proc.GetService())

	throttler := rscthrottler.NewMemThrottler(t.Name(), 1.0)

	runtime.ServiceRuntime(proc.GetService()).SetGlobalVariables(runtime.CNMemoryThrottler, throttler)

	return ctx, ctrl, proc
}

func prepareTestEng(ctrl *gomock.Controller, relationResetReturnError bool) engine.Engine {
	eng := mock_frontend.NewMockEngine(ctrl)
	eng.EXPECT().New(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
	eng.EXPECT().Hints().Return(engine.Hints{
		CommitOrRollbackTimeout: time.Second,
	}).AnyTimes()

	database := mock_frontend.NewMockDatabase(ctrl)
	eng.EXPECT().Database(gomock.Any(), gomock.Any(), gomock.Any()).Return(database, nil).AnyTimes()

	relation := mock_frontend.NewMockRelation(ctrl)
	relation.EXPECT().Write(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
	if relationResetReturnError {
		relation.EXPECT().Reset(gomock.Any()).Return(moerr.NewInternalErrorNoCtx("")).AnyTimes()
	} else {
		relation.EXPECT().Reset(gomock.Any()).Return(nil).AnyTimes()
	}

	relation.EXPECT().Delete(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).AnyTimes()

	database.EXPECT().Relation(gomock.Any(), gomock.Any(), gomock.Any()).Return(relation, nil).AnyTimes()

	return eng
}

func getTestMainTable() (*plan.ObjectRef, *plan.TableDef) {
	objRef := &plan.ObjectRef{Schema: 1, Obj: 1, SchemaName: "test", ObjName: "t1"}

	tableDef := &plan.TableDef{
		TblId:  0,
		Name:   "t1",
		Hidden: false,
		Cols: []*plan.ColDef{
			{ColId: 0, Name: "a", Typ: i64typ, NotNull: true, Primary: true, Default: &pbPlan.Default{
				NullAbility: false,
			}},
			{ColId: 1, Name: "b", Typ: varcharTyp, NotNull: true},
			{ColId: 2, Name: "c", Typ: i32typ},
			{ColId: 3, Name: "d", Typ: i32typ},
			{ColId: 4, Name: catalog.Row_ID, Typ: rowIdTyp},
		},
		TableType: catalog.SystemOrdinaryRel,
		Pkey: &plan.PrimaryKeyDef{
			Cols:        []uint64{0},
			PkeyColId:   0,
			PkeyColName: "a",
			Names:       []string{"a"},
		},
		Name2ColIndex: make(map[string]int32),
		DbName:        "test",
	}
	tableDef.Name2ColIndex["a"] = 0
	tableDef.Name2ColIndex["b"] = 1
	tableDef.Name2ColIndex["c"] = 2
	tableDef.Name2ColIndex["d"] = 3
	tableDef.Name2ColIndex[catalog.Row_ID] = 4

	return objRef, tableDef
}

func getTestUniqueIndexTable(uniqueTblName string) (*plan.ObjectRef, *plan.TableDef) {
	uniqueObjRef := &plan.ObjectRef{Schema: 1, Obj: 2, SchemaName: "test", ObjName: uniqueTblName}
	uniqueTableDef := &plan.TableDef{
		TblId:  1,
		Name:   uniqueTblName,
		Hidden: true,
		Cols: []*plan.ColDef{
			{ColId: 0, Name: catalog.IndexTableIndexColName, Typ: varcharTyp, NotNull: true, Primary: true, Default: &pbPlan.Default{
				NullAbility: false,
			}},
			{ColId: 1, Name: catalog.IndexTablePrimaryColName, Typ: i64typ, NotNull: true, Default: &pbPlan.Default{
				NullAbility: false,
			}},
			{ColId: 2, Name: catalog.Row_ID, Typ: rowIdTyp},
		},
		TableType: catalog.SystemOrdinaryRel,
		Pkey: &plan.PrimaryKeyDef{
			Cols:        []uint64{0},
			PkeyColId:   0,
			PkeyColName: catalog.IndexTableIndexColName,
			Names:       []string{catalog.IndexTableIndexColName},
		},
		Name2ColIndex: make(map[string]int32),
		DbName:        "test",
	}
	uniqueTableDef.Name2ColIndex[catalog.IndexTableIndexColName] = 0
	uniqueTableDef.Name2ColIndex[catalog.IndexTablePrimaryColName] = 1
	uniqueTableDef.Name2ColIndex[catalog.Row_ID] = 2

	return uniqueObjRef, uniqueTableDef
}

func getTestSecondaryIndexTable(secondaryIdxTblName string) (*plan.ObjectRef, *plan.TableDef) {
	secondaryIdxObjRef := &plan.ObjectRef{Schema: 1, Obj: 2, SchemaName: "test", ObjName: secondaryIdxTblName}
	secondaryIdxTableDef := &plan.TableDef{
		TblId:  1,
		Name:   secondaryIdxTblName,
		Hidden: true,
		Cols: []*plan.ColDef{
			{ColId: 0, Name: catalog.IndexTableIndexColName, Typ: varcharTyp, NotNull: true, Primary: true, Default: &pbPlan.Default{
				NullAbility: false,
			}},
			{ColId: 1, Name: catalog.IndexTablePrimaryColName, Typ: i64typ, NotNull: true},
			{ColId: 2, Name: catalog.Row_ID, Typ: rowIdTyp},
		},
		TableType: catalog.SystemOrdinaryRel,
		Pkey: &plan.PrimaryKeyDef{
			Cols:        []uint64{0},
			PkeyColId:   0,
			PkeyColName: catalog.IndexTableIndexColName,
			Names:       []string{catalog.IndexTableIndexColName},
		},
		Name2ColIndex: make(map[string]int32),
		DbName:        "test",
	}
	secondaryIdxTableDef.Name2ColIndex[catalog.IndexTableIndexColName] = 0
	secondaryIdxTableDef.Name2ColIndex[catalog.IndexTablePrimaryColName] = 1
	secondaryIdxTableDef.Name2ColIndex[catalog.Row_ID] = 2

	return secondaryIdxObjRef, secondaryIdxTableDef
}

func buildTestCase(
	multiUpdateCtxs []*MultiUpdateCtx,
	eng engine.Engine,
	inputBats []*batch.Batch,
	affectRows uint64,
	action UpdateAction, relResetExpectErr bool) *testCase {

	retCase := &testCase{
		op: &MultiUpdate{
			ctr:                    container{},
			MultiUpdateCtx:         multiUpdateCtxs,
			Action:                 action,
			IsOnduplicateKeyUpdate: false,
			Engine:                 eng,
			OperatorBase: vm.OperatorBase{
				OperatorInfo: vm.OperatorInfo{
					Idx:     0,
					IsFirst: false,
					IsLast:  false,
				},
			},
		},
		inputBatchs:       inputBats,
		expectErr:         false,
		relResetExpectErr: relResetExpectErr,
		affectedRows:      affectRows,
	}

	return retCase
}

func makeTestPkArray(from int64, rowCount int) []int64 {
	val := make([]int64, rowCount)
	for i := 0; i < rowCount; i++ {
		val[i] = from + int64(i)
	}
	return val
}

func makeTestVarcharArray(rowCount int) []string {
	val := make([]string, rowCount)
	for i := 0; i < rowCount; i++ {
		val[i] = strconv.Itoa(i)
	}
	return val
}

func makeTestRowIDVector(m *mpool.MPool, objectID *types.Objectid, blockNum uint16, rowCount int) *vector.Vector {
	blockID := types.NewBlockidWithObjectID(objectID, blockNum+1000)
	vec := vector.NewVec(types.T_Rowid.ToType())
	for i := 0; i < rowCount; i++ {
		rowID := types.NewRowid(&blockID, uint32(i)+1)
		if err := vector.AppendFixed(vec, rowID, false, m); err != nil {
			vec.Free(m)
			return nil
		}
	}
	return vec
}

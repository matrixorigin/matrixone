// Copyright 2021 Matrix Origin
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

package dedupjoin

import (
	"bytes"
	"context"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/hashbuild"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/message"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

const (
	Rows          = 10     // default rows
	BenchmarkRows = 100000 // default rows for benchmark
)

// add unit tests for cases
type joinTestCase struct {
	arg    *DedupJoin
	flgs   []bool // flgs[i] == true: nullable
	types  []types.Type
	proc   *process.Process
	cancel context.CancelFunc
	barg   *hashbuild.HashBuild
}

var (
	tag int32
)

func makeTestCases(t *testing.T) []joinTestCase {
	return []joinTestCase{
		newTestCase(t, []bool{false}, []types.Type{types.T_int32.ToType()}, []int32{0},
			[][]*plan.Expr{
				{
					newExpr(0, types.T_int32.ToType()),
				},
				{
					newExpr(0, types.T_int32.ToType()),
				},
			}),
		newTestCase(t, []bool{true}, []types.Type{types.T_int32.ToType()}, []int32{1},
			[][]*plan.Expr{
				{
					newExpr(0, types.T_int32.ToType()),
				},
				{
					newExpr(0, types.T_int32.ToType()),
				},
			}),
	}
}

func TestString(t *testing.T) {
	buf := new(bytes.Buffer)
	for _, tc := range makeTestCases(t) {
		tc.arg.String(buf)
	}
}

func TestDedupJoin(t *testing.T) {
	for _, tc := range makeTestCases(t) {
		resetChildren(tc.arg)
		resetHashBuildChildren(tc.barg)
		err := tc.arg.Prepare(tc.proc)
		require.NoError(t, err)
		tc.barg.IsDedup = true
		tc.barg.DelColIdx = -1
		err = tc.barg.Prepare(tc.proc)
		require.NoError(t, err)

		res, err := vm.Exec(tc.barg, tc.proc)
		require.NoError(t, err)
		require.Equal(t, res.Batch == nil, true)
		res, err = vm.Exec(tc.barg, tc.proc)
		require.NoError(t, err)
		require.Equal(t, true, res.Batch == nil)

		tc.arg.Reset(tc.proc, false, nil)
		tc.barg.Reset(tc.proc, false, nil)

		resetChildren(tc.arg)
		resetHashBuildChildren(tc.barg)
		tc.proc.GetMessageBoard().Reset()
		err = tc.arg.Prepare(tc.proc)
		require.NoError(t, err)
		tc.barg.IsDedup = true
		err = tc.barg.Prepare(tc.proc)
		require.NoError(t, err)

		res, err = vm.Exec(tc.barg, tc.proc)
		require.NoError(t, err)
		require.Equal(t, res.Batch == nil, true)
		res, err = vm.Exec(tc.barg, tc.proc)
		require.NoError(t, err)
		require.Equal(t, true, res.Batch == nil)

		tc.arg.Reset(tc.proc, false, nil)
		tc.barg.Reset(tc.proc, false, nil)

		tc.arg.Free(tc.proc, false, nil)
		tc.barg.Free(tc.proc, false, nil)

		resetChildren(tc.arg)
		resetHashBuildChildren(tc.barg)
		tc.proc.GetMessageBoard().Reset()
		tc.arg.OnDuplicateAction = plan.Node_IGNORE
		err = tc.arg.Prepare(tc.proc)
		require.NoError(t, err)
		tc.barg.IsDedup = true
		tc.barg.OnDuplicateAction = plan.Node_IGNORE
		err = tc.barg.Prepare(tc.proc)
		require.NoError(t, err)

		res, err = vm.Exec(tc.barg, tc.proc)
		require.NoError(t, err)
		require.Equal(t, res.Batch == nil, true)
		res, err = vm.Exec(tc.barg, tc.proc)
		require.NoError(t, err)
		require.Equal(t, true, res.Batch == nil)

		tc.arg.Reset(tc.proc, false, nil)
		tc.barg.Reset(tc.proc, false, nil)

		tc.arg.Free(tc.proc, false, nil)
		tc.barg.Free(tc.proc, false, nil)

		tc.proc.Free()
		require.Equal(t, int64(0), tc.proc.Mp().CurrNB())
	}
}

/*
	func BenchmarkJoin(b *testing.B) {
		for i := 0; i < b.N; i++ {
			tcs = []joinTestCase{
				newTestCase([]bool{false}, []types.Type{types.T_int8.ToType()}, []int32{0},
					[][]*plan.Expr{
						{
							newExpr(0, types.T_int8.ToType()),
						},
						{
							newExpr(0, types.T_int8.ToType()),
						},
					}),
				newTestCase([]bool{true}, []types.Type{types.T_int8.ToType()}, []int32{0},
					[][]*plan.Expr{
						{
							newExpr(0, types.T_int8.ToType()),
						},
						{
							newExpr(0, types.T_int8.ToType()),
						},
					}),
			}
			t := new(testing.T)
			for _, tc := range tcs {
				bats := hashBuild(t, tc)
				err := tc.arg.Prepare(tc.proc)
				require.NoError(t, err)
				tc.proc.Reg.MergeReceivers[0].Ch <- testutil.NewRegMsg(newBatch(tc.types, tc.proc, Rows))
				tc.proc.Reg.MergeReceivers[0].Ch <- testutil.NewRegMsg(batch.EmptyBatch)
				tc.proc.Reg.MergeReceivers[0].Ch <- testutil.NewRegMsg(newBatch(tc.types, tc.proc, Rows))
				tc.proc.Reg.MergeReceivers[0].Ch <- testutil.NewRegMsg(newBatch(tc.types, tc.proc, Rows))
				tc.proc.Reg.MergeReceivers[0].Ch <- testutil.NewRegMsg(newBatch(tc.types, tc.proc, Rows))
				tc.proc.Reg.MergeReceivers[0].Ch <- nil
				tc.proc.Reg.MergeReceivers[1].Ch <- testutil.NewRegMsg(bats[0])
				tc.proc.Reg.MergeReceivers[1].Ch <- testutil.NewRegMsg(bats[1])
				for {
					ok, err := tc.arg.Call(tc.proc)
					if ok.Status == vm.ExecStop || err != nil {
						break
					}
				}
			}
		}
	}
*/
func newExpr(pos int32, typ types.Type) *plan.Expr {
	return &plan.Expr{
		Typ: plan.Type{
			Scale: typ.Scale,
			Width: typ.Width,
			Id:    int32(typ.Oid),
		},
		Expr: &plan.Expr_Col{
			Col: &plan.ColRef{
				ColPos: pos,
			},
		},
	}
}

func newRelExpr(relPos, colPos int32, typ types.Type) *plan.Expr {
	return &plan.Expr{
		Typ: plan.Type{
			Scale: typ.Scale,
			Width: typ.Width,
			Id:    int32(typ.Oid),
		},
		Expr: &plan.Expr_Col{
			Col: &plan.ColRef{
				RelPos: relPos,
				ColPos: colPos,
			},
		},
	}
}

func newTestCase(t *testing.T, flgs []bool, ts []types.Type, rp []int32, cs [][]*plan.Expr) joinTestCase {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	proc.SetMessageBoard(message.NewMessageBoard())
	_, cancel := context.WithCancel(context.Background())
	//args := make([]*plan.Expr, 0, 2)
	//args = append(args, &plan.Expr{
	//	Typ: plan.Type{
	//		Id: int32(ts[0].Oid),
	//	},
	//	Expr: &plan.Expr_Col{
	//		Col: &plan.ColRef{
	//			RelPos: 0,
	//			ColPos: 0,
	//		},
	//	},
	//})
	//args = append(args, &plan.Expr{
	//	Typ: plan.Type{
	//		Id: int32(ts[0].Oid),
	//	},
	//	Expr: &plan.Expr_Col{
	//		Col: &plan.ColRef{
	//			RelPos: 1,
	//			ColPos: 0,
	//		},
	//	},
	//})
	tag++
	return joinTestCase{
		types:  ts,
		flgs:   flgs,
		proc:   proc,
		cancel: cancel,
		arg: &DedupJoin{
			RightTypes: ts,
			Conditions: cs,
			OperatorBase: vm.OperatorBase{
				OperatorInfo: vm.OperatorInfo{
					Idx:     0,
					IsFirst: false,
					IsLast:  false,
				},
			},
			JoinMapTag: tag,
		},
		barg: &hashbuild.HashBuild{
			NeedHashMap: true,
			Conditions:  cs[1],
			OperatorBase: vm.OperatorBase{
				OperatorInfo: vm.OperatorInfo{
					Idx:     0,
					IsFirst: false,
					IsLast:  false,
				},
			},
			NeedAllocateSels: true,
			JoinMapTag:       tag,
			JoinMapRefCnt:    1,
		},
	}
}

func resetChildren(arg *DedupJoin) {
	bat := colexec.MakeMockBatchs()
	op := colexec.NewMockOperator().WithBatchs([]*batch.Batch{bat})
	arg.Children = nil
	arg.AppendChild(op)
}

func resetHashBuildChildren(arg *hashbuild.HashBuild) {
	bat := colexec.MakeMockBatchs()
	op := colexec.NewMockOperator().WithBatchs([]*batch.Batch{bat})
	arg.Children = nil
	arg.AppendChild(op)
}

func TestCheckDuplicateKeysInRange_NoConflict(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	proc := testutil.NewProcess(t)
	rel := mock_frontend.NewMockRelation(ctrl)
	keyVec := testutil.MakeInt32Vector([]int32{1}, nil)
	keyBat := batch.NewWithSize(1)
	keyBat.Vecs[0] = keyVec
	keyBat.SetRowCount(1)

	rel.EXPECT().PrimaryKeysMayBeUpserted(gomock.Any(), gomock.Any(), gomock.Any(), keyBat, int32(0)).Return(false, nil)

	rowStr, err := checkDuplicateKeysInRange(
		proc,
		rel,
		types.BuildTS(1, 0),
		types.BuildTS(2, 0),
		keyBat,
		keyVec,
		"id",
		[]plan.Type{{Id: int32(types.T_int32)}},
	)
	require.NoError(t, err)
	require.Empty(t, rowStr)
}

func TestCheckDuplicateKeysInRange_ReturnsActualDuplicateRow(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	proc := testutil.NewProcess(t)
	rel := mock_frontend.NewMockRelation(ctrl)
	keyVec := testutil.MakeInt32Vector([]int32{1, 50, 3}, nil)
	keyBat := batch.NewWithSize(1)
	keyBat.Vecs[0] = keyVec
	keyBat.SetRowCount(3)

	gomock.InOrder(
		rel.EXPECT().PrimaryKeysMayBeUpserted(gomock.Any(), gomock.Any(), gomock.Any(), keyBat, int32(0)).Return(true, nil),
		rel.EXPECT().PrimaryKeysMayBeUpserted(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), int32(0)).DoAndReturn(
			func(_ context.Context, _, _ types.TS, bat *batch.Batch, _ int32) (bool, error) {
				return bat.GetVector(0).RowToString(0) == "50", nil
			},
		).Times(2),
	)

	rowStr, err := checkDuplicateKeysInRange(
		proc,
		rel,
		types.BuildTS(1, 0),
		types.BuildTS(2, 0),
		keyBat,
		keyVec,
		"id",
		[]plan.Type{{Id: int32(types.T_int32)}},
	)
	require.NoError(t, err)
	require.Equal(t, "50", rowStr)
}

func TestCheckSnapshotAdvancedDuplicates_SkipsOldKeyDedupPaths(t *testing.T) {
	proc := testutil.NewProcess(t)
	arg := &DedupJoin{
		OnDuplicateAction: plan.Node_FAIL,
		TargetTableID:     42,
		DelColIdx:         0,
	}
	arg.ctr.batchRowCount = 1

	err := arg.checkSnapshotAdvancedDuplicates(proc)
	require.NoError(t, err)
}

func TestCheckSnapshotAdvancedDuplicates_SkipsWithoutSnapshotAdvance(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	proc := testutil.NewProcess(t)
	txnOp := mock_frontend.NewMockTxnOperator(ctrl)
	initialTS := timestamp.Timestamp{PhysicalTime: 3}
	proc.Base.TxnOperator = txnOp

	arg := &DedupJoin{
		OnDuplicateAction: plan.Node_FAIL,
		TargetTableID:     42,
		InitialSnapshotTS: initialTS,
		DedupColName:      "id",
		DedupColTypes:     []plan.Type{{Id: int32(types.T_int32)}},
		DelColIdx:         -1,
		Conditions: [][]*plan.Expr{
			nil,
			{newRelExpr(1, 0, types.T_int32.ToType())},
		},
	}
	arg.ctr.batchRowCount = 1

	txnOp.EXPECT().SnapshotTS().Return(initialTS)

	err := arg.checkSnapshotAdvancedDuplicates(proc)
	require.NoError(t, err)
}

func TestCheckSnapshotAdvancedDuplicates_SkipsWithoutEngine(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	proc := testutil.NewProcess(t)
	txnOp := mock_frontend.NewMockTxnOperator(ctrl)
	initialTS := timestamp.Timestamp{PhysicalTime: 3}
	currentTS := timestamp.Timestamp{PhysicalTime: 4}
	proc.Base.TxnOperator = txnOp

	arg := &DedupJoin{
		OnDuplicateAction: plan.Node_FAIL,
		TargetTableID:     42,
		InitialSnapshotTS: initialTS,
		DedupColName:      "id",
		DedupColTypes:     []plan.Type{{Id: int32(types.T_int32)}},
		DelColIdx:         -1,
		Conditions: [][]*plan.Expr{
			nil,
			{newRelExpr(1, 0, types.T_int32.ToType())},
		},
	}
	arg.ctr.batchRowCount = 1

	txnOp.EXPECT().SnapshotTS().Return(currentTS)

	err := arg.checkSnapshotAdvancedDuplicates(proc)
	require.NoError(t, err)
}

func TestCheckSnapshotAdvancedDuplicates_ReturnsDuplicate(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	proc := testutil.NewProcess(t)
	txnOp := mock_frontend.NewMockTxnOperator(ctrl)
	eng := mock_frontend.NewMockEngine(ctrl)
	rel := mock_frontend.NewMockRelation(ctrl)

	initialTS := timestamp.Timestamp{PhysicalTime: 1}
	currentTS := timestamp.Timestamp{PhysicalTime: 2}
	proc.Base.TxnOperator = txnOp
	proc.GetSessionInfo().StorageEngine = eng

	buildBat := batch.NewWithSize(1)
	buildBat.Vecs[0] = testutil.MakeInt32Vector([]int32{10, 20}, nil)
	buildBat.SetRowCount(2)

	arg := &DedupJoin{
		OnDuplicateAction: plan.Node_FAIL,
		TargetTableID:     42,
		InitialSnapshotTS: initialTS,
		DedupColName:      "id",
		DedupColTypes:     []plan.Type{{Id: int32(types.T_int32)}},
		DelColIdx:         -1,
		Conditions: [][]*plan.Expr{
			nil,
			{newRelExpr(1, 0, types.T_int32.ToType())},
		},
	}
	arg.ctr.batches = []*batch.Batch{buildBat}
	arg.ctr.batchRowCount = int64(buildBat.RowCount())

	gomock.InOrder(
		txnOp.EXPECT().SnapshotTS().Return(currentTS),
		eng.EXPECT().GetRelationById(gomock.Any(), txnOp, uint64(42)).Return("testdb", "t1", rel, nil),
		rel.EXPECT().GetTableID(gomock.Any()).Return(uint64(42)),
		rel.EXPECT().PrimaryKeysMayBeUpserted(gomock.Any(), types.BuildTS(1, 0), types.BuildTS(2, 0), gomock.Any(), int32(0)).DoAndReturn(
			func(_ context.Context, _, _ types.TS, bat *batch.Batch, _ int32) (bool, error) {
				require.Equal(t, 2, bat.RowCount())
				return true, nil
			},
		),
		rel.EXPECT().PrimaryKeysMayBeUpserted(gomock.Any(), types.BuildTS(1, 0), types.BuildTS(2, 0), gomock.Any(), int32(0)).DoAndReturn(
			func(_ context.Context, _, _ types.TS, bat *batch.Batch, _ int32) (bool, error) {
				require.Equal(t, 1, bat.RowCount())
				return bat.GetVector(0).RowToString(0) == "10", nil
			},
		),
	)

	err := arg.checkSnapshotAdvancedDuplicates(proc)
	require.Error(t, err)
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrDuplicateEntry))
	require.Contains(t, err.Error(), "Duplicate entry '10' for key 'id'")
}

func TestResolveTargetRelation_DetectsDefinitionChange(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	proc := testutil.NewProcess(t)
	txnOp := mock_frontend.NewMockTxnOperator(ctrl)
	eng := mock_frontend.NewMockEngine(ctrl)
	db := mock_frontend.NewMockDatabase(ctrl)
	rel := mock_frontend.NewMockRelation(ctrl)
	proc.Base.TxnOperator = txnOp

	arg := &DedupJoin{
		TargetTableRef: &plan.ObjectRef{
			SchemaName: "testdb",
			ObjName:    "t1",
		},
		TargetTableID: 42,
	}

	gomock.InOrder(
		eng.EXPECT().Database(gomock.Any(), "testdb", txnOp).Return(db, nil),
		db.EXPECT().Relation(gomock.Any(), "t1", proc).Return(rel, nil),
		rel.EXPECT().GetTableID(gomock.Any()).Return(uint64(99)),
	)

	resolvedRel, resolvedID, err := arg.resolveTargetRelation(proc, eng)
	require.Error(t, err)
	require.Nil(t, resolvedRel)
	require.Equal(t, uint64(99), resolvedID)
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrTxnNeedRetryWithDefChanged))
}

func TestFormatDedupRow_IndexTableTuple(t *testing.T) {
	proc := testutil.NewProcess(t)
	vec := vector.NewVec(types.T_varchar.ToType())
	packer := types.NewPacker()
	defer packer.Close()

	packer.EncodeInt32(7)
	packer.EncodeStringType([]byte("abc"))
	require.NoError(t, vector.AppendBytes(vec, packer.GetBuf(), false, proc.Mp()))

	rowStr, err := formatDedupRow(
		vec,
		0,
		catalog.IndexTableIndexColName,
		[]plan.Type{{Id: int32(types.T_varchar)}},
	)
	require.NoError(t, err)
	require.Equal(t, "(7,abc)", rowStr)
}

func TestFormatDedupRow_CompositeTuple(t *testing.T) {
	proc := testutil.NewProcess(t)
	vec := vector.NewVec(types.T_varchar.ToType())
	packer := types.NewPacker()
	defer packer.Close()

	packer.EncodeInt32(8)
	packer.EncodeStringType([]byte("xyz"))
	require.NoError(t, vector.AppendBytes(vec, packer.GetBuf(), false, proc.Mp()))

	rowStr, err := formatDedupRow(
		vec,
		0,
		"pk",
		[]plan.Type{
			{Id: int32(types.T_int32)},
			{Id: int32(types.T_varchar)},
		},
	)
	require.NoError(t, err)
	require.Equal(t, "(8,xyz)", rowStr)
}

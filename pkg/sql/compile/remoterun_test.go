// Copyright 2023 Matrix Origin
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

package compile

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/google/uuid"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/morpc"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/common/reuse"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	"github.com/matrixorigin/matrixone/pkg/pb/pipeline"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/aggexec"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/apply"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/connector"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/dedupjoin"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/deletion"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/dispatch"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/external"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/filter"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/fuzzyfilter"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/group"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/hashbuild"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/hashjoin"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/indexbuild"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/insert"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/intersect"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/intersectall"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/limit"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/lockop"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/loopjoin"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/merge"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/mergeorder"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/mergerecursive"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/mergetop"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/minus"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/multi_update"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/offset"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/order"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/postdml"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/preinsert"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/preinsertunique"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/product"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/projection"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/shuffle"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/source"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/table_function"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/top"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/value_scan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

func makeRemoteBatchMessage(t *testing.T, bat *batch.Batch) morpc.Message {
	t.Helper()
	data, err := bat.MarshalBinary()
	require.NoError(t, err)
	return &pipeline.Message{
		Sid:  pipeline.Status_Last,
		Data: data,
	}
}

func Test_EncodeProcessInfo(t *testing.T) {
	ctrl := gomock.NewController(t)
	txnOperator := mock_frontend.NewMockTxnOperator(ctrl)
	txnOperator.EXPECT().Snapshot().AnyTimes()

	proc := process.NewTopProcess(defines.AttachAccountId(context.TODO(), catalog.System_Account),
		nil,
		nil,
		txnOperator,
		nil,
		nil,
		nil,
		nil,
		nil,
		nil,
		nil)
	proc.Base.Id = "1"
	proc.Base.Lim = process.Limitation{}
	proc.Base.UnixTime = 1000000
	proc.Base.SessionInfo = process.SessionInfo{
		Account:      "",
		User:         "",
		Host:         "",
		Role:         "",
		ConnectionID: 0,
		LastInsertID: 0,
		Database:     "",
		Version:      "",
		// Pin to UTC: time.Time{}.In(time.Local).MarshalBinary() can fail
		// on hosts whose historical zone data for year 1 has an offset
		// outside the int16 minute range MarshalBinary accepts.
		TimeZone:       time.UTC,
		StorageEngine:  nil,
		QueryId:        nil,
		ResultColTypes: nil,
		SeqCurValues:   nil,
		SeqDeleteKeys:  nil,
		SeqAddValues:   nil,
		SeqLastValue:   nil,
		SqlHelper:      nil,
	}

	_, err := encodeProcessInfo(proc, "")
	require.Nil(t, err)
}

func Test_refactorScope(t *testing.T) {
	ctx := context.TODO()
	proc := &process.Process{}
	proc.Base = &process.BaseProcess{}

	s := reuse.Alloc[Scope](nil)
	s.Proc = proc
	c := reuse.Alloc[Compile](nil)
	c.anal = newAnalyzeModule()
	c.proc = proc
	c.proc.Ctx = ctx
	rs := appendWriteBackOperator(c, s)
	require.Equal(t, vm.GetLeafOpParent(nil, rs.RootOp).GetOperatorBase().Idx, -1)
}

func Test_convertPipelineUuid(t *testing.T) {
	id, _ := uuid.NewV7()
	p := &pipeline.Pipeline{
		UuidsToRegIdx: []*pipeline.UuidToRegIdx{
			{Idx: 1, Uuid: id[:]},
		},
	}
	s := reuse.Alloc[Scope](nil)
	s.RemoteReceivRegInfos = make([]RemoteReceivRegInfo, 0)
	err := convertPipelineUuid(p, s)
	require.Nil(t, err)
}

func Test_convertScopeRemoteReceivInfo(t *testing.T) {
	id, _ := uuid.NewV7()
	s := reuse.Alloc[Scope](nil)
	s.RemoteReceivRegInfos = []RemoteReceivRegInfo{
		{Idx: 1, Uuid: id},
	}
	ret := convertScopeRemoteReceivInfo(s)
	require.Equal(t, ret[0].Idx, int32(1))
}

func Test_convertToPipelineInstruction(t *testing.T) {
	exParam := external.ExParam{
		Filter: &external.FilterParam{},
	}
	ops := []vm.Operator{
		&insert.Insert{
			InsertCtx: &insert.InsertCtx{},
		},
		&deletion.Deletion{
			DeleteCtx: &deletion.DeleteCtx{},
		},
		&preinsert.PreInsert{},
		&lockop.LockOp{},
		&preinsertunique.PreInsertUnique{},
		&shuffle.Shuffle{},
		&dispatch.Dispatch{},
		&group.Group{},
		&hashjoin.HashJoin{
			EqConds: [][]*plan.Expr{nil, nil},
		},
		&limit.Limit{},
		&loopjoin.LoopJoin{},
		&offset.Offset{},
		&order.Order{},
		&product.Product{},
		&projection.Projection{},
		&filter.Filter{},
		&top.Top{},
		&intersect.Intersect{},
		&minus.Minus{},
		&intersectall.IntersectAll{},
		&merge.Merge{},
		&mergerecursive.MergeRecursive{},
		&group.MergeGroup{},
		&mergetop.MergeTop{},
		&mergeorder.MergeOrder{},
		&table_function.TableFunction{},
		&external.External{
			Es: &external.ExternalParam{
				ExParam: exParam,
			},
		},
		&hashbuild.HashBuild{},
		&indexbuild.IndexBuild{},
		&source.Source{},
		&apply.Apply{TableFunction: &table_function.TableFunction{}},
		&postdml.PostDml{
			PostDmlCtx: &postdml.PostDmlCtx{
				FullText: &postdml.PostDmlFullTextCtx{},
			},
		},
		&dedupjoin.DedupJoin{
			Conditions: [][]*plan.Expr{nil, nil},
		},
	}
	ctx := &scopeContext{
		id:       1,
		plan:     nil,
		scope:    nil,
		root:     &scopeContext{},
		parent:   &scopeContext{},
		children: nil,
		pipe:     nil,
		regs:     nil,
	}

	proc := &process.Process{}
	proc.Base = &process.BaseProcess{}
	for _, op := range ops {
		_, _, err := convertToPipelineInstruction(op, proc, ctx, 1)
		require.Nil(t, err)
	}
}

func Test_convertToVmInstruction(t *testing.T) {
	ctx := &scopeContext{
		id:       1,
		plan:     nil,
		scope:    nil,
		root:     &scopeContext{},
		parent:   &scopeContext{},
		children: nil,
		pipe:     nil,
		regs:     nil,
	}
	instructions := []*pipeline.Instruction{
		{Op: int32(vm.Deletion), Delete: &pipeline.Deletion{}},
		{Op: int32(vm.Insert), Insert: &pipeline.Insert{}},
		{Op: int32(vm.PreInsert), PreInsert: &pipeline.PreInsert{}},
		{Op: int32(vm.LockOp), LockOp: &pipeline.LockOp{}},
		{Op: int32(vm.PreInsertUnique), PreInsertUnique: &pipeline.PreInsertUnique{}},
		{Op: int32(vm.Shuffle), Shuffle: &pipeline.Shuffle{}},
		{Op: int32(vm.Dispatch), Dispatch: &pipeline.Dispatch{}},
		{Op: int32(vm.Group), Agg: &pipeline.Group{}},
		{Op: int32(vm.HashJoin), HashJoin: &pipeline.HashJoin{}},
		{Op: int32(vm.Limit), Limit: plan.MakePlan2Int64ConstExprWithType(1)},
		{Op: int32(vm.LoopJoin), LoopJoin: &pipeline.LoopJoin{}},
		{Op: int32(vm.Offset), Offset: plan.MakePlan2Int64ConstExprWithType(0)},
		{Op: int32(vm.Order), OrderBy: []*plan.OrderBySpec{}},
		{Op: int32(vm.Product), Product: &pipeline.Product{}},
		{Op: int32(vm.ProductL2), ProductL2: &pipeline.ProductL2{}},
		{Op: int32(vm.Projection), ProjectList: []*plan.Expr{}},
		{Op: int32(vm.Filter), Filters: []*plan.Expr{}, RuntimeFilters: []*plan.Expr{}},
		{Op: int32(vm.Top), Limit: plan.MakePlan2Int64ConstExprWithType(1)},
		{Op: int32(vm.Intersect), SetOp: &pipeline.SetOp{}},
		{Op: int32(vm.IntersectAll), SetOp: &pipeline.SetOp{}},
		{Op: int32(vm.Minus), SetOp: &pipeline.SetOp{}},
		{Op: int32(vm.Connector), Connect: &pipeline.Connector{}},
		{Op: int32(vm.Merge), Merge: &pipeline.Merge{}},
		{Op: int32(vm.MergeRecursive)},
		{Op: int32(vm.MergeGroup), Agg: &pipeline.Group{}},
		{Op: int32(vm.MergeTop), Limit: plan.MakePlan2Int64ConstExprWithType(1)},
		{Op: int32(vm.MergeOrder), OrderBy: []*plan.OrderBySpec{}},
		{Op: int32(vm.TableFunction), TableFunction: &pipeline.TableFunction{}},
		{Op: int32(vm.HashBuild), HashBuild: &pipeline.HashBuild{}},
		{Op: int32(vm.External), ExternalScan: &pipeline.ExternalScan{}},
		{Op: int32(vm.Source), StreamScan: &pipeline.StreamScan{}},
		{Op: int32(vm.IndexBuild), IndexBuild: &pipeline.Indexbuild{}},
		{Op: int32(vm.Apply), Apply: &pipeline.Apply{}, TableFunction: &pipeline.TableFunction{}},
		{Op: int32(vm.PostDml), PostDml: &pipeline.PostDml{}},
		{Op: int32(vm.DedupJoin), DedupJoin: &pipeline.DedupJoin{}},
		{Op: int32(vm.RightDedupJoin), RightDedupJoin: &pipeline.RightDedupJoin{}},
	}
	for _, instruction := range instructions {
		_, err := convertToVmOperator(instruction, ctx, nil)
		require.Nil(t, err)
	}
}

func TestExternalScanParquetRowGroupShardsRoundtrip(t *testing.T) {
	ctx := &scopeContext{
		id:     1,
		root:   &scopeContext{},
		parent: &scopeContext{},
	}
	proc := &process.Process{}
	proc.Base = &process.BaseProcess{}

	shards := []*pipeline.ParquetRowGroupShard{
		{
			FileIndex:     2,
			RowGroupStart: 3,
			RowGroupEnd:   5,
			NumRows:       1024,
			Bytes:         4096,
		},
	}
	op := external.NewArgument().WithEs(
		&external.ExternalParam{
			ExParamConst: external.ExParamConst{
				FileList:              []string{"s3://bucket/part.parquet"},
				FileSize:              []int64{8192},
				FileOffsetTotal:       []*pipeline.FileOffset{{Offset: []int64{0, -1}}},
				ParquetRowGroupShards: shards,
			},
			ExParam: external.ExParam{
				Fileparam: &external.ExFileparam{},
				Filter:    &external.FilterParam{},
			},
		},
	)

	_, pipeInstr, err := convertToPipelineInstruction(op, proc, ctx, 1)
	require.NoError(t, err)
	require.Equal(t, shards, pipeInstr.ExternalScan.ParquetRowGroupShards)

	restored, err := convertToVmOperator(pipeInstr, ctx, nil)
	require.NoError(t, err)
	restoredExternal := restored.(*external.External)
	require.Equal(t, shards, restoredExternal.Es.ParquetRowGroupShards)
}

func Test_DMLOperatorSerializationRoundtrip(t *testing.T) {
	ctx := &scopeContext{
		id:     1,
		root:   &scopeContext{},
		parent: &scopeContext{},
	}
	proc := &process.Process{}
	proc.Base = &process.BaseProcess{}

	t.Run("FuzzyFilter_BuildIdx", func(t *testing.T) {
		op := &fuzzyfilter.FuzzyFilter{
			N:        42.5,
			PkName:   "pk",
			BuildIdx: 7,
		}
		_, pipeInstr, err := convertToPipelineInstruction(op, proc, ctx, 1)
		require.NoError(t, err)
		require.Equal(t, int32(7), pipeInstr.FuzzyFilter.BuildIdx)

		restored, err := convertToVmOperator(pipeInstr, ctx, nil)
		require.NoError(t, err)
		restoredOp := restored.(*fuzzyfilter.FuzzyFilter)
		require.Equal(t, 7, restoredOp.BuildIdx)
		require.Equal(t, "pk", restoredOp.PkName)
	})

	t.Run("MultiUpdate_PartitionCols", func(t *testing.T) {
		op := &multi_update.MultiUpdate{
			MultiUpdateCtx: []*multi_update.MultiUpdateCtx{
				{
					ObjRef:         &plan.ObjectRef{ObjName: "t1"},
					TableDef:       &plan.TableDef{Name: "t1"},
					InsertCols:     []int{0, 1, 2},
					DeleteCols:     []int{3, 4},
					PartitionCols:  []int{5, 6},
					InsertPkColIdx: 1,
				},
			},
			Action: multi_update.UpdateWriteTable,
		}
		_, pipeInstr, err := convertToPipelineInstruction(op, proc, ctx, 1)
		require.NoError(t, err)
		require.Len(t, pipeInstr.MultiUpdate.UpdateCtxList[0].PartitionCols, 2)

		restored, err := convertToVmOperator(pipeInstr, ctx, nil)
		require.NoError(t, err)
		restoredOp := restored.(*multi_update.MultiUpdate)
		require.Equal(t, []int{5, 6}, restoredOp.MultiUpdateCtx[0].PartitionCols)
		require.Equal(t, []int{0, 1, 2}, restoredOp.MultiUpdateCtx[0].InsertCols)
		require.Equal(t, []int{3, 4}, restoredOp.MultiUpdateCtx[0].DeleteCols)
		require.Equal(t, 1, restoredOp.MultiUpdateCtx[0].InsertPkColIdx)
		require.True(t, restoredOp.IsRemote)
	})

	t.Run("DedupJoin_DedupBuildKeepLast", func(t *testing.T) {
		op := &dedupjoin.DedupJoin{
			Conditions:         [][]*plan.Expr{nil, nil},
			DedupBuildKeepLast: true,
		}
		_, pipeInstr, err := convertToPipelineInstruction(op, proc, ctx, 1)
		require.NoError(t, err)
		require.True(t, pipeInstr.DedupJoin.DedupBuildKeepLast)

		restored, err := convertToVmOperator(pipeInstr, ctx, nil)
		require.NoError(t, err)
		require.True(t, restored.(*dedupjoin.DedupJoin).DedupBuildKeepLast)
	})

	t.Run("MergeOrder_SpillThreshold", func(t *testing.T) {
		op := &mergeorder.MergeOrder{
			OrderBySpecs:   []*planpb.OrderBySpec{{Flag: planpb.OrderBySpec_DESC}},
			SpillThreshold: 4096,
		}
		_, pipeInstr, err := convertToPipelineInstruction(op, proc, ctx, 1)
		require.NoError(t, err)
		require.Equal(t, int64(4096), pipeInstr.SpillMem)
		require.Len(t, pipeInstr.OrderBy, 1)

		restored, err := convertToVmOperator(pipeInstr, ctx, nil)
		require.NoError(t, err)
		restoredOp := restored.(*mergeorder.MergeOrder)
		require.Equal(t, int64(4096), restoredOp.SpillThreshold)
		require.Len(t, restoredOp.OrderBySpecs, 1)
		require.Equal(t, planpb.OrderBySpec_DESC, restoredOp.OrderBySpecs[0].Flag)
	})

	t.Run("Deletion_Engine", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		op := &deletion.Deletion{
			DeleteCtx: &deletion.DeleteCtx{
				RowIdIdx:      2,
				PrimaryKeyIdx: 0,
				Ref:           &plan.ObjectRef{ObjName: "t1"},
			},
		}
		_, pipeInstr, err := convertToPipelineInstruction(op, proc, ctx, 1)
		require.NoError(t, err)

		mockEng := mock_frontend.NewMockEngine(ctrl)
		restored, err := convertToVmOperator(pipeInstr, ctx, mockEng)
		require.NoError(t, err)
		restoredOp := restored.(*deletion.Deletion)
		require.Equal(t, mockEng, restoredOp.DeleteCtx.Engine)
		require.Equal(t, 2, restoredOp.DeleteCtx.RowIdIdx)
	})

	t.Run("Deletion_CanTruncate", func(t *testing.T) {
		op := &deletion.Deletion{
			DeleteCtx: &deletion.DeleteCtx{
				CanTruncate:     true,
				RowIdIdx:        1,
				PrimaryKeyIdx:   0,
				AddAffectedRows: true,
				Ref:             &plan.ObjectRef{ObjName: "t1"},
			},
		}
		_, pipeInstr, err := convertToPipelineInstruction(op, proc, ctx, 1)
		require.NoError(t, err)
		require.True(t, pipeInstr.Delete.CanTruncate)

		restored, err := convertToVmOperator(pipeInstr, ctx, nil)
		require.NoError(t, err)
		restoredOp := restored.(*deletion.Deletion)
		require.True(t, restoredOp.DeleteCtx.CanTruncate)
		require.Equal(t, 1, restoredOp.DeleteCtx.RowIdIdx)
		require.Equal(t, 0, restoredOp.DeleteCtx.PrimaryKeyIdx)
		require.True(t, restoredOp.DeleteCtx.AddAffectedRows)
	})

	t.Run("Insert_Engine", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		op := &insert.Insert{
			InsertCtx: &insert.InsertCtx{
				Ref:      &plan.ObjectRef{ObjName: "t1"},
				TableDef: &plan.TableDef{Name: "t1"},
				Attrs:    []string{"a", "b"},
			},
		}
		_, pipeInstr, err := convertToPipelineInstruction(op, proc, ctx, 1)
		require.NoError(t, err)

		mockEng := mock_frontend.NewMockEngine(ctrl)
		restored, err := convertToVmOperator(pipeInstr, ctx, mockEng)
		require.NoError(t, err)
		restoredOp := restored.(*insert.Insert)
		require.Equal(t, mockEng, restoredOp.InsertCtx.Engine)
		require.Equal(t, []string{"a", "b"}, restoredOp.InsertCtx.Attrs)
	})

	t.Run("MultiUpdate_Engine", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		op := &multi_update.MultiUpdate{
			MultiUpdateCtx: []*multi_update.MultiUpdateCtx{
				{
					ObjRef:   &plan.ObjectRef{ObjName: "t1"},
					TableDef: &plan.TableDef{Name: "t1"},
				},
			},
		}
		_, pipeInstr, err := convertToPipelineInstruction(op, proc, ctx, 1)
		require.NoError(t, err)

		mockEng := mock_frontend.NewMockEngine(ctrl)
		restored, err := convertToVmOperator(pipeInstr, ctx, mockEng)
		require.NoError(t, err)
		restoredOp := restored.(*multi_update.MultiUpdate)
		require.Equal(t, mockEng, restoredOp.Engine)
	})
}
func Test_convertToProcessLimitation(t *testing.T) {
	lim := pipeline.ProcessLimitation{
		Size: 100,
	}
	limitation := process.ConvertToProcessLimitation(lim)
	require.Equal(t, limitation.Size, int64(100))
}

func Test_convertToProcessSessionInfo(t *testing.T) {
	ti, _ := time.Now().MarshalBinary()
	sei := pipeline.SessionInfo{
		TimeZone: ti,
	}
	_, err := process.ConvertToProcessSessionInfo(sei)
	require.Nil(t, err)
}

func Test_decodeBatch(t *testing.T) {
	mp := &mpool.MPool{}
	aggexec.RegisterGroupConcatAgg(0, ",")
	bat := &batch.Batch{
		Recursive:  0,
		ShuffleIDX: 0,
		Attrs:      []string{"1"},
		Vecs:       []*vector.Vector{vector.NewVec(types.T_int64.ToType())},
	}
	bat.SetRowCount(1)
	data, err := types.Encode(bat)
	require.Nil(t, err)
	_, err = decodeBatch(mp, data)
	require.Nil(t, err)
}

func Test_GetProcByUuid(t *testing.T) {
	_ = colexec.NewServer(nil)

	{
		// first get action or deletion just convert the k-v to be `ready to remove` status.
		// and the next action will remove it.
		uid, err := uuid.NewV7()
		require.Nil(t, err)

		receiver := &messageReceiverOnServer{
			connectionCtx: context.TODO(),
		}

		p0 := &process.Process{}
		c0 := process.RemotePipelineInformationChannel(make(chan *process.WrapCs))
		require.NoError(t, colexec.Get().PutProcIntoUuidMap(uid, p0, c0))

		// this action will convert it to be ready-to-remove status.
		colexec.Get().DeleteUuids([]uuid.UUID{uid})

		// get a nil p and c.
		p, c, err := receiver.GetProcByUuid(uid)
		require.Nil(t, err)
		require.Nil(t, p)
		require.Nil(t, c)

		colexec.Get().DeleteUuids([]uuid.UUID{uid})
	}

	{
		// if receiver done, get method should exit.
		// 1. return nil.
		// 2. no need to return error.
		cctx, ccancel := context.WithCancel(context.Background())
		receiver := &messageReceiverOnServer{
			connectionCtx: cctx,
		}
		ccancel()
		p, _, err := receiver.GetProcByUuid(uuid.UUID{})
		require.Nil(t, err)
		require.Nil(t, p)

		// two action to delete the uuid can make sure the producer and consumer flag uuid done.
		colexec.Get().DeleteUuids([]uuid.UUID{{}})
		colexec.Get().DeleteUuids([]uuid.UUID{{}})
	}

	{
		// test get succeed.
		uid, err := uuid.NewV7()
		require.Nil(t, err)

		receiver := &messageReceiverOnServer{
			connectionCtx: context.TODO(),
		}

		p0 := &process.Process{}
		c0 := process.RemotePipelineInformationChannel(make(chan *process.WrapCs))
		require.NoError(t, colexec.Get().PutProcIntoUuidMap(uid, p0, c0))

		p, c, err := receiver.GetProcByUuid(uid)
		require.Nil(t, err)
		require.Equal(t, p0, p)
		require.Equal(t, c0, c)

		colexec.Get().DeleteUuids([]uuid.UUID{uid})
		colexec.Get().DeleteUuids([]uuid.UUID{uid})
	}

	{
		// test if receiver done first, put action should return error.
		colexec.Get().GetProcByUuid(uuid.UUID{}, true)
		err := colexec.Get().PutProcIntoUuidMap(uuid.UUID{}, nil, nil)
		require.NotNil(t, err)

		colexec.Get().DeleteUuids([]uuid.UUID{{}})
		colexec.Get().DeleteUuids([]uuid.UUID{{}})
	}
}

func Test_GetProcByUuid_ConcurrentWake(t *testing.T) {
	_ = colexec.NewServer(nil)

	uid, err := uuid.NewV7()
	require.Nil(t, err)

	receiver := &messageReceiverOnServer{
		connectionCtx: context.TODO(),
		messageCtx:    context.TODO(),
	}

	// Start GetProcByUuid in a goroutine BEFORE PutProcIntoUuidMap.
	// This tests the wait-then-wake path: the receiver must block on the
	// changed channel and wake exactly once when the UUID is inserted.
	type result struct {
		proc *process.Process
		ch   process.RemotePipelineInformationChannel
		err  error
	}
	done := make(chan result, 1)
	go func() {
		p, c, e := receiver.GetProcByUuid(uid)
		done <- result{p, c, e}
	}()

	// Give the goroutine time to enter the wait.
	time.Sleep(10 * time.Millisecond)

	p0 := &process.Process{}
	c0 := process.RemotePipelineInformationChannel(make(chan *process.WrapCs))
	require.NoError(t, colexec.Get().PutProcIntoUuidMap(uid, p0, c0))

	select {
	case r := <-done:
		require.Nil(t, r.err)
		require.Equal(t, p0, r.proc)
		require.Equal(t, c0, r.ch)
	case <-time.After(3 * time.Second):
		t.Fatal("GetProcByUuid did not wake after PutProcIntoUuidMap")
	}

	colexec.Get().DeleteUuids([]uuid.UUID{uid})
	colexec.Get().DeleteUuids([]uuid.UUID{uid})
}

var _ morpc.Stream = &fakeStreamSender{}

// fakeStreamSender implement the morpc.Stream interface.
type fakeStreamSender struct {
	// how many packages were sent.
	sentCnt int

	// return it during next send.
	nextSendError error
}

func (s *fakeStreamSender) ID() uint64 { return 0 }
func (s *fakeStreamSender) Send(ctx context.Context, request morpc.Message) error {
	if s.nextSendError == nil {
		s.sentCnt++
	}
	return s.nextSendError
}
func (s *fakeStreamSender) Receive() (chan morpc.Message, error) {
	ch := make(chan morpc.Message, 1)
	ch <- &pipeline.Message{
		Sid: pipeline.Status_MessageEnd,
	}
	return ch, nil
}
func (s *fakeStreamSender) Close(_ bool) error {
	return nil
}

type fakeTxnOperator struct {
	client.TxnOperator
}

func (f fakeTxnOperator) Txn() txn.TxnMeta {
	return txn.TxnMeta{
		ID: []byte("test"),
	}
}

func (f fakeTxnOperator) Snapshot() (txn.CNTxnSnapshot, error) {
	return txn.CNTxnSnapshot{}, nil
}

func Test_prepareRemoteRunSendingData(t *testing.T) {
	proc := testutil.NewProcess(t)
	proc.Ctx = context.WithValue(proc.Ctx, defines.TenantIDKey{}, uint32(0))
	proc.Base.TxnOperator = fakeTxnOperator{}
	// time.Time{}.In(time.Local).MarshalBinary() can fail on hosts where
	// the historical zone data for year 1 has an offset outside the
	// int16 minute range MarshalBinary accepts. Pin to UTC for the test.
	proc.Base.SessionInfo.TimeZone = time.UTC

	// if this is a pipeline with operator list "connector / dispatch".
	// this should return withoutOut == false.
	s1 := &Scope{
		Proc:   proc,
		RootOp: connector.NewArgument(),
	}
	_, withoutOut, _, _, err := prepareRemoteRunSendingData("", s1, proc)
	require.NoError(t, err)
	require.False(t, withoutOut)
	require.NotNil(t, s1.RootOp)
	require.Equal(t, vm.Connector, s1.RootOp.OpType())

	// if this is a pipeline with operator list "scan -> connector / dispatch".
	// this should return withoutOut == false.
	s2 := &Scope{
		Proc:   proc,
		RootOp: dispatch.NewArgument(),
	}
	s2.RootOp.AppendChild(value_scan.NewArgument())
	originChild := s2.RootOp.GetOperatorBase().GetChildren(0)
	_, withoutOut, _, _, err = prepareRemoteRunSendingData("", s2, proc)
	require.NoError(t, err)
	require.False(t, withoutOut)
	require.Equal(t, 1, s2.RootOp.GetOperatorBase().NumChildren())
	require.Same(t, originChild, s2.RootOp.GetOperatorBase().GetChildren(0))

	// if this is a pipeline no need to sent back message, like "scan -> scan".
	// this should return withoutOut == true.
	s3 := &Scope{
		Proc:   proc,
		RootOp: value_scan.NewArgument(),
	}
	s3.RootOp.AppendChild(value_scan.NewArgument())
	_, withoutOut, _, _, err = prepareRemoteRunSendingData("", s3, proc)
	require.NoError(t, err)
	require.True(t, withoutOut)
}

func TestGetScopeForRemoteRunEncodingDoesNotMutateOriginalScope(t *testing.T) {
	root := dispatch.NewArgument()
	child := value_scan.NewArgument()
	root.AppendChild(child)
	s := &Scope{RootOp: root}

	encoded, withoutOutput := getScopeForRemoteRunEncoding(s)

	require.False(t, withoutOutput)
	require.Same(t, root, s.RootOp)
	require.Equal(t, 1, s.RootOp.GetOperatorBase().NumChildren())
	require.Same(t, child, s.RootOp.GetOperatorBase().GetChildren(0))
	require.NotSame(t, s, encoded)
	require.Same(t, child, encoded.RootOp)
}

func TestBuildRemoteDispatchReceiverRootDoesNotMutateOriginalChildren(t *testing.T) {
	origin := dispatch.NewArgument()
	originChild := value_scan.NewArgument()
	fakeChild := value_scan.NewArgument()
	origin.AppendChild(originChild)

	cloned := buildRemoteDispatchReceiverRoot(origin, fakeChild)

	require.NotSame(t, origin, cloned)
	require.Equal(t, 1, origin.GetOperatorBase().NumChildren())
	require.Same(t, originChild, origin.GetOperatorBase().GetChildren(0))
	require.Equal(t, 1, cloned.GetOperatorBase().NumChildren())
	require.Same(t, fakeChild, cloned.GetOperatorBase().GetChildren(0))
}

func Test_MessageSenderSendPipeline(t *testing.T) {
	sender := messageSenderOnClient{
		ctx:          context.Background(),
		streamSender: &fakeStreamSender{},
	}

	{
		// there should only send one time if this is just a small data.
		sender.streamSender.(*fakeStreamSender).sentCnt = 0
		sender.streamSender.(*fakeStreamSender).nextSendError = nil

		err := sender.sendPipeline(make([]byte, 10), make([]byte, 10), true, 100, "")
		require.Nil(t, err)

		require.Equal(t, 1, sender.streamSender.(*fakeStreamSender).sentCnt)
	}

	{
		// there should be cut as multiple message to send for a big data.
		sender.streamSender.(*fakeStreamSender).sentCnt = 0
		sender.streamSender.(*fakeStreamSender).nextSendError = nil

		err := sender.sendPipeline(make([]byte, 10), make([]byte, 10), true, 5, "")
		require.Nil(t, err)

		require.True(t, sender.streamSender.(*fakeStreamSender).sentCnt > 1)
	}

	{
		// error should be thrown once error occurs while sending.
		sender.streamSender.(*fakeStreamSender).sentCnt = 0
		sender.streamSender.(*fakeStreamSender).nextSendError = moerr.NewInternalErrorNoCtx("timeout")

		err := sender.sendPipeline(make([]byte, 10), make([]byte, 10), true, 100, "")
		require.NotNil(t, err)
	}
}

func Test_ReceiveMessageFromCnServer(t *testing.T) {
	proc := testutil.NewProcess(t)
	sender := messageSenderOnClient{
		ctx:          context.Background(),
		streamSender: &fakeStreamSender{},
	}

	{
		// if the root operator is connector.
		s1 := &Scope{
			Proc:   proc,
			RootOp: connector.NewArgument(),
		}
		s1.RootOp.(*connector.Connector).Reg = &process.WaitRegister{
			Ch2: make(chan process.PipelineSignal, 1),
		}
		ch, err1 := sender.streamSender.Receive()
		require.Nil(t, err1)
		sender.receiveCh = ch
		err := receiveMessageFromCnServer(s1, false, &sender)
		require.Nil(t, err)
	}

	{
		// if the root operator is dispatch.
		s2 := &Scope{
			Proc:   proc,
			RootOp: nil,
		}
		d := dispatch.NewArgument()
		d.LocalRegs = []*process.WaitRegister{
			{Ch2: make(chan process.PipelineSignal, 1)},
		}
		d.FuncId = dispatch.SendToAllLocalFunc
		s2.RootOp = d
		ch, err1 := sender.streamSender.Receive()
		require.Nil(t, err1)
		sender.receiveCh = ch

		err := receiveMessageFromCnServer(s2, false, &sender)
		require.Nil(t, err)
	}

	{
		// if others.
		s3 := &Scope{
			Proc:   proc,
			RootOp: value_scan.NewArgument(),
		}
		ch, err1 := sender.streamSender.Receive()
		require.Nil(t, err1)
		sender.receiveCh = ch

		err := receiveMessageFromCnServer(s3, true, &sender)
		require.Nil(t, err)
	}

	{
		// if not withoutOutput and no connector / dispatch, it's an unexpected case, should throw error.
		s4 := &Scope{
			Proc:   proc,
			RootOp: value_scan.NewArgument(),
		}
		ch, err1 := sender.streamSender.Receive()
		require.Nil(t, err1)
		sender.receiveCh = ch

		require.NotNil(t, receiveMessageFromCnServer(s4, false, &sender))
	}
}

func TestReceiveMessageFromCnServerIfConnector_ReturnsOnBlockedReceiverCancel(t *testing.T) {
	proc := testutil.NewProcess(t)
	proc.BuildPipelineContext(context.Background())

	s := &Scope{
		Proc:   proc,
		RootOp: connector.NewArgument(),
	}
	s.RootOp.(*connector.Connector).Reg = &process.WaitRegister{
		Ch2: make(chan process.PipelineSignal, 1),
	}
	s.RootOp.(*connector.Connector).Reg.Ch2 <- process.NewPipelineSignalToDirectly(nil, nil, proc.Mp())

	sender := &messageSenderOnClient{
		ctx:       proc.Ctx,
		mp:        proc.Mp(),
		receiveCh: make(chan morpc.Message, 1),
	}
	sender.receiveCh <- makeRemoteBatchMessage(t, batch.NewWithSize(0))

	done := make(chan error, 1)
	go func() {
		done <- receiveMessageFromCnServerIfConnector(s, sender)
	}()

	proc.Cancel(nil)

	select {
	case err := <-done:
		require.Error(t, err)
		require.True(t, moerr.IsMoErrCode(err, moerr.ErrQueryInterrupted))
	case <-time.After(time.Second):
		<-s.RootOp.(*connector.Connector).Reg.Ch2
		require.Fail(t, "receiveMessageFromCnServerIfConnector did not unblock after cancellation")
	}
}

func TestReceiveMsgAndForward_ReturnsOnBlockedReceiverCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	forwardCh := make(chan process.PipelineSignal, 1)
	forwardCh <- process.NewPipelineSignalToDirectly(nil, nil, nil)

	sender := &messageSenderOnClient{
		ctx:       ctx,
		mp:        mpool.MustNewZero(),
		receiveCh: make(chan morpc.Message, 1),
	}
	sender.receiveCh <- makeRemoteBatchMessage(t, batch.NewWithSize(0))

	done := make(chan error, 1)
	go func() {
		done <- receiveMsgAndForward(sender, forwardCh)
	}()

	cancel()

	select {
	case err := <-done:
		require.Error(t, err)
		require.True(t, moerr.IsMoErrCode(err, moerr.ErrQueryInterrupted))
	case <-time.After(time.Second):
		<-forwardCh
		require.Fail(t, "receiveMsgAndForward did not unblock after cancellation")
	}
}

func TestRemoteNotifyCleanupUsesTypedErrorForSharedReceiver(t *testing.T) {
	reg := process.NewPipelineEdge(3, 3)
	testErr := moerr.NewInternalErrorNoCtx("remote notify failed")

	require.True(t, sendRemoteNotifyCleanupTerminal(nil, reg, testErr))

	receiver := process.InitPipelineSignalReceiver(context.Background(), []*process.WaitRegister{reg})
	bat, err := receiver.GetNextBatch(nil)
	require.Nil(t, bat)
	require.ErrorIs(t, err, testErr)

	require.Equal(t, 2, len(reg.Ch2))
	for len(reg.Ch2) > 0 {
		signal := <-reg.Ch2
		require.Equal(t, process.EventError, signal.EventType)
		require.ErrorIs(t, signal.TerminalErr(), testErr)
	}
}

func TestRemoteNotifyCleanupUsesTypedEndForSingleSender(t *testing.T) {
	reg := process.NewPipelineEdge(1, 2)

	require.True(t, sendRemoteNotifyCleanupTerminal(nil, reg, nil))

	signal := <-reg.Ch2
	require.Equal(t, process.EventEnd, signal.EventType)
	select {
	case <-reg.Done():
		require.Fail(t, "single remote End should not close a shared receiver edge")
	default:
	}
}

func TestRemoteNotifyCleanupUsesSharedTerminalSendBudget(t *testing.T) {
	oldSignalSendTimeout := process.PipelineSignalSendTimeout
	process.PipelineSignalSendTimeout = 100 * time.Millisecond
	t.Cleanup(func() {
		process.PipelineSignalSendTimeout = oldSignalSendTimeout
	})

	reg := process.NewPipelineEdge(1, 0)
	reg.Ch2 <- process.NewPipelineSignalToDirectly(nil, nil, nil)

	start := time.Now()
	require.False(t, sendRemoteNotifyCleanupTerminal(nil, reg, nil))
	elapsed := time.Since(start)

	require.Less(t, elapsed, 180*time.Millisecond)
	select {
	case <-reg.Done():
	default:
		t.Fatal("fallback abort should mark the remote notify edge terminal")
	}
	require.ErrorIs(t, reg.Err(), process.ErrPipelineEndSignalDeliveryFailed)
}

func TestReceiveMessageFromCnServerIfDispatch_PreservesCleanupOnOriginalRoot(t *testing.T) {
	proc := testutil.NewProcess(t)

	reg := &process.WaitRegister{Ch2: make(chan process.PipelineSignal, 2)}
	d := dispatch.NewArgument()
	d.LocalRegs = []*process.WaitRegister{reg}
	d.FuncId = dispatch.SendToAllLocalFunc
	s := &Scope{Proc: proc, RootOp: d}

	sender := &messageSenderOnClient{
		ctx:       context.Background(),
		mp:        proc.Mp(),
		receiveCh: make(chan morpc.Message, 2),
	}
	dataBat := batch.NewWithSize(0)
	dataBat.SetRowCount(1)
	sender.receiveCh <- makeRemoteBatchMessage(t, dataBat)
	sender.receiveCh <- &pipeline.Message{Sid: pipeline.Status_MessageEnd}

	err := receiveMessageFromCnServerIfDispatch(s, sender)
	require.NoError(t, err)

	ctrField := reflect.ValueOf(d).Elem().FieldByName("ctr")
	require.False(t, ctrField.IsNil(), "receiveMessageFromCnServerIfDispatch should keep cleanup state on the original root")

	select {
	case signal := <-reg.Ch2:
		bat, actionErr := signal.Action()
		require.NoError(t, actionErr)
		require.NotNil(t, bat)
		require.Equal(t, 1, bat.RowCount())
	case <-time.After(time.Second):
		require.Fail(t, "dispatch runner did not send the data batch signal")
	}

	done := make(chan struct{}, 1)
	go func() {
		d.Reset(proc, false, nil)
		done <- struct{}{}
	}()

	select {
	case signal := <-reg.Ch2:
		bat, actionErr := signal.Action()
		require.NoError(t, actionErr)
		require.Nil(t, bat)
	case <-time.After(time.Second):
		require.Fail(t, "original root cleanup did not send a terminal signal")
	}

	select {
	case <-done:
	case <-time.After(time.Second):
		require.Fail(t, "original root cleanup did not finish after terminal signal consumption")
	}

	select {
	case <-reg.Ch2:
		require.Fail(t, "original root cleanup should not emit duplicate terminal signals")
	default:
	}
}

func Test_checkPipelineStandaloneExecutableAtRemote(t *testing.T) {
	proc := testutil.NewProcess(t)
	proc.Base.TxnOperator = fakeTxnOperator{}
	// a standalone pipeline tree should return true.
	{
		// s0, pre: s1, s2
		s0 := &Scope{
			Proc:   proc.NewContextChildProc(2),
			RootOp: dispatch.NewArgument(),
		}

		s1 := &Scope{
			Proc: proc.NewContextChildProc(0),
		}
		op1 := connector.NewArgument()
		op1.Reg = s0.Proc.Reg.MergeReceivers[0]
		s1.RootOp = op1

		s2 := &Scope{
			Proc: proc.NewContextChildProc(0),
		}
		op2 := dispatch.NewArgument()
		op2.LocalRegs = []*process.WaitRegister{s0.Proc.Reg.MergeReceivers[1]}
		s2.RootOp = op2

		s0.PreScopes = append(s0.PreScopes, s1, s2)

		require.True(t, checkPipelineStandaloneExecutableAtRemote(s0))
	}

	// a pipeline holds an invalid dispatch should return false.
	{
		// s0, pre: s1
		s0 := &Scope{
			Proc:   proc.NewContextChildProc(1),
			RootOp: dispatch.NewArgument(),
		}

		s1 := &Scope{
			Proc: proc.NewContextChildProc(0),
		}
		op1 := dispatch.NewArgument()
		op1.LocalRegs = []*process.WaitRegister{{}}
		s1.RootOp = op1

		s0.PreScopes = append(s0.PreScopes, s1)

		require.False(t, checkPipelineStandaloneExecutableAtRemote(s0))
	}

	// a pipeline holds an invalid connector should return false.
	{
		// s0, pre: s1
		s0 := &Scope{
			Proc:   proc.NewContextChildProc(1),
			RootOp: dispatch.NewArgument(),
		}

		s1 := &Scope{
			Proc: proc.NewContextChildProc(0),
		}
		op1 := connector.NewArgument()
		op1.Reg = &process.WaitRegister{}
		s1.RootOp = op1

		s0.PreScopes = append(s0.PreScopes, s1)

		require.False(t, checkPipelineStandaloneExecutableAtRemote(s0))
	}

	// depth more than 2.
	{
		// s0, pre: s1, pre: s2.
		s0 := &Scope{
			Proc:   proc.NewContextChildProc(1),
			RootOp: dispatch.NewArgument(),
		}

		s1 := &Scope{
			Proc: proc.NewContextChildProc(1),
		}
		op1 := connector.NewArgument()
		op1.Reg = s0.Proc.Reg.MergeReceivers[0]
		s1.RootOp = op1

		s2 := &Scope{
			Proc: proc.NewContextChildProc(0),
		}
		op2 := connector.NewArgument()
		op2.Reg = &process.WaitRegister{}
		s2.RootOp = op2

		s0.PreScopes = append(s0.PreScopes, s1)
		s1.PreScopes = append(s1.PreScopes, s2)

		require.False(t, checkPipelineStandaloneExecutableAtRemote(s0))
	}
}

// TestDeletionCanTruncateSerializationRoundtrip verifies that CanTruncate is
// properly serialized and deserialized when Deletion operators are sent to remote CN.
func TestDeletionCanTruncateSerializationRoundtrip(t *testing.T) {
	// Create a Deletion operator with CanTruncate=true
	arg := deletion.NewArgument()
	arg.DeleteCtx = &deletion.DeleteCtx{
		CanTruncate:     true,
		RowIdIdx:        1,
		PrimaryKeyIdx:   0,
		AddAffectedRows: true,
		Ref:             &plan.ObjectRef{SchemaName: "test", ObjName: "t1"},
	}

	// Create minimal context for serialization
	ctx := &scopeContext{
		id:       0,
		plan:     &plan.Plan{},
		scope:    &Scope{},
		root:     &scopeContext{},
		parent:   nil,
		children: nil,
		pipe:     nil,
		regs:     make(map[*process.WaitRegister]int32),
	}
	ctx.root = ctx

	// Serialize to pipeline instruction
	_, in, err := convertToPipelineInstruction(arg, nil, ctx, 0)
	require.NoError(t, err)
	require.NotNil(t, in.Delete)
	require.True(t, in.Delete.CanTruncate, "CanTruncate should be serialized")

	// Deserialize back to operator
	opr := &pipeline.Instruction{
		Op:     int32(vm.Deletion),
		Delete: in.Delete,
	}
	op, err := convertToVmOperator(opr, ctx, nil)
	require.NoError(t, err)
	require.NotNil(t, op)

	restored := op.(*deletion.Deletion)
	require.NotNil(t, restored.DeleteCtx)
	require.True(t, restored.DeleteCtx.CanTruncate, "CanTruncate should be deserialized")
	require.Equal(t, 1, restored.DeleteCtx.RowIdIdx)
	require.Equal(t, 0, restored.DeleteCtx.PrimaryKeyIdx)
	require.True(t, restored.DeleteCtx.AddAffectedRows)
}

// newDispatchSrcScopeForTest builds a cross-CN shuffle dispatch source scope:
// its dispatch sends to localBuckets via LocalRegs (same CN) and to remoteBuckets
// via RemoteRegs (other CN), exactly like constructDispatchLocalAndRemote does.
func newDispatchSrcScopeForTest(proc *process.Process, addr string, localBuckets, remoteBuckets []*Scope) *Scope {
	src := &Scope{
		Magic:    Remote,
		NodeInfo: engine.Node{Addr: addr, Mcpu: 1},
		Proc:     proc.NewContextChildProc(0),
	}
	d := dispatch.NewArgument()
	d.FuncId = dispatch.ShuffleToAllFunc
	for _, b := range localBuckets {
		d.LocalRegs = append(d.LocalRegs, b.Proc.Reg.MergeReceivers[0])
	}
	for _, b := range remoteBuckets {
		uid, _ := uuid.NewV7()
		d.RemoteRegs = append(d.RemoteRegs, colexec.ReceiveInfo{Uuid: uid, NodeAddr: b.NodeInfo.Addr})
	}
	src.setRootOperator(d)
	src.IsEnd = true
	return src
}

// TestGroupShuffleBucketsByCNIfNeeded reproduces the issue #24919 root cause and
// verifies the per-CN regrouping fix:
//
//	before regrouping, the bucket that carries a cross-CN shuffle dispatch is wrongly
//	judged non-standalone-executable (its dispatch LocalRegs point to a sibling bucket
//	that lives in a separate send tree) -> RemoteRun converts it to local -> the dispatch
//	lands on the coordinator, mispaired with the compile-time cross-CN receiver FromAddr
//	-> hang.
//
//	after regrouping, the dop same-CN buckets (and the nested dispatch) become one per-CN
//	send unit, so checkPipelineStandaloneExecutableAtRemote returns true and the whole
//	group is really executed at the remote CN.
func TestGroupShuffleBucketsByCNIfNeeded(t *testing.T) {
	c := NewMockCompile(t)
	c.cnList = engine.Nodes{
		engine.Node{Addr: "cn1:6001", Mcpu: 2},
		engine.Node{Addr: "cn2:6001", Mcpu: 2},
	}
	c.addr = "cn1:6001"
	c.anal = &AnalyzeModule{qry: &plan.Query{}}
	c.proc.Base.TxnOperator = fakeTxnOperator{}
	proc := c.proc

	// dop=2, 2 CN -> bucketNum=4. buckets[0,1] on cn1, buckets[2,3] on cn2.
	addrs := []string{"cn1:6001", "cn1:6001", "cn2:6001", "cn2:6001"}
	buckets := make([]*Scope, 4)
	for i := range buckets {
		buckets[i] = &Scope{
			Magic:    Remote,
			NodeInfo: engine.Node{Addr: addrs[i], Mcpu: 1},
			Proc:     proc.NewContextChildProc(1),
		}
		buckets[i].setRootOperator(merge.NewArgument())
	}

	// each CN's dispatch source is attached to that CN's first bucket (like compile.go:4500).
	srcCN1 := newDispatchSrcScopeForTest(proc, "cn1:6001",
		[]*Scope{buckets[0], buckets[1]}, []*Scope{buckets[2], buckets[3]})
	buckets[0].PreScopes = append(buckets[0].PreScopes, srcCN1)
	srcCN2 := newDispatchSrcScopeForTest(proc, "cn2:6001",
		[]*Scope{buckets[2], buckets[3]}, []*Scope{buckets[0], buckets[1]})
	buckets[2].PreScopes = append(buckets[2].PreScopes, srcCN2)

	// before regrouping: the dispatch-carrying buckets are wrongly judged not standalone.
	require.False(t, checkPipelineStandaloneExecutableAtRemote(buckets[0]))
	require.False(t, checkPipelineStandaloneExecutableAtRemote(buckets[2]))

	// after regrouping: one per-CN container each, all standalone-executable at remote.
	grouped := c.groupShuffleBucketsByCNIfNeeded(buckets)
	require.Equal(t, 2, len(grouped))
	for _, container := range grouped {
		require.Equal(t, Remote, container.Magic)
		require.True(t, checkPipelineStandaloneExecutableAtRemote(container))
	}
}

// TestGroupShuffleBucketsByCNIfNeeded_Gating verifies the regrouping is a no-op when
// there is no cross-CN shuffle dispatch (single CN, or no dispatch), so non-shuffle /
// single-CN inserts are completely unaffected.
func TestGroupShuffleBucketsByCNIfNeeded_Gating(t *testing.T) {
	c := NewMockCompile(t)
	c.cnList = engine.Nodes{
		engine.Node{Addr: "cn1:6001", Mcpu: 2},
		engine.Node{Addr: "cn2:6001", Mcpu: 2},
	}
	c.anal = &AnalyzeModule{qry: &plan.Query{}}
	proc := c.proc

	// scopes without any cross-CN dispatch -> returned unchanged.
	ss := make([]*Scope, 4)
	for i := range ss {
		ss[i] = &Scope{
			Magic:    Remote,
			NodeInfo: engine.Node{Addr: "cn1:6001", Mcpu: 1},
			Proc:     proc.NewContextChildProc(0),
		}
		ss[i].setRootOperator(merge.NewArgument())
	}
	require.Equal(t, 4, len(c.groupShuffleBucketsByCNIfNeeded(ss)))

	// single CN -> returned unchanged even if a cross-CN dispatch is present.
	c.cnList = engine.Nodes{engine.Node{Addr: "cn1:6001", Mcpu: 2}}
	require.Equal(t, 4, len(c.groupShuffleBucketsByCNIfNeeded(ss)))
}

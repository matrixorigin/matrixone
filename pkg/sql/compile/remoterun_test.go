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
	"errors"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/google/uuid"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/morpc"
	mock_morpc "github.com/matrixorigin/matrixone/pkg/common/morpc/mock_morpc"
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
		require.False(t, restoredOp.CountDeleteAffectRows,
			"CountDeleteAffectRows must stay false when the source op did not set it")
	})

	t.Run("MultiUpdate_CountDeleteAffectRows", func(t *testing.T) {
		op := &multi_update.MultiUpdate{
			MultiUpdateCtx: []*multi_update.MultiUpdateCtx{
				{
					ObjRef:   &plan.ObjectRef{ObjName: "t1"},
					TableDef: &plan.TableDef{Name: "t1"},
				},
			},
			Action:                multi_update.UpdateWriteTable,
			CountDeleteAffectRows: true,
		}
		_, pipeInstr, err := convertToPipelineInstruction(op, proc, ctx, 1)
		require.NoError(t, err)
		require.True(t, pipeInstr.MultiUpdate.UpdateCtxList[0].CountDeleteAffectRows,
			"serialized UpdateCtx must carry CountDeleteAffectRows")

		restored, err := convertToVmOperator(pipeInstr, ctx, nil)
		require.NoError(t, err)
		restoredOp := restored.(*multi_update.MultiUpdate)
		require.True(t, restoredOp.CountDeleteAffectRows,
			"CountDeleteAffectRows must survive the remote pipeline round-trip")
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

func Test_GetProcByUuid_TimeoutDoesNotPoisonLaterRegistration(t *testing.T) {
	_ = colexec.NewServer(nil)

	uid, err := uuid.NewV7()
	require.NoError(t, err)

	receiver := &messageReceiverOnServer{
		connectionCtx:           context.TODO(),
		messageCtx:              context.TODO(),
		waitRegistrationTimeout: 10 * time.Millisecond,
	}

	p, ch, err := receiver.GetProcByUuid(uid)
	require.Error(t, err)
	require.Nil(t, p)
	require.Nil(t, ch)

	p0 := &process.Process{}
	c0 := process.RemotePipelineInformationChannel(make(chan *process.WrapCs))
	require.NoError(t, colexec.Get().PutProcIntoUuidMap(uid, p0, c0))

	colexec.Get().DeleteUuids([]uuid.UUID{uid})
	colexec.Get().DeleteUuids([]uuid.UUID{uid})
}

func Test_TryGetProcByUuid_NotRegisteredYetDoesNotPoisonLaterRegistration(t *testing.T) {
	_ = colexec.NewServer(nil)

	uid, err := uuid.NewV7()
	require.NoError(t, err)

	receiver := &messageReceiverOnServer{
		connectionCtx: context.TODO(),
		messageCtx:    context.TODO(),
	}

	p, ch, err := receiver.TryGetProcByUuid(uid)
	require.Error(t, err)
	require.True(t, isRemoteDispatchNotRegisteredYetError(err))
	require.Nil(t, p)
	require.Nil(t, ch)

	p0 := &process.Process{}
	c0 := process.RemotePipelineInformationChannel(make(chan *process.WrapCs))
	require.NoError(t, colexec.Get().PutProcIntoUuidMap(uid, p0, c0))

	colexec.Get().DeleteUuids([]uuid.UUID{uid})
	colexec.Get().DeleteUuids([]uuid.UUID{uid})
}

func Test_TryGetProcByUuid_ReturnsRegisteredProc(t *testing.T) {
	_ = colexec.NewServer(nil)

	uid, err := uuid.NewV7()
	require.NoError(t, err)

	p0 := &process.Process{}
	c0 := process.RemotePipelineInformationChannel(make(chan *process.WrapCs))
	require.NoError(t, colexec.Get().PutProcIntoUuidMap(uid, p0, c0))
	defer colexec.Get().DeleteUuids([]uuid.UUID{uid})

	receiver := &messageReceiverOnServer{
		connectionCtx: context.Background(),
		messageCtx:    context.Background(),
	}

	p, ch, err := receiver.TryGetProcByUuid(uid)
	require.NoError(t, err)
	require.Same(t, p0, p)
	require.Equal(t, c0, ch)
}

func Test_TryGetProcByUuid_ClosedRetryDoesNotPoisonLaterRegistration(t *testing.T) {
	_ = colexec.NewServer(nil)

	uid, err := uuid.NewV7()
	require.NoError(t, err)

	connectionCtx, cancelConnection := context.WithCancel(context.Background())
	cancelConnection()
	receiver := &messageReceiverOnServer{
		connectionCtx: connectionCtx,
		messageCtx:    context.Background(),
	}

	p, ch, err := receiver.TryGetProcByUuid(uid)
	require.Error(t, err)
	require.True(t, isRemoteDispatchNotRegisteredYetError(err))
	require.Nil(t, p)
	require.Nil(t, ch)

	dispatchProc := &process.Process{}
	notifyCh := make(chan *process.WrapCs)
	require.NoError(t, colexec.Get().PutProcIntoUuidMap(uid, dispatchProc, notifyCh))

	nextAttempt := &messageReceiverOnServer{
		connectionCtx: context.Background(),
		messageCtx:    context.Background(),
	}
	p, ch, err = nextAttempt.TryGetProcByUuid(uid)
	require.NoError(t, err)
	require.Same(t, dispatchProc, p)
	require.Equal(t, process.RemotePipelineInformationChannel(notifyCh), ch)
	colexec.Get().DeleteUuids([]uuid.UUID{uid})
}

func Test_TryGetProcByUuid_CloseVsRegisterInterleavings(t *testing.T) {
	_ = colexec.NewServer(nil)

	for _, tc := range []struct {
		name                    string
		closeBeforeRegistration bool
	}{
		{name: "close before registration", closeBeforeRegistration: true},
		{name: "registration before close", closeBeforeRegistration: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			uid, err := uuid.NewV7()
			require.NoError(t, err)

			connectionCtx, closeConnection := context.WithCancel(context.Background())
			defer closeConnection()
			lookupDone := make(chan struct{})
			allowLookup := make(chan struct{})
			lookupErr := make(chan error, 1)
			go func() {
				if tc.closeBeforeRegistration {
					<-allowLookup
				}
				receiver := &messageReceiverOnServer{
					connectionCtx: connectionCtx,
					messageCtx:    context.Background(),
				}
				_, _, lookupErrValue := receiver.TryGetProcByUuid(uid)
				lookupErr <- lookupErrValue
				close(lookupDone)
			}()

			if tc.closeBeforeRegistration {
				closeConnection()
				close(allowLookup)
			} else {
				<-lookupDone
			}
			require.True(t, isRemoteDispatchNotRegisteredYetError(<-lookupErr))

			dispatchProc := &process.Process{}
			notifyCh := make(process.RemotePipelineInformationChannel)
			require.NoError(t, colexec.Get().PutProcIntoUuidMap(uid, dispatchProc, notifyCh))
			if !tc.closeBeforeRegistration {
				closeConnection()
			}

			nextAttempt := &messageReceiverOnServer{
				connectionCtx: context.Background(),
				messageCtx:    context.Background(),
			}
			gotProc, gotCh, err := nextAttempt.TryGetProcByUuid(uid)
			require.NoError(t, err)
			require.Same(t, dispatchProc, gotProc)
			require.Equal(t, notifyCh, gotCh)
			colexec.Get().DeleteUuids([]uuid.UUID{uid})
		})
	}
}

type blockingPrepareOperator struct {
	*colexec.MockOperator
	entered chan struct{}
	release chan struct{}
}

func (op *blockingPrepareOperator) Prepare(*process.Process) error {
	close(op.entered)
	<-op.release
	return nil
}

func TestCoordinatorDispatchRegisteredBeforePrepare(t *testing.T) {
	_ = colexec.NewServer(nil)

	uid, err := uuid.NewV7()
	require.NoError(t, err)
	proc := testutil.NewProcess(t)
	proc.BuildPipelineContext(context.Background())
	input := testutil.NewBatch([]types.Type{types.T_int64.ToType()}, false, 1, proc.Mp())
	child := &blockingPrepareOperator{
		MockOperator: colexec.NewMockOperator().WithBatchs([]*batch.Batch{input}),
		entered:      make(chan struct{}),
		release:      make(chan struct{}),
	}
	dispatchOp := dispatch.NewArgument()
	dispatchOp.FuncId = dispatch.SendToAllFunc
	dispatchOp.RemoteRegs = []colexec.ReceiveInfo{{Uuid: uid}}
	dispatchOp.AppendChild(child)
	scope := &Scope{Magic: Normal, Proc: proc, RootOp: dispatchOp}
	runCompile := &Compile{
		scopes:     []*Scope{scope},
		pn:         &planpb.Plan{},
		execType:   plan.ExecTypeTP,
		proc:       proc,
		affectRows: &atomic.Uint64{},
		addr:       "local-cn",
	}

	runDone := make(chan error, 1)
	go func() {
		runDone <- runCompile.runOnce()
	}()
	select {
	case <-child.entered:
	case <-time.After(time.Second):
		t.Fatal("source did not reach the pre-dispatch Prepare barrier")
	}

	receiver := &messageReceiverOnServer{
		connectionCtx: context.Background(),
		messageCtx:    context.Background(),
	}
	registeredProc, notifyCh, err := receiver.TryGetProcByUuid(uid)
	require.NoError(t, err)
	require.Same(t, proc, registeredProc)
	require.NotNil(t, notifyCh)

	ctrl := gomock.NewController(t)
	clientSession := mock_morpc.NewMockClientSession(ctrl)
	clientSession.EXPECT().Write(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
	remoteReceiver := &process.WrapCs{
		Uid: uid,
		Cs:  clientSession,
		Err: make(chan error, 1),
	}
	attached := make(chan struct{})
	go func() {
		notifyCh <- remoteReceiver
		close(attached)
	}()
	close(child.release)

	select {
	case <-attached:
	case <-time.After(time.Second):
		t.Fatal("remote notify did not attach after dispatch Prepare resumed")
	}
	select {
	case err := <-runDone:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("coordinator dispatch pipeline did not finish")
	}

	_, _, err = receiver.TryGetProcByUuid(uid)
	require.True(t, isRemoteDispatchNotRegisteredYetError(err), "runOnce must clean its early registration")
}

func TestRegisterLocalDispatchReceiversNestedAndIdempotent(t *testing.T) {
	_ = colexec.NewServer(nil)

	uid, err := uuid.NewV7()
	require.NoError(t, err)
	rootProc := testutil.NewProcess(t)
	nestedProc := rootProc.NewNoContextChildProc(0)
	dispatchOp := dispatch.NewArgument()
	dispatchOp.FuncId = dispatch.SendToAllFunc
	dispatchOp.RemoteRegs = []colexec.ReceiveInfo{{Uuid: uid}}
	nested := &Scope{Magic: Normal, Proc: nestedProc, RootOp: dispatchOp}
	root := &Scope{Magic: Merge, Proc: rootProc, PreScopes: []*Scope{nested}}

	first, err := registerLocalDispatchReceivers([]*Scope{root}, "local-cn")
	require.NoError(t, err)
	defer first.cleanup()
	second, err := registerLocalDispatchReceivers([]*Scope{root}, "local-cn")
	require.NoError(t, err)
	defer second.cleanup()

	registeredProc, _, ok := colexec.Get().GetProcByUuid(uid, false)
	require.True(t, ok)
	require.Same(t, nestedProc, registeredProc)
}

func TestRegisterLocalDispatchReceiversRegistersRetainedRemoteRootOnly(t *testing.T) {
	_ = colexec.NewServer(nil)

	localUID, err := uuid.NewV7()
	require.NoError(t, err)
	remoteUID, err := uuid.NewV7()
	require.NoError(t, err)
	rootProc := testutil.NewProcess(t)
	remoteProc := rootProc.NewNoContextChildProc(0)
	remoteChild := dispatch.NewArgument()
	remoteChild.FuncId = dispatch.SendToAllFunc
	remoteChild.RemoteRegs = []colexec.ReceiveInfo{{Uuid: remoteUID}}
	localRoot := dispatch.NewArgument()
	localRoot.FuncId = dispatch.SendToAllFunc
	localRoot.RemoteRegs = []colexec.ReceiveInfo{{Uuid: localUID}}
	localRoot.AppendChild(remoteChild)
	remote := &Scope{
		Magic:    Remote,
		Proc:     remoteProc,
		RootOp:   localRoot,
		NodeInfo: engine.Node{Addr: "remote-cn:6002"},
	}
	root := &Scope{Magic: Merge, Proc: rootProc, PreScopes: []*Scope{remote}}

	registrations, err := registerLocalDispatchReceivers([]*Scope{root}, "local-cn:6002")
	require.NoError(t, err)
	defer registrations.cleanup()
	registeredProc, _, ok := colexec.Get().GetProcByUuid(localUID, false)
	require.True(t, ok)
	require.Same(t, remoteProc, registeredProc)
	registeredProc, notifyCh, ok := colexec.Get().GetProcByUuid(remoteUID, false)
	require.False(t, ok)
	require.Nil(t, registeredProc)
	require.Nil(t, notifyCh)
}

func TestRegisterLocalDispatchReceiversSkipsGuaranteedRemoteRunFailures(t *testing.T) {
	_ = colexec.NewServer(nil)

	for _, tc := range []struct {
		name       string
		remoteAddr string
		cannotRun  bool
	}{
		{name: "malformed remote address", remoteAddr: "not-an-address"},
		{name: "cannot remote operator", remoteAddr: "remote-cn:6002", cannotRun: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			uid := uuid.Must(uuid.NewV7())
			proc := testutil.NewProcess(t)
			root := dispatch.NewArgument()
			defer root.Release()
			root.FuncId = dispatch.SendToAllFunc
			root.RecCTE = tc.cannotRun
			root.RemoteRegs = []colexec.ReceiveInfo{{Uuid: uid}}
			s := &Scope{
				Magic:    Remote,
				Proc:     proc,
				RootOp:   root,
				NodeInfo: engine.Node{Addr: tc.remoteAddr},
			}

			registrations, err := registerLocalDispatchReceivers([]*Scope{s}, "local-cn:6002")
			require.NoError(t, err)
			defer registrations.cleanup()
			registeredProc, notifyCh, ok := colexec.Get().GetProcByUuid(uid, false)
			require.False(t, ok)
			require.Nil(t, registeredProc)
			require.Nil(t, notifyCh)
		})
	}
}

func TestRegisterRemoteDispatchReceiversUsesOwningScopeProcess(t *testing.T) {
	_ = colexec.NewServer(nil)

	uid, err := uuid.NewV7()
	require.NoError(t, err)
	rootProc := testutil.NewProcess(t)
	nestedProc := rootProc.NewNoContextChildProc(0)
	dispatchOp := dispatch.NewArgument()
	dispatchOp.FuncId = dispatch.SendToAllFunc
	dispatchOp.RemoteRegs = []colexec.ReceiveInfo{{Uuid: uid}}
	nested := &Scope{Magic: Normal, Proc: nestedProc, RootOp: dispatchOp}
	root := &Scope{Magic: Merge, Proc: rootProc, PreScopes: []*Scope{nested}}

	registrations, err := registerRemoteDispatchReceivers(root)
	require.NoError(t, err)
	defer registrations.cleanup()
	registeredProc, _, ok := colexec.Get().GetProcByUuid(uid, false)
	require.True(t, ok)
	require.Same(t, nestedProc, registeredProc)
}

func TestRegisterLocalDispatchReceiversRollsBackEarlierScopes(t *testing.T) {
	_ = colexec.NewServer(nil)

	uid1, err := uuid.NewV7()
	require.NoError(t, err)
	uid2, err := uuid.NewV7()
	require.NoError(t, err)
	colexec.Get().GetProcByUuid(uid2, true)
	defer colexec.Get().DeleteUuids([]uuid.UUID{uid2})

	proc1 := testutil.NewProcess(t)
	proc1.BuildPipelineContext(context.Background())
	proc2 := testutil.NewProcess(t)
	proc2.BuildPipelineContext(context.Background())
	dispatch1 := dispatch.NewArgument()
	dispatch1.FuncId = dispatch.SendToAllFunc
	dispatch1.RemoteRegs = []colexec.ReceiveInfo{{Uuid: uid1}}
	dispatch2 := dispatch.NewArgument()
	dispatch2.FuncId = dispatch.SendToAllFunc
	dispatch2.RemoteRegs = []colexec.ReceiveInfo{{Uuid: uid2}}
	scopes := []*Scope{
		{Magic: Normal, Proc: proc1, RootOp: dispatch1},
		{Magic: Normal, Proc: proc2, RootOp: dispatch2},
	}

	_, err = registerLocalDispatchReceivers(scopes, "local-cn")
	require.Error(t, err)
	require.ErrorIs(t, context.Cause(proc1.Ctx), err)
	require.ErrorIs(t, context.Cause(proc2.Ctx), err)
	registeredProc, notifyCh, ok := colexec.Get().GetProcByUuid(uid1, false)
	require.False(t, ok)
	require.Nil(t, registeredProc)
	require.Nil(t, notifyCh)

	registrations, err := registerLocalDispatchReceivers(scopes[:1], "local-cn")
	require.NoError(t, err, "rollback must clear the dispatch's early-registration state")
	registeredProc, notifyCh, ok = colexec.Get().GetProcByUuid(uid1, false)
	require.True(t, ok)
	require.Same(t, proc1, registeredProc)
	require.NotNil(t, notifyCh)
	registrations.cleanup()
	registeredProc, notifyCh, ok = colexec.Get().GetProcByUuid(uid1, false)
	require.False(t, ok)
	require.Nil(t, registeredProc)
	require.Nil(t, notifyCh)
}

func TestRemoteDispatchRegistrationRollbackReleasesPendingAttach(t *testing.T) {
	_ = colexec.NewServer(nil)

	uid, err := uuid.NewV7()
	require.NoError(t, err)
	proc := testutil.NewProcess(t)
	proc.BuildPipelineContext(context.Background())
	dispatchOp := dispatch.NewArgument()
	dispatchOp.FuncId = dispatch.SendToAllFunc
	dispatchOp.RemoteRegs = []colexec.ReceiveInfo{{Uuid: uid}}
	registration, err := dispatchOp.RegisterRemoteReceiversWithHandle(proc)
	require.NoError(t, err)
	require.NotNil(t, registration)

	receiver := &messageReceiverOnServer{
		connectionCtx: context.Background(),
		messageCtx:    context.Background(),
	}
	registeredProc, notifyCh, err := receiver.TryGetProcByUuid(uid)
	require.NoError(t, err)
	require.Same(t, proc, registeredProc)

	pendingDone := make(chan string, 1)
	started := make(chan struct{})
	go func() {
		close(started)
		select {
		case notifyCh <- &process.WrapCs{Uid: uid, Err: make(chan error, 1)}:
			pendingDone <- "attached"
		case <-proc.Ctx.Done():
			pendingDone <- "canceled"
		}
	}()
	<-started
	select {
	case result := <-pendingDone:
		t.Fatalf("pending remote notify completed before rollback: %s", result)
	default:
	}

	cause := moerr.NewInternalErrorNoCtx("later receiver registration failed")
	registrations := &remoteDispatchReceiverRegistrations{
		registrations: []*dispatch.RemoteReceiverRegistration{registration},
	}
	registrations.rollback(cause)
	require.ErrorIs(t, context.Cause(proc.Ctx), cause)
	select {
	case result := <-pendingDone:
		require.Equal(t, "canceled", result)
	case <-time.After(time.Second):
		t.Fatal("registration rollback did not release the pending remote notify")
	}
	registeredProc, notifyCh, ok := colexec.Get().GetProcByUuid(uid, false)
	require.False(t, ok)
	require.Nil(t, registeredProc)
	require.Nil(t, notifyCh)
}

func TestSendNotifyMessageRetriesUntilRemoteDispatchRegistered(t *testing.T) {
	proc := testutil.NewProcess(t)
	proc.BuildPipelineContext(context.Background())
	scopeProc := proc.NewContextChildProc(1)

	uid, err := uuid.NewV7()
	require.NoError(t, err)

	s := &Scope{
		Proc: scopeProc,
		RemoteReceivRegInfos: []RemoteReceivRegInfo{
			{
				Idx:      0,
				Uuid:     uid,
				FromAddr: "remote-cn",
			},
		},
	}

	var attempts int
	factory := func(
		ctx context.Context,
		sid string,
		toAddr string,
		mp *mpool.MPool,
		analyzeModule *AnalyzeModule,
	) (*messageSenderOnClient, error) {
		attempts++
		receiveCh := make(chan morpc.Message, 2)
		if attempts == 1 {
			msg := &pipeline.Message{Sid: pipeline.Status_MessageEnd}
			msg.SetMoError(ctx, moerr.NewRemoteDispatchNotRegistered(ctx, uid.String()))
			receiveCh <- msg
		} else {
			receiveCh <- makeRemoteBatchMessage(t, batch.NewWithSize(0))
			receiveCh <- &pipeline.Message{Sid: pipeline.Status_MessageEnd}
		}
		return &messageSenderOnClient{
			ctx:          ctx,
			mp:           mp,
			streamSender: &fakeStreamSender{},
			receiveCh:    receiveCh,
			safeToClose:  true,
		}, nil
	}

	var wg sync.WaitGroup
	resultCh := make(chan notifyMessageResult, 1)
	s.sendNotifyMessageWithFactory(&wg, resultCh, factory)

	select {
	case signal := <-scopeProc.Reg.MergeReceivers[0].Ch2:
		bat, err := signal.Action()
		require.NoError(t, err)
		require.NotNil(t, bat)
		bat.Clean(scopeProc.Mp())
	case <-time.After(time.Second):
		t.Fatal("notify retry did not forward the remote batch")
	}

	select {
	case result := <-resultCh:
		result.clean(scopeProc)
		require.NoError(t, result.err)
	case <-time.After(time.Second):
		t.Fatal("notify retry did not finish")
	}

	wg.Wait()
	require.Equal(t, 2, attempts)
}

func TestSendNotifyMessageWrapperWithNoRemoteReceivers(t *testing.T) {
	proc := testutil.NewProcess(t)
	proc.BuildPipelineContext(context.Background())
	scopeProc := proc.NewContextChildProc(0)

	s := &Scope{Proc: scopeProc}
	var wg sync.WaitGroup
	resultCh := make(chan notifyMessageResult, 1)
	s.sendNotifyMessage(&wg, resultCh)
	wg.Wait()

	select {
	case result := <-resultCh:
		t.Fatalf("unexpected notify result: %+v", result)
	default:
	}
}

func TestSendNotifyMessageReportsSenderFactoryError(t *testing.T) {
	proc := testutil.NewProcess(t)
	proc.BuildPipelineContext(context.Background())
	scopeProc := proc.NewContextChildProc(1)

	uid, err := uuid.NewV7()
	require.NoError(t, err)
	s := &Scope{
		Proc: scopeProc,
		RemoteReceivRegInfos: []RemoteReceivRegInfo{
			{Idx: 0, Uuid: uid, FromAddr: "remote-cn"},
		},
	}

	testErr := errors.New("sender factory failed")
	factory := func(
		ctx context.Context,
		sid string,
		toAddr string,
		mp *mpool.MPool,
		analyzeModule *AnalyzeModule,
	) (*messageSenderOnClient, error) {
		return nil, testErr
	}

	var wg sync.WaitGroup
	resultCh := make(chan notifyMessageResult, 1)
	s.sendNotifyMessageWithFactory(&wg, resultCh, factory)

	select {
	case result := <-resultCh:
		require.ErrorIs(t, result.err, testErr)
	case <-time.After(time.Second):
		t.Fatal("notify sender factory error did not finish")
	}
	select {
	case signal := <-scopeProc.Reg.MergeReceivers[0].Ch2:
		_, err := signal.Action()
		require.ErrorIs(t, err, testErr)
	case <-time.After(time.Second):
		t.Fatal("notify sender factory error did not send cleanup signal")
	}
	wg.Wait()
}

func TestSendNotifyMessageReportsStreamSendError(t *testing.T) {
	proc := testutil.NewProcess(t)
	proc.BuildPipelineContext(context.Background())
	scopeProc := proc.NewContextChildProc(1)

	uid, err := uuid.NewV7()
	require.NoError(t, err)
	s := &Scope{
		Proc: scopeProc,
		RemoteReceivRegInfos: []RemoteReceivRegInfo{
			{Idx: 0, Uuid: uid, FromAddr: "remote-cn"},
		},
	}

	testErr := errors.New("stream send failed")
	factory := func(
		ctx context.Context,
		sid string,
		toAddr string,
		mp *mpool.MPool,
		analyzeModule *AnalyzeModule,
	) (*messageSenderOnClient, error) {
		return &messageSenderOnClient{
			ctx:          ctx,
			mp:           mp,
			streamSender: &fakeStreamSender{nextSendError: testErr},
			receiveCh:    make(chan morpc.Message),
			safeToClose:  true,
		}, nil
	}

	var wg sync.WaitGroup
	resultCh := make(chan notifyMessageResult, 1)
	s.sendNotifyMessageWithFactory(&wg, resultCh, factory)

	select {
	case result := <-resultCh:
		result.clean(scopeProc)
		require.ErrorIs(t, result.err, testErr)
	case <-time.After(time.Second):
		t.Fatal("notify stream send error did not finish")
	}
	select {
	case signal := <-scopeProc.Reg.MergeReceivers[0].Ch2:
		_, err := signal.Action()
		require.ErrorIs(t, err, testErr)
	case <-time.After(time.Second):
		t.Fatal("notify stream send error did not send cleanup signal")
	}
	wg.Wait()
}

func TestSendNotifyMessageTimesOutWaitingForRemoteDispatchRegistration(t *testing.T) {
	originTimeout := notifyMessageWaitRegistrationTimeout
	notifyMessageWaitRegistrationTimeout = 10 * time.Millisecond
	defer func() {
		notifyMessageWaitRegistrationTimeout = originTimeout
	}()

	proc := testutil.NewProcess(t)
	proc.BuildPipelineContext(context.Background())
	scopeProc := proc.NewContextChildProc(1)

	uid, err := uuid.NewV7()
	require.NoError(t, err)
	s := &Scope{
		Proc: scopeProc,
		RemoteReceivRegInfos: []RemoteReceivRegInfo{
			{Idx: 0, Uuid: uid, FromAddr: "remote-cn"},
		},
	}

	factory := func(
		ctx context.Context,
		sid string,
		toAddr string,
		mp *mpool.MPool,
		analyzeModule *AnalyzeModule,
	) (*messageSenderOnClient, error) {
		receiveCh := make(chan morpc.Message, 1)
		msg := &pipeline.Message{Sid: pipeline.Status_MessageEnd}
		msg.SetMoError(ctx, moerr.NewRemoteDispatchNotRegistered(ctx, uid.String()))
		receiveCh <- msg
		return &messageSenderOnClient{
			ctx:          ctx,
			mp:           mp,
			streamSender: &fakeStreamSender{},
			receiveCh:    receiveCh,
			safeToClose:  true,
		}, nil
	}

	var wg sync.WaitGroup
	resultCh := make(chan notifyMessageResult, 1)
	s.sendNotifyMessageWithFactory(&wg, resultCh, factory)

	select {
	case result := <-resultCh:
		result.clean(scopeProc)
		require.Error(t, result.err)
		require.Contains(t, result.err.Error(), "dispatch process not registered within")
	case <-time.After(time.Second):
		t.Fatal("notify retry did not time out")
	}
	select {
	case signal := <-scopeProc.Reg.MergeReceivers[0].Ch2:
		_, err := signal.Action()
		require.Error(t, err)
		require.Contains(t, err.Error(), "dispatch process not registered within")
	case <-time.After(time.Second):
		t.Fatal("notify retry timeout did not send cleanup signal")
	}
	wg.Wait()
}

func TestSendNotifyMessageStopsRetryWhenQueryContextCanceled(t *testing.T) {
	proc := testutil.NewProcess(t)
	proc.BuildPipelineContext(context.Background())
	scopeProc := proc.NewContextChildProc(1)

	uid, err := uuid.NewV7()
	require.NoError(t, err)

	s := &Scope{
		Proc: scopeProc,
		RemoteReceivRegInfos: []RemoteReceivRegInfo{
			{
				Idx:      0,
				Uuid:     uid,
				FromAddr: "remote-cn",
			},
		},
	}

	var attempts atomic.Int32
	notRegisteredSent := make(chan struct{})
	factory := func(
		ctx context.Context,
		sid string,
		toAddr string,
		mp *mpool.MPool,
		analyzeModule *AnalyzeModule,
	) (*messageSenderOnClient, error) {
		attempts.Add(1)
		receiveCh := make(chan morpc.Message, 1)
		msg := &pipeline.Message{Sid: pipeline.Status_MessageEnd}
		msg.SetMoError(ctx, moerr.NewRemoteDispatchNotRegistered(ctx, uid.String()))
		receiveCh <- msg
		close(notRegisteredSent)
		return &messageSenderOnClient{
			ctx:          ctx,
			mp:           mp,
			streamSender: &fakeStreamSender{},
			receiveCh:    receiveCh,
			safeToClose:  true,
		}, nil
	}

	var wg sync.WaitGroup
	resultCh := make(chan notifyMessageResult, 1)
	s.sendNotifyMessageWithFactory(&wg, resultCh, factory)
	go func() {
		<-notRegisteredSent
		time.Sleep(10 * time.Millisecond)
		scopeProc.Cancel(nil)
	}()

	select {
	case result := <-resultCh:
		result.clean(scopeProc)
		require.ErrorIs(t, result.err, context.Canceled)
	case <-time.After(time.Second):
		t.Fatal("notify retry did not stop after query context cancellation")
	}

	select {
	case signal := <-scopeProc.Reg.MergeReceivers[0].Ch2:
		_, err := signal.Action()
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(time.Second):
		t.Fatal("notify retry did not send cleanup signal")
	}

	wg.Wait()
	require.Equal(t, int32(1), attempts.Load())
}

func TestGetProcByUuidCancelPendingRegistrationCancelsConsumedDispatchProc(t *testing.T) {
	_ = colexec.NewServer(nil)

	uid, err := uuid.NewV7()
	require.Nil(t, err)

	procCtx, procCancel := context.WithCancelCause(context.Background())
	dispatchProc := &process.Process{
		Ctx:    procCtx,
		Cancel: procCancel,
	}
	notifyCh := process.RemotePipelineInformationChannel(make(chan *process.WrapCs))
	require.NoError(t, colexec.Get().PutProcIntoUuidMap(uid, dispatchProc, notifyCh))

	receiver := &messageReceiverOnServer{messageCtx: context.Background()}
	cancelCause := moerr.NewInternalErrorNoCtx("registration abandoned")
	receiver.cancelPendingDispatchRegistration(uid, cancelCause)

	require.ErrorIs(t, context.Cause(procCtx), cancelCause)
	colexec.Get().DeleteUuids([]uuid.UUID{uid})
	registeredProc, notifyChannel, ok := colexec.Get().GetProcByUuid(uid, false)
	require.False(t, ok)
	require.Nil(t, registeredProc)
	require.Nil(t, notifyChannel)
}

func TestGetProcByUuidReturnsWhenMessageContextCanceledBeforeRegistration(t *testing.T) {
	_ = colexec.NewServer(nil)

	uid, err := uuid.NewV7()
	require.Nil(t, err)

	messageCtx, cancelMessage := context.WithCancel(context.Background())
	receiver := &messageReceiverOnServer{
		connectionCtx: context.Background(),
		messageCtx:    messageCtx,
	}

	type result struct {
		proc *process.Process
		ch   process.RemotePipelineInformationChannel
		err  error
	}
	done := make(chan result, 1)
	go func() {
		p, c, e := receiver.GetProcByUuid(uid)
		done <- result{proc: p, ch: c, err: e}
	}()

	time.Sleep(10 * time.Millisecond)
	cancelMessage()

	select {
	case r := <-done:
		require.NoError(t, r.err)
		require.Nil(t, r.proc)
		require.Nil(t, r.ch)
	case <-time.After(time.Second):
		t.Fatal("GetProcByUuid did not return after message context cancellation")
	}

	err = colexec.Get().PutProcIntoUuidMap(uid, &process.Process{}, make(chan *process.WrapCs))
	require.Error(t, err)
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

var _ morpc.Stream = &blockingSendStream{}

type blockingSendStream struct {
	sendStarted chan struct{}
}

func (s *blockingSendStream) ID() uint64 { return 0 }
func (s *blockingSendStream) Send(ctx context.Context, request morpc.Message) error {
	close(s.sendStarted)
	<-ctx.Done()
	return ctx.Err()
}
func (s *blockingSendStream) Receive() (chan morpc.Message, error) {
	return make(chan morpc.Message), nil
}
func (s *blockingSendStream) Close(_ bool) error {
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
	defer origin.Release()
	originChild := value_scan.NewArgument()
	fakeChild := value_scan.NewArgument()
	origin.AppendChild(originChild)

	cloned := buildRemoteDispatchReceiverRoot(origin, fakeChild)
	defer cloned.Release()

	require.NotSame(t, origin, cloned)
	require.Equal(t, 1, origin.GetOperatorBase().NumChildren())
	require.Same(t, originChild, origin.GetOperatorBase().GetChildren(0))
	require.Equal(t, 1, cloned.GetOperatorBase().NumChildren())
	require.Same(t, fakeChild, cloned.GetOperatorBase().GetChildren(0))
}

func TestBuildRemoteDispatchReceiverRootReusesEarlyRegistration(t *testing.T) {
	_ = colexec.NewServer(nil)

	uid := uuid.Must(uuid.NewV7())
	proc := testutil.NewProcess(t)
	proc.BuildPipelineContext(context.Background())
	origin := dispatch.NewArgument()
	defer origin.Release()
	origin.FuncId = dispatch.SendToAllFunc
	origin.RemoteRegs = []colexec.ReceiveInfo{{Uuid: uid}}
	registration, err := origin.RegisterRemoteReceiversWithHandle(proc)
	require.NoError(t, err)
	require.NotNil(t, registration)
	defer registration.Cleanup()

	cloned := buildRemoteDispatchReceiverRoot(origin, colexec.NewMockOperator())
	defer cloned.Release()
	cloned.AdoptCleanupState(origin)
	require.NoError(t, cloned.Prepare(proc), "the local runner must reuse, not duplicate, the early registration")

	origin.AdoptCleanupState(cloned)
	origin.Reset(proc, true, moerr.NewInternalErrorNoCtx("test cleanup"))
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

func TestMessageSenderSendPipelineReturnsWhenSendObservesContextCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	stream := &blockingSendStream{sendStarted: make(chan struct{})}
	sender := messageSenderOnClient{
		ctx:          ctx,
		streamSender: stream,
	}

	done := make(chan error, 1)
	go func() {
		done <- sender.sendPipeline(make([]byte, 10), nil, true, 100, "")
	}()

	select {
	case <-stream.sendStarted:
	case <-time.After(time.Second):
		require.FailNow(t, "sendPipeline did not call Stream.Send")
	}
	cancel()

	select {
	case err := <-done:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(time.Second):
		require.Fail(t, "sendPipeline did not return after sender context cancellation")
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
	s.RootOp.(*connector.Connector).Reg = process.NewPipelineEdge(1, 0)
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

	forwardReg := process.NewPipelineEdge(1, 0)
	forwardReg.Ch2 <- process.NewPipelineSignalToDirectly(nil, nil, nil)

	sender := &messageSenderOnClient{
		ctx:       ctx,
		mp:        mpool.MustNewZero(),
		receiveCh: make(chan morpc.Message, 1),
	}
	sender.receiveCh <- makeRemoteBatchMessage(t, batch.NewWithSize(0))

	done := make(chan error, 1)
	go func() {
		done <- receiveMsgAndForward(sender, forwardReg)
	}()

	cancel()

	select {
	case err := <-done:
		require.Error(t, err)
		require.True(t, moerr.IsMoErrCode(err, moerr.ErrQueryInterrupted))
	case <-time.After(time.Second):
		<-forwardReg.Ch2
		require.Fail(t, "receiveMsgAndForward did not unblock after cancellation")
	}
}

func TestReceiveMsgAndForward_ReturnsOnReceiverTerminal(t *testing.T) {
	forwardReg := process.NewPipelineEdge(1, 0)
	require.True(t, forwardReg.Abort(moerr.NewInternalErrorNoCtx("receiver terminal")))

	sender := &messageSenderOnClient{
		ctx:       context.Background(),
		mp:        mpool.MustNewZero(),
		receiveCh: make(chan morpc.Message, 1),
	}
	sender.receiveCh <- makeRemoteBatchMessage(t, batch.NewWithSize(0))

	done := make(chan error, 1)
	go func() {
		done <- receiveMsgAndForward(sender, forwardReg)
	}()

	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(time.Second):
		require.Fail(t, "receiveMsgAndForward did not unblock after receiver terminal")
	}
}

func TestReceiveMessageFromCnServerIfConnector_ReturnsOnReceiverTerminal(t *testing.T) {
	proc := testutil.NewProcess(t)
	proc.BuildPipelineContext(context.Background())

	reg := process.NewPipelineEdge(1, 0)
	require.True(t, reg.Abort(moerr.NewInternalErrorNoCtx("receiver terminal")))
	s := &Scope{
		Proc:   proc,
		RootOp: connector.NewArgument().WithReg(reg),
	}

	sender := &messageSenderOnClient{
		ctx:       context.Background(),
		mp:        proc.Mp(),
		receiveCh: make(chan morpc.Message, 1),
	}
	sender.receiveCh <- makeRemoteBatchMessage(t, batch.NewWithSize(0))

	done := make(chan error, 1)
	go func() {
		done <- receiveMessageFromCnServerIfConnector(s, sender)
	}()

	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(time.Second):
		require.Fail(t, "receiveMessageFromCnServerIfConnector did not unblock after receiver terminal")
	}
}

func TestReceiveMsgAndForward_NilReceiverReturnsError(t *testing.T) {
	sender := &messageSenderOnClient{
		ctx:       context.Background(),
		mp:        mpool.MustNewZero(),
		receiveCh: make(chan morpc.Message, 1),
	}
	sender.receiveCh <- makeRemoteBatchMessage(t, batch.NewWithSize(0))

	err := receiveMsgAndForward(sender, nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "remote batch forward target is nil")
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
	process.PipelineSignalSendTimeout = 200 * time.Millisecond
	t.Cleanup(func() {
		process.PipelineSignalSendTimeout = oldSignalSendTimeout
	})

	reg := process.NewPipelineEdge(1, 0)
	reg.Ch2 <- process.NewPipelineSignalToDirectly(nil, nil, nil)

	start := time.Now()
	require.False(t, sendRemoteNotifyCleanupTerminal(nil, reg, nil))
	elapsed := time.Since(start)

	require.Less(t, elapsed, 300*time.Millisecond)
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

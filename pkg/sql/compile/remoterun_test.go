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
	"bytes"
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/google/uuid"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/hashmap/keycodec"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/morpc"
	mock_morpc "github.com/matrixorigin/matrixone/pkg/common/morpc/mock_morpc"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/common/reuse"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	lockpb "github.com/matrixorigin/matrixone/pkg/pb/lock"
	"github.com/matrixorigin/matrixone/pkg/pb/pipeline"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
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
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/mongoscan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/multi_update"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/offset"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/order"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/partition"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/postdml"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/preinsert"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/preinsertunique"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/product"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/productl2"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/projection"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/rightdedupjoin"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/runtimefilter"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/shuffle"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/table_function"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/top"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/value_scan"
	sqlmongodb "github.com/matrixorigin/matrixone/pkg/sql/mongodb"
	"github.com/matrixorigin/matrixone/pkg/sql/plan"
	planfunction "github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/readutil"
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

	remoteExecutionID := uuid.New()
	data, err := encodeProcessInfo(proc, "", map[string]uint32{
		"cn-a:6001": 2,
		"cn-b:6001": 1,
	}, remoteExecutionID)
	require.Nil(t, err)
	restored := new(pipeline.ProcessInfo)
	require.NoError(t, restored.Unmarshal(data))
	require.Equal(t, map[string]uint32{
		"cn-a:6001": 2,
		"cn-b:6001": 1,
	}, restored.RemoteFragmentCounts)
	restoredExecutionID, err := uuid.FromBytes(restored.RemoteExecutionId)
	require.NoError(t, err)
	require.Equal(t, remoteExecutionID, restoredExecutionID)
}

func TestGenerateProcessHelperRejectsIncompleteRemoteLifecycleMetadata(t *testing.T) {
	tests := []pipeline.ProcessInfo{
		{RemoteFragmentCounts: map[string]uint32{"cn-a:6001": 1}},
		{RemoteExecutionId: func() []byte {
			id := uuid.New()
			return id[:]
		}()},
		{
			RemoteFragmentCounts: map[string]uint32{"cn-a:6001": 1},
			RemoteExecutionId:    []byte{1},
		},
	}
	for i := range tests {
		data, err := tests[i].Marshal()
		require.NoError(t, err)
		_, err = generateProcessHelper(context.Background(), data, nil)
		require.Error(t, err)
	}
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

func TestStringShuffleHashRemoteWireContract(t *testing.T) {
	proc := testutil.NewProcess(t)
	t.Cleanup(proc.Free)
	arg := shuffle.NewArgument()
	t.Cleanup(arg.Release)
	arg.ShuffleType = int32(planpb.ShuffleType_Hash)
	arg.StringHashKey = true

	proc.SetStringShuffleHashAlgorithm(process.StringShuffleHashLegacy)
	_, legacyInstruction, err := convertToPipelineInstruction(
		arg, proc, &scopeContext{}, 1)
	require.NoError(t, err)
	require.Equal(t, int32(vm.Shuffle), legacyInstruction.Op)

	proc.SetStringShuffleHashAlgorithm(process.StringShuffleHashComplete)
	_, stableInstruction, err := convertToPipelineInstruction(
		arg, proc, &scopeContext{}, 1)
	require.NoError(t, err)
	require.Equal(t, int32(vm.ShuffleStable), stableInstruction.Op)
	require.NotEqual(t, legacyInstruction.Op, stableInstruction.Op)

	// Range ownership is unchanged by the complete string hash contract and
	// must not reject an older receiver unnecessarily.
	arg.ShuffleType = int32(planpb.ShuffleType_Range)
	_, rangeInstruction, err := convertToPipelineInstruction(
		arg, proc, &scopeContext{}, 1)
	require.NoError(t, err)
	require.Equal(t, int32(vm.Shuffle), rangeInstruction.Op)

	arg.ShuffleType = int32(planpb.ShuffleType_Hash)
	arg.StringHashKey = false
	_, numericHashInstruction, err := convertToPipelineInstruction(
		arg, proc, &scopeContext{}, 1)
	require.NoError(t, err)
	require.Equal(t, int32(vm.Shuffle), numericHashInstruction.Op)

	encodePipeline := func(t *testing.T, instruction *pipeline.Instruction) []byte {
		t.Helper()
		data, err := (&pipeline.Pipeline{
			PipelineType:    pipeline.Pipeline_Normal,
			InstructionList: []*pipeline.Instruction{instruction},
		}).Marshal()
		require.NoError(t, err)
		return data
	}
	legacyData := encodePipeline(t, legacyInstruction)
	stableData := encodePipeline(t, stableInstruction)
	rangeData := encodePipeline(t, rangeInstruction)
	numericHashData := encodePipeline(t, numericHashInstruction)

	decode := func(algorithm process.StringShuffleHashAlgorithm, data []byte) (*Scope, error) {
		receiverProc := testutil.NewProcess(t)
		t.Cleanup(receiverProc.Free)
		receiverProc.SetStringShuffleHashAlgorithm(algorithm)
		return decodeScope(data, receiverProc, true, nil)
	}

	legacyScope, err := decode(process.StringShuffleHashLegacy, legacyData)
	require.NoError(t, err)
	require.IsType(t, &shuffle.Shuffle{}, legacyScope.RootOp)
	legacyScope.release()

	stableScope, err := decode(process.StringShuffleHashComplete, stableData)
	require.NoError(t, err)
	require.IsType(t, &shuffle.Shuffle{}, stableScope.RootOp)
	require.True(t, stableScope.RootOp.(*shuffle.Shuffle).StringHashKey)
	stableScope.release()
	rangeScope, err := decode(process.StringShuffleHashComplete, rangeData)
	require.NoError(t, err)
	require.IsType(t, &shuffle.Shuffle{}, rangeScope.RootOp)
	rangeScope.release()
	numericHashScope, err := decode(process.StringShuffleHashComplete, numericHashData)
	require.NoError(t, err)
	require.IsType(t, &shuffle.Shuffle{}, numericHashScope.RootOp)
	require.False(t, numericHashScope.RootOp.(*shuffle.Shuffle).StringHashKey)
	numericHashScope.release()

	_, err = decode(process.StringShuffleHashLegacy, stableData)
	require.ErrorContains(t, err,
		"string shuffle hash algorithm mismatch: pipeline=1 process=0")
	_, err = decode(process.StringShuffleHashComplete, legacyData)
	require.ErrorContains(t, err,
		"string shuffle hash algorithm mismatch: pipeline=0 process=1")
	invalidStableRange := *rangeInstruction
	invalidStableRange.Op = int32(vm.ShuffleStable)
	_, err = decode(process.StringShuffleHashComplete,
		encodePipeline(t, &invalidStableRange))
	require.ErrorContains(t, err,
		"complete string shuffle hash marker requires a string-key hash shuffle")
}

func TestRemoteRunOperatorCodecRoundTrip(t *testing.T) {
	ctx := &scopeContext{
		id:     1,
		root:   &scopeContext{},
		parent: &scopeContext{},
	}
	proc := testutil.NewProcess(t)
	rt := moruntime.ServiceRuntime(proc.GetService())
	oldVersion, hadVersion := rt.GetGlobalVariables(moruntime.MOProtocolVersion)
	rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCLatestVersion)
	t.Cleanup(func() {
		if hadVersion {
			rt.SetGlobalVariables(moruntime.MOProtocolVersion, oldVersion)
		} else {
			rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCLatestVersion)
		}
	})

	roundTrip := func(t *testing.T, original vm.Operator) vm.Operator {
		t.Helper()

		_, instruction, err := convertToPipelineInstruction(original, proc, ctx, 1)
		require.NoError(t, err)

		data, err := instruction.Marshal()
		require.NoError(t, err)
		wireInstruction := new(pipeline.Instruction)
		require.NoError(t, wireInstruction.Unmarshal(data))

		restored, err := convertToVmOperator(wireInstruction, ctx, nil)
		require.NoError(t, err)
		return restored
	}

	t.Run("ProductL2", func(t *testing.T) {
		original := &productl2.Productl2{
			Result:       []colexec.ResultPos{{Rel: 1, Pos: 2}},
			OnExpr:       plan.MakePlan2Int64ConstExprWithType(7),
			JoinMapTag:   9,
			VectorOpType: "l2_distance",
		}

		restored := roundTrip(t, original)
		defer restored.Release()
		restoredProductL2, ok := restored.(*productl2.Productl2)
		require.True(t, ok)
		require.Equal(t, original.Result, restoredProductL2.Result)
		require.Equal(t, original.OnExpr, restoredProductL2.OnExpr)
		require.Equal(t, original.JoinMapTag, restoredProductL2.JoinMapTag)
		require.Equal(t, original.VectorOpType, restoredProductL2.VectorOpType)
	})

	t.Run("HashJoinCompressedRowCountContract", func(t *testing.T) {
		original := &hashjoin.HashJoin{
			EqConds:                [][]*planpb.Expr{{}, {}},
			EmitCompressedRowCount: true,
		}
		restored := roundTrip(t, original)
		defer restored.Release()
		restoredHashJoin, ok := restored.(*hashjoin.HashJoin)
		require.True(t, ok)
		require.True(t, restoredHashJoin.EmitCompressedRowCount)
	})

	t.Run("IntersectAll", func(t *testing.T) {
		keyExpr := plan.MakePlan2Int64ConstExprWithType(7)
		original := &intersectall.IntersectAll{KeyExprs: []*planpb.Expr{keyExpr}}
		restored := roundTrip(t, original)
		defer restored.Release()
		require.IsType(t, &intersectall.IntersectAll{}, restored)
		require.Equal(t, vm.IntersectAll, restored.OpType())
		require.Equal(t, original.KeyExprs, restored.(*intersectall.IntersectAll).KeyExprs)
	})

	t.Run("Order", func(t *testing.T) {
		original := order.NewArgument()
		original.OrderBySpec = []*planpb.OrderBySpec{{
			Expr: plan.MakePlan2Int64ConstExprWithType(1),
			Flag: planpb.OrderBySpec_DESC,
		}}
		restored := roundTrip(t, original)
		defer restored.Release()
		restoredOrder, ok := restored.(*order.Order)
		require.True(t, ok)
		require.Equal(t, original.OrderBySpec, restoredOrder.OrderBySpec)
		_, ownsAllocation := any(restoredOrder).(executionAllocationAccountOwner)
		require.True(t, ownsAllocation)
	})

	t.Run("GroupMetadata", func(t *testing.T) {
		original := group.NewArgument()
		original.GroupByHashKey = []int32{0, 2}
		original.DynamicGrouping = true
		restored := roundTrip(t, original)
		defer restored.Release()
		require.Equal(t, original.GroupByHashKey, restored.(*group.Group).GroupByHashKey)
		require.True(t, restored.(*group.Group).DynamicGrouping)
	})

	t.Run("GroupingSetProjectionMetadata", func(t *testing.T) {
		original := projection.NewArgument()
		original.ProjectList = []*planpb.Expr{plan.MakePlan2Int64ConstExprWithType(1)}
		original.GroupingFlags = []bool{true, true, true, false, false, false}
		original.GroupingSetCount = 3
		restored := roundTrip(t, original)
		defer restored.Release()
		restoredProjection, ok := restored.(*projection.Projection)
		require.True(t, ok)
		require.Equal(t, original.ProjectList, restoredProjection.ProjectList)
		require.Equal(t, original.GroupingFlags, restoredProjection.GroupingFlags)
		require.Equal(t, original.GroupingSetCount, restoredProjection.GroupingSetCount)
	})

	t.Run("MergeGroupByHashKey", func(t *testing.T) {
		original := group.NewArgumentMergeGroup()
		original.GroupByHashKey = []int32{1}
		restored := roundTrip(t, original)
		defer restored.Release()
		require.Equal(t, original.GroupByHashKey, restored.(*group.MergeGroup).GroupByHashKey)
	})

	t.Run("MergeGroupGroupingSetMetadata", func(t *testing.T) {
		original := group.NewArgumentMergeGroup()
		original.GroupingAware = true
		original.GroupByTypes = []types.Type{types.T_varchar.ToType(), types.T_int64.ToType()}
		original.EmptyGroupingSetIDs = []int64{1, 3}
		restored := roundTrip(t, original)
		defer restored.Release()
		require.True(t, restored.(*group.MergeGroup).GroupingAware)
		require.Equal(t, original.GroupByTypes, restored.(*group.MergeGroup).GroupByTypes)
		require.Equal(t, original.EmptyGroupingSetIDs, restored.(*group.MergeGroup).EmptyGroupingSetIDs)
	})

	t.Run("MergeGroupLegacyEmptyGroupingSetMetadata", func(t *testing.T) {
		original := group.NewArgumentMergeGroup()
		original.GroupingAware = true
		original.GroupByTypes = []types.Type{types.T_int32.ToType()}
		original.EmptyGroupingSet = true
		restored := roundTrip(t, original)
		defer restored.Release()
		require.True(t, restored.(*group.MergeGroup).EmptyGroupingSet)
		require.Equal(t, original.GroupByTypes, restored.(*group.MergeGroup).GroupByTypes)
	})

	t.Run("SharedTableLock", func(t *testing.T) {
		original := lockop.NewArgumentByEngine(nil)
		original.AddLockTargetWithMode(42, nil, lockpb.LockMode_Shared, 0,
			types.T_int64.ToType(), -1, -1, nil, false)
		original.LockTableWithMode(42, lockpb.LockMode_Shared, false)

		restored := roundTrip(t, original)
		defer restored.Release()
		restoredLock, ok := restored.(*lockop.LockOp)
		require.True(t, ok)
		targets := restoredLock.CopyToPipelineTarget()
		require.Len(t, targets, 1)
		require.True(t, targets[0].LockTable)
		require.Equal(t, lockpb.LockMode_Shared, targets[0].Mode)
	})

	t.Run("SharedRowLock", func(t *testing.T) {
		original := lockop.NewArgumentByEngine(nil)
		original.AddLockTargetWithMode(43, nil, lockpb.LockMode_Shared, 0,
			types.T_int64.ToType(), -1, -1, nil, false)

		restored := roundTrip(t, original)
		defer restored.Release()
		restoredLock, ok := restored.(*lockop.LockOp)
		require.True(t, ok)
		targets := restoredLock.CopyToPipelineTarget()
		require.Len(t, targets, 1)
		require.False(t, targets[0].LockTable)
		require.Equal(t, lockpb.LockMode_Shared, targets[0].Mode)
	})
}

func TestTargetAwareUpdateRemoteProtocolValidation(t *testing.T) {
	ctx := &scopeContext{id: 1, root: &scopeContext{}, parent: &scopeContext{}}
	proc := testutil.NewProcess(t)
	rt := moruntime.ServiceRuntime(proc.GetService())
	oldVersion, hadVersion := rt.GetGlobalVariables(moruntime.MOProtocolVersion)
	t.Cleanup(func() {
		if hadVersion {
			rt.SetGlobalVariables(moruntime.MOProtocolVersion, oldVersion)
		} else {
			rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCLatestVersion)
		}
	})

	selectorPreInsert := &preinsert.PreInsert{HasTargetSelector: true}
	targetAwareMultiUpdate := &multi_update.MultiUpdate{
		MultiUpdateCtx: []*multi_update.MultiUpdateCtx{{
			DedupByTargetRowID: true,
		}},
	}
	targetIndexedMultiUpdate := &multi_update.MultiUpdate{
		MultiUpdateCtx: []*multi_update.MultiUpdateCtx{{
			TargetUpdateCtxIdx: 1,
		}},
	}
	affectedRowsMultiUpdate := &multi_update.MultiUpdate{
		MultiUpdateCtx: []*multi_update.MultiUpdateCtx{{
			AffectedRowsCols: []int{9, 10},
		}},
	}

	rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion19)
	require.NoError(t, validateRemoteTargetAwareUpdatePipelineProtocol(proc, nil))
	require.NoError(t, validateRemoteTargetAwareUpdatePipelineProtocol(proc, &pipeline.Pipeline{}))
	_, _, err := convertToPipelineInstruction(selectorPreInsert, proc, ctx, 1)
	require.ErrorContains(t, err, "requires MORPC protocol version 20")
	_, _, err = convertToPipelineInstruction(targetAwareMultiUpdate, proc, ctx, 1)
	require.ErrorContains(t, err, "requires MORPC protocol version 20")
	_, _, err = convertToPipelineInstruction(targetIndexedMultiUpdate, proc, ctx, 1)
	require.ErrorContains(t, err, "requires MORPC protocol version 20")
	targetAwarePipeline := &pipeline.Pipeline{Children: []*pipeline.Pipeline{{
		InstructionList: []*pipeline.Instruction{{
			Op: int32(vm.PreInsert),
			PreInsert: &pipeline.PreInsert{
				HasTargetSelector: true,
			},
		}},
	}}}
	require.ErrorContains(t,
		validateRemoteTargetAwareUpdatePipelineProtocol(proc, targetAwarePipeline),
		"requires MORPC protocol version 20")
	targetAwareMultiUpdatePipeline := &pipeline.Pipeline{
		InstructionList: []*pipeline.Instruction{{
			Op: int32(vm.MultiUpdate),
			MultiUpdate: &pipeline.MultiUpdate{
				UpdateCtxList: []*planpb.UpdateCtx{{DedupByTargetRowId: true}},
			},
		}},
	}
	require.ErrorContains(t,
		validateRemoteTargetAwareUpdatePipelineProtocol(proc, targetAwareMultiUpdatePipeline),
		"requires MORPC protocol version 20")

	_, _, err = convertToPipelineInstruction(&preinsert.PreInsert{}, proc, ctx, 1)
	require.NoError(t, err, "legacy PRE_INSERT stays wire-compatible")
	_, _, err = convertToPipelineInstruction(&multi_update.MultiUpdate{
		MultiUpdateCtx: []*multi_update.MultiUpdateCtx{{}},
	}, proc, ctx, 1)
	require.NoError(t, err, "non-target-aware MULTI_UPDATE stays wire-compatible")

	rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion20)
	_, _, err = convertToPipelineInstruction(selectorPreInsert, proc, ctx, 1)
	require.NoError(t, err)
	_, _, err = convertToPipelineInstruction(targetAwareMultiUpdate, proc, ctx, 1)
	require.NoError(t, err)
	_, _, err = convertToPipelineInstruction(targetIndexedMultiUpdate, proc, ctx, 1)
	require.NoError(t, err)
	require.NoError(t, validateRemoteTargetAwareUpdatePipelineProtocol(proc, targetAwarePipeline))
	_, _, err = convertToPipelineInstruction(affectedRowsMultiUpdate, proc, ctx, 1)
	require.ErrorContains(t, err, "requires MORPC protocol version 24")
	affectedRowsPipeline := &pipeline.Pipeline{
		InstructionList: []*pipeline.Instruction{{
			Op: int32(vm.MultiUpdate),
			MultiUpdate: &pipeline.MultiUpdate{
				UpdateCtxList: []*planpb.UpdateCtx{{
					AffectedRowsCols: []planpb.ColRef{{ColPos: 9}, {ColPos: 10}},
				}},
			},
		}},
	}
	require.ErrorContains(t,
		validateRemoteTargetAwareUpdatePipelineProtocol(proc, affectedRowsPipeline),
		"requires MORPC protocol version 24")
	combinedPipeline := &pipeline.Pipeline{
		InstructionList: append(targetAwarePipeline.Children[0].InstructionList, affectedRowsPipeline.InstructionList...),
	}
	require.ErrorContains(t,
		validateRemoteTargetAwareUpdatePipelineProtocol(proc, combinedPipeline),
		"requires MORPC protocol version 24",
		"a preceding v20-compatible operator must not hide a later v24 field")

	rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion21)
	_, _, err = convertToPipelineInstruction(affectedRowsMultiUpdate, proc, ctx, 1)
	require.ErrorContains(t, err, "requires MORPC protocol version 24",
		"v21 is reserved for RIGHT DEDUP and must not accept affected-row selectors")
	require.ErrorContains(t,
		validateRemoteTargetAwareUpdatePipelineProtocol(proc, affectedRowsPipeline),
		"requires MORPC protocol version 24")

	rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion22)
	_, _, err = convertToPipelineInstruction(affectedRowsMultiUpdate, proc, ctx, 1)
	require.ErrorContains(t, err, "requires MORPC protocol version 24",
		"v22 is reserved for typed user variables and must not accept affected-row selectors")
	require.ErrorContains(t,
		validateRemoteTargetAwareUpdatePipelineProtocol(proc, affectedRowsPipeline),
		"requires MORPC protocol version 24")

	rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion23)
	_, _, err = convertToPipelineInstruction(affectedRowsMultiUpdate, proc, ctx, 1)
	require.ErrorContains(t, err, "requires MORPC protocol version 24",
		"v23 is reserved for explicit-text provenance and must not accept affected-row selectors")
	require.ErrorContains(t,
		validateRemoteTargetAwareUpdatePipelineProtocol(proc, affectedRowsPipeline),
		"requires MORPC protocol version 24")

	rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion24)
	_, _, err = convertToPipelineInstruction(affectedRowsMultiUpdate, proc, ctx, 1)
	require.NoError(t, err)
	require.NoError(t, validateRemoteTargetAwareUpdatePipelineProtocol(proc, affectedRowsPipeline))
	require.NoError(t, validateRemoteTargetAwareUpdatePipelineProtocol(proc, combinedPipeline))
}

func TestRemoteAutoIncrementStatementLastInsertIDProtocolValidation(t *testing.T) {
	ctx := &scopeContext{id: 1, root: &scopeContext{}, parent: &scopeContext{}}
	proc := testutil.NewProcess(t)
	rt := moruntime.ServiceRuntime(proc.GetService())
	oldVersion, hadVersion := rt.GetGlobalVariables(moruntime.MOProtocolVersion)
	t.Cleanup(func() {
		if hadVersion {
			rt.SetGlobalVariables(moruntime.MOProtocolVersion, oldVersion)
		} else {
			rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCLatestVersion)
		}
	})

	autoPreInsert := &preinsert.PreInsert{HasAutoCol: true}
	ordinaryPreInsert := &preinsert.PreInsert{}
	autoPipeline := &pipeline.Pipeline{Children: []*pipeline.Pipeline{{
		InstructionList: []*pipeline.Instruction{{
			Op:        int32(vm.PreInsert),
			PreInsert: &pipeline.PreInsert{HasAutoCol: true},
		}},
	}}}

	rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion25)
	_, _, err := convertToPipelineInstruction(autoPreInsert, proc, ctx, 1)
	require.ErrorContains(t, err, "requires MORPC protocol version 26")
	require.ErrorContains(t,
		validateRemoteStatementLastInsertIDPipelineProtocol(proc, autoPipeline),
		"requires MORPC protocol version 26")
	encodedPipeline, err := autoPipeline.Marshal()
	require.NoError(t, err)
	_, err = decodeScope(encodedPipeline, proc, true, nil)
	require.ErrorContains(t, err, "requires MORPC protocol version 26")

	_, _, err = convertToPipelineInstruction(ordinaryPreInsert, proc, ctx, 1)
	require.NoError(t, err, "PRE_INSERT without generated IDs remains wire-compatible")

	rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion26)
	_, instruction, err := convertToPipelineInstruction(autoPreInsert, proc, ctx, 1)
	require.NoError(t, err)
	require.True(t, instruction.PreInsert.HasAutoCol)
	require.NoError(t,
		validateRemoteStatementLastInsertIDPipelineProtocol(proc, autoPipeline))
	decoded, err := decodeScope(encodedPipeline, proc, true, nil)
	require.NoError(t, err)
	decoded.release()
}

func TestChangedRowsUpdateRemoteProtocolValidation(t *testing.T) {
	ctx := &scopeContext{id: 1, root: &scopeContext{}, parent: &scopeContext{}}
	proc := testutil.NewProcess(t)
	rt := moruntime.ServiceRuntime(proc.GetService())
	oldVersion, hadVersion := rt.GetGlobalVariables(moruntime.MOProtocolVersion)
	t.Cleanup(func() {
		if hadVersion {
			rt.SetGlobalVariables(moruntime.MOProtocolVersion, oldVersion)
		} else {
			rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCLatestVersion)
		}
	})

	changedRowsCol := 7
	changedRowsUpdate := &multi_update.MultiUpdate{
		MultiUpdateCtx: []*multi_update.MultiUpdateCtx{{ChangedRowsCol: &changedRowsCol}},
	}
	changedRowsPipeline := &pipeline.Pipeline{Children: []*pipeline.Pipeline{{
		InstructionList: []*pipeline.Instruction{{
			Op: int32(vm.MultiUpdate),
			MultiUpdate: &pipeline.MultiUpdate{
				UpdateCtxList: []*planpb.UpdateCtx{{
					ChangedRowsCol: &planpb.ColRef{ColPos: int32(changedRowsCol)},
				}},
			},
		}},
	}}}

	rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion24)
	_, _, err := convertToPipelineInstruction(changedRowsUpdate, proc, ctx, 1)
	require.ErrorContains(t, err, "requires MORPC protocol version 25")
	require.ErrorContains(t,
		validateRemoteUpdateChangedRowsPipelineProtocol(proc, changedRowsPipeline),
		"requires MORPC protocol version 25")
	encodedPipeline, err := changedRowsPipeline.Marshal()
	require.NoError(t, err)
	_, err = decodeScope(encodedPipeline, proc, true, nil)
	require.ErrorContains(t, err, "requires MORPC protocol version 25")

	legacyUpdate := &multi_update.MultiUpdate{
		MultiUpdateCtx: []*multi_update.MultiUpdateCtx{{}},
	}
	_, _, err = convertToPipelineInstruction(legacyUpdate, proc, ctx, 1)
	require.NoError(t, err, "legacy MULTI_UPDATE stays wire-compatible")
	require.NoError(t, validateRemoteUpdateChangedRowsPipelineProtocol(proc, &pipeline.Pipeline{}))

	rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion25)
	_, instruction, err := convertToPipelineInstruction(changedRowsUpdate, proc, ctx, 1)
	require.NoError(t, err)
	require.NotNil(t, instruction.GetMultiUpdate().UpdateCtxList[0].ChangedRowsCol)
	require.Equal(t, int32(changedRowsCol), instruction.GetMultiUpdate().UpdateCtxList[0].ChangedRowsCol.ColPos)
	require.NoError(t, validateRemoteUpdateChangedRowsPipelineProtocol(proc, changedRowsPipeline))
}

func TestRightDedupInputUniqueRemoteProtocolValidation(t *testing.T) {
	ctx := &scopeContext{id: 1, root: &scopeContext{}, parent: &scopeContext{}}
	proc := testutil.NewProcess(t)
	rt := moruntime.ServiceRuntime(proc.GetService())
	oldVersion, hadVersion := rt.GetGlobalVariables(moruntime.MOProtocolVersion)
	t.Cleanup(func() {
		if hadVersion {
			rt.SetGlobalVariables(moruntime.MOProtocolVersion, oldVersion)
		} else {
			rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCLatestVersion)
		}
	})

	unique := &rightdedupjoin.RightDedupJoin{
		Conditions:      [][]*planpb.Expr{{}, {}},
		InputKeysUnique: true,
	}
	ordinary := &rightdedupjoin.RightDedupJoin{Conditions: [][]*planpb.Expr{{}, {}}}
	uniquePipeline := &pipeline.Pipeline{Children: []*pipeline.Pipeline{{
		InstructionList: []*pipeline.Instruction{{
			Op: int32(vm.RightDedupJoin),
			RightDedupJoin: &pipeline.RightDedupJoin{
				InputKeysUnique: true,
			},
		}},
	}}}
	ordinaryPipeline := &pipeline.Pipeline{InstructionList: []*pipeline.Instruction{{
		Op:             int32(vm.RightDedupJoin),
		RightDedupJoin: &pipeline.RightDedupJoin{},
	}}}

	rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion20)
	_, _, err := convertToPipelineInstruction(unique, proc, ctx, 1)
	require.ErrorContains(t, err, "requires MORPC protocol version 21")
	require.ErrorContains(t,
		validateRemoteRightDedupInputKeysUniquePipelineProtocol(proc, uniquePipeline),
		"requires MORPC protocol version 21")
	encodedPipeline, err := uniquePipeline.Marshal()
	require.NoError(t, err)
	_, err = decodeScope(encodedPipeline, proc, true, nil)
	require.ErrorContains(t, err, "requires MORPC protocol version 21")
	_, _, err = convertToPipelineInstruction(ordinary, proc, ctx, 1)
	require.NoError(t, err, "ordinary RIGHT DEDUP remains wire-compatible")
	require.NoError(t,
		validateRemoteRightDedupInputKeysUniquePipelineProtocol(proc, ordinaryPipeline))

	rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion21)
	_, instruction, err := convertToPipelineInstruction(unique, proc, ctx, 1)
	require.NoError(t, err)
	require.True(t, instruction.RightDedupJoin.InputKeysUnique)
	require.NoError(t,
		validateRemoteRightDedupInputKeysUniquePipelineProtocol(proc, uniquePipeline))
}

func TestCrossDomainStringLiteralRemoteProtocolValidation(t *testing.T) {
	proc := testutil.NewProcess(t)
	proc.Ctx = context.WithValue(proc.Ctx, defines.TenantIDKey{}, uint32(0))
	proc.Base.TxnOperator = fakeTxnOperator{}
	proc.Base.SessionInfo.TimeZone = time.UTC
	rt := moruntime.ServiceRuntime(proc.GetService())
	oldVersion, hadVersion := rt.GetGlobalVariables(moruntime.MOProtocolVersion)
	t.Cleanup(func() {
		if hadVersion {
			rt.SetGlobalVariables(moruntime.MOProtocolVersion, oldVersion)
		} else {
			rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCLatestVersion)
		}
	})

	makeScope := func(typ types.Type, form planpb.StringLiteralForm) *Scope {
		literal := &planpb.Expr{
			Typ: planpb.Type{Id: int32(typ.Oid), Charset: uint32(typ.Charset)},
			Expr: &planpb.Expr_Lit{Lit: &planpb.Literal{
				Value: &planpb.Literal_Sval{Sval: "selected"}, LiteralForm: form,
			}},
		}
		return &Scope{
			Magic:  Remote,
			Proc:   proc,
			RootOp: value_scan.NewArgument(),
			Plan: &planpb.Plan{Plan: &planpb.Plan_Query{Query: &planpb.Query{
				Steps: []int32{0},
				Nodes: []*planpb.Node{{NodeId: 0, ProjectList: []*planpb.Expr{literal}}},
			}}},
		}
	}
	makeDynamicScope := func() *Scope {
		boolColumn := &planpb.Expr{Typ: planpb.Type{Id: int32(types.T_bool)},
			Expr: &planpb.Expr_Col{Col: &planpb.ColRef{ColPos: 0}}}
		textColumn := &planpb.Expr{Typ: planpb.Type{Id: int32(types.T_varchar)},
			Expr: &planpb.Expr_Col{Col: &planpb.ColRef{ColPos: 1}}}
		binaryType := planpb.Type{Id: int32(types.T_varbinary), Charset: uint32(types.CharsetBinary)}
		binaryColumn := &planpb.Expr{Typ: binaryType,
			Expr: &planpb.Expr_Col{Col: &planpb.ColRef{ColPos: 2}}}
		implicitCast := &planpb.Expr{Typ: binaryType, Expr: &planpb.Expr_F{F: &planpb.Function{
			Func: &planpb.ObjectRef{ObjName: "cast"}, Args: []*planpb.Expr{textColumn},
		}}}
		dynamic := &planpb.Expr{Typ: binaryType, Expr: &planpb.Expr_F{F: &planpb.Function{
			Func: &planpb.ObjectRef{ObjName: "if"}, Args: []*planpb.Expr{boolColumn, implicitCast, binaryColumn},
		}}}
		return &Scope{
			Magic: Remote, Proc: proc, RootOp: value_scan.NewArgument(),
			Plan: &planpb.Plan{Plan: &planpb.Plan_Query{Query: &planpb.Query{
				Steps: []int32{0}, Nodes: []*planpb.Node{{NodeId: 0, ProjectList: []*planpb.Expr{dynamic}}},
			}}},
		}
	}

	rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion21)
	_, _, _, _, err := prepareRemoteRunSendingData(
		"", makeScope(types.T_varbinary.ToType(), planpb.StringLiteralForm_STRING_LITERAL_TEXT),
		proc, nil, uuid.Nil)
	require.ErrorContains(t, err, "requires MORPC protocol version 23")
	_, _, _, _, err = prepareRemoteRunSendingData("", makeDynamicScope(), proc, nil, uuid.Nil)
	require.ErrorContains(t, err, "requires MORPC protocol version 23")

	compatibleData, _, _, _, err := prepareRemoteRunSendingData(
		"", makeScope(types.T_varchar.ToType(), planpb.StringLiteralForm_STRING_LITERAL_TEXT),
		proc, nil, uuid.Nil)
	require.NoError(t, err, "same-domain ordinary TEXT remains compatible with version 21")
	compatibleScope, err := decodeScope(compatibleData, proc, true, nil)
	require.NoError(t, err)
	compatibleScope.release()

	rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion22)
	_, _, _, _, err = prepareRemoteRunSendingData(
		"", makeScope(types.T_varbinary.ToType(), planpb.StringLiteralForm_STRING_LITERAL_TEXT),
		proc, nil, uuid.Nil)
	require.ErrorContains(t, err, "requires MORPC protocol version 23",
		"version 22 predates cross-domain literal provenance")
	_, _, _, _, err = prepareRemoteRunSendingData("", makeDynamicScope(), proc, nil, uuid.Nil)
	require.ErrorContains(t, err, "requires MORPC protocol version 23",
		"version 22 predates runtime string provenance")

	rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion23)
	dynamicData, _, _, _, err := prepareRemoteRunSendingData("", makeDynamicScope(), proc, nil, uuid.Nil)
	require.NoError(t, err, "version 23 accepts dynamic selected-value provenance")
	dynamicScope, err := decodeScope(dynamicData, proc, true, nil)
	require.NoError(t, err)
	dynamicScope.release()
	scopeData, _, _, _, err := prepareRemoteRunSendingData(
		"", makeScope(types.T_varbinary.ToType(), planpb.StringLiteralForm_STRING_LITERAL_TEXT),
		proc, nil, uuid.Nil)
	require.NoError(t, err)
	restored, err := decodeScope(scopeData, proc, true, nil)
	require.NoError(t, err)
	defer restored.release()
	wirePipeline := &pipeline.Pipeline{}
	require.NoError(t, wirePipeline.Unmarshal(scopeData))
	require.Equal(t, planpb.StringLiteralForm_STRING_LITERAL_TEXT,
		wirePipeline.Qry.GetQuery().Nodes[0].ProjectList[0].GetLit().LiteralForm)

	rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion22)
	_, err = decodeScope(scopeData, proc, true, nil)
	require.ErrorContains(t, err, "requires MORPC protocol version 23")
	_, err = decodeScope(dynamicData, proc, true, nil)
	require.ErrorContains(t, err, "requires MORPC protocol version 23")
}

func TestPrepareRemoteRunSendingDataRejectsPrePadSpaceProtocol(t *testing.T) {
	newProc := func(sqlMode string) *process.Process {
		proc := newResolveVariableProcess(t, sqlMode)
		proc.Ctx = context.WithValue(proc.Ctx, defines.TenantIDKey{}, uint32(0))
		proc.Base.TxnOperator = fakeTxnOperator{}
		proc.Base.SessionInfo.TimeZone = time.UTC
		return proc
	}
	makeScope := func(proc *process.Process, expression *planpb.Expr) *Scope {
		return &Scope{
			Magic:  Remote,
			Proc:   proc,
			RootOp: value_scan.NewArgument(),
			Plan: &planpb.Plan{Plan: &planpb.Plan_Query{Query: &planpb.Query{
				Steps: []int32{0}, Nodes: []*planpb.Node{{NodeId: 0, ProjectList: []*planpb.Expr{expression}}},
			}}},
		}
	}
	padSpaceCast := &planpb.Expr{
		Typ: planpb.Type{Id: int32(types.T_varchar)},
		Expr: &planpb.Expr_F{F: &planpb.Function{
			Func: &planpb.ObjectRef{
				Obj:     planfunction.EncodeOverloadID(planfunction.CAST, 3),
				ObjName: "cast",
			},
			Args: []*planpb.Expr{plan.MakePlan2StringConstExprWithType("MO", false)},
		}},
	}
	ordinaryValue := plan.MakePlan2StringConstExprWithType("MO", false)

	for _, tc := range []struct {
		name       string
		sqlMode    string
		expression *planpb.Expr
	}{
		{
			name:       "PAD SPACE cast",
			expression: padSpaceCast,
		},
		{
			name:       "enabled mode",
			sqlMode:    "PAD_CHAR_TO_FULL_LENGTH",
			expression: ordinaryValue,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			proc := newProc(tc.sqlMode)
			scope := makeScope(proc, tc.expression)
			rt := moruntime.ServiceRuntime(proc.GetService())
			oldVersion, hadVersion := rt.GetGlobalVariables(moruntime.MOProtocolVersion)
			t.Cleanup(func() {
				if hadVersion {
					rt.SetGlobalVariables(moruntime.MOProtocolVersion, oldVersion)
				} else {
					rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCLatestVersion)
				}
			})

			rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion39)
			_, _, _, _, err := prepareRemoteRunSendingData("", scope, proc, nil, uuid.Nil)
			require.ErrorContains(t, err, "PAD SPACE remote execution requires MORPC protocol version 40")
			require.True(t, moerr.IsMoErrCode(err, moerr.ErrNotSupported))

			rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion40)
			_, _, _, _, err = prepareRemoteRunSendingData("", scope, proc, nil, uuid.Nil)
			require.NoError(t, err)
		})
	}
}

func TestRemoteExpressionProtocolValidation(t *testing.T) {
	require.GreaterOrEqual(t, defines.MORPCLatestVersion, defines.MORPCVersion36,
		"the v36 remote-expression capability must remain available after later protocol increments")

	proc := testutil.NewProcess(t)
	proc.Ctx = context.WithValue(proc.Ctx, defines.TenantIDKey{}, uint32(0))
	proc.Base.TxnOperator = fakeTxnOperator{}
	proc.Base.SessionInfo.TimeZone = time.UTC
	rt := moruntime.ServiceRuntime(proc.GetService())
	oldVersion, hadVersion := rt.GetGlobalVariables(moruntime.MOProtocolVersion)
	t.Cleanup(func() {
		if hadVersion {
			rt.SetGlobalVariables(moruntime.MOProtocolVersion, oldVersion)
		} else {
			rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCLatestVersion)
		}
	})

	cast := func(charset uint32) *planpb.Expr {
		text := &planpb.Expr{
			Typ:  planpb.Type{Id: int32(types.T_text)},
			Expr: &planpb.Expr_Lit{Lit: &planpb.Literal{Value: &planpb.Literal_Sval{Sval: "12.5tail"}}},
		}
		return &planpb.Expr{
			Typ: planpb.Type{Id: int32(types.T_decimal64), Width: 4, Scale: 2, Charset: charset},
			Expr: &planpb.Expr_F{F: &planpb.Function{
				Func: &planpb.ObjectRef{ObjName: "cast"},
				Args: []*planpb.Expr{text},
			}},
		}
	}
	comparisonParam := func(pos int32) *planpb.Expr {
		return &planpb.Expr{
			Typ: planpb.Type{Id: int32(types.T_json)},
			Expr: &planpb.Expr_F{F: &planpb.Function{
				Func: &planpb.ObjectRef{
					Obj:     int64(577) << 32,
					ObjName: "__mo_json_comparison_param",
				},
				Args: []*planpb.Expr{{
					Typ:  planpb.Type{Id: int32(types.T_text)},
					Expr: &planpb.Expr_P{P: &planpb.ParamRef{Pos: pos}},
				}},
			}},
		}
	}
	mixedJSONBooleanEquality := func(functionID int32, jsonOnLeft bool) *planpb.Expr {
		jsonOperand := &planpb.Expr{
			Typ:  planpb.Type{Id: int32(types.T_json)},
			Expr: &planpb.Expr_Col{Col: &planpb.ColRef{ColPos: 0}},
		}
		booleanOperand := &planpb.Expr{
			Typ:  planpb.Type{Id: int32(types.T_bool)},
			Expr: &planpb.Expr_Col{Col: &planpb.ColRef{ColPos: 1}},
		}
		args := []*planpb.Expr{jsonOperand, booleanOperand}
		if !jsonOnLeft {
			args[0], args[1] = args[1], args[0]
		}
		return &planpb.Expr{
			Typ: planpb.Type{Id: int32(types.T_bool)},
			Expr: &planpb.Expr_F{F: &planpb.Function{
				Func: &planpb.ObjectRef{Obj: int64(functionID) << 32},
				Args: args,
			}},
		}
	}
	makeScope := func(expressions ...*planpb.Expr) *Scope {
		return &Scope{
			Magic:  Remote,
			Proc:   proc,
			RootOp: value_scan.NewArgument(),
			Plan: &planpb.Plan{Plan: &planpb.Plan_Query{Query: &planpb.Query{
				Steps: []int32{0}, Nodes: []*planpb.Node{{NodeId: 0, ProjectList: expressions}},
			}}},
		}
	}
	t.Run("instruction expression owner", func(t *testing.T) {
		remotePipeline := &pipeline.Pipeline{
			InstructionList: []*pipeline.Instruction{{
				ProjectList: []*planpb.Expr{comparisonParam(0), comparisonParam(1)},
			}},
		}
		rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion35)
		err := validateRemoteExpressionPipelineProtocol(proc, remotePipeline)
		require.ErrorContains(t, err, "prepared JSON comparison parameters require MORPC protocol version 36")
		require.True(t, moerr.IsMoErrCode(err, moerr.ErrNotSupported))

		rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion36)
		require.NoError(t, validateRemoteExpressionPipelineProtocol(proc, remotePipeline))
	})
	t.Run("mixed equality instruction expression owner", func(t *testing.T) {
		remotePipeline := &pipeline.Pipeline{
			InstructionList: []*pipeline.Instruction{{
				ProjectList: []*planpb.Expr{mixedJSONBooleanEquality(0, true)},
			}},
		}
		rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion35)
		err := validateRemoteExpressionPipelineProtocol(proc, remotePipeline)
		require.ErrorContains(t, err, "mixed JSON/BOOL equality requires MORPC protocol version 36")
		require.True(t, moerr.IsMoErrCode(err, moerr.ErrNotSupported))

		rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion36)
		require.NoError(t, validateRemoteExpressionPipelineProtocol(proc, remotePipeline))
	})

	tests := []struct {
		name                string
		expressions         []*planpb.Expr
		incompatibleVersion int64
		compatibleVersion   int64
		errorContains       string
	}{
		{
			name:                "ordinary cast",
			expressions:         []*planpb.Expr{cast(0)},
			incompatibleVersion: defines.MORPCVersion29,
			compatibleVersion:   defines.MORPCVersion36,
		},
		{
			name:                "numeric prefix only",
			expressions:         []*planpb.Expr{cast(255)},
			incompatibleVersion: defines.MORPCVersion29,
			compatibleVersion:   defines.MORPCVersion30,
			errorContains:       "prepared numeric-prefix casts require MORPC protocol version 30",
		},
		{
			name:                "JSON comparison only",
			expressions:         []*planpb.Expr{comparisonParam(0), comparisonParam(1)},
			incompatibleVersion: defines.MORPCVersion35,
			compatibleVersion:   defines.MORPCVersion36,
			errorContains:       "prepared JSON comparison parameters require MORPC protocol version 36",
		},
		{
			name:                "numeric prefix and JSON comparison",
			expressions:         []*planpb.Expr{comparisonParam(0), cast(255)},
			incompatibleVersion: defines.MORPCVersion35,
			compatibleVersion:   defines.MORPCVersion36,
			errorContains:       "prepared JSON comparison parameters require MORPC protocol version 36",
		},
	}
	for _, functionID := range []int32{0, 1, 406} {
		for _, jsonOnLeft := range []bool{true, false} {
			orientation := "json right"
			if jsonOnLeft {
				orientation = "json left"
			}
			tests = append(tests, struct {
				name                string
				expressions         []*planpb.Expr
				incompatibleVersion int64
				compatibleVersion   int64
				errorContains       string
			}{
				name:                fmt.Sprintf("mixed equality %d %s", functionID, orientation),
				expressions:         []*planpb.Expr{mixedJSONBooleanEquality(functionID, jsonOnLeft)},
				incompatibleVersion: defines.MORPCVersion35,
				compatibleVersion:   defines.MORPCVersion36,
				errorContains:       "mixed JSON/BOOL equality requires MORPC protocol version 36",
			})
		}
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			rt.SetGlobalVariables(moruntime.MOProtocolVersion, test.incompatibleVersion)
			incompatibleData, _, _, _, err := prepareRemoteRunSendingData(
				"", makeScope(test.expressions...), proc, nil, uuid.Nil)
			if test.errorContains != "" {
				require.ErrorContains(t, err, test.errorContains)
				require.True(t, moerr.IsMoErrCode(err, moerr.ErrNotSupported))
			} else {
				require.NoError(t, err)
				decoded, decodeErr := decodeScope(incompatibleData, proc, true, nil)
				require.NoError(t, decodeErr)
				decoded.release()
			}

			rt.SetGlobalVariables(moruntime.MOProtocolVersion, test.compatibleVersion)
			compatibleData, _, _, _, err := prepareRemoteRunSendingData(
				"", makeScope(test.expressions...), proc, nil, uuid.Nil)
			require.NoError(t, err)
			decoded, err := decodeScope(compatibleData, proc, true, nil)
			require.NoError(t, err)
			decoded.release()

			rt.SetGlobalVariables(moruntime.MOProtocolVersion, test.incompatibleVersion)
			decoded, err = decodeScope(compatibleData, proc, true, nil)
			if test.errorContains != "" {
				require.ErrorContains(t, err, test.errorContains)
				require.True(t, moerr.IsMoErrCode(err, moerr.ErrNotSupported))
				require.Nil(t, decoded)

				decoded, err = decodeScope(compatibleData, nil, true, nil)
				require.Error(t, err, "a versioned expression requires a process to resolve protocol support")
				require.True(t, moerr.IsMoErrCode(err, moerr.ErrNotSupported))
				require.Nil(t, decoded)
			} else {
				require.NoError(t, err)
				decoded.release()
			}
		})
	}

	t.Run("combined features enforce each minimum", func(t *testing.T) {
		remotePipeline := &pipeline.Pipeline{
			Qry: makeScope(comparisonParam(0), cast(255)).Plan,
		}

		rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion29)
		err := validateRemoteExpressionPipelineProtocol(proc, remotePipeline)
		require.ErrorContains(t, err, "prepared numeric-prefix casts require MORPC protocol version 30")

		rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion35)
		err = validateRemoteExpressionPipelineProtocol(proc, remotePipeline)
		require.ErrorContains(t, err, "prepared JSON comparison parameters require MORPC protocol version 36")

		rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion36)
		require.NoError(t, validateRemoteExpressionPipelineProtocol(proc, remotePipeline))
	})
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
				StrictSqlMode:         true,
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
	require.True(t, pipeInstr.ExternalScan.StrictSqlMode)

	restored, err := convertToVmOperator(pipeInstr, ctx, nil)
	require.NoError(t, err)
	restoredExternal := restored.(*external.External)
	require.Equal(t, shards, restoredExternal.Es.ParquetRowGroupShards)
	require.True(t, restoredExternal.Es.StrictSqlMode)
}

func TestExternalScanParquetWholeFileFanoutRoundtrip(t *testing.T) {
	ctx := &scopeContext{
		id:     1,
		root:   &scopeContext{},
		parent: &scopeContext{},
	}
	proc := &process.Process{}
	proc.Base = &process.BaseProcess{}

	op := external.NewArgument().WithEs(
		&external.ExternalParam{
			ExParamConst: external.ExParamConst{
				FileList:               []string{"s3://bucket/part.parquet"},
				FileSize:               []int64{8192},
				FileOffsetTotal:        []*pipeline.FileOffset{{Offset: []int64{0, -1}}},
				ParquetWholeFileFanout: true,
			},
			ExParam: external.ExParam{
				Fileparam: &external.ExFileparam{},
				Filter:    &external.FilterParam{},
			},
		},
	)

	_, pipeInstr, err := convertToPipelineInstruction(op, proc, ctx, 1)
	require.NoError(t, err)
	require.True(t, pipeInstr.ExternalScan.ParquetWholeFileFanout)

	restored, err := convertToVmOperator(pipeInstr, ctx, nil)
	require.NoError(t, err)
	restoredExternal := restored.(*external.External)
	require.True(t, restoredExternal.Es.ParquetWholeFileFanout)
	require.Empty(t, restoredExternal.Es.ParquetRowGroupShards)
}

func TestParquetWholeFileFanoutRemoteProtocolValidationAtSendAndReceiveBoundaries(t *testing.T) {
	proc := testutil.NewProcess(t)
	rt := moruntime.ServiceRuntime(proc.GetService())
	previous, hadPrevious := rt.GetGlobalVariables(moruntime.MOProtocolVersion)
	t.Cleanup(func() {
		if hadPrevious {
			rt.SetGlobalVariables(moruntime.MOProtocolVersion, previous)
		} else {
			rt.CompareAndDeleteGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion44)
		}
	})

	scope := &Scope{Proc: proc, RootOp: external.NewArgument().WithEs(
		&external.ExternalParam{
			ExParamConst: external.ExParamConst{ParquetWholeFileFanout: true},
			ExParam:      external.ExParam{Fileparam: &external.ExFileparam{}, Filter: &external.FilterParam{}},
		},
	)}

	rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion45)
	data, err := encodeRemoteScope(scope, proc)
	require.NoError(t, err)
	rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion44)
	_, err = encodeRemoteScope(scope, proc)
	require.ErrorContains(t, err, "MORPC protocol version 45")
	_, err = decodeScope(data, proc, true, nil)
	require.ErrorContains(t, err, "MORPC protocol version 45")
}

func TestGroupingSetRemoteProtocolValidationAtSendAndReceiveBoundaries(t *testing.T) {
	proc := testutil.NewProcess(t)
	rt := moruntime.ServiceRuntime(proc.GetService())
	previous, hadPrevious := rt.GetGlobalVariables(moruntime.MOProtocolVersion)
	t.Cleanup(func() {
		if hadPrevious {
			rt.SetGlobalVariables(moruntime.MOProtocolVersion, previous)
		} else {
			rt.CompareAndDeleteGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion45)
		}
	})

	projectionOp := projection.NewArgument()
	projectionOp.GroupingFlags = []bool{true, false}
	projectionOp.GroupingSetCount = 2
	groupOp := group.NewArgument()
	groupOp.DynamicGrouping = true
	mergeGroupOp := group.NewArgumentMergeGroup()
	mergeGroupOp.GroupingAware = true
	legacyEmptyMergeGroupOp := group.NewArgumentMergeGroup()
	legacyEmptyMergeGroupOp.EmptyGroupingSet = true
	dynamicEmptyMergeGroupOp := group.NewArgumentMergeGroup()
	dynamicEmptyMergeGroupOp.EmptyGroupingSetIDs = []int64{1}

	for _, test := range []struct {
		name string
		op   vm.Operator
	}{
		{name: "projection", op: projectionOp},
		{name: "group", op: groupOp},
		{name: "merge group", op: mergeGroupOp},
		{name: "legacy empty merge group", op: legacyEmptyMergeGroupOp},
		{name: "dynamic empty merge group", op: dynamicEmptyMergeGroupOp},
	} {
		t.Run(test.name, func(t *testing.T) {
			scope := &Scope{Proc: proc, RootOp: test.op}
			rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion49)
			data, err := encodeRemoteScope(scope, proc)
			require.NoError(t, err)

			rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion48)
			_, err = encodeRemoteScope(scope, proc)
			require.ErrorContains(t, err, "MORPC protocol version 49")
			_, err = decodeScope(data, proc, true, nil)
			require.ErrorContains(t, err, "MORPC protocol version 49")
		})
	}
}

func TestGroupingSetRemoteProtocolValidationRecursesAndIgnoresLegacyGrouping(t *testing.T) {
	proc := testutil.NewProcess(t)
	rt := moruntime.ServiceRuntime(proc.GetService())
	previous, hadPrevious := rt.GetGlobalVariables(moruntime.MOProtocolVersion)
	t.Cleanup(func() {
		if hadPrevious {
			rt.SetGlobalVariables(moruntime.MOProtocolVersion, previous)
		} else {
			rt.CompareAndDeleteGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion49)
		}
	})
	rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion48)

	legacy := &pipeline.Pipeline{InstructionList: []*pipeline.Instruction{{
		Agg: &pipeline.Group{GroupingFlag: []bool{true, false}},
	}}}
	require.NoError(t, validateRemoteGroupingSetPipelineProtocol(proc, legacy))

	nested := &pipeline.Pipeline{Children: []*pipeline.Pipeline{{
		InstructionList: []*pipeline.Instruction{{ProjectionGroupingSetCount: 2}},
	}}}
	require.ErrorContains(t,
		validateRemoteGroupingSetPipelineProtocol(proc, nested),
		"MORPC protocol version 49")

	rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion49)
	require.NoError(t, validateRemoteGroupingSetPipelineProtocol(proc, nested))
}

func TestExternalScanIcebergRuntimeRoundtrip(t *testing.T) {
	ctx := &scopeContext{
		id:     1,
		root:   &scopeContext{},
		parent: &scopeContext{},
	}
	proc := &process.Process{}
	proc.Base = &process.BaseProcess{}

	dataTasks := []*pipeline.IcebergDataFileTask{{
		FilePath:              "s3://warehouse/sales/orders/data-0001.parquet",
		FileFormat:            "parquet",
		FileSize:              2048,
		RecordCount:           100,
		PartitionSpecId:       7,
		PartitionValues:       map[string]string{"created_day": "19815"},
		SplitOffsets:          []int64{4, 1024},
		RowGroupStart:         0,
		RowGroupEnd:           2,
		CredentialScope:       "scope-ref-1",
		ContentSequenceNumber: 9,
		FileSequenceNumber:    9,
		HasResidualFilter:     true,
		ResidualFilterHash:    "filter_digest:abcdef0123456789",
	}}
	deleteTasks := []*pipeline.IcebergDeleteFileTask{{
		DeleteType:         "position",
		DeleteFilePath:     "s3://warehouse/sales/orders/delete-0001.parquet",
		ReferencedDataFile: "s3://warehouse/sales/orders/data-0001.parquet",
		EqualityFieldIds:   []int32{1, 2},
		DeleteSchemaId:     3,
		PartitionSpecId:    7,
		SequenceNumber:     10,
		CredentialScope:    "scope-ref-1",
	}}
	columns := []*pipeline.IcebergColumnMapping{{
		MoColIndex:        0,
		IcebergFieldId:    1,
		SnapshotFieldName: "order_id",
		CurrentFieldName:  "id",
		MoType:            &planpb.Type{Id: int32(types.T_int64)},
		Required:          true,
		ParquetPathHint:   "order_id",
	}}
	snapshot := &pipeline.IcebergSnapshotRuntime{
		SnapshotId:           22,
		SchemaId:             1,
		PartitionSpecIds:     []int32{7},
		MetadataLocationHash: "meta-hash",
		ManifestListHash:     "manifest-list-hash",
		RefName:              "main",
		PlanningMode:         "client",
	}
	op := external.NewArgument().WithEs(
		&external.ExternalParam{
			ExParamConst: external.ExParamConst{
				FileList:                    []string{"s3://warehouse/sales/orders/data-0001.parquet"},
				FileSize:                    []int64{2048},
				FileOffsetTotal:             []*pipeline.FileOffset{{Offset: []int64{0, -1}}},
				IcebergDataTasks:            dataTasks,
				IcebergDeleteTasks:          deleteTasks,
				IcebergColumns:              columns,
				IcebergSnapshot:             snapshot,
				IcebergObjectIORef:          "object-scope-ref",
				IcebergHiddenReadCols:       []int32{3, 4},
				IcebergDeleteMaxMemoryBytes: 4096,
				IcebergDeleteSpillEnabled:   true,
				IcebergPlanningStats: process.ParquetProfileStats{
					IcebergMetadataBytes:         10,
					IcebergManifestListBytes:     20,
					IcebergManifestBytes:         30,
					IcebergManifestsSelected:     2,
					IcebergManifestsPruned:       1,
					IcebergDataFilesSelected:     2,
					IcebergDataFilesPruned:       3,
					IcebergDataFileBytesSelected: 2048,
					IcebergDataFileBytesPruned:   4096,
					IcebergPlanningCacheHits:     4,
					IcebergPlanningCacheMiss:     5,
				},
				NeedRowOrdinal: true,
			},
			ExParam: external.ExParam{
				Fileparam: &external.ExFileparam{},
				Filter:    &external.FilterParam{},
			},
		},
	)

	_, pipeInstr, err := convertToPipelineInstruction(op, proc, ctx, 1)
	require.NoError(t, err)
	require.Equal(t, dataTasks, pipeInstr.ExternalScan.IcebergDataTasks)
	require.Equal(t, deleteTasks, pipeInstr.ExternalScan.IcebergDeleteTasks)
	require.Equal(t, columns, pipeInstr.ExternalScan.IcebergColumns)
	require.Equal(t, snapshot, pipeInstr.ExternalScan.IcebergSnapshot)
	require.Equal(t, "object-scope-ref", pipeInstr.ExternalScan.IcebergObjectIoRef)
	require.Equal(t, []int32{3, 4}, pipeInstr.ExternalScan.IcebergHiddenReadColumns)
	require.Equal(t, int64(4096), pipeInstr.ExternalScan.IcebergDeleteMaxMemoryBytes)
	require.True(t, pipeInstr.ExternalScan.IcebergDeleteSpillEnabled)
	require.NotNil(t, pipeInstr.ExternalScan.IcebergPlanningStats)
	require.Equal(t, int64(10), pipeInstr.ExternalScan.IcebergPlanningStats.MetadataBytes)
	require.Equal(t, int64(20), pipeInstr.ExternalScan.IcebergPlanningStats.ManifestListBytes)
	require.Equal(t, int64(30), pipeInstr.ExternalScan.IcebergPlanningStats.ManifestBytes)
	require.Equal(t, int64(2), pipeInstr.ExternalScan.IcebergPlanningStats.ManifestsSelected)
	require.Equal(t, int64(1), pipeInstr.ExternalScan.IcebergPlanningStats.ManifestsPruned)
	require.Equal(t, int64(2), pipeInstr.ExternalScan.IcebergPlanningStats.DataFilesSelected)
	require.Equal(t, int64(3), pipeInstr.ExternalScan.IcebergPlanningStats.DataFilesPruned)
	require.Equal(t, int64(2048), pipeInstr.ExternalScan.IcebergPlanningStats.DataFileBytesSelected)
	require.Equal(t, int64(4096), pipeInstr.ExternalScan.IcebergPlanningStats.DataFileBytesPruned)
	require.Equal(t, int64(4), pipeInstr.ExternalScan.IcebergPlanningStats.PlanningCacheHits)
	require.Equal(t, int64(5), pipeInstr.ExternalScan.IcebergPlanningStats.PlanningCacheMiss)
	require.True(t, pipeInstr.ExternalScan.NeedRowOrdinal)
	require.True(t, pipeInstr.ExternalScan.IcebergDataTasks[0].HasResidualFilter)
	require.Equal(t, "filter_digest:abcdef0123456789", pipeInstr.ExternalScan.IcebergDataTasks[0].ResidualFilterHash)

	restored, err := convertToVmOperator(pipeInstr, ctx, nil)
	require.NoError(t, err)
	restoredExternal := restored.(*external.External)
	require.Equal(t, dataTasks, restoredExternal.Es.IcebergDataTasks)
	require.Equal(t, deleteTasks, restoredExternal.Es.IcebergDeleteTasks)
	require.Equal(t, columns, restoredExternal.Es.IcebergColumns)
	require.Equal(t, snapshot, restoredExternal.Es.IcebergSnapshot)
	require.Equal(t, "object-scope-ref", restoredExternal.Es.IcebergObjectIORef)
	require.Equal(t, []int32{3, 4}, restoredExternal.Es.IcebergHiddenReadCols)
	require.Equal(t, int64(4096), restoredExternal.Es.IcebergDeleteMaxMemoryBytes)
	require.True(t, restoredExternal.Es.IcebergDeleteSpillEnabled)
	require.Equal(t, int64(10), restoredExternal.Es.IcebergPlanningStats.IcebergMetadataBytes)
	require.Equal(t, int64(20), restoredExternal.Es.IcebergPlanningStats.IcebergManifestListBytes)
	require.Equal(t, int64(30), restoredExternal.Es.IcebergPlanningStats.IcebergManifestBytes)
	require.Equal(t, int64(2), restoredExternal.Es.IcebergPlanningStats.IcebergManifestsSelected)
	require.Equal(t, int64(1), restoredExternal.Es.IcebergPlanningStats.IcebergManifestsPruned)
	require.Equal(t, int64(2), restoredExternal.Es.IcebergPlanningStats.IcebergDataFilesSelected)
	require.Equal(t, int64(3), restoredExternal.Es.IcebergPlanningStats.IcebergDataFilesPruned)
	require.Equal(t, int64(2048), restoredExternal.Es.IcebergPlanningStats.IcebergDataFileBytesSelected)
	require.Equal(t, int64(4096), restoredExternal.Es.IcebergPlanningStats.IcebergDataFileBytesPruned)
	require.Equal(t, int64(4), restoredExternal.Es.IcebergPlanningStats.IcebergPlanningCacheHits)
	require.Equal(t, int64(5), restoredExternal.Es.IcebergPlanningStats.IcebergPlanningCacheMiss)
	require.True(t, restoredExternal.Es.NeedRowOrdinal)
	require.True(t, restoredExternal.Es.IcebergDataTasks[0].HasResidualFilter)
	require.Equal(t, "filter_digest:abcdef0123456789", restoredExternal.Es.IcebergDataTasks[0].ResidualFilterHash)
}

func Test_DMLOperatorSerializationRoundtrip(t *testing.T) {
	ctx := &scopeContext{
		id:     1,
		root:   &scopeContext{},
		parent: &scopeContext{},
	}
	proc := testutil.NewProcess(t)
	rt := moruntime.ServiceRuntime(proc.GetService())
	oldVersion, hadVersion := rt.GetGlobalVariables(moruntime.MOProtocolVersion)
	rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCLatestVersion)
	t.Cleanup(func() {
		if hadVersion {
			rt.SetGlobalVariables(moruntime.MOProtocolVersion, oldVersion)
		} else {
			rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCLatestVersion)
		}
	})

	t.Run("FuzzyFilter_RuntimeFilterPairContract", func(t *testing.T) {
		probeType := &planpb.Type{
			Id:    int32(types.T_decimal64),
			Width: 18,
			Scale: 2,
		}
		op := &fuzzyfilter.FuzzyFilter{
			N:                  42.5,
			PkName:             "pk",
			PkTyp:              *probeType,
			BuildIdx:           1,
			IfInsertFromUnique: true,
			RuntimeFilterSpec: &planpb.RuntimeFilterSpec{
				Tag:        40,
				UpperLimit: 128,
				BuildExpr: &planpb.Expr{
					Typ: *probeType,
					Expr: &planpb.Expr_Col{
						Col: &planpb.ColRef{ColPos: 0},
					},
				},
				KeyEncoding: planpb.RuntimeFilterKeyEncoding_RUNTIME_FILTER_KEY_RAW_V1,
				ProbeType:   probeType,
			},
		}
		_, pipeInstr, err := convertToPipelineInstruction(op, proc, ctx, 1)
		require.NoError(t, err)
		require.Equal(t, int32(1), pipeInstr.FuzzyFilter.BuildIdx)
		require.Equal(t, op.RuntimeFilterSpec, pipeInstr.FuzzyFilter.RuntimeFilterSpec)

		wireBytes, err := pipeInstr.Marshal()
		require.NoError(t, err)
		wireInstr := new(pipeline.Instruction)
		require.NoError(t, wireInstr.Unmarshal(wireBytes))

		restored, err := convertToVmOperator(wireInstr, ctx, nil)
		require.NoError(t, err)
		restoredOp := restored.(*fuzzyfilter.FuzzyFilter)
		require.Equal(t, 1, restoredOp.BuildIdx)
		require.Equal(t, "pk", restoredOp.PkName)
		require.True(t, restoredOp.IfInsertFromUnique)
		require.Equal(t, op.RuntimeFilterSpec, restoredOp.RuntimeFilterSpec)
		require.NotSame(t, op.RuntimeFilterSpec, restoredOp.RuntimeFilterSpec)
		require.NotSame(t,
			op.RuntimeFilterSpec.ProbeType,
			restoredOp.RuntimeFilterSpec.ProbeType)
		require.Equal(t, keycodec.ExactRuntimeFilterRaw,
			runtimefilter.ExactKeyEncoding(
				restoredOp.RuntimeFilterSpec,
				types.New(types.T_decimal64, 18, 2),
			))
	})

	t.Run("HashBuild_RuntimeFilterPairContract", func(t *testing.T) {
		probeType := &planpb.Type{
			Id:    int32(types.T_varchar),
			Width: types.MaxVarcharLen,
		}
		componentType := planpb.Type{
			Id:    int32(types.T_decimal64),
			Width: 18,
			Scale: 2,
		}
		buildExpr := &planpb.Expr{
			Typ: *probeType,
			Expr: &planpb.Expr_F{F: &planpb.Function{
				Func: &planpb.ObjectRef{
					ObjName: planfunction.SerialFullFunctionName,
					Obj:     planfunction.SerialFullFunctionEncodeID,
				},
				Args: []*planpb.Expr{{
					Typ: componentType,
					Expr: &planpb.Expr_Col{
						Col: &planpb.ColRef{ColPos: 0},
					},
				}},
			}},
		}
		op := hashbuild.NewArgument()
		defer op.Release()
		op.RuntimeFilterSpec = &planpb.RuntimeFilterSpec{
			Tag:                    41,
			UpperLimit:             128,
			MatchPrefix:            true,
			ScalarPredicate:        true,
			BuildExpr:              buildExpr,
			KeyEncoding:            planpb.RuntimeFilterKeyEncoding_RUNTIME_FILTER_KEY_SERIAL_FULL_V1,
			ProbeType:              probeType,
			KeyComponentProbeTypes: []planpb.Type{componentType},
		}

		_, pipeInstr, err := convertToPipelineInstruction(op, proc, ctx, 1)
		require.NoError(t, err)
		require.Equal(t, op.RuntimeFilterSpec, pipeInstr.HashBuild.RuntimeFilterSpec)

		wireBytes, err := pipeInstr.Marshal()
		require.NoError(t, err)
		wireInstr := new(pipeline.Instruction)
		require.NoError(t, wireInstr.Unmarshal(wireBytes))
		require.NotSame(t, op.RuntimeFilterSpec, wireInstr.HashBuild.RuntimeFilterSpec)
		require.NotSame(t, op.RuntimeFilterSpec.ProbeType,
			wireInstr.HashBuild.RuntimeFilterSpec.ProbeType)

		restored, err := convertToVmOperator(wireInstr, ctx, nil)
		require.NoError(t, err)
		restoredOp := restored.(*hashbuild.HashBuild)
		defer restoredOp.Release()
		require.Equal(t, op.RuntimeFilterSpec, restoredOp.RuntimeFilterSpec)
		require.Equal(t,
			planpb.RuntimeFilterKeyEncoding_RUNTIME_FILTER_KEY_SERIAL_FULL_V1,
			restoredOp.RuntimeFilterSpec.GetKeyEncoding())
		require.Equal(t, probeType, restoredOp.RuntimeFilterSpec.GetProbeType())
		require.Nil(t, restoredOp.RuntimeFilterSpec.GetExpr())
		require.Equal(t, *probeType,
			restoredOp.RuntimeFilterSpec.GetBuildExpr().Typ)
		require.Equal(t, []planpb.Type{componentType},
			restoredOp.RuntimeFilterSpec.GetKeyComponentProbeTypes())
		require.True(t, restoredOp.RuntimeFilterSpec.GetMatchPrefix())
		require.True(t, restoredOp.RuntimeFilterSpec.GetScalarPredicate())
	})

	t.Run("HashBuild_LegacyRuntimeFilterHasNoImplicitContract", func(t *testing.T) {
		op := hashbuild.NewArgument()
		defer op.Release()
		op.RuntimeFilterSpec = &planpb.RuntimeFilterSpec{
			Tag:        42,
			UpperLimit: 128,
			Expr: &planpb.Expr{Typ: planpb.Type{
				Id: int32(types.T_decimal64), Width: 18, Scale: 3,
			}},
		}

		_, pipeInstr, err := convertToPipelineInstruction(op, proc, ctx, 1)
		require.NoError(t, err)
		wireBytes, err := pipeInstr.Marshal()
		require.NoError(t, err)
		wireInstr := new(pipeline.Instruction)
		require.NoError(t, wireInstr.Unmarshal(wireBytes))

		spec := wireInstr.HashBuild.GetRuntimeFilterSpec()
		require.NotNil(t, spec)
		require.Nil(t, spec.ProbeType)
		require.Equal(t,
			planpb.RuntimeFilterKeyEncoding_RUNTIME_FILTER_KEY_UNSPECIFIED,
			spec.KeyEncoding)
	})

	t.Run("TableFunction_IndexReaderParam", func(t *testing.T) {
		limit := plan.MakePlan2Int64ConstExprWithType(17)
		op := &table_function.TableFunction{
			FuncName: "unnest",
			FulltextSourceRef: &planpb.ObjectRef{
				SchemaName: "publisher", ObjName: "source", SubscriptionName: "subscriber_alias",
				PubInfo: &planpb.PubInfo{TenantId: 42},
			},
			FulltextIndexRef: &planpb.ObjectRef{
				SchemaName: "publisher", ObjName: "index", SubscriptionName: "subscriber_alias",
				PubInfo: &planpb.PubInfo{TenantId: 42},
			},
			RuntimeFilterSpecs: []*planpb.RuntimeFilterSpec{
				{
					Tag:         42,
					MatchPrefix: true,
					UpperLimit:  128,
					NotOnPk:     true,
					KeyEncoding: planpb.RuntimeFilterKeyEncoding_RUNTIME_FILTER_KEY_FLOAT_ZERO_CLOSED_V1,
					ProbeType: &planpb.Type{
						Id: int32(types.T_float64),
					},
				},
			},
			IndexReaderParam: &planpb.IndexReaderParam{
				PartitionCnCnt: 2,
				PartitionCnIdx: 1,
				Limit:          limit,
			},
		}
		_, pipeInstr, err := convertToPipelineInstruction(op, proc, ctx, 1)
		require.NoError(t, err)
		require.Equal(t, int32(2), pipeInstr.TableFunction.GetIndexReaderParam().GetPartitionCnCnt())
		require.Equal(t, int32(1), pipeInstr.TableFunction.GetIndexReaderParam().GetPartitionCnIdx())
		require.Equal(t, int64(17), pipeInstr.TableFunction.GetIndexReaderParam().GetLimit().GetLit().GetI64Val())
		require.Equal(t, op.FulltextSourceRef, pipeInstr.TableFunction.FulltextSourceRef)
		require.Equal(t, op.FulltextIndexRef, pipeInstr.TableFunction.FulltextIndexRef)
		require.Len(t, pipeInstr.TableFunction.GetRuntimeFilterProbeList(), 1)
		require.Equal(t, int32(42), pipeInstr.TableFunction.GetRuntimeFilterProbeList()[0].GetTag())
		require.True(t, pipeInstr.TableFunction.GetRuntimeFilterProbeList()[0].GetMatchPrefix())
		require.Equal(t, int32(128), pipeInstr.TableFunction.GetRuntimeFilterProbeList()[0].GetUpperLimit())
		require.True(t, pipeInstr.TableFunction.GetRuntimeFilterProbeList()[0].GetNotOnPk())
		require.Equal(t, planpb.RuntimeFilterKeyEncoding_RUNTIME_FILTER_KEY_FLOAT_ZERO_CLOSED_V1,
			pipeInstr.TableFunction.GetRuntimeFilterProbeList()[0].GetKeyEncoding())
		require.Equal(t, int32(types.T_float64),
			pipeInstr.TableFunction.GetRuntimeFilterProbeList()[0].GetProbeType().GetId())

		wireBytes, err := pipeInstr.Marshal()
		require.NoError(t, err)
		wireInstr := new(pipeline.Instruction)
		require.NoError(t, wireInstr.Unmarshal(wireBytes))
		require.NotSame(t, pipeInstr.TableFunction.IndexReaderParam, wireInstr.TableFunction.IndexReaderParam)
		require.NotSame(t, pipeInstr.TableFunction.RuntimeFilterProbeList[0], wireInstr.TableFunction.RuntimeFilterProbeList[0])
		require.NotSame(t, pipeInstr.TableFunction.FulltextSourceRef, wireInstr.TableFunction.FulltextSourceRef)
		require.NotSame(t, pipeInstr.TableFunction.FulltextIndexRef, wireInstr.TableFunction.FulltextIndexRef)

		restored, err := convertToVmOperator(wireInstr, ctx, nil)
		require.NoError(t, err)
		restoredOp := restored.(*table_function.TableFunction)
		require.Equal(t, int32(2), restoredOp.IndexReaderParam.GetPartitionCnCnt())
		require.Equal(t, int32(1), restoredOp.IndexReaderParam.GetPartitionCnIdx())
		require.Equal(t, int64(17), restoredOp.IndexReaderParam.GetLimit().GetLit().GetI64Val())
		require.Equal(t, op.FulltextSourceRef, restoredOp.FulltextSourceRef)
		require.Equal(t, op.FulltextIndexRef, restoredOp.FulltextIndexRef)
		require.Len(t, restoredOp.RuntimeFilterSpecs, 1)
		require.Equal(t, int32(42), restoredOp.RuntimeFilterSpecs[0].GetTag())
		require.True(t, restoredOp.RuntimeFilterSpecs[0].GetMatchPrefix())
		require.Equal(t, int32(128), restoredOp.RuntimeFilterSpecs[0].GetUpperLimit())
		require.True(t, restoredOp.RuntimeFilterSpecs[0].GetNotOnPk())
		require.Equal(t, planpb.RuntimeFilterKeyEncoding_RUNTIME_FILTER_KEY_FLOAT_ZERO_CLOSED_V1,
			restoredOp.RuntimeFilterSpecs[0].GetKeyEncoding())
		require.Equal(t, int32(types.T_float64),
			restoredOp.RuntimeFilterSpecs[0].GetProbeType().GetId())
	})

	t.Run("Apply_FulltextReferences", func(t *testing.T) {
		sourceRef := &planpb.ObjectRef{
			SchemaName: "publisher", ObjName: "source", SubscriptionName: "subscriber_alias",
			PubInfo: &planpb.PubInfo{TenantId: 42},
		}
		indexRef := &planpb.ObjectRef{
			SchemaName: "publisher", ObjName: "index", SubscriptionName: "subscriber_alias",
			PubInfo: &planpb.PubInfo{TenantId: 42},
		}
		op := &apply.Apply{TableFunction: &table_function.TableFunction{
			FuncName:          "fulltext_index_scan",
			FulltextSourceRef: sourceRef,
			FulltextIndexRef:  indexRef,
		}}

		_, pipeInstr, err := convertToPipelineInstruction(op, proc, ctx, 1)
		require.NoError(t, err)
		wireBytes, err := pipeInstr.Marshal()
		require.NoError(t, err)
		wireInstr := new(pipeline.Instruction)
		require.NoError(t, wireInstr.Unmarshal(wireBytes))

		restored, err := convertToVmOperator(wireInstr, ctx, nil)
		require.NoError(t, err)
		restoredOp := restored.(*apply.Apply)
		require.Equal(t, sourceRef, restoredOp.TableFunction.FulltextSourceRef)
		require.Equal(t, indexRef, restoredOp.TableFunction.FulltextIndexRef)
	})

	t.Run("Apply_VectorIndexScan", func(t *testing.T) {
		op := apply.NewArgument()
		op.VectorAttrs = []string{"pkid", "score"}
		op.TxnOffset = 19
		op.VectorIndexScan = &planpb.VectorIndexScan{
			Index:            &planpb.IndexDef{IndexName: "idx", IndexAlgo: "ivfflat"},
			DistanceFunction: "l2_distance",
			CandidateLimit:   plan.MakePlan2Uint64ConstExprWithType(8),
		}

		_, pipeInstr, err := convertToPipelineInstruction(op, proc, ctx, 1)
		require.NoError(t, err)
		require.Nil(t, pipeInstr.TableFunction)
		data, err := pipeInstr.Marshal()
		require.NoError(t, err)
		var decoded pipeline.Instruction
		require.NoError(t, decoded.Unmarshal(data))

		restored, err := convertToVmOperator(&decoded, ctx, nil)
		require.NoError(t, err)
		restoredOp := restored.(*apply.Apply)
		require.Equal(t, op.VectorAttrs, restoredOp.VectorAttrs)
		require.Equal(t, "idx", restoredOp.VectorIndexScan.GetIndex().GetIndexName())
		require.Equal(t, uint64(8), restoredOp.VectorIndexScan.GetCandidateLimit().GetLit().GetU64Val())
		require.Equal(t, 19, restoredOp.TxnOffset)
	})

	t.Run("TableFunction_Limit", func(t *testing.T) {
		op := table_function.NewArgument()
		op.FuncName = "unnest"
		op.Limit = plan.MakePlan2Uint64ConstExprWithType(4)
		op.RuntimeFilterSpecs = []*planpb.RuntimeFilterSpec{
			{Tag: 9, UseMembershipFilter: true},
		}
		op.IndexReaderParam = &planpb.IndexReaderParam{
			Limit:        plan.MakePlan2Uint64ConstExprWithType(4),
			OrigFuncName: "l2_distance",
		}

		_, pipeInstr, err := convertToPipelineInstruction(op, proc, ctx, 1)
		require.NoError(t, err)
		require.Equal(t, uint64(4), pipeInstr.Limit.GetLit().GetU64Val())
		require.Len(t, pipeInstr.TableFunction.RuntimeFilterProbeList, 1)
		require.NotNil(t, pipeInstr.TableFunction.IndexReaderParam)

		data, err := pipeInstr.Marshal()
		require.NoError(t, err)
		var decoded pipeline.Instruction
		require.NoError(t, decoded.Unmarshal(data))

		restored, err := convertToVmOperator(&decoded, ctx, nil)
		require.NoError(t, err)
		restoredOp := restored.(*table_function.TableFunction)
		require.Equal(t, uint64(4), restoredOp.Limit.GetLit().GetU64Val())
		require.Equal(t, op.RuntimeFilterSpecs, restoredOp.RuntimeFilterSpecs)
		require.Equal(t, uint64(4), restoredOp.IndexReaderParam.GetLimit().GetLit().GetU64Val())
		require.Equal(t, "l2_distance", restoredOp.IndexReaderParam.GetOrigFuncName())
	})

	t.Run("MultiUpdate_PartitionCols", func(t *testing.T) {
		changedRowsCol := 7
		op := &multi_update.MultiUpdate{
			MultiUpdateCtx: []*multi_update.MultiUpdateCtx{
				{
					ObjRef:             &plan.ObjectRef{ObjName: "t1"},
					TableDef:           &plan.TableDef{Name: "t1"},
					InsertCols:         []int{0, 1, 2},
					DeleteCols:         []int{3, 4, 8},
					PartitionCols:      []int{5, 6},
					InsertPkColIdx:     1,
					DedupByTargetRowID: true,
					TargetUpdateCtxIdx: 0,
					ChangedRowsCol:     &changedRowsCol,
					AffectedRowsCols:   []int{9, 10},
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
		require.Equal(t, []int{3, 4, 8}, restoredOp.MultiUpdateCtx[0].DeleteCols)
		require.Equal(t, 1, restoredOp.MultiUpdateCtx[0].InsertPkColIdx)
		require.True(t, restoredOp.MultiUpdateCtx[0].DedupByTargetRowID)
		require.Equal(t, 0, restoredOp.MultiUpdateCtx[0].TargetUpdateCtxIdx)
		require.NotNil(t, restoredOp.MultiUpdateCtx[0].ChangedRowsCol)
		require.Equal(t, 7, *restoredOp.MultiUpdateCtx[0].ChangedRowsCol)
		require.Equal(t, []int{9, 10}, restoredOp.MultiUpdateCtx[0].AffectedRowsCols)
		require.True(t, restoredOp.IsRemote)
		require.False(t, restoredOp.CountDeleteAffectRows,
			"CountDeleteAffectRows must stay false when the source op did not set it")
	})

	t.Run("MultiUpdate_CountDeleteAffectRows", func(t *testing.T) {
		op := &multi_update.MultiUpdate{
			MultiUpdateCtx: []*multi_update.MultiUpdateCtx{
				{
					ObjRef:             &plan.ObjectRef{ObjName: "t1"},
					TableDef:           &plan.TableDef{Name: "t1"},
					IgnoreAffectedRows: true,
				},
			},
			Action:                multi_update.UpdateWriteTable,
			CountDeleteAffectRows: true,
		}
		_, pipeInstr, err := convertToPipelineInstruction(op, proc, ctx, 1)
		require.NoError(t, err)
		require.True(t, pipeInstr.MultiUpdate.UpdateCtxList[0].CountDeleteAffectRows,
			"serialized UpdateCtx must carry CountDeleteAffectRows")
		require.True(t, pipeInstr.MultiUpdate.UpdateCtxList[0].IgnoreAffectedRows,
			"serialized UpdateCtx must carry IgnoreAffectedRows")

		restored, err := convertToVmOperator(pipeInstr, ctx, nil)
		require.NoError(t, err)
		restoredOp := restored.(*multi_update.MultiUpdate)
		require.True(t, restoredOp.CountDeleteAffectRows,
			"CountDeleteAffectRows must survive the remote pipeline round-trip")
		require.True(t, restoredOp.MultiUpdateCtx[0].IgnoreAffectedRows,
			"IgnoreAffectedRows must survive the remote pipeline round-trip")
	})

	t.Run("MultiUpdate_RejectZeroTemporal", func(t *testing.T) {
		op := &multi_update.MultiUpdate{
			MultiUpdateCtx: []*multi_update.MultiUpdateCtx{
				{
					ObjRef:   &planpb.ObjectRef{ObjName: "t1"},
					TableDef: &planpb.TableDef{Name: "t1"},
				},
			},
			Action:             multi_update.UpdateWriteTable,
			RejectZeroTemporal: true,
		}
		_, pipeInstr, err := convertToPipelineInstruction(op, proc, ctx, 1)
		require.NoError(t, err)
		require.True(t, pipeInstr.MultiUpdate.RejectZeroTemporal)

		wireBytes, err := pipeInstr.Marshal()
		require.NoError(t, err)
		wireInstr := new(pipeline.Instruction)
		require.NoError(t, wireInstr.Unmarshal(wireBytes))
		require.True(t, wireInstr.MultiUpdate.RejectZeroTemporal)

		restored, err := convertToVmOperator(wireInstr, ctx, nil)
		require.NoError(t, err)
		require.True(t, restored.(*multi_update.MultiUpdate).RejectZeroTemporal)
	})

	t.Run("PreInsert_State", func(t *testing.T) {
		op := &preinsert.PreInsert{
			RejectZeroTemporal: true,
			HasTargetSelector:  true,
			TargetRowNumberCol: 7,
			TargetActiveCol:    8,
			TargetRowIDCol:     9,
		}
		_, pipeInstr, err := convertToPipelineInstruction(op, proc, ctx, 1)
		require.NoError(t, err)
		require.True(t, pipeInstr.PreInsert.RejectZeroTemporal)
		require.True(t, pipeInstr.PreInsert.HasTargetSelector)
		require.Equal(t, int32(7), pipeInstr.PreInsert.TargetRowNumberCol)
		require.Equal(t, int32(8), pipeInstr.PreInsert.TargetActiveCol)
		require.Equal(t, int32(9), pipeInstr.PreInsert.TargetRowIdCol)

		wireBytes, err := pipeInstr.Marshal()
		require.NoError(t, err)
		wireInstr := new(pipeline.Instruction)
		require.NoError(t, wireInstr.Unmarshal(wireBytes))
		require.True(t, wireInstr.PreInsert.RejectZeroTemporal)

		restored, err := convertToVmOperator(wireInstr, ctx, nil)
		require.NoError(t, err)
		restoredPreInsert := restored.(*preinsert.PreInsert)
		require.True(t, restoredPreInsert.RejectZeroTemporal)
		require.True(t, restoredPreInsert.HasTargetSelector)
		require.Equal(t, int32(7), restoredPreInsert.TargetRowNumberCol)
		require.Equal(t, int32(8), restoredPreInsert.TargetActiveCol)
		require.Equal(t, int32(9), restoredPreInsert.TargetRowIDCol)
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

	t.Run("HashPartition_AlgorithmAndSpillThreshold", func(t *testing.T) {
		op := &partition.Partition{
			Algorithm: planpb.Node_PARTITION_ALGORITHM_HASH,
			SpillMem:  8192,
			OrderBySpecs: []*planpb.OrderBySpec{{
				Expr: &planpb.Expr{Typ: planpb.Type{Id: int32(types.T_int64)}},
			}},
		}
		_, pipeInstr, err := convertToPipelineInstruction(op, proc, ctx, 1)
		require.NoError(t, err)
		require.Equal(t, planpb.Node_PARTITION_ALGORITHM_HASH, pipeInstr.PartitionAlgorithm)
		require.Equal(t, int64(8192), pipeInstr.SpillMem)

		wireBytes, err := pipeInstr.Marshal()
		require.NoError(t, err)
		wireInstr := new(pipeline.Instruction)
		require.NoError(t, wireInstr.Unmarshal(wireBytes))
		restored, err := convertToVmOperator(wireInstr, ctx, nil)
		require.NoError(t, err)
		restoredPartition := restored.(*partition.Partition)
		require.Equal(t, planpb.Node_PARTITION_ALGORITHM_HASH, restoredPartition.Algorithm)
		require.Equal(t, int64(8192), restoredPartition.SpillMem)
	})

	t.Run("HashBuild_SpillThreshold", func(t *testing.T) {
		for _, threshold := range []int64{0, 4096} {
			op := &hashbuild.HashBuild{SpillThreshold: threshold}
			_, pipeInstr, err := convertToPipelineInstruction(op, proc, ctx, 1)
			require.NoError(t, err)
			require.Equal(t, threshold, pipeInstr.SpillMem)

			restored, err := convertToVmOperator(pipeInstr, ctx, nil)
			require.NoError(t, err)
			require.Equal(t, threshold, restored.(*hashbuild.HashBuild).SpillThreshold)
		}
	})

	t.Run("HashBuild_TrackNullKeys", func(t *testing.T) {
		op := &hashbuild.HashBuild{TrackNullKeys: true}
		_, pipeInstr, err := convertToPipelineInstruction(op, proc, ctx, 1)
		require.NoError(t, err)
		require.True(t, pipeInstr.HashBuild.TrackNullKeys)

		restored, err := convertToVmOperator(pipeInstr, ctx, nil)
		require.NoError(t, err)
		require.True(t, restored.(*hashbuild.HashBuild).TrackNullKeys)
	})

	t.Run("JoinSpillThreshold_ReceiverRoundtripPreservesPolicy", func(t *testing.T) {
		for _, threshold := range []int64{0, 4096, 100001} {
			for name, op := range map[string]vm.Operator{
				"hashjoin": &hashjoin.HashJoin{
					EqConds:        [][]*planpb.Expr{{}, {}},
					SpillThreshold: threshold,
				},
				"dedupjoin": &dedupjoin.DedupJoin{
					Conditions:     [][]*planpb.Expr{{}, {}},
					SpillThreshold: threshold,
				},
				"rightdedupjoin": &rightdedupjoin.RightDedupJoin{
					Conditions:      [][]*planpb.Expr{{}, {}},
					SpillThreshold:  threshold,
					InputKeysUnique: true,
				},
			} {
				t.Run(fmt.Sprintf("%s/%d", name, threshold), func(t *testing.T) {
					_, pipeInstr, err := convertToPipelineInstruction(op, proc, ctx, 1)
					require.NoError(t, err)
					require.Equal(t, threshold, pipeInstr.SpillMem)

					restored, err := convertToVmOperator(pipeInstr, ctx, nil)
					require.NoError(t, err)
					switch join := restored.(type) {
					case *hashjoin.HashJoin:
						require.Equal(t, threshold, join.SpillThreshold)
					case *dedupjoin.DedupJoin:
						require.Equal(t, threshold, join.SpillThreshold)
					case *rightdedupjoin.RightDedupJoin:
						require.Equal(t, threshold, join.SpillThreshold)
						require.True(t, join.InputKeysUnique)
					default:
						t.Fatalf("unexpected restored operator %T", restored)
					}
				})
			}
		}
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

func TestShuffleSerializationRoundtrip(t *testing.T) {
	ctx := &scopeContext{id: 1, root: &scopeContext{}, parent: &scopeContext{}}
	proc := &process.Process{Base: &process.BaseProcess{}}
	op := shuffle.NewArgument()
	defer op.Release()
	op.ShuffleColIdx = 2
	op.ShuffleType = int32(planpb.ShuffleType_Range)
	op.ShuffleColMin = -10
	op.ShuffleColMax = 100
	op.BucketNum = 8
	op.ShuffleRangeInt64 = []int64{0, 10, 20}
	op.RuntimeFilterSpec = &planpb.RuntimeFilterSpec{Tag: 42}
	op.ShuffleExpr = plan.MakePlan2Int64ConstExprWithType(7)
	op.DrainAllBuckets = true

	_, instruction, err := convertToPipelineInstruction(op, proc, ctx, 1)
	require.NoError(t, err)
	require.True(t, instruction.Shuffle.DrainAllBuckets)

	restored, err := convertToVmOperator(instruction, ctx, nil)
	require.NoError(t, err)
	restoredShuffle := restored.(*shuffle.Shuffle)
	defer restoredShuffle.Release()
	require.Equal(t, op.ShuffleColIdx, restoredShuffle.ShuffleColIdx)
	require.Equal(t, op.ShuffleType, restoredShuffle.ShuffleType)
	require.Equal(t, op.ShuffleColMin, restoredShuffle.ShuffleColMin)
	require.Equal(t, op.ShuffleColMax, restoredShuffle.ShuffleColMax)
	require.Equal(t, op.BucketNum, restoredShuffle.BucketNum)
	require.Equal(t, op.ShuffleRangeInt64, restoredShuffle.ShuffleRangeInt64)
	require.Equal(t, op.RuntimeFilterSpec, restoredShuffle.RuntimeFilterSpec)
	require.Equal(t, op.ShuffleExpr, restoredShuffle.ShuffleExpr)
	require.True(t, restoredShuffle.DrainAllBuckets)
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

func Test_decodeBatchPreservesPrepareParamKindTransportTrailer(t *testing.T) {
	mp := mpool.MustNewZero()
	bat := batch.NewWithSize(1)
	bat.Vecs[0] = vector.NewVec(types.T_text.ToType())
	require.NoError(t, vector.AppendBytes(bat.Vecs[0], []byte("5"), false, mp))
	require.NoError(t, vector.AppendBytes(bat.Vecs[0], []byte("5"), false, mp))
	bat.Vecs[0].SetPrepareParamKinds([]vector.PrepareParamKind{
		vector.PrepareParamFloat,
		vector.PrepareParamNone,
	})
	bat.SetRowCount(2)
	data, err := bat.MarshalBinaryWithPrepareParamKinds(&bytes.Buffer{}, true)
	require.NoError(t, err)
	decoded, err := decodeBatch(mp, data)
	require.NoError(t, err)
	require.Equal(t, vector.PrepareParamFloat, decoded.Vecs[0].GetPrepareParamKindAt(0))
	require.Equal(t, vector.PrepareParamNone, decoded.Vecs[0].GetPrepareParamKindAt(1))
	bat.Clean(mp)
	decoded.Clean(mp)
}

func Test_GetProcByUuid(t *testing.T) {
	_ = colexec.NewServer("")

	{
		// first get action or deletion just convert the k-v to be `ready to remove` status.
		// and the next action will remove it.
		uid, err := uuid.NewV7()
		require.Nil(t, err)

		receiver := &messageReceiverOnServer{
			colexecServer: colexec.GetServer(""),
			connectionCtx: context.TODO(),
		}

		p0 := &process.Process{}
		c0 := process.RemotePipelineInformationChannel(make(chan *process.WrapCs))
		require.NoError(t, colexec.GetServer("").PutProcIntoUuidMap(uid, p0, c0))

		// this action will convert it to be ready-to-remove status.
		colexec.GetServer("").DeleteUuids([]uuid.UUID{uid})

		// A receiver closed before attachment is a terminal protocol state, not
		// a successful nil attachment.
		p, c, err := receiver.GetProcByUuid(uid)
		require.Error(t, err)
		require.Contains(t, err.Error(), "already closed")
		require.Nil(t, p)
		require.Nil(t, c)

		colexec.GetServer("").DeleteUuids([]uuid.UUID{uid})
	}

	{
		// A disconnected notify attempt must exit without poisoning a receiver
		// UUID that a later attempt can still register.
		uid, err := uuid.NewV7()
		require.NoError(t, err)
		cctx, ccancel := context.WithCancel(context.Background())
		receiver := &messageReceiverOnServer{
			colexecServer: colexec.GetServer(""),
			connectionCtx: cctx,
			messageCtx:    context.Background(),
		}
		ccancel()
		p, _, err := receiver.GetProcByUuid(uid)
		require.Error(t, err)
		require.Nil(t, p)

		ownerCh := make(process.RemotePipelineInformationChannel)
		require.NoError(t, colexec.GetServer("").PutProcIntoUuidMap(uid, &process.Process{}, ownerCh))
		colexec.GetServer("").RemoveUuidsOwned([]uuid.UUID{uid}, ownerCh)
	}

	{
		// test get succeed.
		uid, err := uuid.NewV7()
		require.Nil(t, err)

		receiver := &messageReceiverOnServer{
			colexecServer: colexec.GetServer(""),
			connectionCtx: context.TODO(),
		}

		p0 := &process.Process{}
		c0 := process.RemotePipelineInformationChannel(make(chan *process.WrapCs))
		require.NoError(t, colexec.GetServer("").PutProcIntoUuidMap(uid, p0, c0))

		p, c, err := receiver.GetProcByUuid(uid)
		require.Nil(t, err)
		require.Equal(t, p0, p)
		require.Equal(t, c0, c)

		colexec.GetServer("").DeleteUuids([]uuid.UUID{uid})
		colexec.GetServer("").DeleteUuids([]uuid.UUID{uid})
	}

	{
		// test if receiver done first, put action should return error.
		colexec.GetServer("").GetProcByUuid(uuid.UUID{}, true)
		err := colexec.GetServer("").PutProcIntoUuidMap(uuid.UUID{}, nil, nil)
		require.NotNil(t, err)

		colexec.GetServer("").DeleteUuids([]uuid.UUID{{}})
		colexec.GetServer("").DeleteUuids([]uuid.UUID{{}})
	}
}

func Test_GetProcByUuid_ConcurrentWake(t *testing.T) {
	_ = colexec.NewServer("")

	uid, err := uuid.NewV7()
	require.Nil(t, err)

	receiver := &messageReceiverOnServer{
		colexecServer: colexec.GetServer(""),
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

	p0 := &process.Process{}
	c0 := process.RemotePipelineInformationChannel(make(chan *process.WrapCs))
	require.NoError(t, colexec.GetServer("").PutProcIntoUuidMap(uid, p0, c0))

	select {
	case r := <-done:
		require.Nil(t, r.err)
		require.Equal(t, p0, r.proc)
		require.Equal(t, c0, r.ch)
	case <-time.After(3 * time.Second):
		t.Fatal("GetProcByUuid did not wake after PutProcIntoUuidMap")
	}

	colexec.GetServer("").DeleteUuids([]uuid.UUID{uid})
	colexec.GetServer("").DeleteUuids([]uuid.UUID{uid})
}

func Test_GetProcByUuid_CancellationDoesNotPoisonLaterRegistration(t *testing.T) {
	_ = colexec.NewServer("")

	uid, err := uuid.NewV7()
	require.NoError(t, err)

	messageCtx, cancelMessage := context.WithCancelCause(context.Background())
	cancelCause := moerr.NewInternalErrorNoCtx("query canceled before receiver registration")
	receiver := &messageReceiverOnServer{
		colexecServer: colexec.GetServer(""),
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
		p, ch, lookupErr := receiver.GetProcByUuid(uid)
		done <- result{proc: p, ch: ch, err: lookupErr}
	}()
	cancelMessage(cancelCause)

	got := <-done
	require.ErrorIs(t, got.err, cancelCause)
	require.Nil(t, got.proc)
	require.Nil(t, got.ch)

	p0 := &process.Process{}
	c0 := process.RemotePipelineInformationChannel(make(chan *process.WrapCs))
	require.NoError(t, colexec.GetServer("").PutProcIntoUuidMap(uid, p0, c0))

	colexec.GetServer("").DeleteUuids([]uuid.UUID{uid})
	colexec.GetServer("").DeleteUuids([]uuid.UUID{uid})
}

func Test_GetProcByUuid_WaitsPastFormerAdmissionLimitForRegistration(t *testing.T) {
	server := colexec.NewServer("")
	uid := uuid.Must(uuid.NewV7())
	messageCtx, cancelMessage := context.WithCancelCause(context.Background())
	defer cancelMessage(context.Canceled)
	receiver := &messageReceiverOnServer{
		colexecServer: server,
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
		proc, ch, err := receiver.GetProcByUuid(uid)
		done <- result{proc: proc, ch: ch, err: err}
	}()

	formerLimit := time.NewTimer(20 * time.Millisecond)
	defer formerLimit.Stop()
	select {
	case got := <-done:
		t.Fatalf("receiver lookup returned without lifecycle evidence: %v", got.err)
	case <-formerLimit.C:
	}

	ownerProc := &process.Process{}
	ownerCh := make(process.RemotePipelineInformationChannel)
	require.NoError(t, server.PutProcIntoUuidMap(uid, ownerProc, ownerCh))
	select {
	case got := <-done:
		require.NoError(t, got.err)
		require.Same(t, ownerProc, got.proc)
		require.Equal(t, ownerCh, got.ch)
	case <-time.After(time.Second):
		t.Fatal("GetProcByUuid did not attach after delayed registration")
	}
	server.RemoveUuidsOwned([]uuid.UUID{uid}, ownerCh)
}

func TestHandlePrepareDoneNotifyObservesMessageCancellationAfterAttach(t *testing.T) {
	server := colexec.NewServer("")
	uid := uuid.Must(uuid.NewV7())
	messageCtx, cancelMessage := context.WithCancelCause(context.Background())
	dispatchCtx, cancelDispatch := context.WithCancelCause(context.Background())
	dispatchProc := &process.Process{
		Ctx:    dispatchCtx,
		Cancel: cancelDispatch,
	}
	notifyCh := make(process.RemotePipelineInformationChannel, 1)
	require.NoError(t, server.PutProcIntoUuidMap(uid, dispatchProc, notifyCh))
	t.Cleanup(func() {
		server.RemoveUuidsOwned([]uuid.UUID{uid}, notifyCh)
	})

	ctrl := gomock.NewController(t)
	session := mock_morpc.NewMockClientSession(ctrl)
	session.EXPECT().SessionCtx().Return(context.Background()).AnyTimes()
	receiver := &messageReceiverOnServer{
		messageCtx:      messageCtx,
		connectionCtx:   context.Background(),
		messageId:       7,
		messageTyp:      pipeline.Method_PrepareDoneNotifyMessage,
		messageUuid:     uid,
		clientSession:   session,
		colexecServer:   server,
		streamLifecycle: &pipelineStreamLifecycle{batchFlow: newPipelineBatchFlow(2, 1024)},
	}

	done := make(chan error, 1)
	go func() {
		done <- handlePipelineMessage(receiver)
	}()

	var attached *process.WrapCs
	select {
	case attached = <-notifyCh:
		require.NotNil(t, attached)
		require.Equal(t, uid, attached.Uid)
		require.Equal(t, uint32(2), attached.BatchCredits)
		require.Equal(t, uint64(1024), attached.ByteCredits)
		require.NotNil(t, attached.ReserveBatch)
		require.NotNil(t, attached.RollbackBatch)
		seq, err := attached.ReserveBatch(context.Background(), 10)
		require.NoError(t, err)
		require.Equal(t, uint64(1), seq)
		attached.RollbackBatch(seq)
	case <-time.After(time.Second):
		t.Fatal("prepare-done notify did not attach to the published receiver")
	}

	cancelCause := moerr.NewInternalErrorNoCtx("notify message canceled after attach")
	cancelMessage(cancelCause)
	select {
	case err := <-done:
		require.ErrorIs(t, err, cancelCause)
	case <-time.After(time.Second):
		t.Fatal("prepare-done notify did not stop after message cancellation")
	}
	require.ErrorIs(t, context.Cause(dispatchCtx), cancelCause)
	server.RemoveRelatedPipeline(session, receiver.messageId)
}

func Test_TryGetProcByUuid_NotRegisteredYetDoesNotPoisonLaterRegistration(t *testing.T) {
	_ = colexec.NewServer("")

	uid, err := uuid.NewV7()
	require.NoError(t, err)

	receiver := &messageReceiverOnServer{
		colexecServer: colexec.GetServer(""),
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
	require.NoError(t, colexec.GetServer("").PutProcIntoUuidMap(uid, p0, c0))

	colexec.GetServer("").DeleteUuids([]uuid.UUID{uid})
	colexec.GetServer("").DeleteUuids([]uuid.UUID{uid})
}

func Test_TryGetProcByUuid_ReturnsRegisteredProc(t *testing.T) {
	_ = colexec.NewServer("")

	uid, err := uuid.NewV7()
	require.NoError(t, err)

	p0 := &process.Process{}
	c0 := process.RemotePipelineInformationChannel(make(chan *process.WrapCs))
	require.NoError(t, colexec.GetServer("").PutProcIntoUuidMap(uid, p0, c0))
	defer colexec.GetServer("").DeleteUuids([]uuid.UUID{uid})

	receiver := &messageReceiverOnServer{
		colexecServer: colexec.GetServer(""),
		connectionCtx: context.Background(),
		messageCtx:    context.Background(),
	}

	p, ch, err := receiver.TryGetProcByUuid(uid)
	require.NoError(t, err)
	require.Same(t, p0, p)
	require.Equal(t, c0, ch)
}

func Test_TryGetProcByUuid_ClosedRetryDoesNotPoisonLaterRegistration(t *testing.T) {
	_ = colexec.NewServer("")

	uid, err := uuid.NewV7()
	require.NoError(t, err)

	connectionCtx, cancelConnection := context.WithCancel(context.Background())
	cancelConnection()
	receiver := &messageReceiverOnServer{
		colexecServer: colexec.GetServer(""),
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
	require.NoError(t, colexec.GetServer("").PutProcIntoUuidMap(uid, dispatchProc, notifyCh))

	nextAttempt := &messageReceiverOnServer{
		colexecServer: colexec.GetServer(""),
		connectionCtx: context.Background(),
		messageCtx:    context.Background(),
	}
	p, ch, err = nextAttempt.TryGetProcByUuid(uid)
	require.NoError(t, err)
	require.Same(t, dispatchProc, p)
	require.Equal(t, process.RemotePipelineInformationChannel(notifyCh), ch)
	colexec.GetServer("").DeleteUuids([]uuid.UUID{uid})
}

func Test_TryGetProcByUuid_CloseVsRegisterInterleavings(t *testing.T) {
	_ = colexec.NewServer("")

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
					colexecServer: colexec.GetServer(""),
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
			require.NoError(t, colexec.GetServer("").PutProcIntoUuidMap(uid, dispatchProc, notifyCh))
			if !tc.closeBeforeRegistration {
				closeConnection()
			}

			nextAttempt := &messageReceiverOnServer{
				colexecServer: colexec.GetServer(""),
				connectionCtx: context.Background(),
				messageCtx:    context.Background(),
			}
			gotProc, gotCh, err := nextAttempt.TryGetProcByUuid(uid)
			require.NoError(t, err)
			require.Same(t, dispatchProc, gotProc)
			require.Equal(t, notifyCh, gotCh)
			colexec.GetServer("").DeleteUuids([]uuid.UUID{uid})
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
	_ = colexec.NewServer("")

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
		colexecServer: colexec.GetServer(""),
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
	_ = colexec.NewServer("")

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

	registeredProc, _, ok := colexec.GetServer("").GetProcByUuid(uid, false)
	require.True(t, ok)
	require.Same(t, nestedProc, registeredProc)
}

func TestRegisterLocalDispatchReceiversRegistersRetainedRemoteRootOnly(t *testing.T) {
	_ = colexec.NewServer("")

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
	registeredProc, _, ok := colexec.GetServer("").GetProcByUuid(localUID, false)
	require.True(t, ok)
	require.Same(t, remoteProc, registeredProc)
	registeredProc, notifyCh, ok := colexec.GetServer("").GetProcByUuid(remoteUID, false)
	require.False(t, ok)
	require.Nil(t, registeredProc)
	require.Nil(t, notifyCh)
}

func TestRegisterLocalDispatchReceiversTraversesRemoteAncestorForNestedLocalReturn(t *testing.T) {
	_ = colexec.NewServer("")

	for _, tc := range []struct {
		name string
		root vm.Operator
	}{
		{name: "remote non-dispatch root", root: merge.NewArgument()},
		{name: "retained remote dispatch root", root: dispatch.NewArgument()},
	} {
		t.Run(tc.name, func(t *testing.T) {
			uid, err := uuid.NewV7()
			require.NoError(t, err)
			rootProc := testutil.NewProcess(t)
			outerProc := rootProc.NewNoContextChildProc(0)
			localProc := rootProc.NewNoContextChildProc(0)
			localDispatch := dispatch.NewArgument()
			localDispatch.FuncId = dispatch.SendToAllFunc
			localDispatch.RemoteRegs = []colexec.ReceiveInfo{{Uuid: uid}}
			localReturn := &Scope{
				Magic:    Remote,
				Proc:     localProc,
				RootOp:   localDispatch,
				NodeInfo: engine.Node{Addr: "local-cn:6002"},
			}
			outerRemote := &Scope{
				Magic:     Remote,
				Proc:      outerProc,
				RootOp:    tc.root,
				NodeInfo:  engine.Node{Addr: "remote-cn:6002"},
				PreScopes: []*Scope{localReturn},
			}
			// The remote tree is valid to execute on remote-cn. When it does, its child
			// RemoteRun comes back to local-cn and needs this dispatch receiver before
			// the remote sender can notify it.
			require.True(t, checkPipelineStandaloneExecutableAtRemote(outerRemote))

			registrations, err := registerLocalDispatchReceivers([]*Scope{outerRemote}, "local-cn:6002")
			require.NoError(t, err)
			defer registrations.cleanup()
			registeredProc, _, ok := colexec.GetServer("").GetProcByUuid(uid, false)
			require.True(t, ok)
			require.Same(t, localProc, registeredProc)
		})
	}
}

func TestRegisterLocalDispatchReceiversSkipsGuaranteedRemoteRunFailures(t *testing.T) {
	_ = colexec.NewServer("")

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
			registeredProc, notifyCh, ok := colexec.GetServer("").GetProcByUuid(uid, false)
			require.False(t, ok)
			require.Nil(t, registeredProc)
			require.Nil(t, notifyCh)
		})
	}
}

func TestRegisterRemoteDispatchReceiversUsesOwningScopeProcess(t *testing.T) {
	_ = colexec.NewServer("")

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
	registeredProc, _, ok := colexec.GetServer("").GetProcByUuid(uid, false)
	require.True(t, ok)
	require.Same(t, nestedProc, registeredProc)
}

func TestRegisterLocalDispatchReceiversRollsBackEarlierScopes(t *testing.T) {
	_ = colexec.NewServer("")

	uid1, err := uuid.NewV7()
	require.NoError(t, err)
	uid2, err := uuid.NewV7()
	require.NoError(t, err)
	colexec.GetServer("").GetProcByUuid(uid2, true)
	defer colexec.GetServer("").DeleteUuids([]uuid.UUID{uid2})

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
	registeredProc, notifyCh, ok := colexec.GetServer("").GetProcByUuid(uid1, false)
	require.False(t, ok)
	require.Nil(t, registeredProc)
	require.Nil(t, notifyCh)

	registrations, err := registerLocalDispatchReceivers(scopes[:1], "local-cn")
	require.NoError(t, err, "rollback must clear the dispatch's early-registration state")
	registeredProc, notifyCh, ok = colexec.GetServer("").GetProcByUuid(uid1, false)
	require.True(t, ok)
	require.Same(t, proc1, registeredProc)
	require.NotNil(t, notifyCh)
	registrations.cleanup()
	registeredProc, notifyCh, ok = colexec.GetServer("").GetProcByUuid(uid1, false)
	require.False(t, ok)
	require.Nil(t, registeredProc)
	require.Nil(t, notifyCh)
}

func TestRemoteDispatchRegistrationRollbackReleasesPendingAttach(t *testing.T) {
	_ = colexec.NewServer("")

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
		colexecServer: colexec.GetServer(""),
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
	registeredProc, notifyCh, ok := colexec.GetServer("").GetProcByUuid(uid, false)
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
	s.sendNotifyMessageWithFactoryAndWait(
		&wg,
		resultCh,
		factory,
		func(context.Context, int, uuid.UUID) error { return nil },
	)

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

func TestNotifyMessageRetryDelayIsBoundedAndDeterministic(t *testing.T) {
	uid := uuid.UUID{
		0x01, 0x02, 0x03, 0x04,
		0x05, 0x06, 0x07, 0x08,
		0x09, 0x0a, 0x0b, 0x0c,
		0x0d, 0x0e, 0x0f, 0x10,
	}
	for _, tc := range []struct {
		attempt int
		base    time.Duration
	}{
		{attempt: -1, base: notifyMessageRetryInitialInterval},
		{attempt: 0, base: notifyMessageRetryInitialInterval},
		{attempt: 1, base: 2 * notifyMessageRetryInitialInterval},
		{attempt: 2, base: 4 * notifyMessageRetryInitialInterval},
		{attempt: 20, base: notifyMessageRetryMaxInterval},
	} {
		got := notifyMessageRetryDelay(tc.attempt, uid)
		require.Equal(t, got, notifyMessageRetryDelay(tc.attempt, uid))
		require.GreaterOrEqual(t, got, tc.base*80/100)
		require.LessOrEqual(t, got, tc.base*120/100)
	}
}

func TestRemoteNotifyRetryAttachesToEmptyDispatchBeforeCompletion(t *testing.T) {
	colexecServer := colexec.NewServer("")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	proc := testutil.NewProcess(t)
	proc.BuildPipelineContext(ctx)
	scopeProc := proc.NewContextChildProc(1)

	uid, err := uuid.NewV7()
	require.NoError(t, err)
	s := &Scope{
		Proc: scopeProc,
		RemoteReceivRegInfos: []RemoteReceivRegInfo{
			{Idx: 0, Uuid: uid, FromAddr: "remote-cn"},
		},
	}

	firstLookupDone := make(chan struct{})
	allowFirstResponse := make(chan struct{})
	var attempts atomic.Int32
	factory := func(
		ctx context.Context,
		sid string,
		toAddr string,
		mp *mpool.MPool,
		analyzeModule *AnalyzeModule,
	) (*messageSenderOnClient, error) {
		receiveCh := make(chan morpc.Message, 1)
		receiver := &messageReceiverOnServer{
			connectionCtx: ctx,
			messageCtx:    ctx,
			colexecServer: colexecServer,
		}
		if attempts.Add(1) == 1 {
			_, _, lookupErr := receiver.TryGetProcByUuid(uid)
			close(firstLookupDone)
			if lookupErr == nil {
				unexpected := errors.New("first remote notify unexpectedly found the registration")
				cancel()
				return nil, unexpected
			}
			select {
			case <-allowFirstResponse:
			case <-ctx.Done():
				return nil, ctx.Err()
			}
			msg := &pipeline.Message{Sid: pipeline.Status_MessageEnd}
			msg.SetMoError(ctx, lookupErr)
			receiveCh <- msg
		} else {
			dispatchProc, notifyCh, lookupErr := receiver.TryGetProcByUuid(uid)
			if lookupErr != nil {
				cancel()
				return nil, lookupErr
			}
			if dispatchProc == nil {
				unexpected := errors.New("registered remote dispatch returned a nil process")
				cancel()
				return nil, unexpected
			}
			wrap := &process.WrapCs{Uid: uid, Err: make(chan error, 1)}
			go func() {
				select {
				case notifyCh <- wrap:
				case <-ctx.Done():
					return
				}
				select {
				case terminalErr := <-wrap.Err:
					msg := &pipeline.Message{Sid: pipeline.Status_MessageEnd}
					if terminalErr != nil {
						msg.SetMoError(ctx, terminalErr)
					}
					receiveCh <- msg
				case <-ctx.Done():
				}
			}()
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
	s.sendNotifyMessageWithFactoryAndWait(
		&wg,
		resultCh,
		factory,
		func(context.Context, int, uuid.UUID) error { return nil },
	)
	select {
	case <-firstLookupDone:
	case <-time.After(5 * time.Second):
		t.Fatal("first remote notify did not observe the absent registration")
	}

	child := value_scan.NewArgument()
	defer child.Release()
	require.NoError(t, child.Prepare(proc))
	dispatchOp := dispatch.NewArgument()
	defer dispatchOp.Release()
	dispatchOp.FuncId = dispatch.SendToAllFunc
	dispatchOp.RemoteRegs = []colexec.ReceiveInfo{{Uuid: uid}}
	dispatchOp.AppendChild(child)
	registration, err := dispatchOp.RegisterRemoteReceiversWithHandle(proc)
	require.NoError(t, err)
	require.NotNil(t, registration)
	defer registration.Cleanup()
	require.NoError(t, dispatchOp.Prepare(proc))
	close(allowFirstResponse)

	callResult, err := dispatchOp.Call(proc)
	require.NoError(t, err)
	require.Equal(t, vm.ExecStop, callResult.Status)
	dispatchOp.Reset(proc, false, nil)

	select {
	case result := <-resultCh:
		result.clean(scopeProc)
		require.NoError(t, result.err)
	case <-time.After(5 * time.Second):
		t.Fatal("remote notify did not complete after the empty dispatch registered")
	}
	select {
	case signal := <-scopeProc.Reg.MergeReceivers[0].Ch2:
		bat, signalErr := signal.Action()
		require.NoError(t, signalErr)
		require.Nil(t, bat)
	case <-time.After(5 * time.Second):
		t.Fatal("remote receiver did not observe the empty dispatch terminal signal")
	}
	wg.Wait()
	require.Equal(t, int32(2), attempts.Load())
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
		require.ErrorIs(t, context.Cause(scopeProc.Ctx), testErr)
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

func TestSendNotifyMessageNormalizesPipelineCancellationCause(t *testing.T) {
	duplicateErr := moerr.NewDuplicateEntryNoCtx("1", "primary")
	tests := []struct {
		name               string
		cancelCause        error
		cancelQuery        bool
		keepPipelineActive bool
		factoryErr         func(context.Context) error
		wantErr            error
	}{
		{
			name:        "query interruption recovers substantive cancellation cause",
			cancelCause: duplicateErr,
			factoryErr: func(ctx context.Context) error {
				return moerr.NewQueryInterrupted(ctx)
			},
			wantErr: duplicateErr,
		},
		{
			name: "normal query interruption remains secondary",
			factoryErr: func(ctx context.Context) error {
				return moerr.NewQueryInterrupted(ctx)
			},
		},
		{
			name:        "raw cancellation recovers substantive cancellation cause",
			cancelCause: duplicateErr,
			factoryErr: func(context.Context) error {
				return context.Canceled
			},
			wantErr: duplicateErr,
		},
		{
			name: "raw pipeline cancellation without substantive cause is secondary",
			factoryErr: func(context.Context) error {
				return context.Canceled
			},
		},
		{
			name:        "raw query cancellation remains visible",
			cancelQuery: true,
			factoryErr: func(context.Context) error {
				return context.Canceled
			},
			wantErr: context.Canceled,
		},
		{
			name:               "raw cancellation while pipeline is active remains visible",
			keepPipelineActive: true,
			factoryErr: func(context.Context) error {
				return context.Canceled
			},
			wantErr: context.Canceled,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			proc := testutil.NewProcess(t)
			queryCtx := proc.Base.GetContextBase().BuildQueryCtx(proc.GetTopContext())
			proc.BuildPipelineContext(queryCtx)
			scopeProc := proc.NewContextChildProc(1)
			if tt.cancelQuery {
				_, cancelQuery := process.GetQueryCtxFromProc(proc)
				require.NotNil(t, cancelQuery)
				cancelQuery()
			} else if !tt.keepPipelineActive {
				scopeProc.Cancel(tt.cancelCause)
			}

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
				_ string,
				_ string,
				_ *mpool.MPool,
				_ *AnalyzeModule,
			) (*messageSenderOnClient, error) {
				return nil, tt.factoryErr(ctx)
			}

			var wg sync.WaitGroup
			resultCh := make(chan notifyMessageResult, 1)
			s.sendNotifyMessageWithFactory(&wg, resultCh, factory)

			select {
			case result := <-resultCh:
				if tt.wantErr == nil {
					require.NoError(t, result.err)
				} else {
					require.ErrorIs(t, result.err, tt.wantErr)
				}
			case <-time.After(time.Second):
				t.Fatal("remote notify did not report cancellation")
			}

			select {
			case signal := <-scopeProc.Reg.MergeReceivers[0].Ch2:
				_, terminalErr := signal.Action()
				if tt.wantErr == nil {
					require.NoError(t, terminalErr)
				} else {
					require.ErrorIs(t, terminalErr, tt.wantErr)
				}
			case <-time.After(time.Second):
				t.Fatal("remote notify cleanup did not terminate its receiver")
			}
			wg.Wait()
		})
	}
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

func TestSendNotifyMessageLegacyRetryStopsAtQueryCancellation(t *testing.T) {
	proc := testutil.NewProcess(t)
	queryCtx, cancelQuery := context.WithCancelCause(context.Background())
	proc.BuildPipelineContext(queryCtx)
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

	retryEntered := make(chan struct{})
	waitRetry := func(ctx context.Context, _ int, _ uuid.UUID) error {
		close(retryEntered)
		<-ctx.Done()
		return remoteRegistrationContextError(ctx)
	}

	var wg sync.WaitGroup
	resultCh := make(chan notifyMessageResult, 1)
	s.sendNotifyMessageWithFactoryAndWait(&wg, resultCh, factory, waitRetry)
	<-retryEntered
	cancelCause := moerr.NewInternalErrorNoCtx("query canceled while waiting for legacy receiver registration")
	cancelQuery(cancelCause)

	select {
	case result := <-resultCh:
		result.clean(scopeProc)
		require.ErrorIs(t, result.err, cancelCause)
	case <-time.After(time.Second):
		t.Fatal("notify retry did not stop after query cancellation")
	}
	select {
	case signal := <-scopeProc.Reg.MergeReceivers[0].Ch2:
		_, err := signal.Action()
		require.ErrorIs(t, err, cancelCause)
	case <-time.After(time.Second):
		t.Fatal("notify retry cancellation did not send cleanup signal")
	}
	wg.Wait()
}

func TestSendNotifyMessageLegacyRetryWaitsPastFormerAdmissionLimit(t *testing.T) {
	proc := testutil.NewProcess(t)
	proc.BuildPipelineContext(context.Background())
	scopeProc := proc.NewContextChildProc(1)
	uid := uuid.Must(uuid.NewV7())
	s := &Scope{
		Proc: scopeProc,
		RemoteReceivRegInfos: []RemoteReceivRegInfo{
			{Idx: 0, Uuid: uid, FromAddr: "remote-cn"},
		},
	}

	var attempts atomic.Int32
	factory := func(
		ctx context.Context,
		sid string,
		toAddr string,
		mp *mpool.MPool,
		analyzeModule *AnalyzeModule,
	) (*messageSenderOnClient, error) {
		receiveCh := make(chan morpc.Message, 1)
		msg := &pipeline.Message{Sid: pipeline.Status_MessageEnd}
		if attempts.Add(1) == 1 {
			msg.SetMoError(ctx, moerr.NewRemoteDispatchNotRegistered(ctx, uid.String()))
		}
		receiveCh <- msg
		return &messageSenderOnClient{
			ctx:          ctx,
			mp:           mp,
			streamSender: &fakeStreamSender{},
			receiveCh:    receiveCh,
			safeToClose:  true,
		}, nil
	}

	type retryContextObservation struct {
		sameQueryContext bool
		hasDeadline      bool
	}
	retryEntered := make(chan retryContextObservation, 1)
	allowRetry := make(chan struct{})
	waitRetry := func(ctx context.Context, _ int, _ uuid.UUID) error {
		_, hasDeadline := ctx.Deadline()
		retryEntered <- retryContextObservation{
			sameQueryContext: ctx == scopeProc.Ctx,
			hasDeadline:      hasDeadline,
		}
		select {
		case <-allowRetry:
			return nil
		case <-ctx.Done():
			return remoteRegistrationContextError(ctx)
		}
	}

	var wg sync.WaitGroup
	resultCh := make(chan notifyMessageResult, 1)
	s.sendNotifyMessageWithFactoryAndWait(&wg, resultCh, factory, waitRetry)
	observation := <-retryEntered
	require.True(t, observation.sameQueryContext)
	require.False(t, observation.hasDeadline, "legacy retry must use the query lifecycle context directly")

	formerLimit := time.NewTimer(20 * time.Millisecond)
	defer formerLimit.Stop()
	select {
	case result := <-resultCh:
		result.clean(scopeProc)
		t.Fatalf("legacy retry returned without lifecycle evidence: %v", result.err)
	case <-formerLimit.C:
	}
	close(allowRetry)

	select {
	case result := <-resultCh:
		result.clean(scopeProc)
		require.NoError(t, result.err)
	case <-time.After(time.Second):
		t.Fatal("legacy notify retry did not succeed after delayed registration")
	}
	select {
	case signal := <-scopeProc.Reg.MergeReceivers[0].Ch2:
		_, err := signal.Action()
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("successful legacy retry did not send cleanup signal")
	}
	wg.Wait()
	require.Equal(t, int32(2), attempts.Load())
}

func TestSendNotifyMessageSuccessfulAttachUsesQueryContext(t *testing.T) {
	proc := testutil.NewProcess(t)
	proc.BuildPipelineContext(context.Background())
	scopeProc := proc.NewContextChildProc(1)
	uid := uuid.Must(uuid.NewV7())
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
		_, hasDeadline := ctx.Deadline()
		if hasDeadline {
			return nil, errors.New("successful stream did not inherit the query context directly")
		}
		receiveCh := make(chan morpc.Message, 1)
		receiveCh <- &pipeline.Message{Sid: pipeline.Status_MessageEnd}
		return &messageSenderOnClient{
			ctx:          ctx,
			mp:           mp,
			streamSender: &fakeStreamSender{},
			receiveCh:    receiveCh,
			safeToClose:  true,
		}, nil
	}

	var waitCalls atomic.Int32
	var wg sync.WaitGroup
	resultCh := make(chan notifyMessageResult, 1)
	s.sendNotifyMessageWithFactoryAndWait(
		&wg,
		resultCh,
		factory,
		func(context.Context, int, uuid.UUID) error {
			waitCalls.Add(1)
			return errors.New("retry wait must not run after successful attach")
		},
	)

	select {
	case result := <-resultCh:
		result.clean(scopeProc)
		require.NoError(t, result.err)
	case <-time.After(time.Second):
		t.Fatal("successful notify stream did not finish")
	}
	select {
	case signal := <-scopeProc.Reg.MergeReceivers[0].Ch2:
		_, err := signal.Action()
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("successful notify stream did not send its terminal signal")
	}
	wg.Wait()
	require.Zero(t, waitCalls.Load())
}

func TestSendNotifyMessageStopsRetryWhenQueryContextCanceled(t *testing.T) {
	proc := testutil.NewProcess(t)
	queryCtx := proc.Base.GetContextBase().BuildQueryCtx(proc.GetTopContext())
	proc.BuildPipelineContext(queryCtx)
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
		return &messageSenderOnClient{
			ctx:          ctx,
			mp:           mp,
			streamSender: &fakeStreamSender{},
			receiveCh:    receiveCh,
			safeToClose:  true,
		}, nil
	}

	retryEntered := make(chan struct{})
	waitRetry := func(ctx context.Context, _ int, _ uuid.UUID) error {
		close(retryEntered)
		<-ctx.Done()
		return remoteRegistrationContextError(ctx)
	}

	var wg sync.WaitGroup
	resultCh := make(chan notifyMessageResult, 1)
	s.sendNotifyMessageWithFactoryAndWait(&wg, resultCh, factory, waitRetry)
	<-retryEntered
	_, cancelQuery := process.GetQueryCtxFromProc(proc)
	require.NotNil(t, cancelQuery)
	cancelQuery()

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

func TestCancelConsumedDispatchRegistrationCancelsOwnerProcess(t *testing.T) {
	_ = colexec.NewServer("")

	uid, err := uuid.NewV7()
	require.Nil(t, err)

	procCtx, procCancel := context.WithCancelCause(context.Background())
	dispatchProc := &process.Process{
		Ctx:    procCtx,
		Cancel: procCancel,
	}
	notifyCh := process.RemotePipelineInformationChannel(make(chan *process.WrapCs))
	require.NoError(t, colexec.GetServer("").PutProcIntoUuidMap(uid, dispatchProc, notifyCh))

	receiver := &messageReceiverOnServer{
		messageCtx:    context.Background(),
		colexecServer: colexec.GetServer(""),
	}
	cancelCause := moerr.NewInternalErrorNoCtx("registration abandoned")
	registeredProc, notifyChannel, state, _ := colexec.GetServer("").AttachProcByUuidOrWait(uid)
	require.Equal(t, colexec.RemoteReceiverAttachedNow, state)
	require.Same(t, dispatchProc, registeredProc)
	require.Equal(t, notifyCh, notifyChannel)
	receiver.cancelConsumedDispatchRegistration(registeredProc, cancelCause)

	require.ErrorIs(t, context.Cause(procCtx), cancelCause)
	colexec.GetServer("").RemoveUuidsOwned([]uuid.UUID{uid}, notifyCh)
}

func TestGetProcByUuidReturnsWhenMessageContextCanceledBeforeRegistration(t *testing.T) {
	_ = colexec.NewServer("")

	uid, err := uuid.NewV7()
	require.Nil(t, err)

	messageCtx, cancelMessage := context.WithCancel(context.Background())
	receiver := &messageReceiverOnServer{
		colexecServer: colexec.GetServer(""),
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

	cancelMessage()

	select {
	case r := <-done:
		require.ErrorIs(t, r.err, context.Canceled)
		require.Nil(t, r.proc)
		require.Nil(t, r.ch)
	case <-time.After(time.Second):
		t.Fatal("GetProcByUuid did not return after message context cancellation")
	}

	ownerCh := make(process.RemotePipelineInformationChannel)
	err = colexec.GetServer("").PutProcIntoUuidMap(uid, &process.Process{}, ownerCh)
	require.NoError(t, err)
	colexec.GetServer("").RemoveUuidsOwned([]uuid.UUID{uid}, ownerCh)
}

var _ morpc.Stream = &fakeStreamSender{}

// fakeStreamSender implement the morpc.Stream interface.
type fakeStreamSender struct {
	// how many packages were sent.
	sentCnt int
	sent    []morpc.Message

	// return it during next send.
	nextSendError error
}

func (s *fakeStreamSender) ID() uint64 { return 0 }
func (s *fakeStreamSender) Send(ctx context.Context, request morpc.Message) error {
	if s.nextSendError == nil {
		s.sentCnt++
		s.sent = append(s.sent, request)
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
	_, withoutOut, _, _, err := prepareRemoteRunSendingData("", s1, proc, nil, uuid.Nil)
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
	_, withoutOut, _, _, err = prepareRemoteRunSendingData("", s2, proc, nil, uuid.Nil)
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
	_, withoutOut, _, _, err = prepareRemoteRunSendingData("", s3, proc, nil, uuid.Nil)
	require.NoError(t, err)
	require.True(t, withoutOut)
}

func TestPrepareRemoteRunSendingDataPreservesBlockFilters(t *testing.T) {
	proc := testutil.NewProcess(t)
	proc.Ctx = context.WithValue(proc.Ctx, defines.TenantIDKey{}, uint32(0))
	proc.Base.TxnOperator = fakeTxnOperator{}
	proc.Base.SessionInfo.TimeZone = time.UTC

	int64Type := types.T_int64.ToType()
	greaterEqual, err := planfunction.GetFunctionByName(
		context.Background(), ">=", []types.Type{int64Type, int64Type})
	require.NoError(t, err)
	originalFilter := &planpb.Expr{
		Typ: planpb.Type{Id: int32(types.T_bool), NotNullable: true},
		Expr: &planpb.Expr_F{F: &planpb.Function{
			Func: &planpb.ObjectRef{Obj: greaterEqual.GetEncodedOverloadID(), ObjName: ">="},
			Args: []*planpb.Expr{
				{
					Typ:  planpb.Type{Id: int32(types.T_int64), NotNullable: true},
					Expr: &planpb.Expr_Col{Col: &planpb.ColRef{Name: "a", ColPos: 0}},
				},
				plan.MakePlan2Int64ConstExprWithType(42),
			},
		}},
	}
	compiledFilter := plan.DeepCopyExpr(originalFilter)
	var executors []colexec.ExpressionExecutor
	t.Cleanup(func() {
		for _, executor := range executors {
			executor.Free()
		}
	})
	_, err = plan.ReplaceFoldExpr(proc, compiledFilter, &executors)
	require.NoError(t, err)
	require.True(t, plan.HasFoldExprForList([]*planpb.Expr{compiledFilter}))
	require.Nil(t, compiledFilter.GetF().Args[1].GetFold().Data)

	s := &Scope{
		Magic:  Remote,
		Proc:   proc,
		RootOp: value_scan.NewArgument(),
		DataSource: &Source{
			node: &planpb.Node{
				BlockFilterList: []*planpb.Expr{originalFilter},
			},
			BlockFilterList: []*planpb.Expr{compiledFilter},
		},
	}

	scopeData, _, _, _, err := prepareRemoteRunSendingData("", s, proc, nil, uuid.Nil)
	require.NoError(t, err)
	require.True(t, plan.HasFoldExprForList(s.DataSource.BlockFilterList))
	require.Nil(t, compiledFilter.GetF().Args[1].GetFold().Data)
	require.False(t, plan.HasFoldExprForList(s.DataSource.node.BlockFilterList))
	restored, err := decodeScope(scopeData, proc, true, nil)
	require.NoError(t, err)

	require.Len(t, restored.DataSource.BlockFilterList, 1)
	require.False(t, plan.HasFoldExprForList(restored.DataSource.BlockFilterList))
	remoteCompile := &Compile{proc: restored.Proc}
	t.Cleanup(func() {
		for _, executor := range remoteCompile.filterExprExes {
			executor.Free()
		}
	})
	filters, err := restored.handleRuntimeFilters(remoteCompile, nil)
	require.NoError(t, err)
	require.Len(t, filters, 1)
	require.True(t, plan.HasFoldExprForList(filters))
	require.NotEmpty(t, filters[0].GetF().Args[1].GetFold().Data)
	require.False(t, plan.HasFoldExprForList(restored.DataSource.BlockFilterList))
	_, _, _, _, _, canCompile, _ := readutil.CompileFilterExprs(filters, &planpb.TableDef{
		Cols: []*planpb.ColDef{{
			Name:   "a",
			Typ:    planpb.Type{Id: int32(types.T_int64)},
			Seqnum: 0,
		}},
		Name2ColIndex: map[string]int32{"a": 0},
	}, nil)
	require.True(t, canCompile)

	invalidRemote := &Scope{
		IsRemote:   true,
		Proc:       proc,
		DataSource: &Source{BlockFilterList: []*planpb.Expr{compiledFilter}},
	}
	_, err = invalidRemote.handleRuntimeFilters(&Compile{proc: proc}, nil)
	require.ErrorContains(t, err, "sender-owned Fold value")

	legacyScope := &Scope{
		Proc:   proc,
		RootOp: value_scan.NewArgument(),
		DataSource: &Source{
			node: &planpb.Node{
				BlockFilterList: []*planpb.Expr{plan.DeepCopyExpr(originalFilter)},
			},
		},
	}
	legacyScopeData, err := encodeScope(legacyScope)
	require.NoError(t, err)
	legacyRestored, err := decodeScope(legacyScopeData, proc, true, nil)
	require.NoError(t, err)
	require.Len(t, legacyRestored.DataSource.BlockFilterList, 1)
	require.False(t, plan.HasFoldExprForList(legacyRestored.DataSource.BlockFilterList))
	legacyCompile := &Compile{proc: legacyRestored.Proc}
	t.Cleanup(func() {
		for _, executor := range legacyCompile.filterExprExes {
			executor.Free()
		}
	})
	legacyFilters, err := legacyRestored.handleRuntimeFilters(legacyCompile, nil)
	require.NoError(t, err)
	require.Len(t, legacyFilters, 1)
	require.True(t, plan.HasFoldExprForList(legacyFilters))

	nestedFilter := plan.DeepCopyExpr(originalFilter)
	_, err = plan.ReplaceFoldExpr(proc, nestedFilter, &executors)
	require.NoError(t, err)
	nested := &Scope{
		Magic:  Remote,
		Proc:   proc,
		RootOp: value_scan.NewArgument(),
		PreScopes: []*Scope{{
			Proc:   proc,
			RootOp: value_scan.NewArgument(),
			DataSource: &Source{
				node:            &planpb.Node{BlockFilterList: []*planpb.Expr{originalFilter}},
				BlockFilterList: []*planpb.Expr{nestedFilter},
			},
		}},
	}
	nestedData, _, _, _, err := prepareRemoteRunSendingData("", nested, proc, nil, uuid.Nil)
	require.NoError(t, err)
	nestedRestored, err := decodeScope(nestedData, proc, true, nil)
	require.NoError(t, err)
	require.Len(t, nestedRestored.PreScopes, 1)
	require.False(t, plan.HasFoldExprForList(nestedRestored.PreScopes[0].DataSource.BlockFilterList))
	require.True(t, plan.HasFoldExprForList(nested.PreScopes[0].DataSource.BlockFilterList))
}

func TestPrepareRemoteRunSendingDataPreservesEmptyScalarBlockFilter(t *testing.T) {
	proc := testutil.NewProcess(t)
	proc.Ctx = context.WithValue(proc.Ctx, defines.TenantIDKey{}, uint32(0))
	proc.Base.TxnOperator = fakeTxnOperator{}
	proc.Base.SessionInfo.TimeZone = time.UTC

	varcharType := types.T_varchar.ToType()
	equal, err := planfunction.GetFunctionByName(
		context.Background(), "=", []types.Type{varcharType, varcharType})
	require.NoError(t, err)
	originalFilter := &planpb.Expr{
		Typ: planpb.Type{Id: int32(types.T_bool), NotNullable: true},
		Expr: &planpb.Expr_F{F: &planpb.Function{
			Func: &planpb.ObjectRef{Obj: equal.GetEncodedOverloadID(), ObjName: "="},
			Args: []*planpb.Expr{
				{
					Typ:  planpb.Type{Id: int32(types.T_varchar), NotNullable: true},
					Expr: &planpb.Expr_Col{Col: &planpb.ColRef{Name: "a", ColPos: 0}},
				},
				plan.MakePlan2StringConstExprWithType(""),
			},
		}},
	}
	filter := plan.DeepCopyExpr(originalFilter)
	var executors []colexec.ExpressionExecutor
	t.Cleanup(func() {
		for _, executor := range executors {
			executor.Free()
		}
	})
	_, err = plan.ReplaceFoldExpr(proc, filter, &executors)
	require.NoError(t, err)
	s := &Scope{
		Magic:  Remote,
		Proc:   proc,
		RootOp: value_scan.NewArgument(),
		DataSource: &Source{
			node:            &planpb.Node{BlockFilterList: []*planpb.Expr{originalFilter}},
			BlockFilterList: []*planpb.Expr{filter},
		},
	}
	fold := filter.GetF().Args[1].GetFold()
	require.False(t, fold.IsConst)
	require.Nil(t, fold.Data)

	scopeData, _, _, _, err := prepareRemoteRunSendingData("", s, proc, nil, uuid.Nil)
	require.NoError(t, err)
	restored, err := decodeScope(scopeData, proc, true, nil)
	require.NoError(t, err)
	require.False(t, plan.HasFoldExprForList(restored.DataSource.BlockFilterList))
	restoredCompile := &Compile{proc: restored.Proc}
	t.Cleanup(func() {
		for _, executor := range restoredCompile.filterExprExes {
			executor.Free()
		}
	})
	filters, err := restored.handleRuntimeFilters(restoredCompile, nil)
	require.NoError(t, err)
	require.Len(t, filters, 1)
	restoredFold := filters[0].GetF().Args[1].GetFold()
	require.True(t, restoredFold.IsConst)
	require.NotNil(t, restoredFold.Data)
	require.Empty(t, restoredFold.Data)
	_, _, _, _, _, canCompile, _ := readutil.CompileFilterExprs(filters, &planpb.TableDef{
		Cols: []*planpb.ColDef{{
			Name:   "a",
			Typ:    planpb.Type{Id: int32(types.T_varchar)},
			Seqnum: 0,
		}},
		Name2ColIndex: map[string]int32{"a": 0},
	}, nil)
	require.True(t, canCompile)
}

func TestPrepareRemoteRunSendingDataFoldsBlockFilterVariables(t *testing.T) {
	proc := newResolveVariableProcess(t, "ANSI")
	proc.Ctx = context.WithValue(proc.Ctx, defines.TenantIDKey{}, uint32(0))
	proc.Base.TxnOperator = fakeTxnOperator{}
	proc.Base.SessionInfo.TimeZone = time.UTC

	textType := types.T_text.ToType()
	equal, err := planfunction.GetFunctionByName(
		context.Background(), "=", []types.Type{textType, textType})
	require.NoError(t, err)
	originalFilter := &planpb.Expr{
		Typ: planpb.Type{Id: int32(types.T_bool), NotNullable: true},
		Expr: &planpb.Expr_F{F: &planpb.Function{
			Func: &planpb.ObjectRef{Obj: equal.GetEncodedOverloadID(), ObjName: "="},
			Args: []*planpb.Expr{
				{
					Typ:  planpb.Type{Id: int32(types.T_text), NotNullable: true},
					Expr: &planpb.Expr_Col{Col: &planpb.ColRef{Name: "a", ColPos: 0}},
				},
				makeTestVarExpr("sql_mode"),
			},
		}},
	}
	compiledFilter := plan.DeepCopyExpr(originalFilter)
	var executors []colexec.ExpressionExecutor
	t.Cleanup(func() {
		for _, executor := range executors {
			executor.Free()
		}
	})
	_, err = plan.ReplaceFoldExpr(proc, compiledFilter, &executors)
	require.NoError(t, err)

	s := &Scope{
		Magic:  Remote,
		Proc:   proc,
		RootOp: value_scan.NewArgument(),
		DataSource: &Source{
			node:            &planpb.Node{BlockFilterList: []*planpb.Expr{originalFilter}},
			BlockFilterList: []*planpb.Expr{compiledFilter},
		},
	}
	scopeData, _, _, folded, err := prepareRemoteRunSendingData("", s, proc, nil, uuid.Nil)
	require.NoError(t, err)
	require.True(t, folded)
	require.True(t, containsVarExpr(originalFilter))

	restored, err := decodeScope(scopeData, proc, true, nil)
	require.NoError(t, err)
	require.Len(t, restored.DataSource.BlockFilterList, 1)
	require.False(t, containsVarExpr(restored.DataSource.BlockFilterList[0]))
	require.Equal(t, "ANSI", restored.DataSource.BlockFilterList[0].GetF().Args[1].GetLit().GetSval())

	// Remote processes do not carry the coordinator's variable resolver. The
	// serialized block filter must therefore be self-contained before it builds
	// receiver-owned Fold executors.
	restored.Proc.SetResolveVariableFunc(nil)
	remoteCompile := &Compile{proc: restored.Proc}
	t.Cleanup(func() {
		for _, executor := range remoteCompile.filterExprExes {
			executor.Free()
		}
	})
	filters, err := restored.handleRuntimeFilters(remoteCompile, nil)
	require.NoError(t, err)
	require.Len(t, filters, 1)
	_, _, _, _, _, canCompile, _ := readutil.CompileFilterExprs(filters, &planpb.TableDef{
		Cols: []*planpb.ColDef{{
			Name:   "a",
			Typ:    planpb.Type{Id: int32(types.T_text)},
			Seqnum: 0,
		}},
		Name2ColIndex: map[string]int32{"a": 0},
	}, nil)
	require.True(t, canCompile)
}

func TestPrepareRemoteRunSendingDataKeepsConnectorChildTableFunctionParams(t *testing.T) {
	proc := testutil.NewProcess(t)
	proc.Ctx = context.WithValue(proc.Ctx, defines.TenantIDKey{}, uint32(0))
	proc.Base.TxnOperator = fakeTxnOperator{}
	proc.Base.SessionInfo.TimeZone = time.UTC

	tf := &table_function.TableFunction{
		FuncName: "unnest",
		RuntimeFilterSpecs: []*planpb.RuntimeFilterSpec{
			{Tag: 42},
		},
		IndexReaderParam: &planpb.IndexReaderParam{
			PartitionCnCnt: 2,
			PartitionCnIdx: 1,
		},
	}
	conn := connector.NewArgument()
	conn.AppendChild(tf)
	s := &Scope{
		Proc:   proc,
		RootOp: conn,
	}

	scopeData, withoutOut, _, _, err := prepareRemoteRunSendingData("", s, proc, nil, uuid.Nil)
	require.NoError(t, err)
	require.False(t, withoutOut)

	restored, err := decodeScope(scopeData, proc, true, nil)
	require.NoError(t, err)
	restoredOp := restored.RootOp.(*table_function.TableFunction)
	require.Equal(t, int32(2), restoredOp.IndexReaderParam.GetPartitionCnCnt())
	require.Equal(t, int32(1), restoredOp.IndexReaderParam.GetPartitionCnIdx())
	require.Len(t, restoredOp.RuntimeFilterSpecs, 1)
	require.Equal(t, int32(42), restoredOp.RuntimeFilterSpecs[0].GetTag())
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
	_ = colexec.NewServer("")

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
		ctx:              context.Background(),
		streamSender:     &fakeStreamSender{},
		requestFinishAck: true,
	}

	{
		// there should only send one time if this is just a small data.
		sender.streamSender.(*fakeStreamSender).sentCnt = 0
		sender.streamSender.(*fakeStreamSender).sent = nil
		sender.streamSender.(*fakeStreamSender).nextSendError = nil

		err := sender.sendPipeline(make([]byte, 10), make([]byte, 10), true, 100, "")
		require.Nil(t, err)

		require.Equal(t, 1, sender.streamSender.(*fakeStreamSender).sentCnt)
		require.Equal(t, pipeline.StreamTeardownMode_FinishAck,
			sender.streamSender.(*fakeStreamSender).sent[0].(*pipeline.Message).GetRequestedTeardownMode())
	}

	{
		// there should be cut as multiple message to send for a big data.
		sender.streamSender.(*fakeStreamSender).sentCnt = 0
		sender.streamSender.(*fakeStreamSender).sent = nil
		sender.streamSender.(*fakeStreamSender).nextSendError = nil

		err := sender.sendPipeline(make([]byte, 10), make([]byte, 10), true, 5, "")
		require.Nil(t, err)

		require.True(t, sender.streamSender.(*fakeStreamSender).sentCnt > 1)
		for _, sent := range sender.streamSender.(*fakeStreamSender).sent {
			require.Equal(t, pipeline.StreamTeardownMode_FinishAck,
				sent.(*pipeline.Message).GetRequestedTeardownMode())
		}
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

func TestReceiveMsgAndForward_AcknowledgesBatchOnReceiverTerminal(t *testing.T) {
	forwardReg := process.NewPipelineEdge(1, 0)
	require.True(t, forwardReg.Abort(moerr.NewInternalErrorNoCtx("receiver terminal")))

	stream := &fakeStreamSender{}
	sender := &messageSenderOnClient{
		ctx:          context.Background(),
		mp:           mpool.MustNewZero(),
		streamSender: stream,
		receiveCh:    make(chan morpc.Message, 1),
	}
	message := makeRemoteBatchMessage(t, batch.NewWithSize(0)).(*pipeline.Message)
	message.BatchSequence = 5
	sender.receiveCh <- message

	done := make(chan error, 1)
	go func() {
		done <- receiveMsgAndForward(sender, forwardReg)
	}()

	select {
	case err := <-done:
		require.NoError(t, err)
		require.Zero(t, sender.pendingBatchAck)
		require.Len(t, stream.sent, 1)
		ack := stream.sent[0].(*pipeline.Message)
		require.Equal(t, pipeline.Method_PipelineBatchAck, ack.GetCmd())
		require.Equal(t, uint64(5), ack.GetBatchAckSequence())
	case <-time.After(time.Second):
		require.Fail(t, "receiveMsgAndForward did not unblock after receiver terminal")
	}
}

func TestReceiveMessageFromCnServerIfConnector_AcknowledgesBatchOnReceiverTerminal(t *testing.T) {
	proc := testutil.NewProcess(t)
	proc.BuildPipelineContext(context.Background())

	reg := process.NewPipelineEdge(1, 0)
	require.True(t, reg.Abort(moerr.NewInternalErrorNoCtx("receiver terminal")))
	s := &Scope{
		Proc:   proc,
		RootOp: connector.NewArgument().WithReg(reg),
	}

	stream := &fakeStreamSender{}
	sender := &messageSenderOnClient{
		ctx:          context.Background(),
		mp:           proc.Mp(),
		streamSender: stream,
		receiveCh:    make(chan morpc.Message, 1),
	}
	message := makeRemoteBatchMessage(t, batch.NewWithSize(0)).(*pipeline.Message)
	message.BatchSequence = 7
	sender.receiveCh <- message

	done := make(chan error, 1)
	go func() {
		done <- receiveMessageFromCnServerIfConnector(s, sender)
	}()

	select {
	case err := <-done:
		require.NoError(t, err)
		require.Zero(t, sender.pendingBatchAck)
		require.Len(t, stream.sent, 1)
		ack := stream.sent[0].(*pipeline.Message)
		require.Equal(t, pipeline.Method_PipelineBatchAck, ack.GetCmd())
		require.Equal(t, uint64(7), ack.GetBatchAckSequence())
	case <-time.After(time.Second):
		require.Fail(t, "receiveMessageFromCnServerIfConnector did not unblock after receiver terminal")
	}
}

func TestReceiveMessageFromCnServerIfDispatch_AcknowledgesBatchOnReceiverTerminal(t *testing.T) {
	proc := testutil.NewProcess(t)
	reg := process.NewPipelineEdge(1, 0)
	require.True(t, reg.Abort(moerr.NewInternalErrorNoCtx("receiver terminal")))

	d := dispatch.NewArgument()
	d.LocalRegs = []*process.WaitRegister{reg}
	d.FuncId = dispatch.SendToAllLocalFunc
	s := &Scope{Proc: proc, RootOp: d}

	stream := &fakeStreamSender{}
	sender := &messageSenderOnClient{
		ctx:          context.Background(),
		mp:           proc.Mp(),
		streamSender: stream,
		receiveCh:    make(chan morpc.Message, 1),
	}
	dataBat := batch.NewWithSize(0)
	dataBat.SetRowCount(1)
	message := makeRemoteBatchMessage(t, dataBat).(*pipeline.Message)
	message.BatchSequence = 11
	sender.receiveCh <- message

	require.NoError(t, receiveMessageFromCnServerIfDispatch(s, sender))
	require.Zero(t, sender.pendingBatchAck)
	require.Len(t, stream.sent, 1)
	ack := stream.sent[0].(*pipeline.Message)
	require.Equal(t, pipeline.Method_PipelineBatchAck, ack.GetCmd())
	require.Equal(t, uint64(11), ack.GetBatchAckSequence())
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

func TestRemoteNotifyCleanupEndDoesNotWaitForChannelCapacity(t *testing.T) {
	oldSignalSendTimeout := process.PipelineSignalSendTimeout
	process.PipelineSignalSendTimeout = 200 * time.Millisecond
	t.Cleanup(func() {
		process.PipelineSignalSendTimeout = oldSignalSendTimeout
	})

	reg := process.NewPipelineEdge(1, 0)
	reg.Ch2 <- process.NewPipelineSignalToDirectly(batch.EmptyBatch, nil, nil)

	start := time.Now()
	require.True(t, sendRemoteNotifyCleanupTerminal(nil, reg, nil))
	elapsed := time.Since(start)

	require.Less(t, elapsed, 300*time.Millisecond)
	select {
	case <-reg.Done():
	default:
		t.Fatal("durable End should mark the remote notify edge terminal")
	}
	require.NoError(t, reg.Err())

	receiver := process.InitPipelineSignalReceiver(context.Background(), []*process.WaitRegister{reg})
	got, err := receiver.GetNextBatch(nil)
	require.NoError(t, err)
	require.Same(t, batch.EmptyBatch, got)
	got, err = receiver.GetNextBatch(nil)
	require.NoError(t, err)
	require.Nil(t, got)
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

func TestMongoScanPipelineRoundTripContainsNoCredential(t *testing.T) {
	ctx := &scopeContext{
		id:    0,
		plan:  &planpb.Plan{},
		scope: &Scope{},
		root:  &scopeContext{},
		regs:  make(map[*process.WaitRegister]int32),
	}
	ctx.root = ctx
	querySource := `{"filter":{"meta.pump":"pump-1"}}`
	query, err := sqlmongodb.ParseUserQuery(t.Context(), querySource)
	require.NoError(t, err)
	spec := &planpb.MongoScan{
		TableId: 33, MappingId: 11, MappingVersion: 4, ConnectionId: 22, ConnectionVersion: 3,
		Database: "telemetry", Collection: "raw", MaxParallelism: 1,
		Columns:         []*planpb.MongoColumnMapping{{Name: "pump", Path: "meta.pump", MoType: planpb.Type{Id: int32(types.T_varchar)}}},
		PushedPredicate: &planpb.MongoPredicate{Op: planpb.MongoPredicateOp_MONGO_PREDICATE_EQUAL, Path: "meta.pump", ValueBson: []byte{3, 0, 0, 0, 10, 0}},
	}
	require.NoError(t, sqlmongodb.ApplyUserQueryToPlan(t.Context(), query, spec))
	original := mongoscan.NewArgument().WithScan(spec)
	defer original.Release()

	_, instruction, err := convertToPipelineInstruction(original, nil, ctx, 0)
	require.NoError(t, err)
	wire, err := instruction.Marshal()
	require.NoError(t, err)
	for _, forbidden := range []string{"mongodb://", "secret://", "username", "password", "credential", "token"} {
		require.False(t, bytes.Contains(bytes.ToLower(wire), []byte(forbidden)))
	}
	require.False(t, bytes.Contains(wire, []byte(querySource)), "raw __mo_query text must not be transported")

	decoded := new(pipeline.Instruction)
	require.NoError(t, decoded.Unmarshal(wire))
	restoredOperator, err := convertToVmOperator(decoded, ctx, nil)
	require.NoError(t, err)
	restored := restoredOperator.(*mongoscan.MongoScan)
	defer restored.Release()
	require.Equal(t, spec, restored.Scan)
}

func TestMongoScanRemoteProtocolValidationAtSendAndReceiveBoundaries(t *testing.T) {
	proc := testutil.NewProcess(t)
	rt := moruntime.ServiceRuntime(proc.GetService())
	previous, hadPrevious := rt.GetGlobalVariables(moruntime.MOProtocolVersion)
	t.Cleanup(func() {
		if hadPrevious {
			rt.SetGlobalVariables(moruntime.MOProtocolVersion, previous)
		} else {
			rt.CompareAndDeleteGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion39)
		}
	})

	query, err := sqlmongodb.ParseUserQuery(t.Context(), `{"filter":{"value":1}}`)
	require.NoError(t, err)
	explicitQuery := &planpb.MongoScan{MaxParallelism: 1}
	require.NoError(t, sqlmongodb.ApplyUserQueryToPlan(t.Context(), query, explicitQuery))

	for name, spec := range map[string]*planpb.MongoScan{
		"explicit query":       explicitQuery,
		"query column carrier": {MaxParallelism: 1, IncludeQueryColumn: true},
		"pruned empty result":  {MaxParallelism: 1, EmptyResult: true},
	} {
		t.Run(name, func(t *testing.T) {
			scope := &Scope{Proc: proc, RootOp: mongoscan.NewArgument().WithScan(spec)}

			// A statement can compile while v44 is live, then encounter a rollback
			// before remote encoding. The sender must not serialize a payload that an
			// older receiver would silently interpret as a legacy Find.
			rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion44)
			data, err := encodeRemoteScope(scope, proc)
			require.NoError(t, err)
			rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion43)
			_, err = encodeRemoteScope(scope, proc)
			require.ErrorContains(t, err, "MORPC protocol version 44")
			_, err = decodeScope(data, proc, true, nil)
			require.ErrorContains(t, err, "MORPC protocol version 44")
		})
	}
}

func TestPartitionTopNPipelineRoundTrip(t *testing.T) {
	ctx := &scopeContext{
		id:    0,
		plan:  &planpb.Plan{},
		scope: &Scope{},
		root:  &scopeContext{},
		regs:  make(map[*process.WaitRegister]int32),
	}
	ctx.root = ctx
	limit := plan.MakePlan2Uint64ConstExprWithType(7)
	original := partition.NewArgument()
	original.OrderBySpecs = []*planpb.OrderBySpec{
		{Expr: &planpb.Expr{Typ: planpb.Type{Id: int32(types.T_int64)}, Expr: &planpb.Expr_Col{Col: &planpb.ColRef{ColPos: 0}}}},
		{Expr: &planpb.Expr{Typ: planpb.Type{Id: int32(types.T_int64)}, Expr: &planpb.Expr_Col{Col: &planpb.ColRef{ColPos: 1}}}, Flag: planpb.OrderBySpec_DESC},
	}
	original.Limit = limit
	original.PartitionByCount = 1
	original.PreReduce = true
	defer original.Release()

	_, instruction, err := convertToPipelineInstruction(original, nil, ctx, 0)
	require.NoError(t, err)
	wire, err := instruction.Marshal()
	require.NoError(t, err)
	decoded := new(pipeline.Instruction)
	require.NoError(t, decoded.Unmarshal(wire))
	restoredOperator, err := convertToVmOperator(decoded, ctx, nil)
	require.NoError(t, err)
	restored := restoredOperator.(*partition.Partition)
	defer restored.Release()
	require.Equal(t, int32(1), restored.PartitionByCount)
	require.True(t, restored.PreReduce)
	require.Len(t, restored.OrderBySpecs, 2)
	require.Equal(t, uint64(7), restored.Limit.GetLit().GetU64Val())
	require.Equal(t, planpb.OrderBySpec_DESC, restored.OrderBySpecs[1].Flag)
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
//	that lives in a separate send tree) -> the remote tree is not independently executable.
//	Historically RemoteRun converted it to local, mispaired the dispatch with the compile-time
//	cross-CN receiver FromAddr, and hung; now RemoteRun rejects that topology before start.
//
//	after regrouping, the dop same-CN buckets (and the nested dispatch) become one per-CN
//	send unit, so checkPipelineStandaloneExecutableAtRemote returns true and the whole
//	group is really executed at the remote CN.
func TestGroupShuffleBucketsByCNIfNeeded(t *testing.T) {
	for _, test := range []struct {
		name      string
		localOnly bool
	}{
		{name: "mixed local and remote shuffle targets"},
		{name: "local-only shuffle targets", localOnly: true},
	} {
		t.Run(test.name, func(t *testing.T) {
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

			if test.localOnly {
				// A remote CN can own a shuffle source whose targets are all local
				// receiver buckets on that CN. RemoteRegs is empty, but the source
				// still cannot be sent as a separate tree because its LocalRegs
				// belong to sibling bucket trees.
				for _, source := range []*Scope{srcCN1, srcCN2} {
					source.RootOp.(*dispatch.Dispatch).RemoteRegs = nil
				}
			}

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
		})
	}
}

// TestGroupShuffleBucketsByCNIfNeeded_Gating verifies the regrouping is a no-op when
// there is no out-of-tree local receiver dependency, so non-shuffle inserts are
// completely unaffected.
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

func TestCoordinatorLocalShuffleAttachesRemoteDispatchSource(t *testing.T) {
	c := NewMockCompile(t)
	proc := c.proc
	receivers := make([]*Scope, 2)
	for i := range receivers {
		receivers[i] = &Scope{
			Magic:    Remote,
			NodeInfo: engine.Node{Addr: "cn-local:6001", Mcpu: 1},
			Proc:     proc.NewContextChildProc(1),
		}
		receivers[i].setRootOperator(merge.NewArgument())
	}

	remoteSource := newDispatchSrcScopeForTest(proc, "cn-remote:6001", nil, receivers)
	attachShuffleDispatchSource(receivers, remoteSource, true)

	require.Contains(t, receivers[0].PreScopes, remoteSource)
	require.True(t, checkPipelineStandaloneExecutableAtRemote(remoteSource),
		"remote source only has remote receiver routes and must remain remotely executable")
}

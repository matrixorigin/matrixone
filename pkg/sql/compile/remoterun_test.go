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
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/aggexec"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/apply"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/hashbuild"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/indexbuild"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/shufflebuild"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/common/reuse"

	"github.com/golang/mock/gomock"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/sql/colexec/source"

	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	"github.com/matrixorigin/matrixone/pkg/pb/pipeline"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/anti"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/deletion"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/dispatch"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/external"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/filter"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/group"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/insert"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/intersect"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/intersectall"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/join"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/left"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/limit"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/lockop"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/loopjoin"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/mark"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/merge"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/mergegroup"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/mergeorder"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/mergerecursive"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/mergetop"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/minus"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/offset"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/onduplicatekey"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/order"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/preinsert"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/preinsertunique"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/product"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/projection"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/right"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/rightanti"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/rightsemi"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/semi"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/shuffle"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/single"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/table_function"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/top"
	"github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

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
		nil)
	proc.Base.Id = "1"
	proc.Base.Lim = process.Limitation{}
	proc.Base.UnixTime = 1000000
	proc.Base.SessionInfo = process.SessionInfo{
		Account:        "",
		User:           "",
		Host:           "",
		Role:           "",
		ConnectionID:   0,
		AccountId:      0,
		RoleId:         0,
		UserId:         0,
		LastInsertID:   0,
		Database:       "",
		Version:        "",
		TimeZone:       time.Local,
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
		&onduplicatekey.OnDuplicatekey{},
		&preinsert.PreInsert{},
		&lockop.LockOp{},
		&preinsertunique.PreInsertUnique{},
		&anti.AntiJoin{
			Conditions: [][]*plan.Expr{nil, nil},
		},
		&shuffle.Shuffle{},
		&dispatch.Dispatch{},
		&group.Group{},
		&join.InnerJoin{
			Conditions: [][]*plan.Expr{nil, nil},
		},
		&left.LeftJoin{
			Conditions: [][]*plan.Expr{nil, nil},
		},
		&right.RightJoin{
			Conditions: [][]*plan.Expr{nil, nil},
		},
		&rightsemi.RightSemi{
			Conditions: [][]*plan.Expr{nil, nil},
		},
		&rightanti.RightAnti{
			Conditions: [][]*plan.Expr{nil, nil},
		},
		&limit.Limit{},
		&loopjoin.LoopJoin{},
		&offset.Offset{},
		&order.Order{},
		&product.Product{},
		&projection.Projection{},
		&filter.Filter{},
		&semi.SemiJoin{
			Conditions: [][]*plan.Expr{nil, nil},
		},
		&single.SingleJoin{
			Conditions: [][]*plan.Expr{nil, nil},
		},
		&top.Top{},
		&intersect.Intersect{},
		&minus.Minus{},
		&intersectall.IntersectAll{},
		&merge.Merge{},
		&mergerecursive.MergeRecursive{},
		&mergegroup.MergeGroup{},
		&mergetop.MergeTop{},
		&mergeorder.MergeOrder{},
		&mark.MarkJoin{
			Conditions: [][]*plan.Expr{nil, nil},
		},
		&table_function.TableFunction{},
		&external.External{
			Es: &external.ExternalParam{
				ExParam: exParam,
			},
		},
		&hashbuild.HashBuild{},
		&shufflebuild.ShuffleBuild{},
		&indexbuild.IndexBuild{},
		&source.Source{},
		&apply.Apply{TableFunction: &table_function.TableFunction{}},
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
		{Op: int32(vm.OnDuplicateKey), OnDuplicateKey: &pipeline.OnDuplicateKey{}},
		{Op: int32(vm.Anti), Anti: &pipeline.AntiJoin{}},
		{Op: int32(vm.Shuffle), Shuffle: &pipeline.Shuffle{}},
		{Op: int32(vm.Dispatch), Dispatch: &pipeline.Dispatch{}},
		{Op: int32(vm.Group), Agg: &pipeline.Group{}},
		{Op: int32(vm.Join), Join: &pipeline.Join{}},
		{Op: int32(vm.Left), LeftJoin: &pipeline.LeftJoin{}},
		{Op: int32(vm.Right), RightJoin: &pipeline.RightJoin{}},
		{Op: int32(vm.RightSemi), RightSemiJoin: &pipeline.RightSemiJoin{}},
		{Op: int32(vm.RightAnti), RightAntiJoin: &pipeline.RightAntiJoin{}},
		{Op: int32(vm.Limit), Limit: plan.MakePlan2Int64ConstExprWithType(1)},
		{Op: int32(vm.LoopJoin), Join: &pipeline.Join{}},
		{Op: int32(vm.Offset), Offset: plan.MakePlan2Int64ConstExprWithType(0)},
		{Op: int32(vm.Order), OrderBy: []*plan.OrderBySpec{}},
		{Op: int32(vm.Product), Product: &pipeline.Product{}},
		{Op: int32(vm.ProductL2), ProductL2: &pipeline.ProductL2{}},
		{Op: int32(vm.Projection), ProjectList: []*plan.Expr{}},
		{Op: int32(vm.Filter), Filter: &plan.Expr{}},
		{Op: int32(vm.Semi), SemiJoin: &pipeline.SemiJoin{}},
		{Op: int32(vm.Single), SingleJoin: &pipeline.SingleJoin{}},
		{Op: int32(vm.Mark), MarkJoin: &pipeline.MarkJoin{}},
		{Op: int32(vm.Top), Limit: plan.MakePlan2Int64ConstExprWithType(1)},
		{Op: int32(vm.Intersect), Anti: &pipeline.AntiJoin{}},
		{Op: int32(vm.IntersectAll), Anti: &pipeline.AntiJoin{}},
		{Op: int32(vm.Minus), Anti: &pipeline.AntiJoin{}},
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
		{Op: int32(vm.ShuffleBuild), ShuffleBuild: &pipeline.Shufflebuild{}},
		{Op: int32(vm.IndexBuild), IndexBuild: &pipeline.Indexbuild{}},
		{Op: int32(vm.Apply), Apply: &pipeline.Apply{}, TableFunction: &pipeline.TableFunction{}},
	}
	for _, instruction := range instructions {
		_, err := convertToVmOperator(instruction, ctx, nil)
		require.Nil(t, err)
	}
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
	vp := process.NewTopProcess(
		context.TODO(),
		nil,
		nil,
		nil,
		nil,
		nil,
		nil,
		nil,
		nil,
		nil)
	aggexec.RegisterGroupConcatAgg(0, ",")
	agg0 := aggexec.MakeAgg(
		vp, 0, false, []types.Type{types.T_varchar.ToType()}...)

	bat := &batch.Batch{
		Recursive:  0,
		Ro:         false,
		ShuffleIDX: 0,
		Cnt:        1,
		Attrs:      []string{"1"},
		Vecs:       []*vector.Vector{vector.NewVec(types.T_int64.ToType())},
		Aggs:       []aggexec.AggFuncExec{agg0},
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
		p, c, err := receiver.GetProcByUuid(uid, time.Second)
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
		p, _, err := receiver.GetProcByUuid(uuid.UUID{}, time.Second)
		require.Nil(t, err)
		require.Nil(t, p)

		// two action to delete the uuid can make sure the producer and consumer flag uuid done.
		colexec.Get().DeleteUuids([]uuid.UUID{{}})
		colexec.Get().DeleteUuids([]uuid.UUID{{}})
	}

	{
		// if receiver gets proc timeout, should exit.
		// 1. return error.
		receiver := &messageReceiverOnServer{
			connectionCtx: context.TODO(),
		}
		p, _, err := receiver.GetProcByUuid(uuid.UUID{}, time.Second)
		require.NotNil(t, err)
		require.Nil(t, p)

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

		p, c, err := receiver.GetProcByUuid(uid, time.Second)
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

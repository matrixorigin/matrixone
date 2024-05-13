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
	"hash/crc32"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/sql/colexec/aggexec"
	"github.com/matrixorigin/matrixone/pkg/testutil"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/defines"

	"github.com/matrixorigin/matrixone/pkg/common/reuse"

	"github.com/golang/mock/gomock"
	"github.com/google/uuid"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/source"
	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/common/morpc"
	"github.com/matrixorigin/matrixone/pkg/common/morpc/mock_morpc"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	"github.com/matrixorigin/matrixone/pkg/pb/pipeline"
	plan2 "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/anti"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/connector"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/deletion"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/dispatch"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/external"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/group"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/hashbuild"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/insert"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/intersect"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/intersectall"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/join"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/left"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/limit"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/lockop"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/loopanti"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/loopjoin"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/loopleft"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/loopmark"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/loopsemi"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/loopsingle"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/mark"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/merge"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/mergegroup"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/mergelimit"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/mergeoffset"
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
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/restrict"
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

func Test_receiveMessageFromCnServer(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctx := context.TODO()

	streamSender := mock_morpc.NewMockStream(ctrl)
	ch := make(chan morpc.Message)
	streamSender.EXPECT().Receive().Return(ch, nil)
	aggexec.RegisterGroupConcatAgg(0, ",")
	agg0 := aggexec.MakeAgg(
		testutil.NewProcess(), 0, false, []types.Type{types.T_varchar.ToType()}...)

	bat := &batch.Batch{
		Recursive:  0,
		Ro:         false,
		ShuffleIDX: 0,
		Cnt:        1,
		Attrs:      []string{"1"},
		Vecs:       []*vector.Vector{vector.NewVec(types.T_int64.ToType())},
		Aggs:       []aggexec.AggFuncExec{agg0},
		AuxData:    nil,
	}
	bat.SetRowCount(1)
	data, err := types.Encode(bat)
	require.Nil(t, err)

	go func() {
		msg := &pipeline.Message{
			Data: data,
		}
		msg.Checksum = crc32.ChecksumIEEE(data)
		ch <- msg
	}()

	vp := process.New(
		ctx,
		nil,
		nil,
		nil,
		nil,
		nil,
		nil,
		nil,
		nil,
		nil)
	vp.AnalInfos = []*process.AnalyzeInfo{}
	vp.Reg = process.Register{}
	c := reuse.Alloc[Compile](nil)
	c.proc = vp
	s := reuse.Alloc[Scope](nil)
	s.Proc = vp
	sender := &messageSenderOnClient{
		ctx:          ctx,
		streamSender: streamSender,
		c:            c,
	}
	ch2 := make(chan *batch.Batch)
	ctx2, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	lastInstruction := vm.Instruction{
		Arg: &connector.Argument{
			Reg: &process.WaitRegister{
				Ctx: ctx2,
				Ch:  ch2,
			},
		},
	}
	err = receiveMessageFromCnServer(c, s, sender, lastInstruction)
	require.Nil(t, err)
}

func Test_EncodeProcessInfo(t *testing.T) {
	ctrl := gomock.NewController(t)
	txnOperator := mock_frontend.NewMockTxnOperator(ctrl)
	txnOperator.EXPECT().Snapshot().Return(([]byte)("test"), nil)

	a := reuse.Alloc[process.AnalyzeInfo](nil)
	proc := &process.Process{
		Id:          "1",
		Lim:         process.Limitation{},
		UnixTime:    1000000,
		Ctx:         defines.AttachAccountId(context.TODO(), catalog.System_Account),
		TxnOperator: txnOperator,
		AnalInfos:   []*process.AnalyzeInfo{a},
		SessionInfo: process.SessionInfo{
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
		},
	}
	_, err := encodeProcessInfo(proc, "")
	require.Nil(t, err)
}

func Test_refactorScope(t *testing.T) {
	ctx := context.TODO()
	proc := &process.Process{}

	s := reuse.Alloc[Scope](nil)
	s.Proc = proc
	c := reuse.Alloc[Compile](nil)
	c.anal = newAnaylze()
	c.ctx = ctx
	c.proc = proc
	rs := appendWriteBackOperator(c, s)
	require.Equal(t, rs.Instructions[1].Idx, -1)
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
	instructions := []*vm.Instruction{
		{
			Arg: &insert.Argument{
				InsertCtx: &insert.InsertCtx{},
			},
		},
		{
			Arg: &deletion.Argument{
				DeleteCtx: &deletion.DeleteCtx{},
			},
		},
		{
			Arg: &onduplicatekey.Argument{},
		},
		{
			Arg: &preinsert.Argument{},
		},
		{
			Arg: &lockop.Argument{},
		},
		{
			Arg: &preinsertunique.Argument{},
		},
		{
			Arg: &anti.Argument{
				Conditions: [][]*plan.Expr{nil, nil},
			},
		},
		{
			Arg: &shuffle.Argument{},
		},
		{
			Arg: &dispatch.Argument{},
		},
		{
			Arg: &group.Argument{},
		},
		{
			Arg: &join.Argument{
				Conditions: [][]*plan.Expr{nil, nil},
			},
		},
		{
			Arg: &left.Argument{
				Conditions: [][]*plan.Expr{nil, nil},
			},
		},
		{
			Arg: &right.Argument{
				Conditions: [][]*plan.Expr{nil, nil},
			},
		},
		{
			Arg: &rightsemi.Argument{
				Conditions: [][]*plan.Expr{nil, nil},
			},
		},
		{
			Arg: &rightanti.Argument{
				Conditions: [][]*plan.Expr{nil, nil},
			},
		},
		{
			Arg: &limit.Argument{},
		},
		{
			Arg: &loopanti.Argument{},
		},
		{
			Arg: &loopjoin.Argument{},
		},
		{
			Arg: &loopleft.Argument{},
		},
		{
			Arg: &loopsemi.Argument{},
		},
		{
			Arg: &loopsingle.Argument{},
		},
		{
			Arg: &loopmark.Argument{},
		},
		{
			Arg: &offset.Argument{},
		},
		{
			Arg: &order.Argument{},
		},
		{
			Arg: &product.Argument{},
		},
		{
			Arg: &projection.Argument{},
		},
		{
			Arg: &restrict.Argument{},
		},
		{
			Arg: &semi.Argument{
				Conditions: [][]*plan.Expr{nil, nil},
			},
		},
		{
			Arg: &single.Argument{
				Conditions: [][]*plan.Expr{nil, nil},
			},
		},
		{
			Arg: &top.Argument{},
		},
		{
			Arg: &intersect.Argument{},
		},
		{
			Arg: &minus.Argument{},
		},
		{
			Arg: &intersectall.Argument{},
		},
		{
			Arg: &merge.Argument{},
		},
		{
			Arg: &mergerecursive.Argument{},
		},
		{
			Arg: &mergegroup.Argument{},
		},
		{
			Arg: &mergelimit.Argument{},
		},
		{
			Arg: &mergeoffset.Argument{},
		},
		{
			Arg: &mergetop.Argument{},
		},
		{
			Arg: &mergeorder.Argument{},
		},
		//{
		//	Arg: &connector.Argument{},
		//},
		{
			Arg: &mark.Argument{
				Conditions: [][]*plan.Expr{nil, nil},
			},
		},
		{
			Arg: &table_function.Argument{},
		},
		{
			Arg: &hashbuild.Argument{},
		},
		{
			Arg: &external.Argument{
				Es: &external.ExternalParam{
					ExParam: exParam,
				},
			},
		},
		{
			Arg: &source.Argument{},
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
	for _, instruction := range instructions {
		_, _, err := convertToPipelineInstruction(instruction, ctx, 1)
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
		{Op: int32(vm.LoopAnti), Anti: &pipeline.AntiJoin{}},
		{Op: int32(vm.LoopJoin), Join: &pipeline.Join{}},
		{Op: int32(vm.LoopLeft), LeftJoin: &pipeline.LeftJoin{}},
		{Op: int32(vm.LoopSemi), SemiJoin: &pipeline.SemiJoin{}},
		{Op: int32(vm.LoopSingle), SingleJoin: &pipeline.SingleJoin{}},
		{Op: int32(vm.LoopMark), MarkJoin: &pipeline.MarkJoin{}},
		{Op: int32(vm.Offset), Offset: plan.MakePlan2Int64ConstExprWithType(0)},
		{Op: int32(vm.Order), OrderBy: []*plan.OrderBySpec{}},
		{Op: int32(vm.Product), Product: &pipeline.Product{}},
		{Op: int32(vm.Projection), ProjectList: []*plan.Expr{}},
		{Op: int32(vm.Restrict), Filter: &plan.Expr{}},
		{Op: int32(vm.Semi), SemiJoin: &pipeline.SemiJoin{}},
		{Op: int32(vm.Single), SingleJoin: &pipeline.SingleJoin{}},
		{Op: int32(vm.Mark), MarkJoin: &pipeline.MarkJoin{}},
		{Op: int32(vm.Top), Limit: plan.MakePlan2Int64ConstExprWithType(1)},
		{Op: int32(vm.Intersect), Anti: &pipeline.AntiJoin{}},
		{Op: int32(vm.IntersectAll), Anti: &pipeline.AntiJoin{}},
		{Op: int32(vm.Minus), Anti: &pipeline.AntiJoin{}},
		{Op: int32(vm.Connector), Connect: &pipeline.Connector{}},
		{Op: int32(vm.Merge)},
		{Op: int32(vm.MergeRecursive)},
		{Op: int32(vm.MergeGroup), Agg: &pipeline.Group{}},
		{Op: int32(vm.MergeLimit), Limit: plan.MakePlan2Int64ConstExprWithType(1)},
		{Op: int32(vm.MergeOffset), Offset: plan.MakePlan2Int64ConstExprWithType(0)},
		{Op: int32(vm.MergeTop), Limit: plan.MakePlan2Int64ConstExprWithType(1)},
		{Op: int32(vm.MergeOrder), OrderBy: []*plan.OrderBySpec{}},
		{Op: int32(vm.TableFunction), TableFunction: &pipeline.TableFunction{}},
		{Op: int32(vm.HashBuild), HashBuild: &pipeline.HashBuild{}},
		{Op: int32(vm.External), ExternalScan: &pipeline.ExternalScan{}},
		{Op: int32(vm.Source), StreamScan: &pipeline.StreamScan{}},
	}
	for _, instruction := range instructions {
		_, err := convertToVmInstruction(instruction, ctx, nil)
		require.Nil(t, err)
	}
}

func Test_mergeAnalyseInfo(t *testing.T) {
	target := newAnaylze()
	a := reuse.Alloc[process.AnalyzeInfo](nil)
	target.analInfos = []*process.AnalyzeInfo{a}
	ana := &pipeline.AnalysisList{
		List: []*plan2.AnalyzeInfo{
			{},
		},
	}
	mergeAnalyseInfo(target, ana)
	require.Equal(t, len(ana.List), 1)
}

func Test_convertToProcessLimitation(t *testing.T) {
	lim := &pipeline.ProcessLimitation{
		Size: 100,
	}
	limitation := convertToProcessLimitation(lim)
	require.Equal(t, limitation.Size, int64(100))
}

func Test_convertToProcessSessionInfo(t *testing.T) {
	ti, _ := time.Now().MarshalBinary()
	sei := &pipeline.SessionInfo{
		TimeZone: ti,
	}
	_, err := convertToProcessSessionInfo(sei)
	require.Nil(t, err)
}

func Test_convertToPlanAnalyzeInfo(t *testing.T) {
	info := reuse.Alloc[process.AnalyzeInfo](nil)
	info.InputRows = 100
	analyzeInfo := convertToPlanAnalyzeInfo(info)
	require.Equal(t, analyzeInfo.InputRows, int64(100))
}

func Test_decodeBatch(t *testing.T) {
	mp := &mpool.MPool{}
	vp := process.New(
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
		AuxData:    nil,
	}
	bat.SetRowCount(1)
	data, err := types.Encode(bat)
	require.Nil(t, err)
	_, err = decodeBatch(mp, data)
	require.Nil(t, err)
}

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
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/aggexec"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/anti"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/apply"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/connector"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/dedupjoin"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/deletion"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/dispatch"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/external"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/filter"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/group"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/hashbuild"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/indexbuild"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/insert"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/intersect"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/intersectall"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/join"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/left"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/limit"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/lockop"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/loopjoin"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/merge"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/mergegroup"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/mergeorder"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/mergerecursive"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/mergetop"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/minus"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/offset"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/onduplicatekey"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/order"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/postdml"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/preinsert"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/preinsertunique"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/product"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/projection"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/right"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/rightanti"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/rightsemi"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/semi"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/shuffle"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/shufflebuild"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/single"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/source"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/table_function"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/top"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/value_scan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
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
		{Op: int32(vm.PostDml), PostDml: &pipeline.PostDml{}},
		{Op: int32(vm.DedupJoin), DedupJoin: &pipeline.DedupJoin{}},
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
		ShuffleIDX: 0,
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
	proc := testutil.NewProcess()
	proc.Ctx = context.WithValue(proc.Ctx, defines.TenantIDKey{}, uint32(0))
	proc.Base.TxnOperator = fakeTxnOperator{}

	// if this is a pipeline with operator list "connector / dispatch".
	// this should return withoutOut == false.
	s1 := &Scope{
		Proc:   proc,
		RootOp: connector.NewArgument(),
	}
	_, withoutOut, _, err := prepareRemoteRunSendingData("", s1)
	require.NoError(t, err)
	require.False(t, withoutOut)

	// if this is a pipeline with operator list "scan -> connector / dispatch".
	// this should return withoutOut == false.
	s2 := &Scope{
		Proc:   proc,
		RootOp: dispatch.NewArgument(),
	}
	s2.RootOp.AppendChild(value_scan.NewArgument())
	_, withoutOut, _, err = prepareRemoteRunSendingData("", s2)
	require.NoError(t, err)
	require.False(t, withoutOut)

	// if this is a pipeline no need to sent back message, like "scan -> scan".
	// this should return withoutOut == true.
	s3 := &Scope{
		Proc:   proc,
		RootOp: value_scan.NewArgument(),
	}
	s3.RootOp.AppendChild(value_scan.NewArgument())
	_, withoutOut, _, err = prepareRemoteRunSendingData("", s3)
	require.NoError(t, err)
	require.True(t, withoutOut)
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
	proc := testutil.NewProcess()
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

func Test_checkPipelineStandaloneExecutableAtRemote(t *testing.T) {
	proc := testutil.NewProcess()
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

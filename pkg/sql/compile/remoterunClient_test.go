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

package compile

import (
	"bytes"
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/cnservice/cnclient"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/morpc"
	"github.com/matrixorigin/matrixone/pkg/common/morpc/mock_morpc"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/pipeline"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/connector"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/deletion"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/dispatch"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/insert"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/merge"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/mergeblock"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/mergedelete"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/multi_update"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/value_scan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/util/fault"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

var _ cnclient.PipelineClient = new(testPipelineClient)

func TestScopeS3Output(t *testing.T) {
	update := multi_update.NewArgument()
	update.Action = multi_update.UpdateWriteS3
	update.IsRemote = true
	connectorOp := connector.NewArgument()
	connectorOp.AppendChild(update)

	require.Equal(t, remoteS3MultiUpdate, scopeS3Output(&Scope{RootOp: connectorOp}))
	update.IsRemote = false
	require.Equal(t, remoteS3MultiUpdate, scopeS3Output(&Scope{RootOp: connectorOp}))
	update.IsRemote = true
	update.Action = multi_update.UpdateFlushS3Info
	require.Equal(t, remoteS3None, scopeS3Output(&Scope{RootOp: connectorOp}))
	update.Action = multi_update.UpdateWriteS3
	group := &Scope{RootOp: merge.NewArgument(), PreScopes: []*Scope{{RootOp: connectorOp}}}
	require.Equal(t, remoteS3MultiUpdate, scopeS3Output(group))
	group.RootOp = nil
	require.Equal(t, remoteS3MultiUpdate, scopeS3Output(group))
	group.RootOp = dispatch.NewArgument()
	require.Equal(t, remoteS3MultiUpdate, scopeS3Output(group))
	// Consumers are a boundary even when a grouped PreScope still contains a producer.
	for _, consumer := range []vm.Operator{&multi_update.MultiUpdate{Action: multi_update.UpdateFlushS3Info}, &mergeblock.MergeBlock{}, &mergedelete.MergeDelete{}} {
		consumer.GetOperatorBase().AppendChild(update)
		group.RootOp = consumer
		require.Equal(t, remoteS3None, scopeS3Output(group))
		wrapper := &connector.Connector{}
		wrapper.AppendChild(consumer)
		group.RootOp = wrapper
		require.Equal(t, remoteS3None, scopeS3Output(group))
	}
	for _, tc := range []struct {
		op   vm.Operator
		kind remoteS3Output
	}{
		{&insert.Insert{ToWriteS3: true}, remoteS3Insert},
		{&insert.Insert{}, remoteS3None},
		{&deletion.Deletion{RemoteDelete: true}, remoteS3Delete},
		{&deletion.Deletion{}, remoteS3None},
	} {
		require.Equal(t, tc.kind, scopeS3Output(&Scope{RootOp: tc.op}))
	}

	connectorOp.Release()
	update.Release()
}

func TestRemoteS3MetadataNames(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()
	out, name := remoteS3TestOutput(t, proc, remoteS3Insert)
	defer out.Clean(proc.Mp())
	names, err := remoteS3MetadataNames(out)
	require.NoError(t, err)
	require.Equal(t, []string{name}, names)
	// Real producer layout: two block records but only one object-stats record.
	multiBlock := colexec.AllocCNS3ResultBat(false)
	defer multiBlock.Clean(proc.Mp())
	multiStats := objectio.ObjectStats(append([]byte(nil), out.Vecs[1].GetBytesAt(0)...))
	require.NoError(t, objectio.SetObjectStatsBlkCnt(&multiStats, 2))
	require.NoError(t, objectio.SetObjectStatsRowCnt(&multiStats, 8193))
	require.NoError(t, colexec.ExpandObjectStatsToBatch(proc.Mp(), false, multiBlock, true, multiStats))
	require.Equal(t, 2, multiBlock.RowCount())
	require.Equal(t, 1, multiBlock.Vecs[1].Length())
	names, err = remoteS3MetadataNames(multiBlock)
	require.NoError(t, err)
	require.NotEmpty(t, names)
	for _, parsed := range names {
		require.Equal(t, name, parsed)
	}
	for _, vec := range multiBlock.Vecs {
		vec.CleanOnlyData()
	}
	_, err = remoteS3MetadataNames(multiBlock)
	require.ErrorContains(t, err, "no object names")

	// Legacy mixed output: a negative table index carries serialized raw data,
	// while block-info rows carry names even without an object-stats column.
	legacy := batch.NewWithSize(2)
	defer legacy.Clean(proc.Mp())
	legacy.Attrs = []string{catalog.BlockMeta_TableIdx_Insert, catalog.BlockMeta_BlockInfo}
	legacy.Vecs[0] = vector.NewVec(types.T_int16.ToType())
	legacy.Vecs[1] = vector.NewVec(types.T_text.ToType())
	require.NoError(t, vector.AppendFixed(legacy.Vecs[0], int16(-1), false, proc.Mp()))
	require.NoError(t, vector.AppendBytes(legacy.Vecs[1], []byte("raw batch, not block info"), false, proc.Mp()))
	legacy.SetRowCount(1)
	names, err = remoteS3MetadataNames(legacy)
	require.NoError(t, err)
	require.Empty(t, names)
	require.NoError(t, vector.SetFixedAtNoTypeCheck(legacy.Vecs[0], 0, int16(0)))
	_, err = remoteS3MetadataNames(legacy)
	require.ErrorContains(t, err, "invalid remote S3 block info")
	require.NoError(t, vector.SetFixedAtNoTypeCheck(legacy.Vecs[0], 0, int16(-1)))
	stats := objectio.ObjectStats(out.Vecs[1].GetBytesAt(0))
	block := &objectio.BlockInfo{}
	block.SetMetaLocation(objectio.BuildLocation(stats.ObjectName(), objectio.Extent{}, 1, 0))
	require.NoError(t, vector.AppendFixed(legacy.Vecs[0], int16(0), false, proc.Mp()))
	require.NoError(t, vector.AppendBytes(legacy.Vecs[1], objectio.EncodeBlockInfo(block), false, proc.Mp()))
	legacy.SetRowCount(2)
	names, err = remoteS3MetadataNames(legacy)
	require.NoError(t, err)
	require.Equal(t, []string{name}, names)
	// Stats are packed by object, not aligned with the legacy table-index rows.
	legacy.Attrs = append(legacy.Attrs, catalog.ObjectMeta_ObjectStats)
	legacy.Vecs = append(legacy.Vecs, vector.NewVec(types.T_binary.ToType()))
	require.NoError(t, vector.AppendBytes(legacy.Vecs[2], stats[:], false, proc.Mp()))
	names, err = remoteS3MetadataNames(legacy)
	require.NoError(t, err)
	require.Equal(t, []string{name, name}, names)

	malformed := colexec.AllocCNS3ResultBat(true)
	defer malformed.Clean(proc.Mp())
	require.NoError(t, vector.AppendBytes(malformed.Vecs[0], []byte("short"), false, proc.Mp()))
	malformed.SetRowCount(1)
	_, err = remoteS3MetadataNames(malformed)
	require.ErrorContains(t, err, "invalid remote S3 object stats")
}

func TestRemoteS3DeleteIgnoresRawRowsAndRejectsMalformedMetadata(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()
	out, _ := remoteS3TestOutput(t, proc, remoteS3Delete)
	defer out.Clean(proc.Mp())
	out.Vecs[1].CleanOnlyData()
	require.NoError(t, vector.AppendBytes(out.Vecs[1], []byte("not a nested metadata batch"), false, proc.Mp()))
	require.NoError(t, vector.SetFixedAtNoTypeCheck(out.Vecs[2], 0, int8(deletion.FlushDeltaLoc+1)))
	require.NoError(t, retainRemoteS3Output(proc, remoteS3Delete, out))
	require.NoError(t, vector.SetFixedAtNoTypeCheck(out.Vecs[2], 0, int8(deletion.FlushDeltaLoc)))
	require.Error(t, retainRemoteS3Output(proc, remoteS3Delete, out))
}

func TestRemoteS3RejectsMissingNamesBeforeAck(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()
	out := colexec.AllocCNS3ResultBat(false)
	out.SetRowCount(1)
	data, err := out.MarshalBinaryForPipeline(&bytes.Buffer{}, true, true)
	require.NoError(t, err)
	out.Clean(proc.Mp())
	responses := make(chan morpc.Message, 1)
	responses <- &pipeline.Message{Cmd: pipeline.Method_BatchMessage, Sid: pipeline.Status_Last, Data: data, BatchSequence: 1}
	close(responses)
	// No Send expectation: malformed metadata must never be acknowledged.
	stream := mock_morpc.NewMockStream(gomock.NewController(t))
	sender := &messageSenderOnClient{ctx: proc.Ctx, mp: proc.Mp(), receiveCh: responses, streamSender: stream}
	reg := process.NewPipelineEdge(1, 0)
	op := connector.NewArgument()
	defer op.Release()
	op.Reg = reg
	op.AppendChild(&insert.Insert{ToWriteS3: true})
	err = receiveMessageFromCnServerIfConnector(&Scope{Proc: proc, RootOp: op}, sender)
	require.ErrorContains(t, err, "no object names")
	require.Empty(t, reg.Ch2)
	require.Equal(t, uint64(1), sender.pendingBatchAck)
}

func remoteS3TestOutput(t *testing.T, proc *process.Process, kind remoteS3Output) (*batch.Batch, string) {
	t.Helper()
	id := types.Uuid{1, 2, 3}
	name := objectio.BuildObjectName(&id, 1)
	stats := objectio.NewObjectStats()
	require.NoError(t, objectio.SetObjectStatsObjectName(stats, name))
	metadata := colexec.AllocCNS3ResultBat(kind != remoteS3Insert)
	statsCol := 0
	if kind == remoteS3Insert {
		statsCol = 1
		require.NoError(t, vector.AppendBytes(metadata.Vecs[0], nil, false, proc.Mp()))
	}
	require.NoError(t, vector.AppendBytes(metadata.Vecs[statsCol], stats[:], false, proc.Mp()))
	metadata.SetRowCount(1)
	if kind == remoteS3Insert {
		return metadata, name.String()
	}
	data, err := metadata.MarshalBinary()
	require.NoError(t, err)
	metadata.Clean(proc.Mp())
	out := batch.NewWithSize(5)
	for i := range out.Vecs {
		out.Vecs[i] = vector.NewVec(types.T_text.ToType())
	}
	if kind == remoteS3Delete {
		out.Vecs[2] = vector.NewVec(types.T_int8.ToType())
		require.NoError(t, vector.AppendFixed(out.Vecs[2], int8(deletion.FlushDeltaLoc), false, proc.Mp()))
		require.NoError(t, vector.AppendBytes(out.Vecs[1], data, false, proc.Mp()))
	} else {
		out.Vecs[0] = vector.NewVec(types.T_uint8.ToType())
		require.NoError(t, vector.AppendFixed(out.Vecs[0], uint8(1), false, proc.Mp())) // actionDelete
		require.NoError(t, vector.AppendBytes(out.Vecs[4], data, false, proc.Mp()))
	}
	for _, vec := range out.Vecs {
		if vec.Length() == 0 {
			require.NoError(t, vector.AppendBytes(vec, nil, false, proc.Mp()))
		}
	}
	out.SetRowCount(1)
	return out, name.String()
}

func TestRemoteS3ReceiveTakeoverBeforeAck(t *testing.T) {
	for _, path := range []string{"connector", "dispatch", "no-output"} {
		for _, kind := range []remoteS3Output{remoteS3MultiUpdate, remoteS3Insert, remoteS3Delete} {
			for _, failTakeover := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s/%d/fail=%t", path, kind, failTakeover), func(t *testing.T) {
					ctrl := gomock.NewController(t)
					fs, err := fileservice.NewMemoryFS(defines.SharedFileServiceName, fileservice.CacheConfig{}, nil)
					require.NoError(t, err)
					proc := testutil.NewProcess(t, testutil.WithFileService(fs))
					defer proc.Free()
					workspace := &remoteS3CleanupWorkspace{}
					if !failTakeover {
						txn := mock_frontend.NewMockTxnOperator(ctrl)
						txn.EXPECT().GetWorkspace().Return(workspace).AnyTimes()
						proc.Base.TxnOperator = txn
					} else {
						proc.Base.TxnOperator = nil
					}
					out, name := remoteS3TestOutput(t, proc, kind)
					data, err := out.MarshalBinaryForPipeline(&bytes.Buffer{}, true, true)
					require.NoError(t, err)
					out.Clean(proc.Mp())
					responses := make(chan morpc.Message, 2)
					responses <- &pipeline.Message{Cmd: pipeline.Method_BatchMessage, Sid: pipeline.Status_Last, Data: data, BatchSequence: 1}
					close(responses)
					stream := mock_morpc.NewMockStream(ctrl)
					reg := process.NewPipelineEdge(2, 0)
					var root vm.Operator
					var producer vm.Operator
					switch kind {
					case remoteS3MultiUpdate:
						producer = &multi_update.MultiUpdate{Action: multi_update.UpdateWriteS3}
					case remoteS3Insert:
						producer = &insert.Insert{ToWriteS3: true}
					case remoteS3Delete:
						producer = &deletion.Deletion{RemoteDelete: true}
					}
					root = producer
					if path == "connector" {
						op := connector.NewArgument()
						op.Reg = reg
						op.AppendChild(producer)
						root = op
						defer op.Release()
					} else if path == "dispatch" {
						op := dispatch.NewArgument()
						op.FuncId = dispatch.SendToAllLocalFunc
						op.LocalRegs = []*process.WaitRegister{reg}
						op.AppendChild(producer)
						root = op
						defer func() { op.Reset(proc, true, nil); op.Free(proc, true, nil); op.Release() }()
					}
					if path != "no-output" && !failTakeover {
						stream.EXPECT().ID().Return(uint64(7))
						stream.EXPECT().Send(gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, msg morpc.Message) error {
							require.Equal(t, pipeline.Method_PipelineBatchAck, msg.(*pipeline.Message).Cmd)
							require.True(t, msg.(*pipeline.Message).GetBatchAckS3OwnershipRetained())
							require.Len(t, workspace.owners, 1)
							require.Equal(t, []string{name}, workspace.owners[0].Names())
							require.Len(t, reg.Ch2, 1, "forward precedes ACK")
							signal := <-reg.Ch2
							forwarded, err := signal.Action()
							require.NoError(t, err)
							require.Equal(t, 1, forwarded.RowCount())
							if path == "connector" {
								forwarded.Clean(proc.Mp())
							}
							return nil
						})
					}
					sender := &messageSenderOnClient{ctx: proc.Ctx, mp: proc.Mp(), receiveCh: responses, streamSender: stream}
					scope := &Scope{Proc: proc, RootOp: root}
					switch path {
					case "connector":
						err = receiveMessageFromCnServerIfConnector(scope, sender)
					case "dispatch":
						err = receiveMessageFromCnServerIfDispatch(scope, sender)
					default:
						err = receiveMessageFromCnServerIfOnlyRun(scope, sender)
					}
					if path == "no-output" {
						require.ErrorContains(t, err, "no-output stream")
						require.Empty(t, workspace.owners)
						require.Equal(t, uint64(1), sender.pendingBatchAck)
					} else if failTakeover {
						require.ErrorContains(t, err, "cannot retain")
						require.Empty(t, reg.Ch2)
						require.Equal(t, uint64(1), sender.pendingBatchAck)
					} else {
						require.True(t, moerr.IsMoErrCode(err, moerr.ErrStreamClosed))
						require.Zero(t, sender.pendingBatchAck)
						workspace.owners[0].Accept(name)
						require.False(t, workspace.owners[0].Pending())
					}
				})
			}
		}
	}
}

type testPipelineClient struct {
	genStream func(context.Context, string) (morpc.Stream, error)
}

func (tPCli *testPipelineClient) NewStream(ctx context.Context, backend string) (morpc.Stream, error) {
	return tPCli.genStream(ctx, backend)
}

func (tPCli *testPipelineClient) Raw() morpc.RPCClient {
	//TODO implement me
	panic("implement me")
}

func (tPCli *testPipelineClient) Close() error {
	//TODO implement me
	panic("implement me")
}

func TestNewMessageSenderOnClientCleansUpStreamOnReceiveError(t *testing.T) {
	sid := t.Name()
	runtime.SetupServiceBasedRuntime(sid, runtime.DefaultRuntime())

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	tPCli := &testPipelineClient{
		genStream: func(ctx context.Context, s string) (morpc.Stream, error) {
			stream := mock_morpc.NewMockStream(ctrl)
			stream.EXPECT().Receive().Return(nil, moerr.NewInternalErrorNoCtx("return error")).AnyTimes()
			stream.EXPECT().Close(true).Return(nil)
			return stream, nil
		},
	}

	runtime.ServiceRuntime(sid).SetGlobalVariables(runtime.PipelineClient, tPCli)

	client, err := newMessageSenderOnClient(
		context.Background(),
		sid,
		"addr",
		mpool.MustNewZero(),
		nil,
	)
	assert.Error(t, err)
	assert.Nil(t, client)
}

func TestNewMessageSenderOnClientSetsDeadlineBeforeNewStream(t *testing.T) {
	sid := t.Name()
	runtime.SetupServiceBasedRuntime(sid, runtime.DefaultRuntime())

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	tPCli := &testPipelineClient{
		genStream: func(ctx context.Context, backend string) (morpc.Stream, error) {
			_, ok := ctx.Deadline()
			require.True(t, ok)
			require.Equal(t, "addr", backend)

			stream := mock_morpc.NewMockStream(ctrl)
			stream.EXPECT().Receive().Return(make(chan morpc.Message), nil)
			stream.EXPECT().Close(true).Return(nil)
			return stream, nil
		},
	}
	runtime.ServiceRuntime(sid).SetGlobalVariables(runtime.PipelineClient, tPCli)

	client, err := newMessageSenderOnClient(
		context.Background(),
		sid,
		"addr",
		mpool.MustNewZero(),
		nil,
	)
	require.NoError(t, err)
	require.NotNil(t, client)
	require.True(t, client.useInternalTimeout)
	require.NotNil(t, client.ctxCancel)

	client.close()
}

// TestNewMessageSenderOnClientPropagatesBackendCreateTimeout verifies the
// statement boundary used by RemoteRun. A stale fixed endpoint must surface
// the typed MORPC terminal error without canceling the caller's longer query
// context or constructing a sender that would require stream cleanup.
func TestNewMessageSenderOnClientPropagatesBackendCreateTimeout(t *testing.T) {
	sid := t.Name()
	runtime.SetupServiceBasedRuntime(sid, runtime.DefaultRuntime())

	var calls atomic.Int32
	tPCli := &testPipelineClient{
		genStream: func(ctx context.Context, backend string) (morpc.Stream, error) {
			calls.Add(1)
			require.Equal(t, "stale-cn:6002", backend)
			return nil, morpc.ErrBackendCreateTimeout
		},
	}
	runtime.ServiceRuntime(sid).SetGlobalVariables(runtime.PipelineClient, tPCli)

	queryCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	sender, err := newMessageSenderOnClient(
		queryCtx,
		sid,
		"stale-cn:6002",
		mpool.MustNewZero(),
		nil,
	)
	require.Nil(t, sender)
	require.ErrorIs(t, err, morpc.ErrBackendCreateTimeout)
	require.EqualValues(t, 1, calls.Load())
	require.NoError(t, context.Cause(queryCtx),
		"RemoteRun stream creation canceled the owning statement context")
}

func TestNewMessageSenderOnClientReturnsErrorWithoutPipelineClient(t *testing.T) {
	sid := t.Name()
	runtime.SetupServiceBasedRuntime(sid, runtime.DefaultRuntime())

	client, err := newMessageSenderOnClient(
		context.Background(),
		sid,
		"addr",
		mpool.MustNewZero(),
		nil,
	)
	require.Error(t, err)
	require.Nil(t, client)
	require.Contains(t, err.Error(), "pipeline client is not initialized")
}

func TestNewMessageSenderOnClientReturnsErrorWithoutServiceRuntime(t *testing.T) {
	client, err := newMessageSenderOnClient(
		context.Background(),
		t.Name(),
		"addr",
		mpool.MustNewZero(),
		nil,
	)
	require.Error(t, err)
	require.Nil(t, client)
	require.Contains(t, err.Error(), "service runtime is not initialized")
}

func TestPipelineStreamReuseRuntimeGate(t *testing.T) {
	sid := t.Name()
	runtime.SetupServiceBasedRuntime(sid, runtime.DefaultRuntime())
	require.True(t, pipelineStreamReuseEnabled(sid))
	runtime.ServiceRuntime(sid).SetGlobalVariables(runtime.EnablePipelineStreamReuse, false)
	require.False(t, pipelineStreamReuseEnabled(sid))
	runtime.ServiceRuntime(sid).SetGlobalVariables(runtime.EnablePipelineStreamReuse, true)
	require.True(t, pipelineStreamReuseEnabled(sid))
}

func TestMessageSenderBatchCreditProtocol(t *testing.T) {
	ctrl := gomock.NewController(t)
	stream := mock_morpc.NewMockStream(ctrl)
	stream.EXPECT().ID().Return(uint64(17))
	stream.EXPECT().Send(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, request morpc.Message) error {
			message := request.(*pipeline.Message)
			require.Equal(t, pipeline.Method_PipelineBatchAck, message.GetCmd())
			require.Equal(t, uint64(9), message.GetBatchAckSequence())
			return nil
		})

	sender := &messageSenderOnClient{
		ctx:              context.Background(),
		streamSender:     stream,
		requestFinishAck: true,
		pendingBatchAck:  9,
	}
	request := &pipeline.Message{}
	sender.requestStreamProtocols(request)
	require.Equal(t, pipeline.StreamTeardownMode_FinishAck, request.GetRequestedTeardownMode())
	require.Equal(t, pipelineBatchCreditCount, request.GetRequestedBatchCreditCount())
	require.Equal(t, pipelineBatchCreditBytes, request.GetRequestedBatchCreditBytes())
	require.NoError(t, sender.acknowledgeRemoteBatch())
	require.Zero(t, sender.pendingBatchAck)
}

func TestNewMessageSenderOnClientReturnsErrorOnNilStream(t *testing.T) {
	sid := t.Name()
	runtime.SetupServiceBasedRuntime(sid, runtime.DefaultRuntime())

	tPCli := &testPipelineClient{
		genStream: func(ctx context.Context, backend string) (morpc.Stream, error) {
			return nil, nil
		},
	}
	runtime.ServiceRuntime(sid).SetGlobalVariables(runtime.PipelineClient, tPCli)

	client, err := newMessageSenderOnClient(
		context.Background(),
		sid,
		"addr",
		mpool.MustNewZero(),
		nil,
	)
	require.Error(t, err)
	require.Nil(t, client)
	require.Contains(t, err.Error(), "pipeline stream is not initialized")
}

func TestMessageSenderOnClientNegotiatedStreamTeardown(t *testing.T) {
	t.Run("negotiated retry terminal needs explicit cleanup authorization", func(t *testing.T) {
		sender := &messageSenderOnClient{expectedEnd: pipeline.Method_PrepareDoneNotifyMessage}
		message := &pipeline.Message{
			Cmd:                  pipeline.Method_PrepareDoneNotifyMessage,
			Sid:                  pipeline.Status_MessageEnd,
			AcceptedTeardownMode: pipeline.StreamTeardownMode_FinishAck,
		}
		sender.markTerminal(message, false)
		require.True(t, sender.terminalNegotiated)
		require.False(t, sender.reuseEligible)
		sender.prepareForLocalCleanup()
		require.True(t, sender.reuseEligible)
	})

	t.Run("accepted FIN ACK reuses backend", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		stream := mock_morpc.NewMockStream(ctrl)
		responses := make(chan morpc.Message, 1)
		responses <- &pipeline.Message{
			Id:                   7,
			Cmd:                  pipeline.Method_PipelineStreamFinishAck,
			Sid:                  pipeline.Status_MessageEnd,
			AcceptedTeardownMode: pipeline.StreamTeardownMode_FinishAck,
		}
		stream.EXPECT().ID().Return(uint64(7))
		stream.EXPECT().Send(gomock.Any(), gomock.Any()).DoAndReturn(
			func(_ context.Context, request morpc.Message) error {
				message := request.(*pipeline.Message)
				require.Equal(t, pipeline.Method_PipelineStreamFinish, message.GetCmd())
				require.Equal(t, pipeline.Status_Last, message.GetSid())
				return nil
			})
		stream.EXPECT().Close(false).Return(nil)
		sender := &messageSenderOnClient{
			ctx:           context.Background(),
			streamSender:  stream,
			receiveCh:     responses,
			safeToClose:   true,
			reuseEligible: true,
		}
		sender.close()
		sender.close()
	})

	t.Run("legacy End closes backend", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		stream := mock_morpc.NewMockStream(ctrl)
		stream.EXPECT().Close(true).Return(nil)
		sender := &messageSenderOnClient{
			ctx:          context.Background(),
			streamSender: stream,
			safeToClose:  true,
		}
		sender.close()
	})

	for _, tt := range []struct {
		name         string
		closeChannel bool
	}{
		{name: "closed receive channel releases stream ownership", closeChannel: true},
		{name: "nil receive message releases stream ownership"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			stream := mock_morpc.NewMockStream(ctrl)
			responses := make(chan morpc.Message, 1)
			if tt.closeChannel {
				close(responses)
			} else {
				responses <- nil
			}
			stream.EXPECT().Close(true).Return(nil).Times(1)
			sender := &messageSenderOnClient{
				ctx:           context.Background(),
				streamSender:  stream,
				receiveCh:     responses,
				reuseEligible: true,
			}

			message, err := sender.receiveMessage()
			require.Nil(t, message)
			require.Error(t, err)
			require.True(t, moerr.IsMoErrCode(err, moerr.ErrStreamClosed))
			require.True(t, sender.receiveClosed)
			require.False(t, sender.reuseEligible)

			sender.close()
			sender.close()
		})
	}

	t.Run("peer close while waiting for FIN ACK poisons backend", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		stream := mock_morpc.NewMockStream(ctrl)
		responses := make(chan morpc.Message)
		close(responses)
		stream.EXPECT().ID().Return(uint64(13))
		stream.EXPECT().Send(gomock.Any(), gomock.Any()).Return(nil)
		stream.EXPECT().Close(true).Return(nil)
		sender := &messageSenderOnClient{
			ctx:           context.Background(),
			streamSender:  stream,
			receiveCh:     responses,
			safeToClose:   true,
			reuseEligible: true,
		}

		sender.close()
		require.True(t, sender.receiveClosed)
		require.False(t, sender.reuseEligible)
	})

	t.Run("malformed FIN ACK poisons backend", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		stream := mock_morpc.NewMockStream(ctrl)
		responses := make(chan morpc.Message, 1)
		responses <- &pipeline.Message{Id: 9, Cmd: pipeline.Method_PipelineStreamFinishAck, Sid: pipeline.Status_MessageEnd}
		stream.EXPECT().ID().Return(uint64(9))
		stream.EXPECT().Send(gomock.Any(), gomock.Any()).Return(nil)
		stream.EXPECT().Close(true).Return(nil)
		sender := &messageSenderOnClient{
			ctx:           context.Background(),
			streamSender:  stream,
			receiveCh:     responses,
			safeToClose:   true,
			reuseEligible: true,
		}
		sender.close()
	})

	t.Run("query cancellation after End poisons backend", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		stream := mock_morpc.NewMockStream(ctrl)
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		stream.EXPECT().Close(true).Return(nil)
		sender := &messageSenderOnClient{
			ctx:           ctx,
			streamSender:  stream,
			receiveCh:     make(chan morpc.Message),
			safeToClose:   true,
			reuseEligible: true,
		}
		sender.close()
	})

	t.Run("successful local cleanup cancellation still reuses backend", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		stream := mock_morpc.NewMockStream(ctrl)
		ctx, cancel := context.WithCancel(context.Background())
		responses := make(chan morpc.Message, 1)
		responses <- &pipeline.Message{
			Id:                   11,
			Cmd:                  pipeline.Method_PipelineStreamFinishAck,
			Sid:                  pipeline.Status_MessageEnd,
			AcceptedTeardownMode: pipeline.StreamTeardownMode_FinishAck,
		}
		stream.EXPECT().ID().Return(uint64(11))
		stream.EXPECT().Send(gomock.Any(), gomock.Any()).Return(nil)
		stream.EXPECT().Close(false).Return(nil)
		sender := &messageSenderOnClient{
			ctx:           ctx,
			streamSender:  stream,
			receiveCh:     responses,
			safeToClose:   true,
			reuseEligible: true,
		}
		sender.prepareForLocalCleanup()
		cancel()
		sender.close()
	})

	t.Run("cancellation before cleanup completion poisons backend", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		stream := mock_morpc.NewMockStream(ctrl)
		pipelineCtx, cancelPipeline := context.WithCancel(context.Background())
		stream.EXPECT().Close(true).Return(nil)
		sender := &messageSenderOnClient{
			ctx:           pipelineCtx,
			streamSender:  stream,
			receiveCh:     make(chan morpc.Message),
			safeToClose:   true,
			reuseEligible: true,
		}
		cancelPipeline()
		sender.close()
	})

	t.Run("FIN ACK timeout poisons backend", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		stream := mock_morpc.NewMockStream(ctrl)
		stream.EXPECT().ID().Return(uint64(10))
		stream.EXPECT().Send(gomock.Any(), gomock.Any()).Return(nil)
		stream.EXPECT().Close(true).Return(nil)
		oldTimeout := pipelineStreamFinishClientTimeout
		pipelineStreamFinishClientTimeout = 10 * time.Millisecond
		t.Cleanup(func() { pipelineStreamFinishClientTimeout = oldTimeout })
		sender := &messageSenderOnClient{
			ctx:           context.Background(),
			streamSender:  stream,
			receiveCh:     make(chan morpc.Message),
			safeToClose:   true,
			reuseEligible: true,
		}
		sender.close()
	})

	t.Run("clean StopSending End reuses backend", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		stream := mock_morpc.NewMockStream(ctrl)
		responses := make(chan morpc.Message, 2)
		responses <- &pipeline.Message{
			Id:                   12,
			Cmd:                  pipeline.Method_PipelineMessage,
			Sid:                  pipeline.Status_MessageEnd,
			AcceptedTeardownMode: pipeline.StreamTeardownMode_FinishAck,
		}
		responses <- &pipeline.Message{
			Id:                   12,
			Cmd:                  pipeline.Method_PipelineStreamFinishAck,
			Sid:                  pipeline.Status_MessageEnd,
			AcceptedTeardownMode: pipeline.StreamTeardownMode_FinishAck,
		}
		stream.EXPECT().ID().Return(uint64(12)).Times(2)
		gomock.InOrder(
			stream.EXPECT().Send(gomock.Any(), gomock.Any()).DoAndReturn(
				func(_ context.Context, request morpc.Message) error {
					require.Equal(t, pipeline.Method_StopSending, request.(*pipeline.Message).GetCmd())
					return nil
				}),
			stream.EXPECT().Send(gomock.Any(), gomock.Any()).DoAndReturn(
				func(_ context.Context, request morpc.Message) error {
					require.Equal(t, pipeline.Method_PipelineStreamFinish, request.(*pipeline.Message).GetCmd())
					return nil
				}),
		)
		stream.EXPECT().Close(false).Return(nil)
		sender := &messageSenderOnClient{
			ctx:                      context.Background(),
			streamSender:             stream,
			receiveCh:                responses,
			safeToClose:              false,
			expectedEnd:              pipeline.Method_PipelineMessage,
			allowCleanupCancellation: true,
		}
		sender.close()
	})
}

func TestRemoteRun(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	ctx = defines.AttachAccountId(ctx, catalog.System_Account)
	catalog.SetupDefines("")

	proc := testutil.NewProcess(t)
	proc.Ctx = context.WithValue(proc.Ctx, defines.TenantIDKey{}, uint32(0))

	tPCli := &testPipelineClient{
		genStream: func(ctx context.Context, s string) (morpc.Stream, error) {
			stream := mock_morpc.NewMockStream(ctrl)
			stream.EXPECT().Receive().Return(nil, nil).AnyTimes()
			stream.EXPECT().ID().Return(uint64(3)).AnyTimes()
			stream.EXPECT().Send(gomock.Any(), gomock.Any()).Return(moerr.NewInternalErrorNoCtx("send error")).AnyTimes()
			return stream, nil
		},
	}

	runtime.ServiceRuntime("").SetGlobalVariables(runtime.PipelineClient, tPCli)

	fault.Enable()
	fault.AddFaultPoint(ctx, "inject_send_pipeline", ":::", "echo", 0, "test_tbl", false)

	txnCli, txnOp := newTestTxnClientAndOp(ctrl)
	proc.Base.TxnClient = txnCli
	proc.Base.TxnOperator = txnOp

	sql := "insert into test_tbl values (1,1)"
	c := NewCompile("test", "test", sql, "", "", newStubEngine(), proc, nil, false, nil, time.Now())
	c.anal = &AnalyzeModule{qry: &plan.Query{}}

	// if the root operator is connector.
	s1 := &Scope{
		Proc:          proc,
		RootOp:        connector.NewArgument(),
		ScopeAnalyzer: &ScopeAnalyzer{isStoped: true},
	}
	s1.RootOp.(*connector.Connector).Reg = &process.WaitRegister{
		Ch2: make(chan process.PipelineSignal, 1),
	}
	// ch, err1 := sender.streamSender.Receive()
	// require.Nil(t, err1)
	// sender.receiveCh = ch

	_, err := s1.remoteRun(c)
	assert.Error(t, err)
}

func TestRemoteRunNormalizesPipelineCancellationCause(t *testing.T) {
	oldRuntime := runtime.ServiceRuntime("")
	testRuntime := runtime.DefaultRuntime()
	runtime.SetupServiceBasedRuntime("", testRuntime)
	t.Cleanup(func() {
		runtime.SetupServiceBasedRuntime("", oldRuntime)
	})
	catalog.SetupDefines("")

	duplicateErr := moerr.NewDuplicateEntryNoCtx("1", "primary")
	tests := []struct {
		name                       string
		cancelCause                error
		cancelQuery                bool
		deadlineQuery              bool
		remoteErr                  error
		stopResponseErr            error
		stopSendErr                error
		closeStopResponse          bool
		timeoutStopResponse        bool
		assertTerminalBeforeCancel bool
		wantErr                    error
		wantErrCode                uint16
		wantStopSendingCount       int
	}{
		{
			name:                 "substantive cancellation cause survives",
			cancelCause:          duplicateErr,
			wantErr:              duplicateErr,
			wantStopSendingCount: 1,
		},
		{
			name:                 "substantive cancellation cause survives StopSending send failure",
			cancelCause:          duplicateErr,
			stopSendErr:          moerr.NewBackendClosedNoCtx(),
			wantErr:              duplicateErr,
			wantStopSendingCount: 1,
		},
		{
			name:                 "substantive cancellation cause survives StopSending response closure",
			cancelCause:          duplicateErr,
			closeStopResponse:    true,
			wantErr:              duplicateErr,
			wantStopSendingCount: 1,
		},
		{
			name:                 "substantive cancellation cause survives StopSending timeout",
			cancelCause:          duplicateErr,
			timeoutStopResponse:  true,
			wantErr:              duplicateErr,
			wantStopSendingCount: 1,
		},
		{
			name:                 "normal internal cancellation remains secondary",
			wantStopSendingCount: 1,
		},
		{
			name:                 "query cancellation remains terminal",
			cancelQuery:          true,
			wantErr:              context.Canceled,
			wantStopSendingCount: 1,
		},
		{
			name:                 "query cancellation survives StopSending send failure",
			cancelQuery:          true,
			stopSendErr:          moerr.NewBackendClosedNoCtx(),
			wantErr:              context.Canceled,
			wantStopSendingCount: 1,
		},
		{
			name:                 "query cancellation survives StopSending response closure",
			cancelQuery:          true,
			closeStopResponse:    true,
			wantErr:              context.Canceled,
			wantStopSendingCount: 1,
		},
		{
			name:                 "query cancellation survives StopSending timeout",
			cancelQuery:          true,
			timeoutStopResponse:  true,
			wantErr:              context.Canceled,
			wantStopSendingCount: 1,
		},
		{
			name:                 "query deadline survives StopSending send failure",
			deadlineQuery:        true,
			stopSendErr:          moerr.NewBackendClosedNoCtx(),
			wantErr:              context.DeadlineExceeded,
			wantStopSendingCount: 1,
		},
		{
			name:                 "query deadline survives StopSending response closure",
			deadlineQuery:        true,
			closeStopResponse:    true,
			wantErr:              context.DeadlineExceeded,
			wantStopSendingCount: 1,
		},
		{
			name:                 "query deadline survives StopSending timeout",
			deadlineQuery:        true,
			timeoutStopResponse:  true,
			wantErr:              context.DeadlineExceeded,
			wantStopSendingCount: 1,
		},
		{
			name:                       "remote failure reaches receiver before scope cancellation",
			remoteErr:                  duplicateErr,
			assertTerminalBeforeCancel: true,
			wantErr:                    duplicateErr,
			wantErrCode:                moerr.ErrDuplicateEntry,
		},
		{
			name:                 "remote failure returned after internal cancellation survives",
			stopResponseErr:      duplicateErr,
			wantErr:              duplicateErr,
			wantErrCode:          moerr.ErrDuplicateEntry,
			wantStopSendingCount: 1,
		},
		{
			name:                 "remote cancellation returned after internal cancellation remains secondary",
			stopResponseErr:      moerr.NewQueryInterrupted(context.Background()),
			wantStopSendingCount: 1,
		},
		{
			name:                 "StopSending send failure is terminal",
			stopSendErr:          moerr.NewBackendClosedNoCtx(),
			wantErrCode:          moerr.ErrBackendClosed,
			wantStopSendingCount: 1,
		},
		{
			name:                 "canceled StopSending send is a closed stream",
			stopSendErr:          context.Canceled,
			wantErrCode:          moerr.ErrStreamClosed,
			wantStopSendingCount: 1,
		},
		{
			name:                 "StopSending response channel closure is terminal",
			closeStopResponse:    true,
			wantErrCode:          moerr.ErrStreamClosed,
			wantStopSendingCount: 1,
		},
		{
			name:                 "StopSending timeout is terminal and attempted once",
			timeoutStopResponse:  true,
			wantErrCode:          moerr.ErrRPCTimeout,
			wantStopSendingCount: 1,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.timeoutStopResponse {
				oldTimeout := pipelineStopSendingClientTimeout
				pipelineStopSendingClientTimeout = 10 * time.Millisecond
				defer func() { pipelineStopSendingClientTimeout = oldTimeout }()
			}
			ctrl := gomock.NewController(t)
			proc := testutil.NewProcess(t)
			queryParent := proc.GetTopContext()
			if tt.deadlineQuery {
				var cancelDeadline context.CancelFunc
				queryParent, cancelDeadline = context.WithDeadline(queryParent, time.Now().Add(-time.Second))
				t.Cleanup(cancelDeadline)
			}
			queryCtx := proc.Base.GetContextBase().BuildQueryCtx(queryParent)
			_, cancelQuery := process.GetQueryCtxFromProc(proc)
			t.Cleanup(cancelQuery)
			proc.BuildPipelineContext(queryCtx)
			txnCli, txnOp := newTestTxnClientAndOp(ctrl)
			proc.Base.TxnClient = txnCli
			proc.Base.TxnOperator = txnOp

			responses := make(chan morpc.Message, 1)
			stream := mock_morpc.NewMockStream(ctrl)
			stream.EXPECT().Receive().Return(responses, nil)
			stream.EXPECT().ID().Return(uint64(3)).AnyTimes()
			stopSendingCount := 0
			stream.EXPECT().Send(gomock.Any(), gomock.Any()).DoAndReturn(
				func(_ context.Context, request morpc.Message) error {
					message := request.(*pipeline.Message)
					switch message.GetCmd() {
					case pipeline.Method_PipelineMessage:
						if tt.remoteErr != nil {
							response := &pipeline.Message{Sid: pipeline.Status_MessageEnd}
							response.SetMessageType(pipeline.Method_PipelineMessage)
							response.SetMoError(context.Background(), tt.remoteErr)
							responses <- response
						} else if tt.cancelQuery {
							cancelQuery()
						} else {
							proc.Cancel(tt.cancelCause)
						}
					case pipeline.Method_StopSending:
						stopSendingCount++
						if tt.stopSendErr != nil {
							return tt.stopSendErr
						}
						if tt.closeStopResponse {
							close(responses)
							return nil
						}
						if tt.timeoutStopResponse {
							return nil
						}
						response := &pipeline.Message{Sid: pipeline.Status_MessageEnd}
						response.SetMessageType(pipeline.Method_PipelineMessage)
						if tt.stopResponseErr != nil {
							response.SetMoError(context.Background(), tt.stopResponseErr)
						}
						responses <- response
					}
					return nil
				}).AnyTimes()
			stream.EXPECT().Close(true).Return(nil)
			testRuntime.SetGlobalVariables(runtime.PipelineClient, &testPipelineClient{
				genStream: func(context.Context, string) (morpc.Stream, error) {
					return stream, nil
				},
			})

			c := NewCompile(
				"local-cn:6002",
				"test",
				"insert into test_tbl values (1, 1)",
				"",
				"",
				newStubEngine(),
				proc,
				nil,
				false,
				nil,
				time.Now(),
			)
			c.anal = &AnalyzeModule{qry: &plan.Query{}}

			reg := process.NewPipelineEdge(1, 0)
			root := connector.NewArgument().WithReg(reg)
			defer root.Release()
			if tt.assertTerminalBeforeCancel {
				originalCancel := proc.Cancel
				proc.Cancel = func(cause error) {
					require.True(t, moerr.IsMoErrCode(reg.Err(), moerr.ErrDuplicateEntry),
						"remote root terminal must be published before its scope is canceled")
					originalCancel(cause)
				}
			}
			s := &Scope{
				Magic:         Remote,
				Proc:          proc,
				RootOp:        root,
				ScopeAnalyzer: &ScopeAnalyzer{},
				NodeInfo:      engine.Node{Addr: "remote-cn:6002", Mcpu: 1},
			}

			err := s.RemoteRun(c)
			if tt.wantErrCode != 0 {
				require.True(t, moerr.IsMoErrCode(err, tt.wantErrCode), err)
			} else if tt.wantErr == nil {
				require.NoError(t, err)
			} else {
				require.ErrorIs(t, err, tt.wantErr)
			}
			require.Equal(t, tt.wantStopSendingCount, stopSendingCount)

			select {
			case signal := <-reg.Ch2:
				_, terminalErr := signal.Action()
				if tt.wantErrCode == 0 && tt.wantErr == nil {
					require.Equal(t, process.EventEnd, signal.EventType)
					require.NoError(t, terminalErr)
				} else {
					require.Equal(t, process.EventError, signal.EventType)
					if tt.wantErrCode != 0 {
						require.True(t, moerr.IsMoErrCode(terminalErr, tt.wantErrCode), terminalErr)
					} else {
						require.ErrorIs(t, terminalErr, tt.wantErr)
					}
				}
			case <-time.After(time.Second):
				t.Fatal("remote cleanup did not terminate its receiver")
			}
		})
	}
}

func TestRemoteRunFailureReleasesPendingRetainedDispatchAttach(t *testing.T) {
	oldRuntime := runtime.ServiceRuntime("")
	testRuntime := runtime.DefaultRuntime()
	runtime.SetupServiceBasedRuntime("", testRuntime)
	_ = colexec.NewServer("")
	t.Cleanup(func() {
		runtime.SetupServiceBasedRuntime("", oldRuntime)
	})

	runErr := moerr.NewInternalErrorNoCtx("injected new stream failure")
	var newStreamCalled atomic.Bool
	testRuntime.SetGlobalVariables(runtime.PipelineClient, &testPipelineClient{
		genStream: func(context.Context, string) (morpc.Stream, error) {
			newStreamCalled.Store(true)
			return nil, runErr
		},
	})

	ctrl := gomock.NewController(t)
	catalog.SetupDefines("")
	proc := testutil.NewProcess(t)
	accountCtx := defines.AttachAccountId(context.Background(), catalog.System_Account)
	proc.ReplaceTopCtx(accountCtx)
	queryCtx := proc.Base.GetContextBase().BuildQueryCtx(accountCtx)
	proc.BuildPipelineContext(queryCtx)
	txnCli, txnOp := newTestTxnClientAndOp(ctrl)
	proc.Base.TxnClient = txnCli
	proc.Base.TxnOperator = txnOp

	c := NewCompile("local-cn:6002", "test", "select 1", "", "", newStubEngine(), proc, nil, false, nil, time.Now())
	c.anal = &AnalyzeModule{qry: &plan.Query{}}

	uid := uuid.Must(uuid.NewV7())
	child := value_scan.NewArgument()
	defer child.Release()
	root := dispatch.NewArgument()
	defer root.Release()
	root.FuncId = dispatch.SendToAllFunc
	root.RemoteRegs = []colexec.ReceiveInfo{{Uuid: uid}}
	root.AppendChild(child)
	s := &Scope{
		Magic:    Remote,
		Proc:     proc,
		RootOp:   root,
		NodeInfo: engine.Node{Addr: "remote-cn:6002", Mcpu: 1},
	}

	registrations, err := registerLocalDispatchReceivers([]*Scope{s}, c.addr)
	require.NoError(t, err)
	defer registrations.cleanup()
	registeredProc, notifyCh, err := (&messageReceiverOnServer{
		colexecServer: colexec.GetServer(""),
		connectionCtx: context.Background(),
		messageCtx:    context.Background(),
	}).TryGetProcByUuid(uid)
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
		t.Fatalf("pending remote notify completed before RemoteRun failed: %s", result)
	default:
	}

	start := time.Now()
	err = s.RemoteRun(c)
	require.Less(t, time.Since(start), time.Second)
	require.True(t, newStreamCalled.Load(), "test must reach the injected NewStream failure")
	require.ErrorIs(t, err, runErr)
	require.ErrorIs(t, context.Cause(proc.Ctx), runErr)
	select {
	case result := <-pendingDone:
		require.Equal(t, "canceled", result)
	case <-time.After(time.Second):
		t.Fatal("RemoteRun failure did not release the pending retained-root attach")
	}
	registrations.cleanup()
	registeredProc, notifyCh, ok := colexec.GetServer("").GetProcByUuid(uid, false)
	require.False(t, ok)
	require.Nil(t, registeredProc)
	require.Nil(t, notifyCh)
}

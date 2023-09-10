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
	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/common/morpc"
	"github.com/matrixorigin/matrixone/pkg/common/morpc/examples/message"
	"github.com/matrixorigin/matrixone/pkg/common/morpc/mock_morpc"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	"github.com/matrixorigin/matrixone/pkg/pb/pipeline"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func Test_MessageSenderOnClient(t *testing.T) {
	server, err := morpc.NewRPCServer("server", "127.0.0.1:9999", morpc.NewMessageCodec(func() morpc.Message {
		return &message.ExampleMessage{}
	}))
	err = server.Start()
	require.Nil(t, err)

	sender, err := newMessageSenderOnClient(context.TODO(), "127.0.0.1:9999")
	require.Nil(t, err)
	defer sender.close()

	err = server.Close()
	require.Nil(t, err)

	// server has been closed
	_ = sender.send(nil, nil, 0)

	_ = sender.send(make([]byte, maxMessageSizeToMoRpc+1), nil, 0)
}

func Test_MessageReceiverOnServer(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctx := context.Background()

	ti, _ := time.Now().MarshalBinary()
	procInfo := &pipeline.ProcessInfo{
		Lim:         &pipeline.ProcessLimitation{Size: 1},
		SessionInfo: &pipeline.SessionInfo{TimeZone: ti},
	}
	procInfoData, err := procInfo.Marshal()
	require.Nil(t, err)

	id, _ := uuid.NewUUID()
	pipe := &pipeline.Pipeline{
		UuidsToRegIdx: []*pipeline.UuidToRegIdx{
			{Idx: 1, Uuid: id[:]},
		},
		InstructionList: []*pipeline.Instruction{
			{Op: int32(vm.Insert), Insert: &pipeline.Insert{}},
		},
		DataSource: &pipeline.Source{
			Timestamp: &timestamp.Timestamp{},
		},
	}
	pipeData, err := pipe.Marshal()
	require.Nil(t, err)

	msg := &pipeline.Message{
		Cmd:          pipeline.PipelineMessage,
		Uuid:         id[:],
		ProcInfoData: procInfoData,
		Data:         pipeData,
	}

	txnOperator := mock_frontend.NewMockTxnOperator(ctrl)
	cli := mock_frontend.NewMockTxnClient(ctrl)
	cli.EXPECT().NewWithSnapshot([]byte("")).Return(txnOperator, nil)

	cs := mock_morpc.NewMockClientSession(ctrl)
	cs.EXPECT().Write(gomock.Any(), gomock.Any()).Return(nil).Times(5)

	receiver := newMessageReceiverOnServer(
		ctx,
		"",
		msg,
		cs,
		func() morpc.Message { return msg },
		nil,
		nil,
		nil,
		nil,
		nil,
		cli,
		nil,
	)
	receiver.finalAnalysisInfo = []*process.AnalyzeInfo{{}}

	_, err = receiver.acquireMessage()
	require.Nil(t, err)

	err = receiver.sendError(nil)
	require.Nil(t, err)

	err = receiver.sendEndMessage()
	require.Nil(t, err)

	err = receiver.sendBatch(&batch.Batch{})
	require.Nil(t, err)

	data, _ := types.Encode(&batch.Batch{})
	receiver.maxMessageSize = len(data) / 2
	err = receiver.sendBatch(&batch.Batch{})
	require.Nil(t, err)
}

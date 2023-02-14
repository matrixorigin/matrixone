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
	"context"
	"github.com/matrixorigin/matrixone/pkg/cnservice/cnclient"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/morpc"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/pipeline"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"hash/crc32"
	"time"
)

type messageSenderOnClient struct {
	ctx       context.Context
	ctxCancel context.CancelFunc

	streamSender morpc.Stream
	receiveCh    chan morpc.Message
}

func newMessageSenderOnClient(
	ctx context.Context, toAddr string) (messageSenderOnClient, error) {
	var sender = messageSenderOnClient{}

	streamSender, err := cnclient.GetStreamSender(toAddr)
	if err != nil {
		return sender, nil
	}

	sender.streamSender = streamSender
	if _, ok := ctx.Deadline(); !ok {
		sender.ctx, sender.ctxCancel = context.WithTimeout(ctx, time.Second*10000)
	} else {
		sender.ctx = ctx
	}
	return sender, nil
}

// XXX we can set a scope as argument directly next day.
func (sender *messageSenderOnClient) send(
	scopeData, procData []byte, messageType uint64) error {
	message := cnclient.AcquireMessage()
	message.SetID(sender.streamSender.ID())
	message.SetMessageType(messageType)
	message.SetData(scopeData)
	message.SetProcData(procData)
	return sender.streamSender.Send(sender.ctx, message)
}

func (sender *messageSenderOnClient) receive() (morpc.Message, error) {
	var err error
	if sender.receiveCh == nil {
		sender.receiveCh, err = sender.streamSender.Receive()
		if err != nil {
			return nil, err
		}
	}

	select {
	case <-sender.ctx.Done():
		return nil, moerr.NewRPCTimeout(sender.ctx)
	case val, ok := <-sender.receiveCh:
		if !ok || val == nil {
			// ch close
			return nil, moerr.NewStreamClosed(sender.ctx)
		}
		return val, nil
	}
}

func (sender *messageSenderOnClient) close() {
	sender.ctxCancel()
	// XXX not a good way to deal it if close failed.
	_ = sender.streamSender.Close()
}

type messageReceiverOnServer struct {
	ctx       context.Context
	messageId uint64

	clientSession   morpc.ClientSession
	messageAcquirer func() morpc.Message
	maxMessageSize  int

	// XXX what's that. So confused.
	sequence uint64
}

func newMessageReceiverOnServer(
	ctx context.Context,
	cs morpc.ClientSession, messageAcquirer func() morpc.Message,
	messageId uint64) messageReceiverOnServer {
	return messageReceiverOnServer{
		ctx:             ctx,
		messageId:       messageId,
		clientSession:   cs,
		messageAcquirer: messageAcquirer,
		maxMessageSize:  64 * mpool.MB,
		sequence:        0,
	}
}

func (receiver *messageReceiverOnServer) acquireMessage() (*pipeline.Message, error) {
	message, ok := receiver.messageAcquirer().(*pipeline.Message)
	if !ok {
		return nil, moerr.NewInternalError(receiver.ctx, "get a message with wrong type.")
	}
	message.SetID(receiver.messageId)
	return message, nil
}

func (receiver *messageReceiverOnServer) sendError(
	errInfo error) error {
	message, err := receiver.acquireMessage()
	if err != nil {
		return err
	}
	message.SetID(receiver.messageId)
	message.SetSid(pipeline.MessageEnd)
	if errInfo != nil {
		message.SetMoError(receiver.ctx, errInfo)
	}
	return receiver.clientSession.Write(receiver.ctx, message)
}

func (receiver *messageReceiverOnServer) sendBatch(
	b *batch.Batch) error {
	if b == nil {
		return nil
	}
	data, err := types.Encode(b)
	if err != nil {
		return err
	}

	if len(data) <= receiver.maxMessageSize {
		m, errA := receiver.acquireMessage()
		if errA != nil {
			return errA
		}
		m.SetData(data)
		// XXX too bad.
		m.SetCheckSum(crc32.ChecksumIEEE(data))
		m.SetSequence(receiver.sequence)
		receiver.sequence++
		return receiver.clientSession.Write(receiver.ctx, m)
	}
	// if data is too large, cut and send
	for start, end := 0, 0; start < len(data); start = end {
		m, errA := receiver.acquireMessage()
		if errA != nil {
			return errA
		}
		end = start + receiver.maxMessageSize
		if end > len(data) {
			end = len(data)
			m.SetSid(pipeline.BatchEnd)
		} else {
			m.SetSid(pipeline.WaitingNext)
		}
		m.SetData(data[start:end])
		m.SetCheckSum(crc32.ChecksumIEEE(data))
		m.SetSequence(receiver.sequence)
		receiver.sequence++

		if errW := receiver.clientSession.Write(receiver.ctx, m); errW != nil {
			return errW
		}
	}
	return nil
}

func (receiver *messageReceiverOnServer) sendEndMessage(
	analysisInfo []*process.AnalyzeInfo, messageType uint64) error {
	message, err := receiver.acquireMessage()
	if err != nil {
		return err
	}
	message.SetSid(pipeline.MessageEnd)
	message.SetID(receiver.messageId)
	message.SetMessageType(messageType)
	if len(analysisInfo) > 0 {
		anas := &pipeline.AnalysisList{
			List: make([]*plan.AnalyzeInfo, len(analysisInfo)),
		}
		for i, a := range analysisInfo {
			anas.List[i] = convertToPlanAnalyzeInfo(a)
		}
		data, err := anas.Marshal()
		if err != nil {
			return err
		}
		message.SetAnalysis(data)
	}
	return receiver.clientSession.Write(receiver.ctx, message)
}

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
	"fmt"
	"sync/atomic"
	"time"

	"github.com/matrixorigin/matrixone/pkg/cnservice/cnclient"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/morpc"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/pb/pipeline"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/connector"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/dispatch"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/value_scan"
	v2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"go.uber.org/zap"
)

// MaxRpcTime is a default timeout time to rpc context if user never set this deadline.
// this is just a number I casually wrote, the purpose of doing this is that any message sent through rpc need a clear deadline.
const MaxRpcTime = time.Hour * 24

// remoteRun sends a scope to remote node for running.
// and keep receiving the back results.
//
// we assume that, result message is always *pipeline.Message, and there are 3 cases for that:
// first, Message with error information.
// second, Message with EndFlag and Analysis Information.
// third, Message with batch data.
func (s *Scope) remoteRun(c *Compile) (sender *messageSenderOnClient, err error) {
	// a defer for safety.
	defer func() {
		if e := recover(); e != nil {
			err = moerr.ConvertPanicError(s.Proc.Ctx, e)
			getLogger().Error("panic in scope remoteRun",
				zap.String("sql", c.sql),
				zap.String("error", err.Error()))
		}
	}()

	// encode structures which need to send.
	var scopeEncodeData, processEncodeData []byte
	scopeEncodeData, processEncodeData, err = prepareRemoteRunSendingData(c.sql, s)
	if err != nil {
		return nil, err
	}

	// generate a new sender to do send work.
	sender, err = newMessageSenderOnClient(s.Proc.Ctx, s.NodeInfo.Addr, s.Proc.Mp(), c.anal)
	if err != nil {
		c.proc.Errorf(s.Proc.Ctx, "Failed to newMessageSenderOnClient sql=%s, txnID=%s, err=%v",
			c.sql, c.proc.GetTxnOperator().Txn().DebugString(), err)

		return nil, err
	}

	if err = sender.sendPipeline(scopeEncodeData, processEncodeData); err != nil {
		return sender, err
	}

	sender.safeToClose = false
	sender.alreadyClose = false
	err = receiveMessageFromCnServer(c, s, sender)
	return sender, err
}

func prepareRemoteRunSendingData(sqlStr string, s *Scope) (scopeData []byte, processData []byte, err error) {
	// 1.
	// Encode the Scope related.
	// encode all operators in the scope except the last one.
	// because we need to keep it local for receiving and sending batch to next pipeline.
	rootOp := s.RootOp
	if rootOp.GetOperatorBase().NumChildren() == 0 {
		s.RootOp = nil
	} else {
		s.RootOp = s.RootOp.GetOperatorBase().GetChildren(0)
	}
	rootOp.GetOperatorBase().SetChildren(nil)
	defer func() {
		s.appendOperator(rootOp)
	}()

	if scopeData, err = encodeScope(s); err != nil {
		return nil, nil, err
	}

	// 2.
	// Encode the Process related information.
	if processData, err = encodeProcessInfo(s.Proc, sqlStr); err != nil {
		return nil, nil, err
	}

	return scopeData, processData, nil
}

func receiveMessageFromCnServer(c *Compile, s *Scope, sender *messageSenderOnClient) error {
	// generate a new pipeline to send data in local.
	// value_scan -> connector / dispatch -> next pipeline.
	fakeValueScanOperator := value_scan.NewArgument()
	if err := fakeValueScanOperator.Prepare(s.Proc); err != nil {
		return err
	}
	defer func() {
		fakeValueScanOperator.Release()
	}()

	LastOperator := s.RootOp
	lastAnalyze := c.proc.GetAnalyze(LastOperator.GetOperatorBase().GetIdx(), -1, false)
	switch arg := LastOperator.(type) {
	case *connector.Argument:
		oldChildren := arg.Children
		arg.Children = nil
		arg.AppendChild(fakeValueScanOperator)
		defer func() {
			arg.Children = oldChildren
		}()

	case *dispatch.Argument:
		oldChildren := arg.Children
		arg.Children = nil
		arg.AppendChild(fakeValueScanOperator)
		defer func() {
			arg.Children = oldChildren
		}()

	default:
		panic(
			fmt.Sprintf("remote run pipeline has an unexpected operator [id = %d] at last.", LastOperator.GetOperatorBase().Op))
	}

	// the last operator is responsible for distributing received data locally. Need Prepare here.
	if err := LastOperator.Prepare(s.Proc); err != nil {
		return err
	}

	// receive back result and send.
	var bat *batch.Batch
	var end bool
	var err error
	for {
		bat, end, err = sender.receiveBatch()
		if err != nil {
			return err
		}
		if end {
			return nil
		}

		lastAnalyze.Network(bat)
		fakeValueScanOperator.Batchs = append(fakeValueScanOperator.Batchs, bat)

		result, errCall := LastOperator.Call(s.Proc)
		if errCall != nil || result.Status == vm.ExecStop {
			return errCall
		}
	}
}

// messageSenderOnClient support a series of methods
// to do sending message and receiving its returns.
type messageSenderOnClient struct {
	// sender's context
	// and cancel function (it exists if this context was recreated by us).
	ctx       context.Context
	ctxCancel context.CancelFunc

	mp *mpool.MPool

	// anal was used to merge remote-run's cost analysis information.
	anal *anaylze

	// message sender and its data receiver.
	streamSender morpc.Stream
	receiveCh    chan morpc.Message

	// Two Flags to help us know the sender status.
	//
	// safeToClose should be true, if
	// 1. there has received the EndMessage or ErrorMessage from receiver.
	// or
	// 2. we have never sent a message in succeed.
	safeToClose bool
	// alreadyClose should be true once we get a stream closed signal.
	alreadyClose bool
}

func newMessageSenderOnClient(
	ctx context.Context, toAddr string, mp *mpool.MPool, ana *anaylze) (*messageSenderOnClient, error) {

	streamSender, err := cnclient.GetStreamSender(toAddr)
	if err != nil {
		return nil, err
	}

	sender := &messageSenderOnClient{
		safeToClose:  true,
		alreadyClose: false,
		mp:           mp,
		anal:         ana,
		streamSender: streamSender,
	}

	if _, ok := ctx.Deadline(); !ok {
		sender.ctx, sender.ctxCancel = context.WithTimeout(ctx, MaxRpcTime)
	} else {
		sender.ctx = ctx
	}

	if sender.receiveCh == nil {
		sender.receiveCh, err = sender.streamSender.Receive()
	}

	v2.PipelineMessageSenderCounter.Inc()
	return sender, err
}

func (sender *messageSenderOnClient) sendPipeline(
	scopeData, procData []byte) error {
	sdLen := len(scopeData)
	if sdLen <= maxMessageSizeToMoRpc {
		message := cnclient.AcquireMessage()
		message.SetID(sender.streamSender.ID())
		message.SetMessageType(pipeline.Method_PipelineMessage)
		message.SetData(scopeData)
		message.SetProcData(procData)
		message.SetSid(pipeline.Status_Last)
		message.NeedNotReply = false
		return sender.streamSender.Send(sender.ctx, message)
	}

	start := 0
	for start < sdLen {
		end := start + maxMessageSizeToMoRpc

		message := cnclient.AcquireMessage()
		message.SetID(sender.streamSender.ID())
		message.SetMessageType(pipeline.Method_PipelineMessage)
		if end >= sdLen {
			message.SetData(scopeData[start:sdLen])
			message.SetProcData(procData)
			message.SetSid(pipeline.Status_Last)
		} else {
			message.SetData(scopeData[start:end])
			message.SetSid(pipeline.Status_WaitingNext)
		}
		message.NeedNotReply = false

		if err := sender.streamSender.Send(sender.ctx, message); err != nil {
			return err
		}
		start = end
	}
	return nil
}

func (sender *messageSenderOnClient) receiveMessage() (morpc.Message, error) {
	select {
	case <-sender.ctx.Done():
		return nil, nil

	case val, ok := <-sender.receiveCh:
		if !ok || val == nil {
			sender.safeToClose = true
			sender.alreadyClose = true
			return nil, moerr.NewStreamClosed(sender.ctx)
		}
		return val, nil
	}
}

func (sender *messageSenderOnClient) receiveBatch() (bat *batch.Batch, over bool, err error) {
	var val morpc.Message
	var m *pipeline.Message
	var dataBuffer []byte

	for {
		val, err = sender.receiveMessage()
		if err != nil {
			return nil, false, err
		}
		if val == nil {
			return nil, true, nil
		}

		m = val.(*pipeline.Message)
		if info, get := m.TryToGetMoErr(); get {
			sender.safeToClose = true
			return nil, false, info
		}
		if m.IsEndMessage() {
			sender.safeToClose = true

			anaData := m.GetAnalyse()
			if len(anaData) > 0 {
				ana := new(pipeline.AnalysisList)
				if err = ana.Unmarshal(anaData); err != nil {
					return nil, false, err
				}
				sender.dealAnalysis(ana)
			}
			return nil, true, nil
		}

		if dataBuffer == nil {
			dataBuffer = m.Data
		} else {
			dataBuffer = append(dataBuffer, m.Data...)
		}

		if m.WaitingNextToMerge() {
			continue
		}

		bat, err = decodeBatch(sender.mp, dataBuffer)
		return bat, false, err
	}
}

// no matter how we stop the remote-run, we should get the final remote cost here.
func (sender *messageSenderOnClient) waitingTheStopResponse() {
	if sender.alreadyClose || sender.safeToClose {
		return
	}

	// cannot use sender.ctx here, because ctx maybe done.
	maxWaitingTime, cancel := context.WithTimeout(context.TODO(), 30*time.Second)
	defer cancel()

	// send a stop sending message to message-receiver.
	if err := sender.streamSender.Send(
		maxWaitingTime,
		generateStopSendingMessage(sender.streamSender.ID())); err != nil {
		return
	}

	// wait an EndMessage response.
	for {
		select {
		case val, ok := <-sender.receiveCh:
			if !ok || val == nil {
				sender.safeToClose = true
				sender.alreadyClose = true
				return
			}

			message := val.(*pipeline.Message)

			if message.IsEndMessage() || len(message.GetErr()) > 0 {
				sender.safeToClose = true
				// in fact, we should deal the cost analysis information here.
				return
			}

		case <-maxWaitingTime.Done():
			return
		}
	}
}

func generateStopSendingMessage(streamID uint64) *pipeline.Message {
	message := cnclient.AcquireMessage()
	message.SetMessageType(pipeline.Method_StopSending)
	message.SetID(streamID)
	message.NeedNotReply = true
	return message
}

func (sender *messageSenderOnClient) dealAnalysis(ana *pipeline.AnalysisList) {
	if sender.anal == nil {
		return
	}
	mergeAnalyseInfo(sender.anal, ana)
}

func mergeAnalyseInfo(target *anaylze, ana *pipeline.AnalysisList) {
	source := ana.List
	if len(target.analInfos) != len(source) {
		return
	}
	for i := range target.analInfos {
		n := source[i]
		atomic.AddInt64(&target.analInfos[i].OutputSize, n.OutputSize)
		atomic.AddInt64(&target.analInfos[i].OutputRows, n.OutputRows)
		atomic.AddInt64(&target.analInfos[i].InputRows, n.InputRows)
		atomic.AddInt64(&target.analInfos[i].InputSize, n.InputSize)
		atomic.AddInt64(&target.analInfos[i].MemorySize, n.MemorySize)
		target.analInfos[i].MergeArray(n)
		atomic.AddInt64(&target.analInfos[i].TimeConsumed, n.TimeConsumed)
		atomic.AddInt64(&target.analInfos[i].WaitTimeConsumed, n.WaitTimeConsumed)
		atomic.AddInt64(&target.analInfos[i].DiskIO, n.DiskIO)
		atomic.AddInt64(&target.analInfos[i].S3IOByte, n.S3IOByte)
		atomic.AddInt64(&target.analInfos[i].S3IOInputCount, n.S3IOInputCount)
		atomic.AddInt64(&target.analInfos[i].S3IOOutputCount, n.S3IOOutputCount)
		atomic.AddInt64(&target.analInfos[i].NetworkIO, n.NetworkIO)
		atomic.AddInt64(&target.analInfos[i].ScanTime, n.ScanTime)
		atomic.AddInt64(&target.analInfos[i].InsertTime, n.InsertTime)
	}
}

func (sender *messageSenderOnClient) close() {
	sender.waitingTheStopResponse()

	if sender.ctxCancel != nil {
		sender.ctxCancel()
	}
	if sender.alreadyClose {
		return
	}
	_ = sender.streamSender.Close(true)

	v2.PipelineMessageSenderCounter.Desc()
}

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
	"encoding/json"
	"fmt"
	"strings"
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
	"github.com/matrixorigin/matrixone/pkg/sql/models"
	"github.com/matrixorigin/matrixone/pkg/util/fault"
	v2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
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
			getLogger(s.Proc.GetService()).Error("panic in scope remoteRun",
				zap.String("sql", c.sql),
				zap.String("error", err.Error()))
		}
	}()
	s.ScopeAnalyzer.Stop()

	// encode structures which need to send.
	var scopeEncodeData, processEncodeData []byte
	var withoutOutput bool
	scopeEncodeData, withoutOutput, processEncodeData, err = prepareRemoteRunSendingData(c.sql, s)
	if err != nil {
		return nil, err
	}

	// generate a new sender to do send work.
	sender, err = newMessageSenderOnClient(
		s.Proc.Ctx,
		s.Proc.GetService(),
		s.NodeInfo.Addr,
		s.Proc.Mp(),
		c.anal,
	)
	if err != nil {
		c.proc.Errorf(s.Proc.Ctx, "Failed to newMessageSenderOnClient sql=%s, txnID=%s, err=%v",
			c.sql, c.proc.GetTxnOperator().Txn().DebugString(), err)

		return nil, err
	}

	debugMsg := ""
	_, sub_sql, exist := fault.TriggerFault("inject_send_pipeline")
	if exist {
		if strings.Contains(c.sql, sub_sql) {
			debugMsg = fmt.Sprintf("inject_send_pipeline: client2server,compile = %p", c)
		}
	}
	if err = sender.sendPipeline(scopeEncodeData, processEncodeData, withoutOutput, maxMessageSizeToMoRpc, debugMsg); err != nil {
		return sender, err
	}

	sender.safeToClose = false
	sender.alreadyClose = false
	err = receiveMessageFromCnServer(s, withoutOutput, sender)
	return sender, err
}

// checkPipelineStandaloneExecutableAtRemote is responsible for checking the standalone excitability of the pipeline
// once it was sent to other remote node.
//
// it returns true if the pipeline has only the root operator capable of sending data to other outer pipeline.
func checkPipelineStandaloneExecutableAtRemote(s *Scope) bool {
	var regs = make(map[*process.WaitRegister]struct{})
	var toScan []*Scope
	// record which mergeReceivers this scope tree holds.
	{
		toScan = append(toScan, s)
		for len(toScan) > 0 {
			node := toScan[len(toScan)-1]
			toScan = toScan[:len(toScan)-1]

			if len(node.PreScopes) > 0 {
				toScan = append(toScan, node.PreScopes...)
			}

			for i := range node.Proc.Reg.MergeReceivers {
				regs[node.Proc.Reg.MergeReceivers[i]] = struct{}{}
			}
		}
	}

	// check if there are target channels from other trees.
	{
		if len(s.PreScopes) > 0 {
			toScan = append(toScan, s.PreScopes...)
		}

		for len(toScan) > 0 {
			node := toScan[len(toScan)-1]
			toScan = toScan[:len(toScan)-1]

			if len(node.PreScopes) > 0 {
				toScan = append(toScan, node.PreScopes...)
			}

			if node.RootOp.OpType() == vm.Dispatch {
				t := node.RootOp.(*dispatch.Dispatch)
				for i := range t.LocalRegs {
					if _, ok := regs[t.LocalRegs[i]]; !ok {
						s.Proc.Infof(
							s.Proc.Ctx,
							"txn id : %s, the pipeline %p convert to execute locally because it holds a dispatch operator will send data to other local pipeline tree.",
							s.Proc.GetTxnOperator().Txn().ID, s)

						return false
					}
				}
				continue
			}
			if node.RootOp.OpType() == vm.Connector {
				t := node.RootOp.(*connector.Connector)
				if _, ok := regs[t.Reg]; !ok {
					s.Proc.Infof(
						s.Proc.Ctx,
						"txn id : %s, the pipeline %p convert to execute locally because it holds a connector operator will send data to other local pipeline tree.",
						s.Proc.GetTxnOperator().Txn().ID, s)

					return false
				}
				continue
			}
		}
	}

	return true
}

func prepareRemoteRunSendingData(sqlStr string, s *Scope) (scopeData []byte, withoutOutput bool, processData []byte, err error) {
	// if simpleRun is true, it indicates that this pipeline will not produce any output.
	withoutOutput = true

	// if the last operator is a sender operator, we need to keep it in local for sending batch to its receivers correctly.
	if lastOpType := s.RootOp.OpType(); lastOpType == vm.Connector || lastOpType == vm.Dispatch {
		withoutOutput = false

		originRoot := s.RootOp
		if originRoot.GetOperatorBase().NumChildren() == 0 {
			s.RootOp = nil
		} else {
			s.RootOp = originRoot.GetOperatorBase().GetChildren(0)
		}

		// todo: the following code to set children to nil must be a bug.
		// 		but I kept it here because there will be an operator release twice bug once I remove this code.
		//		I cannot find it why, maybe two scopes hold the same operator list pointer.
		originRoot.GetOperatorBase().SetChildren(nil)
		defer func() {
			s.doSetRootOperator(originRoot)
		}()
	}

	// Encode the ScopeList which need to be sent.
	if scopeData, err = encodeScope(s); err != nil {
		return nil, false, nil, err
	}

	// Encode the Process related information.
	if processData, err = encodeProcessInfo(s.Proc, sqlStr); err != nil {
		return nil, false, nil, err
	}

	return scopeData, withoutOutput, processData, nil
}

func receiveMessageFromCnServer(s *Scope, withoutOutput bool, sender *messageSenderOnClient) error {
	if !withoutOutput {
		// if the last operator was connector,
		// we can send data to the receiver channel to reduce spool's copy.
		if _, isConnector := s.RootOp.(*connector.Connector); isConnector {
			return receiveMessageFromCnServerIfConnector(s, sender)
		}

		// generate a new pipeline to send data in local.
		// value_scan -> dispatch -> next pipeline.
		if _, isDispatch := s.RootOp.(*dispatch.Dispatch); isDispatch {
			return receiveMessageFromCnServerIfDispatch(s, sender)
		}

		return moerr.NewInternalError(s.Proc.Ctx, fmt.Sprintf("remote run pipeline has an unexpected operator [id = %d] at last.", s.RootOp.OpType()))
	}

	// if the last operator is neither a connector nor a dispatch,
	// this indicates that it is a pipeline that does not require any local cooperation;
	// we simply need to wait for the remote execution to finish.
	return receiveMessageFromCnServerIfOnlyRun(s, sender)
}

func receiveMessageFromCnServerIfOnlyRun(s *Scope, sender *messageSenderOnClient) error {
	var bat *batch.Batch
	var end bool
	var err error

	mp := s.Proc.Mp()
	// Waiting the EndMessage or ErrorMessage.
	// In fact, for a pipeline that only needs to be executed remotely but without sending data back,
	// there should be no message sent back except for EndMessage or ErrorMessage.
	// However, I have used a loop here to ensure that the query can still be executed normally even if this situation occurs.
	for {
		bat, end, err = sender.receiveBatch()
		if err != nil || end || bat == nil {
			return err
		}
		bat.Clean(mp)
	}
}

func receiveMessageFromCnServerIfConnector(s *Scope, sender *messageSenderOnClient) error {
	var bat *batch.Batch
	var end bool
	var err error

	connectorOperator := s.RootOp.(*connector.Connector)
	connectorAnalyze := process.NewAnalyzer(
		connectorOperator.GetIdx(), connectorOperator.IsFirst, connectorOperator.IsLast, "connector")

	mp := s.Proc.Mp()
	nextChannel := s.RootOp.(*connector.Connector).Reg.Ch2
	for {
		bat, end, err = sender.receiveBatch()
		if err != nil || end || bat == nil {
			return err
		}
		connectorAnalyze.Network(bat)

		nextChannel <- process.NewPipelineSignalToDirectly(bat, nil, mp)
	}
}

func receiveMessageFromCnServerIfDispatch(s *Scope, sender *messageSenderOnClient) error {
	var bat *batch.Batch
	var end bool
	var err error

	arg := s.RootOp.(*dispatch.Dispatch)
	fakeValueScanOperator := value_scan.NewArgument()
	if err = fakeValueScanOperator.Prepare(s.Proc); err != nil {
		return err
	}

	oldChildren := arg.Children
	arg.Children = nil
	arg.AppendChild(fakeValueScanOperator)
	defer func() {
		arg.Children = oldChildren
		fakeValueScanOperator.Batchs = nil
		fakeValueScanOperator.Release()
	}()

	if err = s.RootOp.Prepare(s.Proc); err != nil {
		return err
	}
	dispatchAnalyze := s.RootOp.GetOperatorBase().OpAnalyzer

	mp := s.Proc.Mp()
	for {
		bat, end, err = sender.receiveBatch()
		if err != nil || end || bat == nil {
			return err
		}

		dispatchAnalyze.Network(bat)
		fakeValueScanOperator.Batchs = append(fakeValueScanOperator.Batchs, bat)

		result, errCall := vm.Exec(s.RootOp, s.Proc)
		bat.Clean(mp)
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
	anal *AnalyzeModule

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
	ctx context.Context,
	sid string,
	toAddr string,
	mp *mpool.MPool,
	analyzeModule *AnalyzeModule,
) (*messageSenderOnClient, error) {
	streamSender, err := cnclient.GetPipelineClient(sid).NewStream(toAddr)
	if err != nil {
		return nil, err
	}

	sender := &messageSenderOnClient{
		safeToClose:  true,
		alreadyClose: false,
		mp:           mp,
		anal:         analyzeModule,
		streamSender: streamSender,
	}

	if _, ok := ctx.Deadline(); !ok {
		sender.ctx, sender.ctxCancel = context.WithTimeoutCause(ctx, MaxRpcTime, moerr.CauseNewMessageSenderOnClient)
	} else {
		sender.ctx = ctx
	}

	if sender.receiveCh == nil {
		sender.receiveCh, err = sender.streamSender.Receive()
	}

	v2.PipelineMessageSenderCounter.Inc()
	return sender, moerr.AttachCause(ctx, err)
}

func (sender *messageSenderOnClient) sendPipeline(
	scopeData, procData []byte, noDataBack bool, eachMessageSizeLimitation int, debugMsg string) error {
	sdLen := len(scopeData)
	if sdLen <= eachMessageSizeLimitation {
		message := cnclient.AcquireMessage()
		message.SetDebugMsg(debugMsg)
		message.SetID(sender.streamSender.ID())
		message.SetMessageType(pipeline.Method_PipelineMessage)
		message.SetData(scopeData)
		message.SetProcData(procData)
		message.SetSid(pipeline.Status_Last)
		message.NeedNotReply = noDataBack
		return sender.streamSender.Send(sender.ctx, message)
	}

	start := 0
	for start < sdLen {
		end := start + eachMessageSizeLimitation

		message := cnclient.AcquireMessage()
		message.SetDebugMsg(debugMsg)
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
		message.NeedNotReply = noDataBack

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
				var p models.PhyPlan
				err = json.Unmarshal(anaData, &p)
				if err != nil {
					return nil, false, err
				}

				sender.dealRemoteAnalysis(p)
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
		/* 		bat := batch.NewOffHeapEmpty()
		   		if err := bat.UnmarshalBinary(dataBuffer); err != nil {
		   			bat.Clean(sender.mp)
		   			return bat, false, err
		   		} */
		return bat, false, err
	}
}

// no matter how we stop the remote-run, we should get the final remote cost here.
func (sender *messageSenderOnClient) waitingTheStopResponse() {
	if sender.alreadyClose || sender.safeToClose {
		return
	}

	// cannot use sender.ctx here, because ctx maybe done.
	maxWaitingTime, cancel := context.WithTimeoutCause(context.TODO(), 30*time.Second, moerr.CauseWaitingTheStopResponse)
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
	message.NeedNotReply = false
	return message
}

func (sender *messageSenderOnClient) dealRemoteAnalysis(p models.PhyPlan) {
	if sender.anal == nil {
		return
	}
	sender.anal.AppendRemotePhyPlan(p)
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

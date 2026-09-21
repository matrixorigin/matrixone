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
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/matrixorigin/matrixone/pkg/cnservice/cnclient"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/morpc"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/pb/pipeline"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/connector"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/dispatch"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/value_scan"
	"github.com/matrixorigin/matrixone/pkg/sql/models"
	"github.com/matrixorigin/matrixone/pkg/util/fault"
	v2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
	"github.com/matrixorigin/matrixone/pkg/util/resource"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	vmpipeline "github.com/matrixorigin/matrixone/pkg/vm/pipeline"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"go.uber.org/zap"
)

// MaxRpcTime is a default timeout time to rpc context if user never set this deadline.
// this is just a number I casually wrote, the purpose of doing this is that any message sent through rpc need a clear deadline.
const MaxRpcTime = time.Hour * 24

var (
	pipelineStopSendingClientTimeout  = 30 * time.Second
	pipelineStreamFinishClientTimeout = 30 * time.Second
)

// remoteRun sends a scope to remote node for running.
// and keep receiving the back results.
//
// we assume that, result message is always *pipeline.Message, and there are 3 cases for that:
// first, Message with error information.
// second, Message with EndFlag and Analysis Information.
// third, Message with batch data.
func remoteExecutionTopology(c *Compile, s *Scope) (map[string]uint32, uuid.UUID) {
	if s != nil && s.lazyRemoteExecutionID != uuid.Nil {
		return s.lazyRemoteFragmentCounts, s.lazyRemoteExecutionID
	}
	return c.remoteFragmentCounts, c.remoteExecutionID
}

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
	var withoutOutput bool
	sender, withoutOutput, err = s.prepareRemoteRun(c)
	if err != nil {
		return sender, err
	}

	err = receiveMessageFromCnServer(s, withoutOutput, sender)
	return sender, err
}

// prepareRemoteRun owns the synchronous part of remote admission: scope and
// process encoding, stream creation, and the initial pipeline send. It never
// receives result batches. The event-driven remote state machine uses this
// same boundary and schedules one transport receive at a time afterward.
func (s *Scope) prepareRemoteRun(c *Compile) (sender *messageSenderOnClient, withoutOutput bool, err error) {
	sender, withoutOutput, scopeEncodeData, processEncodeData, debugMsg, err := s.prepareRemoteRunData(c)
	if err != nil {
		return sender, withoutOutput, err
	}
	if err = sender.sendPipeline(scopeEncodeData, processEncodeData, withoutOutput, maxMessageSizeToMoRpc, debugMsg); err != nil {
		return sender, withoutOutput, err
	}
	return sender, withoutOutput, nil
}

// prepareRemoteRunData performs encoding and stream creation but deliberately
// does not wait for the initial MORPC writer admission. The event-driven
// remote path feeds the returned payload into sendPipelineAsync; the legacy
// remoteRun wrapper continues to call sendPipeline synchronously.
func (s *Scope) prepareRemoteRunData(c *Compile) (
	sender *messageSenderOnClient,
	withoutOutput bool,
	scopeEncodeData []byte,
	processEncodeData []byte,
	debugMsg string,
	err error,
) {
	s.ScopeAnalyzer.Stop()

	var folded bool
	remoteFragmentCounts, remoteExecutionID := remoteExecutionTopology(c, s)
	scopeEncodeData, withoutOutput, processEncodeData, folded, err = prepareRemoteRunSendingData(
		c.sql,
		s,
		c.proc,
		remoteFragmentCounts,
		remoteExecutionID,
	)
	if err != nil {
		return nil, false, nil, nil, "", err
	}
	if folded {
		getLogger(s.Proc.GetService()).
			Debug("fold variable expressions before remote run",
				zap.String("local-address", c.addr),
				zap.String("remote-address", s.NodeInfo.Addr))
	}

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
		return nil, false, nil, nil, "", err
	}
	sender.proc = s.Proc
	// Capture the execution-attempt sink, rather than looking it up from proc
	// when the terminal message arrives. Retry/reuse can replace proc.WarningSink
	// before an old RPC callback is delivered; a closed captured sink then drops
	// that stale callback instead of publishing it into the new attempt.
	sender.warningSink = s.Proc.GetWarningSink()

	_, subSQL, exist := fault.TriggerFault("inject_send_pipeline")
	if exist && strings.Contains(c.sql, subSQL) {
		debugMsg = fmt.Sprintf("inject_send_pipeline: client2server,compile = %p", c)
	}
	return sender, withoutOutput, scopeEncodeData, processEncodeData, debugMsg, nil
}

// checkPipelineStandaloneExecutableAtRemote is responsible for checking the standalone excitability of the pipeline
// once it was sent to other remote node.
//
// it returns true if the pipeline has only the root operator capable of sending data to other outer pipeline.
func checkPipelineStandaloneExecutableAtRemote(s *Scope) bool {
	offender := findPipelineExternalLocalReceiver(s)
	if offender == nil {
		return true
	}

	switch offender.OpType() {
	case vm.Dispatch:
		s.Proc.Infof(
			s.Proc.Ctx,
			"txn id : %s, the pipeline %p cannot execute remotely because its dispatch operator targets another local pipeline tree.",
			s.Proc.GetTxnOperator().Txn().ID, s)
	case vm.Connector:
		s.Proc.Infof(
			s.Proc.Ctx,
			"txn id : %s, the pipeline %p cannot execute remotely because its connector targets another local pipeline tree.",
			s.Proc.GetTxnOperator().Txn().ID, s)
	}
	return false
}

// findPipelineExternalLocalReceiver returns the first non-root output operator
// whose in-process receiver is not owned by the scope tree. The root output is
// intentionally excluded: RemoteRun retains it on the caller and forwards the
// remotely executed child tree back through that output.
func findPipelineExternalLocalReceiver(s *Scope) vm.Operator {
	if s == nil {
		return nil
	}

	var regs = make(map[*process.WaitRegister]struct{})
	var toScan []*Scope
	// record which mergeReceivers this scope tree holds.
	{
		toScan = append(toScan, s)
		for len(toScan) > 0 {
			node := toScan[len(toScan)-1]
			toScan = toScan[:len(toScan)-1]
			if node == nil {
				continue
			}

			if len(node.PreScopes) > 0 {
				toScan = append(toScan, node.PreScopes...)
			}

			if node.Proc != nil {
				for i := range node.Proc.Reg.MergeReceivers {
					regs[node.Proc.Reg.MergeReceivers[i]] = struct{}{}
				}
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
			if node == nil {
				continue
			}

			if len(node.PreScopes) > 0 {
				toScan = append(toScan, node.PreScopes...)
			}
			if node.RootOp == nil {
				continue
			}

			if node.RootOp.OpType() == vm.Dispatch {
				t := node.RootOp.(*dispatch.Dispatch)
				for i := range t.LocalRegs {
					if _, ok := regs[t.LocalRegs[i]]; !ok {
						return node.RootOp
					}
				}
				continue
			}
			if node.RootOp.OpType() == vm.Connector {
				t := node.RootOp.(*connector.Connector)
				if _, ok := regs[t.Reg]; !ok {
					return node.RootOp
				}
				continue
			}
		}
	}

	return nil
}

func prepareRemoteRunSendingData(
	sqlStr string,
	s *Scope,
	proc *process.Process,
	remoteFragmentCounts map[string]uint32,
	remoteExecutionID uuid.UUID,
) (scopeData []byte, withoutOutput bool, processData []byte, folded bool, err error) {
	// The output dispatch executes on the initiating CN and is stripped from
	// the encoded scope below. Validate its consumers before losing that edge.
	if queryNeedsGroupingTransport(s.Plan.GetQuery()) {
		if output, ok := s.RootOp.(*dispatch.Dispatch); ok && len(output.RemoteRegs) > 0 {
			workers := make(engine.Nodes, 0, len(output.RemoteRegs))
			for _, dest := range output.RemoteRegs {
				workers = append(workers, engine.Node{Addr: dest.NodeAddr})
			}
			if err = requireGroupingTransportWorkers(proc, workers); err != nil {
				return nil, false, nil, false, err
			}
		}
	}
	if output, ok := s.RootOp.(*connector.Connector); ok &&
		output.Reg != nil && output.Reg.OrderedStream &&
		!supportsDistributedOrderedTop(proc.GetService()) {
		return nil, false, nil, false, moerr.NewNotSupportedNoCtx(
			"distributed ordered Top-N requires MORPC protocol version 53")
	}
	encodedScope, withoutOutput := getScopeForRemoteRunEncoding(s)
	encodedScope = copyBlockFiltersForRemoteRun(encodedScope)
	encodedScope, folded, err = foldVarExprsInRemoteRunScope(encodedScope, proc)
	if err != nil {
		return nil, false, nil, false, err
	}

	// Encode the ScopeList which need to be sent.
	if scopeData, err = encodeRemoteScope(encodedScope, proc); err != nil {
		return nil, false, nil, false, err
	}

	// Encode the Process related information.
	if processData, err = encodeProcessInfo(
		s.Proc,
		sqlStr,
		remoteFragmentCounts,
		remoteExecutionID,
	); err != nil {
		return nil, false, nil, false, err
	}

	return scopeData, withoutOutput, processData, folded, nil
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
		if err = sender.acknowledgeRemoteBatch(); err != nil {
			return err
		}
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
	nextReg := s.RootOp.(*connector.Connector).Reg
	for {
		bat, end, err = sender.receiveBatch()
		if err != nil || end || bat == nil {
			return err
		}
		connectorAnalyze.Network(bat)

		var receiverDone bool
		if receiverDone, err = forwardRemoteBatchWithContext(sender, nextReg, bat, mp); err != nil {
			return err
		}
		// A stopped receiver intentionally discarded the decoded batch, but the
		// remote sender still owns its credit until this ACK is sent.
		if err = sender.acknowledgeRemoteBatch(); err != nil {
			return err
		}
		if receiverDone {
			return nil
		}
	}
}

func receiveMessageFromCnServerIfDispatch(s *Scope, sender *messageSenderOnClient) error {
	var bat *batch.Batch
	var end bool
	var err error

	arg := s.RootOp.(*dispatch.Dispatch)
	fakeValueScanOperator := value_scan.NewArgument()
	dispatchRunner := buildRemoteDispatchReceiverRoot(arg, fakeValueScanOperator)
	dispatchRunner.AdoptCleanupState(arg)
	defer func() {
		arg.AdoptCleanupState(dispatchRunner)
		dispatchRunner.Release()
		fakeValueScanOperator.Free(s.Proc, err != nil, err)
		fakeValueScanOperator.Batchs = nil
		fakeValueScanOperator.Release()
	}()

	dispatchPipeline, err := vmpipeline.New(0, nil, dispatchRunner).NewContinuation(s.Proc)
	if err != nil {
		return err
	}
	mp := s.Proc.Mp()
	for {
		bat, end, err = sender.receiveBatch()
		if err != nil || end || bat == nil {
			return err
		}

		if dispatchAnalyze := dispatchRunner.GetOperatorBase().OpAnalyzer; dispatchAnalyze != nil {
			dispatchAnalyze.Network(bat)
		}
		fakeValueScanOperator.Batchs = append(fakeValueScanOperator.Batchs, bat)

		var result vmpipeline.StepResult
		for {
			result, err = dispatchPipeline.Step()
			if err != nil {
				bat.Clean(mp)
				return err
			}
			if result.Status != vmpipeline.StepWaiting {
				break
			}
			if result.OnReady == nil {
				bat.Clean(mp)
				return moerr.NewInternalErrorNoCtx("remote dispatch continuation waited without readiness")
			}
			ready := make(chan struct{}, 1)
			if err = result.OnReady(func() {
				select {
				case ready <- struct{}{}:
				default:
				}
			}); err != nil {
				bat.Clean(mp)
				return err
			}
			select {
			case <-ready:
			case <-s.Proc.Ctx.Done():
				bat.Clean(mp)
				return s.Proc.Ctx.Err()
			}
		}
		bat.Clean(mp)
		// ExecStop can mean that every receiver has already stopped. Release the
		// decoded batch's remote credit before ending the receive loop.
		if err = sender.acknowledgeRemoteBatch(); err != nil {
			return err
		}
		if result.Status == vmpipeline.StepDone {
			return nil
		}
	}
}

func getScopeForRemoteRunEncoding(s *Scope) (*Scope, bool) {
	withoutOutput := true
	if s.RootOp == nil {
		return s, withoutOutput
	}

	if lastOpType := s.RootOp.OpType(); lastOpType == vm.Connector || lastOpType == vm.Dispatch {
		withoutOutput = false
		copied := *s
		if s.RootOp.GetOperatorBase().NumChildren() == 0 {
			copied.RootOp = nil
		} else {
			copied.RootOp = s.RootOp.GetOperatorBase().GetChildren(0)
		}
		return &copied, withoutOutput
	}
	return s, withoutOutput
}

func buildRemoteDispatchReceiverRoot(arg *dispatch.Dispatch, child vm.Operator) *dispatch.Dispatch {
	copied := dispatch.NewArgument()
	copied.IsSink = arg.IsSink
	copied.RecSink = arg.RecSink
	copied.RecCTE = arg.RecCTE
	copied.ShuffleType = arg.ShuffleType
	copied.FuncId = arg.FuncId
	copied.LocalRegs = arg.LocalRegs
	copied.RemoteRegs = arg.RemoteRegs
	copied.ShuffleRegIdxLocal = arg.ShuffleRegIdxLocal
	copied.ShuffleRegIdxRemote = arg.ShuffleRegIdxRemote
	copied.OperatorBase.OperatorInfo = arg.OperatorBase.OperatorInfo
	copied.AppendChild(child)
	return copied
}

// messageSenderOnClient support a series of methods
// to do sending message and receiving its returns.
type messageSenderOnClient struct {
	// sender's context
	// and cancel function (it exists if this context was recreated by us).
	ctx                context.Context
	ctxCancel          context.CancelFunc
	useInternalTimeout bool

	mp *mpool.MPool

	// anal was used to merge remote-run's cost analysis information.
	anal *AnalyzeModule
	proc *process.Process

	// warningSink is the captured diagnostic destination on the initiating
	// process for this execution attempt. Remote terminal warnings are applied
	// here only after the remote pipeline has finished, preserving one warning
	// per actual evaluated row and preventing a late retry callback from finding
	// a newer destination.
	warningSink any

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
	// receiveClosed records a terminal signal from the receive channel. It
	// poisons backend reuse, but does not release the locally owned morpc Stream;
	// close must still call Stream.Close.
	receiveClosed           bool
	reuseEligible           bool
	terminalNegotiated      bool
	stopResponseTried       bool
	expectedEnd             pipeline.Method
	reportingRequestStarted bool
	stateMu                 sync.Mutex
	closeOnce               sync.Once
	requestFinishAck        bool
	pendingBatchAck         uint64
	// allowCleanupCancellation is set after successful local cleanup. Pipeline
	// and query contexts may be intentionally cancelled by that cleanup; FIN
	// then runs on its own bounded context. Cancellation before this transition
	// still poisons reuse.
	allowCleanupCancellation bool

	// gaugeDecOnce ensures PipelineMessageSenderGauge.Dec() is called at most once when close() runs.
	gaugeDecOnce sync.Once
	terminalMu   sync.Mutex
	terminalSeen bool
}

// remoteBatchDecoder incrementally consumes one MORPC message at a time. It
// is shared by remote-run and remote-notify state machines so neither path
// needs to park an event worker in receiveBatch while waiting for the next
// fragment.
type remoteBatchDecoder struct {
	dataBuffer    []byte
	batchSequence uint64
}

func (d *remoteBatchDecoder) consume(
	sender *messageSenderOnClient,
	message morpc.Message,
	ok bool,
) (*batch.Batch, bool, error) {
	if !ok {
		sender.markReceiveClosed()
		return nil, false, moerr.NewStreamClosed(sender.ctx)
	}
	if message == nil {
		if ctxErr := sender.contextDoneError(); ctxErr != nil {
			return nil, false, ctxErr
		}
		return nil, true, nil
	}
	m, ok := message.(*pipeline.Message)
	if !ok || m == nil {
		return nil, false, moerr.NewInternalErrorNoCtx("remote stream returned an unexpected message")
	}
	if sequence := m.GetBatchSequence(); sequence != 0 {
		if d.batchSequence != 0 && d.batchSequence != sequence {
			return nil, false, moerr.NewInvalidStateNoCtxf(
				"remote batch fragments changed sequence from %d to %d",
				d.batchSequence, sequence)
		}
		d.batchSequence = sequence
	}
	if m.IsEndMessage() {
		if err := sender.dealRemoteTerminal(m.GetAnalyse()); err != nil {
			return nil, false, err
		}
	}
	if info, get := m.TryToGetMoErr(); get {
		sender.markTerminal(m, false)
		return nil, false, info
	}
	if m.IsEndMessage() {
		sender.markTerminal(m, true)
		d.dataBuffer = nil
		d.batchSequence = 0
		return nil, true, nil
	}
	if d.dataBuffer == nil {
		d.dataBuffer = m.Data
	} else {
		d.dataBuffer = append(d.dataBuffer, m.Data...)
	}
	if m.WaitingNextToMerge() {
		return nil, false, nil
	}

	batchSequence := d.batchSequence
	bat, err := decodeBatch(sender.mp, d.dataBuffer)
	d.dataBuffer = nil
	d.batchSequence = 0
	if err == nil {
		if sender.pendingBatchAck != 0 {
			bat.Clean(sender.mp)
			return nil, false, moerr.NewInvalidStateNoCtx(
				"remote batch ACK was not sent before receiving the next batch")
		}
		sender.pendingBatchAck = batchSequence
	}
	return bat, false, err
}

func newMessageSenderOnClient(
	ctx context.Context,
	sid string,
	toAddr string,
	mp *mpool.MPool,
	analyzeModule *AnalyzeModule,
) (*messageSenderOnClient, error) {
	streamCtx := ctx
	var streamCtxCancel context.CancelFunc
	useInternalTimeout := false
	if _, ok := ctx.Deadline(); !ok {
		streamCtx, streamCtxCancel = context.WithTimeoutCause(ctx, MaxRpcTime, moerr.CauseNewMessageSenderOnClient)
		useInternalTimeout = true
	}
	cleanupStreamCtx := func() {
		if streamCtxCancel != nil {
			streamCtxCancel()
		}
	}

	if moruntime.ServiceRuntime(sid) == nil {
		cleanupStreamCtx()
		return nil, moerr.NewInternalErrorNoCtx("service runtime is not initialized")
	}
	pipelineClient := cnclient.GetPipelineClient(sid)
	if pipelineClient == nil {
		cleanupStreamCtx()
		return nil, moerr.NewInternalErrorNoCtx("pipeline client is not initialized")
	}

	streamSender, err := pipelineClient.NewStream(streamCtx, toAddr)
	if err != nil {
		err = moerr.AttachCause(streamCtx, err)
		cleanupStreamCtx()
		return nil, err
	}
	if streamSender == nil {
		cleanupStreamCtx()
		return nil, moerr.NewInternalErrorNoCtx("pipeline stream is not initialized")
	}

	sender := &messageSenderOnClient{
		ctx:                streamCtx,
		ctxCancel:          streamCtxCancel,
		useInternalTimeout: useInternalTimeout,
		safeToClose:        true,
		receiveClosed:      false,
		mp:                 mp,
		anal:               analyzeModule,
		streamSender:       streamSender,
		requestFinishAck:   pipelineStreamReuseEnabled(sid),
	}

	if sender.receiveCh == nil {
		sender.receiveCh, err = sender.streamSender.Receive()
	}

	// Only Inc() when we return a valid sender that the caller will eventually close();
	// when Receive() fails, close the stream here because remoteRun returns nil and never calls close().
	if err != nil {
		err = moerr.AttachCause(streamCtx, err)
		cleanupStreamCtx()
		_ = streamSender.Close(true)
		return nil, err
	}
	v2.PipelineMessageSenderGauge.Inc()
	return sender, nil
}

func pipelineStreamReuseEnabled(serviceID string) bool {
	runtime := moruntime.ServiceRuntime(serviceID)
	if runtime == nil {
		return true
	}
	value, ok := runtime.GetGlobalVariables(moruntime.EnablePipelineStreamReuse)
	if !ok {
		return true
	}
	enabled, ok := value.(bool)
	return ok && enabled
}

func (sender *messageSenderOnClient) requestStreamProtocols(message *pipeline.Message) {
	if !sender.requestFinishAck {
		return
	}
	message.RequestedTeardownMode = pipeline.StreamTeardownMode_FinishAck
	message.RequestedBatchCreditCount = pipelineBatchCreditCount
	message.RequestedBatchCreditBytes = pipelineBatchCreditBytes
}

func (sender *messageSenderOnClient) sendPipeline(
	scopeData, procData []byte, noDataBack bool, eachMessageSizeLimitation int, debugMsg string) error {
	sender.markReportingRequestStarted()
	for _, message := range sender.pipelineMessages(
		scopeData,
		procData,
		noDataBack,
		eachMessageSizeLimitation,
		debugMsg,
	) {
		if err := sender.streamSender.Send(sender.ctx, message); err != nil {
			return err
		}
	}
	sender.markStreamActive(pipeline.Method_PipelineMessage)
	return nil
}

func (sender *messageSenderOnClient) pipelineMessages(
	scopeData, procData []byte,
	noDataBack bool,
	eachMessageSizeLimitation int,
	debugMsg string,
) []*pipeline.Message {
	sdLen := len(scopeData)
	if sdLen <= eachMessageSizeLimitation {
		message := cnclient.AcquireMessage()
		message.SetDebugMsg(debugMsg)
		message.SetID(sender.streamSender.ID())
		message.SetMessageType(pipeline.Method_PipelineMessage)
		message.SetData(scopeData)
		message.SetProcData(procData)
		message.SetSid(pipeline.Status_Last)
		sender.requestStreamProtocols(message)
		message.NeedNotReply = noDataBack
		return []*pipeline.Message{message}
	}

	messages := make([]*pipeline.Message, 0, (sdLen+eachMessageSizeLimitation-1)/eachMessageSizeLimitation)
	for start := 0; start < sdLen; start += eachMessageSizeLimitation {
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
		sender.requestStreamProtocols(message)
		messages = append(messages, message)
	}
	return messages
}

// sendPipelineAsync sends one pipeline fragment per scheduler event. The
// continuation advances only after MORPC writer admission, so an output queue
// that is full becomes a channel event rather than a blocked scheduler lane.
func (sender *messageSenderOnClient) sendPipelineAsync(
	scheduler *scopeTaskScheduler,
	scopeData, procData []byte,
	noDataBack bool,
	eachMessageSizeLimitation int,
	debugMsg string,
	done func(error),
) error {
	if scheduler == nil {
		return moerr.NewInternalErrorNoCtx("nil scheduler for remote pipeline send")
	}
	if done == nil {
		return moerr.NewInternalErrorNoCtx("nil callback for remote pipeline send")
	}
	sender.markReportingRequestStarted()
	messages := sender.pipelineMessages(scopeData, procData, noDataBack, eachMessageSizeLimitation, debugMsg)
	var once sync.Once
	finish := func(err error) { once.Do(func() { done(err) }) }
	index := 0
	var sendNext func()
	sendNext = func() {
		if index >= len(messages) {
			sender.markStreamActive(pipeline.Method_PipelineMessage)
			finish(nil)
			return
		}
		message := messages[index]
		index++
		if err := scheduler.submitStreamSendWithContext("remote-pipeline-send", sender.streamSender, sender.ctx, message, func(err error) {
			if err != nil {
				finish(err)
				return
			}
			sendNext()
		}, true); err != nil {
			finish(err)
		}
	}
	// The initial pipeline message is also the cancellation/stop handshake
	// boundary: even an already-expired query must be admitted far enough to
	// publish the remote terminal cleanup signal. The MORPC send itself still
	// observes the expired context and reports its result asynchronously.
	return scheduler.submitEventSourceWithContext("remote-pipeline-send-start", sendNext, true)
}

func (sender *messageSenderOnClient) markStreamActive(method pipeline.Method) {
	sender.stateMu.Lock()
	defer sender.stateMu.Unlock()
	sender.safeToClose = false
	sender.receiveClosed = false
	sender.reuseEligible = false
	sender.terminalNegotiated = false
	sender.stopResponseTried = false
	sender.allowCleanupCancellation = false
	sender.expectedEnd = method
}

func (sender *messageSenderOnClient) markReceiveClosed() {
	sender.stateMu.Lock()
	defer sender.stateMu.Unlock()
	sender.safeToClose = true
	sender.receiveClosed = true
	sender.reuseEligible = false
	sender.terminalNegotiated = false
}

func (sender *messageSenderOnClient) markTerminal(message *pipeline.Message, successful bool) {
	sender.stateMu.Lock()
	defer sender.stateMu.Unlock()
	sender.safeToClose = true
	sender.terminalNegotiated = message.GetCmd() == sender.expectedEnd &&
		message.GetAcceptedTeardownMode() == pipeline.StreamTeardownMode_FinishAck
	sender.reuseEligible = sender.terminalNegotiated &&
		(successful || sender.allowCleanupCancellation)
}

func (sender *messageSenderOnClient) prepareForLocalCleanup() {
	sender.stateMu.Lock()
	defer sender.stateMu.Unlock()
	sender.allowCleanupCancellation = true
	if sender.terminalNegotiated {
		sender.reuseEligible = true
	}
}

func (sender *messageSenderOnClient) receiveMessage() (morpc.Message, error) {
	select {
	case <-sender.ctx.Done():
		return nil, nil

	case val, ok := <-sender.receiveCh:
		if !ok || val == nil {
			sender.markReceiveClosed()
			return nil, moerr.NewStreamClosed(sender.ctx)
		}
		return val, nil
	}
}

func (sender *messageSenderOnClient) receiveBatch() (bat *batch.Batch, over bool, err error) {
	var val morpc.Message
	var m *pipeline.Message
	var dataBuffer []byte
	var batchSequence uint64

	for {
		val, err = sender.receiveMessage()
		if err != nil {
			return nil, false, err
		}
		if val == nil {
			if ctxErr := sender.contextDoneError(); ctxErr != nil {
				return nil, false, ctxErr
			}
			return nil, true, nil
		}

		m = val.(*pipeline.Message)
		if sequence := m.GetBatchSequence(); sequence != 0 {
			if batchSequence != 0 && batchSequence != sequence {
				return nil, false, moerr.NewInvalidStateNoCtxf(
					"remote batch fragments changed sequence from %d to %d",
					batchSequence, sequence)
			}
			batchSequence = sequence
		}
		if m.IsEndMessage() {
			if err = sender.dealRemoteTerminal(m.GetAnalyse()); err != nil {
				return nil, false, err
			}
		}
		if info, get := m.TryToGetMoErr(); get {
			sender.markTerminal(m, false)
			return nil, false, info
		}
		if m.IsEndMessage() {
			sender.markTerminal(m, true)
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
		if err == nil {
			if sender.pendingBatchAck != 0 {
				bat.Clean(sender.mp)
				return nil, false, moerr.NewInvalidStateNoCtx(
					"remote batch ACK was not sent before receiving the next batch")
			}
			sender.pendingBatchAck = batchSequence
		}
		return bat, false, err
	}
}

func (sender *messageSenderOnClient) acknowledgeRemoteBatch() error {
	sequence := sender.pendingBatchAck
	if sequence == 0 {
		return nil
	}
	message := cnclient.AcquireMessage()
	message.SetID(sender.streamSender.ID())
	message.SetMessageType(pipeline.Method_PipelineBatchAck)
	message.SetSid(pipeline.Status_Last)
	message.BatchAckSequence = sequence
	if err := sender.streamSender.Send(sender.ctx, message); err != nil {
		return err
	}
	sender.pendingBatchAck = 0
	return nil
}

// acknowledgeRemoteBatchAsync publishes the batch credit through the query
// scheduler. Normal remote state-machine paths must not wait in a ready task
// for MORPC writer-queue admission; the synchronous helper above remains for
// legacy blocking receive loops and terminal compatibility cleanup.
func (sender *messageSenderOnClient) acknowledgeRemoteBatchAsync(
	scheduler *scopeTaskScheduler,
	name string,
	done func(error),
) error {
	if scheduler == nil {
		return moerr.NewInternalErrorNoCtx("nil scheduler for remote batch acknowledgement")
	}
	if done == nil {
		return moerr.NewInternalErrorNoCtx("nil callback for remote batch acknowledgement")
	}
	sequence := sender.pendingBatchAck
	if sequence == 0 {
		done(nil)
		return nil
	}
	message := cnclient.AcquireMessage()
	message.SetID(sender.streamSender.ID())
	message.SetMessageType(pipeline.Method_PipelineBatchAck)
	message.SetSid(pipeline.Status_Last)
	message.BatchAckSequence = sequence
	return scheduler.submitStreamSend(name, sender.streamSender, sender.ctx, message, func(err error) {
		if err == nil {
			sender.pendingBatchAck = 0
		}
		done(err)
	})
}

func (sender *messageSenderOnClient) contextDoneError() error {
	if sender.ctx == nil {
		return nil
	}
	err := sender.ctx.Err()
	if err == nil {
		return nil
	}
	if errors.Is(err, context.DeadlineExceeded) && sender.useInternalTimeout {
		return moerr.NewRPCTimeout(sender.ctx)
	}
	return moerr.NewQueryInterrupted(sender.ctx)
}

func forwardRemoteBatchWithContext(
	sender *messageSenderOnClient,
	nextReg *process.WaitRegister,
	bat *batch.Batch,
	mp *mpool.MPool,
) (receiverDone bool, err error) {
	if nextReg == nil || nextReg.Ch2 == nil {
		bat.Clean(mp)
		return true, moerr.NewInternalErrorNoCtx("remote batch forward target is nil")
	}

	ctx := context.TODO()
	if sender == nil || sender.ctx == nil {
		if nextReg.SendDataDirect(ctx, bat, mp) {
			return false, nil
		}
		bat.Clean(mp)
		return true, nil
	}

	if nextReg.SendDataDirect(sender.ctx, bat, mp) {
		return false, nil
	}
	bat.Clean(mp)
	if err := sender.contextDoneError(); err != nil {
		return true, err
	}
	return true, nil
}

// waitingTheStopResponse asks an unfinished remote stream to stop and waits for
// its terminal response. The terminal error remains part of execution state:
// cancellation may have won the caller's receive select immediately before the
// remote pipeline reported its actual failure.
func (sender *messageSenderOnClient) waitingTheStopResponse() error {
	sender.stateMu.Lock()
	if sender.receiveClosed || sender.safeToClose || sender.stopResponseTried {
		sender.stateMu.Unlock()
		return nil
	}
	// RemoteRun and close share this teardown owner. Claim the handshake before
	// doing I/O so a terminal-less attempt cannot be repeated by close and add a
	// second full timeout to the same statement.
	sender.stopResponseTried = true
	sender.stateMu.Unlock()

	// cannot use sender.ctx here, because ctx maybe done.
	maxWaitingTime, cancel := context.WithTimeoutCause(
		context.Background(), pipelineStopSendingClientTimeout, moerr.CauseWaitingTheStopResponse)
	defer cancel()

	// send a stop sending message to message-receiver.
	if err := sender.streamSender.Send(
		maxWaitingTime,
		generateStopSendingMessage(sender.streamSender.ID())); err != nil {
		if maxWaitingTime.Err() != nil {
			return moerr.NewRPCTimeout(maxWaitingTime)
		}
		// The handshake owns an independent live context. A cancellation-shaped
		// Send result therefore describes a closed transport, not successful
		// pipeline cancellation, and must not be suppressible by RemoteRun.
		if isScopeCancellationError(err) {
			return moerr.NewStreamClosedNoCtx()
		}
		return err
	}

	// wait an EndMessage response.
	for {
		select {
		case val, ok := <-sender.receiveCh:
			if !ok || val == nil {
				sender.markReceiveClosed()
				return moerr.NewStreamClosedNoCtx()
			}

			message := val.(*pipeline.Message)

			if message.IsEndMessage() || len(message.GetErr()) > 0 {
				_ = sender.dealRemoteTerminal(message.GetAnalyse())
				if terminalErr, ok := message.TryToGetMoErr(); ok {
					sender.markTerminal(message, false)
					return terminalErr
				}
				// StopSending is also a clean teardown when the original server
				// worker answers with its negotiated terminal response. The later FIN
				// still waits for the same server cleanup barrier. Unnegotiated or
				// mismatched terminal responses remain poisoned.
				sender.markTerminal(message, true)
				// in fact, we should deal the cost analysis information here.
				return nil
			}

		case <-maxWaitingTime.Done():
			return moerr.NewRPCTimeout(maxWaitingTime)
		}
	}
}

func generatePipelineStreamFinishMessage(streamID uint64) *pipeline.Message {
	message := cnclient.AcquireMessage()
	message.SetMessageType(pipeline.Method_PipelineStreamFinish)
	message.SetSid(pipeline.Status_Last)
	message.SetID(streamID)
	message.RequestedTeardownMode = pipeline.StreamTeardownMode_FinishAck
	return message
}

func (sender *messageSenderOnClient) finishStreamForReuse() bool {
	var senderDone <-chan struct{}
	sender.stateMu.Lock()
	allowCleanupCancellation := sender.allowCleanupCancellation
	sender.stateMu.Unlock()
	cancelCtx := sender.ctx
	if allowCleanupCancellation {
		cancelCtx = nil
	}
	if cancelCtx != nil && cancelCtx.Err() != nil {
		return false
	}
	if cancelCtx != nil {
		senderDone = cancelCtx.Done()
	}
	finishCtx, cancel := context.WithTimeout(context.Background(), pipelineStreamFinishClientTimeout)
	defer cancel()
	streamID := sender.streamSender.ID()
	if err := sender.streamSender.Send(finishCtx, generatePipelineStreamFinishMessage(streamID)); err != nil {
		return false
	}
	select {
	case value, ok := <-sender.receiveCh:
		if !ok || value == nil {
			sender.markReceiveClosed()
			return false
		}
		message, ok := value.(*pipeline.Message)
		return ok && message.GetID() == streamID &&
			message.GetCmd() == pipeline.Method_PipelineStreamFinishAck &&
			message.GetSid() == pipeline.Status_MessageEnd && len(message.GetErr()) == 0 &&
			message.GetAcceptedTeardownMode() == pipeline.StreamTeardownMode_FinishAck
	case <-finishCtx.Done():
		return false
	case <-senderDone:
		return false
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

func (sender *messageSenderOnClient) dealRemoteTerminal(data []byte) error {
	sender.terminalMu.Lock()
	defer sender.terminalMu.Unlock()
	if sender.terminalSeen {
		return nil
	}
	var envelope remoteTerminalEnvelope
	if len(data) > 0 {
		if err := json.Unmarshal(data, &envelope); err != nil {
			if marker, ok := sender.warningSink.(groupConcatCutMarker); ok {
				marker.markGroupConcatReportingIncomplete()
			}
			return err
		}
	}
	if sender.proc != nil && envelope.StatementLastInsertID != 0 {
		sender.proc.SetStatementLastInsertIDIfEarlier(envelope.StatementLastInsertID)
	}
	if len(envelope.LocalScope) > 0 {
		sender.dealRemoteAnalysis(envelope.PhyPlan)
	}
	if sender.warningSink != nil && envelope.WarningCount > 0 {
		codes := make([]uint16, 0, len(envelope.WarningDiagnostics))
		messages := make([]string, 0, len(envelope.WarningDiagnostics))
		for _, warning := range envelope.WarningDiagnostics {
			codes = append(codes, warning.Code)
			messages = append(messages, warning.Message)
		}
		appendWarningBatchToSink(sender.warningSink, envelope.WarningCount, codes, messages)
	}
	if envelope.GroupConcatCut {
		if marker, ok := sender.warningSink.(groupConcatCutMarker); ok {
			marker.markGroupConcatCut(envelope.GroupConcatCutMessage)
		}
	}
	if !envelope.GroupConcatCutReported {
		if marker, ok := sender.warningSink.(groupConcatCutMarker); ok {
			marker.markGroupConcatReportingIncomplete()
		}
	}
	if sender.anal != nil && envelope.TerminalResourceVersion > 0 {
		if envelope.Allocation.GenerationCount != 0 {
			if envelope.TerminalResourceVersion <
				remoteAllocationOwnerResourceVersion {
				envelope.Delta.Quality |= resource.QualityPartial |
					resource.QualityMissingAllocationOwner
			} else if envelope.Delta.Quality&
				resource.QualityMissingAllocationOwner == 0 &&
				!envelope.Allocation.OwnerAttributionCoversTotals() {
				// A mixed-version intermediate propagates an explicit missing-
				// owner fact. Without it, absent or partial v4 attribution
				// violates the terminal protocol contract.
				envelope.Delta.Quality |= resource.QualityInvariantFailure
				envelope.Delta.Quality |= resource.QualityPartial |
					resource.QualityMissingAllocationOwner
			}
		}
		sender.anal.appendRemoteResource(
			envelope.Delta,
			envelope.Memory,
			envelope.Allocation,
			envelope.MissingFragmentCount,
			envelope.MissingMemoryDomainCount,
			envelope.PendingAllocationGroups,
			envelope.CompletedAllocationGroups,
		)
	}
	sender.terminalSeen = true
	return nil
}

func (sender *messageSenderOnClient) close() {
	sender.closeOnce.Do(func() {
		// Ensure Gauge is decremented exactly once when this sender is torn down.
		defer sender.gaugeDecOnce.Do(func() { v2.PipelineMessageSenderGauge.Dec() })

		_ = sender.waitingTheStopResponse()
		sender.markMissingGroupConcatTerminal()
		sender.stateMu.Lock()
		receiveClosed, reuseEligible := sender.receiveClosed, sender.reuseEligible
		sender.stateMu.Unlock()
		if !receiveClosed && reuseEligible && sender.finishStreamForReuse() {
			v2.PipelineStreamTeardownCounter.WithLabelValues("client_reuse").Inc()
			if sender.ctxCancel != nil {
				sender.ctxCancel()
			}
			if err := sender.streamSender.Close(false); err != nil {
				_ = sender.streamSender.Close(true)
			}
			return
		}
		if reuseEligible {
			v2.PipelineStreamTeardownCounter.WithLabelValues("client_fin_failed_close").Inc()
		} else {
			v2.PipelineStreamTeardownCounter.WithLabelValues("client_legacy_or_poisoned_close").Inc()
		}
		if sender.ctxCancel != nil {
			sender.ctxCancel()
		}
		_ = sender.streamSender.Close(true)
	})
}

func (sender *messageSenderOnClient) markMissingGroupConcatTerminal() {
	sender.stateMu.Lock()
	started := sender.reportingRequestStarted
	sender.stateMu.Unlock()
	if !started {
		return
	}
	sender.terminalMu.Lock()
	seen := sender.terminalSeen
	sender.terminalMu.Unlock()
	if !seen {
		if marker, ok := sender.warningSink.(groupConcatCutMarker); ok {
			marker.markGroupConcatReportingIncomplete()
		}
	}
}

func (sender *messageSenderOnClient) markReportingRequestStarted() {
	sender.stateMu.Lock()
	sender.reportingRequestStarted = true
	sender.stateMu.Unlock()
}

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
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/morpc"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/lockservice"
	"github.com/matrixorigin/matrixone/pkg/logservice"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/pb/pipeline"
	"github.com/matrixorigin/matrixone/pkg/perfcounter"
	qclient "github.com/matrixorigin/matrixone/pkg/queryservice/client"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/dispatch"
	"github.com/matrixorigin/matrixone/pkg/sql/models"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/udf"
	"github.com/matrixorigin/matrixone/pkg/util/debug/goroutine"
	v2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"go.uber.org/zap"
)

// CnServerMessageHandler receive and deal the message from cn-client.
//
// The message should always *pipeline.Message here.
// there are 2 types of pipeline message now.
//
//  1. notify message :
//     a message to tell the dispatch pipeline where its remote receiver are.
//     and we use this connection's write-back method to send the data.
//     or
//     a message to stop the running pipeline.
//
//  2. scope message :
//     a message contains the encoded pipeline.
//     we decoded it and run it locally.
func CnServerMessageHandler(
	ctx context.Context,
	serverAddress string,
	message morpc.Message,
	cs morpc.ClientSession,
	storageEngine engine.Engine, fileService fileservice.FileService, lockService lockservice.LockService,
	queryClient qclient.QueryClient,
	HaKeeper logservice.CNHAKeeperClient, udfService udf.Service, txnClient client.TxnClient,
	autoIncreaseCM *defines.AutoIncrCacheManager,
	messageAcquirer func() morpc.Message) (err error) {

	startTime := time.Now()
	var lifecycle *pipelineStreamLifecycle
	defer func() {
		v2.PipelineServerDurationHistogram.Observe(time.Since(startTime).Seconds())

		if e := recover(); e != nil {
			if lifecycle != nil {
				lifecycle.remove()
				lifecycle.markCleaned()
			}
			err = moerr.ConvertPanicError(ctx, e)
			getLogger(lockService.GetConfig().ServiceID).Error("panic in CnServerMessageHandler",
				zap.String("error", err.Error()))
			err = errors.Join(err, cs.Close())
		}
	}()

	// check message is valid.
	msg, ok := message.(*pipeline.Message)
	if !ok {
		logutil.Errorf("cn server should receive *pipeline.Message, but get %v", message)
		panic("cn server receive a message with unexpected type")
	}
	if msg.DebugMsg != "" {
		logutil.Infof("%s, goRoutineId=%d", msg.GetDebugMsg(), goroutine.GetRoutineId())
	}
	colexecServer := colexec.GetServer(lockService.GetConfig().ServiceID)
	if colexecServer == nil {
		return moerr.NewInternalErrorf(ctx, "colexec server is not initialized for CN %s", lockService.GetConfig().ServiceID)
	}
	if msg.GetCmd() == pipeline.Method_PipelineStreamFinish {
		return handlePipelineStreamFinish(ctx, msg, cs, messageAcquirer)
	}

	// prepare the receiver structure, just for easy using the `send` method.
	receiver := newMessageReceiverOnServer(ctx, serverAddress, msg,
		cs, messageAcquirer, storageEngine, fileService, lockService, queryClient, HaKeeper, udfService, txnClient, autoIncreaseCM, colexecServer)

	finishNegotiated := false
	if receiver.supportsFinishAck() {
		lifecycle, err = registerPipelineStreamLifecycle(receiver.clientSession, receiver.messageId)
		finishNegotiated = err == nil
		if err != nil {
			return err
		}
		receiver.acceptedTeardownMode = pipeline.StreamTeardownMode_FinishAck
	}

	// Register negotiated lifecycle ownership before execution. A locally
	// requested StopSending may make execution return an error, but its terminal
	// response and FIN still need the same cleanup barrier.
	handlerErr := handlePipelineMessage(&receiver)
	responseSent := false
	if receiver.messageTyp != pipeline.Method_StopSending {
		// stop message only close a running pipeline, there is no need to reply the finished-message.
		if handlerErr != nil {
			err = receiver.sendError(handlerErr)
		} else {
			err = receiver.sendEndMessage()
		}
		responseSent = err == nil
	} else {
		err = handlerErr
	}

	// if this message is responsible for the execution of certain pipelines, they should be ended after message processing is completed.
	if receiver.messageTyp == pipeline.Method_PipelineMessage || receiver.messageTyp == pipeline.Method_PrepareDoneNotifyMessage {
		// keep listening until connection was closed
		// to prevent some strange handle order between 'stop sending message' and others.
		// todo: it is tcp connection now. should be very careful, we should listen to stream context next day.
		if responseSent && finishNegotiated {
			finishReceived := lifecycle.waitForFinish(receiver.messageCtx, receiver.connectionCtx)
			receiver.colexecServer.RemoveRelatedPipeline(receiver.clientSession, receiver.messageId)
			lifecycle.markCleaned()
			if !finishReceived {
				_ = receiver.clientSession.Close()
			}
			return nil
		}
		if lifecycle != nil {
			lifecycle.remove()
		}
		if handlerErr == nil && err == nil {
			receiver.waitUntilDisconnectedOrCancelled()
		}
		receiver.colexecServer.RemoveRelatedPipeline(receiver.clientSession, receiver.messageId)
	}
	return err
}

// waitUntilDisconnectedOrCancelled blocks until either the client connection is
// closed or the message context is cancelled (e.g. the query was killed). It
// keeps listening so a remote dispatch can still stream data back, but no longer
// hangs forever on the TCP connection alone when the query is cancelled
// (issue #25025).
func (receiver *messageReceiverOnServer) waitUntilDisconnectedOrCancelled() {
	select {
	case <-receiver.connectionCtx.Done():
	case <-receiver.messageCtx.Done():
	}
}

func handlePipelineMessage(receiver *messageReceiverOnServer) error {

	switch receiver.messageTyp {
	case pipeline.Method_PrepareDoneNotifyMessage:
		dispatchProc, dispatchNotifyCh, err := receiver.GetProcByUuid(receiver.messageUuid)
		if err != nil {
			return err
		}
		if dispatchProc == nil || dispatchNotifyCh == nil {
			err = moerr.NewInvalidStateNoCtxf(
				"remote dispatch receiver %s attached with incomplete registration",
				receiver.messageUuid.String(),
			)
			receiver.cancelConsumedDispatchRegistration(dispatchProc, err)
			return err
		}

		infoToDispatchOperator := &process.WrapCs{
			ReceiverDone: false,
			MsgId:        receiver.messageId,
			Uid:          receiver.messageUuid,
			Cs:           receiver.clientSession,
			Err:          make(chan error, 1),
		}
		receiver.colexecServer.RecordDispatchPipeline(receiver.clientSession, receiver.messageId, infoToDispatchOperator)

		succeed := false
		select {
		case dispatchNotifyCh <- infoToDispatchOperator:
			succeed = true
		case <-contextDone(receiver.connectionCtx):
			err = moerr.NewStreamClosed(receiver.getMessageContext())
			dispatchProc.Cancel(err)
		case <-contextDone(receiver.messageCtx):
			err = remoteRegistrationContextError(receiver.messageCtx)
			dispatchProc.Cancel(err)
		case <-contextDone(dispatchProc.Ctx):
			err = remoteRegistrationContextError(dispatchProc.Ctx)
		}

		if !succeed {
			return err
		}

		select {
		case <-contextDone(receiver.connectionCtx):
			err = moerr.NewStreamClosed(receiver.getMessageContext())
			dispatchProc.Cancel(err)
		case <-contextDone(receiver.messageCtx):
			err = remoteRegistrationContextError(receiver.messageCtx)
			dispatchProc.Cancel(err)

		// there is no need to check the dispatchProc.Ctx.Done() here.
		// because we need to receive the error from dispatchProc.DispatchNotifyCh.
		case err = <-infoToDispatchOperator.Err:
		}
		return err

	case pipeline.Method_PipelineMessage:
		runCompile, errBuildCompile := receiver.newCompile()
		if errBuildCompile != nil {
			return errBuildCompile
		}
		defer func() {
			runCompile.clear()
			runCompile.Release()
		}()

		// decode and running the pipeline.
		s, err := decodeScope(receiver.scopeData, runCompile.proc, true, runCompile.e)
		if err != nil {
			return err
		}
		if !receiver.needNotReply {
			s = appendWriteBackOperator(runCompile, s)
		}

		// For remote run, workspace needs to be created early before any code
		// that might access it (e.g., operator Prepare methods, InitPipelineContextToExecuteQuery).
		// Previously, workspace was only created in handleDbRelContext which is called
		// later in MergeRun, causing potential nil pointer panics.
		if runCompile.proc.GetTxnOperator() != nil && runCompile.proc.GetTxnOperator().GetWorkspace() == nil {
			if disttaeEngine, ok := runCompile.e.(*disttae.Engine); ok {
				ws := disttae.NewTxnWorkSpace(disttaeEngine, runCompile.proc)
				runCompile.proc.GetTxnOperator().AddWorkspace(ws)
				ws.BindTxnOp(runCompile.proc.GetTxnOperator())
			}
		}

		runCompile.scopes = []*Scope{s}
		runCompile.InitPipelineContextToExecuteQuery()

		registrations, err := registerRemoteDispatchReceivers(s)
		if err != nil {
			return err
		}
		defer registrations.cleanup()
		receiver.colexecServer.RecordBuiltPipeline(receiver.clientSession, receiver.messageId, runCompile.proc)

		// running pipeline.
		if err = TryMarkQueryRunning(runCompile, runCompile.proc.GetTxnOperator()); err != nil {
			return err
		}
		defer func() {
			MarkQueryDone(runCompile, runCompile.proc.GetTxnOperator())
		}()

		err = s.MergeRun(runCompile)
		if err == nil {
			runCompile.GenPhyPlan(runCompile)
			receiver.phyPlan = runCompile.anal.GetPhyPlan()
		}
		return err

	case pipeline.Method_StopSending:
		receiver.colexecServer.CancelPipelineSending(receiver.clientSession, receiver.messageId)

	default:
		panic(fmt.Sprintf("unknown pipeline message type %d.", receiver.messageTyp))
	}
	return nil
}

type remoteDispatchReceiverRegistrations struct {
	once          sync.Once
	registrations []*dispatch.RemoteReceiverRegistration
}

func (r *remoteDispatchReceiverRegistrations) cleanup() {
	if r == nil {
		return
	}
	r.once.Do(func() {
		for i := len(r.registrations) - 1; i >= 0; i-- {
			r.registrations[i].Cleanup()
		}
	})
}

func (r *remoteDispatchReceiverRegistrations) rollback(err error) {
	if r == nil {
		return
	}
	for i := range r.registrations {
		registration := r.registrations[i]
		if registration != nil {
			registration.Cancel(err)
		}
	}
	r.cleanup()
}

func registerRemoteDispatchReceivers(s *Scope) (*remoteDispatchReceiverRegistrations, error) {
	return registerDispatchReceivers([]*Scope{s}, func(*Scope) dispatchReceiverRegistrationMode {
		return registerDispatchReceiverTree
	})
}

func registerLocalDispatchReceivers(scopes []*Scope, localAddr string) (*remoteDispatchReceiverRegistrations, error) {
	return registerDispatchReceivers(scopes, func(s *Scope) dispatchReceiverRegistrationMode {
		if s.Magic != Remote || s.ipAddrMatch(localAddr) {
			return registerDispatchReceiverTree
		}
		if validateRemoteRunAddress(s.NodeInfo.Addr, localAddr) != nil || s.holdAnyCannotRemoteOperator() != nil {
			return skipDispatchReceivers
		}
		if !checkPipelineStandaloneExecutableAtRemote(s) {
			return registerDispatchReceiverTree
		}
		if _, ok := s.RootOp.(*dispatch.Dispatch); ok {
			return registerDispatchReceiverRoot
		}
		// The tree runs on another CN, so its own operators must not be
		// registered here. Its descendants can still RemoteRun back to this CN;
		// visit them to publish those local return dispatches early.
		return traverseDispatchReceiverChildren
	})
}

type dispatchReceiverRegistrationMode uint8

const (
	skipDispatchReceivers dispatchReceiverRegistrationMode = iota
	registerDispatchReceiverRoot
	registerDispatchReceiverTree
	traverseDispatchReceiverChildren
)

func registerDispatchReceivers(
	scopes []*Scope,
	registrationMode func(*Scope) dispatchReceiverRegistrationMode,
) (*remoteDispatchReceiverRegistrations, error) {
	registrations := &remoteDispatchReceiverRegistrations{}
	visitedScopes := make(map[*Scope]struct{})
	visitedDispatches := make(map[*dispatch.Dispatch]struct{})

	var registerScope func(*Scope) error
	registerScope = func(s *Scope) error {
		if s == nil {
			return nil
		}
		if _, ok := visitedScopes[s]; ok {
			return nil
		}
		visitedScopes[s] = struct{}{}
		mode := registrationMode(s)
		if mode == skipDispatchReceivers {
			return nil
		}

		registerDispatch := func(d *dispatch.Dispatch) error {
			if d == nil || len(d.RemoteRegs) == 0 {
				return nil
			}
			if _, ok := visitedDispatches[d]; ok {
				return nil
			}
			visitedDispatches[d] = struct{}{}
			if s.Proc == nil {
				return moerr.NewInternalErrorNoCtx("cannot register remote receiver without its scope process")
			}
			registration, err := d.RegisterRemoteReceiversWithHandle(s.Proc)
			if err != nil {
				return err
			}
			if registration != nil {
				registrations.registrations = append(registrations.registrations, registration)
			}
			return nil
		}

		if mode == registerDispatchReceiverRoot {
			if err := registerDispatch(s.RootOp.(*dispatch.Dispatch)); err != nil {
				return err
			}
		} else if mode == registerDispatchReceiverTree && s.RootOp != nil {
			if err := vm.HandleAllOp(s.RootOp, func(_ vm.Operator, op vm.Operator) error {
				d, ok := op.(*dispatch.Dispatch)
				if !ok {
					return nil
				}
				return registerDispatch(d)
			}); err != nil {
				return err
			}
		}
		for _, pre := range s.PreScopes {
			if err := registerScope(pre); err != nil {
				return err
			}
		}
		return nil
	}

	for _, s := range scopes {
		if err := registerScope(s); err != nil {
			cancelScopeProcesses(scopes, err)
			registrations.rollback(err)
			return nil, err
		}
	}
	return registrations, nil
}

func cancelScopeProcesses(scopes []*Scope, err error) {
	visited := make(map[*Scope]struct{})
	var cancelScope func(*Scope)
	cancelScope = func(s *Scope) {
		if s == nil {
			return
		}
		if _, ok := visited[s]; ok {
			return
		}
		visited[s] = struct{}{}
		if s.Proc != nil && s.Proc.Cancel != nil {
			s.Proc.Cancel(err)
		}
		for _, pre := range s.PreScopes {
			cancelScope(pre)
		}
	}
	for _, s := range scopes {
		cancelScope(s)
	}
}

const (
	maxMessageSizeToMoRpc = 64 * mpool.MB
)

// message receiver's cn information.
type cnInformation struct {
	cnAddr      string
	storeEngine engine.Engine
	fileService fileservice.FileService
	lockService lockservice.LockService
	queryClient qclient.QueryClient
	hakeeper    logservice.CNHAKeeperClient
	udfService  udf.Service
	aicm        *defines.AutoIncrCacheManager
}

// information to rebuild a process.
type processHelper struct {
	id          string
	lim         process.Limitation
	unixTime    int64
	accountId   uint32
	txnOperator client.TxnOperator
	txnClient   client.TxnClient
	sessionInfo process.SessionInfo
	//analysisNodeList []int32
	StmtId        uuid.UUID
	prepareParams *vector.Vector
}

// messageReceiverOnServer supported a series methods to write back results.
type messageReceiverOnServer struct {
	messageCtx    context.Context
	connectionCtx context.Context

	messageId   uint64
	messageTyp  pipeline.Method
	messageUuid uuid.UUID

	cnInformation cnInformation
	// information to build a process.
	procBuildHelper processHelper

	clientSession   morpc.ClientSession
	messageAcquirer func() morpc.Message
	maxMessageSize  int
	scopeData       []byte

	needNotReply bool

	requestedTeardownMode pipeline.StreamTeardownMode
	acceptedTeardownMode  pipeline.StreamTeardownMode

	colexecServer *colexec.Server

	// result.
	phyPlan *models.PhyPlan
}

func newMessageReceiverOnServer(
	ctx context.Context,
	cnAddr string,
	m *pipeline.Message,
	cs morpc.ClientSession,
	messageAcquirer func() morpc.Message,
	storeEngine engine.Engine,
	fileService fileservice.FileService,
	lockService lockservice.LockService,
	queryClient qclient.QueryClient,
	hakeeper logservice.CNHAKeeperClient,
	udfService udf.Service,
	txnClient client.TxnClient,
	aicm *defines.AutoIncrCacheManager,
	colexecServer *colexec.Server) messageReceiverOnServer {

	receiver := messageReceiverOnServer{
		messageCtx:            ctx,
		connectionCtx:         cs.SessionCtx(),
		messageId:             m.GetId(),
		messageTyp:            m.GetCmd(),
		clientSession:         cs,
		messageAcquirer:       messageAcquirer,
		maxMessageSize:        maxMessageSizeToMoRpc,
		needNotReply:          m.NeedNotReply,
		requestedTeardownMode: m.GetRequestedTeardownMode(),
		colexecServer:         colexecServer,
	}
	receiver.cnInformation = cnInformation{
		cnAddr:      cnAddr,
		storeEngine: storeEngine,
		fileService: fileService,
		lockService: lockService,
		queryClient: queryClient,
		hakeeper:    hakeeper,
		udfService:  udfService,
		aicm:        aicm,
	}

	switch m.GetCmd() {
	case pipeline.Method_PrepareDoneNotifyMessage:
		opUuid, err := uuid.FromBytes(m.GetUuid())
		if err != nil {
			logutil.Errorf("decode uuid from pipeline.Message failed, bytes are %v", m.GetUuid())
			panic("cn receive a message with wrong uuid bytes")
		}
		receiver.messageUuid = opUuid

	case pipeline.Method_PipelineMessage:
		var err error
		receiver.procBuildHelper, err = generateProcessHelper(m.GetProcInfoData(), txnClient)
		if err != nil {
			logutil.Errorf("decode process info from pipeline.Message failed, bytes are %v", m.GetProcInfoData())
			panic("cn receive a message with wrong process bytes")
		}
		receiver.scopeData = m.Data
	}

	return receiver
}

func (receiver *messageReceiverOnServer) supportsFinishAck() bool {
	if receiver.requestedTeardownMode != pipeline.StreamTeardownMode_FinishAck {
		return false
	}
	return receiver.messageTyp == pipeline.Method_PipelineMessage ||
		receiver.messageTyp == pipeline.Method_PrepareDoneNotifyMessage
}

func (receiver *messageReceiverOnServer) acquireMessage() (*pipeline.Message, error) {
	message, ok := receiver.messageAcquirer().(*pipeline.Message)
	if !ok {
		return nil, moerr.NewInternalError(receiver.messageCtx, "get a message with wrong type.")
	}
	message.SetID(receiver.messageId)
	return message, nil
}

// newCompile make and return a new compile to run a pipeline.
func (receiver *messageReceiverOnServer) newCompile() (*Compile, error) {
	// compile is almost surely wanting a small or middle pool.  Later.
	mp, err := mpool.NewMPool("compile", 0, mpool.NoFixed)
	if err != nil {
		return nil, err
	}
	pHelper, cnInfo := receiver.procBuildHelper, receiver.cnInformation

	// required deadline.
	runningCtx := defines.AttachAccountId(receiver.messageCtx, pHelper.accountId)
	proc := process.NewTopProcess(
		runningCtx,
		mp,
		pHelper.txnClient,
		pHelper.txnOperator,
		cnInfo.fileService,
		cnInfo.lockService,
		cnInfo.queryClient,
		cnInfo.hakeeper,
		cnInfo.udfService,
		cnInfo.aicm,
		nil)
	proc.Base.UnixTime = pHelper.unixTime
	proc.Base.Id = pHelper.id
	proc.Base.Lim = pHelper.lim
	proc.Base.SessionInfo = pHelper.sessionInfo
	proc.Base.SessionInfo.StorageEngine = cnInfo.storeEngine
	proc.SetPrepareParams(pHelper.prepareParams)
	{
		txn := proc.GetTxnOperator().Txn()
		txnId := txn.GetID()
		proc.Base.StmtProfile = process.NewStmtProfile(uuid.UUID(txnId), pHelper.StmtId)
	}

	c := allocateNewCompile(proc)
	c.execType = plan2.ExecTypeAP_MULTICN
	c.e = cnInfo.storeEngine
	c.MessageBoard = c.MessageBoard.SetMultiCN(c.GetMessageCenter(), c.proc.GetStmtProfile().GetStmtId())
	c.proc.SetMessageBoard(c.MessageBoard)
	c.anal = newAnalyzeModule()
	c.addr = receiver.cnInformation.cnAddr

	// a method to send back.
	c.execType = plan2.ExecTypeAP_MULTICN
	c.fill = func(b *batch.Batch, counter *perfcounter.CounterSet) error {
		return receiver.sendBatch(b)
	}

	return c, nil
}

func (receiver *messageReceiverOnServer) sendError(
	errInfo error) error {
	message, err := receiver.acquireMessage()
	if err != nil {
		return err
	}
	message.SetID(receiver.messageId)
	message.SetSid(pipeline.Status_MessageEnd)
	message.SetMessageType(receiver.messageTyp)
	message.AcceptedTeardownMode = receiver.acceptedTeardownMode
	if errInfo != nil {
		message.SetMoError(receiver.messageCtx, errInfo)
	}
	return receiver.clientSession.Write(receiver.messageCtx, message)
}

func (receiver *messageReceiverOnServer) sendBatch(
	b *batch.Batch) error {
	// there's no need to send the nil batch.
	if b == nil {
		return nil
	}

	data, err := b.MarshalBinary()
	if err != nil {
		return err
	}

	dataLen := len(data)
	if dataLen <= receiver.maxMessageSize {
		m, errA := receiver.acquireMessage()
		if errA != nil {
			return errA
		}
		m.SetMessageType(pipeline.Method_BatchMessage)
		m.SetData(data)
		m.SetSid(pipeline.Status_Last)
		return receiver.clientSession.Write(receiver.messageCtx, m)
	}
	// if data is too large, cut and send
	for start, end := 0, 0; start < dataLen; start = end {
		m, errA := receiver.acquireMessage()
		if errA != nil {
			return errA
		}
		end = start + receiver.maxMessageSize
		if end >= dataLen {
			end = dataLen
			m.SetSid(pipeline.Status_Last)
		} else {
			m.SetSid(pipeline.Status_WaitingNext)
		}
		m.SetMessageType(pipeline.Method_BatchMessage)
		m.SetData(data[start:end])

		if errW := receiver.clientSession.Write(receiver.messageCtx, m); errW != nil {
			return errW
		}
	}
	return nil
}

func (receiver *messageReceiverOnServer) sendEndMessage() error {
	message, err := receiver.acquireMessage()
	if err != nil {
		return err
	}
	message.SetSid(pipeline.Status_MessageEnd)
	message.SetID(receiver.messageId)
	message.SetMessageType(receiver.messageTyp)
	message.AcceptedTeardownMode = receiver.acceptedTeardownMode

	jsonData, err := json.MarshalIndent(receiver.phyPlan, "", "  ")
	if err != nil {
		return err
	}
	message.SetAnalysis(jsonData)

	return receiver.clientSession.Write(receiver.messageCtx, message)
}

func generateProcessHelper(data []byte, cli client.TxnClient) (processHelper, error) {
	procInfo := &pipeline.ProcessInfo{}
	err := procInfo.Unmarshal(data)
	if err != nil {
		return processHelper{}, err
	}

	result := processHelper{
		id:        procInfo.Id,
		lim:       process.ConvertToProcessLimitation(procInfo.Lim),
		unixTime:  procInfo.UnixTime,
		accountId: procInfo.AccountId,
		txnClient: cli,
	}
	if procInfo.PrepareParams.Length > 0 {
		result.prepareParams = vector.NewVecWithData(
			types.T_text.ToType(),
			int(procInfo.PrepareParams.Length),
			procInfo.PrepareParams.Data,
			procInfo.PrepareParams.Area,
		)
		for i := range procInfo.PrepareParams.Nulls {
			if procInfo.PrepareParams.Nulls[i] {
				result.prepareParams.GetNulls().Add(uint64(i))
			}
		}
	}
	result.txnOperator, err = cli.NewWithSnapshot(procInfo.Snapshot)
	if err != nil {
		return processHelper{}, err
	}
	result.sessionInfo, err = process.ConvertToProcessSessionInfo(procInfo.SessionInfo)
	if err != nil {
		return processHelper{}, err
	}
	if sessLogger := procInfo.SessionLogger; len(sessLogger.SessId) > 0 {
		copy(result.sessionInfo.SessionId[:], sessLogger.SessId)
		copy(result.StmtId[:], sessLogger.StmtId)
		result.sessionInfo.LogLevel = process.EnumLogLevel2ZapLogLevel(sessLogger.LogLevel)
		// txnId, ignore. more in txnOperator.
	}

	return result, nil
}

func newRemoteDispatchNotRegisteredYetError(_ context.Context, uid uuid.UUID) error {
	// Registration lag is an expected, retryable protocol state. Keep the
	// typed wire error without reporting every receiver attempt as an ERROR.
	return moerr.NewRemoteDispatchNotRegistered(moerr.NoReportContext(), uid.String())
}

func isRemoteDispatchNotRegisteredYetError(err error) bool {
	return moerr.IsMoErrCode(err, moerr.ErrRemoteDispatchNotRegistered)
}

var remoteDispatchRegistrationTimeout = colexec.RemoteReceiverRegistrationTimeout

func newRemoteDispatchRegistrationTimeoutError(uid uuid.UUID, timeout time.Duration) error {
	return moerr.NewInternalErrorNoCtxf(
		"remote dispatch receiver %s was not registered within %s",
		uid.String(),
		timeout,
	)
}

func (receiver *messageReceiverOnServer) TryGetProcByUuid(uid uuid.UUID) (*process.Process, process.RemotePipelineInformationChannel, error) {
	dispatchProc, notifyChannel, state, waiter := receiver.colexecServer.AttachProcByUuidOrWait(uid)
	waiter.Close()
	switch state {
	case colexec.RemoteReceiverAttachedNow:
		return dispatchProc, notifyChannel, nil
	case colexec.RemoteReceiverAlreadyAttached:
		return nil, nil, moerr.NewInvalidStateNoCtxf(
			"remote dispatch receiver %s is already attached", uid.String())
	case colexec.RemoteReceiverAlreadyClosed:
		return nil, nil, moerr.NewInvalidStateNoCtxf(
			"remote dispatch receiver %s is already closed", uid.String())
	}
	// A PrepareDoneNotify stream is one retry attempt. Its connection can be
	// closed after a NotRegistered response, so it must not own or tombstone the
	// query-wide receiver UUID while registration is still pending.
	return nil, nil, newRemoteDispatchNotRegisteredYetError(receiver.getMessageContext(), uid)
}

func (receiver *messageReceiverOnServer) GetProcByUuid(uid uuid.UUID) (*process.Process, process.RemotePipelineInformationChannel, error) {
	connectionDone := contextDone(receiver.connectionCtx)
	messageDone := contextDone(receiver.messageCtx)
	start := time.Now()
	timeout := remoteDispatchRegistrationTimeout
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	var publishedWaiter *colexec.RemoteReceiverWaiter
	releasePublishedWaiter := func() {
		if publishedWaiter != nil {
			publishedWaiter.Close()
			publishedWaiter = nil
		}
	}
	handleAttachState := func(
		dispatchProc *process.Process,
		notifyChannel process.RemotePipelineInformationChannel,
		state colexec.RemoteReceiverAttachState,
	) (
		*process.Process,
		process.RemotePipelineInformationChannel,
		bool,
		error,
	) {
		switch state {
		case colexec.RemoteReceiverAttachedNow:
			releasePublishedWaiter()
			select {
			case <-connectionDone:
				err := moerr.NewStreamClosed(receiver.getMessageContext())
				receiver.cancelConsumedDispatchRegistration(dispatchProc, err)
				v2.PipelineRemoteReceiverWaitConnectionClosedHistogram.Observe(time.Since(start).Seconds())
				return nil, nil, true, err
			case <-messageDone:
				err := remoteRegistrationContextError(receiver.getMessageContext())
				receiver.cancelConsumedDispatchRegistration(dispatchProc, err)
				v2.PipelineRemoteReceiverWaitMessageCanceledHistogram.Observe(time.Since(start).Seconds())
				return nil, nil, true, err
			default:
			}
			v2.PipelineRemoteReceiverWaitReadyHistogram.Observe(time.Since(start).Seconds())
			return dispatchProc, notifyChannel, true, nil
		case colexec.RemoteReceiverAlreadyAttached:
			releasePublishedWaiter()
			v2.PipelineRemoteReceiverWaitAlreadyAttachedHistogram.Observe(time.Since(start).Seconds())
			return nil, nil, true, moerr.NewInvalidStateNoCtxf(
				"remote dispatch receiver %s is already attached", uid.String())
		case colexec.RemoteReceiverAlreadyClosed:
			releasePublishedWaiter()
			v2.PipelineRemoteReceiverWaitAlreadyClosedHistogram.Observe(time.Since(start).Seconds())
			return nil, nil, true, moerr.NewInvalidStateNoCtxf(
				"remote dispatch receiver %s is already closed", uid.String())
		default:
			return nil, nil, false, nil
		}
	}
	for {
		select {
		case <-connectionDone:
			releasePublishedWaiter()
			v2.PipelineRemoteReceiverWaitConnectionClosedHistogram.Observe(time.Since(start).Seconds())
			return nil, nil, moerr.NewStreamClosed(receiver.getMessageContext())
		case <-messageDone:
			releasePublishedWaiter()
			v2.PipelineRemoteReceiverWaitMessageCanceledHistogram.Observe(time.Since(start).Seconds())
			return nil, nil, remoteRegistrationContextError(receiver.getMessageContext())
		default:
		}

		dispatchProc, notifyChannel, state, waiter := receiver.colexecServer.AttachProcByUuidOrWait(uid)
		if proc, ch, done, err := handleAttachState(dispatchProc, notifyChannel, state); done {
			waiter.Close()
			return proc, ch, err
		}
		if publishedWaiter != nil {
			waiter.Close()
			releasePublishedWaiter()
			return nil, nil, moerr.NewInvalidStateNoCtxf(
				"remote dispatch receiver %s disappeared after publication", uid.String())
		}

		select {
		case <-connectionDone:
			waiter.Close()
			v2.PipelineRemoteReceiverWaitConnectionClosedHistogram.Observe(time.Since(start).Seconds())
			return nil, nil, moerr.NewStreamClosed(receiver.getMessageContext())
		case <-messageDone:
			waiter.Close()
			v2.PipelineRemoteReceiverWaitMessageCanceledHistogram.Observe(time.Since(start).Seconds())
			return nil, nil, remoteRegistrationContextError(receiver.getMessageContext())
		case <-deadline.C:
			// Prefer a receiver published at the timeout boundary. The registry
			// lock is the ownership linearization point; keep the current waiter
			// alive during the final lookup so a just-closed published receiver
			// retains its terminal state until we observe it.
			dispatchProc, notifyChannel, state, finalWaiter :=
				receiver.colexecServer.AttachProcByUuidOrWait(uid)
			finalWaiter.Close()
			if proc, ch, done, err := handleAttachState(dispatchProc, notifyChannel, state); done {
				waiter.Close()
				return proc, ch, err
			}
			waiter.Close()
			releasePublishedWaiter()
			v2.PipelineRemoteReceiverWaitTimeoutHistogram.Observe(time.Since(start).Seconds())
			return nil, nil, newRemoteDispatchRegistrationTimeoutError(
				uid,
				timeout,
			)
		case <-waiter.Done():
			publishedWaiter = waiter
		}
	}
}

func contextDone(ctx context.Context) <-chan struct{} {
	if ctx == nil {
		return nil
	}
	return ctx.Done()
}

func remoteRegistrationContextError(ctx context.Context) error {
	if ctx == nil {
		return context.Canceled
	}
	if err := context.Cause(ctx); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return context.Canceled
}

func (receiver *messageReceiverOnServer) getMessageContext() context.Context {
	if receiver.messageCtx != nil {
		return receiver.messageCtx
	}
	return context.Background()
}

func (receiver *messageReceiverOnServer) cancelConsumedDispatchRegistration(
	dispatchProc *process.Process,
	err error,
) {
	if dispatchProc != nil && dispatchProc.Cancel != nil {
		dispatchProc.Cancel(err)
	}
}

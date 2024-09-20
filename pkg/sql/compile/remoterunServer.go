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
	"runtime"
	"time"

	"github.com/google/uuid"
	"go.uber.org/zap"

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
	qclient "github.com/matrixorigin/matrixone/pkg/queryservice/client"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/models"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/udf"
	v2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
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
	defer func() {
		v2.PipelineServerDurationHistogram.Observe(time.Since(startTime).Seconds())

		if e := recover(); e != nil {
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

	// prepare the receiver structure, just for easy using the `send` method.
	receiver := newMessageReceiverOnServer(ctx, serverAddress, msg,
		cs, messageAcquirer, storageEngine, fileService, lockService, queryClient, HaKeeper, udfService, txnClient, autoIncreaseCM)

	// how to handle the *pipeline.Message.
	if receiver.needNotReply {
		err = handlePipelineMessage(&receiver)
	} else {
		if err = handlePipelineMessage(&receiver); err != nil {
			err = receiver.sendError(err)
		} else {
			err = receiver.sendEndMessage()
		}
	}

	// if this message is responsible for the execution of certain pipelines, they should be ended after message processing is completed.
	if receiver.messageTyp == pipeline.Method_PipelineMessage || receiver.messageTyp == pipeline.Method_PrepareDoneNotifyMessage {
		// keep listening until connection was closed
		// to prevent some strange handle order between 'stop sending message' and others.
		// todo: it is tcp connection now. should be very careful, we should listen to stream context next day.
		if err == nil {
			<-receiver.connectionCtx.Done()
		}
		colexec.Get().RemoveRelatedPipeline(receiver.clientSession, receiver.messageId)
	}
	return err
}

func handlePipelineMessage(receiver *messageReceiverOnServer) error {

	switch receiver.messageTyp {
	case pipeline.Method_PrepareDoneNotifyMessage:
		dispatchProc, dispatchNotifyCh, err := receiver.GetProcByUuid(receiver.messageUuid)
		if err != nil || dispatchProc == nil {
			return err
		}

		infoToDispatchOperator := &process.WrapCs{
			ReceiverDone: false,
			MsgId:        receiver.messageId,
			Uid:          receiver.messageUuid,
			Cs:           receiver.clientSession,
			Err:          make(chan error, 1),
		}
		colexec.Get().RecordDispatchPipeline(receiver.clientSession, receiver.messageId, infoToDispatchOperator)

		// todo : the timeout should be removed.
		//		but I keep it here because I don't know whether it will cause hung sometimes.
		timeLimit, cancel := context.WithTimeout(context.TODO(), HandleNotifyTimeout)

		succeed := false
		select {
		case <-timeLimit.Done():
			err = moerr.NewInternalError(receiver.messageCtx, "send notify msg to dispatch operator timeout")
		case dispatchNotifyCh <- infoToDispatchOperator:
			succeed = true
		case <-receiver.connectionCtx.Done():
		case <-dispatchProc.Ctx.Done():
		}
		cancel()

		if err != nil || !succeed {
			dispatchProc.Cancel()
			return err
		}

		select {
		case <-receiver.connectionCtx.Done():
			dispatchProc.Cancel()

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

		// decode and running the pipeline.
		s, err := decodeScope(receiver.scopeData, runCompile.proc, true, runCompile.e)
		if err != nil {
			return err
		}
		s = appendWriteBackOperator(runCompile, s)

		runCompile.scopes = []*Scope{s}
		runCompile.InitPipelineContextToExecuteQuery()
		defer func() {
			runCompile.clear()
			runCompile.Release()
		}()

		colexec.Get().RecordBuiltPipeline(receiver.clientSession, receiver.messageId, runCompile.proc)

		// running pipeline.
		if err = GetCompileService().recordRunningCompile(runCompile); err != nil {
			return err
		}
		defer func() {
			_, _ = GetCompileService().removeRunningCompile(runCompile)
		}()

		err = s.MergeRun(runCompile)
		if err == nil {
			runCompile.GenPhyPlan(runCompile)
			receiver.phyPlan = runCompile.anal.GetPhyPlan()
		}
		return err

	case pipeline.Method_StopSending:
		colexec.Get().CancelPipelineSending(receiver.clientSession, receiver.messageId)

	default:
		panic(fmt.Sprintf("unknown pipeline message type %d.", receiver.messageTyp))
	}
	return nil
}

const (
	maxMessageSizeToMoRpc = 64 * mpool.MB

	// HandleNotifyTimeout
	// todo: this is a bad design here.
	//  we should do the waiting work in the prepare stage of the dispatch operator but not in the exec stage.
	//      do the waiting work in the exec stage can save some execution time, but it will cause an unstable waiting time.
	//		(because we cannot control the execution time of the running sql,
	//		and the coming time of the first batch of the result is not a constant time.)
	// 		see the codes in pkg/sql/colexec/dispatch/dispatch.go:waitRemoteRegsReady()
	//
	// need to fix this in the future. for now, increase it to make tpch1T can run on 3 CN
	HandleNotifyTimeout = 300 * time.Second
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
	aicm *defines.AutoIncrCacheManager) messageReceiverOnServer {

	receiver := messageReceiverOnServer{
		messageCtx:      ctx,
		connectionCtx:   cs.SessionCtx(),
		messageId:       m.GetId(),
		messageTyp:      m.GetCmd(),
		clientSession:   cs,
		messageAcquirer: messageAcquirer,
		maxMessageSize:  maxMessageSizeToMoRpc,
		needNotReply:    m.NeedNotReply,
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
		cnInfo.aicm)
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

	c := GetCompileService().getCompile(proc)
	c.execType = plan2.ExecTypeAP_MULTICN
	c.e = cnInfo.storeEngine
	c.MessageBoard = c.MessageBoard.SetMultiCN(c.GetMessageCenter(), c.proc.GetStmtProfile().GetStmtId())
	c.proc.SetMessageBoard(c.MessageBoard)
	c.anal = newAnalyzeModule()
	c.addr = receiver.cnInformation.cnAddr

	// a method to send back.
	c.execType = plan2.ExecTypeAP_MULTICN
	c.fill = func(b *batch.Batch) error {
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

func (receiver *messageReceiverOnServer) GetProcByUuid(uid uuid.UUID) (*process.Process, process.RemotePipelineInformationChannel, error) {
	tout, tcancel := context.WithTimeout(context.Background(), HandleNotifyTimeout)

	for {
		select {
		case <-tout.Done():
			colexec.Get().GetProcByUuid(uid, true)
			tcancel()
			return nil, nil, moerr.NewInternalError(receiver.messageCtx, "get dispatch process by uuid timeout")

		case <-receiver.connectionCtx.Done():
			colexec.Get().GetProcByUuid(uid, true)
			tcancel()
			return nil, nil, nil

		default:
			dispatchProc, ok := colexec.Get().GetProcByUuid(uid, true)
			if ok {
				tcancel()
				return dispatchProc, dispatchProc.DispatchNotifyCh, nil
			}

			// it's very bad to call the runtime.Gosched() here.
			// get a process receive channel first, and listen to it may be a better way.
			runtime.Gosched()
		}
	}
}

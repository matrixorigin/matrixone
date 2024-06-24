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
	"errors"
	"fmt"
	"github.com/google/uuid"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/morpc"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/common/reuse"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/lockservice"
	"github.com/matrixorigin/matrixone/pkg/logservice"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/pb/pipeline"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/perfcounter"
	qclient "github.com/matrixorigin/matrixone/pkg/queryservice/client"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/udf"
	v2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"go.uber.org/zap"
	"runtime"
	"time"
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
			getLogger().Error("panic in CnServerMessageHandler",
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
		if err == nil {
			<-ctx.Done()
		}
		colexec.Get().RemoveRunningPipeline(receiver.clientSession, receiver.messageId)
	}
	return err
}

func handlePipelineMessage(receiver *messageReceiverOnServer) error {

	switch receiver.messageTyp {
	case pipeline.Method_PrepareDoneNotifyMessage:
		dispatchProc, err := receiver.GetProcByUuid(receiver.messageUuid)
		if err != nil || dispatchProc == nil {
			return err
		}
		if cancel := colexec.Get().RecordRunningPipeline(receiver.clientSession, receiver.messageId, dispatchProc); cancel {
			dispatchProc.Cancel()
			return nil
		}

		infoToDispatchOperator := process.WrapCs{
			MsgId: receiver.messageId,
			Uid:   receiver.messageUuid,
			Cs:    receiver.clientSession,
			Err:   make(chan error, 1),
		}

		// todo : the timeout should be removed.
		//		but I keep it here because I don't know whether it will cause hung sometimes.
		timeLimit, cancel := context.WithTimeout(context.TODO(), HandleNotifyTimeout)

		succeed := false
		select {
		case <-timeLimit.Done():
			err = moerr.NewInternalError(receiver.ctx, "send notify msg to dispatch operator timeout")
		case dispatchProc.DispatchNotifyCh <- infoToDispatchOperator:
			succeed = true
		case <-receiver.ctx.Done():
		case <-dispatchProc.Ctx.Done():
		}
		cancel()

		if err != nil || !succeed {
			dispatchProc.Cancel()
			return err
		}

		select {
		case <-receiver.ctx.Done():
			dispatchProc.Cancel()

		// there is no need to check the dispatchProc.Ctx.Done() here.
		// because we need to receive the error from dispatchProc.DispatchNotifyCh.
		case err = <-infoToDispatchOperator.Err:
		}
		return err

	case pipeline.Method_PipelineMessage:
		runCompile := receiver.newCompile()
		if cancel := colexec.Get().RecordRunningPipeline(receiver.clientSession, receiver.messageId, runCompile.proc); cancel {
			runCompile.proc.Cancel()
			return nil
		}

		// decode and running the pipeline.
		s, err := decodeScope(receiver.scopeData, runCompile.proc, true, runCompile.e)
		if err != nil {
			return err
		}
		s = appendWriteBackOperator(runCompile, s)
		s.SetContextRecursively(runCompile.ctx)

		defer func() {
			runCompile.proc.FreeVectors()
			runCompile.proc.CleanValueScanBatchs()
			runCompile.proc.AnalInfos = nil
			runCompile.anal.analInfos = nil

			runCompile.Release()
			s.release()
		}()
		err = s.ParallelRun(runCompile)
		if err == nil {
			// record the number of s3 requests
			runCompile.proc.AnalInfos[runCompile.anal.curr].S3IOInputCount += runCompile.counterSet.FileService.S3.Put.Load()
			runCompile.proc.AnalInfos[runCompile.anal.curr].S3IOInputCount += runCompile.counterSet.FileService.S3.List.Load()
			runCompile.proc.AnalInfos[runCompile.anal.curr].S3IOOutputCount += runCompile.counterSet.FileService.S3.Head.Load()
			runCompile.proc.AnalInfos[runCompile.anal.curr].S3IOOutputCount += runCompile.counterSet.FileService.S3.Get.Load()
			runCompile.proc.AnalInfos[runCompile.anal.curr].S3IOOutputCount += runCompile.counterSet.FileService.S3.Delete.Load()
			runCompile.proc.AnalInfos[runCompile.anal.curr].S3IOOutputCount += runCompile.counterSet.FileService.S3.DeleteMulti.Load()

			receiver.finalAnalysisInfo = runCompile.proc.AnalInfos
		} else {
			// there are 3 situations to release analyzeInfo
			// 1 is free analyzeInfo of Local CN when release analyze
			// 2 is free analyzeInfo of remote CN before transfer back
			// 3 is free analyzeInfo of remote CN when errors happen before transfer back
			// this is situation 3
			for i := range runCompile.proc.AnalInfos {
				reuse.Free[process.AnalyzeInfo](runCompile.proc.AnalInfos[i], nil)
			}
		}
		return err

	case pipeline.Method_StopSending:
		colexec.Get().CancelRunningPipeline(receiver.clientSession, receiver.messageId)

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
	id               string
	lim              process.Limitation
	unixTime         int64
	accountId        uint32
	txnOperator      client.TxnOperator
	txnClient        client.TxnClient
	sessionInfo      process.SessionInfo
	analysisNodeList []int32
	StmtId           uuid.UUID
}

// messageReceiverOnServer supported a series methods to write back results.
type messageReceiverOnServer struct {
	ctx         context.Context
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
	finalAnalysisInfo []*process.AnalyzeInfo
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
		ctx:             ctx,
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
		return nil, moerr.NewInternalError(receiver.ctx, "get a message with wrong type.")
	}
	message.SetID(receiver.messageId)
	return message, nil
}

// newCompile make and return a new compile to run a pipeline.
func (receiver *messageReceiverOnServer) newCompile() *Compile {
	// compile is almost surely wanting a small or middle pool.  Later.
	mp, err := mpool.NewMPool("compile", 0, mpool.NoFixed)
	if err != nil {
		panic(err)
	}
	pHelper, cnInfo := receiver.procBuildHelper, receiver.cnInformation
	proc := process.New(
		receiver.ctx,
		mp,
		pHelper.txnClient,
		pHelper.txnOperator,
		cnInfo.fileService,
		cnInfo.lockService,
		cnInfo.queryClient,
		cnInfo.hakeeper,
		cnInfo.udfService,
		cnInfo.aicm)
	proc.UnixTime = pHelper.unixTime
	proc.Id = pHelper.id
	proc.Lim = pHelper.lim
	proc.SessionInfo = pHelper.sessionInfo
	proc.SessionInfo.StorageEngine = cnInfo.storeEngine
	proc.AnalInfos = make([]*process.AnalyzeInfo, len(pHelper.analysisNodeList))
	for i := range proc.AnalInfos {
		proc.AnalInfos[i] = reuse.Alloc[process.AnalyzeInfo](nil)
		proc.AnalInfos[i].NodeId = pHelper.analysisNodeList[i]
	}
	proc.DispatchNotifyCh = make(chan process.WrapCs)
	{
		txn := proc.TxnOperator.Txn()
		txnId := txn.GetID()
		proc.StmtProfile = process.NewStmtProfile(uuid.UUID(txnId), pHelper.StmtId)
	}

	c := reuse.Alloc[Compile](nil)
	c.proc = proc
	c.proc.MessageBoard = c.MessageBoard
	c.e = cnInfo.storeEngine
	c.anal = newAnaylze()
	c.anal.analInfos = proc.AnalInfos
	c.addr = receiver.cnInformation.cnAddr
	c.proc.Ctx = perfcounter.WithCounterSet(c.proc.Ctx, c.counterSet)
	c.ctx = defines.AttachAccountId(c.proc.Ctx, pHelper.accountId)

	c.fill = func(b *batch.Batch) error {
		return receiver.sendBatch(b)
	}
	return c
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
		message.SetMoError(receiver.ctx, errInfo)
	}
	return receiver.clientSession.Write(receiver.ctx, message)
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
		return receiver.clientSession.Write(receiver.ctx, m)
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

		if errW := receiver.clientSession.Write(receiver.ctx, m); errW != nil {
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

	analysisInfo := receiver.finalAnalysisInfo
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

func generateProcessHelper(data []byte, cli client.TxnClient) (processHelper, error) {
	procInfo := &pipeline.ProcessInfo{}
	err := procInfo.Unmarshal(data)
	if err != nil {
		return processHelper{}, err
	}
	if len(procInfo.GetAnalysisNodeList()) == 0 {
		panic(fmt.Sprintf("empty plan: %s", procInfo.Sql))
	}

	result := processHelper{
		id:               procInfo.Id,
		lim:              process.ConvertToProcessLimitation(procInfo.Lim),
		unixTime:         procInfo.UnixTime,
		accountId:        procInfo.AccountId,
		txnClient:        cli,
		analysisNodeList: procInfo.GetAnalysisNodeList(),
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

func (receiver *messageReceiverOnServer) GetProcByUuid(uid uuid.UUID) (*process.Process, error) {
	getCtx, getCancel := context.WithTimeout(context.Background(), HandleNotifyTimeout)
	var opProc *process.Process
	var ok bool

	for {
		select {
		case <-getCtx.Done():
			colexec.Get().GetProcByUuid(uid, true)
			getCancel()
			return nil, moerr.NewInternalError(receiver.ctx, "get dispatch process by uuid timeout")

		case <-receiver.ctx.Done():
			colexec.Get().GetProcByUuid(uid, true)
			getCancel()
			return nil, nil

		default:
			if opProc, ok = colexec.Get().GetProcByUuid(uid, false); !ok {
				// it's bad to call the Gosched() here.
				// cut the HandleNotifyTimeout to 1ms, 1ms, 2ms, 3ms, 5ms, 8ms..., and use them as waiting time may be a better way.
				runtime.Gosched()
			} else {
				getCancel()
				return opProc, nil
			}
		}
	}
}

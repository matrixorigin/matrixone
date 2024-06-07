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
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"hash/crc32"
	"runtime"
	"sync/atomic"
	"time"

	qclient "github.com/matrixorigin/matrixone/pkg/queryservice/client"

	"github.com/matrixorigin/matrixone/pkg/common/reuse"

	"github.com/matrixorigin/matrixone/pkg/logservice"

	"github.com/google/uuid"

	"github.com/matrixorigin/matrixone/pkg/cnservice/cnclient"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/morpc"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/lockservice"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/pb/pipeline"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/perfcounter"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/udf"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

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

// cnInformation records service information to help handle messages.
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

// processHelper records source process information to help
// rebuild the process at the remote node.
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

// messageSenderOnClient is a structure
// for sending message and receiving results on cn-client.
type messageSenderOnClient struct {
	ctx       context.Context
	ctxCancel context.CancelFunc

	streamSender morpc.Stream
	receiveCh    chan morpc.Message

	c *Compile
}

func newMessageSenderOnClient(
	ctx context.Context, c *Compile, toAddr string) (*messageSenderOnClient, error) {
	var sender = new(messageSenderOnClient)

	streamSender, err := cnclient.GetStreamSender(toAddr)
	if err != nil {
		return sender, err
	}

	sender.streamSender = streamSender
	if _, ok := ctx.Deadline(); !ok {
		sender.ctx, sender.ctxCancel = context.WithTimeout(ctx, time.Second*10000)
	} else {
		sender.ctx = ctx
	}
	sender.c = c
	return sender, nil
}

// XXX we can set a scope as argument directly next day.
func (sender *messageSenderOnClient) send(
	scopeData, procData []byte, _ pipeline.Method) error {
	sdLen := len(scopeData)
	if sdLen <= maxMessageSizeToMoRpc {
		message := cnclient.AcquireMessage()
		message.SetID(sender.streamSender.ID())
		message.SetMessageType(pipeline.Method_PipelineMessage)
		message.SetData(scopeData)
		message.SetProcData(procData)
		message.SetSequence(0)
		message.SetSid(pipeline.Status_Last)
		return sender.streamSender.Send(sender.ctx, message)
	}

	start := 0
	cnt := uint64(0)
	for start < sdLen {
		end := start + maxMessageSizeToMoRpc

		message := cnclient.AcquireMessage()
		message.SetID(sender.streamSender.ID())
		message.SetMessageType(pipeline.Method_PipelineMessage)
		message.SetSequence(cnt)
		if end >= sdLen {
			message.SetData(scopeData[start:sdLen])
			message.SetProcData(procData)
			message.SetSid(pipeline.Status_Last)
		} else {
			message.SetData(scopeData[start:end])
			message.SetSid(pipeline.Status_WaitingNext)
		}

		if err := sender.streamSender.Send(sender.ctx, message); err != nil {
			return err
		}
		cnt++
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
			// ch close
			logutil.Errorf("the stream is closed, ok: %v, val: %v", ok, val)
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
			return nil, false, info
		}
		if m.IsEndMessage() {
			anaData := m.GetAnalyse()
			if len(anaData) > 0 {
				ana := new(pipeline.AnalysisList)
				if err = ana.Unmarshal(anaData); err != nil {
					return nil, false, err
				}
				mergeAnalyseInfo(sender.c.anal, ana)
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
		if m.Checksum != crc32.ChecksumIEEE(dataBuffer) {
			return nil, false, moerr.NewInternalErrorNoCtx("Packages delivered by morpc is broken")
		}

		bat, err = decodeBatch(sender.c.proc.Mp(), dataBuffer)
		return bat, false, err
	}
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
		atomic.AddInt64(&target.analInfos[i].InputBlocks, n.InputBlocks)
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
	if sender.ctxCancel != nil {
		sender.ctxCancel()
	}
	// XXX not a good way to deal it if close failed.
	_ = sender.streamSender.Close(true)
}

// messageReceiverOnServer is a structure
// for processing received message and writing results back at cn-server.
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

	// XXX what's that. So confused.
	sequence uint64

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
		sequence:        0,
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

	default:
		logutil.Errorf("unknown cmd %d for pipeline.Message", m.GetCmd())
		panic("unknown message type")
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
	c.e = cnInfo.storeEngine
	c.MessageBoard = c.MessageBoard.SetMultiCN(c.GetMessageCenter(), c.proc.StmtProfile.GetStmtId())
	c.proc.MessageBoard = c.MessageBoard
	c.anal = newAnaylze()
	c.anal.analInfos = proc.AnalInfos
	c.addr = receiver.cnInformation.cnAddr
	c.proc.Ctx = perfcounter.WithCounterSet(c.proc.Ctx, c.counterSet)
	c.ctx = defines.AttachAccountId(c.proc.Ctx, pHelper.accountId)
	c.execType = plan2.ExecTypeAP_MULTICN
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

	checksum := crc32.ChecksumIEEE(data)
	dataLen := len(data)
	if dataLen <= receiver.maxMessageSize {
		m, errA := receiver.acquireMessage()
		if errA != nil {
			return errA
		}
		m.SetMessageType(pipeline.Method_BatchMessage)
		m.SetData(data)
		// XXX too bad.
		m.SetCheckSum(checksum)
		m.SetSequence(receiver.sequence)
		m.SetSid(pipeline.Status_Last)
		receiver.sequence++
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
			m.SetCheckSum(checksum)
		} else {
			m.SetSid(pipeline.Status_WaitingNext)
		}
		m.SetMessageType(pipeline.Method_BatchMessage)
		m.SetData(data[start:end])
		m.SetSequence(receiver.sequence)
		receiver.sequence++

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
		lim:              convertToProcessLimitation(procInfo.Lim),
		unixTime:         procInfo.UnixTime,
		accountId:        procInfo.AccountId,
		txnClient:        cli,
		analysisNodeList: procInfo.GetAnalysisNodeList(),
	}
	result.txnOperator, err = cli.NewWithSnapshot([]byte(procInfo.Snapshot))
	if err != nil {
		return processHelper{}, err
	}
	result.sessionInfo, err = convertToProcessSessionInfo(procInfo.SessionInfo)
	if err != nil {
		return processHelper{}, err
	}
	if sessLogger := procInfo.SessionLogger; sessLogger != nil {
		copy(result.sessionInfo.SessionId[:], sessLogger.SessId)
		copy(result.StmtId[:], sessLogger.StmtId)
		result.sessionInfo.LogLevel = enumLogLevel2ZapLogLevel(sessLogger.LogLevel)
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

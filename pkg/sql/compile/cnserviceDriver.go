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
	"hash/crc32"
	"runtime"
	"time"

	"github.com/google/uuid"
	"github.com/matrixorigin/matrixone/pkg/cnservice/cnclient"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/morpc"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/pb/pipeline"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/agg"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/anti"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/connector"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/dispatch"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/external"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/group"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/hashbuild"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/insert"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/intersect"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/intersectall"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/join"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/left"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/limit"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/loopanti"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/loopjoin"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/loopleft"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/loopmark"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/loopsemi"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/loopsingle"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/mark"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/merge"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/mergegroup"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/mergelimit"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/mergeoffset"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/mergeorder"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/mergetop"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/minus"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/offset"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/order"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/output"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/product"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/projection"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/restrict"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/semi"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/single"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/table_function"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/top"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// messageHandleHelper a structure records some elements to help handle messages.
type messageHandleHelper struct {
	storeEngine       engine.Engine
	fileService       fileservice.FileService
	getClusterDetails engine.GetClusterDetailsFunc
	acquirer          func() morpc.Message
	sequence          uint64
}

// processHelper a structure records information about source process. and to help
// rebuild the process in the remote node.
type processHelper struct {
	id               string
	lim              process.Limitation
	unixTime         int64
	txnOperator      client.TxnOperator
	txnClient        client.TxnClient
	sessionInfo      process.SessionInfo
	analysisNodeList []int32
}

// CnServerMessageHandler deal the client message that received at cn-server.
// the message is always *pipeline.Message here. It's a byte array which encoded by method encodeScope.
// write back Analysis Information and error info if error occurs to client.
func CnServerMessageHandler(ctx context.Context, message morpc.Message,
	cs morpc.ClientSession,
	storeEngine engine.Engine, fileService fileservice.FileService, cli client.TxnClient, messageAcquirer func() morpc.Message,
	getClusterDetails engine.GetClusterDetailsFunc) error {
	var errData []byte
	// structure to help handle the message.
	helper := &messageHandleHelper{
		storeEngine:       storeEngine,
		fileService:       fileService,
		acquirer:          messageAcquirer,
		getClusterDetails: getClusterDetails,
	}
	// decode message and run it, get final analysis information and err info.
	msgTyp, analysis, err := cnMessageHandle(ctx, message, cs, helper, cli)
	if err != nil {
		errData = pipeline.EncodedMessageError(ctx, err)
	}
	// batch data, not scope
	if analysis == nil {
		return nil
	}
	backMessage := messageAcquirer().(*pipeline.Message)
	backMessage.Id = message.GetID()
	backMessage.Cmd = msgTyp
	backMessage.Sid = pipeline.MessageEnd
	backMessage.Err = errData
	backMessage.Analyse = analysis
	return cs.Write(ctx, backMessage)
}

func fillEngineForInsert(s *Scope, e engine.Engine) {
	for i := range s.Instructions {
		if s.Instructions[i].Op == vm.Insert {
			s.Instructions[i].Arg.(*insert.Argument).Engine = e
		}
	}
	for i := range s.PreScopes {
		fillEngineForInsert(s.PreScopes[i], e)
	}
}

func cnMessageHandle(ctx context.Context, message morpc.Message, cs morpc.ClientSession, messageHelper *messageHandleHelper, cli client.TxnClient) (msgTyp uint64, anaData []byte, err error) {
	var c *Compile
	var s *Scope
	var procHelper *processHelper

	m, ok := message.(*pipeline.Message)
	if !ok {
		panic("unexpected message type for cn-server")
	}

	msgTyp = m.GetCmd()
	switch msgTyp {
	case pipeline.PrepareDoneNotifyMessage: // notify the dispatch excutor
		opUuid, err := uuid.FromBytes(m.GetUuid())
		if err != nil {
			return msgTyp, nil, err
		}

		var ch chan process.WrapCs
		ok := false
		for {
			if ch, ok = colexec.Srv.GetNotifyChByUuid(opUuid); !ok {
				runtime.Gosched()
			} else {
				break
			}
		}
		info := process.WrapCs{
			MsgId: m.GetId(),
			Uid:   opUuid,
			Cs:    cs,
		}

		ch <- info
		return msgTyp, nil, nil

	case pipeline.PipelineMessage:
		procHelper, err = generateProcessHelper(m.GetProcInfoData(), cli)
		if err != nil {
			return msgTyp, nil, err
		}
		c = newCompile(ctx, message, procHelper, messageHelper, cs)

		// decode and run the scope.
		s, err = decodeScope(m.GetData(), c.proc, true)
		// remote insert operator need to have engine
		fillEngineForInsert(s, c.e)

		if err != nil {
			return msgTyp, nil, err
		}
		s = refactorScope(c, c.ctx, s)

		err = s.ParallelRun(c, s.IsRemote)
		if err != nil {
			return msgTyp, nil, err
		}
		// encode analysis info and return.
		anas := &pipeline.AnalysisList{
			List: make([]*plan.AnalyzeInfo, len(c.proc.AnalInfos)),
		}
		for i := range anas.List {
			anas.List[i] = convertToPlanAnalyzeInfo(c.proc.AnalInfos[i])
		}
		anaData, err = anas.Marshal()
		return msgTyp, anaData, err
	default:
		return msgTyp, nil, moerr.NewInternalError(ctx, "unknow message type")
	}
}

const maxMessageSizeToMoRpc = 64 * mpool.MB

// sendBackBatchToCnClient send back the result batch to cn client.
func sendBackBatchToCnClient(ctx context.Context, b *batch.Batch, messageId uint64, mHelper *messageHandleHelper, cs morpc.ClientSession) error {
	if b == nil {
		return nil
	}
	encodeData, errEncode := types.Encode(b)
	if errEncode != nil {
		return errEncode
	}

	if len(encodeData) <= maxMessageSizeToMoRpc {
		m := mHelper.acquirer().(*pipeline.Message)
		m.Id = messageId
		m.Data = encodeData
		m.Checksum = crc32.ChecksumIEEE(m.Data)
		m.Sequence = mHelper.sequence
		mHelper.sequence++
		return cs.Write(ctx, m)
	}
	// if data is too large, it should be split into small blocks.
	start := 0
	for start < len(encodeData) {
		end := start + maxMessageSizeToMoRpc
		sid := pipeline.WaitingNext
		if end > len(encodeData) {
			end = len(encodeData)
			sid = pipeline.BatchEnd
		}
		m := mHelper.acquirer().(*pipeline.Message)
		m.Id = messageId
		m.Data = encodeData[start:end]
		m.Sid = uint64(sid)
		m.Checksum = crc32.ChecksumIEEE(m.Data)
		m.Sequence = mHelper.sequence
		mHelper.sequence++
		if err := cs.Write(ctx, m); err != nil {
			return err
		}
		start = end
	}
	return nil
}

// receiveMessageFromCnServer deal the back message from cn-server.
func receiveMessageFromCnServer(c *Compile, mChan chan morpc.Message, nextAnalyze process.Analyze, nextOperator *connector.Argument) error {
	var val morpc.Message
	var dataBuffer []byte
	var sequence uint64
	for {
		select {
		case <-c.ctx.Done():
			return moerr.NewRPCTimeout(c.ctx)
		case val = <-mChan:
		}
		if val == nil {
			return nil
		}
		m := val.(*pipeline.Message)

		// if message contains the back error info.
		if err := pipeline.GetMessageErrorInfo(m); err != nil {
			return err
		}
		if m.IsEndMessage() {
			anaData := m.GetAnalyse()
			if len(anaData) > 0 {
				// get analysis info if it has
				ana := new(pipeline.AnalysisList)
				err := ana.Unmarshal(anaData)
				if err != nil {
					return err
				}
				mergeAnalyseInfo(c.anal, ana)
			}
			return nil
		} else {
			if m.Checksum != crc32.ChecksumIEEE(m.Data) {
				return moerr.NewInternalErrorNoCtx("Packages delivered by morpc is broken")
			}
			if sequence != m.Sequence {
				return moerr.NewInternalErrorNoCtx("Packages passed by morpc are out of order")
			}
			sequence++
			dataBuffer = append(dataBuffer, m.Data...)
			if m.WaitingNextToMerge() {
				continue
			}
			bat, err := decodeBatch(c.proc.Mp(), dataBuffer)
			if err != nil {
				return err
			}
			nextAnalyze.Network(bat)
			sendToConnectOperator(nextOperator, bat)
			dataBuffer = nil
		}
	}
}

// remoteRun sends a scope to remote node for execution, and wait to receive the back message.
// the back message is always *pipeline.Message but contains three cases.
// 1. Message with error information
// 2. Message with end flag and the result of analysis
// 3. Batch Message with batch data
func (s *Scope) remoteRun(c *Compile) error {
	// encode the scope. shouldn't encode the `connector` operator which used to receive the back batch.
	n := len(s.Instructions) - 1
	con := s.Instructions[n]
	s.Instructions = s.Instructions[:n]
	sData, errEncode := encodeScope(s)
	if errEncode != nil {
		return errEncode
	}
	s.Instructions = append(s.Instructions, con)

	// encode the process related information
	pData, errEncodeProc := encodeProcessInfo(c.proc)
	if errEncodeProc != nil {
		return errEncodeProc
	}

	// get the stream-sender
	streamSender, errStream := cnclient.GetStreamSender(s.NodeInfo.Addr)
	if errStream != nil {
		return errStream
	}
	defer func(streamSender morpc.Stream) {
		_ = streamSender.Close()
	}(streamSender)

	// send encoded message and receive the back message.
	// TODO: get dead time from config file may suitable.
	if _, ok := c.ctx.Deadline(); !ok {
		var cancel context.CancelFunc
		c.ctx, cancel = context.WithTimeout(c.ctx, time.Second*10000)
		_ = cancel
	}

	message := cnclient.AcquireMessage()
	{
		message.Id = streamSender.ID()
		message.Data = sData
		message.ProcInfoData = pData
		message.Cmd = pipeline.PipelineMessage
	}

	errSend := streamSender.Send(c.ctx, message)
	if errSend != nil {
		return errSend
	}
	messagesReceive, errReceive := streamSender.Receive()
	if errReceive != nil {
		return errReceive
	}
	nextInstruction := s.Instructions[len(s.Instructions)-1]
	nextAnalyze := c.proc.GetAnalyze(nextInstruction.Idx)
	nextArg := nextInstruction.Arg.(*connector.Argument)
	return receiveMessageFromCnServer(c, messagesReceive, nextAnalyze, nextArg)
}

// encodeScope generate a pipeline.Pipeline from Scope, encode pipeline, and returns.
func encodeScope(s *Scope) ([]byte, error) {
	p, err := fillPipeline(s)
	if err != nil {
		return nil, err
	}
	return p.Marshal()
}

// decodeScope decode a pipeline.Pipeline from bytes, and generate a Scope from it.
func decodeScope(data []byte, proc *process.Process, isRemote bool) (*Scope, error) {
	// unmarshal to pipeline
	p := &pipeline.Pipeline{}
	err := p.Unmarshal(data)
	if err != nil {
		return nil, err
	}
	ctx := &scopeContext{
		parent: nil,
		id:     p.PipelineId,
		regs:   make(map[*process.WaitRegister]int32),
	}
	ctx.root = ctx
	s, err := generateScope(proc, p, ctx, nil, isRemote)
	if err != nil {
		return nil, err
	}
	if err := fillInstructionsForScope(s, ctx, p); err != nil {
		return nil, err
	}

	return s, nil
}

// encodeProcessInfo get needed information from proc, and do serialization work.
func encodeProcessInfo(proc *process.Process) ([]byte, error) {
	procInfo := &pipeline.ProcessInfo{}
	{
		procInfo.Id = proc.Id
		procInfo.Lim = convertToPipelineLimitation(proc.Lim)
		procInfo.UnixTime = proc.UnixTime
		snapshot, err := proc.TxnOperator.Snapshot()
		if err != nil {
			return nil, err
		}
		procInfo.Snapshot = string(snapshot)
		procInfo.AnalysisNodeList = make([]int32, len(proc.AnalInfos))
		for i := range procInfo.AnalysisNodeList {
			procInfo.AnalysisNodeList[i] = proc.AnalInfos[i].NodeId
		}
	}
	{ // session info
		timeBytes, err := time.Time{}.In(proc.SessionInfo.TimeZone).MarshalBinary()
		if err != nil {
			return nil, err
		}

		procInfo.SessionInfo = &pipeline.SessionInfo{
			User:         proc.SessionInfo.GetUser(),
			Host:         proc.SessionInfo.GetHost(),
			Role:         proc.SessionInfo.GetRole(),
			ConnectionId: proc.SessionInfo.GetConnectionID(),
			Database:     proc.SessionInfo.GetDatabase(),
			Version:      proc.SessionInfo.GetVersion(),
			TimeZone:     timeBytes,
		}
	}
	return procInfo.Marshal()
}

// generateProcessHelper generate the processHelper by encoded process info.
func generateProcessHelper(data []byte, cli client.TxnClient) (*processHelper, error) {
	procInfo := &pipeline.ProcessInfo{}
	err := procInfo.Unmarshal(data)
	if err != nil {
		return nil, err
	}

	result := &processHelper{
		id:               procInfo.Id,
		lim:              convertToProcessLimitation(procInfo.Lim),
		unixTime:         procInfo.UnixTime,
		txnClient:        cli,
		analysisNodeList: procInfo.GetAnalysisNodeList(),
	}
	result.txnOperator, err = cli.NewWithSnapshot([]byte(procInfo.Snapshot))
	if err != nil {
		return nil, err
	}
	result.sessionInfo, err = convertToProcessSessionInfo(procInfo.SessionInfo)
	if err != nil {
		return nil, err
	}

	return result, nil
}

func refactorScope(c *Compile, _ context.Context, s *Scope) *Scope {
	rs := c.newMergeScope([]*Scope{s})
	rs.Instructions = append(rs.Instructions, vm.Instruction{
		Op:  vm.Output,
		Idx: -1, // useless
		Arg: &output.Argument{Data: nil, Func: c.fill},
	})
	return rs
}

// fillPipeline convert the scope to pipeline.Pipeline structure through 2 iterations.
func fillPipeline(s *Scope) (*pipeline.Pipeline, error) {
	ctx := &scopeContext{
		id:     0,
		parent: nil,
		regs:   make(map[*process.WaitRegister]int32),
	}
	ctx.root = ctx
	p, ctxId, err := generatePipeline(s, ctx, 1)
	if err != nil {
		return nil, err
	}
	if _, err = fillInstructionsForPipeline(s, ctx, p, ctxId); err != nil {
		return nil, err
	}
	return p, nil
}

// generatePipeline generate a base pipeline.Pipeline structure without instructions
// according to source scope.
func generatePipeline(s *Scope, ctx *scopeContext, ctxId int32) (*pipeline.Pipeline, int32, error) {
	var err error

	p := &pipeline.Pipeline{}
	// Magic and IsEnd
	p.PipelineType = pipeline.Pipeline_PipelineType(s.Magic)
	p.PipelineId = ctx.id
	p.IsEnd = s.IsEnd
	p.IsJoin = s.IsJoin
	p.UuidsToRegIdx = convertScopeRemoteReceivInfo(s)

	// Plan
	if ctxId == 1 {
		// encode and decode cost is too large for it.
		// only encode the first one.
		p.Qry = s.Plan
	}
	p.Node = &pipeline.NodeInfo{
		Id:      s.NodeInfo.Id,
		Addr:    s.NodeInfo.Addr,
		Mcpu:    int32(s.NodeInfo.Mcpu),
		Payload: make([]string, len(s.NodeInfo.Data)),
	}
	ctx.pipe = p
	ctx.scope = s
	{
		for i := range s.NodeInfo.Data {
			p.Node.Payload[i] = string(s.NodeInfo.Data[i])
		}
	}
	p.ChildrenCount = int32(len(s.Proc.Reg.MergeReceivers))
	{
		for i := range s.Proc.Reg.MergeReceivers {
			ctx.regs[s.Proc.Reg.MergeReceivers[i]] = int32(i)
		}
	}
	// DataSource
	if s.DataSource != nil { // if select 1, DataSource is nil
		p.DataSource = &pipeline.Source{
			SchemaName:   s.DataSource.SchemaName,
			TableName:    s.DataSource.RelationName,
			ColList:      s.DataSource.Attributes,
			PushdownId:   s.DataSource.PushdownId,
			PushdownAddr: s.DataSource.PushdownAddr,
			Expr:         s.DataSource.Expr,
			TableDef:     s.DataSource.TableDef,
			Timestamp:    &s.DataSource.Timestamp,
		}
		if s.DataSource.Bat != nil {
			data, err := types.Encode(s.DataSource.Bat)
			if err != nil {
				return nil, -1, err
			}
			p.DataSource.Block = string(data)
		}
	}
	// PreScope
	p.Children = make([]*pipeline.Pipeline, len(s.PreScopes))
	ctx.children = make([]*scopeContext, len(s.PreScopes))
	for i := range s.PreScopes {
		ctx.children[i] = &scopeContext{
			parent: ctx,
			id:     ctxId,
			root:   ctx.root,
			regs:   make(map[*process.WaitRegister]int32),
		}
		ctxId++
		if p.Children[i], ctxId, err = generatePipeline(s.PreScopes[i], ctx.children[i], ctxId); err != nil {
			return nil, -1, err
		}
	}
	return p, ctxId, nil
}

// fillInstructionsForPipeline fills pipeline's instructions.
func fillInstructionsForPipeline(s *Scope, ctx *scopeContext, p *pipeline.Pipeline, ctxId int32) (int32, error) {
	var err error

	for i := range s.PreScopes {
		if ctxId, err = fillInstructionsForPipeline(s.PreScopes[i], ctx.children[i], p.Children[i], ctxId); err != nil {
			return ctxId, err
		}
	}
	// Instructions
	p.InstructionList = make([]*pipeline.Instruction, len(s.Instructions))
	for i := range p.InstructionList {
		if ctxId, p.InstructionList[i], err = convertToPipelineInstruction(&s.Instructions[i], ctx, ctxId); err != nil {
			return ctxId, err
		}
	}
	return ctxId, nil
}

func convertPipelineUuid(p *pipeline.Pipeline, s *Scope) error {
	s.RemoteReceivRegInfos = make([]RemoteReceivRegInfo, len(p.UuidsToRegIdx))
	for i, u := range p.UuidsToRegIdx {
		uid, err := uuid.FromBytes(u.GetUuid())
		if err != nil {
			return moerr.NewInvalidInputNoCtx("decode uuid failed: %s\n", err)
		}
		s.RemoteReceivRegInfos[i] = RemoteReceivRegInfo{
			Idx:      int(u.GetIdx()),
			Uuid:     uid,
			FromAddr: u.FromAddr,
		}
	}
	return nil
}

func convertScopeRemoteReceivInfo(s *Scope) (ret []*pipeline.UuidToRegIdx) {
	ret = make([]*pipeline.UuidToRegIdx, len(s.RemoteReceivRegInfos))
	for i, u := range s.RemoteReceivRegInfos {
		ret[i] = &pipeline.UuidToRegIdx{
			Idx:      int32(u.Idx),
			Uuid:     u.Uuid[:],
			FromAddr: u.FromAddr,
		}
	}
	return ret
}

// generateScope generate a scope from scope context and pipeline.
func generateScope(proc *process.Process, p *pipeline.Pipeline, ctx *scopeContext,
	analNodes []*process.AnalyzeInfo, isRemote bool) (*Scope, error) {
	var err error
	if p.Qry != nil {
		ctx.plan = p.Qry
	}

	s := &Scope{
		Magic:    int(p.GetPipelineType()),
		IsEnd:    p.IsEnd,
		IsJoin:   p.IsJoin,
		Plan:     ctx.plan,
		IsRemote: isRemote,
	}
	if err := convertPipelineUuid(p, s); err != nil {
		return s, err
	}
	dsc := p.GetDataSource()
	if dsc != nil {
		s.DataSource = &Source{
			SchemaName:   dsc.SchemaName,
			RelationName: dsc.TableName,
			Attributes:   dsc.ColList,
			PushdownId:   dsc.PushdownId,
			PushdownAddr: dsc.PushdownAddr,
			Expr:         dsc.Expr,
			TableDef:     dsc.TableDef,
			Timestamp:    *dsc.Timestamp,
		}
		if len(dsc.Block) > 0 {
			bat := new(batch.Batch)
			if err := types.Decode([]byte(dsc.Block), bat); err != nil {
				return nil, err
			}
			s.DataSource.Bat = bat
		}
	}
	if p.Node != nil {
		s.NodeInfo.Id = p.Node.Id
		s.NodeInfo.Addr = p.Node.Addr
		s.NodeInfo.Mcpu = int(p.Node.Mcpu)
		s.NodeInfo.Data = make([][]byte, len(p.Node.Payload))
		for i := range p.Node.Payload {
			s.NodeInfo.Data[i] = []byte(p.Node.Payload[i])
		}
	}
	s.Proc = process.NewWithAnalyze(proc, proc.Ctx, int(p.ChildrenCount), analNodes)
	{
		for i := range s.Proc.Reg.MergeReceivers {
			ctx.regs[s.Proc.Reg.MergeReceivers[i]] = int32(i)
		}
	}
	s.PreScopes = make([]*Scope, len(p.Children))
	ctx.children = make([]*scopeContext, len(s.PreScopes))
	for i := range s.PreScopes {
		ctx.children[i] = &scopeContext{
			parent: ctx,
			root:   ctx.root,
			id:     p.Children[i].PipelineId,
			regs:   make(map[*process.WaitRegister]int32),
		}
		if s.PreScopes[i], err = generateScope(s.Proc, p.Children[i], ctx.children[i], analNodes, isRemote); err != nil {
			return nil, err
		}
	}
	return s, nil
}

// fillInstructionsForScope fills scope's instructions.
func fillInstructionsForScope(s *Scope, ctx *scopeContext, p *pipeline.Pipeline) error {
	var err error

	for i := range s.PreScopes {
		if err = fillInstructionsForScope(s.PreScopes[i], ctx.children[i], p.Children[i]); err != nil {
			return err
		}
	}
	s.Instructions = make([]vm.Instruction, len(p.InstructionList))
	for i := range s.Instructions {
		if s.Instructions[i], err = convertToVmInstruction(p.InstructionList[i], ctx); err != nil {
			return err
		}
	}
	return nil
}

// convert vm.Instruction to pipeline.Instruction
func convertToPipelineInstruction(opr *vm.Instruction, ctx *scopeContext, ctxId int32) (int32, *pipeline.Instruction, error) {
	var err error

	in := &pipeline.Instruction{Op: int32(opr.Op), Idx: int32(opr.Idx), IsFirst: opr.IsFirst, IsLast: opr.IsLast}
	switch t := opr.Arg.(type) {
	case *insert.Argument:
		in.Insert = &pipeline.Insert{
			IsRemote: t.IsRemote,
			Affected: t.Affected,
			// TargetColDefs:  t.TargetColDefs,
			// TableID:        t.TableID,
			// CPkeyColDef:    t.CPkeyColDef,
			// DBName:         t.DBName,
			// TableName:      t.TableName,
			// ClusterTable:   t.ClusterTable,
			// ClusterByDef:   t.ClusterByDef,
			// UniqueIndexDef: t.UniqueIndexDef,
			// HasAutoCol:     t.HasAutoCol,
			Ref:          t.InsertCtx.Ref,
			TableDef:     t.InsertCtx.TableDef,
			Idx:          t.InsertCtx.Idx,
			ClusterTable: t.InsertCtx.ClusterTable,
			ParentIdx:    t.InsertCtx.ParentIdx,
		}
	case *anti.Argument:
		in.Anti = &pipeline.AntiJoin{
			Ibucket:   t.Ibucket,
			Nbucket:   t.Nbucket,
			Expr:      t.Cond,
			Types:     convertToPlanTypes(t.Typs),
			LeftCond:  t.Conditions[0],
			RightCond: t.Conditions[1],
			Result:    t.Result,
		}
	case *dispatch.Argument:
		in.Dispatch = &pipeline.Dispatch{FuncId: int32(t.FuncId)}
		in.Dispatch.LocalConnector = make([]*pipeline.Connector, len(t.LocalRegs))
		for i := range t.LocalRegs {
			idx, ctx0 := ctx.root.findRegister(t.LocalRegs[i])
			if ctx0.root.isRemote(ctx0, 0) && !ctx0.isDescendant(ctx) {
				id := colexec.Srv.RegistConnector(t.LocalRegs[i])
				if ctxId, err = ctx0.addSubPipeline(id, idx, ctxId); err != nil {
					return ctxId, nil, err
				}
			}
			in.Dispatch.LocalConnector[i] = &pipeline.Connector{
				ConnectorIndex: idx,
				PipelineId:     ctx0.id,
			}
		}

		if len(t.RemoteRegs) > 0 {
			in.Dispatch.RemoteConnector = make([]*pipeline.WrapNode, len(t.RemoteRegs))
			for i, r := range t.RemoteRegs {
				wn := &pipeline.WrapNode{
					NodeAddr: r.NodeAddr,
					Uuid:     r.Uuid[:],
				}
				in.Dispatch.RemoteConnector[i] = wn
			}
		}
	case *group.Argument:
		in.Agg = &pipeline.Group{
			NeedEval: t.NeedEval,
			Ibucket:  t.Ibucket,
			Nbucket:  t.Nbucket,
			Exprs:    t.Exprs,
			Types:    convertToPlanTypes(t.Types),
			Aggs:     convertToPipelineAggregates(t.Aggs),
		}
	case *join.Argument:
		relList, colList := getRelColList(t.Result)
		in.Join = &pipeline.Join{
			Ibucket:   t.Ibucket,
			Nbucket:   t.Nbucket,
			RelList:   relList,
			ColList:   colList,
			Expr:      t.Cond,
			Types:     convertToPlanTypes(t.Typs),
			LeftCond:  t.Conditions[0],
			RightCond: t.Conditions[1],
		}
	case *left.Argument:
		relList, colList := getRelColList(t.Result)
		in.LeftJoin = &pipeline.LeftJoin{
			Ibucket:   t.Ibucket,
			Nbucket:   t.Nbucket,
			RelList:   relList,
			ColList:   colList,
			Expr:      t.Cond,
			Types:     convertToPlanTypes(t.Typs),
			LeftCond:  t.Conditions[0],
			RightCond: t.Conditions[1],
		}
	case *limit.Argument:
		in.Limit = t.Limit
	case *loopanti.Argument:
		in.Anti = &pipeline.AntiJoin{
			Result: t.Result,
			Expr:   t.Cond,
			Types:  convertToPlanTypes(t.Typs),
		}
	case *loopjoin.Argument:
		relList, colList := getRelColList(t.Result)
		in.Join = &pipeline.Join{
			RelList: relList,
			ColList: colList,
			Expr:    t.Cond,
			Types:   convertToPlanTypes(t.Typs),
		}
	case *loopleft.Argument:
		relList, colList := getRelColList(t.Result)
		in.LeftJoin = &pipeline.LeftJoin{
			RelList: relList,
			ColList: colList,
			Expr:    t.Cond,
			Types:   convertToPlanTypes(t.Typs),
		}
	case *loopsemi.Argument:
		in.SemiJoin = &pipeline.SemiJoin{
			Result: t.Result,
			Expr:   t.Cond,
			Types:  convertToPlanTypes(t.Typs),
		}
	case *loopsingle.Argument:
		relList, colList := getRelColList(t.Result)
		in.SingleJoin = &pipeline.SingleJoin{
			RelList: relList,
			ColList: colList,
			Expr:    t.Cond,
			Types:   convertToPlanTypes(t.Typs),
		}
	case *offset.Argument:
		in.Offset = t.Offset
	case *order.Argument:
		in.OrderBy = t.Fs
	case *product.Argument:
		relList, colList := getRelColList(t.Result)
		in.Product = &pipeline.Product{
			RelList: relList,
			ColList: colList,
			Types:   convertToPlanTypes(t.Typs),
		}
	case *projection.Argument:
		in.ProjectList = t.Es
	case *restrict.Argument:
		in.Filter = t.E
	case *semi.Argument:
		in.SemiJoin = &pipeline.SemiJoin{
			Ibucket:   t.Ibucket,
			Nbucket:   t.Nbucket,
			Result:    t.Result,
			Expr:      t.Cond,
			Types:     convertToPlanTypes(t.Typs),
			LeftCond:  t.Conditions[0],
			RightCond: t.Conditions[1],
		}
	case *single.Argument:
		relList, colList := getRelColList(t.Result)
		in.SingleJoin = &pipeline.SingleJoin{
			Ibucket:   t.Ibucket,
			Nbucket:   t.Nbucket,
			RelList:   relList,
			ColList:   colList,
			Expr:      t.Cond,
			Types:     convertToPlanTypes(t.Typs),
			LeftCond:  t.Conditions[0],
			RightCond: t.Conditions[1],
		}
	case *top.Argument:
		in.Limit = uint64(t.Limit)
		in.OrderBy = t.Fs
	// we reused ANTI to store the information here because of the lack of related structure.
	case *intersect.Argument: // 1
		in.Anti = &pipeline.AntiJoin{
			Ibucket: t.IBucket,
			Nbucket: t.NBucket,
		}
	case *minus.Argument: // 2
		in.Anti = &pipeline.AntiJoin{
			Ibucket: t.IBucket,
			Nbucket: t.NBucket,
		}
	case *intersectall.Argument:
		in.Anti = &pipeline.AntiJoin{
			Ibucket: t.IBucket,
			Nbucket: t.NBucket,
		}
	case *merge.Argument:
	case *mergegroup.Argument:
		in.Agg = &pipeline.Group{
			NeedEval: t.NeedEval,
		}
	case *mergelimit.Argument:
		in.Limit = t.Limit
	case *mergeoffset.Argument:
		in.Offset = t.Offset
	case *mergetop.Argument:
		in.Limit = uint64(t.Limit)
		in.OrderBy = t.Fs
	case *mergeorder.Argument:
		in.OrderBy = t.Fs
	case *connector.Argument:
		idx, ctx0 := ctx.root.findRegister(t.Reg)
		if ctx0.root.isRemote(ctx0, 0) && !ctx0.isDescendant(ctx) {
			id := colexec.Srv.RegistConnector(t.Reg)
			if ctxId, err = ctx0.addSubPipeline(id, idx, ctxId); err != nil {
				return ctxId, nil, err
			}
		}
		in.Connect = &pipeline.Connector{
			PipelineId:     ctx0.id,
			ConnectorIndex: idx,
		}
	case *mark.Argument:
		in.MarkJoin = &pipeline.MarkJoin{
			Ibucket:   t.Ibucket,
			Nbucket:   t.Nbucket,
			Result:    t.Result,
			LeftCond:  t.Conditions[0],
			RightCond: t.Conditions[1],
			Types:     convertToPlanTypes(t.Typs),
			Expr:      t.Cond,
			OnList:    t.OnList,
		}
	case *table_function.Argument:
		in.TableFunction = &pipeline.TableFunction{
			Attrs:  t.Attrs,
			Rets:   t.Rets,
			Args:   t.Args,
			Params: t.Params,
			Name:   t.Name,
		}
	case *hashbuild.Argument:
		in.HashBuild = &pipeline.HashBuild{
			NeedExpr: t.NeedExpr,
			NeedHash: t.NeedHashMap,
			Ibucket:  t.Ibucket,
			Nbucket:  t.Nbucket,
			Types:    convertToPlanTypes(t.Typs),
			Conds:    t.Conditions,
		}
	case *external.Argument:
		name2ColIndexSlice := make([]*pipeline.ExternalName2ColIndex, len(t.Es.Name2ColIndex))
		i := 0
		for k, v := range t.Es.Name2ColIndex {
			name2ColIndexSlice[i] = &pipeline.ExternalName2ColIndex{Name: k, Index: v}
			i++
		}
		in.ExternalScan = &pipeline.ExternalScan{
			Attrs:         t.Es.Attrs,
			Cols:          t.Es.Cols,
			Name2ColIndex: name2ColIndexSlice,
			CreateSql:     t.Es.CreateSql,
			FileList:      t.Es.FileList,
		}
	default:
		return -1, nil, moerr.NewInternalErrorNoCtx(fmt.Sprintf("unexpected operator: %v", opr.Op))
	}
	return ctxId, in, nil
}

// convert pipeline.Instruction to vm.Instruction
func convertToVmInstruction(opr *pipeline.Instruction, ctx *scopeContext) (vm.Instruction, error) {
	v := vm.Instruction{Op: int(opr.Op), Idx: int(opr.Idx), IsFirst: opr.IsFirst, IsLast: opr.IsLast}
	switch opr.Op {
	case vm.Insert:
		t := opr.GetInsert()
		v.Arg = &insert.Argument{
			Affected: t.Affected,
			IsRemote: t.IsRemote,
			InsertCtx: &insert.InsertCtx{
				Idx:          t.Idx,
				Ref:          t.Ref,
				TableDef:     t.TableDef,
				ParentIdx:    t.ParentIdx,
				ClusterTable: t.ClusterTable,
			},
		}
	case vm.Anti:
		t := opr.GetAnti()
		v.Arg = &anti.Argument{
			Ibucket: t.Ibucket,
			Nbucket: t.Nbucket,
			Cond:    t.Expr,
			Typs:    convertToTypes(t.Types),
			Conditions: [][]*plan.Expr{
				t.LeftCond, t.RightCond,
			},
			Result: t.Result,
		}
	case vm.Dispatch:
		t := opr.GetDispatch()
		regs := make([]*process.WaitRegister, len(t.LocalConnector))
		for i, cp := range t.LocalConnector {
			regs[i] = ctx.root.getRegister(cp.PipelineId, cp.ConnectorIndex)
		}
		rrs := make([]colexec.ReceiveInfo, 0)
		if len(t.RemoteConnector) > 0 {
			for _, rc := range t.RemoteConnector {
				uid, err := uuid.FromBytes(rc.Uuid)
				if err != nil {
					return v, err
				}
				n := colexec.ReceiveInfo{
					NodeAddr: rc.NodeAddr,
					Uuid:     uid,
				}
				rrs = append(rrs, n)
			}
		}
		v.Arg = &dispatch.Argument{
			FuncId:     int(t.FuncId),
			LocalRegs:  regs,
			RemoteRegs: rrs,
		}
	case vm.Group:
		t := opr.GetAgg()
		v.Arg = &group.Argument{
			NeedEval: t.NeedEval,
			Ibucket:  t.Ibucket,
			Nbucket:  t.Nbucket,
			Exprs:    t.Exprs,
			Types:    convertToTypes(t.Types),
			Aggs:     convertToAggregates(t.Aggs),
		}
	case vm.Join:
		t := opr.GetJoin()
		v.Arg = &join.Argument{
			Ibucket:    t.Ibucket,
			Nbucket:    t.Nbucket,
			Cond:       t.Expr,
			Typs:       convertToTypes(t.Types),
			Result:     convertToResultPos(t.RelList, t.ColList),
			Conditions: [][]*plan.Expr{t.LeftCond, t.RightCond},
		}
	case vm.Left:
		t := opr.GetLeftJoin()
		v.Arg = &left.Argument{
			Ibucket:    t.Ibucket,
			Nbucket:    t.Nbucket,
			Cond:       t.Expr,
			Typs:       convertToTypes(t.Types),
			Result:     convertToResultPos(t.RelList, t.ColList),
			Conditions: [][]*plan.Expr{t.LeftCond, t.RightCond},
		}
	case vm.Limit:
		v.Arg = &limit.Argument{Limit: opr.Limit}
	case vm.LoopAnti:
		t := opr.GetAnti()
		v.Arg = &loopanti.Argument{
			Result: t.Result,
			Cond:   t.Expr,
			Typs:   convertToTypes(t.Types),
		}
	case vm.LoopJoin:
		t := opr.GetJoin()
		v.Arg = &loopjoin.Argument{
			Result: convertToResultPos(t.RelList, t.ColList),
			Cond:   t.Expr,
			Typs:   convertToTypes(t.Types),
		}
	case vm.LoopLeft:
		t := opr.GetLeftJoin()
		v.Arg = &loopleft.Argument{
			Result: convertToResultPos(t.RelList, t.ColList),
			Cond:   t.Expr,
			Typs:   convertToTypes(t.Types),
		}
	case vm.LoopSemi:
		t := opr.GetSemiJoin()
		v.Arg = &loopsemi.Argument{
			Result: t.Result,
			Cond:   t.Expr,
			Typs:   convertToTypes(t.Types),
		}
	case vm.LoopSingle:
		t := opr.GetSingleJoin()
		v.Arg = &loopsingle.Argument{
			Result: convertToResultPos(t.RelList, t.ColList),
			Cond:   t.Expr,
			Typs:   convertToTypes(t.Types),
		}
	case vm.LoopMark:
		t := opr.GetMarkJoin()
		v.Arg = &loopmark.Argument{
			Result: t.Result,
			Cond:   t.Expr,
			Typs:   convertToTypes(t.Types),
		}
	case vm.Offset:
		v.Arg = &offset.Argument{Offset: opr.Offset}
	case vm.Order:
		v.Arg = &order.Argument{Fs: opr.OrderBy}
	case vm.Product:
		t := opr.GetProduct()
		v.Arg = &product.Argument{
			Result: convertToResultPos(t.RelList, t.ColList),
			Typs:   convertToTypes(t.Types),
		}
	case vm.Projection:
		v.Arg = &projection.Argument{Es: opr.ProjectList}
	case vm.Restrict:
		v.Arg = &restrict.Argument{E: opr.Filter}
	case vm.Semi:
		t := opr.GetSemiJoin()
		v.Arg = &semi.Argument{
			Ibucket:    t.Ibucket,
			Nbucket:    t.Nbucket,
			Result:     t.Result,
			Cond:       t.Expr,
			Typs:       convertToTypes(t.Types),
			Conditions: [][]*plan.Expr{t.LeftCond, t.RightCond},
		}
	case vm.Single:
		t := opr.GetSingleJoin()
		v.Arg = &single.Argument{
			Ibucket:    t.Ibucket,
			Nbucket:    t.Nbucket,
			Result:     convertToResultPos(t.RelList, t.ColList),
			Cond:       t.Expr,
			Typs:       convertToTypes(t.Types),
			Conditions: [][]*plan.Expr{t.LeftCond, t.RightCond},
		}
	case vm.Mark:
		t := opr.GetMarkJoin()
		v.Arg = &mark.Argument{
			Ibucket:    t.Ibucket,
			Nbucket:    t.Nbucket,
			Result:     t.Result,
			Conditions: [][]*plan.Expr{t.LeftCond, t.RightCond},
			Typs:       convertToTypes(t.Types),
			Cond:       t.Expr,
			OnList:     t.OnList,
		}
	case vm.Top:
		v.Arg = &top.Argument{
			Limit: int64(opr.Limit),
			Fs:    opr.OrderBy,
		}
	// should change next day?
	case vm.Intersect:
		t := opr.GetAnti()
		v.Arg = &intersect.Argument{
			IBucket: t.Ibucket,
			NBucket: t.Nbucket,
		}
	case vm.IntersectAll:
		t := opr.GetAnti()
		v.Arg = &intersectall.Argument{
			IBucket: t.Ibucket,
			NBucket: t.Nbucket,
		}
	case vm.Minus:
		t := opr.GetAnti()
		v.Arg = &minus.Argument{
			IBucket: t.Ibucket,
			NBucket: t.Nbucket,
		}
	case vm.Connector:
		t := opr.GetConnect()
		v.Arg = &connector.Argument{
			Reg: ctx.root.getRegister(t.PipelineId, t.ConnectorIndex),
		}
	case vm.Merge:
		v.Arg = &merge.Argument{}
	case vm.MergeGroup:
		v.Arg = &mergegroup.Argument{
			NeedEval: opr.Agg.NeedEval,
		}
	case vm.MergeLimit:
		v.Arg = &mergelimit.Argument{
			Limit: opr.Limit,
		}
	case vm.MergeOffset:
		v.Arg = &mergeoffset.Argument{
			Offset: opr.Offset,
		}
	case vm.MergeTop:
		v.Arg = &mergetop.Argument{
			Limit: int64(opr.Limit),
			Fs:    opr.OrderBy,
		}
	case vm.MergeOrder:
		v.Arg = &mergeorder.Argument{
			Fs: opr.OrderBy,
		}
	case vm.TableFunction:
		v.Arg = &table_function.Argument{
			Attrs:  opr.TableFunction.Attrs,
			Rets:   opr.TableFunction.Rets,
			Args:   opr.TableFunction.Args,
			Name:   opr.TableFunction.Name,
			Params: opr.TableFunction.Params,
		}
	case vm.HashBuild:
		t := opr.GetHashBuild()
		v.Arg = &hashbuild.Argument{
			Ibucket:     t.Ibucket,
			Nbucket:     t.Nbucket,
			NeedHashMap: t.NeedHash,
			NeedExpr:    t.NeedExpr,
			Typs:        convertToTypes(t.Types),
			Conditions:  t.Conds,
		}
	case vm.External:
		t := opr.GetExternalScan()
		name2ColIndex := make(map[string]int32)
		for _, n2i := range t.Name2ColIndex {
			name2ColIndex[n2i.Name] = n2i.Index
		}
		v.Arg = &external.Argument{
			Es: &external.ExternalParam{
				ExParamConst: external.ExParamConst{
					Attrs:         t.Attrs,
					Cols:          t.Cols,
					CreateSql:     t.CreateSql,
					Name2ColIndex: name2ColIndex,
					FileList:      t.FileList,
					OriginCols:    t.OriginCols,
				},
				ExParam: external.ExParam{
					Fileparam: new(external.ExFileparam),
				},
			},
		}
	default:
		return v, moerr.NewInternalErrorNoCtx(fmt.Sprintf("unexpected operator: %v", opr.Op))
	}
	return v, nil
}

// newCompile generates a new compile for remote run.
// TODO: give addr to compile?
func newCompile(ctx context.Context, message morpc.Message, pHelper *processHelper, mHelper *messageHandleHelper, cs morpc.ClientSession) *Compile {
	// compile is almost surely wanting a small or mid pool.  Later.
	mp, err := mpool.NewMPool("compile", 0, mpool.NoFixed)
	if err != nil {
		panic(err)
	}
	proc := process.New(
		ctx, mp,
		pHelper.txnClient,
		pHelper.txnOperator,
		mHelper.fileService,
		mHelper.getClusterDetails,
	)
	proc.UnixTime = pHelper.unixTime
	proc.Id = pHelper.id
	proc.Lim = pHelper.lim
	proc.SessionInfo = pHelper.sessionInfo
	proc.AnalInfos = make([]*process.AnalyzeInfo, len(pHelper.analysisNodeList))
	for i := range proc.AnalInfos {
		proc.AnalInfos[i].NodeId = pHelper.analysisNodeList[i]
	}
	proc.DispatchNotifyCh = make(chan process.WrapCs, 1)

	c := &Compile{
		ctx:  ctx,
		proc: proc,
		e:    mHelper.storeEngine,
		anal: &anaylze{},
		addr: colexec.CnAddr,
	}

	c.fill = func(_ any, b *batch.Batch) error {
		return sendBackBatchToCnClient(ctx, b, message.GetID(), mHelper, cs)
	}
	return c
}

func mergeAnalyseInfo(target *anaylze, ana *pipeline.AnalysisList) {
	source := ana.List
	if len(target.analInfos) != len(source) {
		return
	}
	for i := range target.analInfos {
		n := source[i]
		target.analInfos[i].OutputSize += n.OutputSize
		target.analInfos[i].OutputRows += n.OutputRows
		target.analInfos[i].InputRows += n.InputRows
		target.analInfos[i].InputSize += n.InputSize
		target.analInfos[i].MemorySize += n.MemorySize
		target.analInfos[i].TimeConsumed += n.TimeConsumed
		target.analInfos[i].WaitTimeConsumed += n.WaitTimeConsumed
		target.analInfos[i].DiskIO += n.DiskIO
		target.analInfos[i].S3IOByte += n.S3IOByte
		target.analInfos[i].S3IOCount += n.S3IOCount
		target.analInfos[i].NetworkIO += n.NetworkIO
		target.analInfos[i].ScanTime += n.ScanTime
		target.analInfos[i].InsertTime += n.InsertTime
	}
}

// convert []types.Type to []*plan.Type
func convertToPlanTypes(ts []types.Type) []*plan.Type {
	result := make([]*plan.Type, len(ts))
	for i, t := range ts {
		result[i] = &plan.Type{
			Id:        int32(t.Oid),
			Width:     t.Width,
			Precision: t.Precision,
			Size:      t.Size,
			Scale:     t.Scale,
		}
	}
	return result
}

// convert []*plan.Type to []types.Type
func convertToTypes(ts []*plan.Type) []types.Type {
	result := make([]types.Type, len(ts))
	for i, t := range ts {
		result[i] = types.Type{
			Oid:       types.T(t.Id),
			Width:     t.Width,
			Precision: t.Precision,
			Size:      t.Size,
			Scale:     t.Scale,
		}
	}
	return result
}

// convert []agg.Aggregate to []*pipeline.Aggregate
func convertToPipelineAggregates(ags []agg.Aggregate) []*pipeline.Aggregate {
	result := make([]*pipeline.Aggregate, len(ags))
	for i, a := range ags {
		result[i] = &pipeline.Aggregate{
			Op:   int32(a.Op),
			Dist: a.Dist,
			Expr: a.E,
		}
	}
	return result
}

// convert []*pipeline.Aggregate to []agg.Aggregate
func convertToAggregates(ags []*pipeline.Aggregate) []agg.Aggregate {
	result := make([]agg.Aggregate, len(ags))
	for i, a := range ags {
		result[i] = agg.Aggregate{
			Op:   int(a.Op),
			Dist: a.Dist,
			E:    a.Expr,
		}
	}
	return result
}

// get relation list and column list from []colexec.ResultPos
func getRelColList(resultPos []colexec.ResultPos) (relList []int32, colList []int32) {
	relList = make([]int32, len(resultPos))
	colList = make([]int32, len(resultPos))
	for i := range resultPos {
		relList[i], colList[i] = resultPos[i].Rel, resultPos[i].Pos
	}
	return
}

// generate []colexec.ResultPos from relation list and column list
func convertToResultPos(relList, colList []int32) []colexec.ResultPos {
	res := make([]colexec.ResultPos, len(relList))
	for i := range res {
		res[i].Rel, res[i].Pos = relList[i], colList[i]
	}
	return res
}

// convert process.Limitation to pipeline.ProcessLimitation
func convertToPipelineLimitation(lim process.Limitation) *pipeline.ProcessLimitation {
	return &pipeline.ProcessLimitation{
		Size:          lim.Size,
		BatchRows:     lim.BatchRows,
		BatchSize:     lim.BatchSize,
		PartitionRows: lim.PartitionRows,
		ReaderSize:    lim.ReaderSize,
	}
}

// convert pipeline.ProcessLimitation to process.Limitation
func convertToProcessLimitation(lim *pipeline.ProcessLimitation) process.Limitation {
	return process.Limitation{
		Size:          lim.Size,
		BatchRows:     lim.BatchRows,
		BatchSize:     lim.BatchSize,
		PartitionRows: lim.PartitionRows,
		ReaderSize:    lim.ReaderSize,
	}
}

// convert pipeline.SessionInfo to process.SessionInfo
func convertToProcessSessionInfo(sei *pipeline.SessionInfo) (process.SessionInfo, error) {
	sessionInfo := process.SessionInfo{
		User:         sei.User,
		Host:         sei.Host,
		Role:         sei.Role,
		ConnectionID: sei.ConnectionId,
		Database:     sei.Database,
		Version:      sei.Version,
		Account:      sei.Account,
	}
	t := time.Time{}
	err := t.UnmarshalBinary(sei.TimeZone)
	if err != nil {
		return sessionInfo, nil
	}
	sessionInfo.TimeZone = t.Location()
	return sessionInfo, nil
}

func convertToPlanAnalyzeInfo(info *process.AnalyzeInfo) *plan.AnalyzeInfo {
	return &plan.AnalyzeInfo{
		InputRows:        info.InputRows,
		OutputRows:       info.OutputRows,
		InputSize:        info.InputSize,
		OutputSize:       info.OutputSize,
		TimeConsumed:     info.TimeConsumed,
		MemorySize:       info.MemorySize,
		WaitTimeConsumed: info.WaitTimeConsumed,
		DiskIO:           info.DiskIO,
		S3IOByte:         info.S3IOByte,
		S3IOCount:        info.S3IOCount,
		NetworkIO:        info.NetworkIO,
		ScanTime:         info.ScanTime,
		InsertTime:       info.InsertTime,
	}
}

// func decodeBatch(proc *process.Process, data []byte) (*batch.Batch, error) {
func decodeBatch(mp *mpool.MPool, data []byte) (*batch.Batch, error) {
	bat := new(batch.Batch)
	//mp := proc.Mp()
	err := types.Decode(data, bat)

	// allocated memory of vec from mPool.
	for i := range bat.Vecs {
		bat.Vecs[i], err = vector.Dup(bat.Vecs[i], mp)
		if err != nil {
			for j := 0; j < i; j++ {
				bat.Vecs[j].Free(mp)
			}
			return nil, err
		}
	}
	// allocated memory of aggVec from mPool.
	for i, ag := range bat.Aggs {
		err = ag.WildAggReAlloc(mp)
		if err != nil {
			for j := 0; j < i; j++ {
				bat.Aggs[j].Free(mp)
			}
			for j := range bat.Vecs {
				bat.Vecs[j].Free(mp)
			}
			return nil, err
		}
	}
	return bat, err
}

func sendToConnectOperator(arg *connector.Argument, bat *batch.Batch) {
	select {
	case <-arg.Reg.Ctx.Done():
	case arg.Reg.Ch <- bat:
	}
}

func (ctx *scopeContext) getRegister(id, idx int32) *process.WaitRegister {
	if ctx.id == id {
		for k, v := range ctx.regs {
			if v == idx {
				return k
			}
		}
	}
	for i := range ctx.children {
		if reg := ctx.children[i].getRegister(id, idx); reg != nil {
			return reg
		}
	}
	return nil
}

func (ctx *scopeContext) findRegister(reg *process.WaitRegister) (int32, *scopeContext) {
	if idx, ok := ctx.regs[reg]; ok {
		return idx, ctx
	}
	for i := range ctx.children {
		if idx, ctx := ctx.children[i].findRegister(reg); idx >= 0 {
			return idx, ctx
		}
	}
	return -1, nil
}

func (ctx *scopeContext) addSubPipeline(id uint64, idx int32, ctxId int32) (int32, error) {
	ds := &Scope{Magic: Pushdown}
	ds.Proc = process.NewWithAnalyze(ctx.scope.Proc, ctx.scope.Proc.Ctx, 0, nil)
	ds.DataSource = &Source{
		PushdownId:   id,
		PushdownAddr: colexec.CnAddr,
	}
	ds.appendInstruction(vm.Instruction{
		Op: vm.Connector,
		Arg: &connector.Argument{
			Reg: ctx.scope.Proc.Reg.MergeReceivers[idx],
		},
	})
	ctx.scope.PreScopes = append(ctx.scope.PreScopes, ds)
	p := &pipeline.Pipeline{}
	p.PipelineId = ctxId
	p.PipelineType = Pushdown
	ctxId++
	p.DataSource = &pipeline.Source{
		PushdownId:   id,
		PushdownAddr: colexec.CnAddr,
	}
	p.InstructionList = append(p.InstructionList, &pipeline.Instruction{
		Op: vm.Connector,
		Connect: &pipeline.Connector{
			ConnectorIndex: idx,
			PipelineId:     ctx.id,
		},
	})
	ctx.pipe.Children = append(ctx.pipe.Children, p)
	return ctxId, nil
}

func (ctx *scopeContext) isDescendant(dsc *scopeContext) bool {
	if ctx.id == dsc.id {
		return true
	}
	for i := range ctx.children {
		if ctx.children[i].isDescendant(dsc) {
			return true
		}
	}
	return false
}

func (ctx *scopeContext) isRemote(targetContext *scopeContext, depth int) bool {
	if targetContext.scope.Magic != Remote {
		return false
	}
	if ctx.id == targetContext.id && depth == 0 {
		return true
	}
	for i := range ctx.children {
		if ctx.children[i].scope.Magic == Remote {
			if ctx.children[i].isRemote(targetContext, depth+1) {
				return true
			}
		} else {
			if ctx.children[i].isRemote(targetContext, depth) {
				return true
			}
		}
	}
	return false
}

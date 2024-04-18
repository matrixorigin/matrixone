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
	"time"
	"unsafe"

	v2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"

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
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/queryservice"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/agg"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/anti"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/connector"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/deletion"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/dispatch"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/external"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/fuzzyfilter"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/group"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/hashbuild"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/insert"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/intersect"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/intersectall"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/join"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/left"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/limit"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/lockop"
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
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/mergerecursive"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/mergetop"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/minus"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/multi_col/group_concat"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/offset"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/onduplicatekey"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/order"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/output"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/preinsert"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/preinsertsecondaryindex"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/preinsertunique"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/product"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/projection"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/restrict"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/right"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/rightanti"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/rightsemi"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/sample"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/semi"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/shuffle"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/single"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/stream"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/table_function"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/top"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/value_scan"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/udf"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"go.uber.org/zap"
)

// CnServerMessageHandler is responsible for processing the cn-client message received at cn-server.
// The message is always *pipeline.Message here. It's a byte array encoded by method encodeScope.
func CnServerMessageHandler(
	ctx context.Context,
	cnAddr string,
	message morpc.Message,
	cs morpc.ClientSession,
	storeEngine engine.Engine,
	fileService fileservice.FileService,
	lockService lockservice.LockService,
	queryService queryservice.QueryService,
	hakeeper logservice.CNHAKeeperClient,
	udfService udf.Service,
	cli client.TxnClient,
	aicm *defines.AutoIncrCacheManager,
	messageAcquirer func() morpc.Message) (err error) {
	defer func() {
		if e := recover(); e != nil {
			err = moerr.ConvertPanicError(ctx, e)
			getLogger().Error("panic in cn message handler",
				zap.String("error", err.Error()))
			err = errors.Join(err, cs.Close())
		}
	}()
	start := time.Now()
	defer func() {
		v2.PipelineServerDurationHistogram.Observe(time.Since(start).Seconds())
	}()

	msg, ok := message.(*pipeline.Message)
	if !ok {
		logutil.Errorf("cn server should receive *pipeline.Message, but get %v", message)
		panic("cn server receive a message with unexpected type")
	}

	receiver := newMessageReceiverOnServer(ctx, cnAddr, msg,
		cs, messageAcquirer, storeEngine, fileService, lockService, queryService, hakeeper, udfService, cli, aicm)

	// rebuild pipeline to run and send the query result back.
	err = cnMessageHandle(&receiver)
	if err != nil {
		return receiver.sendError(err)
	}
	return receiver.sendEndMessage()
}

// cnMessageHandle deal the received message at cn-server.
func cnMessageHandle(receiver *messageReceiverOnServer) error {
	switch receiver.messageTyp {
	case pipeline.Method_PrepareDoneNotifyMessage: // notify the dispatch executor
		dispatchProc, err := receiver.GetProcByUuid(receiver.messageUuid)
		if err != nil || dispatchProc == nil {
			return err
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
		c := receiver.newCompile()
		defer func() {
			mpool.DeleteMPool(c.proc.Mp())
		}()

		// decode and rewrite the scope.
		s, err := decodeScope(receiver.scopeData, c.proc, true, c.e)
		if err != nil {
			return err
		}
		s = appendWriteBackOperator(c, s)
		s.SetContextRecursively(c.ctx)

		err = s.ParallelRun(c, s.IsRemote)
		if err == nil {
			// record the number of s3 requests
			c.proc.AnalInfos[c.anal.curr].S3IOInputCount += c.counterSet.FileService.S3.Put.Load()
			c.proc.AnalInfos[c.anal.curr].S3IOInputCount += c.counterSet.FileService.S3.List.Load()
			c.proc.AnalInfos[c.anal.curr].S3IOOutputCount += c.counterSet.FileService.S3.Head.Load()
			c.proc.AnalInfos[c.anal.curr].S3IOOutputCount += c.counterSet.FileService.S3.Get.Load()
			c.proc.AnalInfos[c.anal.curr].S3IOOutputCount += c.counterSet.FileService.S3.Delete.Load()
			c.proc.AnalInfos[c.anal.curr].S3IOOutputCount += c.counterSet.FileService.S3.DeleteMulti.Load()

			receiver.finalAnalysisInfo = c.proc.AnalInfos
		}
		c.proc.FreeVectors()
		c.proc.CleanValueScanBatchs()
		return err

	default:
		return moerr.NewInternalError(receiver.ctx, "unknown message type")
	}
}

// receiveMessageFromCnServer deal the back message from cn-server.
func receiveMessageFromCnServer(c *Compile, s *Scope, sender *messageSenderOnClient, lastInstruction vm.Instruction) error {
	var bat *batch.Batch
	var end bool
	var err error

	lastAnalyze := c.proc.GetAnalyze(lastInstruction.Idx, -1, false)
	if sender.receiveCh == nil {
		sender.receiveCh, err = sender.streamSender.Receive()
		if err != nil {
			return err
		}
	}

	var lastArg vm.Operator
	var oldChild []vm.Operator
	switch arg := lastInstruction.Arg.(type) {
	case *connector.Argument:
		lastArg = arg
		oldChild = arg.Children
		arg.Children = nil
		defer func() {
			arg.Children = oldChild
		}()
	case *dispatch.Argument:
		lastArg = arg
		oldChild = arg.Children
		defer func() {
			arg.Children = oldChild
		}()
	default:
		return moerr.NewInvalidInput(c.ctx, "last operator should only be connector or dispatcher")
	}

	valueScanOperator := &value_scan.Argument{}
	info := &vm.OperatorInfo{
		Idx:     -1,
		IsFirst: false,
		IsLast:  false,
	}
	lastArg.SetInfo(info)
	lastArg.AppendChild(valueScanOperator)
	for {
		bat, end, err = sender.receiveBatch()
		if err != nil {
			return err
		}
		if end {
			return nil
		}

		lastAnalyze.Network(bat)
		valueScanOperator.Batchs = append(valueScanOperator.Batchs, bat)
		result, errCall := lastArg.Call(s.Proc)
		if errCall != nil || result.Status == vm.ExecStop {
			valueScanOperator.Free(s.Proc, false, errCall)
			return errCall
		}
		valueScanOperator.Free(s.Proc, false, errCall)
	}
}

// remoteRun sends a scope for remote running and receives the results.
// The back result message is always *pipeline.Message contains three cases.
// 1. Message with error information
// 2. Message with an end flag and analysis result
// 3. Batch Message with batch data
func (s *Scope) remoteRun(c *Compile) (err error) {
	// encode the scope but without the last operator.
	// the last operator will be executed on the current node for receiving the result and send them to the next pipeline.
	lastIdx := len(s.Instructions) - 1
	lastInstruction := s.Instructions[lastIdx]

	if lastInstruction.Op == vm.Connector || lastInstruction.Op == vm.Dispatch {
		if err = lastInstruction.Arg.Prepare(s.Proc); err != nil {
			return err
		}
	} else {
		return moerr.NewInvalidInput(c.ctx, "last operator should only be connector or dispatcher")
	}

	s.Instructions = s.Instructions[:lastIdx]
	sData, errEncode := encodeScope(s)
	if errEncode != nil {
		return errEncode
	}
	s.Instructions = append(s.Instructions, lastInstruction)

	// encode the process related information
	pData, errEncodeProc := encodeProcessInfo(s.Proc, c.sql)
	if errEncodeProc != nil {
		return errEncodeProc
	}

	// new sender and do send work.
	sender, err := newMessageSenderOnClient(s.Proc.Ctx, c, s.NodeInfo.Addr)
	if err != nil {
		logutil.Errorf("Failed to newMessageSenderOnClient sql=%s, txnID=%s, err=%v",
			c.sql, c.proc.TxnOperator.Txn().DebugString(), err)
		return err
	}
	defer sender.close()
	err = sender.send(sData, pData, pipeline.Method_PipelineMessage)
	if err != nil {
		return err
	}

	return receiveMessageFromCnServer(c, s, sender, lastInstruction)
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
func decodeScope(data []byte, proc *process.Process, isRemote bool, eng engine.Engine) (*Scope, error) {
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
	s, err := generateScope(proc, p, ctx, proc.AnalInfos, isRemote)
	if err != nil {
		return nil, err
	}
	if err := fillInstructionsForScope(s, ctx, p, eng); err != nil {
		return nil, err
	}

	return s, nil
}

// encodeProcessInfo get needed information from proc, and do serialization work.
func encodeProcessInfo(proc *process.Process, sql string) ([]byte, error) {
	procInfo := &pipeline.ProcessInfo{}
	if len(proc.AnalInfos) == 0 {
		getLogger().Error("empty plan", zap.String("sql", sql))
	}
	{
		procInfo.Id = proc.Id
		procInfo.Sql = sql
		procInfo.Lim = convertToPipelineLimitation(proc.Lim)
		procInfo.UnixTime = proc.UnixTime
		accountId, err := defines.GetAccountId(proc.Ctx)
		if err != nil {
			return nil, err
		}
		procInfo.AccountId = accountId
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
			QueryId:      proc.SessionInfo.QueryId,
		}
	}
	return procInfo.Marshal()
}

func appendWriteBackOperator(c *Compile, s *Scope) *Scope {
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
	p.IsLoad = s.IsLoad
	p.UuidsToRegIdx = convertScopeRemoteReceivInfo(s)
	p.BuildIdx = int32(s.BuildIdx)
	p.ShuffleCnt = int32(s.ShuffleCnt)

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
			SchemaName:             s.DataSource.SchemaName,
			TableName:              s.DataSource.RelationName,
			ColList:                s.DataSource.Attributes,
			PushdownId:             s.DataSource.PushdownId,
			PushdownAddr:           s.DataSource.PushdownAddr,
			Expr:                   s.DataSource.Expr,
			TableDef:               s.DataSource.TableDef,
			Timestamp:              &s.DataSource.Timestamp,
			RuntimeFilterProbeList: s.DataSource.RuntimeFilterSpecs,
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
	for i := range p.UuidsToRegIdx {
		op := p.UuidsToRegIdx[i]
		uid, err := uuid.FromBytes(op.GetUuid())
		if err != nil {
			return moerr.NewInternalErrorNoCtx("decode uuid failed: %s\n", err)
		}
		s.RemoteReceivRegInfos[i] = RemoteReceivRegInfo{
			Idx:      int(op.GetIdx()),
			Uuid:     uid,
			FromAddr: op.FromAddr,
		}
	}
	return nil
}

func convertScopeRemoteReceivInfo(s *Scope) (ret []*pipeline.UuidToRegIdx) {
	ret = make([]*pipeline.UuidToRegIdx, len(s.RemoteReceivRegInfos))
	for i := range s.RemoteReceivRegInfos {
		op := &s.RemoteReceivRegInfos[i]
		uid, _ := op.Uuid.MarshalBinary()
		ret[i] = &pipeline.UuidToRegIdx{
			Idx:      int32(op.Idx),
			Uuid:     uid,
			FromAddr: op.FromAddr,
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
		Magic:      magicType(p.GetPipelineType()),
		IsEnd:      p.IsEnd,
		IsJoin:     p.IsJoin,
		IsLoad:     p.IsLoad,
		Plan:       ctx.plan,
		IsRemote:   isRemote,
		BuildIdx:   int(p.BuildIdx),
		ShuffleCnt: int(p.ShuffleCnt),
	}
	if err := convertPipelineUuid(p, s); err != nil {
		return s, err
	}
	dsc := p.GetDataSource()
	if dsc != nil {
		s.DataSource = &Source{
			SchemaName:         dsc.SchemaName,
			RelationName:       dsc.TableName,
			Attributes:         dsc.ColList,
			PushdownId:         dsc.PushdownId,
			PushdownAddr:       dsc.PushdownAddr,
			Expr:               dsc.Expr,
			TableDef:           dsc.TableDef,
			Timestamp:          *dsc.Timestamp,
			RuntimeFilterSpecs: dsc.RuntimeFilterProbeList,
		}
		if len(dsc.Block) > 0 {
			bat := new(batch.Batch)
			if err := types.Decode([]byte(dsc.Block), bat); err != nil {
				return nil, err
			}
			bat.Cnt = 1
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
func fillInstructionsForScope(s *Scope, ctx *scopeContext, p *pipeline.Pipeline, eng engine.Engine) error {
	var err error

	for i := range s.PreScopes {
		if err = fillInstructionsForScope(s.PreScopes[i], ctx.children[i], p.Children[i], eng); err != nil {
			return err
		}
	}
	s.Instructions = make([]vm.Instruction, len(p.InstructionList))
	for i := range s.Instructions {
		if s.Instructions[i], err = convertToVmInstruction(p.InstructionList[i], ctx, eng); err != nil {
			return err
		}
	}
	if s.isShuffle() {
		for _, rr := range s.Proc.Reg.MergeReceivers {
			rr.Ch = make(chan *batch.Batch, 16)
		}
	}
	return nil
}

// convert vm.Instruction to pipeline.Instruction
// todo: bad design, need to be refactored. and please refer to how sample operator do.
func convertToPipelineInstruction(opr *vm.Instruction, ctx *scopeContext, ctxId int32) (int32, *pipeline.Instruction, error) {
	in := &pipeline.Instruction{Op: int32(opr.Op), Idx: int32(opr.Idx), IsFirst: opr.IsFirst, IsLast: opr.IsLast}
	switch t := opr.Arg.(type) {
	case *insert.Argument:
		in.Insert = &pipeline.Insert{
			ToWriteS3:           t.ToWriteS3,
			Ref:                 t.InsertCtx.Ref,
			Attrs:               t.InsertCtx.Attrs,
			AddAffectedRows:     t.InsertCtx.AddAffectedRows,
			PartitionTableIds:   t.InsertCtx.PartitionTableIDs,
			PartitionTableNames: t.InsertCtx.PartitionTableNames,
			PartitionIdx:        int32(t.InsertCtx.PartitionIndexInBatch),
			TableDef:            t.InsertCtx.TableDef,
		}
	case *deletion.Argument:
		in.Delete = &pipeline.Deletion{
			Ts:           t.Ts,
			AffectedRows: t.AffectedRows(),
			RemoteDelete: t.RemoteDelete,
			SegmentMap:   t.SegmentMap,
			IBucket:      t.IBucket,
			NBucket:      t.Nbucket,
			// deleteCtx
			RowIdIdx:              int32(t.DeleteCtx.RowIdIdx),
			PartitionTableIds:     t.DeleteCtx.PartitionTableIDs,
			PartitionTableNames:   t.DeleteCtx.PartitionTableNames,
			PartitionIndexInBatch: int32(t.DeleteCtx.PartitionIndexInBatch),
			AddAffectedRows:       t.DeleteCtx.AddAffectedRows,
			Ref:                   t.DeleteCtx.Ref,
			PrimaryKeyIdx:         int32(t.DeleteCtx.PrimaryKeyIdx),
		}
	case *onduplicatekey.Argument:
		in.OnDuplicateKey = &pipeline.OnDuplicateKey{
			TableDef:        t.TableDef,
			OnDuplicateIdx:  t.OnDuplicateIdx,
			OnDuplicateExpr: t.OnDuplicateExpr,
		}
	case *fuzzyfilter.Argument:
		in.FuzzyFilter = &pipeline.FuzzyFilter{
			N:                      float32(t.N),
			PkName:                 t.PkName,
			PkTyp:                  plan2.DeepCopyType(t.PkTyp),
			RuntimeFilterBuildList: t.RuntimeFilterSpecs,
		}
	case *preinsert.Argument:
		in.PreInsert = &pipeline.PreInsert{
			SchemaName: t.SchemaName,
			TableDef:   t.TableDef,
			HasAutoCol: t.HasAutoCol,
			IsUpdate:   t.IsUpdate,
			Attrs:      t.Attrs,
		}
	case *lockop.Argument:
		in.LockOp = &pipeline.LockOp{
			Block:   t.Block(),
			Targets: t.CopyToPipelineTarget(),
		}
	case *preinsertunique.Argument:
		in.PreInsertUnique = &pipeline.PreInsertUnique{
			PreInsertUkCtx: t.PreInsertCtx,
		}
	case *preinsertsecondaryindex.Argument:
		in.PreInsertSecondaryIndex = &pipeline.PreInsertSecondaryIndex{
			PreInsertSkCtx: t.PreInsertCtx,
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
			HashOnPk:  t.HashOnPK,
			IsShuffle: t.IsShuffle,
		}
	case *shuffle.Argument:
		in.Shuffle = &pipeline.Shuffle{}
		in.Shuffle.ShuffleColIdx = t.ShuffleColIdx
		in.Shuffle.ShuffleType = t.ShuffleType
		in.Shuffle.ShuffleColMax = t.ShuffleColMax
		in.Shuffle.ShuffleColMin = t.ShuffleColMin
		in.Shuffle.AliveRegCnt = t.AliveRegCnt
	case *dispatch.Argument:
		in.Dispatch = &pipeline.Dispatch{IsSink: t.IsSink, ShuffleType: t.ShuffleType, RecSink: t.RecSink, FuncId: int32(t.FuncId)}
		in.Dispatch.ShuffleRegIdxLocal = make([]int32, len(t.ShuffleRegIdxLocal))
		for i := range t.ShuffleRegIdxLocal {
			in.Dispatch.ShuffleRegIdxLocal[i] = int32(t.ShuffleRegIdxLocal[i])
		}
		in.Dispatch.ShuffleRegIdxRemote = make([]int32, len(t.ShuffleRegIdxRemote))
		for i := range t.ShuffleRegIdxRemote {
			in.Dispatch.ShuffleRegIdxRemote[i] = int32(t.ShuffleRegIdxRemote[i])
		}

		in.Dispatch.LocalConnector = make([]*pipeline.Connector, len(t.LocalRegs))
		for i := range t.LocalRegs {
			idx, ctx0 := ctx.root.findRegister(t.LocalRegs[i])
			in.Dispatch.LocalConnector[i] = &pipeline.Connector{
				ConnectorIndex: idx,
				PipelineId:     ctx0.id,
			}
		}

		if len(t.RemoteRegs) > 0 {
			in.Dispatch.RemoteConnector = make([]*pipeline.WrapNode, len(t.RemoteRegs))
			for i := range t.RemoteRegs {
				wn := &pipeline.WrapNode{
					NodeAddr: t.RemoteRegs[i].NodeAddr,
					Uuid:     t.RemoteRegs[i].Uuid[:],
				}
				in.Dispatch.RemoteConnector[i] = wn
			}
		}
	case *group.Argument:
		in.Agg = &pipeline.Group{
			IsShuffle:    t.IsShuffle,
			PreAllocSize: t.PreAllocSize,
			NeedEval:     t.NeedEval,
			Ibucket:      t.Ibucket,
			Nbucket:      t.Nbucket,
			Exprs:        t.Exprs,
			Types:        convertToPlanTypes(t.Types),
			Aggs:         convertToPipelineAggregates(t.Aggs),
			MultiAggs:    convertPipelineMultiAggs(t.MultiAggs),
		}
	case *sample.Argument:
		t.ConvertToPipelineOperator(in)

	case *join.Argument:
		relList, colList := getRelColList(t.Result)
		in.Join = &pipeline.Join{
			Ibucket:                t.Ibucket,
			Nbucket:                t.Nbucket,
			RelList:                relList,
			ColList:                colList,
			Expr:                   t.Cond,
			Types:                  convertToPlanTypes(t.Typs),
			LeftCond:               t.Conditions[0],
			RightCond:              t.Conditions[1],
			RuntimeFilterBuildList: t.RuntimeFilterSpecs,
			HashOnPk:               t.HashOnPK,
			IsShuffle:              t.IsShuffle,
		}
	case *left.Argument:
		relList, colList := getRelColList(t.Result)
		in.LeftJoin = &pipeline.LeftJoin{
			Ibucket:                t.Ibucket,
			Nbucket:                t.Nbucket,
			RelList:                relList,
			ColList:                colList,
			Expr:                   t.Cond,
			Types:                  convertToPlanTypes(t.Typs),
			LeftCond:               t.Conditions[0],
			RightCond:              t.Conditions[1],
			RuntimeFilterBuildList: t.RuntimeFilterSpecs,
			HashOnPk:               t.HashOnPK,
			IsShuffle:              t.IsShuffle,
		}
	case *right.Argument:
		rels, poses := getRelColList(t.Result)
		in.RightJoin = &pipeline.RightJoin{
			Ibucket:                t.Ibucket,
			Nbucket:                t.Nbucket,
			RelList:                rels,
			ColList:                poses,
			Expr:                   t.Cond,
			LeftTypes:              convertToPlanTypes(t.LeftTypes),
			RightTypes:             convertToPlanTypes(t.RightTypes),
			LeftCond:               t.Conditions[0],
			RightCond:              t.Conditions[1],
			RuntimeFilterBuildList: t.RuntimeFilterSpecs,
			HashOnPk:               t.HashOnPK,
			IsShuffle:              t.IsShuffle,
		}
	case *rightsemi.Argument:
		in.RightSemiJoin = &pipeline.RightSemiJoin{
			Ibucket:                t.Ibucket,
			Nbucket:                t.Nbucket,
			Result:                 t.Result,
			Expr:                   t.Cond,
			RightTypes:             convertToPlanTypes(t.RightTypes),
			LeftCond:               t.Conditions[0],
			RightCond:              t.Conditions[1],
			RuntimeFilterBuildList: t.RuntimeFilterSpecs,
			HashOnPk:               t.HashOnPK,
			IsShuffle:              t.IsShuffle,
		}
	case *rightanti.Argument:
		in.RightAntiJoin = &pipeline.RightAntiJoin{
			Ibucket:                t.Ibucket,
			Nbucket:                t.Nbucket,
			Result:                 t.Result,
			Expr:                   t.Cond,
			RightTypes:             convertToPlanTypes(t.RightTypes),
			LeftCond:               t.Conditions[0],
			RightCond:              t.Conditions[1],
			RuntimeFilterBuildList: t.RuntimeFilterSpecs,
			HashOnPk:               t.HashOnPK,
			IsShuffle:              t.IsShuffle,
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
	case *loopmark.Argument:
		in.MarkJoin = &pipeline.MarkJoin{
			Expr:   t.Cond,
			Types:  convertToPlanTypes(t.Typs),
			Result: t.Result,
		}
	case *offset.Argument:
		in.Offset = t.Offset
	case *order.Argument:
		in.OrderBy = t.OrderBySpec
	case *product.Argument:
		relList, colList := getRelColList(t.Result)
		in.Product = &pipeline.Product{
			RelList:   relList,
			ColList:   colList,
			Types:     convertToPlanTypes(t.Typs),
			IsShuffle: t.IsShuffle,
		}
	case *projection.Argument:
		in.ProjectList = t.Es
	case *restrict.Argument:
		in.Filter = t.E
	case *semi.Argument:
		in.SemiJoin = &pipeline.SemiJoin{
			Ibucket:                t.Ibucket,
			Nbucket:                t.Nbucket,
			Result:                 t.Result,
			Expr:                   t.Cond,
			Types:                  convertToPlanTypes(t.Typs),
			LeftCond:               t.Conditions[0],
			RightCond:              t.Conditions[1],
			RuntimeFilterBuildList: t.RuntimeFilterSpecs,
			HashOnPk:               t.HashOnPK,
			IsShuffle:              t.IsShuffle,
		}
	case *single.Argument:
		relList, colList := getRelColList(t.Result)
		in.SingleJoin = &pipeline.SingleJoin{
			Ibucket:                t.Ibucket,
			Nbucket:                t.Nbucket,
			RelList:                relList,
			ColList:                colList,
			Expr:                   t.Cond,
			Types:                  convertToPlanTypes(t.Typs),
			LeftCond:               t.Conditions[0],
			RightCond:              t.Conditions[1],
			RuntimeFilterBuildList: t.RuntimeFilterSpecs,
			HashOnPk:               t.HashOnPK,
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
		in.Merge = &pipeline.Merge{
			SinkScan: t.SinkScan,
		}
	case *mergerecursive.Argument:
	case *mergegroup.Argument:
		in.Agg = &pipeline.Group{
			NeedEval: t.NeedEval,
		}
		EncodeMergeGroup(t, in.Agg)
	case *mergelimit.Argument:
		in.Limit = t.Limit
	case *mergeoffset.Argument:
		in.Offset = t.Offset
	case *mergetop.Argument:
		in.Limit = uint64(t.Limit)
		in.OrderBy = t.Fs
	case *mergeorder.Argument:
		in.OrderBy = t.OrderBySpecs
	case *connector.Argument:
		idx, ctx0 := ctx.root.findRegister(t.Reg)
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
			HashOnPk:  t.HashOnPK,
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
			NeedExpr:        t.NeedExpr,
			NeedHash:        t.NeedHashMap,
			Ibucket:         t.Ibucket,
			Nbucket:         t.Nbucket,
			Types:           convertToPlanTypes(t.Typs),
			Conds:           t.Conditions,
			HashOnPk:        t.HashOnPK,
			NeedMergedBatch: t.NeedMergedBatch,
		}
	case *external.Argument:
		name2ColIndexSlice := make([]*pipeline.ExternalName2ColIndex, len(t.Es.Name2ColIndex))
		i := 0
		for k, v := range t.Es.Name2ColIndex {
			name2ColIndexSlice[i] = &pipeline.ExternalName2ColIndex{Name: k, Index: v}
			i++
		}
		in.ExternalScan = &pipeline.ExternalScan{
			Attrs:           t.Es.Attrs,
			Cols:            t.Es.Cols,
			FileSize:        t.Es.FileSize,
			FileOffsetTotal: t.Es.FileOffsetTotal,
			Name2ColIndex:   name2ColIndexSlice,
			CreateSql:       t.Es.CreateSql,
			FileList:        t.Es.FileList,
			Filter:          t.Es.Filter.FilterExpr,
		}
	case *stream.Argument:
		in.StreamScan = &pipeline.StreamScan{
			TblDef: t.TblDef,
			Limit:  t.Limit,
			Offset: t.Offset,
		}
	default:
		return -1, nil, moerr.NewInternalErrorNoCtx(fmt.Sprintf("unexpected operator: %v", opr.Op))
	}
	return ctxId, in, nil
}

// convert pipeline.Instruction to vm.Instruction
func convertToVmInstruction(opr *pipeline.Instruction, ctx *scopeContext, eng engine.Engine) (vm.Instruction, error) {
	v := vm.Instruction{Op: vm.OpType(opr.Op), Idx: int(opr.Idx), IsFirst: opr.IsFirst, IsLast: opr.IsLast}
	switch v.Op {
	case vm.Deletion:
		t := opr.GetDelete()
		v.Arg = &deletion.Argument{
			Ts:           t.Ts,
			RemoteDelete: t.RemoteDelete,
			SegmentMap:   t.SegmentMap,
			IBucket:      t.IBucket,
			Nbucket:      t.NBucket,
			DeleteCtx: &deletion.DeleteCtx{
				CanTruncate:           t.CanTruncate,
				RowIdIdx:              int(t.RowIdIdx),
				PartitionTableIDs:     t.PartitionTableIds,
				PartitionTableNames:   t.PartitionTableNames,
				PartitionIndexInBatch: int(t.PartitionIndexInBatch),
				Ref:                   t.Ref,
				AddAffectedRows:       t.AddAffectedRows,
				PrimaryKeyIdx:         int(t.PrimaryKeyIdx),
			},
		}
	case vm.Insert:
		t := opr.GetInsert()
		v.Arg = &insert.Argument{
			ToWriteS3: t.ToWriteS3,
			InsertCtx: &insert.InsertCtx{
				Ref:                   t.Ref,
				AddAffectedRows:       t.AddAffectedRows,
				Attrs:                 t.Attrs,
				PartitionTableIDs:     t.PartitionTableIds,
				PartitionTableNames:   t.PartitionTableNames,
				PartitionIndexInBatch: int(t.PartitionIdx),
				TableDef:              t.TableDef,
			},
		}
	case vm.PreInsert:
		t := opr.GetPreInsert()
		v.Arg = &preinsert.Argument{
			SchemaName: t.GetSchemaName(),
			TableDef:   t.GetTableDef(),
			Attrs:      t.GetAttrs(),
			HasAutoCol: t.GetHasAutoCol(),
			IsUpdate:   t.GetIsUpdate(),
		}
	case vm.LockOp:
		t := opr.GetLockOp()
		lockArg := lockop.NewArgument(eng)
		lockArg.SetBlock(t.Block)
		for _, target := range t.Targets {
			typ := plan2.MakeTypeByPlan2Type(target.GetPrimaryColTyp())
			lockArg.AddLockTarget(target.GetTableId(), target.GetPrimaryColIdxInBat(), typ, target.GetRefreshTsIdxInBat())
		}
		for _, target := range t.Targets {
			if target.LockTable {
				lockArg.LockTable(target.TableId, target.ChangeDef)
			}
		}
		v.Arg = lockArg
	case vm.PreInsertUnique:
		t := opr.GetPreInsertUnique()
		v.Arg = &preinsertunique.Argument{
			PreInsertCtx: t.GetPreInsertUkCtx(),
		}
	case vm.PreInsertSecondaryIndex:
		t := opr.GetPreInsertSecondaryIndex()
		v.Arg = &preinsertsecondaryindex.Argument{
			PreInsertCtx: t.GetPreInsertSkCtx(),
		}
	case vm.OnDuplicateKey:
		t := opr.GetOnDuplicateKey()
		v.Arg = &onduplicatekey.Argument{
			TableDef:        t.TableDef,
			OnDuplicateIdx:  t.OnDuplicateIdx,
			OnDuplicateExpr: t.OnDuplicateExpr,
			IsIgnore:        t.IsIgnore,
		}
	case vm.FuzzyFilter:
		t := opr.GetFuzzyFilter()
		v.Arg = &fuzzyfilter.Argument{
			N:                  float64(t.N),
			PkName:             t.PkName,
			PkTyp:              t.PkTyp,
			RuntimeFilterSpecs: t.RuntimeFilterBuildList,
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
			Result:    t.Result,
			HashOnPK:  t.HashOnPk,
			IsShuffle: t.IsShuffle,
		}
	case vm.Shuffle:
		t := opr.GetShuffle()
		v.Arg = &shuffle.Argument{
			ShuffleColIdx: t.ShuffleColIdx,
			ShuffleType:   t.ShuffleType,
			ShuffleColMin: t.ShuffleColMin,
			ShuffleColMax: t.ShuffleColMax,
			AliveRegCnt:   t.AliveRegCnt,
		}
	case vm.Dispatch:
		t := opr.GetDispatch()
		regs := make([]*process.WaitRegister, len(t.LocalConnector))
		for i, cp := range t.LocalConnector {
			regs[i] = ctx.root.getRegister(cp.PipelineId, cp.ConnectorIndex)
		}
		rrs := make([]colexec.ReceiveInfo, 0)
		if len(t.RemoteConnector) > 0 {
			for i := range t.RemoteConnector {
				uid, err := uuid.FromBytes(t.RemoteConnector[i].Uuid)
				if err != nil {
					return v, err
				}
				n := colexec.ReceiveInfo{
					NodeAddr: t.RemoteConnector[i].NodeAddr,
					Uuid:     uid,
				}
				rrs = append(rrs, n)
			}
		}
		shuffleRegIdxLocal := make([]int, len(t.ShuffleRegIdxLocal))
		for i := range t.ShuffleRegIdxLocal {
			shuffleRegIdxLocal[i] = int(t.ShuffleRegIdxLocal[i])
		}
		shuffleRegIdxRemote := make([]int, len(t.ShuffleRegIdxRemote))
		for i := range t.ShuffleRegIdxRemote {
			shuffleRegIdxRemote[i] = int(t.ShuffleRegIdxRemote[i])
		}

		v.Arg = &dispatch.Argument{
			IsSink:              t.IsSink,
			RecSink:             t.RecSink,
			FuncId:              int(t.FuncId),
			LocalRegs:           regs,
			RemoteRegs:          rrs,
			ShuffleType:         t.ShuffleType,
			ShuffleRegIdxLocal:  shuffleRegIdxLocal,
			ShuffleRegIdxRemote: shuffleRegIdxRemote,
		}
	case vm.Group:
		t := opr.GetAgg()
		v.Arg = &group.Argument{
			IsShuffle:    t.IsShuffle,
			PreAllocSize: t.PreAllocSize,
			NeedEval:     t.NeedEval,
			Ibucket:      t.Ibucket,
			Nbucket:      t.Nbucket,
			Exprs:        t.Exprs,
			Types:        convertToTypes(t.Types),
			Aggs:         convertToAggregates(t.Aggs),
			MultiAggs:    convertToMultiAggs(t.MultiAggs),
		}
	case vm.Sample:
		v.Arg = sample.GenerateFromPipelineOperator(opr)

	case vm.Join:
		t := opr.GetJoin()
		v.Arg = &join.Argument{
			Ibucket:            t.Ibucket,
			Nbucket:            t.Nbucket,
			Cond:               t.Expr,
			Typs:               convertToTypes(t.Types),
			Result:             convertToResultPos(t.RelList, t.ColList),
			Conditions:         [][]*plan.Expr{t.LeftCond, t.RightCond},
			RuntimeFilterSpecs: t.RuntimeFilterBuildList,
			HashOnPK:           t.HashOnPk,
			IsShuffle:          t.IsShuffle,
		}
	case vm.Left:
		t := opr.GetLeftJoin()
		v.Arg = &left.Argument{
			Ibucket:            t.Ibucket,
			Nbucket:            t.Nbucket,
			Cond:               t.Expr,
			Typs:               convertToTypes(t.Types),
			Result:             convertToResultPos(t.RelList, t.ColList),
			Conditions:         [][]*plan.Expr{t.LeftCond, t.RightCond},
			RuntimeFilterSpecs: t.RuntimeFilterBuildList,
			HashOnPK:           t.HashOnPk,
			IsShuffle:          t.IsShuffle,
		}
	case vm.Right:
		t := opr.GetRightJoin()
		v.Arg = &right.Argument{
			Ibucket:            t.Ibucket,
			Nbucket:            t.Nbucket,
			Result:             convertToResultPos(t.RelList, t.ColList),
			LeftTypes:          convertToTypes(t.LeftTypes),
			RightTypes:         convertToTypes(t.RightTypes),
			Cond:               t.Expr,
			Conditions:         [][]*plan.Expr{t.LeftCond, t.RightCond},
			RuntimeFilterSpecs: t.RuntimeFilterBuildList,
			HashOnPK:           t.HashOnPk,
			IsShuffle:          t.IsShuffle,
		}
	case vm.RightSemi:
		t := opr.GetRightSemiJoin()
		v.Arg = &rightsemi.Argument{
			Ibucket:            t.Ibucket,
			Nbucket:            t.Nbucket,
			Result:             t.Result,
			RightTypes:         convertToTypes(t.RightTypes),
			Cond:               t.Expr,
			Conditions:         [][]*plan.Expr{t.LeftCond, t.RightCond},
			RuntimeFilterSpecs: t.RuntimeFilterBuildList,
			HashOnPK:           t.HashOnPk,
			IsShuffle:          t.IsShuffle,
		}
	case vm.RightAnti:
		t := opr.GetRightAntiJoin()
		v.Arg = &rightanti.Argument{
			Ibucket:            t.Ibucket,
			Nbucket:            t.Nbucket,
			Result:             t.Result,
			RightTypes:         convertToTypes(t.RightTypes),
			Cond:               t.Expr,
			Conditions:         [][]*plan.Expr{t.LeftCond, t.RightCond},
			RuntimeFilterSpecs: t.RuntimeFilterBuildList,
			HashOnPK:           t.HashOnPk,
			IsShuffle:          t.IsShuffle,
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
		v.Arg = &order.Argument{OrderBySpec: opr.OrderBy}
	case vm.Product:
		t := opr.GetProduct()
		v.Arg = &product.Argument{
			Result:    convertToResultPos(t.RelList, t.ColList),
			Typs:      convertToTypes(t.Types),
			IsShuffle: t.IsShuffle,
		}
	case vm.Projection:
		v.Arg = &projection.Argument{Es: opr.ProjectList}
	case vm.Restrict:
		v.Arg = &restrict.Argument{E: opr.Filter}
	case vm.Semi:
		t := opr.GetSemiJoin()
		v.Arg = &semi.Argument{
			Ibucket:            t.Ibucket,
			Nbucket:            t.Nbucket,
			Result:             t.Result,
			Cond:               t.Expr,
			Typs:               convertToTypes(t.Types),
			Conditions:         [][]*plan.Expr{t.LeftCond, t.RightCond},
			RuntimeFilterSpecs: t.RuntimeFilterBuildList,
			HashOnPK:           t.HashOnPk,
			IsShuffle:          t.IsShuffle,
		}
	case vm.Single:
		t := opr.GetSingleJoin()
		v.Arg = &single.Argument{
			Ibucket:            t.Ibucket,
			Nbucket:            t.Nbucket,
			Result:             convertToResultPos(t.RelList, t.ColList),
			Cond:               t.Expr,
			Typs:               convertToTypes(t.Types),
			Conditions:         [][]*plan.Expr{t.LeftCond, t.RightCond},
			RuntimeFilterSpecs: t.RuntimeFilterBuildList,
			HashOnPK:           t.HashOnPk,
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
			HashOnPK:   t.HashOnPk,
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
	case vm.MergeRecursive:
		v.Arg = &mergerecursive.Argument{}
	case vm.MergeGroup:
		v.Arg = &mergegroup.Argument{
			NeedEval: opr.Agg.NeedEval,
		}
		DecodeMergeGroup(v.Arg.(*mergegroup.Argument), opr.Agg)
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
			OrderBySpecs: opr.OrderBy,
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
			Ibucket:         t.Ibucket,
			Nbucket:         t.Nbucket,
			NeedHashMap:     t.NeedHash,
			NeedExpr:        t.NeedExpr,
			Typs:            convertToTypes(t.Types),
			Conditions:      t.Conds,
			HashOnPK:        t.HashOnPk,
			NeedMergedBatch: t.NeedMergedBatch,
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
					Attrs:           t.Attrs,
					FileSize:        t.FileSize,
					FileOffsetTotal: t.FileOffsetTotal,
					Cols:            t.Cols,
					CreateSql:       t.CreateSql,
					Name2ColIndex:   name2ColIndex,
					FileList:        t.FileList,
				},
				ExParam: external.ExParam{
					Fileparam: new(external.ExFileparam),
					Filter: &external.FilterParam{
						FilterExpr: t.Filter,
					},
				},
			},
		}
	case vm.Stream:
		t := opr.GetStreamScan()
		v.Arg = &stream.Argument{
			TblDef: t.TblDef,
			Limit:  t.Limit,
			Offset: t.Offset,
		}
	default:
		return v, moerr.NewInternalErrorNoCtx(fmt.Sprintf("unexpected operator: %v", opr.Op))
	}
	return v, nil
}

// convert []types.Type to []*plan.Type
func convertToPlanTypes(ts []types.Type) []*plan.Type {
	result := make([]*plan.Type, len(ts))
	for i, t := range ts {
		result[i] = &plan.Type{
			Id:    int32(t.Oid),
			Width: t.Width,
			Scale: t.Scale,
		}
	}
	return result
}

// convert []*plan.Type to []types.Type
func convertToTypes(ts []*plan.Type) []types.Type {
	result := make([]types.Type, len(ts))
	for i, t := range ts {
		result[i] = types.New(types.T(t.Id), t.Width, t.Scale)
	}
	return result
}

// convert []agg.Aggregate to []*pipeline.Aggregate
func convertToPipelineAggregates(ags []agg.Aggregate) []*pipeline.Aggregate {
	result := make([]*pipeline.Aggregate, len(ags))
	for i, a := range ags {
		result[i] = &pipeline.Aggregate{
			Op:     a.Op,
			Dist:   a.Dist,
			Expr:   a.E,
			Config: a.Config,
		}
	}
	return result
}

// convert []*pipeline.Aggregate to []agg.Aggregate
func convertToAggregates(ags []*pipeline.Aggregate) []agg.Aggregate {
	result := make([]agg.Aggregate, len(ags))
	for i, a := range ags {
		result[i] = agg.Aggregate{
			Op:     a.Op,
			Dist:   a.Dist,
			E:      a.Expr,
			Config: a.Config,
		}
	}
	return result
}

// for now, it's group_concat
func convertPipelineMultiAggs(multiAggs []group_concat.Argument) []*pipeline.MultiArguemnt {
	result := make([]*pipeline.MultiArguemnt, len(multiAggs))
	for i, a := range multiAggs {
		result[i] = &pipeline.MultiArguemnt{
			Dist:        a.Dist,
			GroupExpr:   a.GroupExpr,
			OrderByExpr: a.OrderByExpr,
			Separator:   a.Separator,
			OrderId:     a.OrderId,
		}
	}
	return result
}

func convertToMultiAggs(multiAggs []*pipeline.MultiArguemnt) []group_concat.Argument {
	result := make([]group_concat.Argument, len(multiAggs))
	for i, a := range multiAggs {
		result[i] = group_concat.Argument{
			Dist:        a.Dist,
			GroupExpr:   a.GroupExpr,
			OrderByExpr: a.OrderByExpr,
			Separator:   a.Separator,
			OrderId:     a.OrderId,
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
		QueryId:      sei.QueryId,
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
	a := &plan.AnalyzeInfo{
		InputRows:        info.InputRows,
		OutputRows:       info.OutputRows,
		InputSize:        info.InputSize,
		OutputSize:       info.OutputSize,
		TimeConsumed:     info.TimeConsumed,
		MemorySize:       info.MemorySize,
		WaitTimeConsumed: info.WaitTimeConsumed,
		DiskIO:           info.DiskIO,
		S3IOByte:         info.S3IOByte,
		S3IOInputCount:   info.S3IOInputCount,
		S3IOOutputCount:  info.S3IOOutputCount,
		NetworkIO:        info.NetworkIO,
		ScanTime:         info.ScanTime,
		InsertTime:       info.InsertTime,
	}
	info.DeepCopyArray(a)
	return a
}

// func decodeBatch(proc *process.Process, data []byte) (*batch.Batch, error) {
func decodeBatch(mp *mpool.MPool, vp engine.VectorPool, data []byte) (*batch.Batch, error) {
	bat := new(batch.Batch)

	err := types.Decode(data, bat)
	if err != nil {
		return nil, err
	}
	if bat.IsEmpty() {
		return batch.EmptyBatch, nil
	}

	// allocated memory of vec from mPool.
	for i, vec := range bat.Vecs {
		typ := *vec.GetType()
		rvec := vector.NewVec(typ)
		if vp != nil {
			rvec = vp.GetVector(typ)
		}
		if err = vector.GetUnionAllFunction(typ, mp)(rvec, vec); err != nil {
			rvec.Free(mp)
			bat.Clean(mp)
			return nil, err
		}
		bat.Vecs[i] = rvec
	}
	bat.SetCnt(1)
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

func EncodeMergeGroup(merge *mergegroup.Argument, pipe *pipeline.Group) {
	if !merge.NeedEval || merge.PartialResults == nil {
		return
	}
	pipe.PartialResultTypes = make([]uint32, len(merge.PartialResultTypes))
	pipe.PartialResults = make([]byte, 0)
	for i := range pipe.PartialResultTypes {
		pipe.PartialResultTypes[i] = uint32(merge.PartialResultTypes[i])
		switch merge.PartialResultTypes[i] {
		case types.T_bool:
			result := merge.PartialResults[i].(bool)
			bytes := unsafe.Slice((*byte)(unsafe.Pointer(&result)), merge.PartialResultTypes[i].FixedLength())
			pipe.PartialResults = append(pipe.PartialResults, bytes...)
		case types.T_int8:
			result := merge.PartialResults[i].(int8)
			bytes := unsafe.Slice((*byte)(unsafe.Pointer(&result)), merge.PartialResultTypes[i].FixedLength())
			pipe.PartialResults = append(pipe.PartialResults, bytes...)
		case types.T_int16:
			result := merge.PartialResults[i].(int16)
			bytes := unsafe.Slice((*byte)(unsafe.Pointer(&result)), merge.PartialResultTypes[i].FixedLength())
			pipe.PartialResults = append(pipe.PartialResults, bytes...)
		case types.T_int32:
			result := merge.PartialResults[i].(int32)
			bytes := unsafe.Slice((*byte)(unsafe.Pointer(&result)), merge.PartialResultTypes[i].FixedLength())
			pipe.PartialResults = append(pipe.PartialResults, bytes...)
		case types.T_int64:
			result := merge.PartialResults[i].(int64)
			bytes := unsafe.Slice((*byte)(unsafe.Pointer(&result)), merge.PartialResultTypes[i].FixedLength())
			pipe.PartialResults = append(pipe.PartialResults, bytes...)
		case types.T_uint8:
			result := merge.PartialResults[i].(uint8)
			bytes := unsafe.Slice((*byte)(unsafe.Pointer(&result)), merge.PartialResultTypes[i].FixedLength())
			pipe.PartialResults = append(pipe.PartialResults, bytes...)
		case types.T_uint16:
			result := merge.PartialResults[i].(uint16)
			bytes := unsafe.Slice((*byte)(unsafe.Pointer(&result)), merge.PartialResultTypes[i].FixedLength())
			pipe.PartialResults = append(pipe.PartialResults, bytes...)
		case types.T_uint32:
			result := merge.PartialResults[i].(uint32)
			bytes := unsafe.Slice((*byte)(unsafe.Pointer(&result)), merge.PartialResultTypes[i].FixedLength())
			pipe.PartialResults = append(pipe.PartialResults, bytes...)
		case types.T_uint64:
			result := merge.PartialResults[i].(uint64)
			bytes := unsafe.Slice((*byte)(unsafe.Pointer(&result)), merge.PartialResultTypes[i].FixedLength())
			pipe.PartialResults = append(pipe.PartialResults, bytes...)
		case types.T_float32:
			result := merge.PartialResults[i].(float32)
			bytes := unsafe.Slice((*byte)(unsafe.Pointer(&result)), merge.PartialResultTypes[i].FixedLength())
			pipe.PartialResults = append(pipe.PartialResults, bytes...)
		case types.T_float64:
			result := merge.PartialResults[i].(float64)
			bytes := unsafe.Slice((*byte)(unsafe.Pointer(&result)), merge.PartialResultTypes[i].FixedLength())
			pipe.PartialResults = append(pipe.PartialResults, bytes...)
		case types.T_date:
			result := merge.PartialResults[i].(types.Date)
			bytes := unsafe.Slice((*byte)(unsafe.Pointer(&result)), merge.PartialResultTypes[i].FixedLength())
			pipe.PartialResults = append(pipe.PartialResults, bytes...)
		case types.T_time:
			result := merge.PartialResults[i].(types.Time)
			bytes := unsafe.Slice((*byte)(unsafe.Pointer(&result)), merge.PartialResultTypes[i].FixedLength())
			pipe.PartialResults = append(pipe.PartialResults, bytes...)
		case types.T_datetime:
			result := merge.PartialResults[i].(types.Datetime)
			bytes := unsafe.Slice((*byte)(unsafe.Pointer(&result)), merge.PartialResultTypes[i].FixedLength())
			pipe.PartialResults = append(pipe.PartialResults, bytes...)
		case types.T_timestamp:
			result := merge.PartialResults[i].(types.Timestamp)
			bytes := unsafe.Slice((*byte)(unsafe.Pointer(&result)), merge.PartialResultTypes[i].FixedLength())
			pipe.PartialResults = append(pipe.PartialResults, bytes...)
		case types.T_enum:
			result := merge.PartialResults[i].(types.Enum)
			bytes := unsafe.Slice((*byte)(unsafe.Pointer(&result)), merge.PartialResultTypes[i].FixedLength())
			pipe.PartialResults = append(pipe.PartialResults, bytes...)
		case types.T_decimal64:
			result := merge.PartialResults[i].(types.Decimal64)
			bytes := unsafe.Slice((*byte)(unsafe.Pointer(&result)), merge.PartialResultTypes[i].FixedLength())
			pipe.PartialResults = append(pipe.PartialResults, bytes...)
		case types.T_decimal128:
			result := merge.PartialResults[i].(types.Decimal128)
			bytes := unsafe.Slice((*byte)(unsafe.Pointer(&result)), merge.PartialResultTypes[i].FixedLength())
			pipe.PartialResults = append(pipe.PartialResults, bytes...)
		case types.T_uuid:
			result := merge.PartialResults[i].(types.Uuid)
			bytes := unsafe.Slice((*byte)(unsafe.Pointer(&result)), merge.PartialResultTypes[i].FixedLength())
			pipe.PartialResults = append(pipe.PartialResults, bytes...)
		case types.T_TS:
			result := merge.PartialResults[i].(types.TS)
			bytes := unsafe.Slice((*byte)(unsafe.Pointer(&result)), merge.PartialResultTypes[i].FixedLength())
			pipe.PartialResults = append(pipe.PartialResults, bytes...)
		case types.T_Rowid:
			result := merge.PartialResults[i].(types.Rowid)
			bytes := unsafe.Slice((*byte)(unsafe.Pointer(&result)), merge.PartialResultTypes[i].FixedLength())
			pipe.PartialResults = append(pipe.PartialResults, bytes...)
		case types.T_Blockid:
			result := merge.PartialResults[i].(types.Blockid)
			bytes := unsafe.Slice((*byte)(unsafe.Pointer(&result)), merge.PartialResultTypes[i].FixedLength())
			pipe.PartialResults = append(pipe.PartialResults, bytes...)
		}
	}
}

func DecodeMergeGroup(merge *mergegroup.Argument, pipe *pipeline.Group) {
	if !pipe.NeedEval || pipe.PartialResults == nil {
		return
	}
	merge.PartialResultTypes = make([]types.T, len(pipe.PartialResultTypes))
	merge.PartialResults = make([]any, 0)
	for i := range merge.PartialResultTypes {
		merge.PartialResultTypes[i] = types.T(pipe.PartialResultTypes[i])
		switch merge.PartialResultTypes[i] {
		case types.T_bool:
			result := *(*bool)(unsafe.Pointer(&pipe.PartialResults[0]))
			merge.PartialResults = append(merge.PartialResults, result)
			pipe.PartialResults = pipe.PartialResults[merge.PartialResultTypes[i].FixedLength():]
		case types.T_int8:
			result := *(*int8)(unsafe.Pointer(&pipe.PartialResults[0]))
			merge.PartialResults = append(merge.PartialResults, result)
			pipe.PartialResults = pipe.PartialResults[merge.PartialResultTypes[i].FixedLength():]
		case types.T_int16:
			result := *(*int16)(unsafe.Pointer(&pipe.PartialResults[0]))
			merge.PartialResults = append(merge.PartialResults, result)
			pipe.PartialResults = pipe.PartialResults[merge.PartialResultTypes[i].FixedLength():]
		case types.T_int32:
			result := *(*int32)(unsafe.Pointer(&pipe.PartialResults[0]))
			merge.PartialResults = append(merge.PartialResults, result)
			pipe.PartialResults = pipe.PartialResults[merge.PartialResultTypes[i].FixedLength():]
		case types.T_int64:
			result := *(*int64)(unsafe.Pointer(&pipe.PartialResults[0]))
			merge.PartialResults = append(merge.PartialResults, result)
			pipe.PartialResults = pipe.PartialResults[merge.PartialResultTypes[i].FixedLength():]
		case types.T_uint8:
			result := *(*uint8)(unsafe.Pointer(&pipe.PartialResults[0]))
			merge.PartialResults = append(merge.PartialResults, result)
			pipe.PartialResults = pipe.PartialResults[merge.PartialResultTypes[i].FixedLength():]
		case types.T_uint16:
			result := *(*uint16)(unsafe.Pointer(&pipe.PartialResults[0]))
			merge.PartialResults = append(merge.PartialResults, result)
			pipe.PartialResults = pipe.PartialResults[merge.PartialResultTypes[i].FixedLength():]
		case types.T_uint32:
			result := *(*uint32)(unsafe.Pointer(&pipe.PartialResults[0]))
			merge.PartialResults = append(merge.PartialResults, result)
			pipe.PartialResults = pipe.PartialResults[merge.PartialResultTypes[i].FixedLength():]
		case types.T_uint64:
			result := *(*uint64)(unsafe.Pointer(&pipe.PartialResults[0]))
			merge.PartialResults = append(merge.PartialResults, result)
			pipe.PartialResults = pipe.PartialResults[merge.PartialResultTypes[i].FixedLength():]
		case types.T_float32:
			result := *(*float32)(unsafe.Pointer(&pipe.PartialResults[0]))
			merge.PartialResults = append(merge.PartialResults, result)
			pipe.PartialResults = pipe.PartialResults[merge.PartialResultTypes[i].FixedLength():]
		case types.T_float64:
			result := *(*float64)(unsafe.Pointer(&pipe.PartialResults[0]))
			merge.PartialResults = append(merge.PartialResults, result)
			pipe.PartialResults = pipe.PartialResults[merge.PartialResultTypes[i].FixedLength():]
		case types.T_date:
			result := *(*types.Date)(unsafe.Pointer(&pipe.PartialResults[0]))
			merge.PartialResults = append(merge.PartialResults, result)
			pipe.PartialResults = pipe.PartialResults[merge.PartialResultTypes[i].FixedLength():]
		case types.T_time:
			result := *(*types.Time)(unsafe.Pointer(&pipe.PartialResults[0]))
			merge.PartialResults = append(merge.PartialResults, result)
			pipe.PartialResults = pipe.PartialResults[merge.PartialResultTypes[i].FixedLength():]
		case types.T_datetime:
			result := *(*types.Datetime)(unsafe.Pointer(&pipe.PartialResults[0]))
			merge.PartialResults = append(merge.PartialResults, result)
			pipe.PartialResults = pipe.PartialResults[merge.PartialResultTypes[i].FixedLength():]
		case types.T_timestamp:
			result := *(*types.Timestamp)(unsafe.Pointer(&pipe.PartialResults[0]))
			merge.PartialResults = append(merge.PartialResults, result)
			pipe.PartialResults = pipe.PartialResults[merge.PartialResultTypes[i].FixedLength():]
		case types.T_enum:
			result := *(*types.Enum)(unsafe.Pointer(&pipe.PartialResults[0]))
			merge.PartialResults = append(merge.PartialResults, result)
			pipe.PartialResults = pipe.PartialResults[merge.PartialResultTypes[i].FixedLength():]
		case types.T_decimal64:
			result := *(*types.Decimal64)(unsafe.Pointer(&pipe.PartialResults[0]))
			merge.PartialResults = append(merge.PartialResults, result)
			pipe.PartialResults = pipe.PartialResults[merge.PartialResultTypes[i].FixedLength():]
		case types.T_decimal128:
			result := *(*types.Decimal128)(unsafe.Pointer(&pipe.PartialResults[0]))
			merge.PartialResults = append(merge.PartialResults, result)
			pipe.PartialResults = pipe.PartialResults[merge.PartialResultTypes[i].FixedLength():]
		case types.T_uuid:
			result := *(*types.Uuid)(unsafe.Pointer(&pipe.PartialResults[0]))
			merge.PartialResults = append(merge.PartialResults, result)
			pipe.PartialResults = pipe.PartialResults[merge.PartialResultTypes[i].FixedLength():]
		case types.T_TS:
			result := *(*types.TS)(unsafe.Pointer(&pipe.PartialResults[0]))
			merge.PartialResults = append(merge.PartialResults, result)
			pipe.PartialResults = pipe.PartialResults[merge.PartialResultTypes[i].FixedLength():]
		case types.T_Rowid:
			result := *(*types.Rowid)(unsafe.Pointer(&pipe.PartialResults[0]))
			merge.PartialResults = append(merge.PartialResults, result)
			pipe.PartialResults = pipe.PartialResults[merge.PartialResultTypes[i].FixedLength():]
		case types.T_Blockid:
			result := *(*types.Blockid)(unsafe.Pointer(&pipe.PartialResults[0]))
			merge.PartialResults = append(merge.PartialResults, result)
			pipe.PartialResults = pipe.PartialResults[merge.PartialResultTypes[i].FixedLength():]
		}
	}
}

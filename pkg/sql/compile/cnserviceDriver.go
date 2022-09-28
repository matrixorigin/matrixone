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

	"github.com/matrixorigin/matrixone/pkg/cnservice/cnclient"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/morpc"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/pipeline"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/agg"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/anti"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/connector"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/dispatch"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/group"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/intersect"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/intersectall"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/join"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/left"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/limit"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/loopanti"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/loopjoin"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/loopleft"
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
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/product"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/projection"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/restrict"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/semi"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/single"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/top"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/mheap"
	"github.com/matrixorigin/matrixone/pkg/vm/mmu/guest"
	"github.com/matrixorigin/matrixone/pkg/vm/mmu/host"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// CnServerMessageHandler deal the client message that received at cn-server.
// the message is always *pipeline.Message here. It's a byte array which encoded by method encodeScope.
// write back Analysis Information and error info if error occurs to client.
func CnServerMessageHandler(ctx context.Context, message morpc.Message, cs morpc.ClientSession) error {
	var errCode []byte = nil
	// decode message and run it, get final analysis information and err info.
	analysis, err := pipelineMessageHandle(ctx, message, cs)
	if err != nil {
		errCode = []byte(err.Error())
	}
	return cs.Write(ctx, &pipeline.Message{Sid: pipeline.MessageEnd, Code: errCode, Analyse: analysis})
}

func pipelineMessageHandle(ctx context.Context, message morpc.Message, cs morpc.ClientSession) (anaData []byte, err error) {
	m, ok := message.(*pipeline.Message)
	if !ok {
		panic("unexpected message type for cn-server")
	}
	c := newCompile(ctx)
	var s *Scope
	s, err = decodeScope(m.GetData(), c.proc)
	if err != nil {
		return nil, err
	}
	// refactor the last operator connect to output
	refactorScope(ctx, s, cs)

	err = s.Run(c)
	if err != nil {
		return nil, err
	}
	// get analyse related information
	if query, ok := s.Plan.Plan.(*plan.Plan_Query); ok {
		nodes := query.Query.GetNodes()
		anas := &pipeline.AnalysisList{}
		anas.List = make([]*plan.AnalyzeInfo, len(nodes))
		for i := range anas.List {
			anas.List[i] = nodes[i].AnalyzeInfo
		}
		anaData, err = anas.Marshal()
		if err != nil {
			return nil, err
		}
		return anaData, nil
	}
	return nil, nil
}

// remoteRun sends a scope to a remote node for execution, and wait to receive the back message.
// the back message is always *pipeline.Message but has three cases.
// 1. ErrMessage
// 2. End Message with the result of analysis
// 3. Batch Message
func (s *Scope) remoteRun(c *Compile) error {
	// encode the scope
	sData, errEncode := encodeScope(s)
	if errEncode != nil {
		return errEncode
	}

	// send encoded message
	message := &pipeline.Message{Data: sData}
	r, errSend := cnclient.Client.Send(c.ctx, s.NodeInfo.Addr, message)
	if errSend != nil {
		return errSend
	}
	defer r.Close()

	// range to receive.
	arg := s.Instructions[len(s.Instructions)-1].Arg.(*connector.Argument)
	for {
		val, errReceive := r.Get()
		if errReceive != nil {
			return errReceive
		}
		m := val.(*pipeline.Message)

		errMessage := m.GetCode()
		if len(errMessage) > 0 {
			return moerr.NewInternalError(string(errMessage))
		}

		sid := m.GetID()
		if sid == pipeline.MessageEnd {
			// get analyse information
			anaData := m.GetAnalyse()
			if len(anaData) > 0 {
				// decode analyse
				ana := new(pipeline.AnalysisList)
				err := ana.Unmarshal(anaData)
				if err != nil {
					return err
				}
				mergeAnalyseInfo(c.anal, ana)
			}
			break
		}
		// decoded message
		bat, errBatch := decodeBatch(c.proc, m)
		if errBatch != nil {
			return errBatch
		}
		sendToConnectOperator(arg, bat)
	}

	return nil
}

var _ = new(Scope).remoteRun

// encodeScope generate a pipeline.Pipeline from Scope, encode pipeline, and returns.
func encodeScope(s *Scope) ([]byte, error) {
	p, err := fillPipeline(s)
	if err != nil {
		return nil, err
	}
	return p.Marshal()
}

// decodeScope decode a pipeline.Pipeline from bytes, and generate a Scope from it.
func decodeScope(data []byte, proc *process.Process) (*Scope, error) {
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
	s, err := generateScope(proc, p, ctx, nil)
	if err != nil {
		return nil, err
	}
	return s, fillInstructionsForScope(s, ctx, p)
}

func refactorScope(_ context.Context, _ *Scope, _ morpc.ClientSession) {
	// refactor the scope
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
	if _, err := fillInstructionsForPipeline(s, ctx, p, ctxId); err != nil {
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
	// Plan
	p.Qry = s.Plan
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

// generateScope generate a scope from scope context and pipeline.
func generateScope(proc *process.Process, p *pipeline.Pipeline, ctx *scopeContext, analNodes []*process.AnalyzeInfo) (*Scope, error) {
	var err error

	s := &Scope{
		Magic:  int(p.GetPipelineType()),
		IsEnd:  p.IsEnd,
		IsJoin: p.IsJoin,
		Plan:   p.Qry,
	}
	dsc := p.GetDataSource()
	if dsc != nil {
		s.DataSource = &Source{
			SchemaName:   dsc.SchemaName,
			RelationName: dsc.TableName,
			Attributes:   dsc.ColList,
			PushdownId:   dsc.PushdownId,
			PushdownAddr: dsc.PushdownAddr,
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
		if s.PreScopes[i], err = generateScope(s.Proc, p.Children[i], ctx.children[i], analNodes); err != nil {
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

	in := &pipeline.Instruction{Op: int32(opr.Op), Idx: int32(opr.Idx)}
	switch t := opr.Arg.(type) {
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
		in.Dispatch = &pipeline.Dispatch{
			All: t.All,
		}
		in.Dispatch.Connector = make([]*pipeline.Connector, len(t.Regs))
		for i := range t.Regs {
			idx, ctx0 := ctx.root.findRegister(t.Regs[i])
			if ctx0.root.isRemote(ctx0, 0) && !ctx0.isDescendant(ctx) {
				id := srv.RegistConnector(t.Regs[i])
				if ctxId, err = ctx0.addSubPipeline(id, idx, ctxId); err != nil {
					return ctxId, nil, err
				}
			}
			in.Dispatch.Connector[i] = &pipeline.Connector{
				ConnectorIndex: idx,
				PipelineId:     ctx0.id,
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
		in.OrderBy = convertToPlanOrderByList(t.Fs)
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
		in.OrderBy = convertToPlanOrderByList(t.Fs)
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
	// may useless.
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
		in.OrderBy = convertToPlanOrderByList(t.Fs)
	case *mergeorder.Argument:
		in.OrderBy = convertToPlanOrderByList(t.Fs)
	case *connector.Argument:
		idx, ctx0 := ctx.root.findRegister(t.Reg)
		if ctx0.root.isRemote(ctx0, 0) && !ctx0.isDescendant(ctx) {
			id := srv.RegistConnector(t.Reg)
			if ctxId, err = ctx0.addSubPipeline(id, idx, ctxId); err != nil {
				return ctxId, nil, err
			}
		}
		in.Connect = &pipeline.Connector{
			PipelineId:     ctx0.id,
			ConnectorIndex: idx, // receiver
		}
	case *mark.Argument:
		in.MarkJoin = &pipeline.MarkJoin{
			Ibucket:      t.Ibucket,
			Nbucket:      t.Nbucket,
			Result:       t.Result,
			LeftCond:     t.Conditions[0],
			RightCond:    t.Conditions[1],
			Types:        convertToPlanTypes(t.Typs),
			Cond:         t.Cond,
			OnList:       t.OnList,
			OutputNull:   t.OutputNull,
			OutputMark:   t.OutputMark,
			OutputAnyway: t.OutputAnyway,
			MarkMeaning:  t.MarkMeaning,
		}
	default:
		return -1, nil, moerr.NewInternalError(fmt.Sprintf("unexpected operator: %v", opr.Op))
	}
	return ctxId, in, nil
}

// convert pipeline.Instruction to vm.Instruction
func convertToVmInstruction(opr *pipeline.Instruction, ctx *scopeContext) (vm.Instruction, error) {
	v := vm.Instruction{Op: int(opr.Op), Idx: int(opr.Idx)}
	switch opr.Op {
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
		regs := make([]*process.WaitRegister, len(t.Connector))
		for i, cp := range t.Connector {
			regs[i] = ctx.root.getRegister(cp.PipelineId, cp.ConnectorIndex)
		}
		v.Arg = &dispatch.Argument{
			Regs: regs,
			All:  t.All,
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
	case vm.Offset:
		v.Arg = &offset.Argument{Offset: opr.Offset}
	case vm.Order:
		v.Arg = &order.Argument{Fs: convertToColExecField(opr.OrderBy)}
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
			Ibucket:      t.Ibucket,
			Nbucket:      t.Nbucket,
			Result:       t.Result,
			Conditions:   [][]*plan.Expr{t.LeftCond, t.RightCond},
			Typs:         convertToTypes(t.Types),
			Cond:         t.Cond,
			OnList:       t.OnList,
			OutputNull:   t.OutputNull,
			OutputMark:   t.OutputMark,
			OutputAnyway: t.OutputAnyway,
			MarkMeaning:  t.MarkMeaning,
		}
	case vm.Top:
		v.Arg = &top.Argument{
			Limit: int64(opr.Limit),
			Fs:    convertToColExecField(opr.OrderBy),
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
	// may useless
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
			Fs:    convertToColExecField(opr.OrderBy),
		}
	case vm.MergeOrder:
		v.Arg = &mergeorder.Argument{
			Fs: convertToColExecField(opr.OrderBy),
		}
	default:
		return v, moerr.NewInternalError(fmt.Sprintf("unexpected operator: %v", opr.Op))
	}
	return v, nil
}

// newCompile init a new compile for remote run.
func newCompile(
	ctx context.Context,
) *Compile {
	// not implement now, fill method should send by stream
	c := &Compile{
		ctx: ctx,
		proc: process.New(
			ctx,
			mheap.New(guest.New(1<<30, host.New(1<<20))),
			nil, //TODO must set txn client
			nil, //TODO must set txn operator
			nil, //TODO must set file service
		),
	}
	//c.fill = func(a any, b *batch.Batch) error {
	//	stream, err := CNClient.NewStream("target")
	//	if err != nil {
	//		return err
	//	}
	//	stream.Send()
	//}
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

// convert []colexec.Field to []*plan.OrderBySpec
func convertToPlanOrderByList(field []colexec.Field) []*plan.OrderBySpec {
	// default order direction is ASC.
	convToPlanOrderFlag := func(source colexec.Direction) plan.OrderBySpec_OrderByFlag {
		if source == colexec.Descending {
			return plan.OrderBySpec_DESC
		}
		return plan.OrderBySpec_ASC
	}

	res := make([]*plan.OrderBySpec, len(field))
	for i, f := range field {
		res[i] = &plan.OrderBySpec{
			Expr: f.E,
			Flag: convToPlanOrderFlag(f.Type),
		}
	}
	return res
}

// convert []*plan.OrderBySpec to []colexec.Field
func convertToColExecField(list []*plan.OrderBySpec) []colexec.Field {
	convToColExecDirection := func(source plan.OrderBySpec_OrderByFlag) colexec.Direction {
		if source == plan.OrderBySpec_ASC {
			return colexec.Ascending
		}
		return colexec.Descending
	}

	res := make([]colexec.Field, len(list))
	for i, l := range list {
		res[i].E, res[i].Type = l.Expr, convToColExecDirection(l.Flag)
	}
	return res
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

func decodeBatch(_ *process.Process, msg *pipeline.Message) (*batch.Batch, error) {
	bat := new(batch.Batch) // TODO: allocate the memory from process may suitable.
	err := types.Decode(msg.GetData(), bat)
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
		PushdownAddr: cnAddr,
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
		PushdownAddr: cnAddr,
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

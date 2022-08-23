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
	"github.com/matrixorigin/matrixone/pkg/cnservice/cnclient"
	"github.com/matrixorigin/matrixone/pkg/common/morpc"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/output"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/pipeline"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/aggregate"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/anti"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/connector"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/dispatch"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/group"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/intersect"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/join"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/left"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/limit"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/loopanti"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/loopjoin"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/loopleft"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/loopsemi"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/loopsingle"
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
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// PipelineMessageHandle deal the message that received at cn-server.
// return an extra data and error info if error occurs.
func PipelineMessageHandle(ctx context.Context, message morpc.Message, cs morpc.ClientSession) ([]byte, error) {
	m := message.(*pipeline.Message)
	s, err := decodeScope(m.GetData())
	if err != nil {
		return nil, err
	}
	// refactor the last operator connect to output
	refactorScope(ctx, s, cs)

	c := newCompile(ctx)
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
		anaData, errAna := anas.Marshal()
		if errAna != nil {
			return nil, errAna
		}
		return anaData, nil
	}
	return nil, nil
}

func refactorScope(ctx context.Context, s *Scope, cs morpc.ClientSession) {
	// refactor the scope
	s.Instructions[len(s.Instructions)-1] = vm.Instruction{
		Op: vm.Output,
		Arg: &output.Argument{
			// TODO: need fill.
			Data: nil,
			Func: func(i interface{}, b *batch.Batch) error {
				msg := &pipeline.Message{}
				return cs.Write(ctx, msg)
			},
		},
	}
}

func convertScopeToPipeline(s *Scope, ctx *scopeContext, ctxId int32) (*pipeline.Pipeline, int32, error) {
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
			SchemaName: s.DataSource.SchemaName,
			TableName:  s.DataSource.RelationName,
			ColList:    s.DataSource.Attributes,
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
			root:   ctx.root,
			id:     ctxId,
			regs:   make(map[*process.WaitRegister]int32),
		}
		ctxId++
		if p.Children[i], ctxId, err = convertScopeToPipeline(s.PreScopes[i], ctx.children[i], ctxId); err != nil {
			return nil, -1, err
		}
	}
	// Instructions
	p.InstructionList = make([]*pipeline.Instruction, len(s.Instructions))
	for i := range p.InstructionList {
		if p.InstructionList[i], err = convertToPipelineInstruction(&s.Instructions[i], ctx); err != nil {
			return nil, -1, err
		}
	}
	return p, ctxId, nil
}

func convertPipelineToScope(proc *process.Process, p *pipeline.Pipeline, par *scopeContext, analNodes []*process.AnalyzeInfo) (*Scope, error) {
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
		}
		if len(dsc.Block) > 0 {
			bat := new(batch.Batch)
			if err := types.Decode([]byte(dsc.Block), bat); err != nil {
				return nil, err
			}
			s.DataSource.Bat = bat
		}
	}
	{
		s.NodeInfo.Id = p.Node.Id
		s.NodeInfo.Addr = p.Node.Addr
		s.NodeInfo.Mcpu = int(p.Node.Mcpu)
		s.NodeInfo.Data = make([][]byte, len(p.Node.Payload))
		for i := range p.Node.Payload {
			s.NodeInfo.Data[i] = []byte(p.Node.Payload[i])
		}
	}
	ctx := &scopeContext{
		parent: par,
		id:     p.PipelineId,
		regs:   make(map[*process.WaitRegister]int32),
	}
	if par == nil {
		ctx.root = ctx
	} else {
		ctx.root = par.root
	}
	s.Proc = process.NewWithAnalyze(proc, proc.Ctx, int(p.ChildrenCount), analNodes)
	{
		for i := range s.Proc.Reg.MergeReceivers {
			ctx.regs[s.Proc.Reg.MergeReceivers[i]] = int32(i)
		}

	}
	s.Instructions = make([]vm.Instruction, len(p.InstructionList))
	// Instructions
	for i := range s.Instructions {
		if s.Instructions[i], err = convertToVmInstruction(p.InstructionList[i], ctx); err != nil {
			return nil, err
		}
	}
	// PreScope
	s.PreScopes = make([]*Scope, len(s.PreScopes))
	for i := range s.PreScopes {
		if s.PreScopes[i], err = convertPipelineToScope(s.Proc, p.Children[i], ctx, analNodes); err != nil {
			return nil, err
		}
	}
	return s, nil
}

// convert vm.Instruction to pipeline.Instruction
func convertToPipelineInstruction(opr *vm.Instruction, ctx *scopeContext) (*pipeline.Instruction, error) {
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
			// Children: nil,
		}
		for i := range t.Regs {
			in.Dispatch.Connector[i] = ctx.root.findRegister(t.Regs[i])
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
	// operators that nothing need to do.
	case *connector.Argument:
		in.Connect = ctx.root.findRegister(t.Reg)
	default:
		return nil, moerr.New(moerr.INTERNAL_ERROR, "unexpected operator: %v", opr.Op)
	}
	return in, nil
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
		t := opr.GetJoin()
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
	default:
		return v, moerr.New(moerr.INTERNAL_ERROR, "unexpected operator: %v", opr.Op)
	}
	return v, nil
}

func encodeScope(s *Scope) ([]byte, error) {
	// convert scope to pipeline.Pipeline
	ctx := &scopeContext{
		id:     0,
		parent: nil,
		regs:   make(map[*process.WaitRegister]int32),
	}
	ctx.root = ctx
	p, _, err := convertScopeToPipeline(s, ctx, 0)
	if err != nil {
		return nil, err
	}
	// marshal to pipeline
	data, err := p.Marshal()
	if err != nil {
		return nil, err
	}
	return data, nil
}

func decodeScope(data []byte) (*Scope, error) {
	// unmarshal to pipeline
	p := &pipeline.Pipeline{}
	err := p.Unmarshal(data)
	if err != nil {
		return nil, err
	}
	// test
	proc := testutil.NewProcess()
	proc.Ctx = context.TODO()

	// convert pipeline.Pipeline to scope.
	return convertPipelineToScope(proc, p, nil, nil)
}

func newCompile(ctx context.Context) *Compile {
	// TODO: fill method should send by stream
	c := &Compile{
		ctx: ctx,
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

func (s *Scope) remoteRun(c *Compile) error {
	// encode the scope
	sData, errEncode := encodeScope(s)
	if errEncode != nil {
		return errEncode
	}

	// send encoded message
	message := &pipeline.Message{Data: sData}
	r, errSend := cnclient.Client.Send(c.ctx, s.NodeInfo.Addr, message)
	defer r.Close()
	if errSend != nil {
		return errSend
	}

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
			return errors.New(string(errMessage))
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

func convertToPipelineAggregates(ags []aggregate.Aggregate) []*pipeline.Aggregate {
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

func convertToAggregates(ags []*pipeline.Aggregate) []aggregate.Aggregate {
	result := make([]aggregate.Aggregate, len(ags))
	for i, a := range ags {
		result[i] = aggregate.Aggregate{
			Op:   int(a.Op),
			Dist: a.Dist,
			E:    a.Expr,
		}
	}
	return result
}

func convertToPlanOrderByList(field []colexec.Field) []*plan.OrderBySpec {
	// default order direction is DESC.
	convToPlanOrderFlag := func(source colexec.Direction) plan.OrderBySpec_OrderByFlag {
		if source == colexec.Ascending {
			return plan.OrderBySpec_ASC
		}
		return plan.OrderBySpec_DESC
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

func getRelColList(resultPos []colexec.ResultPos) (relList []int32, colList []int32) {
	relList = make([]int32, len(resultPos))
	colList = make([]int32, len(resultPos))
	for i := range resultPos {
		relList[i], colList[i] = resultPos[i].Rel, resultPos[i].Pos
	}
	return
}

func convertToResultPos(relList, colList []int32) []colexec.ResultPos {
	res := make([]colexec.ResultPos, len(relList))
	for i := range res {
		res[i].Rel, res[i].Pos = relList[i], colList[i]
	}
	return res
}

func decodeBatch(proc *process.Process, msg *pipeline.Message) (*batch.Batch, error) {
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
func (ctx *scopeContext) findRegister(reg *process.WaitRegister) *pipeline.Connector {
	if index, ok := ctx.regs[reg]; ok {
		return &pipeline.Connector{
			PipelineId:     ctx.id,
			ConnectorIndex: index,
		}
	}
	for i := range ctx.children {
		if cp := ctx.children[i].findRegister(reg); cp != nil {
			return cp
		}
	}
	return nil
}

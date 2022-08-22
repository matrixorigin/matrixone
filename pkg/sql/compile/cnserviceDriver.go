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

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/pipeline"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/aggregate"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/anti"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/connector"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/deletion"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/dispatch"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/group"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/insert"
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
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/output"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/product"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/projection"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/restrict"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/semi"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/single"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/top"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/update"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

/*
var CNClient *cnservice.CNClient

func StartCNClient(cfg *cnservice.ClientConfig) error {
	client, err := cnservice.NewCNClient(cfg)
	if err != nil {
		return err
	}
	CNClient = client.(*cnservice.CNClient)
	return nil
}

func StartCNService(cfg *cnservice.Config) error {
	server, err := cnservice.NewService(cfg,
		cnservice.WithMessageHandle(pipelineMessageHandle))
	if err != nil {
		return err
	}
	return server.Start()
}

func pipelineMessageHandle(message morpc.Message, cs morpc.ClientSession) error {
	m := message.(*pipeline.Message)
	s, err := decodeScope(m.GetData())
	if err != nil {
		return err
	}
	// refactor the last operator connect to output
	refactorScope(s, cs)

	c := newCompile()
	return s.Run(c)
}

func refactorScope(s *Scope, cs morpc.ClientSession) {
	// refactor the scope
	s.Instructions[len(s.Instructions)-1] = vm.Instruction{
		Op: vm.Output,
		Arg: &output.Argument{
			// TODO: need fill.
			Data: nil,
			Func: func(i interface{}, b *batch.Batch) error {
				msg := &pipeline.Message{}
				return cs.Write(msg, morpc.SendOptions{})
			},
		},
	}
}
*/

func convertScopeToPipeline(s *Scope) *pipeline.Pipeline {
	p := &pipeline.Pipeline{}
	// Magic and IsEnd
	p.PipelineType = pipeline.Pipeline_PipelineType(s.Magic)
	p.IsEnd = s.IsEnd
	p.IsJoin = s.IsJoin
	// Plan
	p.Qry = s.Plan
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
				return nil
			}
			p.DataSource.Block = string(data)
		}
	}
	// PreScope
	p.Children = make([]*pipeline.Pipeline, len(s.PreScopes))
	for i := range s.PreScopes {
		p.Children[i] = convertScopeToPipeline(s.PreScopes[i])
	}
	// Instructions
	p.InstructionList = make([]*pipeline.Instruction, len(s.Instructions))
	for i := range p.InstructionList {
		p.InstructionList[i] = convertToPipelineInstruction(&s.Instructions[i])
	}
	return p
}

func convertPipelineToScope(p *pipeline.Pipeline) *Scope {
	s := &Scope{
		Magic: int(p.GetPipelineType()),
		IsEnd: p.IsEnd,
		Plan:  p.Qry,
	}
	dsc := p.GetDataSource()
	if dsc != nil {
		s.DataSource = &Source{
			SchemaName:   dsc.SchemaName,
			RelationName: dsc.TableName,
			Attributes:   dsc.ColList,
		}
	}
	s.Instructions = make([]vm.Instruction, len(p.InstructionList))
	// Instructions
	for i := range s.Instructions {
		s.Instructions[i] = convertToVmInstruction(p.InstructionList[i])
	}
	// PreScope
	s.PreScopes = make([]*Scope, len(s.PreScopes))
	for i := range s.PreScopes {
		s.PreScopes[i] = convertPipelineToScope(p.Children[i])
	}
	return s
}

// convert vm.Instruction to pipeline.Instruction
func convertToPipelineInstruction(opr *vm.Instruction) *pipeline.Instruction {
	p := &pipeline.Instruction{Op: int32(opr.Op), Idx: int32(opr.Idx)}

	switch t := opr.Arg.(type) {
	case *anti.Argument:
		p.Anti = &pipeline.AntiJoin{
			Ibucket:   t.Ibucket,
			Nbucket:   t.Nbucket,
			Expr:      t.Cond,
			Types:     convertToPlanTypes(t.Typs),
			LeftCond:  t.Conditions[0],
			RightCond: t.Conditions[1],
			Result:    t.Result,
		}
	case *dispatch.Argument:
		p.Dispatch = &pipeline.Dispatch{
			All: t.All,
			// Children: nil,
		}
	case *group.Argument:
		p.Agg = &pipeline.Group{
			NeedEval: t.NeedEval,
			Ibucket:  t.Ibucket,
			Nbucket:  t.Nbucket,
			Exprs:    t.Exprs,
			Types:    convertToPlanTypes(t.Types),
			Aggs:     convertToPipelineAggregates(t.Aggs),
		}
	case *join.Argument:
		relList, colList := getRelColList(t.Result)
		p.Join = &pipeline.Join{
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
		p.LeftJoin = &pipeline.LeftJoin{
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
		p.Limit = t.Limit
	case *loopanti.Argument:
		p.Anti = &pipeline.AntiJoin{
			Result: t.Result,
			Expr:   t.Cond,
			Types:  convertToPlanTypes(t.Typs),
		}
	case *loopjoin.Argument:
		relList, colList := getRelColList(t.Result)
		p.Join = &pipeline.Join{
			RelList: relList,
			ColList: colList,
			Expr:    t.Cond,
			Types:   convertToPlanTypes(t.Typs),
		}
	case *loopleft.Argument:
		relList, colList := getRelColList(t.Result)
		p.LeftJoin = &pipeline.LeftJoin{
			RelList: relList,
			ColList: colList,
			Expr:    t.Cond,
			Types:   convertToPlanTypes(t.Typs),
		}
	case *loopsemi.Argument:
		p.SemiJoin = &pipeline.SemiJoin{
			Result: t.Result,
			Expr:   t.Cond,
			Types:  convertToPlanTypes(t.Typs),
		}
	case *loopsingle.Argument:
		relList, colList := getRelColList(t.Result)
		p.SingleJoin = &pipeline.SingleJoin{
			RelList: relList,
			ColList: colList,
			Expr:    t.Cond,
			Types:   convertToPlanTypes(t.Typs),
		}
	case *offset.Argument:
		p.Offset = t.Offset
	case *order.Argument:
		p.OrderBy = convertToPlanOrderByList(t.Fs)
	case *product.Argument:
		relList, colList := getRelColList(t.Result)
		p.Product = &pipeline.Product{
			RelList: relList,
			ColList: colList,
			Types:   convertToPlanTypes(t.Typs),
		}
	case *projection.Argument:
		p.ProjectList = t.Es
	case *restrict.Argument:
		p.Filter = t.E
	case *semi.Argument:
		p.SemiJoin = &pipeline.SemiJoin{
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
		p.SingleJoin = &pipeline.SingleJoin{
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
		p.Limit = uint64(t.Limit)
		p.OrderBy = convertToPlanOrderByList(t.Fs)
	// we reused ANTI to store the information here because of the lack of related structure.
	case *intersect.Argument: // 1
		p.Anti = &pipeline.AntiJoin{
			Ibucket: t.IBucket,
			Nbucket: t.NBucket,
		}
	case *minus.Argument: // 2
		p.Anti = &pipeline.AntiJoin{
			Ibucket: t.IBucket,
			Nbucket: t.NBucket,
		}
	// may useless.
	case *merge.Argument:
	case *mergegroup.Argument:
		p.Agg = &pipeline.Group{
			NeedEval: t.NeedEval,
		}
	case *mergelimit.Argument:
		p.Limit = t.Limit
	case *mergeoffset.Argument:
		p.Offset = t.Offset
	case *mergetop.Argument:
		p.Limit = uint64(t.Limit)
		p.OrderBy = convertToPlanOrderByList(t.Fs)
	case *mergeorder.Argument:
		p.OrderBy = convertToPlanOrderByList(t.Fs)
	// unexpected.
	case *update.Argument:
	case *deletion.Argument:
	case *insert.Argument:
	// operators that nothing need to do.
	case *connector.Argument:
	case *output.Argument:
	}
	return p
}

// convert pipeline.Instruction to vm.Instruction
func convertToVmInstruction(opr *pipeline.Instruction) vm.Instruction {
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
		v.Arg = &dispatch.Argument{
			All: t.All,
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
		v.Arg = &connector.Argument{}
	case vm.Output:
		v.Arg = &output.Argument{}
	}
	return v
}

func encodeScope(s *Scope) ([]byte, error) {
	// convert scope to pipeline.Pipeline
	p := convertScopeToPipeline(s)
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
	// convert pipeline.Pipeline to scope.
	s := convertPipelineToScope(p)
	return s, nil
}

func newCompile() *Compile {
	// TODO: fill method should send by stream
	c := &Compile{
		ctx: context.Background(),
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

/*
func (s *Scope) remoteRun(c *Compile) error {
	// encode the scope
	sData, errEncode := encodeScope(s)
	if errEncode != nil {
		return errEncode
	}

	// send encoded message
	message := &pipeline.Message{Data: sData}
	r, errSend := CNClient.Send(c.ctx, s.NodeInfo.Addr, message, morpc.SendOptions{})
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

		errMessage := m.GetData()
		if len(errMessage) > 0 {
			return errors.New(string(errMessage))
		}

		sid := m.GetID()
		if sid == cnservice.MessageEnd {
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
*/

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
	return nil, nil
}

func sendToConnectOperator(arg *connector.Argument, bat *batch.Batch) {
	select {
	case <-arg.Reg.Ctx.Done():
	case arg.Reg.Ch <- bat:
	}
}

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
	"fmt"
	"unsafe"

	"github.com/matrixorigin/matrixone/pkg/sql/colexec/apply"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/hashbuild"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/indexbuild"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/productl2"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/shufflebuild"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/table_scan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/unionall"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/pipeline"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/aggexec"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/anti"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/connector"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/deletion"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/dispatch"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/external"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/filter"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/fuzzyfilter"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/group"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/indexjoin"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/insert"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/intersect"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/intersectall"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/join"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/left"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/limit"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/lockop"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/loopjoin"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/mark"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/merge"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/mergegroup"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/mergeorder"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/mergerecursive"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/mergetop"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/minus"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/offset"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/onduplicatekey"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/order"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/output"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/preinsert"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/preinsertsecondaryindex"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/preinsertunique"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/product"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/projection"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/right"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/rightanti"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/rightsemi"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/sample"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/semi"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/shuffle"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/single"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/source"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/table_function"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/top"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/value_scan"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/process"

	"github.com/google/uuid"
)

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
	s, err := generateScope(proc, p, ctx, isRemote)
	if err != nil {
		return nil, err
	}
	if err = fillInstructionsForScope(s, ctx, p, eng); err != nil {
		s.release()
		return nil, err
	}

	return s, nil
}

// encodeProcessInfo get needed information from proc, and do serialization work.
func encodeProcessInfo(
	proc *process.Process,
	sql string,
) ([]byte, error) {
	v, err := proc.BuildProcessInfo(sql)
	if err != nil {
		return nil, err
	}
	return v.Marshal()
}

func appendWriteBackOperator(c *Compile, s *Scope) *Scope {
	rs := c.newMergeScope([]*Scope{s})
	op := output.NewArgument().
		WithFunc(c.fill)
	op.SetIdx(-1)
	rs.setRootOperator(op)
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
	p.IsLoad = s.IsLoad
	p.UuidsToRegIdx = convertScopeRemoteReceivInfo(s)

	// Plan
	if ctxId == 1 {
		// encode and decode cost is too large for it.
		// only encode the first one.
		p.Qry = s.Plan
	}
	var data []byte
	if s.NodeInfo.Data != nil {
		if data, err = s.NodeInfo.Data.MarshalBinary(); err != nil {
			return nil, -1, err
		}
	}
	p.Node = &pipeline.NodeInfo{
		Id:      s.NodeInfo.Id,
		Addr:    s.NodeInfo.Addr,
		Mcpu:    int32(s.NodeInfo.Mcpu),
		Payload: string(data),
	}
	ctx.pipe = p
	ctx.scope = s
	p.ChildrenCount = int32(len(s.Proc.Reg.MergeReceivers))
	{
		for i := range s.Proc.Reg.MergeReceivers {
			p.ChannelBufferSize = append(p.ChannelBufferSize, int32(cap(s.Proc.Reg.MergeReceivers[i].Ch2)))
			p.NilBatchCnt = append(p.NilBatchCnt, int32(s.Proc.Reg.MergeReceivers[i].NilBatchCnt))
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
			Expr:                   s.DataSource.FilterExpr,
			TableDef:               s.DataSource.TableDef,
			Timestamp:              &s.DataSource.Timestamp,
			RuntimeFilterProbeList: s.DataSource.RuntimeFilterSpecs,
			IsConst:                s.DataSource.isConst,
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
	var ins *pipeline.Instruction
	err = vm.HandleAllOp(s.RootOp, func(parentOp vm.Operator, op vm.Operator) error {
		if ctxId, ins, err = convertToPipelineInstruction(op, s.Proc, ctx, ctxId); err != nil {
			return err
		}
		p.InstructionList = append(p.InstructionList, ins)
		return nil
	})
	if err != nil {
		return ctxId, err
	}

	return ctxId, nil
}

func convertPipelineUuid(p *pipeline.Pipeline, s *Scope) error {
	s.RemoteReceivRegInfos = make([]RemoteReceivRegInfo, len(p.UuidsToRegIdx))
	for i := range p.UuidsToRegIdx {
		op := p.UuidsToRegIdx[i]
		uid, err := uuid.FromBytes(op.GetUuid())
		if err != nil {
			return moerr.NewInternalErrorNoCtxf("decode uuid failed: %s\n", err)
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
func generateScope(proc *process.Process, p *pipeline.Pipeline, ctx *scopeContext, isRemote bool) (*Scope, error) {
	var err error
	var s *Scope
	defer func() {
		if err != nil {
			s.release()
		}
	}()

	if p.Qry != nil {
		ctx.plan = p.Qry
	}

	s = newScope(magicType(p.GetPipelineType()))
	s.IsEnd = p.IsEnd
	s.IsLoad = p.IsLoad
	s.IsRemote = isRemote
	if err = convertPipelineUuid(p, s); err != nil {
		return nil, err
	}
	dsc := p.GetDataSource()
	if dsc != nil {
		s.DataSource = &Source{
			SchemaName:         dsc.SchemaName,
			RelationName:       dsc.TableName,
			Attributes:         dsc.ColList,
			PushdownId:         dsc.PushdownId,
			PushdownAddr:       dsc.PushdownAddr,
			FilterExpr:         dsc.Expr,
			TableDef:           dsc.TableDef,
			Timestamp:          *dsc.Timestamp,
			RuntimeFilterSpecs: dsc.RuntimeFilterProbeList,
			isConst:            dsc.IsConst,
		}
	}
	//var relData engine.RelData
	if p.Node != nil {
		s.NodeInfo.Id = p.Node.Id
		s.NodeInfo.Addr = p.Node.Addr
		s.NodeInfo.Mcpu = int(p.Node.Mcpu)

		bs := []byte(p.Node.Payload)
		var relData engine.RelData
		if len(bs) > 0 {
			rd, err := disttae.UnmarshalRelationData(bs)
			if err != nil {
				return nil, err
			}
			relData = rd
		}
		s.NodeInfo.Data = relData
	}
	s.Proc = proc.NewNoContextChildProcWithChannel(int(p.ChildrenCount), p.ChannelBufferSize, p.NilBatchCnt)
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
		if s.PreScopes[i], err = generateScope(s.Proc, p.Children[i], ctx.children[i], isRemote); err != nil {
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
	for i := range p.InstructionList {
		ins, err := convertToVmOperator(p.InstructionList[i], ctx, eng)
		if err != nil {
			return err
		}
		s.doSetRootOperator(ins)
	}
	return nil
}

// convert vm.Instruction to pipeline.Instruction
// todo: bad design, need to be refactored. and please refer to how sample operator do.
func convertToPipelineInstruction(op vm.Operator, proc *process.Process, ctx *scopeContext, ctxId int32) (int32, *pipeline.Instruction, error) {
	opBase := op.GetOperatorBase()
	in := &pipeline.Instruction{
		Op:      int32(op.OpType()),
		Idx:     int32(opBase.Idx),
		IsFirst: opBase.IsFirst,
		IsLast:  opBase.IsLast,

		CnAddr:      opBase.CnAddr,
		OperatorId:  opBase.OperatorID,
		ParallelId:  opBase.ParallelID,
		MaxParallel: opBase.MaxParallel,
	}
	switch t := op.(type) {
	case *insert.Insert:
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
	case *deletion.Deletion:
		in.Delete = &pipeline.Deletion{
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
	case *onduplicatekey.OnDuplicatekey:
		in.OnDuplicateKey = &pipeline.OnDuplicateKey{
			Attrs:              t.Attrs,
			InsertColCount:     t.InsertColCount,
			UniqueColCheckExpr: t.UniqueColCheckExpr,
			UniqueCols:         t.UniqueCols,
			OnDuplicateIdx:     t.OnDuplicateIdx,
			OnDuplicateExpr:    t.OnDuplicateExpr,
		}
	case *fuzzyfilter.FuzzyFilter:
		in.FuzzyFilter = &pipeline.FuzzyFilter{
			N:                  float32(t.N),
			PkName:             t.PkName,
			PkTyp:              t.PkTyp,
			BuildIdx:           int32(t.BuildIdx),
			IfInsertFromUnique: t.IfInsertFromUnique,
		}
	case *preinsert.PreInsert:
		in.PreInsert = &pipeline.PreInsert{
			SchemaName:        t.SchemaName,
			TableDef:          t.TableDef,
			HasAutoCol:        t.HasAutoCol,
			IsUpdate:          t.IsUpdate,
			Attrs:             t.Attrs,
			EstimatedRowCount: int64(t.EstimatedRowCount),
			CompPkeyExpr:      t.CompPkeyExpr,
			ClusterByExpr:     t.ClusterByExpr,
		}
	case *lockop.LockOp:
		in.LockOp = &pipeline.LockOp{
			Block:   t.Block(),
			Targets: t.CopyToPipelineTarget(),
		}
	case *preinsertunique.PreInsertUnique:
		in.PreInsertUnique = &pipeline.PreInsertUnique{
			PreInsertUkCtx: t.PreInsertCtx,
		}
	case *preinsertsecondaryindex.PreInsertSecIdx:
		in.PreInsertSecondaryIndex = &pipeline.PreInsertSecondaryIndex{
			PreInsertSkCtx: t.PreInsertCtx,
		}
	case *anti.AntiJoin:
		in.Anti = &pipeline.AntiJoin{
			Expr:                   t.Cond,
			LeftCond:               t.Conditions[0],
			RightCond:              t.Conditions[1],
			Result:                 t.Result,
			HashOnPk:               t.HashOnPK,
			IsShuffle:              t.IsShuffle,
			ShuffleIdx:             t.ShuffleIdx,
			RuntimeFilterBuildList: t.RuntimeFilterSpecs,
			JoinMapTag:             t.JoinMapTag,
		}
		in.ProjectList = t.ProjectList
	case *shuffle.Shuffle:
		in.Shuffle = &pipeline.Shuffle{}
		in.Shuffle.ShuffleColIdx = t.ShuffleColIdx
		in.Shuffle.ShuffleType = t.ShuffleType
		in.Shuffle.ShuffleColMax = t.ShuffleColMax
		in.Shuffle.ShuffleColMin = t.ShuffleColMin
		in.Shuffle.AliveRegCnt = t.AliveRegCnt
		in.Shuffle.ShuffleRangesUint64 = t.ShuffleRangeUint64
		in.Shuffle.ShuffleRangesInt64 = t.ShuffleRangeInt64
		in.Shuffle.RuntimeFilterSpec = t.RuntimeFilterSpec
	case *dispatch.Dispatch:
		in.Dispatch = &pipeline.Dispatch{IsSink: t.IsSink, ShuffleType: t.ShuffleType, RecSink: t.RecSink, RecCte: t.RecCTE, FuncId: int32(t.FuncId)}
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
	case *group.Group:
		in.Agg = &pipeline.Group{
			PreAllocSize: t.PreAllocSize,
			NeedEval:     t.NeedEval,
			Exprs:        t.Exprs,
			Aggs:         convertToPipelineAggregates(t.Aggs),
		}
		in.ProjectList = t.ProjectList
	case *sample.Sample:
		t.ConvertToPipelineOperator(in)

	case *join.InnerJoin:
		relList, colList := getRelColList(t.Result)
		in.Join = &pipeline.Join{
			RelList:                relList,
			ColList:                colList,
			Expr:                   t.Cond,
			LeftCond:               t.Conditions[0],
			RightCond:              t.Conditions[1],
			RuntimeFilterBuildList: t.RuntimeFilterSpecs,
			HashOnPk:               t.HashOnPK,
			IsShuffle:              t.IsShuffle,
			ShuffleIdx:             t.ShuffleIdx,
			JoinMapTag:             t.JoinMapTag,
		}
		in.ProjectList = t.ProjectList
	case *left.LeftJoin:
		relList, colList := getRelColList(t.Result)
		in.LeftJoin = &pipeline.LeftJoin{
			RelList:                relList,
			ColList:                colList,
			Expr:                   t.Cond,
			Types:                  convertToPlanTypes(t.Typs),
			LeftCond:               t.Conditions[0],
			RightCond:              t.Conditions[1],
			RuntimeFilterBuildList: t.RuntimeFilterSpecs,
			HashOnPk:               t.HashOnPK,
			IsShuffle:              t.IsShuffle,
			ShuffleIdx:             t.ShuffleIdx,
			JoinMapTag:             t.JoinMapTag,
		}
		in.ProjectList = t.ProjectList
	case *right.RightJoin:
		rels, poses := getRelColList(t.Result)
		in.RightJoin = &pipeline.RightJoin{
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
			ShuffleIdx:             t.ShuffleIdx,
			JoinMapTag:             t.JoinMapTag,
		}
	case *rightsemi.RightSemi:
		in.RightSemiJoin = &pipeline.RightSemiJoin{
			Result:                 t.Result,
			Expr:                   t.Cond,
			RightTypes:             convertToPlanTypes(t.RightTypes),
			LeftCond:               t.Conditions[0],
			RightCond:              t.Conditions[1],
			RuntimeFilterBuildList: t.RuntimeFilterSpecs,
			HashOnPk:               t.HashOnPK,
			IsShuffle:              t.IsShuffle,
			ShuffleIdx:             t.ShuffleIdx,
			JoinMapTag:             t.JoinMapTag,
		}
	case *rightanti.RightAnti:
		in.RightAntiJoin = &pipeline.RightAntiJoin{
			Result:                 t.Result,
			Expr:                   t.Cond,
			RightTypes:             convertToPlanTypes(t.RightTypes),
			LeftCond:               t.Conditions[0],
			RightCond:              t.Conditions[1],
			RuntimeFilterBuildList: t.RuntimeFilterSpecs,
			HashOnPk:               t.HashOnPK,
			IsShuffle:              t.IsShuffle,
			ShuffleIdx:             t.ShuffleIdx,
			JoinMapTag:             t.JoinMapTag,
		}
	case *limit.Limit:
		in.Limit = t.LimitExpr
	case *loopjoin.LoopJoin:
		relList, colList := getRelColList(t.Result)
		in.Join = &pipeline.Join{
			RelList:    relList,
			ColList:    colList,
			Types:      convertToPlanTypes(t.Typs),
			Expr:       t.Cond,
			JoinMapTag: t.JoinMapTag,
			JoinType:   int32(t.JoinType),
		}
		in.ProjectList = t.ProjectList
	case *offset.Offset:
		in.Offset = t.OffsetExpr
	case *order.Order:
		in.OrderBy = t.OrderBySpec
	case *product.Product:
		relList, colList := getRelColList(t.Result)
		in.Product = &pipeline.Product{
			RelList:    relList,
			ColList:    colList,
			IsShuffle:  t.IsShuffle,
			JoinMapTag: t.JoinMapTag,
		}
	case *productl2.Productl2:
		relList, colList := getRelColList(t.Result)
		in.ProductL2 = &pipeline.ProductL2{
			RelList:    relList,
			ColList:    colList,
			JoinMapTag: t.JoinMapTag,
		}
		in.ProjectList = t.ProjectList
	case *projection.Projection:
		in.ProjectList = t.ProjectList
	case *filter.Filter:
		in.Filter = t.E
	case *semi.SemiJoin:
		in.SemiJoin = &pipeline.SemiJoin{
			Result:                 t.Result,
			Expr:                   t.Cond,
			LeftCond:               t.Conditions[0],
			RightCond:              t.Conditions[1],
			RuntimeFilterBuildList: t.RuntimeFilterSpecs,
			HashOnPk:               t.HashOnPK,
			IsShuffle:              t.IsShuffle,
			ShuffleIdx:             t.ShuffleIdx,
			JoinMapTag:             t.JoinMapTag,
		}
		in.ProjectList = t.ProjectList
	case *indexjoin.IndexJoin:
		in.IndexJoin = &pipeline.IndexJoin{
			Result:                 t.Result,
			RuntimeFilterBuildList: t.RuntimeFilterSpecs,
		}
		in.ProjectList = t.ProjectList
	case *single.SingleJoin:
		relList, colList := getRelColList(t.Result)
		in.SingleJoin = &pipeline.SingleJoin{
			RelList:                relList,
			ColList:                colList,
			Expr:                   t.Cond,
			Types:                  convertToPlanTypes(t.Typs),
			LeftCond:               t.Conditions[0],
			RightCond:              t.Conditions[1],
			RuntimeFilterBuildList: t.RuntimeFilterSpecs,
			HashOnPk:               t.HashOnPK,
			JoinMapTag:             t.JoinMapTag,
		}
		in.ProjectList = t.ProjectList
	case *top.Top:
		in.Limit = t.Limit
		in.OrderBy = t.Fs
	// we reused ANTI to store the information here because of the lack of related structure.
	case *intersect.Intersect: // 1
		in.Anti = &pipeline.AntiJoin{}
	case *minus.Minus: // 2
		in.Anti = &pipeline.AntiJoin{}
	case *intersectall.IntersectAll:
		in.Anti = &pipeline.AntiJoin{}
	case *merge.Merge:
		in.Merge = &pipeline.Merge{
			SinkScan: t.SinkScan,
			Partial:  t.Partial,
			StartIdx: t.StartIDX,
			EndIdx:   t.EndIDX,
		}
	case *mergerecursive.MergeRecursive:
	case *mergegroup.MergeGroup:
		in.Agg = &pipeline.Group{
			NeedEval: t.NeedEval,
		}
		in.ProjectList = t.ProjectList
		EncodeMergeGroup(t, in.Agg)
	case *mergetop.MergeTop:
		in.Limit = t.Limit
		in.OrderBy = t.Fs
	case *mergeorder.MergeOrder:
		in.OrderBy = t.OrderBySpecs
	case *connector.Connector:
		idx, ctx0 := ctx.root.findRegister(t.Reg)
		in.Connect = &pipeline.Connector{
			PipelineId:     ctx0.id,
			ConnectorIndex: idx,
		}
	case *mark.MarkJoin:
		in.MarkJoin = &pipeline.MarkJoin{
			Result:     t.Result,
			LeftCond:   t.Conditions[0],
			RightCond:  t.Conditions[1],
			Expr:       t.Cond,
			OnList:     t.OnList,
			HashOnPk:   t.HashOnPK,
			JoinMapTag: t.JoinMapTag,
		}
		in.ProjectList = t.ProjectList
	case *table_function.TableFunction:
		in.TableFunction = &pipeline.TableFunction{
			Attrs:  t.Attrs,
			Rets:   t.Rets,
			Args:   t.Args,
			Params: t.Params,
			Name:   t.FuncName,
		}

	case *external.External:
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
			TbColToDataCol:  t.Es.TbColToDataCol,
			StrictSqlMode:   t.Es.StrictSqlMode,
		}
		in.ProjectList = t.ProjectList
	case *source.Source:
		in.StreamScan = &pipeline.StreamScan{
			TblDef: t.TblDef,
			Limit:  t.Limit,
			Offset: t.Offset,
		}
		in.ProjectList = t.ProjectList
	case *table_scan.TableScan:
		in.TableScan = &pipeline.TableScan{}
		in.TableScan.Types = t.Types
		in.ProjectList = t.ProjectList
	case *value_scan.ValueScan:
		if err := op.Prepare(proc); err != nil {
			return -1, nil, err
		}
		in.ValueScan = &pipeline.ValueScan{}
		if t.Batchs != nil {
			data, err := types.Encode(t.Batchs[0])
			if err != nil {
				return -1, nil, err
			}
			in.ValueScan.BatchBlock = string(data)
		}

		in.ProjectList = t.ProjectList
	case *unionall.UnionAll:
		in.UnionAll = &pipeline.UnionAll{}
	case *hashbuild.HashBuild:
		in.HashBuild = &pipeline.HashBuild{
			NeedHashMap:       t.NeedHashMap,
			HashOnPK:          t.HashOnPK,
			NeedBatches:       t.NeedBatches,
			NeedAllocateSels:  t.NeedAllocateSels,
			Conditions:        t.Conditions,
			JoinMapTag:        t.JoinMapTag,
			JoinMapRefCnt:     t.JoinMapRefCnt,
			RuntimeFilterSpec: t.RuntimeFilterSpec,
		}
	case *shufflebuild.ShuffleBuild:
		in.ShuffleBuild = &pipeline.Shufflebuild{
			HashOnPK:          t.HashOnPK,
			NeedBatches:       t.NeedBatches,
			NeedAllocateSels:  t.NeedAllocateSels,
			Conditions:        t.Conditions,
			RuntimeFilterSpec: t.RuntimeFilterSpec,
			JoinMapTag:        t.JoinMapTag,
			ShuffleIdx:        t.ShuffleIdx,
		}
	case *indexbuild.IndexBuild:
		in.IndexBuild = &pipeline.Indexbuild{
			RuntimeFilterSpec: t.RuntimeFilterSpec,
		}
	case *apply.Apply:
		relList, colList := getRelColList(t.Result)
		in.Apply = &pipeline.Apply{
			ApplyType: int32(t.ApplyType),
			RelList:   relList,
			ColList:   colList,
			Types:     convertToPlanTypes(t.Typs),
		}
		in.ProjectList = t.ProjectList
		in.TableFunction = &pipeline.TableFunction{
			Attrs:  t.TableFunction.Attrs,
			Rets:   t.TableFunction.Rets,
			Args:   t.TableFunction.Args,
			Params: t.TableFunction.Params,
			Name:   t.TableFunction.FuncName,
		}
	default:
		return -1, nil, moerr.NewInternalErrorNoCtx(fmt.Sprintf("unexpected operator: %v", op.OpType()))
	}
	return ctxId, in, nil
}

// convert pipeline.Instruction to vm.Instruction
func convertToVmOperator(opr *pipeline.Instruction, ctx *scopeContext, eng engine.Engine) (vm.Operator, error) {
	var op vm.Operator

	switch vm.OpType(opr.Op) {
	case vm.Deletion:
		t := opr.GetDelete()
		arg := deletion.NewArgument()
		arg.RemoteDelete = t.RemoteDelete
		arg.SegmentMap = t.SegmentMap
		arg.IBucket = t.IBucket
		arg.Nbucket = t.NBucket
		arg.DeleteCtx = &deletion.DeleteCtx{
			CanTruncate:           t.CanTruncate,
			RowIdIdx:              int(t.RowIdIdx),
			PartitionTableIDs:     t.PartitionTableIds,
			PartitionTableNames:   t.PartitionTableNames,
			PartitionIndexInBatch: int(t.PartitionIndexInBatch),
			Ref:                   t.Ref,
			AddAffectedRows:       t.AddAffectedRows,
			PrimaryKeyIdx:         int(t.PrimaryKeyIdx),
		}
		op = arg
	case vm.Insert:
		t := opr.GetInsert()
		arg := insert.NewArgument()
		arg.ToWriteS3 = t.ToWriteS3
		arg.InsertCtx = &insert.InsertCtx{
			Ref:                   t.Ref,
			AddAffectedRows:       t.AddAffectedRows,
			Attrs:                 t.Attrs,
			PartitionTableIDs:     t.PartitionTableIds,
			PartitionTableNames:   t.PartitionTableNames,
			PartitionIndexInBatch: int(t.PartitionIdx),
			TableDef:              t.TableDef,
		}
		op = arg
	case vm.PreInsert:
		t := opr.GetPreInsert()
		arg := preinsert.NewArgument()
		arg.SchemaName = t.GetSchemaName()
		arg.TableDef = t.GetTableDef()
		arg.Attrs = t.GetAttrs()
		arg.HasAutoCol = t.GetHasAutoCol()
		arg.IsUpdate = t.GetIsUpdate()
		arg.EstimatedRowCount = int64(t.GetEstimatedRowCount())
		arg.CompPkeyExpr = t.CompPkeyExpr
		arg.ClusterByExpr = t.ClusterByExpr
		op = arg
	case vm.LockOp:
		t := opr.GetLockOp()
		lockArg := lockop.NewArgumentByEngine(eng)
		lockArg.SetBlock(t.Block)
		for _, target := range t.Targets {
			typ := plan2.MakeTypeByPlan2Type(target.PrimaryColTyp)
			lockArg.AddLockTarget(target.GetTableId(), target.GetPrimaryColIdxInBat(), typ, target.GetRefreshTsIdxInBat())
		}
		for _, target := range t.Targets {
			if target.LockTable {
				lockArg.LockTable(target.TableId, target.ChangeDef)
			}
		}
		op = lockArg
	case vm.PreInsertUnique:
		t := opr.GetPreInsertUnique()
		arg := preinsertunique.NewArgument()
		arg.PreInsertCtx = t.GetPreInsertUkCtx()
		op = arg
	case vm.PreInsertSecondaryIndex:
		t := opr.GetPreInsertSecondaryIndex()
		arg := preinsertsecondaryindex.NewArgument()
		arg.PreInsertCtx = t.GetPreInsertSkCtx()
		op = arg
	case vm.OnDuplicateKey:
		t := opr.GetOnDuplicateKey()
		arg := onduplicatekey.NewArgument()
		arg.Attrs = t.Attrs
		arg.InsertColCount = t.InsertColCount
		arg.UniqueColCheckExpr = t.UniqueColCheckExpr
		arg.UniqueCols = t.UniqueCols
		arg.OnDuplicateIdx = t.OnDuplicateIdx
		arg.OnDuplicateExpr = t.OnDuplicateExpr
		arg.IsIgnore = t.IsIgnore
		op = arg
	case vm.FuzzyFilter:
		t := opr.GetFuzzyFilter()
		arg := fuzzyfilter.NewArgument()
		arg.N = float64(t.N)
		arg.PkName = t.PkName
		arg.PkTyp = t.PkTyp
		arg.IfInsertFromUnique = t.IfInsertFromUnique
		op = arg
	case vm.Anti:
		t := opr.GetAnti()
		arg := anti.NewArgument()
		arg.Cond = t.Expr
		arg.Conditions = [][]*plan.Expr{
			t.LeftCond, t.RightCond,
		}
		arg.Result = t.Result
		arg.HashOnPK = t.HashOnPk
		arg.IsShuffle = t.IsShuffle
		arg.ShuffleIdx = t.ShuffleIdx
		arg.RuntimeFilterSpecs = t.RuntimeFilterBuildList
		arg.JoinMapTag = t.JoinMapTag
		arg.ProjectList = opr.ProjectList
		op = arg
	case vm.Shuffle:
		t := opr.GetShuffle()
		arg := shuffle.NewArgument()
		arg.ShuffleColIdx = t.ShuffleColIdx
		arg.ShuffleType = t.ShuffleType
		arg.ShuffleColMin = t.ShuffleColMin
		arg.ShuffleColMax = t.ShuffleColMax
		arg.AliveRegCnt = t.AliveRegCnt
		arg.ShuffleRangeInt64 = t.ShuffleRangesInt64
		arg.ShuffleRangeUint64 = t.ShuffleRangesUint64
		arg.RuntimeFilterSpec = t.RuntimeFilterSpec
		op = arg
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
					return op, err
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

		arg := dispatch.NewArgument()
		arg.IsSink = t.IsSink
		arg.RecSink = t.RecSink
		arg.RecCTE = t.RecCte
		arg.FuncId = int(t.FuncId)
		arg.LocalRegs = regs
		arg.RemoteRegs = rrs
		arg.ShuffleType = t.ShuffleType
		arg.ShuffleRegIdxLocal = shuffleRegIdxLocal
		arg.ShuffleRegIdxRemote = shuffleRegIdxRemote
		op = arg
	case vm.Group:
		t := opr.GetAgg()
		arg := group.NewArgument()
		arg.PreAllocSize = t.PreAllocSize
		arg.NeedEval = t.NeedEval
		arg.Exprs = t.Exprs
		arg.Aggs = convertToAggregates(t.Aggs)
		arg.ProjectList = opr.ProjectList
		op = arg
	case vm.Sample:
		op = sample.GenerateFromPipelineOperator(opr)

	case vm.Join:
		t := opr.GetJoin()
		arg := join.NewArgument()
		arg.Cond = t.Expr
		arg.Result = convertToResultPos(t.RelList, t.ColList)
		arg.Conditions = [][]*plan.Expr{t.LeftCond, t.RightCond}
		arg.RuntimeFilterSpecs = t.RuntimeFilterBuildList
		arg.HashOnPK = t.HashOnPk
		arg.IsShuffle = t.IsShuffle
		arg.ShuffleIdx = t.ShuffleIdx
		arg.JoinMapTag = t.JoinMapTag
		arg.ProjectList = opr.ProjectList
		op = arg
	case vm.Left:
		t := opr.GetLeftJoin()
		arg := left.NewArgument()
		arg.Cond = t.Expr
		arg.Typs = convertToTypes(t.Types)
		arg.Result = convertToResultPos(t.RelList, t.ColList)
		arg.Conditions = [][]*plan.Expr{t.LeftCond, t.RightCond}
		arg.RuntimeFilterSpecs = t.RuntimeFilterBuildList
		arg.HashOnPK = t.HashOnPk
		arg.IsShuffle = t.IsShuffle
		arg.ShuffleIdx = t.ShuffleIdx
		arg.JoinMapTag = t.JoinMapTag
		arg.ProjectList = opr.ProjectList
		op = arg
	case vm.Right:
		t := opr.GetRightJoin()
		arg := right.NewArgument()
		arg.Result = convertToResultPos(t.RelList, t.ColList)
		arg.LeftTypes = convertToTypes(t.LeftTypes)
		arg.RightTypes = convertToTypes(t.RightTypes)
		arg.Cond = t.Expr
		arg.Conditions = [][]*plan.Expr{t.LeftCond, t.RightCond}
		arg.RuntimeFilterSpecs = t.RuntimeFilterBuildList
		arg.HashOnPK = t.HashOnPk
		arg.IsShuffle = t.IsShuffle
		arg.ShuffleIdx = t.ShuffleIdx
		arg.JoinMapTag = t.JoinMapTag
		op = arg
	case vm.RightSemi:
		t := opr.GetRightSemiJoin()
		arg := rightsemi.NewArgument()
		arg.Result = t.Result
		arg.RightTypes = convertToTypes(t.RightTypes)
		arg.Cond = t.Expr
		arg.Conditions = [][]*plan.Expr{t.LeftCond, t.RightCond}
		arg.RuntimeFilterSpecs = t.RuntimeFilterBuildList
		arg.HashOnPK = t.HashOnPk
		arg.IsShuffle = t.IsShuffle
		arg.ShuffleIdx = t.ShuffleIdx
		arg.JoinMapTag = t.JoinMapTag
		op = arg
	case vm.RightAnti:
		t := opr.GetRightAntiJoin()
		arg := rightanti.NewArgument()
		arg.Result = t.Result
		arg.RightTypes = convertToTypes(t.RightTypes)
		arg.Cond = t.Expr
		arg.Conditions = [][]*plan.Expr{t.LeftCond, t.RightCond}
		arg.RuntimeFilterSpecs = t.RuntimeFilterBuildList
		arg.HashOnPK = t.HashOnPk
		arg.IsShuffle = t.IsShuffle
		arg.ShuffleIdx = t.ShuffleIdx
		arg.JoinMapTag = t.JoinMapTag
		op = arg
	case vm.Limit:
		op = limit.NewArgument().WithLimit(opr.Limit)
	case vm.LoopJoin:
		t := opr.GetJoin()
		arg := loopjoin.NewArgument()
		arg.Result = convertToResultPos(t.RelList, t.ColList)
		arg.Typs = convertToTypes(t.Types)
		arg.Cond = t.Expr
		arg.JoinMapTag = t.JoinMapTag
		arg.JoinType = int(t.JoinType)
		arg.ProjectList = opr.ProjectList
		op = arg
	case vm.IndexJoin:
		t := opr.GetIndexJoin()
		arg := indexjoin.NewArgument()
		arg.Result = t.Result
		arg.RuntimeFilterSpecs = t.RuntimeFilterBuildList
		arg.ProjectList = opr.ProjectList
		op = arg
	case vm.Offset:
		op = offset.NewArgument().WithOffset(opr.Offset)
	case vm.Order:
		arg := order.NewArgument()
		arg.OrderBySpec = opr.OrderBy
		op = arg
	case vm.Product:
		t := opr.GetProduct()
		arg := product.NewArgument()
		arg.Result = convertToResultPos(t.RelList, t.ColList)
		arg.IsShuffle = t.IsShuffle
		arg.JoinMapTag = t.JoinMapTag
		arg.ProjectList = opr.ProjectList
		op = arg
	case vm.ProductL2:
		t := opr.GetProductL2()
		arg := productl2.NewArgument()
		arg.Result = convertToResultPos(t.RelList, t.ColList)
		arg.OnExpr = t.Expr
		arg.JoinMapTag = t.JoinMapTag
		arg.ProjectList = opr.ProjectList
		op = arg
	case vm.Projection:
		arg := projection.NewArgument()
		arg.ProjectList = opr.ProjectList
		op = arg
	case vm.Filter:
		arg := filter.NewArgument()
		arg.E = opr.Filter
		op = arg
	case vm.Semi:
		t := opr.GetSemiJoin()
		arg := semi.NewArgument()
		arg.Result = t.Result
		arg.Cond = t.Expr
		arg.Conditions = [][]*plan.Expr{t.LeftCond, t.RightCond}
		arg.RuntimeFilterSpecs = t.RuntimeFilterBuildList
		arg.HashOnPK = t.HashOnPk
		arg.IsShuffle = t.IsShuffle
		arg.ShuffleIdx = t.ShuffleIdx
		arg.JoinMapTag = t.JoinMapTag
		arg.ProjectList = opr.ProjectList
		op = arg
	case vm.Single:
		t := opr.GetSingleJoin()
		arg := single.NewArgument()
		arg.Result = convertToResultPos(t.RelList, t.ColList)
		arg.Cond = t.Expr
		arg.Typs = convertToTypes(t.Types)
		arg.Conditions = [][]*plan.Expr{t.LeftCond, t.RightCond}
		arg.RuntimeFilterSpecs = t.RuntimeFilterBuildList
		arg.HashOnPK = t.HashOnPk
		arg.JoinMapTag = t.JoinMapTag
		arg.ProjectList = opr.ProjectList
		op = arg
	case vm.Mark:
		t := opr.GetMarkJoin()
		arg := mark.NewArgument()
		arg.Result = t.Result
		arg.Conditions = [][]*plan.Expr{t.LeftCond, t.RightCond}
		arg.Cond = t.Expr
		arg.OnList = t.OnList
		arg.HashOnPK = t.HashOnPk
		arg.JoinMapTag = t.JoinMapTag
		arg.ProjectList = opr.ProjectList
		op = arg
	case vm.Top:
		op = top.NewArgument().
			WithLimit(opr.Limit).
			WithFs(opr.OrderBy)
	// should change next day?
	case vm.Intersect:
		op = intersect.NewArgument()
	case vm.IntersectAll:
		op = intersect.NewArgument()
	case vm.Minus:
		op = minus.NewArgument()
	case vm.Connector:
		t := opr.GetConnect()
		op = connector.NewArgument().
			WithReg(ctx.root.getRegister(t.PipelineId, t.ConnectorIndex))
	case vm.Merge:
		t := opr.GetMerge()
		mergeOp := merge.NewArgument()
		mergeOp.SinkScan = t.SinkScan
		mergeOp.Partial = t.Partial
		mergeOp.StartIDX = t.StartIdx
		mergeOp.EndIDX = t.EndIdx
		op = mergeOp
	case vm.MergeRecursive:
		op = mergerecursive.NewArgument()
	case vm.MergeGroup:
		arg := mergegroup.NewArgument()
		arg.NeedEval = opr.Agg.NeedEval
		arg.ProjectList = opr.ProjectList
		op = arg
		DecodeMergeGroup(op.(*mergegroup.MergeGroup), opr.Agg)
	case vm.MergeTop:
		op = mergetop.NewArgument().
			WithLimit(opr.Limit).
			WithFs(opr.OrderBy)
	case vm.MergeOrder:
		arg := mergeorder.NewArgument()
		arg.OrderBySpecs = opr.OrderBy
		op = arg
	case vm.TableFunction:
		arg := table_function.NewArgument()
		arg.Attrs = opr.TableFunction.Attrs
		arg.Rets = opr.TableFunction.Rets
		arg.Args = opr.TableFunction.Args
		arg.FuncName = opr.TableFunction.Name
		arg.Params = opr.TableFunction.Params
		op = arg
	case vm.External:
		t := opr.GetExternalScan()
		name2ColIndex := make(map[string]int32)
		for _, n2i := range t.Name2ColIndex {
			name2ColIndex[n2i.Name] = n2i.Index
		}
		op = external.NewArgument().WithEs(
			&external.ExternalParam{
				ExParamConst: external.ExParamConst{
					Attrs:           t.Attrs,
					FileSize:        t.FileSize,
					FileOffsetTotal: t.FileOffsetTotal,
					Cols:            t.Cols,
					CreateSql:       t.CreateSql,
					Name2ColIndex:   name2ColIndex,
					FileList:        t.FileList,
					TbColToDataCol:  t.TbColToDataCol,
					StrictSqlMode:   t.StrictSqlMode,
				},
				ExParam: external.ExParam{
					Fileparam: new(external.ExFileparam),
					Filter: &external.FilterParam{
						FilterExpr: t.Filter,
					},
				},
			},
		)
		op.(*external.External).ProjectList = opr.ProjectList
	case vm.Source:
		t := opr.GetStreamScan()
		arg := source.NewArgument()
		arg.TblDef = t.TblDef
		arg.Limit = t.Limit
		arg.Offset = t.Offset
		arg.ProjectList = opr.ProjectList
		op = arg
	case vm.TableScan:
		op = table_scan.NewArgument().WithTypes(opr.TableScan.Types)
		op.(*table_scan.TableScan).ProjectList = opr.ProjectList
	case vm.ValueScan:
		op = value_scan.NewArgument()
		op.(*value_scan.ValueScan).ProjectList = opr.ProjectList
		if len(opr.ValueScan.BatchBlock) > 0 {
			bat := new(batch.Batch)
			if err := types.Decode([]byte(opr.ValueScan.BatchBlock), bat); err != nil {
				return nil, err
			}
			bat.Cnt = 1
			op.(*value_scan.ValueScan).Batchs = append(op.(*value_scan.ValueScan).Batchs, bat)
		}
	case vm.UnionAll:
		op = unionall.NewArgument()
	case vm.HashBuild:
		arg := hashbuild.NewArgument()
		t := opr.GetHashBuild()
		arg.NeedHashMap = t.NeedHashMap
		arg.HashOnPK = t.HashOnPK
		arg.NeedBatches = t.NeedBatches
		arg.NeedAllocateSels = t.NeedAllocateSels
		arg.Conditions = t.Conditions
		arg.JoinMapTag = t.JoinMapTag
		arg.JoinMapRefCnt = t.JoinMapRefCnt
		arg.RuntimeFilterSpec = t.RuntimeFilterSpec
		op = arg
	case vm.ShuffleBuild:
		arg := shufflebuild.NewArgument()
		t := opr.GetShuffleBuild()
		arg.HashOnPK = t.HashOnPK
		arg.NeedBatches = t.NeedBatches
		arg.NeedAllocateSels = t.NeedAllocateSels
		arg.Conditions = t.Conditions
		arg.RuntimeFilterSpec = t.RuntimeFilterSpec
		arg.JoinMapTag = t.JoinMapTag
		arg.ShuffleIdx = t.ShuffleIdx
		op = arg
	case vm.IndexBuild:
		arg := indexbuild.NewArgument()
		arg.RuntimeFilterSpec = opr.GetIndexBuild().RuntimeFilterSpec
		op = arg
	case vm.Apply:
		arg := apply.NewArgument()
		t := opr.GetApply()
		arg.ApplyType = int(t.ApplyType)
		arg.Result = convertToResultPos(t.RelList, t.ColList)
		arg.Typs = convertToTypes(t.Types)
		arg.ProjectList = opr.ProjectList
		arg.TableFunction = table_function.NewArgument()
		arg.TableFunction.Attrs = opr.TableFunction.Attrs
		arg.TableFunction.Rets = opr.TableFunction.Rets
		arg.TableFunction.Args = opr.TableFunction.Args
		arg.TableFunction.FuncName = opr.TableFunction.Name
		arg.TableFunction.Params = opr.TableFunction.Params
		op = arg
	default:
		return op, moerr.NewInternalErrorNoCtx(fmt.Sprintf("unexpected operator: %v", opr.Op))
	}
	op.GetOperatorBase().SetInfo(&vm.OperatorInfo{
		Idx:     int(opr.Idx),
		IsFirst: opr.IsFirst,
		IsLast:  opr.IsLast,

		CnAddr:      opr.CnAddr,
		OperatorID:  opr.OperatorId,
		ParallelID:  opr.ParallelId,
		MaxParallel: opr.MaxParallel,
	})
	return op, nil
}

// convert []types.Type to []*plan.Type
func convertToPlanTypes(ts []types.Type) []plan.Type {
	result := make([]plan.Type, len(ts))
	for i, t := range ts {
		result[i] = plan.Type{
			Id:    int32(t.Oid),
			Width: t.Width,
			Scale: t.Scale,
		}
	}
	return result
}

// convert []*plan.Type to []types.Type
func convertToTypes(ts []plan.Type) []types.Type {
	result := make([]types.Type, len(ts))
	for i, t := range ts {
		result[i] = types.New(types.T(t.Id), t.Width, t.Scale)
	}
	return result
}

// convert []aggexec.AggFuncExecExpression to []*pipeline.Aggregate
func convertToPipelineAggregates(ags []aggexec.AggFuncExecExpression) []*pipeline.Aggregate {
	result := make([]*pipeline.Aggregate, len(ags))
	for i, a := range ags {
		result[i] = &pipeline.Aggregate{
			Op:     a.GetAggID(),
			Dist:   a.IsDistinct(),
			Expr:   a.GetArgExpressions(),
			Config: a.GetExtraConfig(),
		}
	}
	return result
}

// convert []*pipeline.Aggregate to []aggexec.AggFuncExecExpression
func convertToAggregates(ags []*pipeline.Aggregate) []aggexec.AggFuncExecExpression {
	result := make([]aggexec.AggFuncExecExpression, len(ags))
	for i, a := range ags {
		result[i] = aggexec.MakeAggFunctionExpression(a.Op, a.Dist, a.Expr, a.Config)
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

// func decodeBatch(proc *process.Process, data []byte) (*batch.Batch, error) {
func decodeBatch(mp *mpool.MPool, data []byte) (*batch.Batch, error) {
	bat := new(batch.Batch)
	if err := bat.UnmarshalBinaryWithAnyMp(data, mp); err != nil {
		bat.Clean(mp)
		return nil, err
	}
	return bat, nil
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

func EncodeMergeGroup(merge *mergegroup.MergeGroup, pipe *pipeline.Group) {
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
		case types.T_bit:
			result := merge.PartialResults[i].(uint64)
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

func DecodeMergeGroup(merge *mergegroup.MergeGroup, pipe *pipeline.Group) {
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
		case types.T_bit:
			result := *(*uint64)(unsafe.Pointer(&pipe.PartialResults[0]))
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

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

	"github.com/matrixorigin/matrixone/pkg/common/bitmap"

	"github.com/matrixorigin/matrixone/pkg/vm/message"

	"github.com/matrixorigin/matrixone/pkg/sql/colexec/apply"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/mergeblock"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/productl2"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/table_scan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/unionall"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/value_scan"

	"github.com/matrixorigin/matrixone/pkg/sql/colexec/shufflebuild"

	"github.com/matrixorigin/matrixone/pkg/sql/colexec/aggexec"

	"github.com/matrixorigin/matrixone/pkg/sql/colexec/indexbuild"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/indexjoin"

	"github.com/google/uuid"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/pipeline"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/anti"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/connector"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/deletion"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/dispatch"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/external"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/fill"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/filter"
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
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/loopjoin"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/mark"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/merge"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/mergecte"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/mergegroup"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/mergeorder"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/mergerecursive"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/mergetop"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/minus"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/offset"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/onduplicatekey"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/order"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/partition"
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
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/timewin"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/top"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/window"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

var constBat *batch.Batch

func init() {
	constBat = batch.NewWithSize(0)
	constBat.SetRowCount(1)
}

func dupOperatorRecursively(sourceOp vm.Operator, index int, maxParallel int) vm.Operator {
	op := dupOperator(sourceOp, index, maxParallel)
	opBase := op.GetOperatorBase()
	numChildren := sourceOp.GetOperatorBase().NumChildren()
	for i := 0; i < numChildren; i++ {
		child := sourceOp.GetOperatorBase().GetChildren(i)
		opBase.AppendChild(dupOperatorRecursively(child, index, maxParallel))
	}
	return op
}

func dupOperator(sourceOp vm.Operator, index int, maxParallel int) vm.Operator {
	srcOpBase := sourceOp.GetOperatorBase()
	info := vm.OperatorInfo{
		Idx:         srcOpBase.Idx,
		IsFirst:     srcOpBase.IsFirst,
		IsLast:      srcOpBase.IsLast,
		CnAddr:      srcOpBase.CnAddr,
		OperatorID:  srcOpBase.OperatorID,
		MaxParallel: srcOpBase.MaxParallel,
		ParallelID:  srcOpBase.ParallelID,
	}
	switch sourceOp.OpType() {
	case vm.Anti:
		t := sourceOp.(*anti.AntiJoin)
		op := anti.NewArgument()
		op.Cond = t.Cond
		op.Conditions = t.Conditions
		op.Result = t.Result
		op.HashOnPK = t.HashOnPK
		op.IsShuffle = t.IsShuffle
		op.RuntimeFilterSpecs = t.RuntimeFilterSpecs
		op.JoinMapTag = t.JoinMapTag
		op.ProjectList = t.ProjectList
		op.SetInfo(&info)
		return op
	case vm.Group:
		t := sourceOp.(*group.Group)
		op := group.NewArgument()
		op.PreAllocSize = t.PreAllocSize
		op.NeedEval = t.NeedEval
		op.Exprs = t.Exprs
		op.Aggs = t.Aggs
		op.ProjectList = t.ProjectList
		op.SetInfo(&info)
		return op
	case vm.Sample:
		t := sourceOp.(*sample.Sample)
		op := t.SampleDup()
		op.SetInfo(&info)
		return op
	case vm.Join:
		t := sourceOp.(*join.InnerJoin)
		op := join.NewArgument()
		op.Result = t.Result
		op.Cond = t.Cond
		op.Conditions = t.Conditions
		op.RuntimeFilterSpecs = t.RuntimeFilterSpecs
		op.JoinMapTag = t.JoinMapTag
		op.HashOnPK = t.HashOnPK
		op.IsShuffle = t.IsShuffle
		op.ProjectList = t.ProjectList
		op.SetInfo(&info)
		return op
	case vm.Left:
		t := sourceOp.(*left.LeftJoin)
		op := left.NewArgument()
		op.Cond = t.Cond
		op.Result = t.Result
		op.Typs = t.Typs
		op.Conditions = t.Conditions
		op.RuntimeFilterSpecs = t.RuntimeFilterSpecs
		op.JoinMapTag = t.JoinMapTag
		op.HashOnPK = t.HashOnPK
		op.IsShuffle = t.IsShuffle
		op.ProjectList = t.ProjectList
		op.SetInfo(&info)
		return op
	case vm.Right:
		t := sourceOp.(*right.RightJoin)
		op := right.NewArgument()
		if t.Channel == nil {
			t.Channel = make(chan *bitmap.Bitmap, maxParallel)
		}
		op.Channel = t.Channel
		op.NumCPU = uint64(maxParallel)
		op.IsMerger = (index == 0)
		op.Cond = t.Cond
		op.Result = t.Result
		op.RightTypes = t.RightTypes
		op.LeftTypes = t.LeftTypes
		op.Conditions = t.Conditions
		op.RuntimeFilterSpecs = t.RuntimeFilterSpecs
		op.JoinMapTag = t.JoinMapTag
		op.HashOnPK = t.HashOnPK
		op.IsShuffle = t.IsShuffle
		op.SetInfo(&info)
		return op
	case vm.RightSemi:
		t := sourceOp.(*rightsemi.RightSemi)
		op := rightsemi.NewArgument()
		if t.Channel == nil {
			t.Channel = make(chan *bitmap.Bitmap, maxParallel)
		}
		op.Channel = t.Channel
		op.NumCPU = uint64(maxParallel)
		op.IsMerger = (index == 0)
		op.Cond = t.Cond
		op.Result = t.Result
		op.RightTypes = t.RightTypes
		op.Conditions = t.Conditions
		op.RuntimeFilterSpecs = t.RuntimeFilterSpecs
		op.JoinMapTag = t.JoinMapTag
		op.HashOnPK = t.HashOnPK
		op.IsShuffle = t.IsShuffle
		op.SetInfo(&info)
		return op
	case vm.RightAnti:
		t := sourceOp.(*rightanti.RightAnti)
		op := rightanti.NewArgument()
		if t.Channel == nil {
			t.Channel = make(chan *bitmap.Bitmap, maxParallel)
		}
		op.Channel = t.Channel
		op.NumCPU = uint64(maxParallel)
		op.IsMerger = (index == 0)
		op.Cond = t.Cond
		op.Result = t.Result
		op.RightTypes = t.RightTypes
		op.Conditions = t.Conditions
		op.RuntimeFilterSpecs = t.RuntimeFilterSpecs
		op.JoinMapTag = t.JoinMapTag
		op.HashOnPK = t.HashOnPK
		op.IsShuffle = t.IsShuffle
		op.SetInfo(&info)
		return op
	case vm.Limit:
		t := sourceOp.(*limit.Limit)
		op := limit.NewArgument()
		op.LimitExpr = t.LimitExpr
		op.SetInfo(&info)
		return op
	case vm.LoopJoin:
		t := sourceOp.(*loopjoin.LoopJoin)
		op := loopjoin.NewArgument()
		op.Result = t.Result
		op.Typs = t.Typs
		op.Cond = t.Cond
		op.JoinMapTag = t.JoinMapTag
		op.JoinType = t.JoinType
		op.ProjectList = t.ProjectList
		op.SetInfo(&info)
		return op
	case vm.IndexJoin:
		t := sourceOp.(*indexjoin.IndexJoin)
		op := indexjoin.NewArgument()
		op.Result = t.Result
		op.RuntimeFilterSpecs = t.RuntimeFilterSpecs
		op.ProjectList = t.ProjectList
		op.SetInfo(&info)
		return op
	case vm.Offset:
		t := sourceOp.(*offset.Offset)
		op := offset.NewArgument()
		op.OffsetExpr = t.OffsetExpr
		op.SetInfo(&info)
		return op
	case vm.Order:
		t := sourceOp.(*order.Order)
		op := order.NewArgument()
		op.OrderBySpec = t.OrderBySpec
		op.SetInfo(&info)
		return op
	case vm.Product:
		t := sourceOp.(*product.Product)
		op := product.NewArgument()
		op.Result = t.Result
		op.IsShuffle = t.IsShuffle
		op.JoinMapTag = t.JoinMapTag
		op.ProjectList = t.ProjectList
		op.SetInfo(&info)
		return op
	case vm.ProductL2:
		t := sourceOp.(*productl2.Productl2)
		op := productl2.NewArgument()
		op.Result = t.Result
		op.OnExpr = t.OnExpr
		op.JoinMapTag = t.JoinMapTag
		op.ProjectList = t.ProjectList
		op.SetInfo(&info)
		return op
	case vm.Projection:
		t := sourceOp.(*projection.Projection)
		op := projection.NewArgument()
		op.ProjectList = t.ProjectList
		op.SetInfo(&info)
		return op
	case vm.Filter:
		t := sourceOp.(*filter.Filter)
		op := filter.NewArgument()
		op.E = t.GetExeExpr()
		if op.E == nil {
			op.E = t.E
		}
		op.SetInfo(&info)
		return op
	case vm.Semi:
		t := sourceOp.(*semi.SemiJoin)
		op := semi.NewArgument()
		op.Result = t.Result
		op.Cond = t.Cond
		op.Conditions = t.Conditions
		op.RuntimeFilterSpecs = t.RuntimeFilterSpecs
		op.JoinMapTag = t.JoinMapTag
		op.HashOnPK = t.HashOnPK
		op.IsShuffle = t.IsShuffle
		op.ProjectList = t.ProjectList
		op.SetInfo(&info)
		return op
	case vm.Single:
		t := sourceOp.(*single.SingleJoin)
		op := single.NewArgument()
		op.Result = t.Result
		op.Cond = t.Cond
		op.Typs = t.Typs
		op.Conditions = t.Conditions
		op.RuntimeFilterSpecs = t.RuntimeFilterSpecs
		op.JoinMapTag = t.JoinMapTag
		op.HashOnPK = t.HashOnPK
		op.ProjectList = t.ProjectList
		op.SetInfo(&info)
		return op
	case vm.Top:
		t := sourceOp.(*top.Top)
		op := top.NewArgument()
		op.Limit = t.Limit
		op.TopValueTag = t.TopValueTag
		op.Fs = t.Fs
		op.SetInfo(&info)
		return op
	case vm.Intersect:
		op := intersect.NewArgument()
		op.SetInfo(&info)
		return op
	case vm.Minus: // 2
		op := minus.NewArgument()
		op.SetInfo(&info)
		return op
	case vm.IntersectAll:
		op := intersectall.NewArgument()
		op.SetInfo(&info)
		return op
	case vm.Merge:
		t := sourceOp.(*merge.Merge)
		op := merge.NewArgument()
		op.SinkScan = t.SinkScan
		op.Partial = t.Partial
		op.StartIDX = t.StartIDX
		op.EndIDX = t.EndIDX
		op.SetInfo(&info)
		return op
	case vm.MergeRecursive:
		op := mergerecursive.NewArgument()
		op.SetInfo(&info)
		return op
	case vm.MergeCTE:
		op := mergecte.NewArgument()
		op.SetInfo(&info)
		return op
	case vm.Mark:
		t := sourceOp.(*mark.MarkJoin)
		op := mark.NewArgument()
		op.Result = t.Result
		op.Conditions = t.Conditions
		op.Cond = t.Cond
		op.OnList = t.OnList
		op.HashOnPK = t.HashOnPK
		op.JoinMapTag = t.JoinMapTag
		op.ProjectList = t.ProjectList
		op.SetInfo(&info)
		return op
	case vm.TableFunction:
		t := sourceOp.(*table_function.TableFunction)
		op := table_function.NewArgument()
		op.FuncName = t.FuncName
		op.Args = t.Args
		op.Rets = t.Rets
		op.Attrs = t.Attrs
		op.Params = t.Params
		op.SetInfo(&info)
		return op
	case vm.External:
		t := sourceOp.(*external.External)
		op := external.NewArgument().WithEs(
			&external.ExternalParam{
				ExParamConst: external.ExParamConst{
					Attrs:           t.Es.Attrs,
					Cols:            t.Es.Cols,
					Idx:             index,
					Name2ColIndex:   t.Es.Name2ColIndex,
					CreateSql:       t.Es.CreateSql,
					FileList:        t.Es.FileList,
					FileSize:        t.Es.FileSize,
					FileOffsetTotal: t.Es.FileOffsetTotal,
					Extern:          t.Es.Extern,
					TbColToDataCol:  t.Es.TbColToDataCol,
					StrictSqlMode:   t.Es.StrictSqlMode,
				},
				ExParam: external.ExParam{
					Filter: &external.FilterParam{
						FilterExpr: t.Es.Filter.FilterExpr,
					},
					Fileparam: &external.ExFileparam{
						End:       t.Es.Fileparam.End,
						FileCnt:   t.Es.Fileparam.FileCnt,
						FileFin:   t.Es.Fileparam.FileFin,
						FileIndex: t.Es.Fileparam.FileIndex,
					},
				},
			},
		)
		op.ProjectList = t.ProjectList
		op.SetInfo(&info)
		return op
	case vm.Source:
		t := sourceOp.(*source.Source)
		op := source.NewArgument()
		op.TblDef = t.TblDef
		op.Limit = t.Limit
		op.Offset = t.Offset
		op.Configs = t.Configs
		op.ProjectList = t.ProjectList
		op.ProjectList = t.ProjectList
		op.SetInfo(&info)
		return op
	case vm.Connector:
		op := connector.NewArgument()
		op.Reg = sourceOp.(*connector.Connector).Reg
		op.SetInfo(&info)
		return op
	case vm.Shuffle:
		sourceArg := sourceOp.(*shuffle.Shuffle)
		op := shuffle.NewArgument()
		op.ShuffleType = sourceArg.ShuffleType
		op.ShuffleColIdx = sourceArg.ShuffleColIdx
		op.ShuffleColMax = sourceArg.ShuffleColMax
		op.ShuffleColMin = sourceArg.ShuffleColMin
		op.AliveRegCnt = sourceArg.AliveRegCnt
		op.ShuffleRangeInt64 = sourceArg.ShuffleRangeInt64
		op.ShuffleRangeUint64 = sourceArg.ShuffleRangeUint64
		op.RuntimeFilterSpec = plan2.DeepCopyRuntimeFilterSpec(sourceArg.RuntimeFilterSpec)
		op.SetInfo(&info)
		return op
	case vm.Dispatch:
		sourceArg := sourceOp.(*dispatch.Dispatch)
		op := dispatch.NewArgument()
		op.IsSink = sourceArg.IsSink
		op.RecSink = sourceArg.RecSink
		op.ShuffleType = sourceArg.ShuffleType
		op.ShuffleRegIdxLocal = sourceArg.ShuffleRegIdxLocal
		op.ShuffleRegIdxRemote = sourceArg.ShuffleRegIdxRemote
		op.FuncId = sourceArg.FuncId
		op.LocalRegs = make([]*process.WaitRegister, len(sourceArg.LocalRegs))
		op.RemoteRegs = make([]colexec.ReceiveInfo, len(sourceArg.RemoteRegs))
		for j := range op.LocalRegs {
			op.LocalRegs[j] = sourceArg.LocalRegs[j]
		}
		for j := range op.RemoteRegs {
			op.RemoteRegs[j] = sourceArg.RemoteRegs[j]
		}
		op.SetInfo(&info)
		return op
	case vm.Insert:
		t := sourceOp.(*insert.Insert)
		op := insert.NewArgument()
		op.InsertCtx = t.InsertCtx
		op.ToWriteS3 = t.ToWriteS3
		op.SetInfo(&info)
		return op
	case vm.PreInsert:
		t := sourceOp.(*preinsert.PreInsert)
		op := preinsert.NewArgument()
		op.SchemaName = t.SchemaName
		op.TableDef = t.TableDef
		op.Attrs = t.Attrs
		op.IsUpdate = t.IsUpdate
		op.HasAutoCol = t.HasAutoCol
		op.EstimatedRowCount = t.EstimatedRowCount
		op.CompPkeyExpr = t.CompPkeyExpr
		op.ClusterByExpr = t.ClusterByExpr
		op.SetInfo(&info)
		return op
	case vm.Deletion:
		t := sourceOp.(*deletion.Deletion)
		op := deletion.NewArgument()
		op.IBucket = t.IBucket
		op.Nbucket = t.Nbucket
		op.DeleteCtx = t.DeleteCtx
		op.RemoteDelete = t.RemoteDelete
		op.SegmentMap = t.SegmentMap
		op.SetInfo(&info)
		return op
	case vm.LockOp:
		t := sourceOp.(*lockop.LockOp)
		op := lockop.NewArgument()
		*op = *t
		op.SetChildren(nil) // make sure res.arg.children is nil
		op.SetInfo(&info)
		return op
	case vm.FuzzyFilter:
		t := sourceOp.(*fuzzyfilter.FuzzyFilter)
		op := fuzzyfilter.NewArgument()
		op.N = t.N
		op.PkName = t.PkName
		op.PkTyp = t.PkTyp
		op.BuildIdx = t.BuildIdx
		op.SetInfo(&info)
		return op
	case vm.TableScan:
		t := sourceOp.(*table_scan.TableScan)
		op := table_scan.NewArgument().WithTypes(t.Types)
		op.ProjectList = t.ProjectList
		op.SetInfo(&info)
		return op
	case vm.ValueScan:
		t := sourceOp.(*value_scan.ValueScan)
		op := value_scan.NewArgument()
		op.ProjectList = t.ProjectList
		op.SetInfo(&info)
		return op
	case vm.Apply:
		t := sourceOp.(*apply.Apply)
		op := apply.NewArgument()
		op.ProjectList = t.ProjectList
		op.SetInfo(&info)
		return op
	}
	panic(fmt.Sprintf("unexpected instruction type '%d' to dup", sourceOp.OpType()))
}

func constructRestrict(n *plan.Node, filterExpr *plan2.Expr) *filter.Filter {
	op := filter.NewArgument()
	op.E = filterExpr
	op.IsEnd = n.IsEnd
	return op
}

func constructDeletion(n *plan.Node, eg engine.Engine) (*deletion.Deletion, error) {
	oldCtx := n.DeleteCtx
	delCtx := &deletion.DeleteCtx{
		Ref:                   oldCtx.Ref,
		RowIdIdx:              int(oldCtx.RowIdIdx),
		CanTruncate:           oldCtx.CanTruncate,
		AddAffectedRows:       oldCtx.AddAffectedRows,
		PartitionTableIDs:     oldCtx.PartitionTableIds,
		PartitionTableNames:   oldCtx.PartitionTableNames,
		PartitionIndexInBatch: int(oldCtx.PartitionIdx),
		PrimaryKeyIdx:         int(oldCtx.PrimaryKeyIdx),
		Engine:                eg,
	}

	op := deletion.NewArgument()
	op.DeleteCtx = delCtx
	return op, nil
}

func constructOnduplicateKey(n *plan.Node, _ engine.Engine) *onduplicatekey.OnDuplicatekey {
	oldCtx := n.OnDuplicateKey
	op := onduplicatekey.NewArgument()
	op.OnDuplicateIdx = oldCtx.OnDuplicateIdx
	op.OnDuplicateExpr = oldCtx.OnDuplicateExpr
	op.Attrs = oldCtx.Attrs
	op.InsertColCount = oldCtx.InsertColCount
	op.UniqueCols = oldCtx.UniqueCols
	op.UniqueColCheckExpr = oldCtx.UniqueColCheckExpr
	op.IsIgnore = oldCtx.IsIgnore
	return op
}

func constructFuzzyFilter(n, tableScan, sinkScan *plan.Node) *fuzzyfilter.FuzzyFilter {
	pkName := n.TableDef.Pkey.PkeyColName
	var pkTyp plan.Type
	if pkName == catalog.CPrimaryKeyColName {
		pkTyp = n.TableDef.Pkey.CompPkeyCol.Typ
	} else {
		cols := n.TableDef.Cols
		for _, c := range cols {
			if c.Name == pkName {
				pkTyp = c.Typ
			}
		}
	}

	op := fuzzyfilter.NewArgument()
	op.PkName = pkName
	op.PkTyp = pkTyp
	op.IfInsertFromUnique = n.IfInsertFromUnique

	if (tableScan.Stats.Cost / sinkScan.Stats.Cost) < 0.3 {
		// build on tableScan, because the existing data is significantly less than the data to be inserted
		// this will happend
		op.BuildIdx = 0
		if op.IfInsertFromUnique {
			// probe on sinkScan with test
			op.N = tableScan.Stats.Cost
		} else {
			// probe on sinkScan with test and add
			op.N = sinkScan.Stats.Cost + tableScan.Stats.Cost
		}
	} else {
		// build on sinkScan, as tableScan can guarantee uniqueness, probe on tableScan with test
		op.BuildIdx = 1
		op.N = sinkScan.Stats.Cost
	}

	// currently can not build runtime filter on table scan and probe it on sink scan
	// so only use runtime filter when build on sink scan
	if op.BuildIdx == 1 {
		if len(n.RuntimeFilterBuildList) > 0 {
			op.RuntimeFilterSpec = n.RuntimeFilterBuildList[0]
		}
	} else {
		tableScan.RuntimeFilterProbeList = nil
		n.RuntimeFilterBuildList = nil
	}
	return op
}

func constructPreInsert(ns []*plan.Node, n *plan.Node, eg engine.Engine, proc *process.Process) (*preinsert.PreInsert, error) {
	preCtx := n.PreInsertCtx
	schemaName := preCtx.Ref.SchemaName

	//var attrs []string
	attrs := make([]string, 0)
	for _, col := range preCtx.TableDef.Cols {
		if col.Hidden && col.Name != catalog.FakePrimaryKeyColName {
			continue
		}
		attrs = append(attrs, col.GetOriginCaseName())
	}

	ctx := proc.GetTopContext()
	txnOp := proc.GetTxnOperator()
	if n.ScanSnapshot != nil && n.ScanSnapshot.TS != nil {
		if !n.ScanSnapshot.TS.Equal(timestamp.Timestamp{LogicalTime: 0, PhysicalTime: 0}) &&
			n.ScanSnapshot.TS.Less(proc.GetTxnOperator().Txn().SnapshotTS) {
			if proc.GetCloneTxnOperator() != nil {
				txnOp = proc.GetCloneTxnOperator()
			} else {
				txnOp = proc.GetTxnOperator().CloneSnapshotOp(*n.ScanSnapshot.TS)
				proc.SetCloneTxnOperator(txnOp)
			}

			if n.ScanSnapshot.Tenant != nil {
				ctx = context.WithValue(ctx, defines.TenantIDKey{}, n.ScanSnapshot.Tenant.TenantID)
			}
		}
	}

	if preCtx.Ref.SchemaName != "" {
		dbSource, err := eg.Database(ctx, preCtx.Ref.SchemaName, txnOp)
		if err != nil {
			return nil, err
		}
		if _, err = dbSource.Relation(ctx, preCtx.Ref.ObjName, proc); err != nil {
			schemaName = defines.TEMPORARY_DBNAME
		}
	}

	op := preinsert.NewArgument()
	op.HasAutoCol = preCtx.HasAutoCol
	op.SchemaName = schemaName
	op.TableDef = preCtx.TableDef
	op.Attrs = attrs
	op.IsUpdate = preCtx.IsUpdate
	op.EstimatedRowCount = int64(ns[n.Children[0]].Stats.Outcnt)
	op.CompPkeyExpr = preCtx.CompPkeyExpr
	op.ClusterByExpr = preCtx.ClusterByExpr

	return op, nil
}

func constructPreInsertUk(n *plan.Node) *preinsertunique.PreInsertUnique {
	preCtx := n.PreInsertUkCtx
	op := preinsertunique.NewArgument()
	op.PreInsertCtx = preCtx
	return op
}

func constructPreInsertSk(n *plan.Node) *preinsertsecondaryindex.PreInsertSecIdx {
	op := preinsertsecondaryindex.NewArgument()
	op.PreInsertCtx = n.PreInsertSkCtx
	return op
}

func constructMergeblock(eg engine.Engine, n *plan.Node) *mergeblock.MergeBlock {
	return mergeblock.NewArgument().
		WithEngine(eg).
		WithObjectRef(n.InsertCtx.Ref).
		WithParitionNames(n.InsertCtx.PartitionTableNames).
		WithAddAffectedRows(n.InsertCtx.AddAffectedRows)
}

func constructLockOp(n *plan.Node, eng engine.Engine) (*lockop.LockOp, error) {
	arg := lockop.NewArgumentByEngine(eng)
	for _, target := range n.LockTargets {
		typ := plan2.MakeTypeByPlan2Type(target.PrimaryColTyp)
		if target.IsPartitionTable {
			arg.AddLockTargetWithPartition(target.GetPartitionTableIds(), target.GetPrimaryColIdxInBat(), typ, target.GetRefreshTsIdxInBat(), target.GetFilterColIdxInBat())
		} else {
			arg.AddLockTarget(target.GetTableId(), target.GetPrimaryColIdxInBat(), typ, target.GetRefreshTsIdxInBat())
		}

	}
	for _, target := range n.LockTargets {
		if target.LockTable {
			if target.IsPartitionTable {
				for _, pTblId := range target.PartitionTableIds {
					arg.LockTable(pTblId, false)
				}
			} else {
				arg.LockTable(target.TableId, false)
			}
		}
	}
	return arg, nil
}

func constructInsert(n *plan.Node, eg engine.Engine) *insert.Insert {
	oldCtx := n.InsertCtx
	var attrs []string
	for _, col := range oldCtx.TableDef.Cols {
		if col.Name != catalog.Row_ID {
			attrs = append(attrs, col.GetOriginCaseName())
		}
	}
	newCtx := &insert.InsertCtx{
		Ref:                   oldCtx.Ref,
		AddAffectedRows:       oldCtx.AddAffectedRows,
		Engine:                eg,
		Attrs:                 attrs,
		PartitionTableIDs:     oldCtx.PartitionTableIds,
		PartitionTableNames:   oldCtx.PartitionTableNames,
		PartitionIndexInBatch: int(oldCtx.PartitionIdx),
		TableDef:              oldCtx.TableDef,
	}
	arg := insert.NewArgument()
	arg.InsertCtx = newCtx
	return arg
}

func constructProjection(n *plan.Node) *projection.Projection {
	arg := projection.NewArgument()
	arg.ProjectList = n.ProjectList
	return arg
}

func constructExternal(n *plan.Node, param *tree.ExternParam, ctx context.Context, fileList []string, FileSize []int64, fileOffset []*pipeline.FileOffset, strictSqlMode bool) *external.External {
	var attrs []string

	for _, col := range n.TableDef.Cols {
		if !col.Hidden {
			attrs = append(attrs, col.GetOriginCaseName())
		}
	}

	var tbColToDataCol map[string]int32
	if n.ExternScan != nil {
		tbColToDataCol = n.ExternScan.TbColToDataCol
	}

	return external.NewArgument().WithEs(
		&external.ExternalParam{
			ExParamConst: external.ExParamConst{
				Attrs:           attrs,
				Cols:            n.TableDef.Cols,
				Extern:          param,
				Name2ColIndex:   n.TableDef.Name2ColIndex,
				TbColToDataCol:  tbColToDataCol,
				FileOffsetTotal: fileOffset,
				CreateSql:       n.TableDef.Createsql,
				Ctx:             ctx,
				FileList:        fileList,
				FileSize:        FileSize,
				ClusterTable:    n.GetClusterTable(),
				StrictSqlMode:   strictSqlMode,
			},
			ExParam: external.ExParam{
				Fileparam: new(external.ExFileparam),
				Filter: &external.FilterParam{
					FilterExpr: colexec.RewriteFilterExprList(n.FilterList),
				},
			},
		},
	)
}

func constructStream(n *plan.Node, p [2]int64) *source.Source {
	arg := source.NewArgument()
	arg.TblDef = n.TableDef
	arg.Offset = p[0]
	arg.Limit = p[1]
	return arg
}

func constructTableFunction(n *plan.Node) *table_function.TableFunction {
	attrs := make([]string, len(n.TableDef.Cols))
	for j, col := range n.TableDef.Cols {
		attrs[j] = col.GetOriginCaseName()
	}
	arg := table_function.NewArgument()
	arg.Attrs = attrs
	arg.Rets = n.TableDef.Cols
	arg.Args = n.TblFuncExprList
	arg.FuncName = n.TableDef.TblFunc.Name
	arg.Params = n.TableDef.TblFunc.Param
	return arg
}

func constructTop(n *plan.Node, topN *plan.Expr) *top.Top {
	arg := top.NewArgument()
	arg.Fs = n.OrderBy
	arg.Limit = topN
	if len(n.SendMsgList) > 0 {
		arg.TopValueTag = n.SendMsgList[0].MsgTag
	}
	return arg
}

func constructJoin(n *plan.Node, typs []types.Type, proc *process.Process) *join.InnerJoin {
	result := make([]colexec.ResultPos, len(n.ProjectList))
	for i, expr := range n.ProjectList {
		result[i].Rel, result[i].Pos = constructJoinResult(expr, proc)
	}
	cond, conds := extraJoinConditions(n.OnList)

	arg := join.NewArgument()
	arg.Result = result
	arg.Cond = cond
	arg.Conditions = constructJoinConditions(conds, proc)
	arg.RuntimeFilterSpecs = n.RuntimeFilterBuildList
	arg.HashOnPK = n.Stats.HashmapStats != nil && n.Stats.HashmapStats.HashOnPK
	arg.IsShuffle = n.Stats.HashmapStats != nil && n.Stats.HashmapStats.Shuffle
	for i := range n.SendMsgList {
		if n.SendMsgList[i].MsgType == int32(message.MsgJoinMap) {
			arg.JoinMapTag = n.SendMsgList[i].MsgTag
		}
	}
	if arg.JoinMapTag <= 0 {
		panic("wrong joinmap tag!")
	}
	return arg
}

func constructSemi(n *plan.Node, typs []types.Type, proc *process.Process) *semi.SemiJoin {
	result := make([]int32, len(n.ProjectList))
	for i, expr := range n.ProjectList {
		rel, pos := constructJoinResult(expr, proc)
		if rel != 0 {
			panic(moerr.NewNYIf(proc.GetTopContext(), "semi result '%s'", expr))
		}
		result[i] = pos
	}
	cond, conds := extraJoinConditions(n.OnList)
	arg := semi.NewArgument()
	arg.Result = result
	arg.Cond = cond
	arg.Conditions = constructJoinConditions(conds, proc)
	arg.RuntimeFilterSpecs = n.RuntimeFilterBuildList
	arg.HashOnPK = n.Stats.HashmapStats != nil && n.Stats.HashmapStats.HashOnPK
	arg.IsShuffle = n.Stats.HashmapStats != nil && n.Stats.HashmapStats.Shuffle
	for i := range n.SendMsgList {
		if n.SendMsgList[i].MsgType == int32(message.MsgJoinMap) {
			arg.JoinMapTag = n.SendMsgList[i].MsgTag
		}
	}
	if arg.JoinMapTag <= 0 {
		panic("wrong joinmap tag!")
	}
	return arg
}

func constructLeft(n *plan.Node, typs []types.Type, proc *process.Process) *left.LeftJoin {
	result := make([]colexec.ResultPos, len(n.ProjectList))
	for i, expr := range n.ProjectList {
		result[i].Rel, result[i].Pos = constructJoinResult(expr, proc)
	}
	cond, conds := extraJoinConditions(n.OnList)
	arg := left.NewArgument()
	arg.Typs = typs
	arg.Result = result
	arg.Cond = cond
	arg.Conditions = constructJoinConditions(conds, proc)
	arg.RuntimeFilterSpecs = n.RuntimeFilterBuildList
	arg.HashOnPK = n.Stats.HashmapStats != nil && n.Stats.HashmapStats.HashOnPK
	arg.IsShuffle = n.Stats.HashmapStats != nil && n.Stats.HashmapStats.Shuffle
	for i := range n.SendMsgList {
		if n.SendMsgList[i].MsgType == int32(message.MsgJoinMap) {
			arg.JoinMapTag = n.SendMsgList[i].MsgTag
		}
	}
	if arg.JoinMapTag <= 0 {
		panic("wrong joinmap tag!")
	}
	return arg
}

func constructRight(n *plan.Node, left_typs, right_typs []types.Type, proc *process.Process) *right.RightJoin {
	result := make([]colexec.ResultPos, len(n.ProjectList))
	for i, expr := range n.ProjectList {
		result[i].Rel, result[i].Pos = constructJoinResult(expr, proc)
	}
	cond, conds := extraJoinConditions(n.OnList)
	arg := right.NewArgument()
	arg.LeftTypes = left_typs
	arg.RightTypes = right_typs
	arg.Result = result
	arg.Cond = cond
	arg.Conditions = constructJoinConditions(conds, proc)
	arg.RuntimeFilterSpecs = n.RuntimeFilterBuildList
	arg.HashOnPK = n.Stats.HashmapStats != nil && n.Stats.HashmapStats.HashOnPK
	arg.IsShuffle = n.Stats.HashmapStats != nil && n.Stats.HashmapStats.Shuffle
	for i := range n.SendMsgList {
		if n.SendMsgList[i].MsgType == int32(message.MsgJoinMap) {
			arg.JoinMapTag = n.SendMsgList[i].MsgTag
		}
	}
	if arg.JoinMapTag <= 0 {
		panic("wrong joinmap tag!")
	}
	return arg
}

func constructRightSemi(n *plan.Node, right_typs []types.Type, proc *process.Process) *rightsemi.RightSemi {
	result := make([]int32, len(n.ProjectList))
	for i, expr := range n.ProjectList {
		_, result[i] = constructJoinResult(expr, proc)
	}
	cond, conds := extraJoinConditions(n.OnList)
	// 使用NewArgument来初始化
	arg := rightsemi.NewArgument()
	arg.RightTypes = right_typs
	arg.Result = result
	arg.Cond = cond
	arg.Conditions = constructJoinConditions(conds, proc)
	arg.RuntimeFilterSpecs = n.RuntimeFilterBuildList
	arg.HashOnPK = n.Stats.HashmapStats != nil && n.Stats.HashmapStats.HashOnPK
	arg.IsShuffle = n.Stats.HashmapStats != nil && n.Stats.HashmapStats.Shuffle
	for i := range n.SendMsgList {
		if n.SendMsgList[i].MsgType == int32(message.MsgJoinMap) {
			arg.JoinMapTag = n.SendMsgList[i].MsgTag
		}
	}
	if arg.JoinMapTag <= 0 {
		panic("wrong joinmap tag!")
	}
	return arg
}

func constructRightAnti(n *plan.Node, right_typs []types.Type, proc *process.Process) *rightanti.RightAnti {
	result := make([]int32, len(n.ProjectList))
	for i, expr := range n.ProjectList {
		_, result[i] = constructJoinResult(expr, proc)
	}
	cond, conds := extraJoinConditions(n.OnList)
	arg := rightanti.NewArgument()
	arg.RightTypes = right_typs
	arg.Result = result
	arg.Cond = cond
	arg.Conditions = constructJoinConditions(conds, proc)
	arg.RuntimeFilterSpecs = n.RuntimeFilterBuildList
	arg.HashOnPK = n.Stats.HashmapStats != nil && n.Stats.HashmapStats.HashOnPK
	arg.IsShuffle = n.Stats.HashmapStats != nil && n.Stats.HashmapStats.Shuffle
	for i := range n.SendMsgList {
		if n.SendMsgList[i].MsgType == int32(message.MsgJoinMap) {
			arg.JoinMapTag = n.SendMsgList[i].MsgTag
		}
	}
	if arg.JoinMapTag <= 0 {
		panic("wrong joinmap tag!")
	}
	return arg
}

func constructSingle(n *plan.Node, typs []types.Type, proc *process.Process) *single.SingleJoin {
	result := make([]colexec.ResultPos, len(n.ProjectList))
	for i, expr := range n.ProjectList {
		result[i].Rel, result[i].Pos = constructJoinResult(expr, proc)
	}
	cond, conds := extraJoinConditions(n.OnList)
	arg := single.NewArgument()
	arg.Typs = typs
	arg.Result = result
	arg.Cond = cond
	arg.Conditions = constructJoinConditions(conds, proc)
	arg.RuntimeFilterSpecs = n.RuntimeFilterBuildList
	arg.HashOnPK = n.Stats.HashmapStats != nil && n.Stats.HashmapStats.HashOnPK
	for i := range n.SendMsgList {
		if n.SendMsgList[i].MsgType == int32(message.MsgJoinMap) {
			arg.JoinMapTag = n.SendMsgList[i].MsgTag
		}
	}
	if arg.JoinMapTag <= 0 {
		panic("wrong joinmap tag!")
	}
	return arg
}

func constructProduct(n *plan.Node, typs []types.Type, proc *process.Process) *product.Product {
	result := make([]colexec.ResultPos, len(n.ProjectList))
	for i, expr := range n.ProjectList {
		result[i].Rel, result[i].Pos = constructJoinResult(expr, proc)
	}
	arg := product.NewArgument()
	arg.Result = result
	for i := range n.SendMsgList {
		if n.SendMsgList[i].MsgType == int32(message.MsgJoinMap) {
			arg.JoinMapTag = n.SendMsgList[i].MsgTag
		}
	}
	if arg.JoinMapTag <= 0 {
		panic("wrong joinmap tag!")
	}
	return arg
}

func constructAnti(n *plan.Node, typs []types.Type, proc *process.Process) *anti.AntiJoin {
	result := make([]int32, len(n.ProjectList))
	for i, expr := range n.ProjectList {
		rel, pos := constructJoinResult(expr, proc)
		if rel != 0 {
			panic(moerr.NewNYIf(proc.GetTopContext(), "anti result '%s'", expr))
		}
		result[i] = pos
	}
	cond, conds := extraJoinConditions(n.OnList)
	arg := anti.NewArgument()
	arg.Result = result
	arg.Cond = cond
	arg.Conditions = constructJoinConditions(conds, proc)
	arg.HashOnPK = n.Stats.HashmapStats != nil && n.Stats.HashmapStats.HashOnPK
	arg.IsShuffle = n.Stats.HashmapStats != nil && n.Stats.HashmapStats.Shuffle
	arg.RuntimeFilterSpecs = n.RuntimeFilterBuildList
	for i := range n.SendMsgList {
		if n.SendMsgList[i].MsgType == int32(message.MsgJoinMap) {
			arg.JoinMapTag = n.SendMsgList[i].MsgTag
		}
	}
	if arg.JoinMapTag <= 0 {
		panic("wrong joinmap tag!")
	}
	return arg
}

/*
func constructMark(n *plan.Node, typs []types.Type, proc *process.Process) *mark.Argument {
	result := make([]int32, len(n.ProjectList))
	for i, expr := range n.ProjectList {
		rel, pos := constructJoinResult(expr, proc)
		if rel == 0 {
			result[i] = pos
		} else if rel == -1 {
			result[i] = -1
		} else {
			panic(moerr.NewNYI(proc.GetTopContext(), "loop mark result '%s'", expr))
		}
	}
	cond, conds := extraJoinConditions(n.OnList)
	return &mark.Argument{
		Typs:       typs,
		Result:     result,
		Cond:       cond,
		Conditions: constructJoinConditions(conds, proc),
		OnList:     n.OnList,
	}
}
*/

func constructOrder(n *plan.Node) *order.Order {
	arg := order.NewArgument()
	arg.OrderBySpec = n.OrderBy
	return arg
}

func constructUnionAll(_ *plan.Node) *unionall.UnionAll {
	arg := unionall.NewArgument()
	return arg
}

func constructFill(n *plan.Node) *fill.Fill {
	aggIdx := make([]int32, len(n.AggList))
	for i, expr := range n.AggList {
		f := expr.Expr.(*plan.Expr_F)
		obj := int64(uint64(f.F.Func.Obj) & function.DistinctMask)
		aggIdx[i], _ = function.DecodeOverloadID(obj)
	}
	arg := fill.NewArgument()
	arg.ColLen = len(n.AggList)
	arg.FillType = n.FillType
	arg.FillVal = n.FillVal
	arg.AggIds = aggIdx
	return arg
}

func constructTimeWindow(_ context.Context, n *plan.Node) *timewin.TimeWin {
	var aggregationExpressions []aggexec.AggFuncExecExpression = nil
	var typs []types.Type
	var wStart, wEnd bool
	i := 0
	for _, expr := range n.AggList {
		if e, ok := expr.Expr.(*plan.Expr_Col); ok {
			if e.Col.Name == plan2.TimeWindowStart {
				wStart = true
			}
			if e.Col.Name == plan2.TimeWindowEnd {
				wEnd = true
			}
			continue
		}
		f := expr.Expr.(*plan.Expr_F)
		isDistinct := (uint64(f.F.Func.Obj) & function.Distinct) != 0
		functionID := int64(uint64(f.F.Func.Obj) & function.DistinctMask)
		e := f.F.Args[0]
		if e != nil {
			aggregationExpressions = append(
				aggregationExpressions,
				aggexec.MakeAggFunctionExpression(functionID, isDistinct, f.F.Args, nil))

			typs = append(typs, types.New(types.T(e.Typ.Id), e.Typ.Width, e.Typ.Scale))
		}
		i++
	}

	var err error
	str := n.Interval.Expr.(*plan.Expr_List).List.List[1].Expr.(*plan.Expr_Lit).Lit.Value.(*plan.Literal_Sval).Sval
	itr := &timewin.Interval{}
	itr.Typ, err = types.IntervalTypeOf(str)
	if err != nil {
		panic(err)
	}
	itr.Val = n.Interval.Expr.(*plan.Expr_List).List.List[0].Expr.(*plan.Expr_Lit).Lit.Value.(*plan.Literal_I64Val).I64Val

	var sld *timewin.Interval
	if n.Sliding != nil {
		sld = &timewin.Interval{}
		str = n.Sliding.Expr.(*plan.Expr_List).List.List[1].Expr.(*plan.Expr_Lit).Lit.Value.(*plan.Literal_Sval).Sval
		sld.Typ, err = types.IntervalTypeOf(str)
		if err != nil {
			panic(err)
		}
		sld.Val = n.Sliding.Expr.(*plan.Expr_List).List.List[0].Expr.(*plan.Expr_Lit).Lit.Value.(*plan.Literal_I64Val).I64Val
	}

	arg := timewin.NewArgument()
	arg.Types = typs
	arg.Aggs = aggregationExpressions
	arg.Ts = n.OrderBy[0].Expr
	arg.WStart = wStart
	arg.WEnd = wEnd
	arg.Interval = itr
	arg.Sliding = sld
	return arg
}

func constructWindow(_ context.Context, n *plan.Node, proc *process.Process) *window.Window {
	aggregationExpressions := make([]aggexec.AggFuncExecExpression, len(n.WinSpecList))
	typs := make([]types.Type, len(n.WinSpecList))

	for i, expr := range n.WinSpecList {
		f := expr.Expr.(*plan.Expr_W).W.WindowFunc.Expr.(*plan.Expr_F)
		isDistinct := (uint64(f.F.Func.Obj) & function.Distinct) != 0
		functionID := int64(uint64(f.F.Func.Obj) & function.DistinctMask)

		var e *plan.Expr = nil
		var cfg []byte = nil
		var args = f.F.Args
		if len(f.F.Args) > 0 {

			//for group_concat, the last arg is separator string
			//for cluster_centers, the last arg is kmeans_args string
			if (f.F.Func.ObjName == plan2.NameGroupConcat ||
				f.F.Func.ObjName == plan2.NameClusterCenters) && len(f.F.Args) > 1 {
				argExpr := f.F.Args[len(f.F.Args)-1]
				vec, err := colexec.EvalExpressionOnce(proc, argExpr, []*batch.Batch{constBat})
				if err != nil {
					panic(err)
				}
				cfg = []byte(vec.GetStringAt(0))
				vec.Free(proc.Mp())

				args = f.F.Args[:len(f.F.Args)-1]
			}

			e = f.F.Args[0]
		}
		aggregationExpressions[i] = aggexec.MakeAggFunctionExpression(
			functionID, isDistinct, args, cfg)

		if e != nil {
			typs[i] = types.New(types.T(e.Typ.Id), e.Typ.Width, e.Typ.Scale)
		}
	}
	arg := window.NewArgument()
	arg.Types = typs
	arg.Aggs = aggregationExpressions
	arg.WinSpecList = n.WinSpecList
	return arg
}

/*
func constructOffset(n *plan.Node, proc *process.Process) *offset.Argument {
	vec, err := colexec.EvalExpr(constBat, proc, n.Offset)
	if err != nil {
		panic(err)
	}
	return &offset.Argument{
		Offset: uint64(vec.Col.([]int64)[0]),
	}
}
*/

func constructOffset(n *plan.Node) *offset.Offset {
	arg := offset.NewArgument().WithOffset(n.Offset)
	return arg
}

func constructLimit(n *plan.Node) *limit.Limit {
	arg := limit.NewArgument().WithLimit(n.Limit)
	return arg
}

func constructSample(n *plan.Node, outputRowCount bool) *sample.Sample {
	if n.SampleFunc.Rows != plan2.NotSampleByRows {
		return sample.NewSampleByRows(int(n.SampleFunc.Rows), n.AggList, n.GroupBy, n.SampleFunc.UsingRow, outputRowCount)
	}
	if n.SampleFunc.Percent != plan2.NotSampleByPercents {
		return sample.NewSampleByPercent(n.SampleFunc.Percent, n.AggList, n.GroupBy)
	}
	panic("only support sample by rows / percent now.")
}

func constructGroup(_ context.Context, n, cn *plan.Node, needEval bool, shuffleDop int, proc *process.Process) *group.Group {
	aggregationExpressions := make([]aggexec.AggFuncExecExpression, len(n.AggList))
	for i, expr := range n.AggList {
		if f, ok := expr.Expr.(*plan.Expr_F); ok {
			isDistinct := (uint64(f.F.Func.Obj) & function.Distinct) != 0
			functionID := int64(uint64(f.F.Func.Obj) & function.DistinctMask)

			var cfg []byte = nil
			var args = f.F.Args
			if len(f.F.Args) > 0 {
				//for group_concat, the last arg is separator string
				//for cluster_centers, the last arg is kmeans_args string
				if (f.F.Func.ObjName == plan2.NameGroupConcat ||
					f.F.Func.ObjName == plan2.NameClusterCenters) && len(f.F.Args) > 1 {
					argExpr := f.F.Args[len(f.F.Args)-1]
					vec, err := colexec.EvalExpressionOnce(proc, argExpr, []*batch.Batch{constBat})
					if err != nil {
						panic(err)
					}
					cfg = []byte(vec.GetStringAt(0))
					vec.Free(proc.Mp())

					args = f.F.Args[:len(f.F.Args)-1]
				}
			}

			aggregationExpressions[i] = aggexec.MakeAggFunctionExpression(
				functionID, isDistinct, args, cfg)
		}
	}

	typs := make([]types.Type, len(cn.ProjectList))
	for i, e := range cn.ProjectList {
		typs[i] = types.New(types.T(e.Typ.Id), e.Typ.Width, e.Typ.Scale)
	}

	var preAllocSize uint64 = 0
	if n.Stats != nil && n.Stats.HashmapStats != nil && n.Stats.HashmapStats.Shuffle {
		if cn.NodeType == plan.Node_TABLE_SCAN && len(cn.FilterList) == 0 {
			// if group on scan without filter, stats for hashmap is accurate to do preAlloc
			// tune it up a little bit in case it is not so average after shuffle
			preAllocSize = uint64(n.Stats.HashmapStats.HashmapSize / float64(shuffleDop) * 1.05)
		}
	}

	arg := group.NewArgument()
	arg.Aggs = aggregationExpressions
	arg.NeedEval = needEval
	arg.Exprs = n.GroupBy
	arg.PreAllocSize = preAllocSize
	return arg
}

func constructDispatchLocal(all bool, isSink, rec bool, recCTE bool, regs []*process.WaitRegister) *dispatch.Dispatch {
	arg := dispatch.NewArgument()
	arg.LocalRegs = regs
	arg.IsSink = isSink
	arg.RecSink = rec
	arg.RecCTE = recCTE
	if all {
		arg.FuncId = dispatch.SendToAllLocalFunc
	} else {
		arg.FuncId = dispatch.SendToAnyLocalFunc
	}
	return arg
}

// This function do not setting funcId.
// PLEASE SETTING FuncId AFTER YOU CALL IT.
func constructDispatchLocalAndRemote(idx int, target []*Scope, source *Scope) (bool, *dispatch.Dispatch) {
	arg := dispatch.NewArgument()
	scopeLen := len(target)
	arg.LocalRegs = make([]*process.WaitRegister, 0, scopeLen)
	arg.RemoteRegs = make([]colexec.ReceiveInfo, 0, scopeLen)
	arg.ShuffleRegIdxLocal = make([]int, 0, len(target))
	arg.ShuffleRegIdxRemote = make([]int, 0, len(target))
	hasRemote := false

	for _, s := range target {
		if !isSameCN(s.NodeInfo.Addr, source.NodeInfo.Addr) {
			hasRemote = true
			break
		}
	}
	if hasRemote && source.NodeInfo.Mcpu > 1 {
		panic("pipeline end with dispatch should have been merged in multi CN!")
	}

	for i, s := range target {
		if isSameCN(s.NodeInfo.Addr, source.NodeInfo.Addr) {
			// Local reg.
			// Put them into arg.LocalRegs
			s.Proc.Reg.MergeReceivers[idx].NilBatchCnt = source.NodeInfo.Mcpu
			arg.LocalRegs = append(arg.LocalRegs, s.Proc.Reg.MergeReceivers[idx])
			arg.ShuffleRegIdxLocal = append(arg.ShuffleRegIdxLocal, i)
		} else {
			// Remote reg.
			// Generate uuid for them and put into arg.RemoteRegs & scope. receive info
			newUuid, _ := uuid.NewV7()

			arg.RemoteRegs = append(arg.RemoteRegs, colexec.ReceiveInfo{
				Uuid:     newUuid,
				NodeAddr: s.NodeInfo.Addr,
			})
			arg.ShuffleRegIdxRemote = append(arg.ShuffleRegIdxRemote, i)
			s.RemoteReceivRegInfos = append(s.RemoteReceivRegInfos, RemoteReceivRegInfo{
				Idx:      idx,
				Uuid:     newUuid,
				FromAddr: source.NodeInfo.Addr,
			})
		}
	}
	return hasRemote, arg
}

func constructShuffleJoinArg(ss []*Scope, node *plan.Node, left bool) *shuffle.Shuffle {
	arg := shuffle.NewArgument()
	var expr *plan.Expr
	cond := node.OnList[node.Stats.HashmapStats.ShuffleColIdx]
	switch condImpl := cond.Expr.(type) {
	case *plan.Expr_F:
		if left {
			expr = condImpl.F.Args[0]
		} else {
			expr = condImpl.F.Args[1]
		}
	}

	hashCol, typ := plan2.GetHashColumn(expr)
	arg.ShuffleColIdx = hashCol.ColPos
	arg.ShuffleType = int32(node.Stats.HashmapStats.ShuffleType)
	arg.ShuffleColMin = node.Stats.HashmapStats.ShuffleColMin
	arg.ShuffleColMax = node.Stats.HashmapStats.ShuffleColMax
	arg.AliveRegCnt = int32(len(ss))
	switch types.T(typ) {
	case types.T_int64, types.T_int32, types.T_int16:
		arg.ShuffleRangeInt64 = plan2.ShuffleRangeReEvalSigned(node.Stats.HashmapStats.Ranges, int(arg.AliveRegCnt), node.Stats.HashmapStats.Nullcnt, int64(node.Stats.TableCnt))
	case types.T_uint64, types.T_uint32, types.T_uint16, types.T_varchar, types.T_char, types.T_text, types.T_bit, types.T_datalink:
		arg.ShuffleRangeUint64 = plan2.ShuffleRangeReEvalUnsigned(node.Stats.HashmapStats.Ranges, int(arg.AliveRegCnt), node.Stats.HashmapStats.Nullcnt, int64(node.Stats.TableCnt))
	}
	if left && len(node.RuntimeFilterProbeList) > 0 {
		arg.RuntimeFilterSpec = plan2.DeepCopyRuntimeFilterSpec(node.RuntimeFilterProbeList[0])
	}
	return arg
}

func constructShuffleArgForGroup(ss []*Scope, node *plan.Node) *shuffle.Shuffle {
	arg := shuffle.NewArgument()
	hashCol, typ := plan2.GetHashColumn(node.GroupBy[node.Stats.HashmapStats.ShuffleColIdx])
	arg.ShuffleColIdx = hashCol.ColPos
	arg.ShuffleType = int32(node.Stats.HashmapStats.ShuffleType)
	arg.ShuffleColMin = node.Stats.HashmapStats.ShuffleColMin
	arg.ShuffleColMax = node.Stats.HashmapStats.ShuffleColMax
	arg.AliveRegCnt = int32(len(ss))
	switch types.T(typ) {
	case types.T_int64, types.T_int32, types.T_int16:
		arg.ShuffleRangeInt64 = plan2.ShuffleRangeReEvalSigned(node.Stats.HashmapStats.Ranges, int(arg.AliveRegCnt), node.Stats.HashmapStats.Nullcnt, int64(node.Stats.TableCnt))
	case types.T_uint64, types.T_uint32, types.T_uint16, types.T_varchar, types.T_char, types.T_text, types.T_bit, types.T_datalink:
		arg.ShuffleRangeUint64 = plan2.ShuffleRangeReEvalUnsigned(node.Stats.HashmapStats.Ranges, int(arg.AliveRegCnt), node.Stats.HashmapStats.Nullcnt, int64(node.Stats.TableCnt))
	}
	return arg
}

// cross-cn dispath  will send same batch to all register
func constructDispatch(idx int, target []*Scope, source *Scope, node *plan.Node, left bool) *dispatch.Dispatch {
	hasRemote, arg := constructDispatchLocalAndRemote(idx, target, source)
	if node.Stats.HashmapStats.Shuffle {
		arg.FuncId = dispatch.ShuffleToAllFunc
		if node.Stats.HashmapStats.ShuffleTypeForMultiCN == plan.ShuffleTypeForMultiCN_Hybrid {
			if left {
				arg.ShuffleType = plan2.ShuffleToLocalMatchedReg
			} else {
				arg.ShuffleType = plan2.ShuffleToMultiMatchedReg
			}
		} else {
			arg.ShuffleType = plan2.ShuffleToRegIndex
		}
		return arg
	}
	if hasRemote {
		arg.FuncId = dispatch.SendToAllFunc
	} else {
		arg.FuncId = dispatch.SendToAllLocalFunc
	}
	return arg
}

func constructMergeGroup(needEval bool) *mergegroup.MergeGroup {
	arg := mergegroup.NewArgument()
	arg.NeedEval = needEval
	return arg
}

func constructMergeTop(n *plan.Node, topN *plan.Expr) *mergetop.MergeTop {
	arg := mergetop.NewArgument()
	arg.Fs = n.OrderBy
	arg.Limit = topN
	return arg
}

func constructMergeOrder(n *plan.Node) *mergeorder.MergeOrder {
	arg := mergeorder.NewArgument()
	arg.OrderBySpecs = n.OrderBy
	return arg
}

func constructPartition(n *plan.Node) *partition.Partition {
	arg := partition.NewArgument()
	arg.OrderBySpecs = n.OrderBy
	return arg
}

func constructIndexJoin(n *plan.Node, proc *process.Process) *indexjoin.IndexJoin {
	result := make([]int32, len(n.ProjectList))
	for i, expr := range n.ProjectList {
		rel, pos := constructJoinResult(expr, proc)
		if rel != 0 {
			panic(moerr.NewNYIf(proc.GetTopContext(), "loop semi result '%s'", expr))
		}
		result[i] = pos
	}
	arg := indexjoin.NewArgument()
	arg.Result = result
	arg.RuntimeFilterSpecs = n.RuntimeFilterBuildList
	return arg
}

func constructProductL2(n *plan.Node, proc *process.Process) *productl2.Productl2 {
	result := make([]colexec.ResultPos, len(n.ProjectList))
	for i, expr := range n.ProjectList {
		result[i].Rel, result[i].Pos = constructJoinResult(expr, proc)
	}
	arg := productl2.NewArgument()
	arg.Result = result
	arg.OnExpr = colexec.RewriteFilterExprList(n.OnList)
	for i := range n.SendMsgList {
		if n.SendMsgList[i].MsgType == int32(message.MsgJoinMap) {
			arg.JoinMapTag = n.SendMsgList[i].MsgTag
		}
	}
	if arg.JoinMapTag <= 0 {
		panic("wrong joinmap tag!")
	}
	return arg
}

func constructLoopJoin(n *plan.Node, typs []types.Type, proc *process.Process, jointype int) *loopjoin.LoopJoin {
	result := make([]colexec.ResultPos, len(n.ProjectList))
	for i, expr := range n.ProjectList {
		result[i].Rel, result[i].Pos = constructJoinResult(expr, proc)
	}
	arg := loopjoin.NewArgument()
	arg.Result = result
	arg.Typs = typs
	arg.Cond = colexec.RewriteFilterExprList(n.OnList)
	arg.JoinType = jointype
	for i := range n.SendMsgList {
		if n.SendMsgList[i].MsgType == int32(message.MsgJoinMap) {
			arg.JoinMapTag = n.SendMsgList[i].MsgTag
		}
	}
	if arg.JoinMapTag <= 0 {
		panic("wrong joinmap tag!")
	}
	return arg
}

func constructJoinBuildOperator(c *Compile, op vm.Operator, mcpu int32) vm.Operator {
	switch op.OpType() {
	case vm.IndexJoin:
		indexJoin := op.(*indexjoin.IndexJoin)
		ret := indexbuild.NewArgument()
		if len(indexJoin.RuntimeFilterSpecs) > 0 {
			ret.RuntimeFilterSpec = indexJoin.RuntimeFilterSpecs[0]
		}
		ret.SetIdx(indexJoin.Idx)
		ret.SetIsFirst(true)
		return ret
	default:
		res := constructHashBuild(op, c.proc, mcpu)
		res.SetIdx(op.GetOperatorBase().GetIdx())
		res.SetIsFirst(true)
		return res
	}
}

func constructHashBuild(op vm.Operator, proc *process.Process, mcpu int32) *hashbuild.HashBuild {
	// XXX BUG
	// relation index of arg.Conditions should be rewritten to 0 here.
	ret := hashbuild.NewArgument()

	switch op.OpType() {
	case vm.Anti:
		arg := op.(*anti.AntiJoin)
		ret.NeedHashMap = true
		ret.Conditions = arg.Conditions[1]
		ret.HashOnPK = arg.HashOnPK
		if arg.Cond == nil {
			ret.NeedBatches = false
			ret.NeedAllocateSels = false
		} else {
			ret.NeedBatches = true
			ret.NeedAllocateSels = true
		}
		ret.JoinMapTag = arg.JoinMapTag

	case vm.Mark:
		arg := op.(*mark.MarkJoin)
		ret.NeedHashMap = true
		ret.Conditions = arg.Conditions[1]
		ret.NeedBatches = true
		ret.HashOnPK = arg.HashOnPK
		ret.NeedAllocateSels = true
		ret.JoinMapTag = arg.JoinMapTag

	case vm.Join:
		arg := op.(*join.InnerJoin)
		ret.NeedHashMap = true
		ret.Conditions = arg.Conditions[1]
		ret.HashOnPK = arg.HashOnPK

		// to find if hashmap need to keep build batches for probe
		var needMergedBatch bool
		if arg.Cond != nil {
			needMergedBatch = true
		}
		for _, rp := range arg.Result {
			if rp.Rel == 1 {
				needMergedBatch = true
				break
			}
		}
		ret.NeedBatches = needMergedBatch
		ret.NeedAllocateSels = true
		if len(arg.RuntimeFilterSpecs) > 0 {
			ret.RuntimeFilterSpec = arg.RuntimeFilterSpecs[0]
		}
		ret.JoinMapTag = arg.JoinMapTag

	case vm.Left:
		arg := op.(*left.LeftJoin)
		ret.NeedHashMap = true
		ret.Conditions = arg.Conditions[1]
		ret.NeedBatches = true
		ret.HashOnPK = arg.HashOnPK
		ret.NeedAllocateSels = true
		if len(arg.RuntimeFilterSpecs) > 0 {
			ret.RuntimeFilterSpec = arg.RuntimeFilterSpecs[0]
		}
		ret.JoinMapTag = arg.JoinMapTag

	case vm.Right:
		arg := op.(*right.RightJoin)
		ret.NeedHashMap = true
		ret.Conditions = arg.Conditions[1]
		ret.NeedBatches = true
		ret.HashOnPK = arg.HashOnPK
		ret.NeedAllocateSels = true
		if len(arg.RuntimeFilterSpecs) > 0 {
			ret.RuntimeFilterSpec = arg.RuntimeFilterSpecs[0]
		}
		ret.JoinMapTag = arg.JoinMapTag

	case vm.RightSemi:
		arg := op.(*rightsemi.RightSemi)
		ret.NeedHashMap = true
		ret.Conditions = arg.Conditions[1]
		ret.NeedBatches = true
		ret.HashOnPK = arg.HashOnPK
		ret.NeedAllocateSels = true
		if len(arg.RuntimeFilterSpecs) > 0 {
			ret.RuntimeFilterSpec = arg.RuntimeFilterSpecs[0]
		}
		ret.JoinMapTag = arg.JoinMapTag

	case vm.RightAnti:
		arg := op.(*rightanti.RightAnti)
		ret.NeedHashMap = true
		ret.Conditions = arg.Conditions[1]
		ret.NeedBatches = true
		ret.HashOnPK = arg.HashOnPK
		ret.NeedAllocateSels = true
		if len(arg.RuntimeFilterSpecs) > 0 {
			ret.RuntimeFilterSpec = arg.RuntimeFilterSpecs[0]
		}
		ret.JoinMapTag = arg.JoinMapTag

	case vm.Semi:
		arg := op.(*semi.SemiJoin)
		ret.NeedHashMap = true
		ret.Conditions = arg.Conditions[1]
		ret.HashOnPK = arg.HashOnPK
		if arg.Cond == nil {
			ret.NeedBatches = false
			ret.NeedAllocateSels = false
		} else {
			ret.NeedBatches = true
			ret.NeedAllocateSels = true
		}
		if len(arg.RuntimeFilterSpecs) > 0 {
			ret.RuntimeFilterSpec = arg.RuntimeFilterSpecs[0]
		}
		ret.JoinMapTag = arg.JoinMapTag

	case vm.Single:
		arg := op.(*single.SingleJoin)
		ret.NeedHashMap = true
		ret.Conditions = arg.Conditions[1]
		ret.NeedBatches = true
		ret.HashOnPK = arg.HashOnPK
		ret.NeedAllocateSels = true
		if len(arg.RuntimeFilterSpecs) > 0 {
			ret.RuntimeFilterSpec = arg.RuntimeFilterSpecs[0]
		}
		ret.JoinMapTag = arg.JoinMapTag
	case vm.Product:
		arg := op.(*product.Product)
		ret.NeedHashMap = false
		ret.NeedBatches = true
		ret.NeedAllocateSels = true
		ret.JoinMapTag = arg.JoinMapTag
	case vm.ProductL2:
		arg := op.(*productl2.Productl2)
		ret.NeedHashMap = false
		ret.NeedBatches = true
		ret.NeedAllocateSels = true
		ret.JoinMapTag = arg.JoinMapTag
	case vm.LoopJoin:
		arg := op.(*loopjoin.LoopJoin)
		ret.NeedHashMap = false
		ret.NeedBatches = true
		ret.NeedAllocateSels = true
		ret.JoinMapTag = arg.JoinMapTag
	default:
		ret.Release()
		panic(moerr.NewInternalErrorf(proc.Ctx, "unsupport join type '%v'", op.OpType()))
	}
	ret.JoinMapRefCnt = mcpu
	return ret
}

func constructShuffleBuild(op vm.Operator, proc *process.Process) *shufflebuild.ShuffleBuild {
	ret := shufflebuild.NewArgument()

	switch op.OpType() {
	case vm.Anti:
		arg := op.(*anti.AntiJoin)
		ret.Conditions = arg.Conditions[1]
		ret.HashOnPK = arg.HashOnPK
		if arg.Cond == nil {
			ret.NeedBatches = false
			ret.NeedAllocateSels = false
		} else {
			ret.NeedBatches = true
			ret.NeedAllocateSels = true
		}
		if len(arg.RuntimeFilterSpecs) > 0 {
			ret.RuntimeFilterSpec = plan2.DeepCopyRuntimeFilterSpec(arg.RuntimeFilterSpecs[0])
		}
		ret.JoinMapTag = arg.JoinMapTag
		ret.ShuffleIdx = arg.ShuffleIdx

	case vm.Join:
		arg := op.(*join.InnerJoin)
		ret.Conditions = arg.Conditions[1]
		ret.HashOnPK = arg.HashOnPK

		// to find if hashmap need to keep build batches for probe
		var needMergedBatch bool
		if arg.Cond != nil {
			needMergedBatch = true
		}
		for _, rp := range arg.Result {
			if rp.Rel == 1 {
				needMergedBatch = true
				break
			}
		}
		ret.NeedBatches = needMergedBatch
		ret.NeedAllocateSels = true
		if len(arg.RuntimeFilterSpecs) > 0 {
			ret.RuntimeFilterSpec = plan2.DeepCopyRuntimeFilterSpec(arg.RuntimeFilterSpecs[0])
		}
		ret.JoinMapTag = arg.JoinMapTag
		ret.ShuffleIdx = arg.ShuffleIdx

	case vm.Left:
		arg := op.(*left.LeftJoin)
		ret.Conditions = arg.Conditions[1]
		ret.NeedBatches = true
		ret.HashOnPK = arg.HashOnPK
		ret.NeedAllocateSels = true
		if len(arg.RuntimeFilterSpecs) > 0 {
			ret.RuntimeFilterSpec = plan2.DeepCopyRuntimeFilterSpec(arg.RuntimeFilterSpecs[0])
		}
		ret.JoinMapTag = arg.JoinMapTag
		ret.ShuffleIdx = arg.ShuffleIdx

	case vm.Right:
		arg := op.(*right.RightJoin)
		ret.Conditions = arg.Conditions[1]
		ret.NeedBatches = true
		ret.HashOnPK = arg.HashOnPK
		ret.NeedAllocateSels = true
		if len(arg.RuntimeFilterSpecs) > 0 {
			ret.RuntimeFilterSpec = plan2.DeepCopyRuntimeFilterSpec(arg.RuntimeFilterSpecs[0])
		}
		ret.JoinMapTag = arg.JoinMapTag
		ret.ShuffleIdx = arg.ShuffleIdx

	case vm.RightSemi:
		arg := op.(*rightsemi.RightSemi)
		ret.Conditions = arg.Conditions[1]
		ret.NeedBatches = true
		ret.HashOnPK = arg.HashOnPK
		ret.NeedAllocateSels = true
		if len(arg.RuntimeFilterSpecs) > 0 {
			ret.RuntimeFilterSpec = plan2.DeepCopyRuntimeFilterSpec(arg.RuntimeFilterSpecs[0])
		}
		ret.JoinMapTag = arg.JoinMapTag
		ret.ShuffleIdx = arg.ShuffleIdx

	case vm.RightAnti:
		arg := op.(*rightanti.RightAnti)
		ret.Conditions = arg.Conditions[1]
		ret.NeedBatches = true
		ret.HashOnPK = arg.HashOnPK
		ret.NeedAllocateSels = true
		if len(arg.RuntimeFilterSpecs) > 0 {
			ret.RuntimeFilterSpec = plan2.DeepCopyRuntimeFilterSpec(arg.RuntimeFilterSpecs[0])
		}
		ret.JoinMapTag = arg.JoinMapTag
		ret.ShuffleIdx = arg.ShuffleIdx

	case vm.Semi:
		arg := op.(*semi.SemiJoin)
		ret.Conditions = arg.Conditions[1]
		ret.HashOnPK = arg.HashOnPK
		if arg.Cond == nil {
			ret.NeedBatches = false
			ret.NeedAllocateSels = false
		} else {
			ret.NeedBatches = true
			ret.NeedAllocateSels = true
		}
		if len(arg.RuntimeFilterSpecs) > 0 {
			ret.RuntimeFilterSpec = plan2.DeepCopyRuntimeFilterSpec(arg.RuntimeFilterSpecs[0])
		}
		ret.JoinMapTag = arg.JoinMapTag
		ret.ShuffleIdx = arg.ShuffleIdx

	default:
		ret.Release()
		panic(moerr.NewInternalErrorf(proc.Ctx, "unsupported type for shuffle join: '%v'", op.OpType()))
	}
	return ret
}

func constructJoinResult(expr *plan.Expr, proc *process.Process) (int32, int32) {
	e, ok := expr.Expr.(*plan.Expr_Col)
	if !ok {
		panic(moerr.NewNYIf(proc.GetTopContext(), "join result '%s'", expr))
	}
	return e.Col.RelPos, e.Col.ColPos
}

func constructJoinConditions(exprs []*plan.Expr, proc *process.Process) [][]*plan.Expr {
	conds := make([][]*plan.Expr, 2)
	conds[0] = make([]*plan.Expr, len(exprs))
	conds[1] = make([]*plan.Expr, len(exprs))
	for i, expr := range exprs {
		conds[0][i], conds[1][i] = constructJoinCondition(expr, proc)
	}
	return conds
}

func constructJoinCondition(expr *plan.Expr, proc *process.Process) (*plan.Expr, *plan.Expr) {
	if e, ok := expr.Expr.(*plan.Expr_Lit); ok { // constant bool
		b, ok := e.Lit.Value.(*plan.Literal_Bval)
		if !ok {
			panic(moerr.NewNYIf(proc.GetTopContext(), "join condition '%s'", expr))
		}
		if b.Bval {
			return expr, expr
		}
		return expr, &plan.Expr{
			Typ: expr.Typ,
			Expr: &plan.Expr_Lit{
				Lit: &plan.Literal{
					Value: &plan.Literal_Bval{Bval: true},
				},
			},
		}
	}
	e, ok := expr.Expr.(*plan.Expr_F)
	if !ok || !plan2.IsEqualFunc(e.F.Func.GetObj()) {
		panic(moerr.NewNYIf(proc.GetTopContext(), "join condition '%s'", expr))
	}
	if exprRelPos(e.F.Args[0]) == 1 {
		return e.F.Args[1], e.F.Args[0]
	}
	return e.F.Args[0], e.F.Args[1]
}

/*
	func constructApply(n, right *plan.Node, applyType int, proc *process.Process) *apply.Apply {
		result := make([]colexec.ResultPos, len(n.ProjectList))
		for i, expr := range n.ProjectList {
			result[i].Rel, result[i].Pos = constructJoinResult(expr, proc)
		}
		arg := apply.NewArgument()
		arg.ApplyType = applyType
		arg.Result = result
		arg.Args = plan2.DeepCopyExprList(right.TblFuncExprList)
		arg.FuncName = right.TableDef.TblFunc.Name
		return arg
	}
*/
func constructTableScan(n *plan.Node) *table_scan.TableScan {
	types := make([]plan.Type, len(n.TableDef.Cols))
	for j, col := range n.TableDef.Cols {
		types[j] = col.Typ
	}
	return table_scan.NewArgument().WithTypes(types)
}

func constructValueScan() *value_scan.ValueScan {
	return value_scan.NewArgument()
}

func extraJoinConditions(exprs []*plan.Expr) (*plan.Expr, []*plan.Expr) {
	exprs = colexec.SplitAndExprs(exprs)
	eqConds := make([]*plan.Expr, 0, len(exprs))
	notEqConds := make([]*plan.Expr, 0, len(exprs))
	for i, expr := range exprs {
		if e, ok := expr.Expr.(*plan.Expr_F); ok {
			if !plan2.IsEqualFunc(e.F.Func.GetObj()) {
				notEqConds = append(notEqConds, exprs[i])
				continue
			}
			lpos, rpos := plan2.HasColExpr(e.F.Args[0], -1), plan2.HasColExpr(e.F.Args[1], -1)
			if lpos == -1 || rpos == -1 || (lpos == rpos) {
				notEqConds = append(notEqConds, exprs[i])
				continue
			}
			eqConds = append(eqConds, exprs[i])
		} else {
			notEqConds = append(notEqConds, exprs[i])
		}
	}
	if len(notEqConds) == 0 {
		return nil, eqConds
	}
	return colexec.RewriteFilterExprList(notEqConds), eqConds
}

func exprRelPos(expr *plan.Expr) int32 {
	switch e := expr.Expr.(type) {
	case *plan.Expr_Col:
		return e.Col.RelPos
	case *plan.Expr_F:
		for i := range e.F.Args {
			if relPos := exprRelPos(e.F.Args[i]); relPos >= 0 {
				return relPos
			}
		}
	}
	return -1
}

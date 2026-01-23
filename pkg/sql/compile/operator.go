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
	"math"

	"github.com/google/uuid"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/bitmap"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/pipeline"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/aggexec"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/apply"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/connector"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/dedupjoin"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/deletion"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/dispatch"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/external"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/fill"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/filter"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/fuzzyfilter"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/group"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/hashbuild"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/hashjoin"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/indexbuild"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/indexjoin"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/insert"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/intersect"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/intersectall"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/limit"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/lockop"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/loopjoin"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/merge"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/mergeblock"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/mergecte"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/mergeorder"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/mergerecursive"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/mergetop"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/minus"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/multi_update"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/offset"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/onduplicatekey"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/order"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/partition"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/postdml"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/preinsert"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/preinsertsecondaryindex"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/preinsertunique"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/product"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/productl2"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/projection"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/rightdedupjoin"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/sample"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/shuffle"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/shuffleV2"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/shufflebuild"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/source"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/table_clone"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/table_function"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/table_scan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/timewin"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/top"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/unionall"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/value_scan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/window"
	"github.com/matrixorigin/matrixone/pkg/sql/features"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/rule"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/message"
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
		MaxParallel: int32(maxParallel),
		ParallelID:  int32(index),
	}
	switch sourceOp.OpType() {
	case vm.ShuffleBuild:
		t := sourceOp.(*shufflebuild.ShuffleBuild)
		op := shufflebuild.NewArgument()
		op.HashOnPK = t.HashOnPK
		op.NeedBatches = t.NeedBatches
		op.NeedAllocateSels = t.NeedAllocateSels
		op.Conditions = t.Conditions
		op.RuntimeFilterSpec = t.RuntimeFilterSpec
		op.JoinMapTag = t.JoinMapTag
		if t.ShuffleIdx == -1 { // shuffleV2
			op.ShuffleIdx = int32(index)
		}
		op.IsDedup = t.IsDedup
		op.OnDuplicateAction = t.OnDuplicateAction
		op.DedupColTypes = t.DedupColTypes
		op.DedupColName = t.DedupColName
		return op

	case vm.Group:
		t := sourceOp.(*group.Group)
		op := group.NewArgument()
		op.NeedEval = t.NeedEval
		op.SpillMem = t.SpillMem
		op.GroupingFlag = t.GroupingFlag
		op.GroupBy = t.GroupBy
		op.Aggs = t.Aggs
		op.ProjectList = t.ProjectList
		op.SetInfo(&info)
		return op
	case vm.Sample:
		t := sourceOp.(*sample.Sample)
		op := t.SampleDup()
		op.SetInfo(&info)
		return op
	case vm.HashJoin:
		t := sourceOp.(*hashjoin.HashJoin)
		op := hashjoin.NewArgument()
		op.JoinType = t.JoinType
		op.IsRightJoin = t.IsRightJoin
		op.NonEqCond = t.NonEqCond
		op.ResultCols = t.ResultCols
		op.LeftTypes = t.LeftTypes
		op.RightTypes = t.RightTypes
		op.EqConds = t.EqConds
		op.RuntimeFilterSpecs = t.RuntimeFilterSpecs
		op.JoinMapTag = t.JoinMapTag
		op.HashOnPK = t.HashOnPK
		op.CanSkipProbe = t.CanSkipProbe
		op.IsShuffle = t.IsShuffle
		if !t.IsShuffle {
			if t.Channel == nil {
				t.Channel = make(chan *bitmap.Bitmap, maxParallel)
			}
			op.Channel = t.Channel
			op.NumCPU = uint64(maxParallel)
			op.IsMerger = (index == 0)
		}
		if t.ShuffleIdx == -1 { // shuffleV2
			op.ShuffleIdx = int32(index)
		}
		op.SetInfo(&info)
		return op

	case vm.LoopJoin:
		t := sourceOp.(*loopjoin.LoopJoin)
		op := loopjoin.NewArgument()
		op.ResultCols = t.ResultCols
		op.RightTypes = t.RightTypes
		op.NonEqCond = t.NonEqCond
		op.JoinMapTag = t.JoinMapTag
		op.JoinType = t.JoinType
		op.SetInfo(&info)
		return op

	case vm.IndexJoin:
		t := sourceOp.(*indexjoin.IndexJoin)
		op := indexjoin.NewArgument()
		op.ResultCols = t.ResultCols
		op.RuntimeFilterSpecs = t.RuntimeFilterSpecs
		op.SetInfo(&info)
		return op

	case vm.Limit:
		t := sourceOp.(*limit.Limit)
		op := limit.NewArgument()
		op.LimitExpr = t.LimitExpr
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
		op.SetInfo(&info)
		return op
	case vm.ProductL2:
		t := sourceOp.(*productl2.Productl2)
		op := productl2.NewArgument()
		op.Result = t.Result
		op.OnExpr = t.OnExpr
		op.JoinMapTag = t.JoinMapTag
		op.VectorOpType = t.VectorOpType
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
		op.FilterExprs = t.FilterExprs
		op.RuntimeFilterExprs = t.RuntimeFilterExprs
		op.SetInfo(&info)
		return op
	case vm.Top:
		t := sourceOp.(*top.Top)
		op := top.NewArgument()
		op.Limit = t.Limit
		if t.TopValueTag > 0 {
			op.TopValueTag = t.TopValueTag + int32(index)<<16
		}
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
	case vm.TableFunction:
		t := sourceOp.(*table_function.TableFunction)
		op := table_function.NewArgument()
		op.FuncName = t.FuncName
		op.Args = t.Args
		op.OffsetTotal = t.OffsetTotal
		op.Rets = t.Rets
		op.CanOpt = t.CanOpt
		op.Attrs = t.Attrs
		op.Params = t.Params
		op.IsSingle = t.IsSingle
		op.SetInfo(&info)
		if op.FuncName == "generate_series" {
			op.GenerateSeriesCtrNumState(t.OffsetTotal[index][0], t.OffsetTotal[index][1], t.GetGenerateSeriesCtrNumStateStep(), t.OffsetTotal[index][0])
		}
		return op
	case vm.External:
		t := sourceOp.(*external.External)
		op := external.NewArgument().WithEs(
			&external.ExternalParam{
				ExParamConst: external.ExParamConst{
					Attrs:           t.Es.Attrs,
					Cols:            t.Es.Cols,
					ColumnListLen:   t.Es.ColumnListLen,
					Idx:             index,
					CreateSql:       t.Es.CreateSql,
					FileList:        t.Es.FileList,
					FileSize:        t.Es.FileSize,
					FileOffsetTotal: t.Es.FileOffsetTotal,
					Extern:          t.Es.Extern,
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
	case vm.ShuffleV2:
		sourceArg := sourceOp.(*shuffleV2.ShuffleV2)
		if sourceArg.GetShufflePool() == nil {
			sourceArg.SetShufflePool(shuffleV2.NewShufflePool(sourceArg.BucketNum, int32(maxParallel)))
		}
		op := shuffleV2.NewArgument()
		op.SetShufflePool(sourceArg.GetShufflePool())
		op.ShuffleType = sourceArg.ShuffleType
		op.ShuffleColIdx = sourceArg.ShuffleColIdx
		op.ShuffleColMax = sourceArg.ShuffleColMax
		op.ShuffleColMin = sourceArg.ShuffleColMin
		op.BucketNum = sourceArg.BucketNum
		op.ShuffleRangeInt64 = sourceArg.ShuffleRangeInt64
		op.ShuffleRangeUint64 = sourceArg.ShuffleRangeUint64
		op.CurrentShuffleIdx = int32(index)
		op.SetInfo(&info)
		return op
	case vm.Shuffle:
		sourceArg := sourceOp.(*shuffle.Shuffle)
		if sourceArg.GetShufflePool() == nil {
			sourceArg.SetShufflePool(shuffle.NewShufflePool(sourceArg.BucketNum, int32(maxParallel)))
		}
		op := shuffle.NewArgument()
		op.SetShufflePool(sourceArg.GetShufflePool())
		op.ShuffleType = sourceArg.ShuffleType
		op.ShuffleColIdx = sourceArg.ShuffleColIdx
		op.ShuffleColMax = sourceArg.ShuffleColMax
		op.ShuffleColMin = sourceArg.ShuffleColMin
		op.BucketNum = sourceArg.BucketNum
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
	case vm.PartitionInsert:
		t := sourceOp.(*insert.PartitionInsert)
		op := insert.NewPartitionInsertFrom(t)
		op.SetInfo(&info)
		return op
	case vm.PartitionDelete:
		t := sourceOp.(*deletion.PartitionDelete)
		op := deletion.NewPartitionDeleteFrom(t)
		op.SetInfo(&info)
		return op
	case vm.PreInsert:
		t := sourceOp.(*preinsert.PreInsert)
		op := preinsert.NewArgument()
		op.SchemaName = t.SchemaName
		op.TableDef = t.TableDef
		op.Attrs = t.Attrs
		op.IsOldUpdate = t.IsOldUpdate
		op.IsNewUpdate = t.IsNewUpdate
		op.HasAutoCol = t.HasAutoCol
		op.EstimatedRowCount = t.EstimatedRowCount
		op.CompPkeyExpr = t.CompPkeyExpr
		op.ClusterByExpr = t.ClusterByExpr
		op.ColOffset = t.ColOffset
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
		op.ApplyType = t.ApplyType
		op.Result = t.Result
		op.Typs = t.Typs
		op.TableFunction = table_function.NewArgument()
		op.TableFunction.FuncName = t.TableFunction.FuncName
		op.TableFunction.Args = t.TableFunction.Args
		op.TableFunction.Rets = t.TableFunction.Rets
		op.TableFunction.Attrs = t.TableFunction.Attrs
		op.TableFunction.Params = t.TableFunction.Params
		op.TableFunction.IsSingle = t.TableFunction.IsSingle
		op.TableFunction.SetInfo(&info)
		op.SetInfo(&info)
		return op
	case vm.MultiUpdate:
		t := sourceOp.(*multi_update.MultiUpdate)
		op := multi_update.NewArgument()
		op.MultiUpdateCtx = t.MultiUpdateCtx
		op.Action = t.Action
		op.IsRemote = t.IsRemote
		op.IsOnduplicateKeyUpdate = t.IsOnduplicateKeyUpdate
		op.Engine = t.Engine
		op.SetInfo(&info)
		return op
	case vm.DedupJoin:
		t := sourceOp.(*dedupjoin.DedupJoin)
		op := dedupjoin.NewArgument()
		if t.Channel == nil {
			t.Channel = make(chan *bitmap.Bitmap, maxParallel)
		}
		op.Channel = t.Channel
		op.NumCPU = uint64(maxParallel)
		op.IsMerger = (index == 0)
		op.Result = t.Result
		op.LeftTypes = t.LeftTypes
		op.RightTypes = t.RightTypes
		op.Conditions = t.Conditions
		op.IsShuffle = t.IsShuffle
		op.ShuffleIdx = t.ShuffleIdx
		if t.ShuffleIdx == -1 { // shuffleV2
			op.ShuffleIdx = int32(index)
		}
		op.RuntimeFilterSpecs = t.RuntimeFilterSpecs
		op.JoinMapTag = t.JoinMapTag
		op.OnDuplicateAction = t.OnDuplicateAction
		op.DedupColName = t.DedupColName
		op.DedupColTypes = t.DedupColTypes
		op.UpdateColIdxList = t.UpdateColIdxList
		op.UpdateColExprList = t.UpdateColExprList
		op.DelColIdx = t.DelColIdx
		return op
	case vm.RightDedupJoin:
		t := sourceOp.(*rightdedupjoin.RightDedupJoin)
		op := rightdedupjoin.NewArgument()
		op.Result = t.Result
		op.LeftTypes = t.LeftTypes
		op.RightTypes = t.RightTypes
		op.Conditions = t.Conditions
		op.IsShuffle = t.IsShuffle
		op.ShuffleIdx = t.ShuffleIdx
		if t.ShuffleIdx == -1 { // shuffleV2
			op.ShuffleIdx = int32(index)
		}
		op.RuntimeFilterSpecs = t.RuntimeFilterSpecs
		op.JoinMapTag = t.JoinMapTag
		op.OnDuplicateAction = t.OnDuplicateAction
		op.DedupColName = t.DedupColName
		op.DedupColTypes = t.DedupColTypes
		op.UpdateColIdxList = t.UpdateColIdxList
		op.UpdateColExprList = t.UpdateColExprList
		op.DelColIdx = t.DelColIdx
		op.SetInfo(&info)
		return op
	case vm.PostDml:
		t := sourceOp.(*postdml.PostDml)
		op := postdml.NewArgument()
		op.PostDmlCtx = t.PostDmlCtx
		op.SetInfo(&info)
		return op
	}
	panic(fmt.Sprintf("unexpected instruction type '%d' to dup", sourceOp.OpType()))
}

func constructRestrict(node *plan.Node, filterExprs []*plan.Expr) *filter.Filter {
	op := filter.NewArgument()
	op.FilterExprs = filterExprs
	op.IsEnd = node.IsEnd
	return op
}

func constructDeletion(
	proc *process.Process,
	node *plan.Node,
	eng engine.Engine,
) (vm.Operator, error) {
	oldCtx := node.DeleteCtx
	delCtx := &deletion.DeleteCtx{
		Ref:             oldCtx.Ref,
		RowIdIdx:        int(oldCtx.RowIdIdx),
		CanTruncate:     oldCtx.CanTruncate,
		AddAffectedRows: oldCtx.AddAffectedRows,
		PrimaryKeyIdx:   int(oldCtx.PrimaryKeyIdx),
		Engine:          eng,
	}

	op := deletion.NewArgument()
	op.DeleteCtx = delCtx

	ps := proc.GetPartitionService()
	if !ps.Enabled() || !features.IsPartitioned(oldCtx.TableDef.FeatureFlag) {
		return op, nil
	}
	return deletion.NewPartitionDelete(op, oldCtx.TableDef.TblId), nil
}

func constructOnduplicateKey(node *plan.Node, _ engine.Engine) *onduplicatekey.OnDuplicatekey {
	oldCtx := node.OnDuplicateKey
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

func constructFuzzyFilter(node, tableScan, sinkScan *plan.Node) *fuzzyfilter.FuzzyFilter {
	pkName := node.TableDef.Pkey.PkeyColName
	var pkTyp plan.Type
	if pkName == catalog.CPrimaryKeyColName {
		pkTyp = node.TableDef.Pkey.CompPkeyCol.Typ
	} else {
		cols := node.TableDef.Cols
		for _, c := range cols {
			if c.Name == pkName {
				pkTyp = c.Typ
			}
		}
	}

	op := fuzzyfilter.NewArgument()
	op.PkName = pkName
	op.PkTyp = pkTyp
	op.IfInsertFromUnique = node.IfInsertFromUnique

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
		if len(node.RuntimeFilterBuildList) > 0 {
			op.RuntimeFilterSpec = node.RuntimeFilterBuildList[0]
		}
	} else {
		tableScan.RuntimeFilterProbeList = nil
		node.RuntimeFilterBuildList = nil
	}
	return op
}

func constructPreInsert(nodes []*plan.Node, node *plan.Node, eng engine.Engine, proc *process.Process) (*preinsert.PreInsert, error) {
	preCtx := node.PreInsertCtx
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
	if node.ScanSnapshot != nil && node.ScanSnapshot.TS != nil {
		if !node.ScanSnapshot.TS.Equal(timestamp.Timestamp{LogicalTime: 0, PhysicalTime: 0}) &&
			node.ScanSnapshot.TS.Less(proc.GetTxnOperator().Txn().SnapshotTS) {
			if proc.GetCloneTxnOperator() != nil {
				txnOp = proc.GetCloneTxnOperator()
			} else {
				txnOp = proc.GetTxnOperator().CloneSnapshotOp(*node.ScanSnapshot.TS)
				proc.SetCloneTxnOperator(txnOp)
			}

			if node.ScanSnapshot.Tenant != nil {
				ctx = context.WithValue(ctx, defines.TenantIDKey{}, node.ScanSnapshot.Tenant.TenantID)
			}
		}
	}

	if preCtx.Ref.SchemaName != "" {
		_, err := eng.Database(ctx, preCtx.Ref.SchemaName, txnOp)
		if err != nil {
			return nil, err
		}

	}

	op := preinsert.NewArgument()
	op.HasAutoCol = preCtx.HasAutoCol
	op.SchemaName = schemaName
	op.TableDef = preCtx.TableDef
	op.Attrs = attrs
	op.IsOldUpdate = preCtx.IsOldUpdate
	op.IsNewUpdate = preCtx.IsNewUpdate
	op.EstimatedRowCount = int64(nodes[node.Children[0]].Stats.Outcnt)
	op.CompPkeyExpr = preCtx.CompPkeyExpr
	op.ClusterByExpr = preCtx.ClusterByExpr
	op.ColOffset = preCtx.ColOffset

	return op, nil
}

func constructPreInsertUk(node *plan.Node) *preinsertunique.PreInsertUnique {
	preCtx := node.PreInsertUkCtx
	op := preinsertunique.NewArgument()
	op.PreInsertCtx = preCtx
	return op
}

func constructPreInsertSk(node *plan.Node) *preinsertsecondaryindex.PreInsertSecIdx {
	op := preinsertsecondaryindex.NewArgument()
	op.PreInsertCtx = node.PreInsertSkCtx
	return op
}

func constructMergeblock(eng engine.Engine, node *plan.Node) *mergeblock.MergeBlock {
	return mergeblock.NewArgument().
		WithEngine(eng).
		WithObjectRef(node.InsertCtx.Ref).
		WithAddAffectedRows(node.InsertCtx.AddAffectedRows)
}

func constructLockOp(node *plan.Node, eng engine.Engine) (*lockop.LockOp, error) {
	arg := lockop.NewArgumentByEngine(eng)
	for _, target := range node.LockTargets {
		partitionColPos := int32(-1)
		if target.HasPartitionCol {
			partitionColPos = target.PartitionColIdxInBat
		}
		typ := plan2.MakeTypeByPlan2Type(target.PrimaryColTyp)
		arg.AddLockTarget(target.GetTableId(), target.GetObjRef(), target.GetPrimaryColIdxInBat(), typ, partitionColPos, target.GetRefreshTsIdxInBat(), target.GetLockRows(), target.GetLockTableAtTheEnd())
	}
	for _, target := range node.LockTargets {
		if target.LockTable {
			arg.LockTable(target.TableId, false)
		}
	}
	return arg, nil
}

func constructMultiUpdate(
	node *plan.Node,
	eng engine.Engine,
	proc *process.Process,
	action multi_update.UpdateAction,
	isRemote bool,
) (vm.Operator, error) {
	arg := multi_update.NewArgument()
	arg.Engine = eng
	arg.IsRemote = isRemote

	arg.MultiUpdateCtx = make([]*multi_update.MultiUpdateCtx, len(node.UpdateCtxList))
	for i, updateCtx := range node.UpdateCtxList {
		insertCols := make([]int, len(updateCtx.InsertCols))
		for j, col := range updateCtx.InsertCols {
			insertCols[j] = int(col.ColPos)
		}

		deleteCols := make([]int, len(updateCtx.DeleteCols))
		for j, col := range updateCtx.DeleteCols {
			deleteCols[j] = int(col.ColPos)
		}

		partitionCols := make([]int, len(updateCtx.PartitionCols))
		for j, col := range updateCtx.PartitionCols {
			partitionCols[j] = int(col.ColPos)
		}

		arg.MultiUpdateCtx[i] = &multi_update.MultiUpdateCtx{
			ObjRef:        updateCtx.ObjRef,
			TableDef:      updateCtx.TableDef,
			InsertCols:    insertCols,
			DeleteCols:    deleteCols,
			PartitionCols: partitionCols,
		}
	}
	arg.Action = action

	ps := proc.GetPartitionService()
	if !ps.Enabled() || !features.IsPartitioned(node.UpdateCtxList[0].TableDef.FeatureFlag) {
		return arg, nil
	}

	return multi_update.NewPartitionMultiUpdate(
		arg,
		node.UpdateCtxList[0].TableDef.TblId,
	), nil
}

func constructInsert(
	proc *process.Process,
	node *plan.Node,
	eng engine.Engine,
	toS3 bool,
) (vm.Operator, error) {
	oldCtx := node.InsertCtx
	var attrs []string
	for _, col := range oldCtx.TableDef.Cols {
		if col.Name != catalog.Row_ID {
			attrs = append(attrs, col.GetOriginCaseName())
		}
	}
	newCtx := &insert.InsertCtx{
		Ref:             oldCtx.Ref,
		AddAffectedRows: oldCtx.AddAffectedRows,
		Engine:          eng,
		Attrs:           attrs,
		TableDef:        oldCtx.TableDef,
	}
	arg := insert.NewArgument()
	arg.InsertCtx = newCtx
	arg.ToWriteS3 = toS3

	ps := proc.GetPartitionService()
	if !ps.Enabled() || !features.IsPartitioned(oldCtx.TableDef.FeatureFlag) {
		return arg, nil
	}

	return insert.NewPartitionInsert(arg, oldCtx.TableDef.TblId), nil
}

func constructProjection(node *plan.Node) *projection.Projection {
	arg := projection.NewArgument()
	arg.ProjectList = node.ProjectList
	return arg
}

func constructExternal(node *plan.Node, param *tree.ExternParam, ctx context.Context, fileList []string, FileSize []int64, fileOffset []*pipeline.FileOffset, strictSqlMode bool) *external.External {
	var attrs []plan.ExternAttr

	for i, col := range node.TableDef.Cols {
		if !col.Hidden {
			attr := plan.ExternAttr{ColName: col.Name,
				ColIndex:      int32(i),
				ColFieldIndex: node.ExternScan.TbColToDataCol[col.Name]}
			attrs = append(attrs, attr)
		}
	}

	return external.NewArgument().WithEs(
		&external.ExternalParam{
			ExParamConst: external.ExParamConst{
				Attrs:           attrs,
				Cols:            node.TableDef.Cols,
				ColumnListLen:   int32(len(node.ExternScan.TbColToDataCol)),
				Extern:          param,
				FileOffsetTotal: fileOffset,
				CreateSql:       node.TableDef.Createsql,
				Ctx:             ctx,
				FileList:        fileList,
				FileSize:        FileSize,
				ClusterTable:    node.GetClusterTable(),
				StrictSqlMode:   strictSqlMode,
			},
			ExParam: external.ExParam{
				Fileparam: new(external.ExFileparam),
				Filter: &external.FilterParam{
					FilterExpr: colexec.RewriteFilterExprList(node.FilterList),
				},
			},
		},
	)
}

func constructStream(node *plan.Node, p [2]int64) *source.Source {
	arg := source.NewArgument()
	arg.TblDef = node.TableDef
	arg.Offset = p[0]
	arg.Limit = p[1]
	return arg
}

func constructTableFunction(node *plan.Node, qry *plan.Query) *table_function.TableFunction {
	attrs := make([]string, len(node.TableDef.Cols))
	for j, col := range node.TableDef.Cols {
		attrs[j] = col.GetOriginCaseName()
	}
	arg := table_function.NewArgument()
	arg.Attrs = attrs
	arg.Rets = node.TableDef.Cols
	arg.Args = node.TblFuncExprList
	arg.FuncName = node.TableDef.TblFunc.Name
	arg.Params = node.TableDef.TblFunc.Param
	arg.IsSingle = node.TableDef.TblFunc.IsSingle
	arg.Limit = node.Limit
	// probe side runtime filter specs
	arg.RuntimeFilterSpecs = node.RuntimeFilterProbeList
	arg.IndexReaderParam = node.IndexReaderParam
	return arg
}

func constructTop(node *plan.Node, topN *plan.Expr) *top.Top {
	arg := top.NewArgument()
	arg.Fs = node.OrderBy
	arg.Limit = topN
	if len(node.SendMsgList) > 0 && node.SendMsgList[0].MsgType == int32(message.MsgTopValue) {
		arg.TopValueTag = node.SendMsgList[0].MsgTag
	}
	return arg
}

func constructHashJoin(node, left *plan.Node, left_types, right_types []types.Type, proc *process.Process) *hashjoin.HashJoin {
	result := make([]colexec.ResultPos, len(node.ProjectList))
	for i, expr := range node.ProjectList {
		result[i].Rel, result[i].Pos = constructJoinResult(expr, proc)
	}
	nonEqCond, eqConds := extraJoinConditions(node.OnList)
	arg := hashjoin.NewArgument()
	arg.JoinType = node.JoinType
	arg.IsRightJoin = node.IsRightJoin
	arg.LeftTypes = left_types
	arg.RightTypes = right_types
	arg.ResultCols = result
	arg.NonEqCond = nonEqCond
	arg.EqConds = constructJoinConditions(eqConds, proc)
	arg.RuntimeFilterSpecs = node.RuntimeFilterBuildList
	arg.HashOnPK = node.Stats.HashmapStats != nil && node.Stats.HashmapStats.HashOnPK
	arg.CanSkipProbe = node.JoinType == plan.Node_SEMI && !node.IsRightJoin && left.NodeType == plan.Node_TABLE_SCAN
	arg.IsShuffle = node.Stats.HashmapStats != nil && node.Stats.HashmapStats.Shuffle
	for i := range node.SendMsgList {
		if node.SendMsgList[i].MsgType == int32(message.MsgJoinMap) {
			arg.JoinMapTag = node.SendMsgList[i].MsgTag
		}
	}
	if arg.JoinMapTag <= 0 {
		panic("wrong joinmap tag!")
	}
	return arg
}

func constructDedupJoin(node *plan.Node, leftTypes, rightTypes []types.Type, proc *process.Process) *dedupjoin.DedupJoin {
	result := make([]colexec.ResultPos, len(node.ProjectList))
	for i, expr := range node.ProjectList {
		result[i].Rel, result[i].Pos = constructJoinResult(expr, proc)
	}
	cond, conds := extraJoinConditions(node.OnList)
	if cond != nil {
		panic("dedupjoin should not have non-equi join condition")
	}
	arg := dedupjoin.NewArgument()
	arg.LeftTypes = leftTypes
	arg.RightTypes = rightTypes
	arg.Result = result
	arg.Conditions = constructJoinConditions(conds, proc)
	arg.RuntimeFilterSpecs = node.RuntimeFilterBuildList
	arg.OnDuplicateAction = node.OnDuplicateAction
	arg.DedupColName = node.DedupColName
	arg.DedupColTypes = node.DedupColTypes
	arg.DelColIdx = -1
	if node.DedupJoinCtx != nil {
		arg.UpdateColIdxList = node.DedupJoinCtx.UpdateColIdxList
		arg.UpdateColExprList = node.DedupJoinCtx.UpdateColExprList
		if node.OnDuplicateAction == plan.Node_FAIL && len(node.DedupJoinCtx.OldColList) > 0 {
			arg.DelColIdx = node.DedupJoinCtx.OldColList[0].ColPos
		}
	}
	arg.IsShuffle = node.Stats.HashmapStats != nil && node.Stats.HashmapStats.Shuffle
	for i := range node.SendMsgList {
		if node.SendMsgList[i].MsgType == int32(message.MsgJoinMap) {
			arg.JoinMapTag = node.SendMsgList[i].MsgTag
		}
	}
	if arg.JoinMapTag <= 0 {
		panic("wrong joinmap tag!")
	}
	return arg
}

func constructRightDedupJoin(node *plan.Node, leftTypes, rightTypes []types.Type, proc *process.Process) *rightdedupjoin.RightDedupJoin {
	result := make([]colexec.ResultPos, len(node.ProjectList))
	for i, expr := range node.ProjectList {
		result[i].Rel, result[i].Pos = constructJoinResult(expr, proc)
	}
	cond, conds := extraJoinConditions(node.OnList)
	if cond != nil {
		panic("dedupjoin should not have non-equi join condition")
	}
	arg := rightdedupjoin.NewArgument()
	arg.LeftTypes = leftTypes
	arg.RightTypes = rightTypes
	arg.Result = result
	arg.Conditions = constructJoinConditions(conds, proc)
	arg.RuntimeFilterSpecs = node.RuntimeFilterBuildList
	arg.OnDuplicateAction = node.OnDuplicateAction
	arg.DedupColName = node.DedupColName
	arg.DedupColTypes = node.DedupColTypes
	arg.DelColIdx = -1
	if node.DedupJoinCtx != nil {
		arg.UpdateColIdxList = node.DedupJoinCtx.UpdateColIdxList
		arg.UpdateColExprList = node.DedupJoinCtx.UpdateColExprList
		if node.OnDuplicateAction == plan.Node_FAIL && len(node.DedupJoinCtx.OldColList) > 0 {
			arg.DelColIdx = node.DedupJoinCtx.OldColList[0].ColPos
		}
	}
	arg.IsShuffle = node.Stats.HashmapStats != nil && node.Stats.HashmapStats.Shuffle
	for i := range node.SendMsgList {
		if node.SendMsgList[i].MsgType == int32(message.MsgJoinMap) {
			arg.JoinMapTag = node.SendMsgList[i].MsgTag
		}
	}
	if arg.JoinMapTag <= 0 {
		panic("wrong joinmap tag!")
	}
	return arg
}

func constructProduct(node *plan.Node, typs []types.Type, proc *process.Process) *product.Product {
	result := make([]colexec.ResultPos, len(node.ProjectList))
	for i, expr := range node.ProjectList {
		result[i].Rel, result[i].Pos = constructJoinResult(expr, proc)
	}
	arg := product.NewArgument()
	arg.Result = result
	for i := range node.SendMsgList {
		if node.SendMsgList[i].MsgType == int32(message.MsgJoinMap) {
			arg.JoinMapTag = node.SendMsgList[i].MsgTag
		}
	}
	if arg.JoinMapTag <= 0 {
		panic("wrong joinmap tag!")
	}
	return arg
}

func constructOrder(node *plan.Node) *order.Order {
	arg := order.NewArgument()
	arg.OrderBySpec = node.OrderBy
	return arg
}

func constructUnionAll(_ *plan.Node) *unionall.UnionAll {
	arg := unionall.NewArgument()
	return arg
}

func constructFill(node *plan.Node) *fill.Fill {
	aggIdx := make([]int32, len(node.AggList))
	for i, expr := range node.AggList {
		f := expr.Expr.(*plan.Expr_F)
		obj := int64(uint64(f.F.Func.Obj) & function.DistinctMask)
		aggIdx[i], _ = function.DecodeOverloadID(obj)
	}
	arg := fill.NewArgument()
	arg.ColLen = len(node.AggList)
	arg.FillType = node.FillType
	arg.FillVal = node.FillVal
	arg.AggIds = aggIdx
	return arg
}

func constructTimeWindow(_ context.Context, node *plan.Node, proc *process.Process) *timewin.TimeWin {
	var aggregationExpressions []aggexec.AggFuncExecExpression = nil
	var typs []types.Type
	var wStart, wEnd bool
	i := 0
	for _, expr := range node.AggList {
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

	arg := timewin.NewArgument()
	err := arg.MakeIntervalAndSliding(node.Interval, node.Sliding)
	if err != nil {
		panic(err)
	}
	arg.Types = typs
	arg.Aggs = aggregationExpressions
	arg.Ts = node.GroupBy[0]
	arg.WStart = wStart
	arg.WEnd = wEnd
	arg.EndExpr = node.WEnd
	arg.TsType = node.Timestamp.Typ
	return arg
}

func constructWindow(_ context.Context, node *plan.Node, proc *process.Process) *window.Window {
	aggregationExpressions := make([]aggexec.AggFuncExecExpression, len(node.WinSpecList))
	typs := make([]types.Type, len(node.WinSpecList))

	for i, expr := range node.WinSpecList {
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
				vec, free, err := colexec.GetReadonlyResultFromNoColumnExpression(proc, argExpr)
				if err != nil {
					panic(err)
				}
				cfg = []byte(vec.GetStringAt(0))
				free()

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
	arg.WinSpecList = node.WinSpecList
	return arg
}

func constructOffset(node *plan.Node) *offset.Offset {
	arg := offset.NewArgument().WithOffset(node.Offset)
	return arg
}

func constructLimit(node *plan.Node) *limit.Limit {
	arg := limit.NewArgument().WithLimit(node.Limit)
	return arg
}

func constructSample(node *plan.Node, outputRowCount bool) *sample.Sample {
	if node.SampleFunc.Rows != plan2.NotSampleByRows {
		return sample.NewSampleByRows(int(node.SampleFunc.Rows), node.AggList, node.GroupBy, node.SampleFunc.UsingRow, outputRowCount)
	}
	if node.SampleFunc.Percent != plan2.NotSampleByPercents {
		return sample.NewSampleByPercent(node.SampleFunc.Percent, node.AggList, node.GroupBy)
	}
	panic("only support sample by rows / percent now.")
}

func constructGroup(_ context.Context, node, childNode *plan.Node, needEval bool, shuffleDop int, proc *process.Process) *group.Group {
	aggregationExpressions := make([]aggexec.AggFuncExecExpression, len(node.AggList))
	for i, expr := range node.AggList {
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
					vec, free, err := colexec.GetReadonlyResultFromNoColumnExpression(proc, argExpr)
					if err != nil {
						panic(err)
					}
					cfg = []byte(vec.GetStringAt(0))
					free()

					args = f.F.Args[:len(f.F.Args)-1]
				}
			}

			aggregationExpressions[i] = aggexec.MakeAggFunctionExpression(
				functionID, isDistinct, args, cfg)
		}
	}

	typs := make([]types.Type, len(childNode.ProjectList))
	for i, e := range childNode.ProjectList {
		typs[i] = types.New(types.T(e.Typ.Id), e.Typ.Width, e.Typ.Scale)
	}

	arg := group.NewArgument()
	arg.Aggs = aggregationExpressions
	arg.NeedEval = needEval
	arg.SpillMem = node.SpillMem
	arg.GroupingFlag = node.GroupingFlag
	arg.GroupBy = node.GroupBy
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

func constructShuffleOperatorForJoinV2(bucketNum int32, node *plan.Node, left bool) *shuffleV2.ShuffleV2 {
	arg := shuffleV2.NewArgument()
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
	arg.BucketNum = bucketNum
	switch types.T(typ) {
	case types.T_int64, types.T_int32, types.T_int16:
		arg.ShuffleRangeInt64 = plan2.ShuffleRangeReEvalSigned(node.Stats.HashmapStats.Ranges, int(arg.BucketNum), node.Stats.HashmapStats.Nullcnt, int64(node.Stats.TableCnt))
	case types.T_uint64, types.T_uint32, types.T_uint16, types.T_varchar, types.T_char, types.T_text, types.T_bit, types.T_datalink:
		arg.ShuffleRangeUint64 = plan2.ShuffleRangeReEvalUnsigned(node.Stats.HashmapStats.Ranges, int(arg.BucketNum), node.Stats.HashmapStats.Nullcnt, int64(node.Stats.TableCnt))
	}
	return arg
}

func constructShuffleOperatorForJoin(bucketNum int32, node *plan.Node, left bool) *shuffle.Shuffle {
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
	arg.BucketNum = bucketNum
	switch types.T(typ) {
	case types.T_int64, types.T_int32, types.T_int16:
		arg.ShuffleRangeInt64 = plan2.ShuffleRangeReEvalSigned(node.Stats.HashmapStats.Ranges, int(arg.BucketNum), node.Stats.HashmapStats.Nullcnt, int64(node.Stats.TableCnt))
	case types.T_uint64, types.T_uint32, types.T_uint16, types.T_varchar, types.T_char, types.T_text, types.T_bit, types.T_datalink:
		arg.ShuffleRangeUint64 = plan2.ShuffleRangeReEvalUnsigned(node.Stats.HashmapStats.Ranges, int(arg.BucketNum), node.Stats.HashmapStats.Nullcnt, int64(node.Stats.TableCnt))
	}
	if left && len(node.RuntimeFilterProbeList) > 0 {
		arg.RuntimeFilterSpec = plan2.DeepCopyRuntimeFilterSpec(node.RuntimeFilterProbeList[0])
	}
	return arg
}

func constructShuffleArgForGroupV2(node *plan.Node, dop int32) *shuffleV2.ShuffleV2 {
	arg := shuffleV2.NewArgument()
	hashCol, typ := plan2.GetHashColumn(node.GroupBy[node.Stats.HashmapStats.ShuffleColIdx])
	arg.ShuffleColIdx = hashCol.ColPos
	arg.ShuffleType = int32(node.Stats.HashmapStats.ShuffleType)
	arg.ShuffleColMin = node.Stats.HashmapStats.ShuffleColMin
	arg.ShuffleColMax = node.Stats.HashmapStats.ShuffleColMax
	arg.BucketNum = dop
	switch types.T(typ) {
	case types.T_int64, types.T_int32, types.T_int16:
		arg.ShuffleRangeInt64 = plan2.ShuffleRangeReEvalSigned(node.Stats.HashmapStats.Ranges, int(arg.BucketNum), node.Stats.HashmapStats.Nullcnt, int64(node.Stats.TableCnt))
	case types.T_uint64, types.T_uint32, types.T_uint16, types.T_varchar, types.T_char, types.T_text, types.T_bit, types.T_datalink:
		arg.ShuffleRangeUint64 = plan2.ShuffleRangeReEvalUnsigned(node.Stats.HashmapStats.Ranges, int(arg.BucketNum), node.Stats.HashmapStats.Nullcnt, int64(node.Stats.TableCnt))
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
	arg.BucketNum = int32(len(ss))
	switch types.T(typ) {
	case types.T_int64, types.T_int32, types.T_int16:
		arg.ShuffleRangeInt64 = plan2.ShuffleRangeReEvalSigned(node.Stats.HashmapStats.Ranges, int(arg.BucketNum), node.Stats.HashmapStats.Nullcnt, int64(node.Stats.TableCnt))
	case types.T_uint64, types.T_uint32, types.T_uint16, types.T_varchar, types.T_char, types.T_text, types.T_bit, types.T_datalink:
		arg.ShuffleRangeUint64 = plan2.ShuffleRangeReEvalUnsigned(node.Stats.HashmapStats.Ranges, int(arg.BucketNum), node.Stats.HashmapStats.Nullcnt, int64(node.Stats.TableCnt))
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

func constructMergeGroup(node *plan.Node, aggs []aggexec.AggFuncExecExpression) *group.MergeGroup {
	arg := group.NewArgumentMergeGroup()
	// here the node is a Group node, merge group is "generated" by the
	// group node and then merge them
	arg.SpillMem = node.SpillMem
	arg.Aggs = aggs
	return arg
}

func constructMergeTop(node *plan.Node, topN *plan.Expr) *mergetop.MergeTop {
	arg := mergetop.NewArgument()
	arg.Fs = node.OrderBy
	arg.Limit = topN
	return arg
}

func constructMergeOrder(node *plan.Node) *mergeorder.MergeOrder {
	arg := mergeorder.NewArgument()
	arg.OrderBySpecs = node.OrderBy
	return arg
}

func constructPartition(node *plan.Node) *partition.Partition {
	arg := partition.NewArgument()
	arg.OrderBySpecs = node.OrderBy
	return arg
}

func constructIndexJoin(node *plan.Node, proc *process.Process) *indexjoin.IndexJoin {
	result := make([]int32, len(node.ProjectList))
	for i, expr := range node.ProjectList {
		rel, pos := constructJoinResult(expr, proc)
		if rel != 0 {
			panic(moerr.NewNYIf(proc.GetTopContext(), "loop semi result '%s'", expr))
		}
		result[i] = pos
	}
	arg := indexjoin.NewArgument()
	arg.ResultCols = result
	arg.RuntimeFilterSpecs = node.RuntimeFilterBuildList
	return arg
}

func constructProductL2(node *plan.Node, proc *process.Process) *productl2.Productl2 {
	result := make([]colexec.ResultPos, len(node.ProjectList))
	for i, expr := range node.ProjectList {
		result[i].Rel, result[i].Pos = constructJoinResult(expr, proc)
	}
	arg := productl2.NewArgument()
	arg.VectorOpType = node.ExtraOptions
	arg.Result = result
	arg.OnExpr = colexec.RewriteFilterExprList(node.OnList)
	for i := range node.SendMsgList {
		if node.SendMsgList[i].MsgType == int32(message.MsgJoinMap) {
			arg.JoinMapTag = node.SendMsgList[i].MsgTag
		}
	}
	if arg.JoinMapTag <= 0 {
		panic("wrong joinmap tag!")
	}
	return arg
}

func constructLoopJoin(node *plan.Node, rightTypes []types.Type, proc *process.Process) *loopjoin.LoopJoin {
	result := make([]colexec.ResultPos, len(node.ProjectList))
	for i, expr := range node.ProjectList {
		result[i].Rel, result[i].Pos = constructJoinResult(expr, proc)
	}
	arg := loopjoin.NewArgument()
	arg.ResultCols = result
	arg.RightTypes = rightTypes
	arg.NonEqCond = colexec.RewriteFilterExprList(node.OnList)
	arg.JoinType = node.JoinType
	for i := range node.SendMsgList {
		if node.SendMsgList[i].MsgType == int32(message.MsgJoinMap) {
			arg.JoinMapTag = node.SendMsgList[i].MsgTag
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

// If the join condition is table1.col = table2.col.
// for hash build operator, we only get table2's data, the origin relation index for right-condition is 1 but wrong.
//
// rewriteJoinExprToHashBuildExpr set the relation index to be 0 for resolving this problem.
func rewriteJoinExprToHashBuildExpr(src []*plan.Expr) []*plan.Expr {
	var doRelIndexRewrite func(expr *plan.Expr)
	doRelIndexRewrite = func(expr *plan.Expr) {
		switch t := expr.Expr.(type) {
		case *plan.Expr_F:
			for i := range t.F.Args {
				doRelIndexRewrite(t.F.Args[i])
			}
		case *plan.Expr_List:
			for i := range t.List.List {
				doRelIndexRewrite(t.List.List[i])
			}
		case *plan.Expr_Col:
			t.Col.RelPos = 0
		}
	}

	dst := make([]*plan.Expr, len(src))
	for i := range src {
		dst[i] = plan2.DeepCopyExpr(src[i])
		doRelIndexRewrite(dst[i])
	}
	return dst
}

func constructHashBuild(op vm.Operator, proc *process.Process, mcpu int32) *hashbuild.HashBuild {
	ret := hashbuild.NewArgument()

	switch op.OpType() {
	case vm.HashJoin:
		arg := op.(*hashjoin.HashJoin)
		ret.NeedHashMap = true
		ret.Conditions = rewriteJoinExprToHashBuildExpr(arg.EqConds[1])

		// to find if hashmap need to keep build batches for probe
		//var needMergedBatch bool
		//if arg.NonEqCond != nil {
		//	needMergedBatch = true
		//} else {
		//	for _, rp := range arg.ResultCols {
		//		if rp.Rel == 1 {
		//			needMergedBatch = true
		//			break
		//		}
		//	}
		//}
		//ret.NeedBatches = needMergedBatch

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

	case vm.DedupJoin:
		arg := op.(*dedupjoin.DedupJoin)
		ret.NeedHashMap = true
		ret.Conditions = arg.Conditions[1]
		ret.NeedBatches = true
		ret.NeedAllocateSels = arg.OnDuplicateAction == plan.Node_UPDATE
		ret.IsDedup = true
		ret.OnDuplicateAction = arg.OnDuplicateAction
		ret.DedupColName = arg.DedupColName
		ret.DedupColTypes = arg.DedupColTypes
		ret.DelColIdx = arg.DelColIdx
		if len(arg.RuntimeFilterSpecs) > 0 {
			ret.RuntimeFilterSpec = arg.RuntimeFilterSpecs[0]
		}
		ret.JoinMapTag = arg.JoinMapTag

	case vm.RightDedupJoin:
		arg := op.(*rightdedupjoin.RightDedupJoin)
		ret.NeedHashMap = true
		ret.Conditions = arg.Conditions[1]
		ret.NeedBatches = false
		ret.NeedAllocateSels = false
		ret.IsDedup = false
		ret.OnDuplicateAction = arg.OnDuplicateAction
		ret.DedupColName = arg.DedupColName
		ret.DedupColTypes = arg.DedupColTypes
		ret.DelColIdx = arg.DelColIdx
		if len(arg.RuntimeFilterSpecs) > 0 {
			ret.RuntimeFilterSpec = arg.RuntimeFilterSpecs[0]
		}
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
	case vm.HashJoin:
		arg := op.(*hashjoin.HashJoin)
		ret.Conditions = rewriteJoinExprToHashBuildExpr(arg.EqConds[1])
		// to find if hashmap need to keep build batches for probe
		//var needMergedBatch bool
		//if arg.NonEqCond != nil {
		//	needMergedBatch = true
		//} else {
		//	for _, rp := range arg.ResultCols {
		//		if rp.Rel == 1 {
		//			needMergedBatch = true
		//			break
		//		}
		//	}
		//}
		//ret.NeedBatches = needMergedBatch
		ret.NeedBatches = true
		ret.HashOnPK = arg.HashOnPK
		ret.NeedAllocateSels = true
		if len(arg.RuntimeFilterSpecs) > 0 {
			ret.RuntimeFilterSpec = plan2.DeepCopyRuntimeFilterSpec(arg.RuntimeFilterSpecs[0])
		}
		ret.JoinMapTag = arg.JoinMapTag
		ret.ShuffleIdx = arg.ShuffleIdx

	case vm.DedupJoin:
		arg := op.(*dedupjoin.DedupJoin)
		ret.Conditions = arg.Conditions[1]
		ret.NeedBatches = true
		ret.NeedAllocateSels = arg.OnDuplicateAction == plan.Node_UPDATE
		ret.IsDedup = true
		ret.OnDuplicateAction = arg.OnDuplicateAction
		ret.DedupColName = arg.DedupColName
		ret.DedupColTypes = arg.DedupColTypes
		ret.DelColIdx = arg.DelColIdx
		if len(arg.RuntimeFilterSpecs) > 0 {
			ret.RuntimeFilterSpec = plan2.DeepCopyRuntimeFilterSpec(arg.RuntimeFilterSpecs[0])
		}
		ret.JoinMapTag = arg.JoinMapTag
		ret.ShuffleIdx = arg.ShuffleIdx

	case vm.RightDedupJoin:
		arg := op.(*rightdedupjoin.RightDedupJoin)
		ret.Conditions = arg.Conditions[1]
		ret.NeedBatches = false
		ret.NeedAllocateSels = false
		ret.IsDedup = false
		ret.OnDuplicateAction = arg.OnDuplicateAction
		ret.DedupColName = arg.DedupColName
		ret.DedupColTypes = arg.DedupColTypes
		ret.DelColIdx = arg.DelColIdx
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

func constructApply(n, right *plan.Node, applyType int, proc *process.Process) *apply.Apply {
	result := make([]colexec.ResultPos, len(n.ProjectList))
	for i, expr := range n.ProjectList {
		result[i].Rel, result[i].Pos = constructJoinResult(expr, proc)
	}
	rightTyps := make([]types.Type, len(right.TableDef.Cols))
	for i, expr := range right.TableDef.Cols {
		rightTyps[i] = dupType(&expr.Typ)
	}
	arg := apply.NewArgument()
	arg.ApplyType = applyType
	arg.Result = result
	arg.Typs = rightTyps
	arg.TableFunction = constructTableFunction(right, nil)
	return arg
}

func constructTableScan(node *plan.Node) *table_scan.TableScan {
	types := make([]plan.Type, len(node.TableDef.Cols))
	for j, col := range node.TableDef.Cols {
		types[j] = col.Typ
	}
	return table_scan.NewArgument().WithTypes(types)
}

func constructValueScan(proc *process.Process, node *plan.Node) (*value_scan.ValueScan, error) {
	op := value_scan.NewArgument()
	if node == nil {
		return op, nil
	}
	op.NodeType = node.NodeType
	if node.RowsetData == nil {
		return op, nil
	}

	op.ColCount = len(node.TableDef.Cols)
	op.Batchs = make([]*batch.Batch, 2)
	op.Batchs[0] = batch.NewWithSize(len(node.RowsetData.Cols))
	op.Batchs[0].SetRowCount(len(node.RowsetData.Cols[0].Data))
	rowsetData := &plan.RowsetData{
		Cols: make([]*plan.ColData, op.ColCount),
	}
	for i := 0; i < op.ColCount; i++ {
		rowsetData.Cols[i] = new(plan.ColData)
	}

	for i, col := range node.RowsetData.Cols {
		vec := vector.NewVec(plan2.MakeTypeByPlan2Type(node.TableDef.Cols[i].Typ))
		op.Batchs[0].Vecs[i] = vec
		for j, rowsetExpr := range col.Data {
			get, err := rule.GetConstantValue2(proc, rowsetExpr.Expr, vec)
			if err != nil {
				op.Batchs[0].Clean(proc.Mp())
				return nil, err
			}
			if !get {
				rowsetExpr.RowPos = int32(j)
				rowsetData.Cols[i].Data = append(rowsetData.Cols[i].Data, rowsetExpr)
			}
		}
	}
	op.RowsetData = rowsetData

	return op, nil
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

func constructPostDml(node *plan.Node, eng engine.Engine) *postdml.PostDml {
	oldCtx := node.PostDmlCtx
	delCtx := &postdml.PostDmlCtx{
		Ref:                    oldCtx.Ref,
		AddAffectedRows:        oldCtx.AddAffectedRows,
		PrimaryKeyIdx:          oldCtx.PrimaryKeyIdx,
		PrimaryKeyName:         oldCtx.PrimaryKeyName,
		IsDelete:               oldCtx.IsDelete,
		IsInsert:               oldCtx.IsInsert,
		IsDeleteWithoutFilters: oldCtx.IsDeleteWithoutFilters,
	}

	if oldCtx.FullText != nil {
		delCtx.FullText = &postdml.PostDmlFullTextCtx{
			SourceTableName: oldCtx.FullText.SourceTableName,
			IndexTableName:  oldCtx.FullText.IndexTableName,
			Parts:           oldCtx.FullText.Parts,
			AlgoParams:      oldCtx.FullText.AlgoParams,
		}
	}

	op := postdml.NewArgument()
	op.PostDmlCtx = delCtx
	return op
}

func constructTableClone(
	c *Compile,
	clonePlan *plan.CloneTable,
) (*table_clone.TableClone, error) {

	metaCopy := table_clone.NewTableClone()

	metaCopy.Ctx = &table_clone.TableCloneCtx{
		Eng:       c.e,
		SrcTblDef: clonePlan.SrcTableDef,
		SrcObjDef: clonePlan.SrcObjDef,

		ScanSnapshot:    clonePlan.ScanSnapshot,
		DstTblName:      clonePlan.DstTableName,
		DstDatabaseName: clonePlan.DstDatabaseName,
	}

	var (
		err error
		ret executor.Result
		sql string

		account     = uint32(math.MaxUint32)
		colOffset   map[int32]uint64
		hasAutoIncr bool
	)

	for _, colDef := range clonePlan.SrcTableDef.Cols {
		if colDef.Typ.AutoIncr {
			hasAutoIncr = true
			break
		}
	}

	if !hasAutoIncr {
		return metaCopy, nil
	}

	sql = fmt.Sprintf(
		"select col_index, offset from mo_catalog.mo_increment_columns where table_id = %d",
		clonePlan.SrcTableDef.TblId,
	)

	if clonePlan.ScanSnapshot != nil {
		if clonePlan.ScanSnapshot.Tenant != nil {
			account = clonePlan.ScanSnapshot.Tenant.TenantID
		}

		if clonePlan.ScanSnapshot.TS != nil {
			sql = fmt.Sprintf(
				"select col_index, offset from mo_catalog.mo_increment_columns {MO_TS = %d} where table_id = %d",
				clonePlan.ScanSnapshot.TS.PhysicalTime, clonePlan.SrcTableDef.TblId,
			)
		}
	}

	if account == math.MaxUint32 {
		if account, err = defines.GetAccountId(c.proc.Ctx); err != nil {
			return nil, err
		}
	}

	if ret, err = c.runSqlWithResultAndOptions(
		sql,
		int32(account),
		executor.StatementOption{}.WithDisableLog(),
	); err != nil {
		return nil, err
	}

	ret.ReadRows(func(rows int, cols []*vector.Vector) bool {
		if colOffset == nil {
			colOffset = make(map[int32]uint64)
		}

		colIdxes := vector.MustFixedColWithTypeCheck[int32](cols[0])
		offsets := vector.MustFixedColWithTypeCheck[uint64](cols[1])

		for i := 0; i < rows; i++ {
			colOffset[colIdxes[i]] = offsets[i]
		}

		return true
	})

	ret.Close()

	metaCopy.Ctx.SrcAutoIncrOffsets = colOffset

	return metaCopy, nil
}

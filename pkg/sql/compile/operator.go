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

	"github.com/matrixorigin/matrixone/pkg/sql/colexec/productl2"

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
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/mergecte"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/mergegroup"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/mergelimit"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/mergeoffset"
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
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/restrict"
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

func dupInstruction(sourceIns *vm.Instruction, regMap map[*process.WaitRegister]*process.WaitRegister, index int) vm.Instruction {
	res := vm.Instruction{
		Op:          sourceIns.Op,
		Idx:         sourceIns.Idx,
		IsFirst:     sourceIns.IsFirst,
		IsLast:      sourceIns.IsLast,
		CnAddr:      sourceIns.CnAddr,
		OperatorID:  sourceIns.OperatorID,
		MaxParallel: sourceIns.MaxParallel,
		ParallelID:  sourceIns.ParallelID,
	}
	switch sourceIns.Op {
	case vm.Anti:
		t := sourceIns.Arg.(*anti.Argument)
		arg := anti.NewArgument()
		arg.Cond = t.Cond
		arg.Typs = t.Typs
		arg.Conditions = t.Conditions
		arg.Result = t.Result
		arg.HashOnPK = t.HashOnPK
		arg.IsShuffle = t.IsShuffle
		arg.RuntimeFilterSpecs = t.RuntimeFilterSpecs
		res.Arg = arg
	case vm.Group:
		t := sourceIns.Arg.(*group.Argument)
		arg := group.NewArgument()
		arg.IsShuffle = t.IsShuffle
		arg.PreAllocSize = t.PreAllocSize
		arg.NeedEval = t.NeedEval
		arg.Exprs = t.Exprs
		arg.Types = t.Types
		arg.Aggs = t.Aggs
		res.Arg = arg
	case vm.Sample:
		t := sourceIns.Arg.(*sample.Argument)
		res.Arg = t.SimpleDup()
	case vm.Join:
		t := sourceIns.Arg.(*join.Argument)
		arg := join.NewArgument()
		arg.Result = t.Result
		arg.Cond = t.Cond
		arg.Typs = t.Typs
		arg.Conditions = t.Conditions
		arg.RuntimeFilterSpecs = t.RuntimeFilterSpecs
		arg.HashOnPK = t.HashOnPK
		arg.IsShuffle = t.IsShuffle
		res.Arg = arg
	case vm.Left:
		t := sourceIns.Arg.(*left.Argument)
		arg := left.NewArgument()
		arg.Cond = t.Cond
		arg.Result = t.Result
		arg.Typs = t.Typs
		arg.Conditions = t.Conditions
		arg.RuntimeFilterSpecs = t.RuntimeFilterSpecs
		arg.HashOnPK = t.HashOnPK
		arg.IsShuffle = t.IsShuffle
		res.Arg = arg
	case vm.Right:
		t := sourceIns.Arg.(*right.Argument)
		arg := right.NewArgument()
		arg.Cond = t.Cond
		arg.Result = t.Result
		arg.RightTypes = t.RightTypes
		arg.LeftTypes = t.LeftTypes
		arg.Conditions = t.Conditions
		arg.RuntimeFilterSpecs = t.RuntimeFilterSpecs
		arg.HashOnPK = t.HashOnPK
		arg.IsShuffle = t.IsShuffle
		res.Arg = arg
	case vm.RightSemi:
		t := sourceIns.Arg.(*rightsemi.Argument)
		arg := rightsemi.NewArgument()
		arg.Cond = t.Cond
		arg.Result = t.Result
		arg.RightTypes = t.RightTypes
		arg.Conditions = t.Conditions
		arg.RuntimeFilterSpecs = t.RuntimeFilterSpecs
		arg.HashOnPK = t.HashOnPK
		arg.IsShuffle = t.IsShuffle
		res.Arg = arg
	case vm.RightAnti:
		t := sourceIns.Arg.(*rightanti.Argument)
		arg := rightanti.NewArgument()
		arg.Cond = t.Cond
		arg.Result = t.Result
		arg.RightTypes = t.RightTypes
		arg.Conditions = t.Conditions
		arg.RuntimeFilterSpecs = t.RuntimeFilterSpecs
		arg.HashOnPK = t.HashOnPK
		arg.IsShuffle = t.IsShuffle
		res.Arg = arg
	case vm.Limit:
		t := sourceIns.Arg.(*limit.Argument)
		arg := limit.NewArgument()
		arg.LimitExpr = t.LimitExpr
		res.Arg = arg
	case vm.LoopAnti:
		t := sourceIns.Arg.(*loopanti.Argument)
		arg := loopanti.NewArgument()
		arg.Result = t.Result
		arg.Cond = t.Cond
		arg.Typs = t.Typs
		res.Arg = arg
	case vm.LoopJoin:
		t := sourceIns.Arg.(*loopjoin.Argument)
		arg := loopjoin.NewArgument()
		arg.Result = t.Result
		arg.Cond = t.Cond
		arg.Typs = t.Typs
		res.Arg = arg
	case vm.IndexJoin:
		t := sourceIns.Arg.(*indexjoin.Argument)
		arg := indexjoin.NewArgument()
		arg.Result = t.Result
		arg.Typs = t.Typs
		arg.RuntimeFilterSpecs = t.RuntimeFilterSpecs
		res.Arg = arg
	case vm.LoopLeft:
		t := sourceIns.Arg.(*loopleft.Argument)
		arg := loopleft.NewArgument()
		arg.Cond = t.Cond
		arg.Typs = t.Typs
		arg.Result = t.Result
		res.Arg = arg
	case vm.LoopSemi:
		t := sourceIns.Arg.(*loopsemi.Argument)
		arg := loopsemi.NewArgument()
		arg.Result = t.Result
		arg.Cond = t.Cond
		arg.Typs = t.Typs
		res.Arg = arg
	case vm.LoopSingle:
		t := sourceIns.Arg.(*loopsingle.Argument)
		arg := loopsingle.NewArgument()
		arg.Result = t.Result
		arg.Cond = t.Cond
		arg.Typs = t.Typs
		res.Arg = arg
	case vm.LoopMark:
		t := sourceIns.Arg.(*loopmark.Argument)
		arg := loopmark.NewArgument()
		arg.Result = t.Result
		arg.Cond = t.Cond
		arg.Typs = t.Typs
		res.Arg = arg
	case vm.Offset:
		t := sourceIns.Arg.(*offset.Argument)
		arg := offset.NewArgument()
		arg.OffsetExpr = t.OffsetExpr
		res.Arg = arg
	case vm.Order:
		t := sourceIns.Arg.(*order.Argument)
		arg := order.NewArgument()
		arg.OrderBySpec = t.OrderBySpec
		res.Arg = arg
	case vm.Product:
		t := sourceIns.Arg.(*product.Argument)
		arg := product.NewArgument()
		arg.Result = t.Result
		arg.Typs = t.Typs
		arg.IsShuffle = t.IsShuffle
		res.Arg = arg
	case vm.ProductL2:
		t := sourceIns.Arg.(*productl2.Argument)
		arg := productl2.NewArgument()
		arg.Result = t.Result
		arg.Typs = t.Typs
		arg.OnExpr = t.OnExpr
		res.Arg = arg
	case vm.Projection:
		t := sourceIns.Arg.(*projection.Argument)
		arg := projection.NewArgument()
		arg.Es = t.Es
		res.Arg = arg
	case vm.Restrict:
		t := sourceIns.Arg.(*restrict.Argument)
		arg := restrict.NewArgument()
		arg.E = t.E
		res.Arg = arg
	case vm.Semi:
		t := sourceIns.Arg.(*semi.Argument)
		arg := semi.NewArgument()
		arg.Result = t.Result
		arg.Cond = t.Cond
		arg.Typs = t.Typs
		arg.Conditions = t.Conditions
		arg.RuntimeFilterSpecs = t.RuntimeFilterSpecs
		arg.HashOnPK = t.HashOnPK
		arg.IsShuffle = t.IsShuffle
		res.Arg = arg
	case vm.Single:
		t := sourceIns.Arg.(*single.Argument)
		arg := single.NewArgument()
		arg.Result = t.Result
		arg.Cond = t.Cond
		arg.Typs = t.Typs
		arg.Conditions = t.Conditions
		arg.RuntimeFilterSpecs = t.RuntimeFilterSpecs
		arg.HashOnPK = t.HashOnPK
		res.Arg = arg
	case vm.Top:
		t := sourceIns.Arg.(*top.Argument)
		arg := top.NewArgument()
		arg.Limit = t.Limit
		arg.TopValueTag = t.TopValueTag
		arg.Fs = t.Fs
		res.Arg = arg
	case vm.Intersect:
		arg := intersect.NewArgument()
		res.Arg = arg
	case vm.Minus: // 2
		arg := minus.NewArgument()
		res.Arg = arg
	case vm.IntersectAll:
		arg := intersectall.NewArgument()
		res.Arg = arg
	case vm.Merge:
		t := sourceIns.Arg.(*merge.Argument)
		arg := merge.NewArgument()
		arg.SinkScan = t.SinkScan
		res.Arg = arg
	case vm.MergeRecursive:
		res.Arg = mergerecursive.NewArgument()
	case vm.MergeCTE:
		res.Arg = mergecte.NewArgument()
	case vm.MergeGroup:
		t := sourceIns.Arg.(*mergegroup.Argument)
		arg := mergegroup.NewArgument()
		arg.NeedEval = t.NeedEval
		arg.PartialResults = t.PartialResults
		arg.PartialResultTypes = t.PartialResultTypes
		res.Arg = arg
	case vm.MergeLimit:
		t := sourceIns.Arg.(*mergelimit.Argument)
		arg := mergelimit.NewArgument()
		arg.Limit = t.Limit
		res.Arg = arg
	case vm.MergeOffset:
		t := sourceIns.Arg.(*mergeoffset.Argument)
		arg := mergeoffset.NewArgument()
		arg.Offset = t.Offset
		res.Arg = arg
	case vm.MergeTop:
		t := sourceIns.Arg.(*mergetop.Argument)
		arg := mergetop.NewArgument()
		arg.Limit = t.Limit
		arg.Fs = t.Fs
		res.Arg = arg
	case vm.MergeOrder:
		t := sourceIns.Arg.(*mergeorder.Argument)
		arg := mergeorder.NewArgument()
		arg.OrderBySpecs = t.OrderBySpecs
		res.Arg = arg
	case vm.Mark:
		t := sourceIns.Arg.(*mark.Argument)
		arg := mark.NewArgument()
		arg.Result = t.Result
		arg.Conditions = t.Conditions
		arg.Typs = t.Typs
		arg.Cond = t.Cond
		arg.OnList = t.OnList
		arg.HashOnPK = t.HashOnPK
		res.Arg = arg
	case vm.TableFunction:
		t := sourceIns.Arg.(*table_function.Argument)
		arg := table_function.NewArgument()
		arg.FuncName = t.FuncName
		arg.Args = t.Args
		arg.Rets = t.Rets
		arg.Attrs = t.Attrs
		arg.Params = t.Params
		res.Arg = arg
	case vm.External:
		t := sourceIns.Arg.(*external.Argument)
		res.Arg = external.NewArgument().WithEs(
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
	case vm.Source:
		t := sourceIns.Arg.(*source.Argument)
		arg := source.NewArgument()
		arg.TblDef = t.TblDef
		arg.Limit = t.Limit
		arg.Offset = t.Offset
		arg.Configs = t.Configs
		res.Arg = arg
	case vm.Connector:
		ok := false
		if regMap != nil {
			arg := connector.NewArgument()
			sourceReg := sourceIns.Arg.(*connector.Argument).Reg
			if arg.Reg, ok = regMap[sourceReg]; !ok {
				panic("nonexistent wait register")
			}
			res.Arg = arg
		}
	case vm.Shuffle:
		sourceArg := sourceIns.Arg.(*shuffle.Argument)
		arg := shuffle.NewArgument()
		arg.ShuffleType = sourceArg.ShuffleType
		arg.ShuffleColIdx = sourceArg.ShuffleColIdx
		arg.ShuffleColMax = sourceArg.ShuffleColMax
		arg.ShuffleColMin = sourceArg.ShuffleColMin
		arg.AliveRegCnt = sourceArg.AliveRegCnt
		arg.ShuffleRangeInt64 = sourceArg.ShuffleRangeInt64
		arg.ShuffleRangeUint64 = sourceArg.ShuffleRangeUint64
		arg.RuntimeFilterSpec = plan2.DeepCopyRuntimeFilterSpec(sourceArg.RuntimeFilterSpec)
		res.Arg = arg
	case vm.Dispatch:
		ok := false
		if regMap != nil {
			sourceArg := sourceIns.Arg.(*dispatch.Argument)
			arg := dispatch.NewArgument()
			arg.IsSink = sourceArg.IsSink
			arg.RecSink = sourceArg.RecSink
			arg.FuncId = sourceArg.FuncId
			arg.LocalRegs = make([]*process.WaitRegister, len(sourceArg.LocalRegs))
			arg.RemoteRegs = make([]colexec.ReceiveInfo, len(sourceArg.RemoteRegs))
			for j := range arg.LocalRegs {
				sourceReg := sourceArg.LocalRegs[j]
				if arg.LocalRegs[j], ok = regMap[sourceReg]; !ok {
					panic("nonexistent wait register")
				}
			}
			for j := range arg.RemoteRegs {
				arg.RemoteRegs[j] = sourceArg.RemoteRegs[j]
			}
			res.Arg = arg
		}
	case vm.Insert:
		t := sourceIns.Arg.(*insert.Argument)
		arg := insert.NewArgument()
		arg.InsertCtx = t.InsertCtx
		arg.ToWriteS3 = t.ToWriteS3
		res.Arg = arg
	case vm.PreInsert:
		t := sourceIns.Arg.(*preinsert.Argument)
		arg := preinsert.NewArgument()
		arg.SchemaName = t.SchemaName
		arg.TableDef = t.TableDef
		arg.Attrs = t.Attrs
		arg.IsUpdate = t.IsUpdate
		arg.HasAutoCol = t.HasAutoCol
		arg.EstimatedRowCount = t.EstimatedRowCount
		res.Arg = arg
	case vm.Deletion:
		t := sourceIns.Arg.(*deletion.Argument)
		arg := deletion.NewArgument()
		arg.IBucket = t.IBucket
		arg.Nbucket = t.Nbucket
		arg.DeleteCtx = t.DeleteCtx
		arg.RemoteDelete = t.RemoteDelete
		arg.SegmentMap = t.SegmentMap
		res.Arg = arg
	case vm.LockOp:
		t := sourceIns.Arg.(*lockop.Argument)
		arg := lockop.NewArgument()
		*arg = *t
		res.Arg = arg
	case vm.FuzzyFilter:
		t := sourceIns.Arg.(*fuzzyfilter.Argument)
		arg := fuzzyfilter.NewArgument()
		arg.N = t.N
		arg.PkName = t.PkName
		arg.PkTyp = t.PkTyp
		res.Arg = arg
	default:
		panic(fmt.Sprintf("unexpected instruction type '%d' to dup", sourceIns.Op))
	}
	return res
}

func constructRestrict(n *plan.Node, filterExpr *plan2.Expr) *restrict.Argument {
	arg := restrict.NewArgument()
	arg.E = filterExpr
	arg.IsEnd = n.IsEnd
	return arg
}

func constructDeletion(n *plan.Node, eg engine.Engine) (*deletion.Argument, error) {
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

	arg := deletion.NewArgument()
	arg.DeleteCtx = delCtx
	return arg, nil
}

func constructOnduplicateKey(n *plan.Node, eg engine.Engine) *onduplicatekey.Argument {
	oldCtx := n.OnDuplicateKey
	arg := onduplicatekey.NewArgument()
	arg.Engine = eg
	arg.OnDuplicateIdx = oldCtx.OnDuplicateIdx
	arg.OnDuplicateExpr = oldCtx.OnDuplicateExpr
	arg.Attrs = oldCtx.Attrs
	arg.InsertColCount = oldCtx.InsertColCount
	arg.UniqueCols = oldCtx.UniqueCols
	arg.UniqueColCheckExpr = oldCtx.UniqueColCheckExpr
	arg.IsIgnore = oldCtx.IsIgnore
	return arg
}

func constructFuzzyFilter(n, right *plan.Node) *fuzzyfilter.Argument {
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

	arg := fuzzyfilter.NewArgument()
	arg.PkName = pkName
	arg.PkTyp = pkTyp
	arg.N = right.Stats.Outcnt
	if len(n.RuntimeFilterBuildList) > 0 {
		arg.RuntimeFilterSpec = n.RuntimeFilterBuildList[0]
	}
	return arg
}

func constructPreInsert(ns []*plan.Node, n *plan.Node, eg engine.Engine, proc *process.Process) (*preinsert.Argument, error) {
	preCtx := n.PreInsertCtx
	schemaName := preCtx.Ref.SchemaName

	//var attrs []string
	attrs := make([]string, 0)
	for _, col := range preCtx.TableDef.Cols {
		if col.Hidden && col.Name != catalog.FakePrimaryKeyColName {
			continue
		}
		attrs = append(attrs, col.Name)
	}

	ctx := proc.Ctx
	txnOp := proc.TxnOperator
	if n.ScanSnapshot != nil && n.ScanSnapshot.TS != nil {
		if !n.ScanSnapshot.TS.Equal(timestamp.Timestamp{LogicalTime: 0, PhysicalTime: 0}) &&
			n.ScanSnapshot.TS.Less(proc.TxnOperator.Txn().SnapshotTS) {
			txnOp = proc.TxnOperator.CloneSnapshotOp(*n.ScanSnapshot.TS)

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

	arg := preinsert.NewArgument()
	arg.Ctx = proc.Ctx
	arg.HasAutoCol = preCtx.HasAutoCol
	arg.SchemaName = schemaName
	arg.TableDef = preCtx.TableDef
	arg.Attrs = attrs
	arg.IsUpdate = preCtx.IsUpdate
	arg.EstimatedRowCount = int64(ns[n.Children[0]].Stats.Outcnt)

	return arg, nil
}

func constructPreInsertUk(n *plan.Node, proc *process.Process) (*preinsertunique.Argument, error) {
	preCtx := n.PreInsertUkCtx
	arg := preinsertunique.NewArgument()
	arg.Ctx = proc.Ctx
	arg.PreInsertCtx = preCtx
	return arg, nil
}

func constructPreInsertSk(n *plan.Node, proc *process.Process) (*preinsertsecondaryindex.Argument, error) {
	arg := preinsertsecondaryindex.NewArgument()
	arg.Ctx = proc.Ctx
	arg.PreInsertCtx = n.PreInsertSkCtx
	return arg, nil
}

func constructLockOp(n *plan.Node, eng engine.Engine) (*lockop.Argument, error) {
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

func constructInsert(n *plan.Node, eg engine.Engine) (*insert.Argument, error) {
	oldCtx := n.InsertCtx
	var attrs []string
	for _, col := range oldCtx.TableDef.Cols {
		if col.Name != catalog.Row_ID {
			attrs = append(attrs, col.Name)
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
	return arg, nil
}

func constructProjection(n *plan.Node) *projection.Argument {
	arg := projection.NewArgument()
	arg.Es = n.ProjectList
	return arg
}

func constructExternal(n *plan.Node, param *tree.ExternParam, ctx context.Context, fileList []string, FileSize []int64, fileOffset []*pipeline.FileOffset) *external.Argument {
	attrs := make([]string, len(n.TableDef.Cols))
	for j, col := range n.TableDef.Cols {
		attrs[j] = col.Name
	}
	return external.NewArgument().WithEs(
		&external.ExternalParam{
			ExParamConst: external.ExParamConst{
				Attrs:           attrs,
				Cols:            n.TableDef.Cols,
				Extern:          param,
				Name2ColIndex:   n.TableDef.Name2ColIndex,
				FileOffsetTotal: fileOffset,
				CreateSql:       n.TableDef.Createsql,
				Ctx:             ctx,
				FileList:        fileList,
				FileSize:        FileSize,
				ClusterTable:    n.GetClusterTable(),
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

func constructStream(n *plan.Node, p [2]int64) *source.Argument {
	arg := source.NewArgument()
	arg.TblDef = n.TableDef
	arg.Offset = p[0]
	arg.Limit = p[1]
	return arg
}

func constructTableFunction(n *plan.Node) *table_function.Argument {
	attrs := make([]string, len(n.TableDef.Cols))
	for j, col := range n.TableDef.Cols {
		attrs[j] = col.Name
	}
	arg := table_function.NewArgument()
	arg.Attrs = attrs
	arg.Rets = n.TableDef.Cols
	arg.Args = n.TblFuncExprList
	arg.FuncName = n.TableDef.TblFunc.Name
	arg.Params = n.TableDef.TblFunc.Param
	return arg
}

func constructTop(n *plan.Node, topN *plan.Expr) *top.Argument {
	arg := top.NewArgument()
	arg.Fs = n.OrderBy
	arg.Limit = topN
	if len(n.SendMsgList) > 0 {
		arg.TopValueTag = n.SendMsgList[0].MsgTag
	}
	return arg
}

func constructJoin(n *plan.Node, typs []types.Type, proc *process.Process) *join.Argument {
	result := make([]colexec.ResultPos, len(n.ProjectList))
	for i, expr := range n.ProjectList {
		result[i].Rel, result[i].Pos = constructJoinResult(expr, proc)
	}
	cond, conds := extraJoinConditions(n.OnList)

	arg := join.NewArgument()
	arg.Typs = typs
	arg.Result = result
	arg.Cond = cond
	arg.Conditions = constructJoinConditions(conds, proc)
	arg.RuntimeFilterSpecs = n.RuntimeFilterBuildList
	arg.HashOnPK = n.Stats.HashmapStats != nil && n.Stats.HashmapStats.HashOnPK
	arg.IsShuffle = n.Stats.HashmapStats != nil && n.Stats.HashmapStats.Shuffle
	return arg
}

func constructSemi(n *plan.Node, typs []types.Type, proc *process.Process) *semi.Argument {
	result := make([]int32, len(n.ProjectList))
	for i, expr := range n.ProjectList {
		rel, pos := constructJoinResult(expr, proc)
		if rel != 0 {
			panic(moerr.NewNYI(proc.Ctx, "semi result '%s'", expr))
		}
		result[i] = pos
	}
	cond, conds := extraJoinConditions(n.OnList)
	arg := semi.NewArgument()
	arg.Typs = typs
	arg.Result = result
	arg.Cond = cond
	arg.Conditions = constructJoinConditions(conds, proc)
	arg.RuntimeFilterSpecs = n.RuntimeFilterBuildList
	arg.HashOnPK = n.Stats.HashmapStats != nil && n.Stats.HashmapStats.HashOnPK
	arg.IsShuffle = n.Stats.HashmapStats != nil && n.Stats.HashmapStats.Shuffle
	return arg
}

func constructLeft(n *plan.Node, typs []types.Type, proc *process.Process) *left.Argument {
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
	return arg
}

func constructRight(n *plan.Node, left_typs, right_typs []types.Type, proc *process.Process) *right.Argument {
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
	return arg
}

func constructRightSemi(n *plan.Node, right_typs []types.Type, proc *process.Process) *rightsemi.Argument {
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
	return arg
}

func constructRightAnti(n *plan.Node, right_typs []types.Type, proc *process.Process) *rightanti.Argument {
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
	return arg
}

func constructSingle(n *plan.Node, typs []types.Type, proc *process.Process) *single.Argument {
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
	return arg
}

func constructProduct(n *plan.Node, typs []types.Type, proc *process.Process) *product.Argument {
	result := make([]colexec.ResultPos, len(n.ProjectList))
	for i, expr := range n.ProjectList {
		result[i].Rel, result[i].Pos = constructJoinResult(expr, proc)
	}
	arg := product.NewArgument()
	arg.Typs = typs
	arg.Result = result
	return arg
}

func constructAnti(n *plan.Node, typs []types.Type, proc *process.Process) *anti.Argument {
	result := make([]int32, len(n.ProjectList))
	for i, expr := range n.ProjectList {
		rel, pos := constructJoinResult(expr, proc)
		if rel != 0 {
			panic(moerr.NewNYI(proc.Ctx, "anti result '%s'", expr))
		}
		result[i] = pos
	}
	cond, conds := extraJoinConditions(n.OnList)
	arg := anti.NewArgument()
	arg.Typs = typs
	arg.Result = result
	arg.Cond = cond
	arg.Conditions = constructJoinConditions(conds, proc)
	arg.HashOnPK = n.Stats.HashmapStats != nil && n.Stats.HashmapStats.HashOnPK
	arg.IsShuffle = n.Stats.HashmapStats != nil && n.Stats.HashmapStats.Shuffle
	arg.RuntimeFilterSpecs = n.RuntimeFilterBuildList
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
			panic(moerr.NewNYI(proc.Ctx, "loop mark result '%s'", expr))
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

func constructOrder(n *plan.Node) *order.Argument {
	arg := order.NewArgument()
	arg.OrderBySpec = n.OrderBy
	return arg
}

func constructFill(n *plan.Node) *fill.Argument {
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

func constructTimeWindow(_ context.Context, n *plan.Node) *timewin.Argument {
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

func constructWindow(_ context.Context, n *plan.Node, proc *process.Process) *window.Argument {
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
				cfg = []byte(vec.UnsafeGetStringAt(0))
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

func constructLimit(n *plan.Node) *limit.Argument {
	arg := limit.NewArgument()
	arg.LimitExpr = plan2.DeepCopyExpr(n.Limit)
	return arg
}

func constructSample(n *plan.Node, outputRowCount bool) *sample.Argument {
	if n.SampleFunc.Rows != plan2.NotSampleByRows {
		return sample.NewSampleByRows(int(n.SampleFunc.Rows), n.AggList, n.GroupBy, n.SampleFunc.UsingRow, outputRowCount)
	}
	if n.SampleFunc.Percent != plan2.NotSampleByPercents {
		return sample.NewSampleByPercent(n.SampleFunc.Percent, n.AggList, n.GroupBy)
	}
	panic("only support sample by rows / percent now.")
}

func constructGroup(_ context.Context, n, cn *plan.Node, needEval bool, shuffleDop int, proc *process.Process) *group.Argument {
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
					cfg = []byte(vec.UnsafeGetStringAt(0))
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

	shuffleGroup := false
	var preAllocSize uint64 = 0
	if n.Stats != nil && n.Stats.HashmapStats != nil && n.Stats.HashmapStats.Shuffle {
		shuffleGroup = true
		if cn.NodeType == plan.Node_TABLE_SCAN && len(cn.FilterList) == 0 {
			// if group on scan without filter, stats for hashmap is accurate to do preAlloc
			// tune it up a little bit in case it is not so average after shuffle
			preAllocSize = uint64(n.Stats.HashmapStats.HashmapSize / float64(shuffleDop) * 1.05)
		}
	}

	arg := group.NewArgument()
	arg.Aggs = aggregationExpressions
	arg.Types = typs
	arg.NeedEval = needEval
	arg.Exprs = n.GroupBy
	arg.IsShuffle = shuffleGroup
	arg.PreAllocSize = preAllocSize
	return arg
}

func constructDispatchLocal(all bool, isSink, RecSink bool, regs []*process.WaitRegister) *dispatch.Argument {
	arg := dispatch.NewArgument()
	arg.LocalRegs = regs
	arg.IsSink = isSink
	arg.RecSink = RecSink
	if all {
		arg.FuncId = dispatch.SendToAllLocalFunc
	} else {
		arg.FuncId = dispatch.SendToAnyLocalFunc
	}
	return arg
}

// ss[currentIdx] means it's local scope the dispatch rule should be like below:
// dispatch batch to all other cn and also put one into proc.MergeReciever[0] for
// local deletion
func constructDeleteDispatchAndLocal(
	currentIdx int,
	rs []*Scope,
	ss []*Scope,
	uuids []uuid.UUID,
	c *Compile) {
	arg := dispatch.NewArgument()
	arg.RemoteRegs = make([]colexec.ReceiveInfo, 0, len(ss)-1)
	// rs is used to get batch from dispatch operator (include
	// local batch)
	rs[currentIdx].NodeInfo = ss[currentIdx].NodeInfo
	rs[currentIdx].Magic = Remote
	rs[currentIdx].PreScopes = append(rs[currentIdx].PreScopes, ss[currentIdx])
	rs[currentIdx].Proc = process.NewWithAnalyze(c.proc, c.ctx, len(ss), c.anal.analInfos)
	rs[currentIdx].RemoteReceivRegInfos = make([]RemoteReceivRegInfo, 0, len(ss)-1)

	// use arg.RemoteRegs to know the uuid,
	// use this uuid to register Server.uuidCsChanMap (uuid,proc.DispatchNotifyCh),
	// So how to use this?
	// the answer is below:
	// when the remote Cn run the scope, if scope's RemoteReceivRegInfos
	// is not empty, it will start to give a PrepareDoneNotifyMessage to
	// tell the dispatcher it's prepared, and also to know,this messgae
	// will carry the uuid and a clientSession. In dispatch instruction,
	// first it will use the uuid to get the proc.DispatchNotifyCh from the Server.
	// (remember the DispatchNotifyCh is in a process,not a global one,because we
	// need to send the WrapCs (a struct,contains clientSession,uuid and So on) in the
	// sepcified process).
	// And then Dispatcher will use this clientSession to dispatch batches to remoteCN.
	// When remoteCn get the batches, it should know send it to where by itself.
	for i := 0; i < len(ss); i++ {
		if i != currentIdx {
			// just use this uuid in dispatch, we need to
			// use it in the prepare func (store the map [uuid -> proc.DispatchNotifyCh])
			arg.RemoteRegs = append(
				arg.RemoteRegs,
				colexec.ReceiveInfo{
					Uuid:     uuids[i],
					NodeAddr: ss[i].NodeInfo.Addr,
				})
			// let remote scope knows it need to recieve bacth from
			// remote CN, it will use this to send PrepareDoneNotifyMessage
			// and then to recieve batches from remote CNs
			rs[currentIdx].RemoteReceivRegInfos = append(
				rs[currentIdx].RemoteReceivRegInfos,
				RemoteReceivRegInfo{
					Uuid: uuids[currentIdx],
					// I use i to tag, scope should send the batches (recieved from remote CNs)
					// to process.MergeRecievers[i]
					Idx:      i,
					FromAddr: ss[i].NodeInfo.Addr,
				})
		}
	}
	if len(arg.RemoteRegs) == 0 {
		arg.FuncId = dispatch.SendToAllLocalFunc
	} else {
		arg.FuncId = dispatch.SendToAllFunc
	}

	arg.LocalRegs = append(
		arg.LocalRegs,
		rs[currentIdx].Proc.Reg.MergeReceivers[currentIdx])

	ss[currentIdx].appendInstruction(vm.Instruction{
		Op:  vm.Dispatch,
		Arg: arg,
	})
	// add merge to recieve all batches
	rs[currentIdx].appendInstruction(vm.Instruction{
		Op:  vm.Merge,
		Arg: merge.NewArgument(),
	})
}

// This function do not setting funcId.
// PLEASE SETTING FuncId AFTER YOU CALL IT.
func constructDispatchLocalAndRemote(idx int, ss []*Scope, currentCNAddr string) (bool, *dispatch.Argument) {
	arg := dispatch.NewArgument()
	scopeLen := len(ss)
	arg.LocalRegs = make([]*process.WaitRegister, 0, scopeLen)
	arg.RemoteRegs = make([]colexec.ReceiveInfo, 0, scopeLen)
	arg.ShuffleRegIdxLocal = make([]int, 0, len(ss))
	arg.ShuffleRegIdxRemote = make([]int, 0, len(ss))
	hasRemote := false
	for i, s := range ss {
		if s.IsEnd {
			continue
		}
		if len(s.NodeInfo.Addr) == 0 || len(currentCNAddr) == 0 ||
			isSameCN(s.NodeInfo.Addr, currentCNAddr) {
			// Local reg.
			// Put them into arg.LocalRegs
			arg.LocalRegs = append(arg.LocalRegs, s.Proc.Reg.MergeReceivers[idx])
			arg.ShuffleRegIdxLocal = append(arg.ShuffleRegIdxLocal, i)
		} else {
			// Remote reg.
			// Generate uuid for them and put into arg.RemoteRegs & scope. receive info
			hasRemote = true
			newUuid, _ := uuid.NewV7()

			arg.RemoteRegs = append(arg.RemoteRegs, colexec.ReceiveInfo{
				Uuid:     newUuid,
				NodeAddr: s.NodeInfo.Addr,
			})
			arg.ShuffleRegIdxRemote = append(arg.ShuffleRegIdxRemote, i)
			s.RemoteReceivRegInfos = append(s.RemoteReceivRegInfos, RemoteReceivRegInfo{
				Idx:      idx,
				Uuid:     newUuid,
				FromAddr: currentCNAddr,
			})
		}
	}
	return hasRemote, arg
}

func constructShuffleJoinArg(ss []*Scope, node *plan.Node, left bool) *shuffle.Argument {
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
	case types.T_uint64, types.T_uint32, types.T_uint16, types.T_varchar, types.T_char, types.T_text, types.T_bit:
		arg.ShuffleRangeUint64 = plan2.ShuffleRangeReEvalUnsigned(node.Stats.HashmapStats.Ranges, int(arg.AliveRegCnt), node.Stats.HashmapStats.Nullcnt, int64(node.Stats.TableCnt))
	}
	if left && len(node.RuntimeFilterProbeList) > 0 {
		arg.RuntimeFilterSpec = plan2.DeepCopyRuntimeFilterSpec(node.RuntimeFilterProbeList[0])
	}
	return arg
}

func constructShuffleGroupArg(ss []*Scope, node *plan.Node) *shuffle.Argument {
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
	case types.T_uint64, types.T_uint32, types.T_uint16, types.T_varchar, types.T_char, types.T_text, types.T_bit:
		arg.ShuffleRangeUint64 = plan2.ShuffleRangeReEvalUnsigned(node.Stats.HashmapStats.Ranges, int(arg.AliveRegCnt), node.Stats.HashmapStats.Nullcnt, int64(node.Stats.TableCnt))
	}
	return arg
}

// cross-cn dispath  will send same batch to all register
func constructDispatch(idx int, ss []*Scope, currentCNAddr string, node *plan.Node, left bool) *dispatch.Argument {
	hasRemote, arg := constructDispatchLocalAndRemote(idx, ss, currentCNAddr)
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

func constructMergeGroup(needEval bool) *mergegroup.Argument {
	arg := mergegroup.NewArgument()
	arg.NeedEval = needEval
	return arg
}

func constructMergeTop(n *plan.Node, topN *plan.Expr) *mergetop.Argument {
	arg := mergetop.NewArgument()
	arg.Fs = n.OrderBy
	arg.Limit = topN
	return arg
}

func constructMergeOffset(n *plan.Node) *mergeoffset.Argument {
	arg := mergeoffset.NewArgument().WithOffset(n.Offset)
	return arg
}

func constructMergeLimit(n *plan.Node) *mergelimit.Argument {
	arg := mergelimit.NewArgument().WithLimit(n.Limit)
	return arg
}

func constructMergeOrder(n *plan.Node) *mergeorder.Argument {
	arg := mergeorder.NewArgument()
	arg.OrderBySpecs = n.OrderBy
	return arg
}

func constructPartition(n *plan.Node) *partition.Argument {
	arg := partition.NewArgument()
	arg.OrderBySpecs = n.OrderBy
	return arg
}

func constructIndexJoin(n *plan.Node, typs []types.Type, proc *process.Process) *indexjoin.Argument {
	result := make([]int32, len(n.ProjectList))
	for i, expr := range n.ProjectList {
		rel, pos := constructJoinResult(expr, proc)
		if rel != 0 {
			panic(moerr.NewNYI(proc.Ctx, "loop semi result '%s'", expr))
		}
		result[i] = pos
	}
	arg := indexjoin.NewArgument()
	arg.Typs = typs
	arg.Result = result
	arg.RuntimeFilterSpecs = n.RuntimeFilterBuildList
	return arg
}

func constructProductL2(n *plan.Node, typs []types.Type, proc *process.Process) *productl2.Argument {
	result := make([]colexec.ResultPos, len(n.ProjectList))
	for i, expr := range n.ProjectList {
		result[i].Rel, result[i].Pos = constructJoinResult(expr, proc)
	}
	arg := productl2.NewArgument()
	arg.Typs = typs
	arg.Result = result
	arg.OnExpr = colexec.RewriteFilterExprList(n.OnList)
	return arg
}

func constructLoopJoin(n *plan.Node, typs []types.Type, proc *process.Process) *loopjoin.Argument {
	result := make([]colexec.ResultPos, len(n.ProjectList))
	for i, expr := range n.ProjectList {
		result[i].Rel, result[i].Pos = constructJoinResult(expr, proc)
	}
	arg := loopjoin.NewArgument()
	arg.Typs = typs
	arg.Result = result
	arg.Cond = colexec.RewriteFilterExprList(n.OnList)
	return arg
}

func constructLoopSemi(n *plan.Node, typs []types.Type, proc *process.Process) *loopsemi.Argument {
	result := make([]int32, len(n.ProjectList))
	for i, expr := range n.ProjectList {
		rel, pos := constructJoinResult(expr, proc)
		if rel != 0 {
			panic(moerr.NewNYI(proc.Ctx, "loop semi result '%s'", expr))
		}
		result[i] = pos
	}
	arg := loopsemi.NewArgument()
	arg.Typs = typs
	arg.Result = result
	arg.Cond = colexec.RewriteFilterExprList(n.OnList)
	return arg
}

func constructLoopLeft(n *plan.Node, typs []types.Type, proc *process.Process) *loopleft.Argument {
	result := make([]colexec.ResultPos, len(n.ProjectList))
	for i, expr := range n.ProjectList {
		result[i].Rel, result[i].Pos = constructJoinResult(expr, proc)
	}
	arg := loopleft.NewArgument()
	arg.Typs = typs
	arg.Result = result
	arg.Cond = colexec.RewriteFilterExprList(n.OnList)
	return arg
}

func constructLoopSingle(n *plan.Node, typs []types.Type, proc *process.Process) *loopsingle.Argument {
	result := make([]colexec.ResultPos, len(n.ProjectList))
	for i, expr := range n.ProjectList {
		result[i].Rel, result[i].Pos = constructJoinResult(expr, proc)
	}
	arg := loopsingle.NewArgument()
	arg.Typs = typs
	arg.Result = result
	arg.Cond = colexec.RewriteFilterExprList(n.OnList)
	return arg
}

func constructLoopAnti(n *plan.Node, typs []types.Type, proc *process.Process) *loopanti.Argument {
	result := make([]int32, len(n.ProjectList))
	for i, expr := range n.ProjectList {
		rel, pos := constructJoinResult(expr, proc)
		if rel != 0 {
			panic(moerr.NewNYI(proc.Ctx, "loop anti result '%s'", expr))
		}
		result[i] = pos
	}
	arg := loopanti.NewArgument()
	arg.Typs = typs
	arg.Result = result
	arg.Cond = colexec.RewriteFilterExprList(n.OnList)
	return arg
}

func constructLoopMark(n *plan.Node, typs []types.Type, proc *process.Process) *loopmark.Argument {
	result := make([]int32, len(n.ProjectList))
	for i, expr := range n.ProjectList {
		rel, pos := constructJoinResult(expr, proc)
		if rel == 0 {
			result[i] = pos
		} else if rel == -1 {
			result[i] = -1
		} else {
			panic(moerr.NewNYI(proc.Ctx, "loop mark result '%s'", expr))
		}
	}
	arg := loopmark.NewArgument()
	arg.Typs = typs
	arg.Result = result
	arg.Cond = colexec.RewriteFilterExprList(n.OnList)
	return arg
}

func constructJoinBuildInstruction(c *Compile, in vm.Instruction, isDup bool, isShuffle bool) vm.Instruction {
	switch in.Op {
	case vm.IndexJoin:
		arg := in.Arg.(*indexjoin.Argument)
		ret := indexbuild.NewArgument()
		if len(arg.RuntimeFilterSpecs) > 0 {
			ret.RuntimeFilterSpec = arg.RuntimeFilterSpecs[0]
		}
		return vm.Instruction{
			Op:      vm.IndexBuild,
			Idx:     in.Idx,
			IsFirst: true,
			Arg:     ret,
		}
	default:
		if isShuffle {
			return vm.Instruction{
				Op:      vm.ShuffleBuild,
				Idx:     in.Idx,
				IsFirst: true,
				Arg:     constructShuffleBuild(in, c.proc, isDup),
			}
		}
		return vm.Instruction{
			Op:      vm.HashBuild,
			Idx:     in.Idx,
			IsFirst: true,
			Arg:     constructHashBuild(in, c.proc, isDup),
		}
	}
}

func constructHashBuild(in vm.Instruction, proc *process.Process, isDup bool) *hashbuild.Argument {
	// XXX BUG
	// relation index of arg.Conditions should be rewritten to 0 here.
	ret := hashbuild.NewArgument()

	switch in.Op {
	case vm.Anti:
		arg := in.Arg.(*anti.Argument)
		ret.NeedHashMap = true
		ret.Typs = arg.Typs
		ret.Conditions = arg.Conditions[1]
		ret.IsDup = isDup
		ret.HashOnPK = arg.HashOnPK
		if arg.Cond == nil {
			ret.NeedMergedBatch = false
			ret.NeedAllocateSels = false
		} else {
			ret.NeedMergedBatch = true
			ret.NeedAllocateSels = true
		}

	case vm.Mark:
		arg := in.Arg.(*mark.Argument)
		ret.NeedHashMap = true
		ret.Typs = arg.Typs
		ret.Conditions = arg.Conditions[1]
		ret.IsDup = isDup
		ret.NeedMergedBatch = true
		ret.HashOnPK = arg.HashOnPK
		ret.NeedAllocateSels = true

	case vm.Join:
		arg := in.Arg.(*join.Argument)
		ret.NeedHashMap = true
		ret.Typs = arg.Typs
		ret.Conditions = arg.Conditions[1]
		ret.IsDup = isDup
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
		ret.NeedMergedBatch = needMergedBatch
		ret.NeedAllocateSels = true
		if len(arg.RuntimeFilterSpecs) > 0 {
			ret.RuntimeFilterSpec = arg.RuntimeFilterSpecs[0]
		}

	case vm.Left:
		arg := in.Arg.(*left.Argument)
		ret.NeedHashMap = true
		ret.Typs = arg.Typs
		ret.Conditions = arg.Conditions[1]
		ret.IsDup = isDup
		ret.NeedMergedBatch = true
		ret.HashOnPK = arg.HashOnPK
		ret.NeedAllocateSels = true
		if len(arg.RuntimeFilterSpecs) > 0 {
			ret.RuntimeFilterSpec = arg.RuntimeFilterSpecs[0]
		}

	case vm.Right:
		arg := in.Arg.(*right.Argument)
		ret.NeedHashMap = true
		ret.Typs = arg.RightTypes
		ret.Conditions = arg.Conditions[1]
		ret.IsDup = isDup
		ret.NeedMergedBatch = true
		ret.HashOnPK = arg.HashOnPK
		ret.NeedAllocateSels = true
		if len(arg.RuntimeFilterSpecs) > 0 {
			ret.RuntimeFilterSpec = arg.RuntimeFilterSpecs[0]
		}

	case vm.RightSemi:
		arg := in.Arg.(*rightsemi.Argument)
		ret.NeedHashMap = true
		ret.Typs = arg.RightTypes
		ret.Conditions = arg.Conditions[1]
		ret.IsDup = isDup
		ret.NeedMergedBatch = true
		ret.HashOnPK = arg.HashOnPK
		ret.NeedAllocateSels = true
		if len(arg.RuntimeFilterSpecs) > 0 {
			ret.RuntimeFilterSpec = arg.RuntimeFilterSpecs[0]
		}

	case vm.RightAnti:
		arg := in.Arg.(*rightanti.Argument)
		ret.NeedHashMap = true
		ret.Typs = arg.RightTypes
		ret.Conditions = arg.Conditions[1]
		ret.IsDup = isDup
		ret.NeedMergedBatch = true
		ret.HashOnPK = arg.HashOnPK
		ret.NeedAllocateSels = true
		if len(arg.RuntimeFilterSpecs) > 0 {
			ret.RuntimeFilterSpec = arg.RuntimeFilterSpecs[0]
		}

	case vm.Semi:
		arg := in.Arg.(*semi.Argument)
		ret.NeedHashMap = true
		ret.Typs = arg.Typs
		ret.Conditions = arg.Conditions[1]
		ret.IsDup = isDup
		ret.HashOnPK = arg.HashOnPK
		if arg.Cond == nil {
			ret.NeedMergedBatch = false
			ret.NeedAllocateSels = false
		} else {
			ret.NeedMergedBatch = true
			ret.NeedAllocateSels = true
		}
		if len(arg.RuntimeFilterSpecs) > 0 {
			ret.RuntimeFilterSpec = arg.RuntimeFilterSpecs[0]
		}

	case vm.Single:
		arg := in.Arg.(*single.Argument)
		ret.NeedHashMap = true
		ret.Typs = arg.Typs
		ret.Conditions = arg.Conditions[1]
		ret.IsDup = isDup
		ret.NeedMergedBatch = true
		ret.HashOnPK = arg.HashOnPK
		ret.NeedAllocateSels = true
		if len(arg.RuntimeFilterSpecs) > 0 {
			ret.RuntimeFilterSpec = arg.RuntimeFilterSpecs[0]
		}
	case vm.Product:
		arg := in.Arg.(*product.Argument)
		ret.NeedHashMap = false
		ret.Typs = arg.Typs
		ret.IsDup = isDup
		ret.NeedMergedBatch = true
		ret.NeedAllocateSels = true
	case vm.ProductL2:
		arg := in.Arg.(*productl2.Argument)
		ret.NeedHashMap = false
		ret.Typs = arg.Typs
		ret.IsDup = isDup
		ret.NeedMergedBatch = true
		ret.NeedAllocateSels = true
	case vm.LoopAnti:
		arg := in.Arg.(*loopanti.Argument)
		ret.NeedHashMap = false
		ret.Typs = arg.Typs
		ret.IsDup = isDup
		ret.NeedMergedBatch = true
		ret.NeedAllocateSels = true

	case vm.LoopJoin:
		arg := in.Arg.(*loopjoin.Argument)
		ret.NeedHashMap = false
		ret.Typs = arg.Typs
		ret.IsDup = isDup
		ret.NeedMergedBatch = true
		ret.NeedAllocateSels = true

	case vm.LoopLeft:
		arg := in.Arg.(*loopleft.Argument)
		ret.NeedHashMap = false
		ret.Typs = arg.Typs
		ret.IsDup = isDup
		ret.NeedMergedBatch = true
		ret.NeedAllocateSels = true

	case vm.LoopSemi:
		arg := in.Arg.(*loopsemi.Argument)
		ret.NeedHashMap = false
		ret.Typs = arg.Typs
		ret.IsDup = isDup
		ret.NeedMergedBatch = true
		ret.NeedAllocateSels = true

	case vm.LoopSingle:
		arg := in.Arg.(*loopsingle.Argument)
		ret.NeedHashMap = false
		ret.Typs = arg.Typs
		ret.IsDup = isDup
		ret.NeedMergedBatch = true
		ret.NeedAllocateSels = true

	case vm.LoopMark:
		arg := in.Arg.(*loopmark.Argument)
		ret.NeedHashMap = false
		ret.Typs = arg.Typs
		ret.IsDup = isDup
		ret.NeedMergedBatch = true
		ret.NeedAllocateSels = true

	default:
		ret.Release()
		panic(moerr.NewInternalError(proc.Ctx, "unsupport join type '%v'", in.Op))
	}
	return ret
}

func constructShuffleBuild(in vm.Instruction, proc *process.Process, isDup bool) *shufflebuild.Argument {
	ret := shufflebuild.NewArgument()

	switch in.Op {
	case vm.Anti:
		arg := in.Arg.(*anti.Argument)
		ret.Typs = arg.Typs
		ret.Conditions = arg.Conditions[1]
		ret.IsDup = isDup
		ret.HashOnPK = arg.HashOnPK
		if arg.Cond == nil {
			ret.NeedMergedBatch = false
			ret.NeedAllocateSels = false
		} else {
			ret.NeedMergedBatch = true
			ret.NeedAllocateSels = true
		}
		if len(arg.RuntimeFilterSpecs) > 0 {
			ret.RuntimeFilterSpec = plan2.DeepCopyRuntimeFilterSpec(arg.RuntimeFilterSpecs[0])
		}

	case vm.Join:
		arg := in.Arg.(*join.Argument)
		ret.Typs = arg.Typs
		ret.Conditions = arg.Conditions[1]
		ret.IsDup = isDup
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
		ret.NeedMergedBatch = needMergedBatch
		ret.NeedAllocateSels = true
		if len(arg.RuntimeFilterSpecs) > 0 {
			ret.RuntimeFilterSpec = plan2.DeepCopyRuntimeFilterSpec(arg.RuntimeFilterSpecs[0])
		}

	case vm.Left:
		arg := in.Arg.(*left.Argument)
		ret.Typs = arg.Typs
		ret.Conditions = arg.Conditions[1]
		ret.IsDup = isDup
		ret.NeedMergedBatch = true
		ret.HashOnPK = arg.HashOnPK
		ret.NeedAllocateSels = true
		if len(arg.RuntimeFilterSpecs) > 0 {
			ret.RuntimeFilterSpec = plan2.DeepCopyRuntimeFilterSpec(arg.RuntimeFilterSpecs[0])
		}

	case vm.Right:
		arg := in.Arg.(*right.Argument)
		ret.Typs = arg.RightTypes
		ret.Conditions = arg.Conditions[1]
		ret.IsDup = isDup
		ret.NeedMergedBatch = true
		ret.HashOnPK = arg.HashOnPK
		ret.NeedAllocateSels = true
		if len(arg.RuntimeFilterSpecs) > 0 {
			ret.RuntimeFilterSpec = plan2.DeepCopyRuntimeFilterSpec(arg.RuntimeFilterSpecs[0])
		}

	case vm.RightSemi:
		arg := in.Arg.(*rightsemi.Argument)
		ret.Typs = arg.RightTypes
		ret.Conditions = arg.Conditions[1]
		ret.IsDup = isDup
		ret.NeedMergedBatch = true
		ret.HashOnPK = arg.HashOnPK
		ret.NeedAllocateSels = true
		if len(arg.RuntimeFilterSpecs) > 0 {
			ret.RuntimeFilterSpec = plan2.DeepCopyRuntimeFilterSpec(arg.RuntimeFilterSpecs[0])
		}

	case vm.RightAnti:
		arg := in.Arg.(*rightanti.Argument)
		ret.Typs = arg.RightTypes
		ret.Conditions = arg.Conditions[1]
		ret.IsDup = isDup
		ret.NeedMergedBatch = true
		ret.HashOnPK = arg.HashOnPK
		ret.NeedAllocateSels = true
		if len(arg.RuntimeFilterSpecs) > 0 {
			ret.RuntimeFilterSpec = plan2.DeepCopyRuntimeFilterSpec(arg.RuntimeFilterSpecs[0])
		}

	case vm.Semi:
		arg := in.Arg.(*semi.Argument)
		ret.Typs = arg.Typs
		ret.Conditions = arg.Conditions[1]
		ret.IsDup = isDup
		ret.HashOnPK = arg.HashOnPK
		if arg.Cond == nil {
			ret.NeedMergedBatch = false
			ret.NeedAllocateSels = false
		} else {
			ret.NeedMergedBatch = true
			ret.NeedAllocateSels = true
		}
		if len(arg.RuntimeFilterSpecs) > 0 {
			ret.RuntimeFilterSpec = plan2.DeepCopyRuntimeFilterSpec(arg.RuntimeFilterSpecs[0])
		}

	default:
		ret.Release()
		panic(moerr.NewInternalError(proc.Ctx, "unsupported type for shuffle join: '%v'", in.Op))
	}
	return ret
}

func constructJoinResult(expr *plan.Expr, proc *process.Process) (int32, int32) {
	e, ok := expr.Expr.(*plan.Expr_Col)
	if !ok {
		panic(moerr.NewNYI(proc.Ctx, "join result '%s'", expr))
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
			panic(moerr.NewNYI(proc.Ctx, "join condition '%s'", expr))
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
		panic(moerr.NewNYI(proc.Ctx, "join condition '%s'", expr))
	}
	if exprRelPos(e.F.Args[0]) == 1 {
		return e.F.Args[1], e.F.Args[0]
	}
	return e.F.Args[0], e.F.Args[1]
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

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

	"github.com/google/uuid"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/pipeline"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/agg"
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
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/stream"
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
	res := vm.Instruction{Op: sourceIns.Op, Idx: sourceIns.Idx, IsFirst: sourceIns.IsFirst, IsLast: sourceIns.IsLast}
	switch sourceIns.Op {
	case vm.Anti:
		t := sourceIns.Arg.(*anti.Argument)
		res.Arg = &anti.Argument{
			Ibucket:    t.Ibucket,
			Nbucket:    t.Nbucket,
			Cond:       t.Cond,
			Typs:       t.Typs,
			Conditions: t.Conditions,
			Result:     t.Result,
			HashOnPK:   t.HashOnPK,
			IsShuffle:  t.IsShuffle,
		}
	case vm.Group:
		t := sourceIns.Arg.(*group.Argument)
		res.Arg = &group.Argument{
			IsShuffle:    t.IsShuffle,
			PreAllocSize: t.PreAllocSize,
			NeedEval:     t.NeedEval,
			Ibucket:      t.Ibucket,
			Nbucket:      t.Nbucket,
			Exprs:        t.Exprs,
			Types:        t.Types,
			Aggs:         t.Aggs,
			MultiAggs:    t.MultiAggs,
		}
	case vm.Sample:
		t := sourceIns.Arg.(*sample.Argument)
		res.Arg = t.SimpleDup()
	case vm.Join:
		t := sourceIns.Arg.(*join.Argument)
		res.Arg = &join.Argument{
			Ibucket:            t.Ibucket,
			Nbucket:            t.Nbucket,
			Result:             t.Result,
			Cond:               t.Cond,
			Typs:               t.Typs,
			Conditions:         t.Conditions,
			RuntimeFilterSpecs: t.RuntimeFilterSpecs,
			HashOnPK:           t.HashOnPK,
			IsShuffle:          t.IsShuffle,
		}
	case vm.Left:
		t := sourceIns.Arg.(*left.Argument)
		res.Arg = &left.Argument{
			Ibucket:            t.Ibucket,
			Nbucket:            t.Nbucket,
			Cond:               t.Cond,
			Result:             t.Result,
			Typs:               t.Typs,
			Conditions:         t.Conditions,
			RuntimeFilterSpecs: t.RuntimeFilterSpecs,
			HashOnPK:           t.HashOnPK,
			IsShuffle:          t.IsShuffle,
		}
	case vm.Right:
		t := sourceIns.Arg.(*right.Argument)
		res.Arg = &right.Argument{
			Ibucket:            t.Ibucket,
			Nbucket:            t.Nbucket,
			Cond:               t.Cond,
			Result:             t.Result,
			RightTypes:         t.RightTypes,
			LeftTypes:          t.LeftTypes,
			Conditions:         t.Conditions,
			RuntimeFilterSpecs: t.RuntimeFilterSpecs,
			HashOnPK:           t.HashOnPK,
			IsShuffle:          t.IsShuffle,
		}
	case vm.RightSemi:
		t := sourceIns.Arg.(*rightsemi.Argument)
		res.Arg = &rightsemi.Argument{
			Ibucket:            t.Ibucket,
			Nbucket:            t.Nbucket,
			Cond:               t.Cond,
			Result:             t.Result,
			RightTypes:         t.RightTypes,
			Conditions:         t.Conditions,
			RuntimeFilterSpecs: t.RuntimeFilterSpecs,
			HashOnPK:           t.HashOnPK,
			IsShuffle:          t.IsShuffle,
		}
	case vm.RightAnti:
		t := sourceIns.Arg.(*rightanti.Argument)
		res.Arg = &rightanti.Argument{
			Ibucket:            t.Ibucket,
			Nbucket:            t.Nbucket,
			Cond:               t.Cond,
			Result:             t.Result,
			RightTypes:         t.RightTypes,
			Conditions:         t.Conditions,
			RuntimeFilterSpecs: t.RuntimeFilterSpecs,
			HashOnPK:           t.HashOnPK,
			IsShuffle:          t.IsShuffle,
		}
	case vm.Limit:
		t := sourceIns.Arg.(*limit.Argument)
		res.Arg = &limit.Argument{
			Limit: t.Limit,
		}
	case vm.LoopAnti:
		t := sourceIns.Arg.(*loopanti.Argument)
		res.Arg = &loopanti.Argument{
			Result: t.Result,
			Cond:   t.Cond,
			Typs:   t.Typs,
		}
	case vm.LoopJoin:
		t := sourceIns.Arg.(*loopjoin.Argument)
		res.Arg = &loopjoin.Argument{
			Result: t.Result,
			Cond:   t.Cond,
			Typs:   t.Typs,
		}
	case vm.LoopLeft:
		t := sourceIns.Arg.(*loopleft.Argument)
		res.Arg = &loopleft.Argument{
			Cond:   t.Cond,
			Typs:   t.Typs,
			Result: t.Result,
		}
	case vm.LoopSemi:
		t := sourceIns.Arg.(*loopsemi.Argument)
		res.Arg = &loopsemi.Argument{
			Result: t.Result,
			Cond:   t.Cond,
			Typs:   t.Typs,
		}
	case vm.LoopSingle:
		t := sourceIns.Arg.(*loopsingle.Argument)
		res.Arg = &loopsingle.Argument{
			Result: t.Result,
			Cond:   t.Cond,
			Typs:   t.Typs,
		}
	case vm.LoopMark:
		t := sourceIns.Arg.(*loopmark.Argument)
		res.Arg = &loopmark.Argument{
			Result: t.Result,
			Cond:   t.Cond,
			Typs:   t.Typs,
		}
	case vm.Offset:
		t := sourceIns.Arg.(*offset.Argument)
		res.Arg = &offset.Argument{
			Offset: t.Offset,
		}
	case vm.Order:
		t := sourceIns.Arg.(*order.Argument)
		res.Arg = &order.Argument{
			OrderBySpec: t.OrderBySpec,
		}
	case vm.Product:
		t := sourceIns.Arg.(*product.Argument)
		res.Arg = &product.Argument{
			Result:    t.Result,
			Typs:      t.Typs,
			IsShuffle: t.IsShuffle,
		}
	case vm.Projection:
		t := sourceIns.Arg.(*projection.Argument)
		res.Arg = &projection.Argument{
			Es: t.Es,
		}
	case vm.Restrict:
		t := sourceIns.Arg.(*restrict.Argument)
		res.Arg = &restrict.Argument{
			E: t.E,
		}
	case vm.Semi:
		t := sourceIns.Arg.(*semi.Argument)
		res.Arg = &semi.Argument{
			Ibucket:            t.Ibucket,
			Nbucket:            t.Nbucket,
			Result:             t.Result,
			Cond:               t.Cond,
			Typs:               t.Typs,
			Conditions:         t.Conditions,
			RuntimeFilterSpecs: t.RuntimeFilterSpecs,
			HashOnPK:           t.HashOnPK,
			IsShuffle:          t.IsShuffle,
		}
	case vm.Single:
		t := sourceIns.Arg.(*single.Argument)
		res.Arg = &single.Argument{
			Ibucket:            t.Ibucket,
			Nbucket:            t.Nbucket,
			Result:             t.Result,
			Cond:               t.Cond,
			Typs:               t.Typs,
			Conditions:         t.Conditions,
			RuntimeFilterSpecs: t.RuntimeFilterSpecs,
			HashOnPK:           t.HashOnPK,
		}
	case vm.Top:
		t := sourceIns.Arg.(*top.Argument)
		res.Arg = &top.Argument{
			Limit: t.Limit,
			Fs:    t.Fs,
		}
	case vm.Intersect:
		t := sourceIns.Arg.(*intersect.Argument)
		res.Arg = &intersect.Argument{
			IBucket: t.IBucket,
			NBucket: t.NBucket,
		}
	case vm.Minus: // 2
		t := sourceIns.Arg.(*minus.Argument)
		res.Arg = &minus.Argument{
			IBucket: t.IBucket,
			NBucket: t.NBucket,
		}
	case vm.IntersectAll:
		t := sourceIns.Arg.(*intersectall.Argument)
		res.Arg = &intersectall.Argument{
			IBucket: t.IBucket,
			NBucket: t.NBucket,
		}
	case vm.Merge:
		res.Arg = &merge.Argument{SinkScan: sourceIns.Arg.(*merge.Argument).SinkScan}
	case vm.MergeRecursive:
		res.Arg = &mergerecursive.Argument{}
	case vm.MergeCTE:
		res.Arg = &mergecte.Argument{}
	case vm.MergeGroup:
		t := sourceIns.Arg.(*mergegroup.Argument)
		res.Arg = &mergegroup.Argument{
			NeedEval:           t.NeedEval,
			PartialResults:     t.PartialResults,
			PartialResultTypes: t.PartialResultTypes,
		}
	case vm.MergeLimit:
		t := sourceIns.Arg.(*mergelimit.Argument)
		res.Arg = &mergelimit.Argument{
			Limit: t.Limit,
		}
	case vm.MergeOffset:
		t := sourceIns.Arg.(*mergeoffset.Argument)
		res.Arg = &mergeoffset.Argument{
			Offset: t.Offset,
		}
	case vm.MergeTop:
		t := sourceIns.Arg.(*mergetop.Argument)
		res.Arg = &mergetop.Argument{
			Limit: t.Limit,
			Fs:    t.Fs,
		}
	case vm.MergeOrder:
		t := sourceIns.Arg.(*mergeorder.Argument)
		res.Arg = &mergeorder.Argument{
			OrderBySpecs: t.OrderBySpecs,
		}
	case vm.Mark:
		t := sourceIns.Arg.(*mark.Argument)
		res.Arg = &mark.Argument{
			Ibucket:    t.Ibucket,
			Nbucket:    t.Nbucket,
			Result:     t.Result,
			Conditions: t.Conditions,
			Typs:       t.Typs,
			Cond:       t.Cond,
			OnList:     t.OnList,
			HashOnPK:   t.HashOnPK,
		}
	case vm.TableFunction:
		t := sourceIns.Arg.(*table_function.Argument)
		res.Arg = &table_function.Argument{
			Name:   t.Name,
			Args:   t.Args,
			Rets:   t.Rets,
			Attrs:  t.Attrs,
			Params: t.Params,
		}

	case vm.HashBuild:
		t := sourceIns.Arg.(*hashbuild.Argument)
		res.Arg = &hashbuild.Argument{
			NeedHashMap:     t.NeedHashMap,
			NeedExpr:        t.NeedExpr,
			Ibucket:         t.Ibucket,
			Nbucket:         t.Nbucket,
			Typs:            t.Typs,
			Conditions:      t.Conditions,
			HashOnPK:        t.HashOnPK,
			NeedMergedBatch: t.NeedMergedBatch,
		}
	case vm.External:
		t := sourceIns.Arg.(*external.Argument)
		res.Arg = &external.Argument{
			Es: &external.ExternalParam{
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
		}
	case vm.Stream:
		t := sourceIns.Arg.(*stream.Argument)
		res.Arg = &stream.Argument{
			TblDef:  t.TblDef,
			Limit:   t.Limit,
			Offset:  t.Offset,
			Configs: t.Configs,
		}
	case vm.Connector:
		ok := false
		if regMap != nil {
			arg := &connector.Argument{}
			sourceReg := sourceIns.Arg.(*connector.Argument).Reg
			if arg.Reg, ok = regMap[sourceReg]; !ok {
				panic("nonexistent wait register")
			}
			res.Arg = arg
		}
	case vm.Shuffle:
		sourceArg := sourceIns.Arg.(*shuffle.Argument)
		arg := &shuffle.Argument{
			ShuffleType:   sourceArg.ShuffleType,
			ShuffleColIdx: sourceArg.ShuffleColIdx,
			ShuffleColMax: sourceArg.ShuffleColMax,
			ShuffleColMin: sourceArg.ShuffleColMin,
			AliveRegCnt:   sourceArg.AliveRegCnt,
		}
		res.Arg = arg
	case vm.Dispatch:
		ok := false
		if regMap != nil {
			sourceArg := sourceIns.Arg.(*dispatch.Argument)
			arg := &dispatch.Argument{
				IsSink:     sourceArg.IsSink,
				RecSink:    sourceArg.RecSink,
				FuncId:     sourceArg.FuncId,
				LocalRegs:  make([]*process.WaitRegister, len(sourceArg.LocalRegs)),
				RemoteRegs: make([]colexec.ReceiveInfo, len(sourceArg.RemoteRegs)),
			}
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
		res.Arg = &insert.Argument{
			ToWriteS3: t.ToWriteS3,
			InsertCtx: t.InsertCtx,
		}
	case vm.PreInsert:
		t := sourceIns.Arg.(*preinsert.Argument)
		res.Arg = &preinsert.Argument{
			SchemaName: t.SchemaName,
			TableDef:   t.TableDef,
			Attrs:      t.Attrs,
			IsUpdate:   t.IsUpdate,
			HasAutoCol: t.HasAutoCol,
		}
	case vm.Deletion:
		t := sourceIns.Arg.(*deletion.Argument)
		res.Arg = &deletion.Argument{
			Ts:           t.Ts,
			IBucket:      t.IBucket,
			Nbucket:      t.Nbucket,
			DeleteCtx:    t.DeleteCtx,
			RemoteDelete: t.RemoteDelete,
			SegmentMap:   t.SegmentMap,
		}
	case vm.LockOp:
		t := sourceIns.Arg.(*lockop.Argument)
		arg := new(lockop.Argument)
		*arg = *t
		res.Arg = arg
	case vm.FuzzyFilter:
		t := sourceIns.Arg.(*fuzzyfilter.Argument)
		res.Arg = &fuzzyfilter.Argument{
			N:                  t.N,
			PkName:             t.PkName,
			PkTyp:              t.PkTyp,
			RuntimeFilterSpecs: t.RuntimeFilterSpecs,
		}
	default:
		panic(fmt.Sprintf("unexpected instruction type '%d' to dup", sourceIns.Op))
	}
	return res
}

func constructRestrict(n *plan.Node, filterExpr *plan2.Expr) *restrict.Argument {
	return &restrict.Argument{
		E:     filterExpr,
		IsEnd: n.IsEnd,
	}
}

func constructDeletion(n *plan.Node, eg engine.Engine, proc *process.Process) (*deletion.Argument, error) {
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
	}
	// get the relation instance of the original table
	rel, _, err := getRel(proc.Ctx, proc, eg, oldCtx.Ref, nil)
	if err != nil {
		return nil, err
	}
	delCtx.Source = rel
	if len(oldCtx.PartitionTableNames) > 0 {
		dbSource, err := eg.Database(proc.Ctx, oldCtx.Ref.SchemaName, proc.TxnOperator)
		if err != nil {
			return nil, err
		}

		delCtx.PartitionSources = make([]engine.Relation, len(oldCtx.PartitionTableNames))
		// get the relation instances for each partition sub table
		for i, pTableName := range oldCtx.PartitionTableNames {
			pRel, err := dbSource.Relation(proc.Ctx, pTableName, proc)
			if err != nil {
				return nil, err
			}
			delCtx.PartitionSources[i] = pRel
		}
	}

	return &deletion.Argument{
		DeleteCtx: delCtx,
	}, nil
}

func constructOnduplicateKey(n *plan.Node, eg engine.Engine) *onduplicatekey.Argument {
	oldCtx := n.OnDuplicateKey
	return &onduplicatekey.Argument{
		Engine:          eg,
		OnDuplicateIdx:  oldCtx.OnDuplicateIdx,
		OnDuplicateExpr: oldCtx.OnDuplicateExpr,
		TableDef:        oldCtx.TableDef,
		IsIgnore:        oldCtx.IsIgnore,
	}
}

func constructFuzzyFilter(c *Compile, n, left, right *plan.Node) *fuzzyfilter.Argument {
	pkName := n.TableDef.Pkey.PkeyColName
	var pkTyp *plan.Type
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

	arg := &fuzzyfilter.Argument{
		PkName:             pkName,
		PkTyp:              pkTyp,
		N:                  right.Stats.Cost,
		RuntimeFilterSpecs: n.RuntimeFilterBuildList,
	}

	registerRuntimeFilters(arg, c, n.RuntimeFilterBuildList, 0)

	return arg
}

func constructPreInsert(n *plan.Node, eg engine.Engine, proc *process.Process) (*preinsert.Argument, error) {
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

	if preCtx.Ref.SchemaName != "" {
		dbSource, err := eg.Database(proc.Ctx, preCtx.Ref.SchemaName, proc.TxnOperator)
		if err != nil {
			return nil, err
		}
		if _, err = dbSource.Relation(proc.Ctx, preCtx.Ref.ObjName, proc); err != nil {
			schemaName = defines.TEMPORARY_DBNAME
		}
	}

	return &preinsert.Argument{
		Ctx:        proc.Ctx,
		HasAutoCol: preCtx.HasAutoCol,
		SchemaName: schemaName,
		TableDef:   preCtx.TableDef,
		Attrs:      attrs,
		IsUpdate:   preCtx.IsUpdate,
	}, nil
}

func constructPreInsertUk(n *plan.Node, proc *process.Process) (*preinsertunique.Argument, error) {
	preCtx := n.PreInsertUkCtx
	return &preinsertunique.Argument{
		Ctx:          proc.Ctx,
		PreInsertCtx: preCtx,
	}, nil
}

func constructPreInsertSk(n *plan.Node, proc *process.Process) (*preinsertsecondaryindex.Argument, error) {
	preCtx := n.PreInsertSkCtx
	return &preinsertsecondaryindex.Argument{
		Ctx:          proc.Ctx,
		PreInsertCtx: preCtx,
	}, nil
}

func constructLockOp(n *plan.Node, proc *process.Process, eng engine.Engine) (*lockop.Argument, error) {
	arg := lockop.NewArgument(eng)
	for _, target := range n.LockTargets {
		typ := plan2.MakeTypeByPlan2Type(target.GetPrimaryColTyp())
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

func constructInsert(n *plan.Node, eg engine.Engine, proc *process.Process) (*insert.Argument, error) {
	oldCtx := n.InsertCtx
	ctx := proc.Ctx

	var attrs []string
	for _, col := range oldCtx.TableDef.Cols {
		if col.Name != catalog.Row_ID {
			attrs = append(attrs, col.Name)
		}
	}
	originRel, _, err := getRel(ctx, proc, eg, oldCtx.Ref, nil)
	if err != nil {
		return nil, err
	}
	newCtx := &insert.InsertCtx{
		Ref:                   oldCtx.Ref,
		AddAffectedRows:       oldCtx.AddAffectedRows,
		Rel:                   originRel,
		Attrs:                 attrs,
		PartitionTableIDs:     oldCtx.PartitionTableIds,
		PartitionTableNames:   oldCtx.PartitionTableNames,
		PartitionIndexInBatch: int(oldCtx.PartitionIdx),
		TableDef:              oldCtx.TableDef,
	}
	if len(oldCtx.PartitionTableNames) > 0 {
		dbSource, err := eg.Database(proc.Ctx, oldCtx.Ref.SchemaName, proc.TxnOperator)
		if err != nil {
			return nil, err
		}

		newCtx.PartitionSources = make([]engine.Relation, len(oldCtx.PartitionTableNames))
		// get the relation instances for each partition sub table
		for i, pTableName := range oldCtx.PartitionTableNames {
			pRel, err := dbSource.Relation(proc.Ctx, pTableName, proc)
			if err != nil {
				return nil, err
			}
			newCtx.PartitionSources[i] = pRel
		}
	}

	return &insert.Argument{
		InsertCtx: newCtx,
	}, nil
}

func constructProjection(n *plan.Node) *projection.Argument {
	return &projection.Argument{
		Es: n.ProjectList,
	}
}

func constructExternal(n *plan.Node, param *tree.ExternParam, ctx context.Context, fileList []string, FileSize []int64, fileOffset []*pipeline.FileOffset) *external.Argument {
	attrs := make([]string, len(n.TableDef.Cols))
	for j, col := range n.TableDef.Cols {
		attrs[j] = col.Name
	}
	return &external.Argument{
		Es: &external.ExternalParam{
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
	}
}

func constructStream(n *plan.Node, p [2]int64) *stream.Argument {
	return &stream.Argument{
		TblDef: n.TableDef,
		Offset: p[0],
		Limit:  p[1],
	}
}

func constructTableFunction(n *plan.Node) *table_function.Argument {
	attrs := make([]string, len(n.TableDef.Cols))
	for j, col := range n.TableDef.Cols {
		attrs[j] = col.Name
	}
	return &table_function.Argument{
		Attrs:  attrs,
		Rets:   n.TableDef.Cols,
		Args:   n.TblFuncExprList,
		Name:   n.TableDef.TblFunc.Name,
		Params: n.TableDef.TblFunc.Param,
	}
}

func constructTop(n *plan.Node, topN int64) *top.Argument {
	return &top.Argument{
		Fs:    n.OrderBy,
		Limit: topN,
	}
}

func constructJoin(n *plan.Node, typs []types.Type, proc *process.Process) *join.Argument {
	result := make([]colexec.ResultPos, len(n.ProjectList))
	for i, expr := range n.ProjectList {
		result[i].Rel, result[i].Pos = constructJoinResult(expr, proc)
	}
	cond, conds := extraJoinConditions(n.OnList)

	return &join.Argument{
		Typs:               typs,
		Result:             result,
		Cond:               cond,
		Conditions:         constructJoinConditions(conds, proc),
		RuntimeFilterSpecs: n.RuntimeFilterBuildList,
		HashOnPK:           n.Stats.HashmapStats != nil && n.Stats.HashmapStats.HashOnPK,
		IsShuffle:          n.Stats.HashmapStats != nil && n.Stats.HashmapStats.Shuffle,
	}
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
	return &semi.Argument{
		Typs:               typs,
		Result:             result,
		Cond:               cond,
		Conditions:         constructJoinConditions(conds, proc),
		RuntimeFilterSpecs: n.RuntimeFilterBuildList,
		HashOnPK:           n.Stats.HashmapStats != nil && n.Stats.HashmapStats.HashOnPK,
		IsShuffle:          n.Stats.HashmapStats != nil && n.Stats.HashmapStats.Shuffle,
	}
}

func constructLeft(n *plan.Node, typs []types.Type, proc *process.Process) *left.Argument {
	result := make([]colexec.ResultPos, len(n.ProjectList))
	for i, expr := range n.ProjectList {
		result[i].Rel, result[i].Pos = constructJoinResult(expr, proc)
	}
	cond, conds := extraJoinConditions(n.OnList)
	return &left.Argument{
		Typs:               typs,
		Result:             result,
		Cond:               cond,
		Conditions:         constructJoinConditions(conds, proc),
		RuntimeFilterSpecs: n.RuntimeFilterBuildList,
		HashOnPK:           n.Stats.HashmapStats != nil && n.Stats.HashmapStats.HashOnPK,
		IsShuffle:          n.Stats.HashmapStats != nil && n.Stats.HashmapStats.Shuffle,
	}
}

func constructRight(n *plan.Node, left_typs, right_typs []types.Type, Ibucket, Nbucket uint64, proc *process.Process) *right.Argument {
	result := make([]colexec.ResultPos, len(n.ProjectList))
	for i, expr := range n.ProjectList {
		result[i].Rel, result[i].Pos = constructJoinResult(expr, proc)
	}
	cond, conds := extraJoinConditions(n.OnList)
	return &right.Argument{
		LeftTypes:          left_typs,
		RightTypes:         right_typs,
		Nbucket:            Nbucket,
		Ibucket:            Ibucket,
		Result:             result,
		Cond:               cond,
		Conditions:         constructJoinConditions(conds, proc),
		RuntimeFilterSpecs: n.RuntimeFilterBuildList,
		HashOnPK:           n.Stats.HashmapStats != nil && n.Stats.HashmapStats.HashOnPK,
		IsShuffle:          n.Stats.HashmapStats != nil && n.Stats.HashmapStats.Shuffle,
	}
}

func constructRightSemi(n *plan.Node, right_typs []types.Type, Ibucket, Nbucket uint64, proc *process.Process) *rightsemi.Argument {
	result := make([]int32, len(n.ProjectList))
	for i, expr := range n.ProjectList {
		_, result[i] = constructJoinResult(expr, proc)
	}
	cond, conds := extraJoinConditions(n.OnList)
	return &rightsemi.Argument{
		RightTypes:         right_typs,
		Nbucket:            Nbucket,
		Ibucket:            Ibucket,
		Result:             result,
		Cond:               cond,
		Conditions:         constructJoinConditions(conds, proc),
		RuntimeFilterSpecs: n.RuntimeFilterBuildList,
		HashOnPK:           n.Stats.HashmapStats != nil && n.Stats.HashmapStats.HashOnPK,
		IsShuffle:          n.Stats.HashmapStats != nil && n.Stats.HashmapStats.Shuffle,
	}
}

func constructRightAnti(n *plan.Node, right_typs []types.Type, Ibucket, Nbucket uint64, proc *process.Process) *rightanti.Argument {
	result := make([]int32, len(n.ProjectList))
	for i, expr := range n.ProjectList {
		_, result[i] = constructJoinResult(expr, proc)
	}
	cond, conds := extraJoinConditions(n.OnList)
	return &rightanti.Argument{
		RightTypes:         right_typs,
		Nbucket:            Nbucket,
		Ibucket:            Ibucket,
		Result:             result,
		Cond:               cond,
		Conditions:         constructJoinConditions(conds, proc),
		RuntimeFilterSpecs: n.RuntimeFilterBuildList,
		HashOnPK:           n.Stats.HashmapStats != nil && n.Stats.HashmapStats.HashOnPK,
		IsShuffle:          n.Stats.HashmapStats != nil && n.Stats.HashmapStats.Shuffle,
	}
}

func constructSingle(n *plan.Node, typs []types.Type, proc *process.Process) *single.Argument {
	result := make([]colexec.ResultPos, len(n.ProjectList))
	for i, expr := range n.ProjectList {
		result[i].Rel, result[i].Pos = constructJoinResult(expr, proc)
	}
	cond, conds := extraJoinConditions(n.OnList)
	return &single.Argument{
		Typs:               typs,
		Result:             result,
		Cond:               cond,
		Conditions:         constructJoinConditions(conds, proc),
		RuntimeFilterSpecs: n.RuntimeFilterBuildList,
		HashOnPK:           n.Stats.HashmapStats != nil && n.Stats.HashmapStats.HashOnPK,
	}
}

func constructProduct(n *plan.Node, typs []types.Type, proc *process.Process) *product.Argument {
	result := make([]colexec.ResultPos, len(n.ProjectList))
	for i, expr := range n.ProjectList {
		result[i].Rel, result[i].Pos = constructJoinResult(expr, proc)
	}
	return &product.Argument{Typs: typs, Result: result}
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
	return &anti.Argument{
		Typs:       typs,
		Result:     result,
		Cond:       cond,
		Conditions: constructJoinConditions(conds, proc),
		HashOnPK:   n.Stats.HashmapStats != nil && n.Stats.HashmapStats.HashOnPK,
		IsShuffle:  n.Stats.HashmapStats != nil && n.Stats.HashmapStats.Shuffle,
	}
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
	return &order.Argument{
		OrderBySpec: n.OrderBy,
	}
}

func constructFill(n *plan.Node) *fill.Argument {
	aggIdx := make([]int32, len(n.AggList))
	for i, expr := range n.AggList {
		f := expr.Expr.(*plan.Expr_F)
		obj := int64(uint64(f.F.Func.Obj) & function.DistinctMask)
		aggIdx[i], _ = function.DecodeOverloadID(obj)
	}
	return &fill.Argument{
		ColLen:   len(n.AggList),
		FillType: n.FillType,
		FillVal:  n.FillVal,
		AggIds:   aggIdx,
	}
}

func constructTimeWindow(ctx context.Context, n *plan.Node, proc *process.Process) *timewin.Argument {
	var aggs []agg.Aggregate
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
		distinct := (uint64(f.F.Func.Obj) & function.Distinct) != 0
		obj := int64(uint64(f.F.Func.Obj) & function.DistinctMask)
		e := f.F.Args[0]
		if e != nil {
			aggs = append(aggs, agg.Aggregate{
				E:    e,
				Dist: distinct,
				Op:   obj,
			})
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

	return &timewin.Argument{
		Types:    typs,
		Aggs:     aggs,
		Ts:       n.OrderBy[0].Expr,
		WStart:   wStart,
		WEnd:     wEnd,
		Interval: itr,
		Sliding:  sld,
	}
}

func constructWindow(ctx context.Context, n *plan.Node, proc *process.Process) *window.Argument {
	aggs := make([]agg.Aggregate, len(n.WinSpecList))
	typs := make([]types.Type, len(n.WinSpecList))
	for i, expr := range n.WinSpecList {
		f := expr.Expr.(*plan.Expr_W).W.WindowFunc.Expr.(*plan.Expr_F)
		distinct := (uint64(f.F.Func.Obj) & function.Distinct) != 0
		obj := int64(uint64(f.F.Func.Obj) & function.DistinctMask)
		var e *plan.Expr = nil
		var cfg []byte

		if len(f.F.Args) > 0 {

			//for group_concat, the last arg is separator string
			//for cluster_centers, the last arg is kmeans_args string
			if (f.F.Func.ObjName == plan2.NameGroupConcat ||
				f.F.Func.ObjName == plan2.NameClusterCenters) && len(f.F.Args) > 1 {
				argExpr := f.F.Args[len(f.F.Args)-1]
				executor, err := colexec.NewExpressionExecutor(proc, argExpr)
				if err != nil {
					panic(err)
				}
				vec, err := executor.Eval(proc, []*batch.Batch{constBat})
				if err != nil {
					panic(err)
				}
				cfg = []byte(vec.GetStringAt(0))
			}

			e = f.F.Args[0]
		}
		aggs[i] = agg.Aggregate{
			E:      e,
			Dist:   distinct,
			Op:     obj,
			Config: cfg,
		}
		if e != nil {
			typs[i] = types.New(types.T(e.Typ.Id), e.Typ.Width, e.Typ.Scale)
		}
	}
	return &window.Argument{
		Types:       typs,
		Aggs:        aggs,
		WinSpecList: n.WinSpecList,
	}
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

func constructLimit(n *plan.Node, proc *process.Process) *limit.Argument {
	executor, err := colexec.NewExpressionExecutor(proc, n.Limit)
	if err != nil {
		panic(err)
	}
	defer executor.Free()
	vec, err := executor.Eval(proc, []*batch.Batch{constBat})
	if err != nil {
		panic(err)
	}

	return &limit.Argument{
		Limit: uint64(vector.MustFixedCol[int64](vec)[0]),
	}
}

func constructSample(n *plan.Node) *sample.Argument {
	if n.SampleFunc.Rows != plan2.NotSampleByRows {
		return sample.NewSampleByRows(int(n.SampleFunc.Rows), n.AggList, n.GroupBy)
	}
	if n.SampleFunc.Percent != plan2.NotSampleByPercents {
		return sample.NewSampleByPercent(n.SampleFunc.Percent, n.AggList, n.GroupBy)
	}
	panic("only support sample by rows / percent now.")
}

func constructGroup(ctx context.Context, n, cn *plan.Node, ibucket, nbucket int, needEval bool, shuffleDop int, proc *process.Process) *group.Argument {
	aggs := make([]agg.Aggregate, len(n.AggList))
	var cfg []byte
	for i, expr := range n.AggList {
		if f, ok := expr.Expr.(*plan.Expr_F); ok {
			distinct := (uint64(f.F.Func.Obj) & function.Distinct) != 0
			obj := int64(uint64(f.F.Func.Obj) & function.DistinctMask)
			if len(f.F.Args) > 0 {
				//for group_concat, the last arg is separator string
				//for cluster_centers, the last arg is kmeans_args string
				if (f.F.Func.ObjName == plan2.NameGroupConcat ||
					f.F.Func.ObjName == plan2.NameClusterCenters) && len(f.F.Args) > 1 {
					argExpr := f.F.Args[len(f.F.Args)-1]
					executor, err := colexec.NewExpressionExecutor(proc, argExpr)
					if err != nil {
						panic(err)
					}
					vec, err := executor.Eval(proc, []*batch.Batch{constBat})
					if err != nil {
						panic(err)
					}
					cfg = []byte(vec.GetStringAt(0))
				}
			}

			aggs[i] = agg.Aggregate{
				E:      f.F.Args[0],
				Dist:   distinct,
				Op:     obj,
				Config: cfg,
			}
		}
	}
	typs := make([]types.Type, len(cn.ProjectList))
	for i, e := range cn.ProjectList {
		typs[i] = types.New(types.T(e.Typ.Id), e.Typ.Width, e.Typ.Scale)
	}

	shuffle := false
	var preAllocSize uint64 = 0
	if n.Stats != nil && n.Stats.HashmapStats != nil && n.Stats.HashmapStats.Shuffle {
		shuffle = true
		if cn.NodeType == plan.Node_TABLE_SCAN && len(cn.FilterList) == 0 {
			// if group on scan without filter, stats for hashmap is accurate to do preAlloc
			// tune it up a little bit in case it is not so average after shuffle
			preAllocSize = uint64(n.Stats.HashmapStats.HashmapSize / float64(shuffleDop) * 1.05)
		}
	}

	return &group.Argument{
		Aggs:         aggs,
		Types:        typs,
		NeedEval:     needEval,
		Exprs:        n.GroupBy,
		Ibucket:      uint64(ibucket),
		Nbucket:      uint64(nbucket),
		IsShuffle:    shuffle,
		PreAllocSize: preAllocSize,
	}
}

// ibucket: bucket number
// nbucket:
// construct operator argument
func constructIntersectAll(ibucket, nbucket int) *intersectall.Argument {
	return &intersectall.Argument{
		IBucket: uint64(ibucket),
		NBucket: uint64(nbucket),
	}
}

func constructMinus(ibucket, nbucket int) *minus.Argument {
	return &minus.Argument{
		IBucket: uint64(ibucket),
		NBucket: uint64(nbucket),
	}
}

func constructIntersect(ibucket, nbucket int) *intersect.Argument {
	return &intersect.Argument{
		IBucket: uint64(ibucket),
		NBucket: uint64(nbucket),
	}
}

func constructDispatchLocal(all bool, isSink, RecSink bool, regs []*process.WaitRegister) *dispatch.Argument {
	arg := new(dispatch.Argument)
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
/*
func constructDeleteDispatchAndLocal(
	currentIdx int,
	rs []*Scope,
	ss []*Scope,
	uuids []uuid.UUID,
	c *Compile) {
	arg := new(dispatch.Argument)
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
		Arg: &merge.Argument{},
	})
}
*/

// This function do not setting funcId.
// PLEASE SETTING FuncId AFTER YOU CALL IT.
func constructDispatchLocalAndRemote(idx int, ss []*Scope, currentCNAddr string) (bool, *dispatch.Argument) {
	arg := new(dispatch.Argument)
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
	arg := new(shuffle.Argument)
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

	hashCol, _ := plan2.GetHashColumn(expr)
	arg.ShuffleColIdx = hashCol.ColPos
	arg.ShuffleType = int32(node.Stats.HashmapStats.ShuffleType)
	arg.ShuffleColMin = node.Stats.HashmapStats.ShuffleColMin
	arg.ShuffleColMax = node.Stats.HashmapStats.ShuffleColMax
	arg.AliveRegCnt = int32(len(ss))
	return arg
}

func constructShuffleGroupArg(ss []*Scope, node *plan.Node) *shuffle.Argument {
	arg := new(shuffle.Argument)
	hashCol, _ := plan2.GetHashColumn(node.GroupBy[node.Stats.HashmapStats.ShuffleColIdx])
	arg.ShuffleColIdx = hashCol.ColPos
	arg.ShuffleType = int32(node.Stats.HashmapStats.ShuffleType)
	arg.ShuffleColMin = node.Stats.HashmapStats.ShuffleColMin
	arg.ShuffleColMax = node.Stats.HashmapStats.ShuffleColMax
	arg.AliveRegCnt = int32(len(ss))
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
	return &mergegroup.Argument{
		NeedEval: needEval,
	}
}

func constructMergeTop(n *plan.Node, topN int64) *mergetop.Argument {
	return &mergetop.Argument{
		Fs:    n.OrderBy,
		Limit: topN,
	}
}

func constructMergeOffset(n *plan.Node, proc *process.Process) *mergeoffset.Argument {
	executor, err := colexec.NewExpressionExecutor(proc, n.Offset)
	if err != nil {
		panic(err)
	}
	defer executor.Free()
	vec, err := executor.Eval(proc, []*batch.Batch{constBat})
	if err != nil {
		panic(err)
	}

	return &mergeoffset.Argument{
		Offset: uint64(vector.MustFixedCol[int64](vec)[0]),
	}
}

func constructMergeLimit(n *plan.Node, proc *process.Process) *mergelimit.Argument {
	executor, err := colexec.NewExpressionExecutor(proc, n.Limit)
	if err != nil {
		panic(err)
	}
	defer executor.Free()
	vec, err := executor.Eval(proc, []*batch.Batch{constBat})
	if err != nil {
		panic(err)
	}

	return &mergelimit.Argument{
		Limit: uint64(vector.MustFixedCol[int64](vec)[0]),
	}
}

func constructMergeOrder(n *plan.Node) *mergeorder.Argument {
	return &mergeorder.Argument{
		OrderBySpecs: n.OrderBy,
	}
}

func constructPartition(n *plan.Node) *partition.Argument {
	return &partition.Argument{
		OrderBySpecs: n.OrderBy,
	}
}

func constructLoopJoin(n *plan.Node, typs []types.Type, proc *process.Process) *loopjoin.Argument {
	result := make([]colexec.ResultPos, len(n.ProjectList))
	for i, expr := range n.ProjectList {
		result[i].Rel, result[i].Pos = constructJoinResult(expr, proc)
	}
	return &loopjoin.Argument{
		Typs:   typs,
		Result: result,
		Cond:   colexec.RewriteFilterExprList(n.OnList),
	}
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
	return &loopsemi.Argument{
		Typs:   typs,
		Result: result,
		Cond:   colexec.RewriteFilterExprList(n.OnList),
	}
}

func constructLoopLeft(n *plan.Node, typs []types.Type, proc *process.Process) *loopleft.Argument {
	result := make([]colexec.ResultPos, len(n.ProjectList))
	for i, expr := range n.ProjectList {
		result[i].Rel, result[i].Pos = constructJoinResult(expr, proc)
	}
	return &loopleft.Argument{
		Typs:   typs,
		Result: result,
		Cond:   colexec.RewriteFilterExprList(n.OnList),
	}
}

func constructLoopSingle(n *plan.Node, typs []types.Type, proc *process.Process) *loopsingle.Argument {
	result := make([]colexec.ResultPos, len(n.ProjectList))
	for i, expr := range n.ProjectList {
		result[i].Rel, result[i].Pos = constructJoinResult(expr, proc)
	}
	return &loopsingle.Argument{
		Typs:   typs,
		Result: result,
		Cond:   colexec.RewriteFilterExprList(n.OnList),
	}
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
	return &loopanti.Argument{
		Typs:   typs,
		Result: result,
		Cond:   colexec.RewriteFilterExprList(n.OnList),
	}
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
	return &loopmark.Argument{
		Typs:   typs,
		Result: result,
		Cond:   colexec.RewriteFilterExprList(n.OnList),
	}
}

func registerRuntimeFilters[T runtimeFilterSenderSetter](arg T, c *Compile, specs []*plan.RuntimeFilterSpec, shuffleCnt int) {
	if specs == nil {
		return
	}

	RuntimeFilterSenders := make([]*colexec.RuntimeFilterChan, 0, len(specs))
	for _, rfSpec := range specs {
		c.lock.Lock()
		receiver, ok := c.runtimeFilterReceiverMap[rfSpec.Tag]
		if !ok {
			if shuffleCnt == 0 {
				shuffleCnt = 1
			}
			receiver = &runtimeFilterReceiver{
				size: shuffleCnt,
				ch:   make(chan *pipeline.RuntimeFilter, 1),
			}
			c.runtimeFilterReceiverMap[rfSpec.Tag] = receiver
		}
		c.lock.Unlock()

		RuntimeFilterSenders = append(RuntimeFilterSenders, &colexec.RuntimeFilterChan{
			Spec: rfSpec,
			Chan: receiver.ch,
		})
	}

	// Set the runtime filters for the concrete type
	arg.SetRuntimeFilterSenders(RuntimeFilterSenders)

}

func constructHashBuild(c *Compile, in vm.Instruction, proc *process.Process, shuffleCnt int, isDup bool) *hashbuild.Argument {
	// XXX BUG
	// relation index of arg.Conditions should be rewritten to 0 here.

	switch in.Op {
	case vm.Anti:
		arg := in.Arg.(*anti.Argument)
		return &hashbuild.Argument{
			NeedHashMap:     true,
			Typs:            arg.Typs,
			Conditions:      arg.Conditions[1],
			IsDup:           isDup,
			NeedMergedBatch: true,
			HashOnPK:        arg.HashOnPK,
		}

	case vm.Mark:
		arg := in.Arg.(*mark.Argument)
		return &hashbuild.Argument{
			NeedHashMap:     true,
			Typs:            arg.Typs,
			Conditions:      arg.Conditions[1],
			IsDup:           isDup,
			NeedMergedBatch: true,
			HashOnPK:        arg.HashOnPK,
		}

	case vm.Join:
		arg := in.Arg.(*join.Argument)
		retArg := &hashbuild.Argument{
			NeedHashMap: true,
			Typs:        arg.Typs,
			Conditions:  arg.Conditions[1],
			IsDup:       isDup,
			HashOnPK:    arg.HashOnPK,
		}

		// to find if hashmap need to merge batches into one large batch and keep this batch for join
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
		retArg.NeedMergedBatch = needMergedBatch

		registerRuntimeFilters(retArg, c, arg.RuntimeFilterSpecs, shuffleCnt)

		return retArg

	case vm.Left:
		arg := in.Arg.(*left.Argument)
		retArg := &hashbuild.Argument{
			NeedHashMap:     true,
			Typs:            arg.Typs,
			Conditions:      arg.Conditions[1],
			IsDup:           isDup,
			NeedMergedBatch: true,
			HashOnPK:        arg.HashOnPK,
		}

		registerRuntimeFilters(retArg, c, arg.RuntimeFilterSpecs, shuffleCnt)

		return retArg

	case vm.Right:
		arg := in.Arg.(*right.Argument)
		retArg := &hashbuild.Argument{
			Ibucket:         arg.Ibucket,
			Nbucket:         arg.Nbucket,
			NeedHashMap:     true,
			Typs:            arg.RightTypes,
			Conditions:      arg.Conditions[1],
			IsDup:           isDup,
			NeedMergedBatch: true,
			HashOnPK:        arg.HashOnPK,
		}

		registerRuntimeFilters(retArg, c, arg.RuntimeFilterSpecs, shuffleCnt)

		return retArg

	case vm.RightSemi:
		arg := in.Arg.(*rightsemi.Argument)
		retArg := &hashbuild.Argument{
			Ibucket:         arg.Ibucket,
			Nbucket:         arg.Nbucket,
			NeedHashMap:     true,
			Typs:            arg.RightTypes,
			Conditions:      arg.Conditions[1],
			IsDup:           isDup,
			NeedMergedBatch: true,
			HashOnPK:        arg.HashOnPK,
		}

		registerRuntimeFilters(retArg, c, arg.RuntimeFilterSpecs, shuffleCnt)

		return retArg

	case vm.RightAnti:
		arg := in.Arg.(*rightanti.Argument)
		retArg := &hashbuild.Argument{
			Ibucket:         arg.Ibucket,
			Nbucket:         arg.Nbucket,
			NeedHashMap:     true,
			Typs:            arg.RightTypes,
			Conditions:      arg.Conditions[1],
			IsDup:           isDup,
			NeedMergedBatch: true,
			HashOnPK:        arg.HashOnPK,
		}

		registerRuntimeFilters(retArg, c, arg.RuntimeFilterSpecs, shuffleCnt)

		return retArg

	case vm.Semi:
		arg := in.Arg.(*semi.Argument)
		retArg := &hashbuild.Argument{
			NeedHashMap:     true,
			Typs:            arg.Typs,
			Conditions:      arg.Conditions[1],
			IsDup:           isDup,
			NeedMergedBatch: true,
			HashOnPK:        arg.HashOnPK,
		}

		registerRuntimeFilters(retArg, c, arg.RuntimeFilterSpecs, shuffleCnt)

		return retArg

	case vm.Single:
		arg := in.Arg.(*single.Argument)
		retArg := &hashbuild.Argument{
			NeedHashMap:     true,
			Typs:            arg.Typs,
			Conditions:      arg.Conditions[1],
			IsDup:           isDup,
			NeedMergedBatch: true,
			HashOnPK:        arg.HashOnPK,
		}

		registerRuntimeFilters(retArg, c, arg.RuntimeFilterSpecs, shuffleCnt)

		return retArg

	case vm.Product:
		arg := in.Arg.(*product.Argument)
		return &hashbuild.Argument{
			NeedHashMap:     false,
			Typs:            arg.Typs,
			IsDup:           isDup,
			NeedMergedBatch: true,
		}

	case vm.LoopAnti:
		arg := in.Arg.(*loopanti.Argument)
		return &hashbuild.Argument{
			NeedHashMap:     false,
			Typs:            arg.Typs,
			IsDup:           isDup,
			NeedMergedBatch: true,
		}

	case vm.LoopJoin:
		arg := in.Arg.(*loopjoin.Argument)
		return &hashbuild.Argument{
			NeedHashMap:     false,
			Typs:            arg.Typs,
			IsDup:           isDup,
			NeedMergedBatch: true,
		}

	case vm.LoopLeft:
		arg := in.Arg.(*loopleft.Argument)
		return &hashbuild.Argument{
			NeedHashMap:     false,
			Typs:            arg.Typs,
			IsDup:           isDup,
			NeedMergedBatch: true,
		}

	case vm.LoopSemi:
		arg := in.Arg.(*loopsemi.Argument)
		return &hashbuild.Argument{
			NeedHashMap:     false,
			Typs:            arg.Typs,
			IsDup:           isDup,
			NeedMergedBatch: true,
		}

	case vm.LoopSingle:
		arg := in.Arg.(*loopsingle.Argument)
		return &hashbuild.Argument{
			NeedHashMap:     false,
			Typs:            arg.Typs,
			IsDup:           isDup,
			NeedMergedBatch: true,
		}

	case vm.LoopMark:
		arg := in.Arg.(*loopmark.Argument)
		return &hashbuild.Argument{
			NeedHashMap:     false,
			Typs:            arg.Typs,
			IsDup:           isDup,
			NeedMergedBatch: true,
		}

	default:
		panic(moerr.NewInternalError(proc.Ctx, "unsupport join type '%v'", in.Op))
	}
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

// Get the 'engine.Relation' of the table by using 'ObjectRef' and 'TableDef', if 'TableDef' is nil, the relations of its index table will not be obtained
// the first return value is Relation of the original table
// the second return value is Relations of index tables
func getRel(ctx context.Context, proc *process.Process, eg engine.Engine, ref *plan.ObjectRef, tableDef *plan.TableDef) (engine.Relation, []engine.Relation, error) {
	var dbSource engine.Database
	var relation engine.Relation
	var err error
	var isTemp bool
	oldDbName := ref.SchemaName
	if ref.SchemaName != "" {
		dbSource, err = eg.Database(ctx, ref.SchemaName, proc.TxnOperator)
		if err != nil {
			return nil, nil, err
		}
		relation, err = dbSource.Relation(ctx, ref.ObjName, proc)
		if err == nil {
			isTemp = defines.TEMPORARY_DBNAME == ref.SchemaName
		} else {
			dbSource, err = eg.Database(ctx, defines.TEMPORARY_DBNAME, proc.TxnOperator)
			if err != nil {
				return nil, nil, moerr.NewNoSuchTable(ctx, ref.SchemaName, ref.ObjName)
			}
			newObjeName := engine.GetTempTableName(ref.SchemaName, ref.ObjName)
			newSchemaName := defines.TEMPORARY_DBNAME
			ref.SchemaName = newSchemaName
			ref.ObjName = newObjeName
			relation, err = dbSource.Relation(ctx, newObjeName, proc)
			if err != nil {
				return nil, nil, err
			}
			isTemp = true
		}
	} else {
		_, _, relation, err = eg.GetRelationById(ctx, proc.TxnOperator, uint64(ref.Obj))
		if err != nil {
			return nil, nil, err
		}
	}

	var uniqueIndexTables []engine.Relation
	if tableDef != nil {
		uniqueIndexTables = make([]engine.Relation, 0)
		if tableDef.Indexes != nil {
			for _, indexdef := range tableDef.Indexes {
				var indexTable engine.Relation
				if indexdef.TableExist {
					if isTemp {
						indexTable, err = dbSource.Relation(ctx, engine.GetTempTableName(oldDbName, indexdef.IndexTableName), proc)
					} else {
						indexTable, err = dbSource.Relation(ctx, indexdef.IndexTableName, proc)
					}
					if err != nil {
						return nil, nil, err
					}
					// NOTE: uniqueIndexTables is not yet used by the callee
					uniqueIndexTables = append(uniqueIndexTables, indexTable)
				}
			}
		}
	}
	return relation, uniqueIndexTables, err
}

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
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/agg"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/anti"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/connector"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/external"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/hashbuild"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/intersect"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/intersectall"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/loopanti"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/loopjoin"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/loopleft"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/loopsemi"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/loopsingle"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/mark"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/merge"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/minus"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/offset"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/table_function"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/update"

	"github.com/matrixorigin/matrixone/pkg/sql/colexec/deletion"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/insert"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/dispatch"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/group"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/join"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/left"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/limit"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/mergegroup"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/mergelimit"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/mergeoffset"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/mergeorder"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/mergetop"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/order"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/product"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/projection"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/restrict"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/semi"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/single"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/top"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

var constBat *batch.Batch

func init() {
	constBat = batch.NewWithSize(0)
	constBat.Zs = []int64{1}
}

func dupInstruction(sourceIns *vm.Instruction, regMap map[*process.WaitRegister]*process.WaitRegister) vm.Instruction {
	res := vm.Instruction{Op: sourceIns.Op, Idx: sourceIns.Idx}
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
		}
	case vm.Group:
		t := sourceIns.Arg.(*group.Argument)
		res.Arg = &group.Argument{
			NeedEval: t.NeedEval,
			Ibucket:  t.Ibucket,
			Nbucket:  t.Nbucket,
			Exprs:    t.Exprs,
			Types:    t.Types,
			Aggs:     t.Aggs,
		}
	case vm.Join:
		t := sourceIns.Arg.(*join.Argument)
		res.Arg = &join.Argument{
			Ibucket:    t.Ibucket,
			Nbucket:    t.Nbucket,
			Result:     t.Result,
			Cond:       t.Cond,
			Typs:       t.Typs,
			Conditions: t.Conditions,
		}
	case vm.Left:
		t := sourceIns.Arg.(*left.Argument)
		res.Arg = &left.Argument{
			Ibucket:    t.Ibucket,
			Nbucket:    t.Nbucket,
			Cond:       t.Cond,
			Result:     t.Result,
			Typs:       t.Typs,
			Conditions: t.Conditions,
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
		t := sourceIns.Arg.(*loopanti.Argument)
		res.Arg = &loopanti.Argument{
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
	case vm.Offset:
		t := sourceIns.Arg.(*offset.Argument)
		res.Arg = &offset.Argument{
			Offset: t.Offset,
		}
	case vm.Order:
		t := sourceIns.Arg.(*order.Argument)
		res.Arg = &order.Argument{
			Fs: t.Fs,
		}
	case vm.Product:
		t := sourceIns.Arg.(*product.Argument)
		res.Arg = &product.Argument{
			Result: t.Result,
			Typs:   t.Typs,
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
			Ibucket:    t.Ibucket,
			Nbucket:    t.Nbucket,
			Result:     t.Result,
			Cond:       t.Cond,
			Typs:       t.Typs,
			Conditions: t.Conditions,
		}
	case vm.Single:
		t := sourceIns.Arg.(*single.Argument)
		res.Arg = &single.Argument{
			Ibucket:    t.Ibucket,
			Nbucket:    t.Nbucket,
			Result:     t.Result,
			Cond:       t.Cond,
			Typs:       t.Typs,
			Conditions: t.Conditions,
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
		res.Arg = &merge.Argument{}
	case vm.MergeGroup:
		t := sourceIns.Arg.(*mergegroup.Argument)
		res.Arg = &mergegroup.Argument{
			NeedEval: t.NeedEval,
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
			Fs: t.Fs,
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
			NeedHashMap: t.NeedHashMap,
			NeedExpr:    t.NeedExpr,
			Ibucket:     t.Ibucket,
			Nbucket:     t.Nbucket,
			Typs:        t.Typs,
			Conditions:  t.Conditions,
		}
	case vm.External:
		t := sourceIns.Arg.(*external.Argument)
		res.Arg = &external.Argument{
			Es: &external.ExternalParam{
				Attrs:         t.Es.Attrs,
				Cols:          t.Es.Cols,
				Name2ColIndex: t.Es.Name2ColIndex,
				CreateSql:     t.Es.CreateSql,
				FileList:      t.Es.FileList,
				Fileparam: &external.ExternalFileparam{
					End:       t.Es.Fileparam.End,
					FileCnt:   t.Es.Fileparam.FileCnt,
					FileFin:   t.Es.Fileparam.FileFin,
					FileIndex: t.Es.Fileparam.FileIndex,
				},
			},
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
	case vm.Dispatch:
		ok := false
		if regMap != nil {
			sourceArg := sourceIns.Arg.(*dispatch.Argument)
			arg := &dispatch.Argument{
				All:  sourceArg.All,
				Regs: make([]*process.WaitRegister, len(sourceArg.Regs)),
			}
			for j := range arg.Regs {
				sourceReg := sourceArg.Regs[j]
				if arg.Regs[j], ok = regMap[sourceReg]; !ok {
					panic("nonexistent wait register")
				}
			}
			res.Arg = arg
		}
	default:
		panic(fmt.Sprintf("unexpected instruction type '%d' to dup", sourceIns.Op))
	}
	return res
}

func constructRestrict(n *plan.Node) *restrict.Argument {
	return &restrict.Argument{
		E: colexec.RewriteFilterExprList(n.FilterList),
	}
}

func constructDeletion(n *plan.Node, eg engine.Engine, proc *process.Process) (*deletion.Argument, error) {
	count := len(n.DeleteTablesCtx)
	ds := make([]*deletion.DeleteCtx, count)
	for i := 0; i < count; i++ {
		dbSource, err := eg.Database(proc.Ctx, n.DeleteTablesCtx[i].DbName, proc.TxnOperator)
		if err != nil {
			return nil, err
		}
		relation, err := dbSource.Relation(proc.Ctx, n.DeleteTablesCtx[i].TblName)
		if err != nil {
			return nil, err
		}

		indexTables := make([]engine.Relation, 0)
		for _, info := range n.DeleteTablesCtx[i].IndexInfos {
			indexTable, err := dbSource.Relation(proc.Ctx, info.TableName)
			if err != nil {
				return nil, err
			}
			indexTables = append(indexTables, indexTable)
		}

		ds[i] = &deletion.DeleteCtx{
			TableSource:  relation,
			TableName:    n.DeleteTablesCtx[i].TblName,
			DbName:       n.DeleteTablesCtx[i].DbName,
			UseDeleteKey: n.DeleteTablesCtx[i].UseDeleteKey,
			CanTruncate:  n.DeleteTablesCtx[i].CanTruncate,
			ColIndex:     n.DeleteTablesCtx[i].ColIndex,
			IndexInfos:   n.DeleteTablesCtx[i].IndexInfos,
			IndexTables:  indexTables,
			IndexAttrs:   n.DeleteTablesCtx[i].IndexAttrs,
		}
	}

	return &deletion.Argument{
		DeleteCtxs: ds,
	}, nil
}

func constructInsert(n *plan.Node, eg engine.Engine, proc *process.Process) (*insert.Argument, error) {
	db, err := eg.Database(proc.Ctx, n.ObjRef.SchemaName, proc.TxnOperator)
	if err != nil {
		return nil, err
	}
	relation, err := db.Relation(proc.Ctx, n.TableDef.Name)
	if err != nil {
		return nil, err
	}
	indexTables := make([]engine.Relation, 0)
	for _, info := range n.TableDef.IndexInfos {
		indexTable, err := db.Relation(proc.Ctx, info.TableName)
		if err != nil {
			return nil, err
		}
		indexTables = append(indexTables, indexTable)
	}
	return &insert.Argument{
		TargetTable:   relation,
		TargetColDefs: n.TableDef.Cols,
		Engine:        eg,
		DB:            db,
		TableID:       relation.GetTableID(proc.Ctx),
		DBName:        n.ObjRef.SchemaName,
		TableName:     n.TableDef.Name,
		CPkeyColDef:   n.TableDef.CompositePkey,
		IndexTables:   indexTables,
		IndexInfos:    n.TableDef.IndexInfos,
	}, nil
}

func constructUpdate(n *plan.Node, eg engine.Engine, proc *process.Process) (*update.Argument, error) {
	us := make([]*update.UpdateCtx, len(n.UpdateCtxs))
	tableID := make([]string, len(n.UpdateCtxs))
	db := make([]engine.Database, len(n.UpdateCtxs))
	dbName := make([]string, len(n.UpdateCtxs))
	tblName := make([]string, len(n.UpdateCtxs))
	for i, updateCtx := range n.UpdateCtxs {
		dbSource, err := eg.Database(proc.Ctx, updateCtx.DbName, proc.TxnOperator)
		if err != nil {
			return nil, err
		}
		db[i] = dbSource
		relation, err := dbSource.Relation(proc.Ctx, updateCtx.TblName)
		if err != nil {
			return nil, err
		}

		tableID[i] = relation.GetTableID(proc.Ctx)
		dbName[i] = updateCtx.DbName
		tblName[i] = updateCtx.TblName
		colNames := make([]string, 0, len(updateCtx.UpdateCols))
		for _, col := range updateCtx.UpdateCols {
			colNames = append(colNames, col.Name)
		}

		var k int
		for k = 0; k < len(n.TableDefVec); k++ {
			if updateCtx.TblName == n.TableDefVec[k].Name {
				break
			}
		}
		indexTables := make([]engine.Relation, 0)
		for _, info := range n.TableDefVec[k].IndexInfos {
			indexTable, err := dbSource.Relation(proc.Ctx, info.TableName)
			if err != nil {
				return nil, err
			}
			indexTables = append(indexTables, indexTable)
		}

		us[i] = &update.UpdateCtx{
			PriKey:      updateCtx.PriKey,
			PriKeyIdx:   updateCtx.PriKeyIdx,
			HideKey:     updateCtx.HideKey,
			HideKeyIdx:  updateCtx.HideKeyIdx,
			UpdateAttrs: colNames,
			OtherAttrs:  updateCtx.OtherAttrs,
			OrderAttrs:  updateCtx.OrderAttrs,
			TableSource: relation,
			CPkeyColDef: updateCtx.CompositePkey,
			IndexInfos:  n.TableDefVec[k].IndexInfos,
			IndexTables: indexTables,
			IndexAttrs:  updateCtx.IndexAttrs,
		}
	}
	return &update.Argument{
		UpdateCtxs:  us,
		Engine:      eg,
		DB:          db,
		TableID:     tableID,
		DBName:      dbName,
		TblName:     tblName,
		TableDefVec: n.TableDefVec,
	}, nil
}

func constructProjection(n *plan.Node) *projection.Argument {
	return &projection.Argument{
		Es: n.ProjectList,
	}
}

func constructExternal(n *plan.Node, ctx context.Context, fileList []string) *external.Argument {
	attrs := make([]string, len(n.TableDef.Cols))
	for j, col := range n.TableDef.Cols {
		attrs[j] = col.Name
	}
	return &external.Argument{
		Es: &external.ExternalParam{
			Attrs:         attrs,
			Cols:          n.TableDef.Cols,
			Name2ColIndex: n.TableDef.Name2ColIndex,
			CreateSql:     n.TableDef.Createsql,
			Ctx:           ctx,
			FileList:      fileList,
			Fileparam:     new(external.ExternalFileparam),
		},
	}
}
func constructTableFunction(n *plan.Node, ctx context.Context, name string) *table_function.Argument {
	attrs := make([]string, len(n.TableDef.Cols))
	for j, col := range n.TableDef.Cols {
		attrs[j] = col.Name
	}
	return &table_function.Argument{
		Attrs:  attrs,
		Rets:   n.TableDef.Cols,
		Args:   n.TblFuncExprList,
		Name:   name,
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
		result[i].Rel, result[i].Pos = constructJoinResult(expr)
	}
	cond, conds := extraJoinConditions(n.OnList)
	return &join.Argument{
		Typs:       typs,
		Result:     result,
		Cond:       cond,
		Conditions: constructJoinConditions(conds),
	}
}

func constructSemi(n *plan.Node, typs []types.Type, proc *process.Process) *semi.Argument {
	result := make([]int32, len(n.ProjectList))
	for i, expr := range n.ProjectList {
		rel, pos := constructJoinResult(expr)
		if rel != 0 {
			panic(moerr.NewNYI("semi result '%s'", expr))
		}
		result[i] = pos
	}
	cond, conds := extraJoinConditions(n.OnList)
	return &semi.Argument{
		Typs:       typs,
		Result:     result,
		Cond:       cond,
		Conditions: constructJoinConditions(conds),
	}
}

func constructLeft(n *plan.Node, typs []types.Type, proc *process.Process) *left.Argument {
	result := make([]colexec.ResultPos, len(n.ProjectList))
	for i, expr := range n.ProjectList {
		result[i].Rel, result[i].Pos = constructJoinResult(expr)
	}
	cond, conds := extraJoinConditions(n.OnList)
	return &left.Argument{
		Typs:       typs,
		Result:     result,
		Cond:       cond,
		Conditions: constructJoinConditions(conds),
	}
}

func constructSingle(n *plan.Node, typs []types.Type, proc *process.Process) *single.Argument {
	result := make([]colexec.ResultPos, len(n.ProjectList))
	for i, expr := range n.ProjectList {
		result[i].Rel, result[i].Pos = constructJoinResult(expr)
	}
	cond, conds := extraJoinConditions(n.OnList)
	return &single.Argument{
		Typs:       typs,
		Result:     result,
		Cond:       cond,
		Conditions: constructJoinConditions(conds),
	}
}

func constructProduct(n *plan.Node, typs []types.Type, proc *process.Process) *product.Argument {
	result := make([]colexec.ResultPos, len(n.ProjectList))
	for i, expr := range n.ProjectList {
		result[i].Rel, result[i].Pos = constructJoinResult(expr)
	}
	return &product.Argument{Typs: typs, Result: result}
}

func constructAnti(n *plan.Node, typs []types.Type, proc *process.Process) *anti.Argument {
	result := make([]int32, len(n.ProjectList))
	for i, expr := range n.ProjectList {
		rel, pos := constructJoinResult(expr)
		if rel != 0 {
			panic(moerr.NewNYI("anti result '%s'", expr))
		}
		result[i] = pos
	}
	cond, conds := extraJoinConditions(n.OnList)
	return &anti.Argument{
		Typs:       typs,
		Result:     result,
		Cond:       cond,
		Conditions: constructJoinConditions(conds),
	}
}

func constructMark(n *plan.Node, typs []types.Type, proc *process.Process, onList []*plan.Expr) *mark.Argument {
	result := make([]int32, len(n.ProjectList))
	for i, expr := range n.ProjectList {
		rel, pos := constructJoinResult(expr)
		if rel != 0 {
			panic(moerr.NewNYI("mark result '%s'", expr))
		}
		result[i] = pos
	}
	cond, conds := extraJoinConditions(n.OnList)
	return &mark.Argument{
		Typs:       typs,
		Result:     result,
		Cond:       cond,
		Conditions: constructJoinConditions(conds),
		OnList:     onList,
	}
}

var _ = constructMark

func constructOrder(n *plan.Node, proc *process.Process) *order.Argument {
	return &order.Argument{
		Fs: n.OrderBy,
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
	vec, err := colexec.EvalExpr(constBat, proc, n.Limit)
	if err != nil {
		panic(err)
	}
	defer vec.Free(proc.Mp())
	return &limit.Argument{
		Limit: uint64(vec.Col.([]int64)[0]),
	}
}

func constructGroup(n, cn *plan.Node, ibucket, nbucket int, needEval bool) *group.Argument {
	aggs := make([]agg.Aggregate, len(n.AggList))
	for i, expr := range n.AggList {
		if f, ok := expr.Expr.(*plan.Expr_F); ok {
			distinct := (uint64(f.F.Func.Obj) & function.Distinct) != 0
			obj := int64(uint64(f.F.Func.Obj) & function.DistinctMask)
			fun, err := function.GetFunctionByID(obj)
			if err != nil {
				panic(err)
			}
			aggs[i] = agg.Aggregate{
				E:    f.F.Args[0],
				Dist: distinct,
				Op:   fun.AggregateInfo,
			}
		}
	}
	typs := make([]types.Type, len(cn.ProjectList))
	for i, e := range cn.ProjectList {
		typs[i].Oid = types.T(e.Typ.Id)
		typs[i].Width = e.Typ.Width
		typs[i].Size = e.Typ.Size
		typs[i].Scale = e.Typ.Scale
		typs[i].Precision = e.Typ.Precision
	}
	return &group.Argument{
		Aggs:     aggs,
		Types:    typs,
		NeedEval: needEval,
		Exprs:    n.GroupBy,
		Ibucket:  uint64(ibucket),
		Nbucket:  uint64(nbucket),
	}
}

// ibucket: bucket number
// nbucket:
// construct operator argument
func constructIntersectAll(_ *plan.Node, proc *process.Process, ibucket, nbucket int) *intersectall.Argument {
	return &intersectall.Argument{
		IBucket: uint64(ibucket),
		NBucket: uint64(nbucket),
	}
}

func constructMinus(n *plan.Node, proc *process.Process, ibucket, nbucket int) *minus.Argument {
	return &minus.Argument{
		IBucket: uint64(ibucket),
		NBucket: uint64(nbucket),
	}
}

func constructIntersect(n *plan.Node, proc *process.Process, ibucket, nbucket int) *intersect.Argument {
	return &intersect.Argument{
		IBucket: uint64(ibucket),
		NBucket: uint64(nbucket),
	}
}

func constructDispatch(all bool, regs []*process.WaitRegister) *dispatch.Argument {
	arg := new(dispatch.Argument)
	arg.All = all
	arg.Regs = regs
	return arg
}

func constructMergeGroup(_ *plan.Node, needEval bool) *mergegroup.Argument {
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
	vec, err := colexec.EvalExpr(constBat, proc, n.Offset)
	if err != nil {
		panic(err)
	}
	defer vec.Free(proc.Mp())
	return &mergeoffset.Argument{
		Offset: uint64(vec.Col.([]int64)[0]),
	}
}

func constructMergeLimit(n *plan.Node, proc *process.Process) *mergelimit.Argument {
	vec, err := colexec.EvalExpr(constBat, proc, n.Limit)
	if err != nil {
		panic(err)
	}
	defer vec.Free(proc.Mp())
	return &mergelimit.Argument{
		Limit: uint64(vec.Col.([]int64)[0]),
	}
}

func constructMergeOrder(n *plan.Node, proc *process.Process) *mergeorder.Argument {
	return &mergeorder.Argument{
		Fs: n.OrderBy,
	}
}

func constructLoopJoin(n *plan.Node, typs []types.Type, proc *process.Process) *loopjoin.Argument {
	result := make([]colexec.ResultPos, len(n.ProjectList))
	for i, expr := range n.ProjectList {
		result[i].Rel, result[i].Pos = constructJoinResult(expr)
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
		rel, pos := constructJoinResult(expr)
		if rel != 0 {
			panic(moerr.NewNYI("loop semi result '%s'", expr))
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
		result[i].Rel, result[i].Pos = constructJoinResult(expr)
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
		result[i].Rel, result[i].Pos = constructJoinResult(expr)
	}
	return &loopsingle.Argument{
		Result: result,
		Cond:   colexec.RewriteFilterExprList(n.OnList),
	}
}

func constructLoopAnti(n *plan.Node, typs []types.Type, proc *process.Process) *loopanti.Argument {
	result := make([]int32, len(n.ProjectList))
	for i, expr := range n.ProjectList {
		rel, pos := constructJoinResult(expr)
		if rel != 0 {
			panic(moerr.NewNYI("loop anti result '%s'", expr))
		}
		result[i] = pos
	}
	return &loopanti.Argument{
		Typs:   typs,
		Result: result,
		Cond:   colexec.RewriteFilterExprList(n.OnList),
	}
}

func constructHashBuild(in vm.Instruction) *hashbuild.Argument {
	switch in.Op {
	case vm.Anti:
		arg := in.Arg.(*anti.Argument)
		return &hashbuild.Argument{
			NeedHashMap: true,
			Typs:        arg.Typs,
			Conditions:  arg.Conditions[1],
		}
	case vm.Mark:
		arg := in.Arg.(*mark.Argument)
		return &hashbuild.Argument{
			NeedHashMap: true,
			Typs:        arg.Typs,
			Conditions:  arg.Conditions[1],
		}
	case vm.Join:
		arg := in.Arg.(*join.Argument)
		return &hashbuild.Argument{
			NeedHashMap: true,
			Typs:        arg.Typs,
			Conditions:  arg.Conditions[1],
		}
	case vm.Left:
		arg := in.Arg.(*left.Argument)
		return &hashbuild.Argument{
			NeedHashMap: true,
			Typs:        arg.Typs,
			Conditions:  arg.Conditions[1],
		}
	case vm.Semi:
		arg := in.Arg.(*semi.Argument)
		return &hashbuild.Argument{
			NeedHashMap: true,
			Typs:        arg.Typs,
			Conditions:  arg.Conditions[1],
		}
	case vm.Single:
		arg := in.Arg.(*single.Argument)
		return &hashbuild.Argument{
			NeedHashMap: true,
			Typs:        arg.Typs,
			Conditions:  arg.Conditions[1],
		}
	case vm.Product:
		arg := in.Arg.(*product.Argument)
		return &hashbuild.Argument{
			NeedHashMap: false,
			Typs:        arg.Typs,
		}
	case vm.LoopAnti:
		arg := in.Arg.(*loopanti.Argument)
		return &hashbuild.Argument{
			NeedHashMap: false,
			Typs:        arg.Typs,
		}
	case vm.LoopJoin:
		arg := in.Arg.(*loopjoin.Argument)
		return &hashbuild.Argument{
			NeedHashMap: false,
			Typs:        arg.Typs,
		}
	case vm.LoopLeft:
		arg := in.Arg.(*loopleft.Argument)
		return &hashbuild.Argument{
			NeedHashMap: false,
			Typs:        arg.Typs,
		}
	case vm.LoopSemi:
		arg := in.Arg.(*loopsemi.Argument)
		return &hashbuild.Argument{
			NeedHashMap: false,
			Typs:        arg.Typs,
		}
	case vm.LoopSingle:
		arg := in.Arg.(*loopsingle.Argument)
		return &hashbuild.Argument{
			NeedHashMap: false,
			Typs:        arg.Typs,
		}

	default:
		panic(moerr.NewInternalError("unsupport join type '%v'", in.Op))
	}
}

func constructJoinResult(expr *plan.Expr) (int32, int32) {
	e, ok := expr.Expr.(*plan.Expr_Col)
	if !ok {
		panic(moerr.NewNYI("join result '%s'", expr))
	}
	return e.Col.RelPos, e.Col.ColPos
}

func constructJoinConditions(exprs []*plan.Expr) [][]*plan.Expr {
	conds := make([][]*plan.Expr, 2)
	conds[0] = make([]*plan.Expr, len(exprs))
	conds[1] = make([]*plan.Expr, len(exprs))
	for i, expr := range exprs {
		conds[0][i], conds[1][i] = constructJoinCondition(expr)
	}
	return conds
}

func constructJoinCondition(expr *plan.Expr) (*plan.Expr, *plan.Expr) {
	if e, ok := expr.Expr.(*plan.Expr_C); ok { // constant bool
		b, ok := e.C.Value.(*plan.Const_Bval)
		if !ok {
			panic(moerr.NewNYI("join condition '%s'", expr))
		}
		if b.Bval {
			return expr, expr
		}
		return expr, &plan.Expr{
			Typ: expr.Typ,
			Expr: &plan.Expr_C{
				C: &plan.Const{
					Value: &plan.Const_Bval{Bval: true},
				},
			},
		}
	}
	e, ok := expr.Expr.(*plan.Expr_F)
	if !ok || !supportedJoinCondition(e.F.Func.GetObj()) {
		panic(moerr.NewNYI("join condition '%s'", expr))
	}
	if exprRelPos(e.F.Args[0]) == 1 {
		return e.F.Args[1], e.F.Args[0]
	}
	return e.F.Args[0], e.F.Args[1]
}

func isEquiJoin(exprs []*plan.Expr) bool {
	for _, expr := range exprs {
		if e, ok := expr.Expr.(*plan.Expr_F); ok {
			if !supportedJoinCondition(e.F.Func.GetObj()) {
				continue
			}
			lpos, rpos := hasColExpr(e.F.Args[0], -1), hasColExpr(e.F.Args[1], -1)
			if lpos == -1 || rpos == -1 || (lpos == rpos) {
				continue
			}
			return true
		}
	}
	return false || isEquiJoin0(exprs)
}

func isEquiJoin0(exprs []*plan.Expr) bool {
	for _, expr := range exprs {
		if e, ok := expr.Expr.(*plan.Expr_F); ok {
			if !supportedJoinCondition(e.F.Func.GetObj()) {
				return false
			}
			lpos, rpos := hasColExpr(e.F.Args[0], -1), hasColExpr(e.F.Args[1], -1)
			if lpos == -1 || rpos == -1 || (lpos == rpos) {
				return false
			}
		}
	}
	return true
}

func extraJoinConditions(exprs []*plan.Expr) (*plan.Expr, []*plan.Expr) {
	exprs = colexec.SplitAndExprs(exprs)
	eqConds := make([]*plan.Expr, 0, len(exprs))
	notEqConds := make([]*plan.Expr, 0, len(exprs))
	for i, expr := range exprs {
		if e, ok := expr.Expr.(*plan.Expr_F); ok {
			if !supportedJoinCondition(e.F.Func.GetObj()) {
				notEqConds = append(notEqConds, exprs[i])
				continue
			}
			lpos, rpos := hasColExpr(e.F.Args[0], -1), hasColExpr(e.F.Args[1], -1)
			if lpos == -1 || rpos == -1 || (lpos == rpos) {
				notEqConds = append(notEqConds, exprs[i])
				continue
			}
			eqConds = append(eqConds, exprs[i])
		}
	}
	if len(notEqConds) == 0 {
		return nil, eqConds
	}
	return colexec.RewriteFilterExprList(notEqConds), eqConds
}

func supportedJoinCondition(id int64) bool {
	fid, _ := function.DecodeOverloadID(id)
	return fid == function.EQUAL
}

func hasColExpr(expr *plan.Expr, pos int32) int32 {
	switch e := expr.Expr.(type) {
	case *plan.Expr_Col:
		if pos == -1 {
			return e.Col.RelPos
		}
		if pos != e.Col.RelPos {
			return -1
		}
		return pos
	case *plan.Expr_F:
		for i := range e.F.Args {
			pos0 := hasColExpr(e.F.Args[i], pos)
			switch {
			case pos0 == -1:
			case pos == -1:
				pos = pos0
			case pos != pos0:
				return -1
			}
		}
		return pos
	default:
		return pos
	}
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

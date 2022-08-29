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

	"github.com/matrixorigin/matrixone/pkg/sql/colexec/intersectall"

	"github.com/matrixorigin/matrixone/pkg/sql/colexec/anti"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/external"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/hashbuild"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/intersect"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/loopanti"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/mark"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/minus"

	"github.com/matrixorigin/matrixone/pkg/sql/colexec/loopjoin"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/loopleft"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/loopsemi"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/loopsingle"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/update"

	"github.com/matrixorigin/matrixone/pkg/sql/colexec/deletion"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/insert"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/errno"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/aggregate"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/connector"
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
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/offset"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/order"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/output"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/product"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/projection"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/restrict"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/semi"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/single"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/top"
	"github.com/matrixorigin/matrixone/pkg/sql/errors"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

var constBat *batch.Batch

func init() {
	constBat = batch.NewWithSize(0)
	constBat.Zs = []int64{1}
}

func dupInstruction(in vm.Instruction) vm.Instruction {
	rin := vm.Instruction{
		Op:  in.Op,
		Idx: in.Idx,
	}
	switch arg := in.Arg.(type) {
	case *top.Argument:
		rin.Arg = &top.Argument{
			Fs:    arg.Fs,
			Limit: arg.Limit,
		}
	case *limit.Argument:
		rin.Arg = &limit.Argument{
			Limit: arg.Limit,
		}
	case *join.Argument:
		rin.Arg = &join.Argument{
			Typs:       arg.Typs,
			Cond:       arg.Cond,
			Result:     arg.Result,
			Conditions: arg.Conditions,
		}
	case *semi.Argument:
		rin.Arg = &semi.Argument{
			Typs:       arg.Typs,
			Cond:       arg.Cond,
			Result:     arg.Result,
			Conditions: arg.Conditions,
		}
	case *left.Argument:
		rin.Arg = &left.Argument{
			Typs:       arg.Typs,
			Cond:       arg.Cond,
			Result:     arg.Result,
			Conditions: arg.Conditions,
		}
	case *group.Argument:
		rin.Arg = &group.Argument{
			Aggs:    arg.Aggs,
			Exprs:   arg.Exprs,
			Types:   arg.Types,
			Ibucket: arg.Ibucket,
			Nbucket: arg.Nbucket,
		}
	case *single.Argument:
		rin.Arg = &single.Argument{
			Typs:       arg.Typs,
			Cond:       arg.Cond,
			Result:     arg.Result,
			Conditions: arg.Conditions,
		}
	case *product.Argument:
		rin.Arg = &product.Argument{
			Typs:   arg.Typs,
			Result: arg.Result,
		}
	case *anti.Argument:
		rin.Arg = &anti.Argument{
			Typs:       arg.Typs,
			Cond:       arg.Cond,
			Result:     arg.Result,
			Conditions: arg.Conditions,
		}
	case *mark.Argument:
		{
			rin.Arg = &mark.Argument{
				Typs:         arg.Typs,
				Cond:         arg.Cond,
				Result:       arg.Result,
				Conditions:   arg.Conditions,
				OutputNull:   arg.OutputNull,
				OutputMark:   arg.OutputMark,
				MarkMeaning:  arg.MarkMeaning,
				OutputAnyway: arg.OutputAnyway,
				OnList:       arg.OnList,
			}
		}
	case *offset.Argument:
		rin.Arg = &offset.Argument{
			Offset: arg.Offset,
		}
	case *order.Argument:
		rin.Arg = &order.Argument{
			Fs: arg.Fs,
		}
	case *projection.Argument:
		rin.Arg = &projection.Argument{
			Es: arg.Es,
		}
	case *restrict.Argument:
		rin.Arg = &restrict.Argument{
			E: arg.E,
		}
	case *output.Argument:
		rin.Arg = &output.Argument{
			Data: arg.Data,
			Func: arg.Func,
		}
	case *loopjoin.Argument:
		rin.Arg = &loopjoin.Argument{
			Typs:   arg.Typs,
			Cond:   arg.Cond,
			Result: arg.Result,
		}
	case *loopsemi.Argument:
		rin.Arg = &loopsemi.Argument{
			Typs:   arg.Typs,
			Cond:   arg.Cond,
			Result: arg.Result,
		}
	case *loopleft.Argument:
		rin.Arg = &loopleft.Argument{
			Typs:   arg.Typs,
			Cond:   arg.Cond,
			Result: arg.Result,
		}
	case *loopanti.Argument:
		rin.Arg = &loopanti.Argument{
			Typs:   arg.Typs,
			Cond:   arg.Cond,
			Result: arg.Result,
		}
	case *loopsingle.Argument:
		rin.Arg = &loopsingle.Argument{
			Typs:   arg.Typs,
			Cond:   arg.Cond,
			Result: arg.Result,
		}
	case *dispatch.Argument:
	case *connector.Argument:
	case *minus.Argument:
		rin.Arg = &minus.Argument{
			IBucket: arg.IBucket,
			NBucket: arg.NBucket,
		}
	case *intersect.Argument:
		rin.Arg = &intersect.Argument{
			IBucket: arg.IBucket,
			NBucket: arg.NBucket,
		}
	case *intersectall.Argument:
		rin.Arg = &intersectall.Argument{
			IBucket: arg.IBucket,
			NBucket: arg.NBucket,
		}
	case *external.Argument:
		rin.Arg = &external.Argument{
			Es: arg.Es,
		}
	default:
		panic(errors.New(errno.SyntaxErrororAccessRuleViolation, fmt.Sprintf("Unsupport instruction %T\n", in.Arg)))
	}
	return rin
}

func constructRestrict(n *plan.Node) *restrict.Argument {
	return &restrict.Argument{
		E: colexec.RewriteFilterExprList(n.FilterList),
	}
}

func constructDeletion(n *plan.Node, eg engine.Engine, txnOperator TxnOperator) (*deletion.Argument, error) {
	ctx := context.TODO()
	count := len(n.DeleteTablesCtx)
	ds := make([]*deletion.DeleteCtx, count)
	for i := 0; i < count; i++ {

		dbSource, err := eg.Database(ctx, n.DeleteTablesCtx[i].DbName, txnOperator)
		if err != nil {
			return nil, err
		}
		relation, err := dbSource.Relation(ctx, n.DeleteTablesCtx[i].TblName)
		if err != nil {
			return nil, err
		}

		ds[i] = &deletion.DeleteCtx{
			TableSource:  relation,
			UseDeleteKey: n.DeleteTablesCtx[i].UseDeleteKey,
			CanTruncate:  n.DeleteTablesCtx[i].CanTruncate,
			IsHideKey:    n.DeleteTablesCtx[i].IsHideKey,
		}
	}

	return &deletion.Argument{
		DeleteCtxs: ds,
	}, nil
}

func constructInsert(n *plan.Node, eg engine.Engine, txnOperator TxnOperator) (*insert.Argument, error) {
	ctx := context.TODO()
	db, err := eg.Database(ctx, n.ObjRef.SchemaName, txnOperator)
	if err != nil {
		return nil, err
	}
	relation, err := db.Relation(ctx, n.TableDef.Name)
	if err != nil {
		return nil, err
	}
	return &insert.Argument{
		TargetTable:   relation,
		TargetColDefs: n.TableDef.Cols,
	}, nil
}

func constructUpdate(n *plan.Node, eg engine.Engine, txnOperator TxnOperator) (*update.Argument, error) {
	ctx := context.TODO()
	us := make([]*update.UpdateCtx, len(n.UpdateCtxs))
	for i, updateCtx := range n.UpdateCtxs {

		dbSource, err := eg.Database(ctx, updateCtx.DbName, txnOperator)
		if err != nil {
			return nil, err
		}
		relation, err := dbSource.Relation(ctx, updateCtx.TblName)
		if err != nil {
			return nil, err
		}

		colNames := make([]string, 0, len(updateCtx.UpdateCols))
		for _, col := range updateCtx.UpdateCols {
			colNames = append(colNames, col.Name)
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
		}
	}
	return &update.Argument{
		UpdateCtxs: us,
	}, nil
}

func constructProjection(n *plan.Node) *projection.Argument {
	return &projection.Argument{
		Es: n.ProjectList,
	}
}

func constructExternal(n *plan.Node, ctx context.Context) *external.Argument {
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
		},
	}
}

func constructTop(n *plan.Node, proc *process.Process) *top.Argument {
	vec, err := colexec.EvalExpr(constBat, proc, n.Limit)
	if err != nil {
		panic(err)
	}
	fs := make([]colexec.Field, len(n.OrderBy))
	for i, e := range n.OrderBy {
		fs[i].E = e.Expr
		if e.Flag == plan.OrderBySpec_DESC {
			fs[i].Type = colexec.Descending
		}
	}
	return &top.Argument{
		Fs:    fs,
		Limit: vec.Col.([]int64)[0],
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
			panic(errors.New(errno.SyntaxErrororAccessRuleViolation, fmt.Sprintf("semi result '%s' not support now", expr)))
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
			panic(errors.New(errno.SyntaxErrororAccessRuleViolation, fmt.Sprintf("anti result '%s' not support now", expr)))
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
			panic(errors.New(errno.SyntaxErrororAccessRuleViolation, fmt.Sprintf("mark result '%s' not support now", expr)))
		}
		result[i] = pos
	}
	cond, conds := extraJoinConditions(n.OnList)
	return &mark.Argument{
		Typs:         typs,
		Result:       result,
		Cond:         cond,
		Conditions:   constructJoinConditions(conds),
		OutputMark:   false,
		OutputNull:   false,
		MarkMeaning:  false,
		OutputAnyway: false,
		OnList:       onList,
	}
}

var _ = constructMark

func constructOrder(n *plan.Node, proc *process.Process) *order.Argument {
	fs := make([]colexec.Field, len(n.OrderBy))
	for i, e := range n.OrderBy {
		fs[i].E = e.Expr
		if e.Flag == plan.OrderBySpec_DESC {
			fs[i].Type = colexec.Descending
		}
	}
	return &order.Argument{
		Fs: fs,
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
	return &limit.Argument{
		Limit: uint64(vec.Col.([]int64)[0]),
	}
}

func constructGroup(n, cn *plan.Node, ibucket, nbucket int, needEval bool) *group.Argument {
	aggs := make([]aggregate.Aggregate, len(n.AggList))
	for i, expr := range n.AggList {
		if f, ok := expr.Expr.(*plan.Expr_F); ok {
			distinct := (uint64(f.F.Func.Obj) & function.Distinct) != 0
			obj := int64(uint64(f.F.Func.Obj) & function.DistinctMask)
			fun, err := function.GetFunctionByID(obj)
			if err != nil {
				panic(err)
			}
			aggs[i] = aggregate.Aggregate{
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

func constructMergeTop(n *plan.Node, proc *process.Process) *mergetop.Argument {
	vec, err := colexec.EvalExpr(constBat, proc, n.Limit)
	if err != nil {
		panic(err)
	}
	fs := make([]colexec.Field, len(n.OrderBy))
	for i, e := range n.OrderBy {
		fs[i].E = e.Expr
		if e.Flag == plan.OrderBySpec_DESC {
			fs[i].Type = colexec.Descending
		}
	}
	return &mergetop.Argument{
		Fs:    fs,
		Limit: vec.Col.([]int64)[0],
	}
}

func constructMergeOffset(n *plan.Node, proc *process.Process) *mergeoffset.Argument {
	vec, err := colexec.EvalExpr(constBat, proc, n.Offset)
	if err != nil {
		panic(err)
	}
	return &mergeoffset.Argument{
		Offset: uint64(vec.Col.([]int64)[0]),
	}
}

func constructMergeLimit(n *plan.Node, proc *process.Process) *mergelimit.Argument {
	vec, err := colexec.EvalExpr(constBat, proc, n.Limit)
	if err != nil {
		panic(err)
	}
	return &mergelimit.Argument{
		Limit: uint64(vec.Col.([]int64)[0]),
	}
}

func constructMergeOrder(n *plan.Node, proc *process.Process) *mergeorder.Argument {
	fs := make([]colexec.Field, len(n.OrderBy))
	for i, e := range n.OrderBy {
		fs[i].E = e.Expr
		if e.Flag == plan.OrderBySpec_DESC {
			fs[i].Type = colexec.Descending
		}
	}
	return &mergeorder.Argument{
		Fs: fs,
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
			panic(errors.New(errno.SyntaxErrororAccessRuleViolation, fmt.Sprintf("loop semi result '%s' not support now", expr)))
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
			panic(errors.New(errno.SyntaxErrororAccessRuleViolation, fmt.Sprintf("loop anti result '%s' not support now", expr)))
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
		panic(errors.New(errno.SyntaxErrororAccessRuleViolation, fmt.Sprintf("unsupport join type '%v'", in.Op)))
	}
}

func constructJoinResult(expr *plan.Expr) (int32, int32) {
	e, ok := expr.Expr.(*plan.Expr_Col)
	if !ok {
		panic(errors.New(errno.SyntaxErrororAccessRuleViolation, fmt.Sprintf("join result '%s' not support now", expr)))
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
			panic(errors.New(errno.SyntaxErrororAccessRuleViolation, fmt.Sprintf("join condition '%s' not support now", expr)))
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
		panic(errors.New(errno.SyntaxErrororAccessRuleViolation, fmt.Sprintf("join condition '%s' not support now", expr)))
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

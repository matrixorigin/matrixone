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

package compile2

import (
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/sql/colexec2/loopcomplement"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec2/loopjoin"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec2/loopleft"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec2/loopsemi"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec2/update"

	"github.com/matrixorigin/matrixone/pkg/sql/colexec2/deletion"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec2/insert"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/errno"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	colexec "github.com/matrixorigin/matrixone/pkg/sql/colexec2"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec2/aggregate"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec2/complement"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec2/connector"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec2/dispatch"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec2/group"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec2/join"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec2/left"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec2/limit"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec2/mergegroup"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec2/mergelimit"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec2/mergeoffset"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec2/mergeorder"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec2/mergetop"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec2/offset"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec2/order"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec2/output"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec2/product"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec2/projection"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec2/restrict"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec2/semi"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec2/top"
	"github.com/matrixorigin/matrixone/pkg/sql/errors"
	"github.com/matrixorigin/matrixone/pkg/sql/plan2/function"
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
		Op: in.Op,
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
			IsPreBuild: arg.IsPreBuild,
			Result:     arg.Result,
			Conditions: arg.Conditions,
		}
	case *semi.Argument:
		rin.Arg = &semi.Argument{
			IsPreBuild: arg.IsPreBuild,
			Result:     arg.Result,
			Conditions: arg.Conditions,
		}
	case *left.Argument:
		rin.Arg = &left.Argument{
			IsPreBuild: arg.IsPreBuild,
			Result:     arg.Result,
			Conditions: arg.Conditions,
		}
	case *product.Argument:
		rin.Arg = &product.Argument{
			Result: arg.Result,
		}
	case *complement.Argument:
		rin.Arg = &complement.Argument{
			IsPreBuild: arg.IsPreBuild,
			Result:     arg.Result,
			Conditions: arg.Conditions,
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
			Cond:   arg.Cond,
			Result: arg.Result,
		}
	case *loopsemi.Argument:
		rin.Arg = &loopsemi.Argument{
			Cond:   arg.Cond,
			Result: arg.Result,
		}
	case *loopleft.Argument:
		rin.Arg = &loopleft.Argument{
			Cond:   arg.Cond,
			Result: arg.Result,
		}
	case *loopcomplement.Argument:
		rin.Arg = &loopcomplement.Argument{
			Cond:   arg.Cond,
			Result: arg.Result,
		}
	case *dispatch.Argument:
	case *connector.Argument:
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

func constructDeletion(n *plan.Node, eg engine.Engine, snapshot engine.Snapshot) (*deletion.Argument, error) {
	dbSource, err := eg.Database(n.ObjRef.SchemaName, snapshot)
	if err != nil {
		return nil, err
	}
	relation, err := dbSource.Relation(n.TableDef.Name, snapshot)
	if err != nil {
		return nil, err
	}
	return &deletion.Argument{
		TableSource:  relation,
		UseDeleteKey: n.DeleteInfo.UseDeleteKey,
		CanTruncate:  n.DeleteInfo.CanTruncate,
	}, nil
}

func constructInsert(n *plan.Node, eg engine.Engine, snapshot engine.Snapshot) (*insert.Argument, error) {
	db, err := eg.Database(n.ObjRef.SchemaName, snapshot)
	if err != nil {
		return nil, err
	}
	relation, err := db.Relation(n.TableDef.Name, snapshot)
	if err != nil {
		return nil, err
	}
	return &insert.Argument{
		TargetTable:   relation,
		TargetColDefs: n.TableDef.Cols,
	}, nil
}

func constructUpdate(n *plan.Node, eg engine.Engine, snapshot engine.Snapshot) (*update.Argument, error) {
	dbSource, err := eg.Database(n.ObjRef.SchemaName, snapshot)
	if err != nil {
		return nil, err
	}
	relation, err := dbSource.Relation(n.TableDef.Name, snapshot)
	if err != nil {
		return nil, err
	}
	return &update.Argument{
		TableSource: relation,
		PriKey:      n.UpdateInfo.PriKey,
		PriKeyIdx:   n.UpdateInfo.PriKeyIdx,
		HideKey:     n.UpdateInfo.HideKey,
		UpdateAttrs: n.UpdateInfo.UpdateAttrs,
		OtherAttrs:  n.UpdateInfo.OtherAttrs,
		AttrOrders:  n.UpdateInfo.AttrOrders,
	}, nil
}

func constructProjection(n *plan.Node) *projection.Argument {
	return &projection.Argument{
		Es: n.ProjectList,
	}
}

func constructTop(n *plan.Node, proc *process.Process) *top.Argument {
	vec, err := colexec.EvalExpr(constBat, proc, n.Limit)
	if err != nil {
		panic(err)
	}
	fs := make([]top.Field, len(n.OrderBy))
	for i, e := range n.OrderBy {
		fs[i].E = e.Expr
		if e.Flag == plan.OrderBySpec_DESC {
			fs[i].Type = top.Descending
		}
	}
	return &top.Argument{
		Fs:    fs,
		Limit: vec.Col.([]int64)[0],
	}
}

func constructJoin(n *plan.Node, proc *process.Process) *join.Argument {
	result := make([]join.ResultPos, len(n.ProjectList))
	for i, expr := range n.ProjectList {
		result[i].Rel, result[i].Pos = constructJoinResult(expr)
	}
	conds := make([][]join.Condition, 2)
	{
		conds[0] = make([]join.Condition, len(n.OnList))
		conds[1] = make([]join.Condition, len(n.OnList))
	}
	for i, expr := range n.OnList {
		conds[0][i].Expr, conds[1][i].Expr = constructJoinCondition(expr)
	}
	return &join.Argument{
		IsPreBuild: false,
		Conditions: conds,
		Result:     result,
	}
}

func constructSemi(n *plan.Node, proc *process.Process) *semi.Argument {
	result := make([]int32, len(n.ProjectList))
	for i, expr := range n.ProjectList {
		rel, pos := constructJoinResult(expr)
		if rel != 0 {
			panic(errors.New(errno.SyntaxErrororAccessRuleViolation, fmt.Sprintf("complement result '%s' not support now", expr)))
		}
		result[i] = pos
	}
	conds := make([][]semi.Condition, 2)
	{
		conds[0] = make([]semi.Condition, len(n.OnList))
		conds[1] = make([]semi.Condition, len(n.OnList))
	}
	for i, expr := range n.OnList {
		conds[0][i].Expr, conds[1][i].Expr = constructJoinCondition(expr)
	}
	return &semi.Argument{
		IsPreBuild: false,
		Conditions: conds,
		Result:     result,
	}
}

func constructLeft(n *plan.Node, proc *process.Process) *left.Argument {
	result := make([]left.ResultPos, len(n.ProjectList))
	for i, expr := range n.ProjectList {
		result[i].Rel, result[i].Pos = constructJoinResult(expr)
	}
	conds := make([][]left.Condition, 2)
	{
		conds[0] = make([]left.Condition, len(n.OnList))
		conds[1] = make([]left.Condition, len(n.OnList))
	}
	for i, expr := range n.OnList {
		conds[0][i].Expr, conds[1][i].Expr = constructJoinCondition(expr)
	}
	return &left.Argument{
		IsPreBuild: false,
		Conditions: conds,
		Result:     result,
	}
}

func constructProduct(n *plan.Node, proc *process.Process) *product.Argument {
	result := make([]product.ResultPos, len(n.ProjectList))
	for i, expr := range n.ProjectList {
		result[i].Rel, result[i].Pos = constructJoinResult(expr)
	}
	return &product.Argument{Result: result}
}

func constructComplement(n *plan.Node, proc *process.Process) *complement.Argument {
	result := make([]int32, len(n.ProjectList))
	for i, expr := range n.ProjectList {
		rel, pos := constructJoinResult(expr)
		if rel != 0 {
			panic(errors.New(errno.SyntaxErrororAccessRuleViolation, fmt.Sprintf("complement result '%s' not support now", expr)))
		}
		result[i] = pos
	}
	conds := make([][]complement.Condition, 2)
	{
		conds[0] = make([]complement.Condition, len(n.OnList))
		conds[1] = make([]complement.Condition, len(n.OnList))
	}
	for i, expr := range n.OnList {
		conds[0][i].Expr, conds[1][i].Expr = constructJoinCondition(expr)
	}
	return &complement.Argument{
		IsPreBuild: false,
		Conditions: conds,
		Result:     result,
	}
}

func constructOrder(n *plan.Node, proc *process.Process) *order.Argument {
	fs := make([]order.Field, len(n.OrderBy))
	for i, e := range n.OrderBy {
		fs[i].E = e.Expr
		if e.Flag == plan.OrderBySpec_DESC {
			fs[i].Type = order.Descending
		}
	}
	return &order.Argument{
		Fs: fs,
	}
}

func constructOffset(n *plan.Node, proc *process.Process) *offset.Argument {
	vec, err := colexec.EvalExpr(constBat, proc, n.Offset)
	if err != nil {
		panic(err)
	}
	return &offset.Argument{
		Offset: uint64(vec.Col.([]int64)[0]),
	}
}

func constructLimit(n *plan.Node, proc *process.Process) *limit.Argument {
	vec, err := colexec.EvalExpr(constBat, proc, n.Limit)
	if err != nil {
		panic(err)
	}
	return &limit.Argument{
		Limit: uint64(vec.Col.([]int64)[0]),
	}
}

func constructGroup(n, cn *plan.Node) *group.Argument {
	aggs := make([]aggregate.Aggregate, len(n.AggList))
	for i, expr := range n.AggList {
		if f, ok := expr.Expr.(*plan.Expr_F); ok {
			distinct := (uint64(f.F.Func.Obj) & function.Distinct) != 0
			f.F.Func.Obj = int64(uint64(f.F.Func.Obj) & function.DistinctMask)
			fun, err := function.GetFunctionByID(f.F.Func.GetObj())
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
		Aggs:  aggs,
		Types: typs,
		Exprs: n.GroupBy,
	}
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
	fs := make([]top.Field, len(n.OrderBy))
	for i, e := range n.OrderBy {
		fs[i].E = e.Expr
		if e.Flag == plan.OrderBySpec_DESC {
			fs[i].Type = top.Descending
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
	fs := make([]order.Field, len(n.OrderBy))
	for i, e := range n.OrderBy {
		fs[i].E = e.Expr
		if e.Flag == plan.OrderBySpec_DESC {
			fs[i].Type = order.Descending
		}
	}
	return &mergeorder.Argument{
		Fs: fs,
	}
}

func constructLoopJoin(n *plan.Node, proc *process.Process) *loopjoin.Argument {
	result := make([]loopjoin.ResultPos, len(n.ProjectList))
	for i, expr := range n.ProjectList {
		result[i].Rel, result[i].Pos = constructJoinResult(expr)
	}
	return &loopjoin.Argument{
		Result: result,
		Cond:   colexec.RewriteFilterExprList(n.OnList),
	}
}

func constructLoopSemi(n *plan.Node, proc *process.Process) *loopsemi.Argument {
	result := make([]int32, len(n.ProjectList))
	for i, expr := range n.ProjectList {
		rel, pos := constructJoinResult(expr)
		if rel != 0 {
			panic(errors.New(errno.SyntaxErrororAccessRuleViolation, fmt.Sprintf("complement result '%s' not support now", expr)))
		}
		result[i] = pos
	}
	return &loopsemi.Argument{
		Result: result,
		Cond:   colexec.RewriteFilterExprList(n.OnList),
	}
}

func constructLoopLeft(n *plan.Node, proc *process.Process) *loopleft.Argument {
	result := make([]loopleft.ResultPos, len(n.ProjectList))
	for i, expr := range n.ProjectList {
		result[i].Rel, result[i].Pos = constructJoinResult(expr)
	}
	return &loopleft.Argument{
		Result: result,
		Cond:   colexec.RewriteFilterExprList(n.OnList),
	}
}

func constructLoopComplement(n *plan.Node, proc *process.Process) *loopcomplement.Argument {
	result := make([]int32, len(n.ProjectList))
	for i, expr := range n.ProjectList {
		rel, pos := constructJoinResult(expr)
		if rel != 0 {
			panic(errors.New(errno.SyntaxErrororAccessRuleViolation, fmt.Sprintf("complement result '%s' not support now", expr)))
		}
		result[i] = pos
	}
	return &loopcomplement.Argument{
		Result: result,
		Cond:   colexec.RewriteFilterExprList(n.OnList),
	}
}

func constructJoinResult(expr *plan.Expr) (int32, int32) {
	e, ok := expr.Expr.(*plan.Expr_Col)
	if !ok {
		panic(errors.New(errno.SyntaxErrororAccessRuleViolation, fmt.Sprintf("join result '%s' not support now", expr)))
	}
	return e.Col.RelPos, e.Col.ColPos
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
				return false
			}
			if !hasColExpr(e.F.Args[0]) {
				return false
			}
			if !hasColExpr(e.F.Args[1]) {
				return false
			}
		}
	}
	return true
}

func supportedJoinCondition(id int64) bool {
	fid, _ := function.DecodeOverloadID(id)
	return fid == function.EQUAL
}

func hasColExpr(expr *plan.Expr) bool {
	switch e := expr.Expr.(type) {
	case *plan.Expr_Col:
		return true
	case *plan.Expr_F:
		for i := range e.F.Args {
			if hasColExpr(e.F.Args[i]) {
				return true
			}
		}
		return false
	default:
		return false
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

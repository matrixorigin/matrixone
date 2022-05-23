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

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/errno"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	colexec "github.com/matrixorigin/matrixone/pkg/sql/colexec2"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec2/aggregate"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec2/group"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec2/limit"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec2/mergegroup"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec2/mergelimit"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec2/mergeoffset"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec2/mergeorder"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec2/mergetop"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec2/offset"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec2/order"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec2/output"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec2/projection"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec2/restrict"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec2/top"
	"github.com/matrixorigin/matrixone/pkg/sql/errors"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

var constBat *batch.Batch

func init() {
	constBat = batch.NewWithSize(0)
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
	default:
		panic(errors.New(errno.SyntaxErrororAccessRuleViolation, fmt.Sprintf("Unsupport instruction %T\n", in.Arg)))
	}
	return rin
}

func constructRestrict(n *plan.Node) *restrict.Argument {
	return &restrict.Argument{
		E: colexec.RewriteFilterExprList(n.WhereList),
	}
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
		if e.Collation == "DESC" {
			fs[i].Type = top.Descending
		}
	}
	return &top.Argument{
		Fs:    fs,
		Limit: vec.Col.([]int64)[0],
	}
}

func constructOrder(n *plan.Node, proc *process.Process) *order.Argument {
	fs := make([]order.Field, len(n.OrderBy))
	for i, e := range n.OrderBy {
		fs[i].E = e.Expr
		if e.Collation == "DESC" {
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

func constructGroup(n *plan.Node) *group.Argument {
	aggs := make([]aggregate.Aggregate, len(n.AggList))
	/*
		for i, expr := range n.AggList {
			switch f := expr.Expr.(type) {
			case *plan.Expr_F:
				fun, err := function.GetFunctionByIndex(int(f.F.Func.Schema), int(f.F.Func.Obj))
				if err != nil {
				}
				fun.Flag
			}
		}
	*/
	return &group.Argument{
		Aggs:  aggs,
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
		if e.Collation == "DESC" {
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
		if e.Collation == "DESC" {
			fs[i].Type = order.Descending
		}
	}
	return &mergeorder.Argument{
		Fs: fs,
	}
}

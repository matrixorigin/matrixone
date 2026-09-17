// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package plan

import (
	"context"
	"slices"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	pb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
)

// PreparedIntegerSourceParamPositions follows the optimized relational inputs
// of integer assignments. A scalar-only walk misses a marker behind a ColRef.
// The visited set bounds work by the expression graph, including recursive CTEs.
func PreparedIntegerSourceParamPositions(p *Plan) []int32 {
	q := p.GetQuery()
	if q == nil || !isPreparedDMLStmt(q.StmtType) {
		return nil
	}
	type key struct {
		node       *Node
		expr       *Expr
		relational bool
	}
	seen := make(map[key]bool)
	positions := make(map[int32]struct{})
	var walk func(*Node, *Expr, bool)
	output := func(id, col int32) {
		if id >= 0 && int(id) < len(q.Nodes) {
			n := q.Nodes[id]
			if n != nil && col >= 0 && int(col) < len(n.ProjectList) {
				walk(n, n.ProjectList[col], true)
			}
		}
	}
	walk = func(n *Node, e *Expr, relational bool) {
		if e == nil || seen[key{n, e, relational}] || isExplicitPreparedCast(e) {
			return
		}
		seen[key{n, e, relational}] = true
		if p := e.GetP(); p != nil {
			if relational {
				positions[p.Pos] = struct{}{}
			}
			return
		}
		if lit := e.GetLit(); lit != nil {
			walk(n, lit.Src, relational)
			return
		}
		if col := e.GetCol(); col != nil {
			switch {
			case n.NodeType == pb.Node_UNION || n.NodeType == pb.Node_UNION_ALL:
				for _, child := range n.Children {
					output(child, col.ColPos)
				}
			case col.RelPos >= 0 && int(col.RelPos) < len(n.Children):
				output(n.Children[col.RelPos], col.ColPos)
			case n.NodeType == pb.Node_AGG && col.RelPos == -1:
				if col.ColPos >= 0 && int(col.ColPos) < len(n.GroupBy) {
					walk(n, n.GroupBy[col.ColPos], true)
				}
			case n.NodeType == pb.Node_AGG && col.RelPos == -2:
				i := int(col.ColPos) - len(n.GroupBy)
				if i >= 0 && i < len(n.AggList) {
					walk(n, n.AggList[i], true)
				}
			case n.NodeType == pb.Node_WINDOW && col.RelPos == -1:
				for _, w := range n.WinSpecList {
					walk(n, w, true)
				}
			case n.NodeType == pb.Node_SINK_SCAN || n.NodeType == pb.Node_RECURSIVE_SCAN || n.NodeType == pb.Node_RECURSIVE_CTE:
				for _, step := range n.SourceStep {
					if step >= 0 && int(step) < len(q.Steps) {
						output(q.Steps[step], col.ColPos)
					}
				}
			}
			return
		}
		if w := e.GetW(); w != nil {
			walk(n, w.WindowFunc, relational)
		}
		if f := e.GetF(); f != nil && f.Func != nil {
			name := f.Func.ObjName
			if name == "cast" && len(f.Args) > 0 {
				walk(n, f.Args[0], relational)
			} else if args, ok := function.NumericFunctionResultArgs(name, len(f.Args), true); ok {
				for _, i := range args {
					walk(n, f.Args[i], relational)
				}
			}
		}
	}
	for _, n := range q.Nodes {
		if n == nil {
			continue
		}
		rootsSeen := make(map[*Expr]bool)
		var roots func(*Expr)
		roots = func(e *Expr) {
			if e == nil || rootsSeen[e] {
				return
			}
			rootsSeen[e] = true
			if f := e.GetF(); f != nil && f.Func != nil {
				if types.T(e.Typ.Id).IsInteger() && len(f.Args) > 0 &&
					(f.Func.ObjName == "cast_assign" || f.Func.ObjName == "cast_ignore") {
					walk(n, f.Args[0], false)
				}
				for _, arg := range f.Args {
					roots(arg)
				}
			}
		}
		for _, e := range n.ProjectList {
			roots(e)
		}
		for _, e := range n.OnUpdateExprs {
			roots(e)
		}
		if n.DedupJoinCtx != nil {
			for _, e := range n.DedupJoinCtx.UpdateColExprList {
				roots(e)
			}
		}
		if n.RowsetData != nil {
			for _, col := range n.RowsetData.Cols {
				for _, row := range col.Data {
					if row != nil {
						roots(row.Expr)
					}
				}
			}
		}
	}
	result := make([]int32, 0, len(positions))
	for p := range positions {
		result = append(result, p)
	}
	slices.Sort(result)
	return result
}

type preparedIntegerBindingsKey struct{}

type preparedIntegerCompilerContext struct {
	CompilerContext
	ctx context.Context
}

func (c *preparedIntegerCompilerContext) GetContext() context.Context    { return c.ctx }
func (c *preparedIntegerCompilerContext) SetContext(ctx context.Context) { c.ctx = ctx }

// WithPreparedIntegerBindings owns a request-local parameter domain map. The
// embedded compiler context's context is never changed, even by build helpers.
// Keys are parser ordinals (one-based); transport ParamRefs are normalized later.
func WithPreparedIntegerBindings(requestCtx context.Context, c CompilerContext, values []any, positions []int32) CompilerContext {
	bindings := make(map[int]types.Type, len(positions))
	for _, pos := range positions {
		if pos < 0 || int(pos) >= len(values) {
			continue
		}
		p, ok := values[pos].(ParamValue)
		if !ok || p.Value == nil {
			continue
		}
		typ := types.T_text.ToType()
		if p.HasSourceType {
			typ = p.SourceType
		} else if p.IsBinaryProtocol && p.HasRuntimeType {
			typ = p.RuntimeType
		}
		bindings[int(pos)+1] = typ
	}
	ctx := requestCtx
	if ctx == nil {
		ctx = c.GetContext()
	}
	if ctx == nil {
		ctx = context.Background()
	}
	return &preparedIntegerCompilerContext{c, context.WithValue(ctx, preparedIntegerBindingsKey{}, bindings)}
}

func preparedIntegerBinding(ctx context.Context, ordinal int) (types.Type, bool) {
	bindings, _ := ctx.Value(preparedIntegerBindingsKey{}).(map[int]types.Type)
	typ, ok := bindings[ordinal]
	return typ, ok
}

func exprHasPreparedExactMarker(expr *Expr) bool {
	if expr == nil {
		return false
	}
	if types.T(expr.Typ.Id).IsFloat() && expr.GetF() != nil &&
		!function.IsExactNumericExpression(expr, nil) {
		return false
	}
	if expr.GetPreparedNumeric().GetProvisionalResultPeer() {
		return true
	}
	if fn := expr.GetF(); fn != nil {
		for _, arg := range fn.Args {
			if exprHasPreparedExactMarker(arg) {
				return true
			}
		}
	}
	if literal := expr.GetLit(); literal != nil {
		return exprHasPreparedExactMarker(literal.Src)
	}
	return false
}

func exprHasPreparedExactBinding(ctx context.Context, expr *Expr) bool {
	if expr == nil {
		return false
	}
	if param := expr.GetP(); param != nil {
		typ, ok := preparedIntegerBinding(ctx, int(param.Pos))
		return ok && (typ.Oid.IsInteger() || typ.IsDecimal())
	}
	if fn := expr.GetF(); fn != nil {
		for _, arg := range fn.Args {
			if exprHasPreparedExactBinding(ctx, arg) {
				return true
			}
		}
	}
	if literal := expr.GetLit(); literal != nil {
		return exprHasPreparedExactBinding(ctx, literal.Src)
	}
	return false
}

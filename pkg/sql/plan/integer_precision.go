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
	"math"

	"github.com/gogo/protobuf/proto"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	pb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
)

type integerPrecisionBuilder struct {
	graph    *pb.IntegerPrecisionGraph
	ids      map[*Expr]uint32
	pureNull map[*Expr]bool
	imports  map[*pb.IntegerPrecisionGraph]map[uint32]uint32
}

func newIntegerPrecisionBuilder() *integerPrecisionBuilder {
	return &integerPrecisionBuilder{graph: &pb.IntegerPrecisionGraph{SourceBudget: 65}, ids: make(map[*Expr]uint32), pureNull: make(map[*Expr]bool), imports: make(map[*pb.IntegerPrecisionGraph]map[uint32]uint32)}
}

func (g *integerPrecisionBuilder) append(n *pb.IntegerPrecisionNode) uint32 {
	g.graph.Nodes = append(g.graph.Nodes, n)
	return uint32(len(g.graph.Nodes))
}

func (g *integerPrecisionBuilder) reference(id uint32) uint32 {
	if id == 0 {
		return 0
	}
	return g.append(&pb.IntegerPrecisionNode{Kind: pb.IntegerPrecisionNode_REFERENCE, Inputs: []uint32{id}, Arithmetic: g.graph.Nodes[id-1].Arithmetic})
}

func (g *integerPrecisionBuilder) declared(e *Expr) uint32 {
	p := function.IntegerDeclaredPrecision(makeTypeByPlan2Expr(e))
	if !p.Known {
		return 0
	}
	return g.append(&pb.IntegerPrecisionNode{Precision: uint32(p.Raw), Unsigned: p.Unsigned})
}

func (g *integerPrecisionBuilder) source(ctx *BindContext, e *Expr) uint32 {
	if e == nil {
		return 0
	}
	if id := g.ids[e]; id != 0 {
		return id
	}
	if ctx != nil && e.GetCol() != nil {
		if id := ctx.outputColumnProvenanceForExpr(e).IntegerPrecisionID; id != 0 {
			g.ids[e] = id
			return id
		}
	}
	// An explicit SQL cast is registered before an enclosing function binds.
	// Unregistered casts here are the binder's physical adapters.
	if f := e.GetF(); f != nil && f.Func != nil && f.Func.ObjName == "cast" && len(f.Args) > 0 {
		return g.source(ctx, f.Args[0])
	}
	if !types.T(e.Typ.Id).IsInteger() {
		return 0
	}
	id := g.declared(e)
	g.ids[e] = id
	return id
}

func (g *integerPrecisionBuilder) captureSource(b *baseBinder, ast tree.Expr, e *Expr) {
	if literal, ok := ast.(*tree.NumVal); ok && literal.ValType == tree.P_null {
		g.pureNull[e] = true
		return
	}

	if !types.T(e.Typ.Id).IsInteger() {
		return
	}
	// Unary signs on an integer literal are part of its syntactic source,
	// not a declared BIGINT envelope. Preserve the signless lexical digits.
	literalAST := ast
	for {
		switch a := literalAST.(type) {
		case *tree.ParenExpr:
			literalAST = a.Expr
		case *tree.UnaryExpr:
			if a.Op != tree.UNARY_PLUS && a.Op != tree.UNARY_MINUS {
				goto literalDone
			}
			literalAST = a.Expr
		default:
			goto literalDone
		}
	}
literalDone:
	if literal, ok := literalAST.(*tree.NumVal); ok && (literal.ValType == tree.P_int64 || literal.ValType == tree.P_uint64) {
		ast = literal
	}
	var node *pb.IntegerPrecisionNode
	switch a := ast.(type) {
	case *tree.NumVal:
		if a.ValType != tree.P_int64 && a.ValType != tree.P_uint64 {
			return
		}
		n := uint64(len(a.String()))
		if n == 0 || n > math.MaxUint32-2 {
			return
		}
		g.graph.SourceBudget = max(g.graph.SourceBudget, n)
		node = &pb.IntegerPrecisionNode{Kind: pb.IntegerPrecisionNode_SQL_LITERAL, Precision: uint32(n), Unsigned: types.T(e.Typ.Id).IsUnsignedInt()}
	case *tree.CastExpr:
		p := function.IntegerCastPrecision(types.T(e.Typ.Id).IsUnsignedInt())
		node = &pb.IntegerPrecisionNode{Kind: pb.IntegerPrecisionNode_EXPLICIT_CAST, Precision: uint32(p.Raw), Unsigned: p.Unsigned}
	case *tree.ParamExpr:
		p := function.IntegerCastPrecision(types.T(e.Typ.Id).IsUnsignedInt())
		// AST ordinals are one-based; the graph always uses execution slots.
		// PreparePlan's ParamRef visitor normalizes expressions separately.
		pos := int32(a.Offset) - 1
		g.graph.ParameterCount = max(g.graph.ParameterCount, uint32(pos+1))
		g.graph.SourceBudget = max(g.graph.SourceBudget, uint64(g.graph.ParameterCount))
		node = &pb.IntegerPrecisionNode{Kind: pb.IntegerPrecisionNode_PARAMETER, Precision: uint32(p.Raw), Unsigned: p.Unsigned, ParamPos: pos}
	default:
		if g.ids[e] == 0 {
			g.source(b.ctx, e)
		}
		return
	}
	g.ids[e] = g.append(node)
}

func (g *integerPrecisionBuilder) functionInputs(ctx *BindContext, name string, args []*Expr) []uint32 {
	switch name {
	case "+", "*":
		if len(args) != 2 {
			return nil
		}
		return []uint32{g.source(ctx, args[0]), g.source(ctx, args[1])}
	case "case":
		ids := make([]uint32, 0, len(args)/2+1)
		for i, arg := range args {
			if i%2 == 0 && i != len(args)-1 {
				continue
			}
			if g.pureNull[arg] && g.ids[arg] == 0 {
				continue
			}
			id := g.source(ctx, arg)
			// A typed NULL has an explicit source node; only pure NULL is neutral.
			if id == 0 && g.pureNull[arg] {
				continue
			}
			ids = append(ids, id)
		}
		return ids
	}
	return nil
}

func (g *integerPrecisionBuilder) captureFunction(name string, inputs []uint32, e *Expr) {
	if len(inputs) == 0 || !types.T(e.Typ.Id).IsInteger() {
		return
	}
	kind := pb.IntegerPrecisionNode_COMMON_INTEGER
	arithmetic := false
	unsigned := types.T(e.Typ.Id).IsUnsignedInt()
	switch name {
	case "+":
		kind = pb.IntegerPrecisionNode_ADD
		arithmetic = true
		unsigned = false
	case "*":
		kind = pb.IntegerPrecisionNode_MUL
		arithmetic = true
		unsigned = false
	case "case":
		for _, id := range inputs {
			if id != 0 {
				arithmetic = arithmetic || g.graph.Nodes[id-1].Arithmetic
			}
		}
	default:
		return
	}
	g.ids[e] = g.append(&pb.IntegerPrecisionNode{Kind: kind, Inputs: inputs, Unsigned: unsigned, Arithmetic: arithmetic})
}

func integerPrecisionRoot(g *pb.IntegerPrecisionGraph, id uint32) pb.IntegerPrecisionOutput_RootClass {
	if id == 0 {
		return pb.IntegerPrecisionOutput_DECLARED_SOURCE
	}
	switch g.Nodes[id-1].Kind {
	case pb.IntegerPrecisionNode_ADD, pb.IntegerPrecisionNode_MUL, pb.IntegerPrecisionNode_COMMON_INTEGER, pb.IntegerPrecisionNode_EXPLICIT_CAST:
		return pb.IntegerPrecisionOutput_DIRECT_FUNCTION
	case pb.IntegerPrecisionNode_REFERENCE:
		return pb.IntegerPrecisionOutput_SQL_REFERENCE
	case pb.IntegerPrecisionNode_SQL_LITERAL:
		return pb.IntegerPrecisionOutput_SQL_LITERAL
	}
	return pb.IntegerPrecisionOutput_DECLARED_SOURCE
}

func (builder *QueryBuilder) captureIntegerPrecisionOutputs(ctx *BindContext) {
	if builder.qry.StmtType != pb.Query_SELECT && !builder.qry.HasReturning {
		return
	}

	g := builder.integerPrecision
	outputs := make([]*pb.IntegerPrecisionOutput, len(ctx.headings))
	anyArithmetic := false
	for i := range outputs {
		id := ctx.outputColumnProvenanceForProject(int32(i)).IntegerPrecisionID
		outputs[i] = &pb.IntegerPrecisionOutput{Node: id, RootClass: integerPrecisionRoot(g.graph, id)}
		if id != 0 {
			anyArithmetic = anyArithmetic || g.graph.Nodes[id-1].Arithmetic
		}
	}
	builder.qry.IntegerPrecision = nil
	if anyArithmetic {
		g.graph.Outputs = outputs
		builder.qry.IntegerPrecision = g.graph
	}
}

// evaluateIntegerPrecision requires the graph's admission proof. It allocates
// once per metadata evaluation, never per row, and evaluates canonical recipes.
func evaluateIntegerPrecision(g *pb.IntegerPrecisionGraph) []function.IntegerLogicalPrecision {
	values := make([]function.IntegerLogicalPrecision, len(g.Nodes))
	for i, n := range g.Nodes {
		p := function.IntegerLogicalPrecision{Raw: uint64(n.Precision), Unsigned: n.Unsigned, Known: true}
		input := func(j int) function.IntegerLogicalPrecision {
			id := n.Inputs[j]
			if id == 0 {
				return function.IntegerLogicalPrecision{}
			}
			return values[id-1]
		}
		switch n.Kind {
		case pb.IntegerPrecisionNode_ADD, pb.IntegerPrecisionNode_MUL:
			literal := func(j int) bool {
				id := n.Inputs[j]
				return id != 0 && g.Nodes[id-1].Kind == pb.IntegerPrecisionNode_SQL_LITERAL
			}
			p = function.IntegerAddMulPrecision(n.Kind == pb.IntegerPrecisionNode_MUL, input(0), input(1), literal(0), literal(1))
		case pb.IntegerPrecisionNode_REFERENCE:
			p = input(0)
		case pb.IntegerPrecisionNode_COMMON_INTEGER:
			var length uint64
			for j := range n.Inputs {
				v := input(j)
				if !v.Known {
					p.Known = false
					break
				}
				length = max(length, v.DisplayLength())
			}
			if p.Known {
				p.Raw = length
				if !p.Unsigned {
					p.Raw--
				}
			}
		}
		values[i] = p
	}
	return values
}

func integerSchemaType(typ pb.Type, g *pb.IntegerPrecisionGraph, out *pb.IntegerPrecisionOutput, view bool, values []function.IntegerLogicalPrecision) pb.Type {
	if g == nil || out == nil || out.Node == 0 || !types.T(typ.Id).IsInteger() || !g.Nodes[out.Node-1].Arithmetic {
		return typ
	}
	p := values[out.Node-1]
	if !p.Known || p.Unsigned != types.T(typ.Id).IsUnsignedInt() {
		return typ
	}
	cutoff := uint64(11)
	if view || out.RootClass == pb.IntegerPrecisionOutput_SQL_REFERENCE {
		cutoff = 9
	}
	oid := types.T_int64
	if p.DisplayLength() <= cutoff {
		oid = types.T_int32
	}
	if p.Unsigned {
		if oid == types.T_int32 {
			oid = types.T_uint32
		} else {
			oid = types.T_uint64
		}
	}
	typ.Id = int32(oid)
	typ.Width = int32(oid.TypeLen() * 8)
	typ.Scale = 0
	return typ
}

func cloneIntegerPrecisionGraph(g *pb.IntegerPrecisionGraph) *pb.IntegerPrecisionGraph {
	if g == nil {
		return nil
	}
	return proto.Clone(g).(*pb.IntegerPrecisionGraph)
}

func cloneIntegerResultPresentation(p *pb.IntegerResultPresentation) *pb.IntegerResultPresentation {
	if p == nil {
		return nil
	}
	return proto.Clone(p).(*pb.IntegerResultPresentation)
}

// importID preserves admitted source budgets when a short outer query imports
// an already bound view/subquery. IDs remain local to the destination query.
func (g *integerPrecisionBuilder) importID(source *pb.IntegerPrecisionGraph, id uint32) uint32 {
	if source == nil || id == 0 {
		return 0
	}
	if source == g.graph {
		return id
	}
	mapping, ok := g.imports[source]
	if !ok {
		mapping = make(map[uint32]uint32, len(source.Nodes))
		for i, n := range source.Nodes {
			copy := *n
			copy.Inputs = make([]uint32, len(n.Inputs))
			for j, edge := range n.Inputs {
				copy.Inputs[j] = mapping[edge]
			}
			mapping[uint32(i+1)] = g.append(&copy)
		}
		g.imports[source] = mapping
		g.graph.SourceBudget = max(g.graph.SourceBudget, source.SourceBudget)
		g.graph.ParameterCount = max(g.graph.ParameterCount, source.ParameterCount)
	}
	return mapping[id]
}

// IntegerPrecisionParameterBindings describes the full inferred domain of
// arithmetic source markers, independently of any current execution value.
func IntegerPrecisionParameterBindings(p *Plan) []PreparedSourceBinding {
	if p == nil {
		return nil
	}
	q := p.GetQuery()
	if q == nil && p.GetDdl() != nil {
		q = p.GetDdl().GetQuery()
	}
	if q == nil || q.IntegerPrecision == nil {
		return nil
	}
	// A query can also contain unrelated integer markers. Only descendants
	// of an arithmetic result own the retained domain; sealed casts stop here.
	g := q.IntegerPrecision
	reachable := make([]bool, len(g.Nodes))
	for _, out := range g.Outputs {
		if out.Node != 0 && g.Nodes[out.Node-1].Arithmetic {
			reachable[out.Node-1] = true
		}
	}
	for i := len(g.Nodes) - 1; i >= 0; i-- {
		if reachable[i] {
			for _, edge := range g.Nodes[i].Inputs {
				if edge != 0 {
					reachable[edge-1] = true
				}
			}
		}
	}
	var bindings []PreparedSourceBinding
	seen := make(map[int32]bool)
	for i, n := range q.IntegerPrecision.Nodes {
		if !reachable[i] || n.Kind != pb.IntegerPrecisionNode_PARAMETER || seen[n.ParamPos] {
			continue
		}
		seen[n.ParamPos] = true
		oid := types.T_int64
		if n.Unsigned {
			oid = types.T_uint64
		}
		bindings = append(bindings, PreparedSourceBinding{Position: n.ParamPos, Type: oid.ToType()})
	}
	return bindings
}

// The legacy DDL parameter-replacement path rebinds expressions on a private
// plan copy. Refresh its source recipes from the same admitted values. A
// noninteger source invalidates its edges rather than inventing integer digits.
func refreshIntegerPrecisionParameters(q *pb.Query, values []any, selected []bool) {
	if q == nil || q.IntegerPrecision == nil {
		return
	}
	g := q.IntegerPrecision
	unknown := make(map[uint32]bool)
	for i, n := range g.Nodes {
		if n.Kind != pb.IntegerPrecisionNode_PARAMETER || int(n.ParamPos) >= len(values) || selected != nil && !selected[n.ParamPos] {
			continue
		}
		param, ok := values[n.ParamPos].(ParamValue)
		if !ok {
			unknown[uint32(i+1)] = true
			continue
		}
		typ := param.SourceType
		if param.HasRuntimeType {
			typ = param.RuntimeType
		} else if !param.HasSourceType {
			unknown[uint32(i+1)] = true
			continue
		}
		if typ.Oid == types.T_any {
			continue
		}
		if !typ.Oid.IsInteger() {
			unknown[uint32(i+1)] = true
			continue
		}
		precision := function.IntegerCastPrecision(typ.Oid.IsUnsignedInt())
		n.Unsigned = precision.Unsigned
		n.Precision = uint32(precision.Raw)
	}
	for _, n := range g.Nodes {
		for i, edge := range n.Inputs {
			if unknown[edge] {
				n.Inputs[i] = 0
			}
		}
	}
	for _, out := range g.Outputs {
		if unknown[out.Node] {
			out.Node = 0
			out.RootClass = pb.IntegerPrecisionOutput_DECLARED_SOURCE
		}
	}
}

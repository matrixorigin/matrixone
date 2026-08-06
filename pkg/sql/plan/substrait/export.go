// Copyright 2026 Matrix Origin
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

// Package substrait exports the deliberately small Substrait subset accepted
// by the Sirius v1 execution contract. Export is pure: it never opens a
// relation, touches a transaction, or registers a storage lease.
package substrait

import (
	"context"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	spb "github.com/substrait-io/substrait-protobuf/go/substraitpb"
	"github.com/substrait-io/substrait-protobuf/go/substraitpb/extensions"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
)

const (
	Version        = "0.78.0"
	TaeReadTypeURL = "type.googleapis.com/matrixone.sirius.v1.TaeRead"
	MaxPlanBytes   = 16 << 20
)

// Candidate is a fully validated logical plan with unresolved storage reads.
// The read handles are installed only after snapshot admission succeeds.
type Candidate struct {
	query *planpb.Query
	reads []Read
}

// Read identifies one physical table scan which needs a TaeRead lease.
type Read struct {
	NodeID  int32
	TableID uint64
	Schema  []byte // deterministic Substrait NamedStruct bytes
}

func (c *Candidate) Reads() []Read { return append([]Read(nil), c.reads...) }

// Export validates q without performing I/O.
func Export(q *planpb.Query) (*Candidate, error) {
	if q == nil || q.StmtType != planpb.Query_SELECT || len(q.Steps) != 1 || len(q.BackgroundQueries) != 0 {
		return nil, moerr.NewInternalErrorNoCtxf("substrait: exactly one query root is required")
	}
	c := &Candidate{query: q}
	e := exporter{query: q, readValues: make(map[int32][]byte), validateOnly: true}
	if _, err := e.node(q.Steps[0]); err != nil {
		return nil, err
	}
	c.reads = e.reads
	return c, nil
}

// Build binds admitted TaeRead messages to every scan and serializes the plan.
func (c *Candidate) Build(readValues map[int32][]byte) ([]byte, error) {
	if c == nil {
		return nil, moerr.NewInternalErrorNoCtxf("substrait: nil candidate")
	}
	e := exporter{query: c.query, readValues: readValues}
	root, err := e.node(c.query.Steps[0])
	if err != nil {
		return nil, err
	}
	p := &spb.Plan{
		Version:          &spb.Version{MajorNumber: 0, MinorNumber: 78, PatchNumber: 0, Producer: "matrixone"},
		Relations:        []*spb.PlanRel{{RelType: &spb.PlanRel_Root{Root: &spb.RelRoot{Input: root}}}},
		ExpectedTypeUrls: []string{TaeReadTypeURL},
		Extensions:       e.extensions(),
	}
	b, err := proto.MarshalOptions{Deterministic: true}.Marshal(p)
	if err != nil {
		return nil, moerr.NewInternalErrorNoCtxf("substrait: marshal plan: %v", err)
	}
	if len(b) > MaxPlanBytes {
		return nil, moerr.NewInternalErrorNoCtxf("substrait: plan is %d bytes, maximum is %d", len(b), MaxPlanBytes)
	}
	return b, nil
}

type exporter struct {
	query        *planpb.Query
	readValues   map[int32][]byte
	reads        []Read
	functions    map[string]uint32
	validateOnly bool
	visiting     map[int32]bool
}

func (e *exporter) node(id int32) (*spb.Rel, error) {
	if id < 0 || int(id) >= len(e.query.Nodes) {
		return nil, moerr.NewInternalErrorNoCtxf("substrait: invalid node id %d", id)
	}
	if e.visiting == nil {
		e.visiting = make(map[int32]bool)
	}
	if e.visiting[id] {
		return nil, moerr.NewInternalErrorNoCtxf("substrait: cyclic plan at node %d", id)
	}
	e.visiting[id] = true
	defer delete(e.visiting, id)
	n := e.query.Nodes[id]
	if n == nil || n.NodeId != id {
		return nil, moerr.NewInternalErrorNoCtxf("substrait: node %d is missing or misindexed", id)
	}
	var rel *spb.Rel
	var err error
	switch n.NodeType {
	case planpb.Node_TABLE_SCAN:
		rel, err = e.read(n)
	case planpb.Node_FILTER:
		var input *spb.Rel
		input, err = e.unary(n)
		if err == nil {
			var inputWidth int
			inputWidth, err = e.nodeWidth(n.Children[0])
			if err == nil {
				err = validateExprFields(n.FilterList, inputWidth)
			}
		}
		if err == nil {
			var condition *spb.Expression
			condition, err = e.conjunction(n.FilterList)
			if err == nil {
				rel = &spb.Rel{RelType: &spb.Rel_Filter{Filter: &spb.FilterRel{Input: input, Condition: condition}}}
			}
		}
	case planpb.Node_PROJECT:
		var input *spb.Rel
		input, err = e.unary(n)
		if err == nil {
			var inputWidth int
			inputWidth, err = e.nodeWidth(n.Children[0])
			if err == nil {
				err = validateExprFields(n.ProjectList, inputWidth)
			}
			exprs := make([]*spb.Expression, len(n.ProjectList))
			mapping := make([]int32, len(n.ProjectList))
			if err == nil {
				for i := range n.ProjectList {
					exprs[i], err = e.expr(n.ProjectList[i])
					if err != nil {
						break
					}
					mapping[i] = int32(inputWidth + i)
				}
			}
			if err == nil {
				rel = &spb.Rel{RelType: &spb.Rel_Project{Project: &spb.ProjectRel{Common: &spb.RelCommon{EmitKind: &spb.RelCommon_Emit_{Emit: &spb.RelCommon_Emit{OutputMapping: mapping}}}, Input: input, Expressions: exprs}}}
			}
		}
	case planpb.Node_AGG:
		rel, err = e.aggregate(n)
	case planpb.Node_SORT:
		rel, err = e.sort(n)
	default:
		return nil, moerr.NewInternalErrorNoCtxf("substrait: node %d uses unsupported operator %s", id, n.NodeType.String())
	}
	if err != nil {
		return nil, err
	}
	return e.fetch(rel, n)
}

func (e *exporter) nodeWidth(id int32) (int, error) {
	if id < 0 || int(id) >= len(e.query.Nodes) || e.query.Nodes[id] == nil {
		return 0, moerr.NewInternalErrorNoCtxf("substrait: invalid node id %d", id)
	}
	n := e.query.Nodes[id]
	switch n.NodeType {
	case planpb.Node_TABLE_SCAN:
		if len(n.ProjectList) > 0 {
			return len(n.ProjectList), nil
		}
		count := 0
		if n.TableDef == nil {
			return 0, moerr.NewInternalErrorNoCtxf("substrait: scan has no table")
		}
		for _, c := range n.TableDef.Cols {
			if c != nil && !c.Hidden {
				count++
			}
		}
		return count, nil
	case planpb.Node_PROJECT:
		return len(n.ProjectList), nil
	case planpb.Node_AGG:
		return len(n.GroupBy) + len(n.AggList), nil
	case planpb.Node_FILTER, planpb.Node_SORT:
		if len(n.Children) != 1 {
			return 0, moerr.NewInternalErrorNoCtxf("substrait: node %d requires one child", id)
		}
		return e.nodeWidth(n.Children[0])
	default:
		return 0, moerr.NewInternalErrorNoCtxf("substrait: unsupported width for %s", n.NodeType.String())
	}
}

func (e *exporter) unary(n *planpb.Node) (*spb.Rel, error) {
	if len(n.Children) != 1 {
		return nil, moerr.NewInternalErrorNoCtxf("substrait: %s node %d requires one child", n.NodeType.String(), n.NodeId)
	}
	return e.node(n.Children[0])
}

func (e *exporter) read(n *planpb.Node) (*spb.Rel, error) {
	if len(n.Children) != 0 || n.TableDef == nil || n.ObjRef == nil || n.TableDef.TblId == 0 || uint64(n.ObjRef.Obj) != n.TableDef.TblId || n.TableDef.IsTemporary || n.ScanSnapshot != nil || n.ObjRef.Snapshot != nil || n.ObjRef.PubInfo != nil || (n.TableDef.TableType != "" && n.TableDef.TableType != "r") {
		return nil, moerr.NewInternalErrorNoCtxf("substrait: node %d is not a persistent TAE table scan", n.NodeId)
	}
	schema, err := namedStruct(n.TableDef)
	if err != nil {
		return nil, err
	}
	schemaBytes, err := proto.MarshalOptions{Deterministic: true}.Marshal(schema)
	if err != nil {
		return nil, err
	}
	e.reads = append(e.reads, Read{NodeID: n.NodeId, TableID: n.TableDef.TblId, Schema: schemaBytes})
	value := e.readValues[n.NodeId]
	if !e.validateOnly && len(value) == 0 {
		return nil, moerr.NewInternalErrorNoCtxf("substrait: node %d has no admitted TaeRead", n.NodeId)
	}
	rel := &spb.Rel{RelType: &spb.Rel_Read{Read: &spb.ReadRel{
		BaseSchema: schema,
		ReadType:   &spb.ReadRel_ExtensionTable_{ExtensionTable: &spb.ReadRel_ExtensionTable{Detail: &anypb.Any{TypeUrl: TaeReadTypeURL, Value: value}}},
	}}}
	if err = validateExprFields(n.FilterList, len(schema.Struct.Types)); err != nil {
		return nil, err
	}
	if err = validateExprFields(n.ProjectList, len(schema.Struct.Types)); err != nil {
		return nil, err
	}
	if len(n.FilterList) > 0 {
		condition, xerr := e.conjunction(n.FilterList)
		if xerr != nil {
			return nil, xerr
		}
		rel = &spb.Rel{RelType: &spb.Rel_Filter{Filter: &spb.FilterRel{Input: rel, Condition: condition}}}
	}
	if len(n.ProjectList) > 0 {
		width := len(schema.Struct.Types)
		exprs := make([]*spb.Expression, len(n.ProjectList))
		mapping := make([]int32, len(n.ProjectList))
		for i := range n.ProjectList {
			exprs[i], err = e.expr(n.ProjectList[i])
			if err != nil {
				return nil, err
			}
			mapping[i] = int32(width + i)
		}
		rel = &spb.Rel{RelType: &spb.Rel_Project{Project: &spb.ProjectRel{Common: &spb.RelCommon{EmitKind: &spb.RelCommon_Emit_{Emit: &spb.RelCommon_Emit{OutputMapping: mapping}}}, Input: rel, Expressions: exprs}}}
	}
	return rel, nil
}

func (e *exporter) aggregate(n *planpb.Node) (*spb.Rel, error) {
	for _, flag := range n.GroupingFlag {
		if flag {
			return nil, moerr.NewInternalErrorNoCtxf("substrait: grouping sets are unsupported")
		}
	}
	input, err := e.unary(n)
	if err != nil {
		return nil, err
	}
	inputWidth, err := e.nodeWidth(n.Children[0])
	if err != nil {
		return nil, err
	}
	if err = validateExprFields(n.GroupBy, inputWidth); err != nil {
		return nil, err
	}
	groups := make([]*spb.Expression, len(n.GroupBy))
	refs := make([]uint32, len(groups))
	for i := range n.GroupBy {
		groups[i], err = e.expr(n.GroupBy[i])
		refs[i] = uint32(i)
		if err != nil {
			return nil, err
		}
	}
	measures := make([]*spb.AggregateRel_Measure, len(n.AggList))
	for i, x := range n.AggList {
		f := x.GetF()
		if f == nil || f.Func == nil {
			return nil, moerr.NewInternalErrorNoCtxf("substrait: aggregate %d is not a function", i)
		}
		if uint64(f.Func.Obj)&function.Distinct != 0 || len(f.AggConfig) != 0 {
			return nil, moerr.NewInternalErrorNoCtxf("substrait: unsupported aggregate form %q", f.Func.ObjName)
		}
		name, ok := aggregateIdentity(f.Func)
		if !ok {
			return nil, moerr.NewInternalErrorNoCtxf("substrait: unsupported aggregate %q", f.Func.ObjName)
		}
		if len(f.Args) != 1 {
			return nil, moerr.NewInternalErrorNoCtxf("substrait: unsupported aggregate form %q", f.Func.ObjName)
		}
		if err := validateMOOverload(f.Func, f.Args, &x.Typ); err != nil {
			return nil, err
		}
		if err := validateExprFields(f.Args, inputWidth); err != nil {
			return nil, err
		}
		args := make([]*spb.FunctionArgument, len(f.Args))
		for j := range f.Args {
			a, xerr := e.expr(f.Args[j])
			if xerr != nil {
				return nil, xerr
			}
			args[j] = valueArg(a)
		}
		out, xerr := substraitType(&x.Typ)
		if xerr != nil {
			return nil, xerr
		}
		measures[i] = &spb.AggregateRel_Measure{Measure: &spb.AggregateFunction{FunctionReference: e.function(name), Arguments: args, OutputType: out, Phase: spb.AggregationPhase_AGGREGATION_PHASE_INITIAL_TO_RESULT, Invocation: spb.AggregateFunction_AGGREGATION_INVOCATION_ALL}}
	}
	return &spb.Rel{RelType: &spb.Rel_Aggregate{Aggregate: &spb.AggregateRel{Input: input, GroupingExpressions: groups, Groupings: []*spb.AggregateRel_Grouping{{ExpressionReferences: refs}}, Measures: measures}}}, nil
}

func (e *exporter) sort(n *planpb.Node) (*spb.Rel, error) {
	input, err := e.unary(n)
	if err != nil {
		return nil, err
	}
	inputWidth, err := e.nodeWidth(n.Children[0])
	if err != nil {
		return nil, err
	}
	sorts := make([]*spb.SortField, len(n.OrderBy))
	for i, order := range n.OrderBy {
		if order == nil || order.Collation != "" || int32(order.Flag)&int32(planpb.OrderBySpec_UNIQUE) != 0 {
			return nil, moerr.NewInternalErrorNoCtxf("substrait: unsupported sort at %d", i)
		}
		if err := validateExprFields([]*planpb.Expr{order.Expr}, inputWidth); err != nil {
			return nil, err
		}
		x, xerr := e.expr(order.Expr)
		if xerr != nil {
			return nil, xerr
		}
		flag := int32(order.Flag)
		directionBits := flag & (int32(planpb.OrderBySpec_ASC) | int32(planpb.OrderBySpec_DESC))
		nullBits := flag & (int32(planpb.OrderBySpec_NULLS_FIRST) | int32(planpb.OrderBySpec_NULLS_LAST))
		known := directionBits | nullBits
		if directionBits != int32(planpb.OrderBySpec_ASC) && directionBits != int32(planpb.OrderBySpec_DESC) || nullBits == int32(planpb.OrderBySpec_NULLS_FIRST)|int32(planpb.OrderBySpec_NULLS_LAST) || flag != known {
			return nil, moerr.NewInternalErrorNoCtxf("substrait: unsupported sort flags %d", flag)
		}
		desc := flag&int32(planpb.OrderBySpec_DESC) != 0
		nullsFirst := flag&int32(planpb.OrderBySpec_NULLS_FIRST) != 0
		if flag&int32(planpb.OrderBySpec_NULLS_LAST) == 0 && !nullsFirst {
			nullsFirst = desc
		}
		direction := spb.SortField_SORT_DIRECTION_ASC_NULLS_LAST
		if !desc && nullsFirst {
			direction = spb.SortField_SORT_DIRECTION_ASC_NULLS_FIRST
		}
		if desc && nullsFirst {
			direction = spb.SortField_SORT_DIRECTION_DESC_NULLS_FIRST
		}
		if desc && !nullsFirst {
			direction = spb.SortField_SORT_DIRECTION_DESC_NULLS_LAST
		}
		sorts[i] = &spb.SortField{Expr: x, SortKind: &spb.SortField_Direction{Direction: direction}}
	}
	return &spb.Rel{RelType: &spb.Rel_Sort{Sort: &spb.SortRel{Input: input, Sorts: sorts}}}, nil
}

func (e *exporter) fetch(input *spb.Rel, n *planpb.Node) (*spb.Rel, error) {
	if n.Limit == nil && n.Offset == nil {
		return input, nil
	}
	count, err := nonnegativeIntLiteral(n.Limit, -1)
	if err != nil {
		return nil, moerr.NewInternalErrorNoCtxf("substrait: limit: %v", err)
	}
	offset, err := nonnegativeIntLiteral(n.Offset, 0)
	if err != nil {
		return nil, moerr.NewInternalErrorNoCtxf("substrait: offset: %v", err)
	}
	return &spb.Rel{RelType: &spb.Rel_Fetch{Fetch: &spb.FetchRel{Input: input, OffsetMode: &spb.FetchRel_Offset{Offset: offset}, CountMode: &spb.FetchRel_Count{Count: count}}}}, nil
}

func (e *exporter) conjunction(xs []*planpb.Expr) (*spb.Expression, error) {
	if len(xs) == 0 {
		return nil, moerr.NewInternalErrorNoCtxf("substrait: empty filter")
	}
	for _, predicate := range xs {
		if predicate == nil || types.T(predicate.Typ.Id) != types.T_bool {
			return nil, moerr.NewInternalErrorNoCtxf("substrait: filter predicate is not boolean")
		}
	}
	x, err := e.expr(xs[0])
	if err != nil {
		return nil, err
	}
	for i := 1; i < len(xs); i++ {
		right, xerr := e.expr(xs[i])
		if xerr != nil {
			return nil, xerr
		}
		x = e.scalar("and", &planpb.Type{Id: int32(types.T_bool)}, x, right)
	}
	return x, nil
}

func (e *exporter) expr(x *planpb.Expr) (*spb.Expression, error) {
	if x == nil {
		return nil, moerr.NewInternalErrorNoCtxf("substrait: nil expression")
	}
	if _, err := substraitType(&x.Typ); err != nil {
		return nil, err
	}
	switch v := x.Expr.(type) {
	case *planpb.Expr_Col:
		// Joins and correlated expressions are rejected structurally, so col_pos
		// is the unambiguous ordinal in the single input regardless of MO's
		// binding-tag value in rel_pos.
		if v.Col == nil || v.Col.ColPos < 0 {
			return nil, moerr.NewInternalErrorNoCtxf("substrait: invalid column reference")
		}
		return field(v.Col.ColPos), nil
	case *planpb.Expr_Lit:
		return literal(v.Lit, &x.Typ)
	case *planpb.Expr_F:
		if v.F == nil || v.F.Func == nil {
			return nil, moerr.NewInternalErrorNoCtxf("substrait: malformed function")
		}
		name, ok := scalarIdentity(v.F.Func)
		if !ok {
			return nil, moerr.NewInternalErrorNoCtxf("substrait: unsupported scalar function %q", v.F.Func.ObjName)
		}
		if want := scalarArity(name); len(v.F.Args) != want {
			return nil, moerr.NewInternalErrorNoCtxf("substrait: %s requires %d arguments", name, want)
		}
		if err := validateMOOverload(v.F.Func, v.F.Args, &x.Typ); err != nil {
			return nil, err
		}
		if err := validateScalarSignature(name, &x.Typ, v.F.Args); err != nil {
			return nil, err
		}
		if _, err := substraitType(&x.Typ); err != nil {
			return nil, err
		}
		args := make([]*spb.Expression, len(v.F.Args))
		for i := range v.F.Args {
			var err error
			args[i], err = e.expr(v.F.Args[i])
			if err != nil {
				return nil, err
			}
		}
		return e.scalar(name, &x.Typ, args...), nil
	default:
		return nil, moerr.NewInternalErrorNoCtxf("substrait: unsupported expression %T", x.Expr)
	}
}

func (e *exporter) scalar(name string, typ *planpb.Type, args ...*spb.Expression) *spb.Expression {
	out, _ := substraitType(typ)
	fargs := make([]*spb.FunctionArgument, len(args))
	for i := range args {
		fargs[i] = valueArg(args[i])
	}
	return &spb.Expression{RexType: &spb.Expression_ScalarFunction_{ScalarFunction: &spb.Expression_ScalarFunction{FunctionReference: e.function(name), Arguments: fargs, OutputType: out}}}
}

func (e *exporter) function(name string) uint32 {
	if e.functions == nil {
		e.functions = make(map[string]uint32)
	}
	if anchor := e.functions[name]; anchor != 0 {
		return anchor
	}
	anchor := uint32(len(e.functions) + 1)
	e.functions[name] = anchor
	return anchor
}

func (e *exporter) extensions() []*extensions.SimpleExtensionDeclaration {
	byAnchor := make([]string, len(e.functions)+1)
	for name, anchor := range e.functions {
		byAnchor[anchor] = name
	}
	result := make([]*extensions.SimpleExtensionDeclaration, 0, len(e.functions))
	for anchor := 1; anchor < len(byAnchor); anchor++ {
		result = append(result, &extensions.SimpleExtensionDeclaration{MappingType: &extensions.SimpleExtensionDeclaration_ExtensionFunction_{ExtensionFunction: &extensions.SimpleExtensionDeclaration_ExtensionFunction{FunctionAnchor: uint32(anchor), Name: byAnchor[anchor]}}})
	}
	return result
}

func namedStruct(t *planpb.TableDef) (*spb.NamedStruct, error) {
	names := make([]string, 0, len(t.Cols))
	fields := make([]*spb.Type, 0, len(t.Cols))
	hidden := false
	for _, c := range t.Cols {
		if c == nil {
			return nil, moerr.NewInternalErrorNoCtxf("substrait: table %q has a nil column", t.Name)
		}
		if c.Hidden {
			hidden = true
			continue
		}
		if hidden {
			return nil, moerr.NewInternalErrorNoCtxf("substrait: table %q has non-suffix hidden columns", t.Name)
		}
		typ, err := substraitType(&c.Typ)
		if err != nil {
			return nil, moerr.NewInternalErrorNoCtxf("substrait: column %q: %v", c.Name, err)
		}
		names = append(names, c.Name)
		fields = append(fields, typ)
	}
	if len(fields) == 0 {
		return nil, moerr.NewInternalErrorNoCtxf("substrait: table %q has no exportable columns", t.Name)
	}
	return &spb.NamedStruct{Names: names, Struct: &spb.Type_Struct{Types: fields, Nullability: spb.Type_NULLABILITY_REQUIRED}}, nil
}

func validateExprFields(exprs []*planpb.Expr, width int) error {
	for _, expr := range exprs {
		if expr == nil {
			return moerr.NewInternalErrorNoCtxf("substrait: nil expression")
		}
		switch value := expr.Expr.(type) {
		case *planpb.Expr_Col:
			if value.Col == nil || value.Col.ColPos < 0 || int(value.Col.ColPos) >= width {
				return moerr.NewInternalErrorNoCtxf("substrait: column ordinal is outside input width %d", width)
			}
		case *planpb.Expr_F:
			if value.F == nil {
				return moerr.NewInternalErrorNoCtxf("substrait: malformed function")
			}
			if err := validateExprFields(value.F.Args, width); err != nil {
				return err
			}
		case *planpb.Expr_Lit:
		default:
			return moerr.NewInternalErrorNoCtxf("substrait: unsupported expression %T", expr.Expr)
		}
	}
	return nil
}

// CanonicalSchema serializes the exact Substrait schema used in ReadRel and
// lets snapshot admission detect catalog drift after logical planning.
func CanonicalSchema(t *planpb.TableDef) ([]byte, error) {
	schema, err := namedStruct(t)
	if err != nil {
		return nil, err
	}
	b, err := proto.MarshalOptions{Deterministic: true}.Marshal(schema)
	if err != nil {
		return nil, moerr.NewInternalErrorNoCtxf("substrait: marshal schema: %v", err)
	}
	return b, nil
}

func substraitType(t *planpb.Type) (*spb.Type, error) {
	if t == nil {
		return nil, moerr.NewInternalErrorNoCtxf("missing type")
	}
	n := spb.Type_NULLABILITY_NULLABLE
	if t.NotNullable {
		n = spb.Type_NULLABILITY_REQUIRED
	}
	switch types.T(t.Id) {
	case types.T_bool:
		return &spb.Type{Kind: &spb.Type_Bool{Bool: &spb.Type_Boolean{Nullability: n}}}, nil
	case types.T_int8:
		return &spb.Type{Kind: &spb.Type_I8_{I8: &spb.Type_I8{Nullability: n}}}, nil
	case types.T_int16:
		return &spb.Type{Kind: &spb.Type_I16_{I16: &spb.Type_I16{Nullability: n}}}, nil
	case types.T_int32:
		return &spb.Type{Kind: &spb.Type_I32_{I32: &spb.Type_I32{Nullability: n}}}, nil
	case types.T_int64:
		return &spb.Type{Kind: &spb.Type_I64_{I64: &spb.Type_I64{Nullability: n}}}, nil
	case types.T_float32:
		return &spb.Type{Kind: &spb.Type_Fp32{Fp32: &spb.Type_FP32{Nullability: n}}}, nil
	case types.T_float64:
		return &spb.Type{Kind: &spb.Type_Fp64{Fp64: &spb.Type_FP64{Nullability: n}}}, nil
	case types.T_char:
		return &spb.Type{Kind: &spb.Type_String_{String_: &spb.Type_String{Nullability: n}}}, nil
	case types.T_varchar:
		if t.Width < 0 {
			return nil, moerr.NewInternalErrorNoCtxf("unsupported negative varchar width")
		}
		return &spb.Type{Kind: &spb.Type_Varchar{Varchar: &spb.Type_VarChar{Length: t.Width, Nullability: n}}}, nil
	case types.T_date:
		return &spb.Type{Kind: &spb.Type_Date_{Date: &spb.Type_Date{Nullability: n}}}, nil
	case types.T_timestamp:
		return &spb.Type{Kind: &spb.Type_PrecisionTimestamp_{PrecisionTimestamp: &spb.Type_PrecisionTimestamp{Precision: 6, Nullability: n}}}, nil
	default:
		return nil, moerr.NewInternalErrorNoCtxf("unsupported type %s", types.T(t.Id).String())
	}
}

func field(pos int32) *spb.Expression {
	return &spb.Expression{RexType: &spb.Expression_Selection{Selection: &spb.Expression_FieldReference{ReferenceType: &spb.Expression_FieldReference_DirectReference{DirectReference: &spb.Expression_ReferenceSegment{ReferenceType: &spb.Expression_ReferenceSegment_StructField_{StructField: &spb.Expression_ReferenceSegment_StructField{Field: pos}}}}, RootType: &spb.Expression_FieldReference_RootReference_{RootReference: &spb.Expression_FieldReference_RootReference{}}}}}
}

func literal(l *planpb.Literal, typ *planpb.Type) (*spb.Expression, error) {
	if l == nil {
		return nil, moerr.NewInternalErrorNoCtxf("substrait: nil literal")
	}
	wrap := func(lit *spb.Expression_Literal) *spb.Expression {
		return &spb.Expression{RexType: &spb.Expression_Literal_{Literal: lit}}
	}
	if l.Isnull {
		t, err := substraitType(typ)
		if err != nil {
			return nil, err
		}
		return wrap(&spb.Expression_Literal{LiteralType: &spb.Expression_Literal_Null{Null: t}}), nil
	}
	oid := types.T(typ.Id)
	mismatch := func() (*spb.Expression, error) {
		return nil, moerr.NewInternalErrorNoCtxf("substrait: literal value does not match declared type %s", oid.String())
	}
	switch v := l.Value.(type) {
	case *planpb.Literal_Bval:
		if oid != types.T_bool {
			return mismatch()
		}
		return wrap(&spb.Expression_Literal{LiteralType: &spb.Expression_Literal_Boolean{Boolean: v.Bval}}), nil
	case *planpb.Literal_I8Val:
		if oid != types.T_int8 {
			return mismatch()
		}
		return wrap(&spb.Expression_Literal{LiteralType: &spb.Expression_Literal_I8{I8: v.I8Val}}), nil
	case *planpb.Literal_I16Val:
		if oid != types.T_int16 {
			return mismatch()
		}
		return wrap(&spb.Expression_Literal{LiteralType: &spb.Expression_Literal_I16{I16: v.I16Val}}), nil
	case *planpb.Literal_I32Val:
		if oid != types.T_int32 {
			return mismatch()
		}
		return wrap(&spb.Expression_Literal{LiteralType: &spb.Expression_Literal_I32{I32: v.I32Val}}), nil
	case *planpb.Literal_I64Val:
		if oid != types.T_int64 {
			return mismatch()
		}
		return wrap(&spb.Expression_Literal{LiteralType: &spb.Expression_Literal_I64{I64: v.I64Val}}), nil
	case *planpb.Literal_Fval:
		if oid != types.T_float32 {
			return mismatch()
		}
		return wrap(&spb.Expression_Literal{LiteralType: &spb.Expression_Literal_Fp32{Fp32: v.Fval}}), nil
	case *planpb.Literal_Dval:
		if oid != types.T_float64 {
			return mismatch()
		}
		return wrap(&spb.Expression_Literal{LiteralType: &spb.Expression_Literal_Fp64{Fp64: v.Dval}}), nil
	case *planpb.Literal_Sval:
		if oid != types.T_char && oid != types.T_varchar {
			return mismatch()
		}
		if oid == types.T_varchar {
			return wrap(&spb.Expression_Literal{LiteralType: &spb.Expression_Literal_VarChar_{VarChar: &spb.Expression_Literal_VarChar{Value: v.Sval, Length: uint32(typ.Width)}}}), nil
		}
		return wrap(&spb.Expression_Literal{LiteralType: &spb.Expression_Literal_String_{String_: v.Sval}}), nil
	case *planpb.Literal_Dateval:
		if oid != types.T_date {
			return mismatch()
		}
		return wrap(&spb.Expression_Literal{LiteralType: &spb.Expression_Literal_Date{Date: v.Dateval}}), nil
	case *planpb.Literal_Timestampval:
		if oid != types.T_timestamp {
			return mismatch()
		}
		return wrap(&spb.Expression_Literal{LiteralType: &spb.Expression_Literal_PrecisionTimestamp_{PrecisionTimestamp: &spb.Expression_Literal_PrecisionTimestamp{Precision: 6, Value: v.Timestampval}}}), nil
	default:
		return nil, moerr.NewInternalErrorNoCtxf("substrait: unsupported literal %T", l.Value)
	}
}

func valueArg(x *spb.Expression) *spb.FunctionArgument {
	return &spb.FunctionArgument{ArgType: &spb.FunctionArgument_Value{Value: x}}
}

func scalarName(name string) (string, bool) {
	n := strings.ToLower(name)
	m := map[string]string{"and": "and", "or": "or", "not": "not", "=": "equal", "equal": "equal", "!=": "not_equal", "<>": "not_equal", "<": "lt", "<=": "lte", ">": "gt", ">=": "gte", "is_null": "is_null", "is_not_null": "is_not_null", "<=>": "is_not_distinct_from", "+": "add", "-": "subtract", "*": "multiply", "/": "divide", "%": "modulus", "between": "between"}
	v, ok := m[n]
	return v, ok
}

func scalarIdentity(ref *planpb.ObjectRef) (string, bool) {
	if ref == nil || uint64(ref.Obj)&function.Distinct != 0 {
		return "", false
	}
	fid, _ := function.DecodeOverloadID(ref.Obj)
	ids := map[int32]string{function.AND: "and", function.OR: "or", function.NOT: "not", function.EQUAL: "equal", function.NOT_EQUAL: "not_equal", function.LESS_THAN: "lt", function.LESS_EQUAL: "lte", function.GREAT_THAN: "gt", function.GREAT_EQUAL: "gte", function.ISNULL: "is_null", function.ISNOTNULL: "is_not_null", function.NULL_SAFE_EQUAL: "is_not_distinct_from", function.PLUS: "add", function.MINUS: "subtract", function.MULTI: "multiply", function.DIV: "divide", function.MOD: "modulus", function.BETWEEN: "between"}
	name, ok := ids[fid]
	if !ok {
		return "", false
	}
	display, ok := scalarName(ref.ObjName)
	return name, ok && display == name
}

func scalarArity(name string) int {
	switch name {
	case "not", "is_null", "is_not_null":
		return 1
	case "between":
		return 3
	default:
		return 2
	}
}

func aggregateIdentity(ref *planpb.ObjectRef) (string, bool) {
	if ref == nil {
		return "", false
	}
	fid, _ := function.DecodeOverloadID(ref.Obj)
	ids := map[int32]string{function.COUNT: "count", function.STARCOUNT: "count", function.SUM: "sum", function.MIN: "min", function.MAX: "max", function.AVG: "avg"}
	name, ok := ids[fid]
	if !ok {
		return "", false
	}
	display := strings.ToLower(ref.ObjName)
	if fid == function.STARCOUNT {
		return name, display == "starcount" || display == "count"
	}
	return name, display == name
}

func validateScalarSignature(name string, out *planpb.Type, args []*planpb.Expr) error {
	isBool := func(t *planpb.Type) bool { return t != nil && types.T(t.Id) == types.T_bool }
	same := func() bool {
		if len(args) == 0 {
			return false
		}
		for _, a := range args {
			if a == nil || a.Typ.Id != args[0].Typ.Id {
				return false
			}
		}
		return true
	}
	numeric := func(t *planpb.Type) bool {
		if t == nil {
			return false
		}
		switch types.T(t.Id) {
		case types.T_int8, types.T_int16, types.T_int32, types.T_int64, types.T_float32, types.T_float64:
			return true
		}
		return false
	}
	switch name {
	case "and", "or", "not":
		if !isBool(out) {
			return moerr.NewInternalErrorNoCtxf("substrait: %s has non-boolean result", name)
		}
		for _, a := range args {
			if a == nil || !isBool(&a.Typ) {
				return moerr.NewInternalErrorNoCtxf("substrait: %s has non-boolean argument", name)
			}
		}
	case "equal", "not_equal", "lt", "lte", "gt", "gte", "is_not_distinct_from", "between":
		if !isBool(out) || !same() {
			return moerr.NewInternalErrorNoCtxf("substrait: unsupported %s signature", name)
		}
	case "is_null", "is_not_null":
		if !isBool(out) {
			return moerr.NewInternalErrorNoCtxf("substrait: unsupported %s signature", name)
		}
	case "add", "subtract", "multiply", "divide", "modulus":
		if !numeric(out) || !same() || args[0].Typ.Id != out.Id {
			return moerr.NewInternalErrorNoCtxf("substrait: unsupported %s signature", name)
		}
	}
	return nil
}

func validateMOOverload(ref *planpb.ObjectRef, args []*planpb.Expr, out *planpb.Type) error {
	inputs := make([]types.Type, len(args))
	for i, a := range args {
		if a == nil {
			return moerr.NewInternalErrorNoCtxf("substrait: nil function argument")
		}
		inputs[i] = types.Type{Oid: types.T(a.Typ.Id), Width: a.Typ.Width, Scale: a.Typ.Scale}
	}
	resolved, err := function.GetFunctionByName(context.Background(), ref.ObjName, inputs)
	if err != nil {
		return moerr.NewInternalErrorNoCtxf("substrait: resolve MO overload %q: %v", ref.ObjName, err)
	}
	_, cast := resolved.ShouldDoImplicitTypeCast()
	ret := resolved.GetReturnType()
	if cast || resolved.GetEncodedOverloadID() != ref.Obj || out == nil || ret.Oid != types.T(out.Id) || ret.Width != out.Width || ret.Scale != out.Scale {
		return moerr.NewInternalErrorNoCtxf("substrait: MO overload identity or result type mismatch for %q", ref.ObjName)
	}
	return nil
}

func nonnegativeIntLiteral(x *planpb.Expr, absent int64) (int64, error) {
	if x == nil {
		return absent, nil
	}
	l := x.GetLit()
	if l == nil || l.Isnull {
		return 0, moerr.NewInternalErrorNoCtxf("must be a constant integer")
	}
	var v int64
	switch n := l.Value.(type) {
	case *planpb.Literal_I8Val:
		v = int64(n.I8Val)
	case *planpb.Literal_I16Val:
		v = int64(n.I16Val)
	case *planpb.Literal_I32Val:
		v = int64(n.I32Val)
	case *planpb.Literal_I64Val:
		v = n.I64Val
	default:
		return 0, moerr.NewInternalErrorNoCtxf("must be a signed integer")
	}
	if v < 0 {
		return 0, moerr.NewInternalErrorNoCtxf("must be non-negative")
	}
	return v, nil
}

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
	"math"
	"strings"
	"sync"
	"unicode/utf8"

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
	query    *planpb.Query
	reads    []Read
	headings []string
}

// Read identifies one physical table scan which needs a TaeRead lease.
type Read struct {
	NodeID int32
	// AccountID is bound by Admit immediately before snapshot preparation;
	// logical export deliberately leaves it unset.
	AccountID     uint64
	DatabaseID    uint64
	TableID       uint64
	SchemaVersion uint32
	Columns       []ColumnMapping
	Schema        []byte // deterministic Substrait NamedStruct bytes
}

// ColumnMapping binds one exported ordinal to the physical TAE column used at
// the planning snapshot. Names and logical types alone are not stable across a
// drop/re-add schema evolution.
type ColumnMapping struct {
	ColumnID       uint64
	SequenceNumber uint32
}

func (c *Candidate) Reads() []Read {
	result := append([]Read(nil), c.reads...)
	for i := range result {
		result[i].Schema = append([]byte(nil), result[i].Schema...)
		result[i].Columns = append([]ColumnMapping(nil), result[i].Columns...)
	}
	return result
}

// Export validates q without performing I/O.
func Export(q *planpb.Query) (*Candidate, error) {
	if q == nil {
		return nil, moerr.NewInternalErrorNoCtx("substrait: missing query")
	}
	if q.StmtType != planpb.Query_SELECT || len(q.Steps) != 1 || len(q.BackgroundQueries) != 0 {
		return nil, notEligiblef(EligibilityPlanShape, "exactly one SELECT query root is required")
	}
	c := &Candidate{query: q}
	e := exporter{query: q, readValues: make(map[int32][]byte), validateOnly: true}
	if _, err := e.node(q.Steps[0]); err != nil {
		return nil, err
	}
	width, err := e.nodeWidth(q.Steps[0])
	if err != nil {
		return nil, err
	}
	if len(q.Headings) != width {
		return nil, moerr.NewInternalErrorNoCtxf("substrait: query has %d headings for %d output columns", len(q.Headings), width)
	}
	c.reads = e.reads
	c.headings = append([]string(nil), q.Headings...)
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
		Relations:        []*spb.PlanRel{{RelType: &spb.PlanRel_Root{Root: &spb.RelRoot{Input: root, Names: append([]string(nil), c.headings...)}}}},
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
		return nil, notEligiblef(EligibilityOperator, "node %d uses unsupported operator %s", id, n.NodeType.String())
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
		return 0, notEligiblef(EligibilityOperator, "unsupported width for %s", n.NodeType.String())
	}
}

func (e *exporter) unary(n *planpb.Node) (*spb.Rel, error) {
	if len(n.Children) != 1 {
		return nil, moerr.NewInternalErrorNoCtxf("substrait: %s node %d requires one child", n.NodeType.String(), n.NodeId)
	}
	return e.node(n.Children[0])
}

func (e *exporter) read(n *planpb.Node) (*spb.Rel, error) {
	if len(n.Children) != 0 || n.TableDef == nil || n.ObjRef == nil || n.ObjRef.Db <= 0 || n.TableDef.TblId == 0 || uint64(n.ObjRef.Obj) != n.TableDef.TblId {
		return nil, moerr.NewInternalErrorNoCtxf("substrait: node %d has a malformed table scan", n.NodeId)
	}
	if n.TableDef.IsTemporary || n.ScanSnapshot != nil || n.ObjRef.Snapshot != nil || n.ObjRef.PubInfo != nil || (n.TableDef.TableType != "" && n.TableDef.TableType != "r") {
		return nil, notEligiblef(EligibilityPlanShape, "node %d is not a persistent TAE table scan", n.NodeId)
	}
	schema, err := namedStruct(n.TableDef)
	if err != nil {
		return nil, err
	}
	schemaBytes, err := proto.MarshalOptions{Deterministic: true}.Marshal(schema)
	if err != nil {
		return nil, err
	}
	columns, err := columnMapping(n.TableDef)
	if err != nil {
		return nil, err
	}
	e.reads = append(e.reads, Read{
		NodeID:        n.NodeId,
		DatabaseID:    uint64(n.ObjRef.Db),
		TableID:       n.TableDef.TblId,
		SchemaVersion: n.TableDef.Version,
		Columns:       columns,
		Schema:        schemaBytes,
	})
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
	if len(n.GroupingFlag) != 0 {
		if len(n.GroupingFlag) != len(n.GroupBy) {
			return nil, notEligiblef(EligibilityOperator, "grouping sets are unsupported")
		}
		for _, flag := range n.GroupingFlag {
			if !flag {
				return nil, notEligiblef(EligibilityOperator, "grouping sets are unsupported")
			}
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
			return nil, notEligiblef(EligibilityOperator, "unsupported aggregate form %q", f.Func.ObjName)
		}
		name, ok := aggregateName(f.Func.ObjName)
		if !ok {
			return nil, notEligiblef(EligibilityOperator, "unsupported aggregate %q", f.Func.ObjName)
		}
		if len(f.Args) != 1 {
			return nil, notEligiblef(EligibilityExpression, "unsupported aggregate form %q", f.Func.ObjName)
		}
		functionID, _ := function.DecodeOverloadID(f.Func.Obj)
		if functionID == function.STARCOUNT {
			literal := f.Args[0].GetLit()
			if literal == nil || literal.Isnull {
				return nil, moerr.NewInternalErrorNoCtxf("substrait: starcount requires a non-NULL literal argument")
			}
		}
		outputType := x.Typ
		if len(n.GroupBy) == 0 && aggregateCanReturnNullOnEmpty(functionID) {
			outputType.NotNullable = false
		}
		supported, capabilityErr := hasSemanticCapability(semanticAggregate, name, f.Func, f.Args, &outputType)
		if capabilityErr != nil {
			return nil, capabilityErr
		}
		if !supported {
			return nil, notEligiblef(EligibilityExpression, "aggregate overload %q has no declared Sirius semantic equivalence", f.Func.ObjName)
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
		out, xerr := substraitType(&outputType)
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
		if order == nil {
			return nil, moerr.NewInternalErrorNoCtxf("substrait: malformed sort at %d", i)
		}
		if order.Collation != "" || int32(order.Flag)&int32(planpb.OrderBySpec_UNIQUE) != 0 {
			return nil, notEligiblef(EligibilityOperator, "unsupported sort at %d", i)
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
		invalidDirection := directionBits != 0 && directionBits != int32(planpb.OrderBySpec_ASC) && directionBits != int32(planpb.OrderBySpec_DESC)
		conflictingNulls := nullBits == int32(planpb.OrderBySpec_NULLS_FIRST)|int32(planpb.OrderBySpec_NULLS_LAST)
		if invalidDirection || conflictingNulls || flag != known {
			return nil, notEligiblef(EligibilityOperator, "unsupported sort flags %d", flag)
		}
		desc := directionBits == int32(planpb.OrderBySpec_DESC)
		nullsFirst := flag&int32(planpb.OrderBySpec_NULLS_FIRST) != 0
		if flag&int32(planpb.OrderBySpec_NULLS_LAST) == 0 && !nullsFirst {
			nullsFirst = !desc
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
	fetch := &spb.FetchRel{Input: input}
	if n.Limit != nil {
		count, err := nonnegativeIntLiteral(n.Limit, 0)
		if err != nil {
			return nil, moerr.NewInternalErrorNoCtxf("substrait: limit: %v", err)
		}
		fetch.CountMode = &spb.FetchRel_Count{Count: count}
	}
	if n.Offset != nil {
		offset, err := nonnegativeIntLiteral(n.Offset, 0)
		if err != nil {
			return nil, moerr.NewInternalErrorNoCtxf("substrait: offset: %v", err)
		}
		fetch.OffsetMode = &spb.FetchRel_Offset{Offset: offset}
	}
	return &spb.Rel{RelType: &spb.Rel_Fetch{Fetch: fetch}}, nil
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
		name, ok := scalarName(v.F.Func.ObjName)
		if !ok {
			return nil, notEligiblef(EligibilityExpression, "unsupported scalar function %q", v.F.Func.ObjName)
		}
		if want := scalarArity(name); len(v.F.Args) != want {
			return nil, notEligiblef(EligibilityExpression, "%s requires %d arguments", name, want)
		}
		if err := validateScalarSignature(name, &x.Typ, v.F.Args); err != nil {
			return nil, err
		}
		supported, capabilityErr := hasSemanticCapability(semanticScalar, name, v.F.Func, v.F.Args, &x.Typ)
		if capabilityErr != nil {
			return nil, capabilityErr
		}
		if !supported {
			return nil, notEligiblef(EligibilityExpression, "scalar overload %q has no declared Sirius semantic equivalence", v.F.Func.ObjName)
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
		return nil, notEligiblef(EligibilityExpression, "unsupported expression %T", x.Expr)
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
			if IsNotEligible(err) {
				return nil, err
			}
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

func columnMapping(t *planpb.TableDef) ([]ColumnMapping, error) {
	if t == nil {
		return nil, moerr.NewInternalErrorNoCtx("substrait: missing table for column mapping")
	}
	result := make([]ColumnMapping, 0, len(t.Cols))
	hidden := false
	for _, column := range t.Cols {
		if column == nil {
			return nil, moerr.NewInternalErrorNoCtxf("substrait: table %q has a nil column", t.Name)
		}
		if column.Hidden {
			hidden = true
			continue
		}
		if hidden {
			return nil, moerr.NewInternalErrorNoCtxf("substrait: table %q has non-suffix hidden columns", t.Name)
		}
		result = append(result, ColumnMapping{ColumnID: column.ColId, SequenceNumber: column.Seqnum})
	}
	if len(result) == 0 {
		return nil, moerr.NewInternalErrorNoCtxf("substrait: table %q has no exportable columns", t.Name)
	}
	return result, nil
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
			return notEligiblef(EligibilityExpression, "unsupported expression %T", expr.Expr)
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
		if t.Width <= 0 {
			return nil, notEligiblef(EligibilityType, "char width %d is outside the supported bound", t.Width)
		}
		return &spb.Type{Kind: &spb.Type_FixedChar_{FixedChar: &spb.Type_FixedChar{Length: t.Width, Nullability: n}}}, nil
	case types.T_varchar:
		if t.Width < 0 {
			return nil, notEligiblef(EligibilityType, "negative varchar width %d", t.Width)
		}
		return &spb.Type{Kind: &spb.Type_Varchar{Varchar: &spb.Type_VarChar{Length: t.Width, Nullability: n}}}, nil
	case types.T_date:
		return &spb.Type{Kind: &spb.Type_Date_{Date: &spb.Type_Date{Nullability: n}}}, nil
	case types.T_timestamp:
		if t.Scale != 6 {
			return nil, notEligiblef(EligibilityType, "timestamp precision %d is not microsecond precision", t.Scale)
		}
		return &spb.Type{Kind: &spb.Type_PrecisionTimestamp_{PrecisionTimestamp: &spb.Type_PrecisionTimestamp{Precision: 6, Nullability: n}}}, nil
	default:
		return nil, notEligiblef(EligibilityType, "unsupported type %s", types.T(t.Id).String())
	}
}

func field(pos int32) *spb.Expression {
	return &spb.Expression{RexType: &spb.Expression_Selection{Selection: &spb.Expression_FieldReference{ReferenceType: &spb.Expression_FieldReference_DirectReference{DirectReference: &spb.Expression_ReferenceSegment{ReferenceType: &spb.Expression_ReferenceSegment_StructField_{StructField: &spb.Expression_ReferenceSegment_StructField{Field: pos}}}}, RootType: &spb.Expression_FieldReference_RootReference_{RootReference: &spb.Expression_FieldReference_RootReference{}}}}}
}

func literal(l *planpb.Literal, typ *planpb.Type) (*spb.Expression, error) {
	if l == nil || typ == nil {
		return nil, moerr.NewInternalErrorNoCtxf("substrait: nil literal")
	}
	declaredType, err := substraitType(typ)
	if err != nil {
		return nil, err
	}
	wrap := func(lit *spb.Expression_Literal) *spb.Expression {
		return &spb.Expression{RexType: &spb.Expression_Literal_{Literal: lit}}
	}
	if l.Isnull {
		return wrap(&spb.Expression_Literal{LiteralType: &spb.Expression_Literal_Null{Null: declaredType}}), nil
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
		if utf8.RuneCountInString(v.Sval) != int(typ.Width) {
			return nil, notEligiblef(EligibilityType, "char literal length does not match width %d", typ.Width)
		}
		return wrap(&spb.Expression_Literal{LiteralType: &spb.Expression_Literal_FixedChar{FixedChar: v.Sval}}), nil
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
	m := map[string]string{"and": "and", "or": "or", "not": "not", "=": "equal", "equal": "equal", "!=": "not_equal", "<>": "not_equal", "<": "lt", "<=": "lte", ">": "gt", ">=": "gte", "is_null": "is_null", "isnull": "is_null", "is_not_null": "is_not_null", "isnotnull": "is_not_null", "<=>": "is_not_distinct_from", "+": "add", "-": "subtract", "*": "multiply", "/": "divide", "%": "modulus", "mod": "modulus", "between": "between"}
	v, ok := m[n]
	return v, ok
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

func aggregateName(name string) (string, bool) {
	switch strings.ToLower(name) {
	case "count", "starcount":
		return "count", true
	case "sum":
		return "sum", true
	case "min":
		return "min", true
	case "max":
		return "max", true
	default:
		return "", false
	}
}

type semanticCapabilityKind uint8

const (
	semanticScalar semanticCapabilityKind = iota + 1
	semanticAggregate
)

type semanticTypeKey struct {
	id, width, scale int32
	notNullable      bool
}

type semanticCapabilityKey struct {
	kind       semanticCapabilityKind
	overloadID int64
	argumentN  uint8
	arguments  [3]semanticTypeKey
	result     semanticTypeKey
}

type semanticCapability struct {
	name        string
	equivalence string
}

type semanticDeclaration struct {
	kind        semanticCapabilityKind
	functionID  int32
	moName      string
	name        string
	inputs      []types.Type
	equivalence string
}

// The registry is deliberately exact: a declaration resolves to one MatrixOne
// overload ID and is expanded across explicit argument/result nullability
// shapes. A base function ID or display name is never sufficient admission.
var semanticDeclarations = []semanticDeclaration{
	{semanticScalar, function.AND, "and", "and", []types.Type{types.T_bool.ToType(), types.T_bool.ToType()}, "sirius-v1:boolean-three-valued-logic"},
	{semanticScalar, function.OR, "or", "or", []types.Type{types.T_bool.ToType(), types.T_bool.ToType()}, "sirius-v1:boolean-three-valued-logic"},
	{semanticScalar, function.NOT, "not", "not", []types.Type{types.T_bool.ToType()}, "sirius-v1:boolean-three-valued-logic"},
	{semanticScalar, function.EQUAL, "=", "equal", []types.Type{types.T_int64.ToType(), types.T_int64.ToType()}, "sirius-v1:signed-i64-comparison"},
	{semanticScalar, function.NOT_EQUAL, "!=", "not_equal", []types.Type{types.T_int64.ToType(), types.T_int64.ToType()}, "sirius-v1:signed-i64-comparison"},
	{semanticScalar, function.LESS_THAN, "<", "lt", []types.Type{types.T_int64.ToType(), types.T_int64.ToType()}, "sirius-v1:signed-i64-comparison"},
	{semanticScalar, function.LESS_EQUAL, "<=", "lte", []types.Type{types.T_int64.ToType(), types.T_int64.ToType()}, "sirius-v1:signed-i64-comparison"},
	{semanticScalar, function.GREAT_THAN, ">", "gt", []types.Type{types.T_int64.ToType(), types.T_int64.ToType()}, "sirius-v1:signed-i64-comparison"},
	{semanticScalar, function.GREAT_EQUAL, ">=", "gte", []types.Type{types.T_int64.ToType(), types.T_int64.ToType()}, "sirius-v1:signed-i64-comparison"},
	{semanticScalar, function.ISNULL, "isnull", "is_null", []types.Type{types.T_int64.ToType()}, "sirius-v1:null-predicate"},
	{semanticScalar, function.ISNOTNULL, "isnotnull", "is_not_null", []types.Type{types.T_int64.ToType()}, "sirius-v1:null-predicate"},
	{semanticScalar, function.NULL_SAFE_EQUAL, "<=>", "is_not_distinct_from", []types.Type{types.T_int64.ToType(), types.T_int64.ToType()}, "sirius-v1:null-safe-signed-i64-equality"},
	{semanticScalar, function.PLUS, "+", "add", []types.Type{types.T_int64.ToType(), types.T_int64.ToType()}, "sirius-v1:checked-signed-i64-arithmetic"},
	{semanticScalar, function.MINUS, "-", "subtract", []types.Type{types.T_int64.ToType(), types.T_int64.ToType()}, "sirius-v1:checked-signed-i64-arithmetic"},
	{semanticScalar, function.MULTI, "*", "multiply", []types.Type{types.T_int64.ToType(), types.T_int64.ToType()}, "sirius-v1:checked-signed-i64-arithmetic"},
	{semanticScalar, function.MOD, "mod", "modulus", []types.Type{types.T_int64.ToType(), types.T_int64.ToType()}, "sirius-v1:mysql-signed-i64-modulus"},
	{semanticScalar, function.BETWEEN, "between", "between", []types.Type{types.T_int64.ToType(), types.T_int64.ToType(), types.T_int64.ToType()}, "sirius-v1:signed-i64-between"},
	{semanticAggregate, function.COUNT, "count", "count", []types.Type{types.T_int64.ToType()}, "sirius-v1:count-i64"},
	{semanticAggregate, function.STARCOUNT, "starcount", "count", []types.Type{types.T_int64.ToType()}, "sirius-v1:count-all"},
	{semanticAggregate, function.MIN, "min", "min", []types.Type{types.T_int64.ToType()}, "sirius-v1:signed-i64-min"},
	{semanticAggregate, function.MAX, "max", "max", []types.Type{types.T_int64.ToType()}, "sirius-v1:signed-i64-max"},
}

var (
	semanticRegistryOnce sync.Once
	semanticRegistry     map[semanticCapabilityKey]semanticCapability
	semanticRegistryErr  error
)

func loadSemanticRegistry() (map[semanticCapabilityKey]semanticCapability, error) {
	semanticRegistryOnce.Do(func() {
		semanticRegistry, semanticRegistryErr = buildSemanticCapabilities(semanticDeclarations)
	})
	return semanticRegistry, semanticRegistryErr
}

func buildSemanticCapabilities(declarations []semanticDeclaration) (map[semanticCapabilityKey]semanticCapability, error) {
	result := make(map[semanticCapabilityKey]semanticCapability)
	for _, declaration := range declarations {
		resolved, err := function.GetFunctionByName(context.Background(), declaration.moName, declaration.inputs)
		if err != nil {
			return nil, moerr.NewInternalErrorNoCtxf("substrait semantic capability %q: %v", declaration.moName, err)
		}
		_, casts := resolved.ShouldDoImplicitTypeCast()
		functionID, _ := function.DecodeOverloadID(resolved.GetEncodedOverloadID())
		if casts || functionID != declaration.functionID || declaration.equivalence == "" || len(declaration.inputs) > 3 {
			return nil, moerr.NewInternalErrorNoCtxf("substrait semantic capability %q has an invalid MatrixOne declaration", declaration.moName)
		}
		for nullability := 0; nullability < 1<<len(declaration.inputs); nullability++ {
			key := semanticCapabilityKey{kind: declaration.kind, overloadID: resolved.GetEncodedOverloadID(), argumentN: uint8(len(declaration.inputs))}
			arguments := make([]*planpb.Expr, len(declaration.inputs))
			for i, input := range declaration.inputs {
				notNullable := nullability&(1<<i) != 0
				key.arguments[i] = semanticTypeKey{id: int32(input.Oid), width: input.Width, scale: input.Scale, notNullable: notNullable}
				arguments[i] = &planpb.Expr{Typ: planpb.Type{Id: int32(input.Oid), Width: input.Width, Scale: input.Scale, NotNullable: notNullable}}
			}
			output := resolved.GetReturnType()
			key.result = semanticTypeKey{
				id:          int32(output.Oid),
				width:       output.Width,
				scale:       output.Scale,
				notNullable: function.DeduceNotNullable(resolved.GetEncodedOverloadID(), arguments),
			}
			capability := semanticCapability{name: declaration.name, equivalence: declaration.equivalence}
			if err := addSemanticCapability(result, key, capability, declaration.moName); err != nil {
				return nil, err
			}
			if declaration.kind == semanticAggregate && aggregateCanReturnNullOnEmpty(functionID) && key.result.notNullable {
				nullableKey := key
				nullableKey.result.notNullable = false
				if err := addSemanticCapability(result, nullableKey, capability, declaration.moName); err != nil {
					return nil, err
				}
			}
		}
	}
	return result, nil
}

func aggregateCanReturnNullOnEmpty(functionID int32) bool {
	switch functionID {
	case function.MIN, function.MAX:
		return true
	default:
		return false
	}
}

func addSemanticCapability(result map[semanticCapabilityKey]semanticCapability, key semanticCapabilityKey, capability semanticCapability, moName string) error {
	if existing, ok := result[key]; ok && existing != capability {
		return moerr.NewInternalErrorNoCtxf("substrait semantic capability collision for %q", moName)
	}
	result[key] = capability
	return nil
}

func hasSemanticCapability(kind semanticCapabilityKind, name string, ref *planpb.ObjectRef, args []*planpb.Expr, out *planpb.Type) (bool, error) {
	if ref == nil || out == nil || len(args) > 3 {
		return false, nil
	}
	key := semanticCapabilityKey{kind: kind, overloadID: ref.Obj, argumentN: uint8(len(args)), result: semanticTypeFromPlan(out)}
	for i, argument := range args {
		if argument == nil {
			return false, nil
		}
		key.arguments[i] = semanticTypeFromPlan(&argument.Typ)
	}
	registry, err := loadSemanticRegistry()
	if err != nil {
		return false, err
	}
	capability, ok := registry[key]
	return ok && capability.name == name && capability.equivalence != "", nil
}

func semanticTypeFromPlan(value *planpb.Type) semanticTypeKey {
	if value == nil {
		return semanticTypeKey{}
	}
	return semanticTypeKey{id: value.Id, width: value.Width, scale: value.Scale, notNullable: value.NotNullable}
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
			return notEligiblef(EligibilityExpression, "%s has non-boolean result", name)
		}
		for _, a := range args {
			if a == nil || !isBool(&a.Typ) {
				return notEligiblef(EligibilityExpression, "%s has non-boolean argument", name)
			}
		}
	case "equal", "not_equal", "lt", "lte", "gt", "gte", "is_not_distinct_from", "between":
		if !isBool(out) || !same() {
			return notEligiblef(EligibilityExpression, "unsupported %s signature", name)
		}
	case "is_null", "is_not_null":
		if !isBool(out) {
			return notEligiblef(EligibilityExpression, "unsupported %s signature", name)
		}
	case "add", "subtract", "multiply", "divide", "modulus":
		if !numeric(out) || !same() || args[0].Typ.Id != out.Id {
			return notEligiblef(EligibilityExpression, "unsupported %s signature", name)
		}
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
	case *planpb.Literal_U64Val:
		if n.U64Val > math.MaxInt64 {
			return 0, moerr.NewInternalErrorNoCtxf("must fit the Substrait signed integer range")
		}
		v = int64(n.U64Val)
	default:
		return 0, moerr.NewInternalErrorNoCtxf("must be an integer")
	}
	if v < 0 {
		return 0, moerr.NewInternalErrorNoCtxf("must be non-negative")
	}
	return v, nil
}

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
	"encoding/binary"
	"math"
	"strings"
	"sync"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/rule"
	spb "github.com/substrait-io/substrait-protobuf/go/substraitpb"
	"github.com/substrait-io/substrait-protobuf/go/substraitpb/extensions"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
)

const (
	Version        = "0.78.0"
	TaeReadTypeURL = "type.googleapis.com/matrixone.sirius.v1.TaeRead"
	MaxPlanBytes   = 16 << 20
	// Bound the materialization of optimizer-folded IN lists before allocating
	// one Substrait expression per member.
	maxLiteralVectorValues = 1 << 16
)

// Candidate is a fully validated logical plan with unresolved storage reads.
// The read handles are installed only after snapshot admission succeeds.
type Candidate struct {
	query    *planpb.Query
	reads    []Read
	headings []string
	types    []planpb.Type
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

// OutputTypes returns the MatrixOne result contract corresponding to the
// Substrait root names. Transport decoding uses it to restore MO physical
// representations (notably DATE and unsigned EXTRACT results).
func (c *Candidate) OutputTypes() []planpb.Type {
	if c == nil {
		return nil
	}
	return append([]planpb.Type(nil), c.types...)
}

// Export validates q without performing I/O.
func Export(q *planpb.Query) (*Candidate, error) {
	if q == nil {
		return nil, moerr.NewInternalErrorNoCtx("substrait: missing query")
	}
	if q.StmtType != planpb.Query_SELECT || len(q.Steps) == 0 || len(q.BackgroundQueries) != 0 {
		return nil, notEligiblef(EligibilityPlanShape, "a SELECT query root is required")
	}
	// Adaptive rank selection inspects the complete MO node array, including
	// nodes outside the final reachable tree. Reject it at the same whole-plan
	// boundary instead of letting export silently bypass that compile decision.
	for nodeID, node := range q.Nodes {
		if node != nil && node.RankOption != nil {
			return nil, notEligiblef(EligibilityOperator, "node %d carries unsupported rank semantics", nodeID)
		}
	}
	c := &Candidate{query: q}
	e := exporter{query: q, readValues: make(map[int32][]byte), validateOnly: true}
	for step, rootID := range q.Steps {
		if rootID < 0 || int(rootID) >= len(q.Nodes) {
			return nil, moerr.NewInternalErrorNoCtxf("substrait: invalid root node id %d at step %d", rootID, step)
		}
		e.stepOrdinal = int32(step)
		root := q.Nodes[rootID]
		if step < len(q.Steps)-1 && (root == nil || root.NodeType != planpb.Node_SINK) {
			return nil, notEligiblef(EligibilityPlanShape, "step %d is not a shared sink producer", step)
		}
		if step == len(q.Steps)-1 && root != nil && root.NodeType == planpb.Node_SINK {
			return nil, notEligiblef(EligibilityPlanShape, "the final SELECT step cannot be a sink")
		}
		if _, err := e.node(rootID); err != nil {
			return nil, err
		}
	}
	finalID := q.Steps[len(q.Steps)-1]
	width, err := e.nodeWidth(finalID)
	if err != nil {
		return nil, err
	}
	if len(q.Headings) != width {
		return nil, moerr.NewInternalErrorNoCtxf("substrait: query has %d headings for %d output columns", len(q.Headings), width)
	}
	c.reads = e.reads
	c.headings = append([]string(nil), q.Headings...)
	c.types, err = e.outputTypes(finalID)
	if err != nil {
		return nil, err
	}
	if len(c.types) != width {
		return nil, moerr.NewInternalErrorNoCtxf("substrait: query root has %d output types for %d columns", len(c.types), width)
	}
	return c, nil
}

// Build binds admitted TaeRead messages to every scan and serializes the plan.
func (c *Candidate) Build(readValues map[int32][]byte) ([]byte, error) {
	if c == nil {
		return nil, moerr.NewInternalErrorNoCtxf("substrait: nil candidate")
	}
	e := exporter{query: c.query, readValues: readValues}
	relations := make([]*spb.PlanRel, 0, len(c.query.Steps))
	for step, rootID := range c.query.Steps {
		e.stepOrdinal = int32(step)
		relation, err := e.node(rootID)
		if err != nil {
			return nil, err
		}
		if step < len(c.query.Steps)-1 {
			relations = append(relations, &spb.PlanRel{RelType: &spb.PlanRel_Rel{Rel: relation}})
		} else {
			relations = append(relations, &spb.PlanRel{RelType: &spb.PlanRel_Root{Root: &spb.RelRoot{Input: relation, Names: append([]string(nil), c.headings...)}}})
		}
	}
	p := &spb.Plan{
		Version:          &spb.Version{MajorNumber: 0, MinorNumber: 78, PatchNumber: 0, Producer: "matrixone"},
		Relations:        relations,
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
	readSeen     map[int32]bool
	stepOrdinal  int32
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
	if n.NodeType != planpb.Node_SORT && len(n.OrderBy) != 0 {
		return nil, notEligiblef(EligibilityOperator, "node %d carries sort semantics outside a SORT node", id)
	}
	var rel *spb.Rel
	var err error
	switch n.NodeType {
	case planpb.Node_TABLE_SCAN:
		rel, err = e.read(n)
	case planpb.Node_FILTER:
		rel, err = e.unary(n)
		if err == nil {
			inputWidth, widthErr := e.nodeWidth(n.Children[0])
			if widthErr != nil {
				err = widthErr
			} else {
				rel, err = e.applyFilter(rel, n.FilterList, []int{inputWidth})
				if err == nil {
					rel, err = e.applyProject(rel, inputWidth, n.ProjectList, []int{inputWidth})
				}
			}
		}
	case planpb.Node_PROJECT:
		rel, err = e.unary(n)
		if err == nil {
			inputWidth, widthErr := e.nodeWidth(n.Children[0])
			if widthErr != nil {
				err = widthErr
			} else {
				rel, err = e.applyProject(rel, inputWidth, n.ProjectList, []int{inputWidth})
			}
		}
	case planpb.Node_AGG:
		rel, err = e.aggregate(n)
	case planpb.Node_SORT:
		rel, err = e.sort(n)
	case planpb.Node_JOIN:
		rel, err = e.join(n)
	case planpb.Node_SINK:
		rel, err = e.sink(n)
	case planpb.Node_SINK_SCAN:
		rel, err = e.sinkScan(n)
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
		if len(n.ProjectList) > 0 {
			return len(n.ProjectList), nil
		}
		return len(n.GroupBy) + len(n.AggList), nil
	case planpb.Node_JOIN:
		if len(n.Children) != 2 {
			return 0, notEligiblef(EligibilityOperator, "unsupported width for %s", n.NodeType.String())
		}
		if len(n.ProjectList) > 0 {
			return len(n.ProjectList), nil
		}
		left, err := e.nodeWidth(n.Children[0])
		if err != nil {
			return 0, err
		}
		if (n.JoinType == planpb.Node_SEMI || n.JoinType == planpb.Node_ANTI) && !n.IsRightJoin {
			return left, nil
		}
		right, err := e.nodeWidth(n.Children[1])
		if err != nil {
			return 0, err
		}
		if n.JoinType == planpb.Node_SEMI || n.JoinType == planpb.Node_ANTI {
			return right, nil
		}
		return left + right, nil
	case planpb.Node_FILTER, planpb.Node_SORT, planpb.Node_SINK:
		if len(n.Children) != 1 {
			return 0, moerr.NewInternalErrorNoCtxf("substrait: node %d requires one child", id)
		}
		if len(n.ProjectList) > 0 {
			return len(n.ProjectList), nil
		}
		return e.nodeWidth(n.Children[0])
	case planpb.Node_SINK_SCAN:
		if len(n.ProjectList) > 0 {
			return len(n.ProjectList), nil
		}
		if len(n.SourceStep) != 1 || n.SourceStep[0] < 0 || int(n.SourceStep[0]) >= len(e.query.Steps) {
			return 0, moerr.NewInternalErrorNoCtxf("substrait: sink scan %d has malformed source metadata", id)
		}
		return e.nodeWidth(e.query.Steps[n.SourceStep[0]])
	default:
		return 0, notEligiblef(EligibilityOperator, "unsupported width for %s", n.NodeType.String())
	}
}

func (e *exporter) outputTypes(id int32) ([]planpb.Type, error) {
	if id < 0 || int(id) >= len(e.query.Nodes) || e.query.Nodes[id] == nil {
		return nil, moerr.NewInternalErrorNoCtxf("substrait: invalid node id %d", id)
	}
	n := e.query.Nodes[id]
	if len(n.ProjectList) > 0 {
		result := make([]planpb.Type, len(n.ProjectList))
		for i, expression := range n.ProjectList {
			if expression != nil {
				result[i] = expression.Typ
			}
		}
		return result, nil
	}
	if n.NodeType == planpb.Node_TABLE_SCAN && n.TableDef != nil {
		result := make([]planpb.Type, 0, len(n.TableDef.Cols))
		for _, column := range n.TableDef.Cols {
			if column != nil && !column.Hidden {
				result = append(result, column.Typ)
			}
		}
		return result, nil
	}
	switch n.NodeType {
	case planpb.Node_FILTER, planpb.Node_SORT, planpb.Node_SINK:
		if len(n.Children) != 1 {
			return nil, moerr.NewInternalErrorNoCtxf("substrait: node %d requires one child", id)
		}
		return e.outputTypes(n.Children[0])
	case planpb.Node_AGG:
		result := make([]planpb.Type, 0, len(n.GroupBy)+len(n.AggList))
		for _, expression := range n.GroupBy {
			result = append(result, expression.Typ)
		}
		for _, expression := range n.AggList {
			outputType := expression.Typ
			if call := expression.GetF(); len(n.GroupBy) == 0 && call != nil && call.Func != nil {
				functionID, _ := function.DecodeOverloadID(call.Func.Obj)
				if aggregateCanReturnNullOnEmpty(functionID) {
					outputType.NotNullable = false
				}
			}
			result = append(result, outputType)
		}
		return result, nil
	case planpb.Node_JOIN:
		if len(n.Children) != 2 {
			return nil, moerr.NewInternalErrorNoCtxf("substrait: join node %d requires two children", id)
		}
		left, err := e.outputTypes(n.Children[0])
		if err != nil {
			return nil, err
		}
		if (n.JoinType == planpb.Node_SEMI || n.JoinType == planpb.Node_ANTI) && !n.IsRightJoin {
			return left, nil
		}
		right, err := e.outputTypes(n.Children[1])
		if err != nil {
			return nil, err
		}
		if n.JoinType == planpb.Node_SEMI || n.JoinType == planpb.Node_ANTI {
			return right, nil
		}
		switch n.JoinType {
		case planpb.Node_LEFT:
			for i := range right {
				right[i].NotNullable = false
			}
		case planpb.Node_RIGHT:
			for i := range left {
				left[i].NotNullable = false
			}
		}
		return append(left, right...), nil
	case planpb.Node_SINK_SCAN:
		if len(n.SourceStep) != 1 || n.SourceStep[0] < 0 || int(n.SourceStep[0]) >= len(e.query.Steps) {
			return nil, moerr.NewInternalErrorNoCtxf("substrait: sink scan %d has malformed source metadata", id)
		}
		return e.outputTypes(e.query.Steps[n.SourceStep[0]])
	default:
		return nil, notEligiblef(EligibilityOperator, "unsupported output types for %s", n.NodeType.String())
	}
}

func (e *exporter) unary(n *planpb.Node) (*spb.Rel, error) {
	if len(n.Children) != 1 {
		return nil, moerr.NewInternalErrorNoCtxf("substrait: %s node %d requires one child", n.NodeType.String(), n.NodeId)
	}
	return e.node(n.Children[0])
}

func (e *exporter) applyFilter(input *spb.Rel, predicates []*planpb.Expr, inputs []int) (*spb.Rel, error) {
	if len(predicates) == 0 {
		return input, nil
	}
	if err := validateExprFields(predicates, inputs); err != nil {
		return nil, err
	}
	condition, err := e.conjunction(predicates, inputs)
	if err != nil {
		return nil, err
	}
	return &spb.Rel{RelType: &spb.Rel_Filter{Filter: &spb.FilterRel{Input: input, Condition: condition}}}, nil
}

func (e *exporter) applyProject(input *spb.Rel, inputWidth int, expressions []*planpb.Expr, inputs []int) (*spb.Rel, error) {
	if len(expressions) == 0 {
		return input, nil
	}
	if err := validateExprFields(expressions, inputs); err != nil {
		return nil, err
	}
	projected := make([]*spb.Expression, len(expressions))
	mapping := make([]int32, len(expressions))
	for i := range expressions {
		var err error
		projected[i], err = e.expr(expressions[i], inputs)
		if err != nil {
			return nil, err
		}
		mapping[i] = int32(inputWidth + i)
	}
	return &spb.Rel{RelType: &spb.Rel_Project{Project: &spb.ProjectRel{
		Common:      emit(mapping),
		Input:       input,
		Expressions: projected,
	}}}, nil
}

func emit(mapping []int32) *spb.RelCommon {
	return &spb.RelCommon{EmitKind: &spb.RelCommon_Emit_{Emit: &spb.RelCommon_Emit{OutputMapping: mapping}}}
}

func (e *exporter) sink(n *planpb.Node) (*spb.Rel, error) {
	input, err := e.unary(n)
	if err != nil {
		return nil, err
	}
	inputWidth, err := e.nodeWidth(n.Children[0])
	if err != nil {
		return nil, err
	}
	return e.applyProject(input, inputWidth, n.ProjectList, []int{inputWidth})
}

func (e *exporter) sinkScan(n *planpb.Node) (*spb.Rel, error) {
	if len(n.Children) != 0 || len(n.SourceStep) != 1 {
		return nil, moerr.NewInternalErrorNoCtxf("substrait: sink scan %d has malformed source metadata", n.NodeId)
	}
	sourceStep := n.SourceStep[0]
	if sourceStep < 0 || sourceStep >= e.stepOrdinal || int(sourceStep) >= len(e.query.Steps) {
		return nil, moerr.NewInternalErrorNoCtxf("substrait: sink scan %d must reference an earlier producer step", n.NodeId)
	}
	producerID := e.query.Steps[sourceStep]
	producer := e.query.Nodes[producerID]
	if producer == nil || producer.NodeType != planpb.Node_SINK {
		return nil, moerr.NewInternalErrorNoCtxf("substrait: sink scan %d references a non-sink step", n.NodeId)
	}
	producerWidth, err := e.nodeWidth(producerID)
	if err != nil {
		return nil, err
	}
	reference := &spb.Rel{RelType: &spb.Rel_Reference{Reference: &spb.ReferenceRel{SubtreeOrdinal: sourceStep}}}
	return e.applyProject(reference, producerWidth, n.ProjectList, []int{producerWidth})
}

func (e *exporter) join(n *planpb.Node) (*spb.Rel, error) {
	if len(n.Children) != 2 {
		return nil, notEligiblef(EligibilityOperator, "join node %d requires two inputs", n.NodeId)
	}
	left, err := e.node(n.Children[0])
	if err != nil {
		return nil, err
	}
	right, err := e.node(n.Children[1])
	if err != nil {
		return nil, err
	}
	leftWidth, err := e.nodeWidth(n.Children[0])
	if err != nil {
		return nil, err
	}
	rightWidth, err := e.nodeWidth(n.Children[1])
	if err != nil {
		return nil, err
	}
	inputs := []int{leftWidth, rightWidth}
	var condition *spb.Expression
	if len(n.OnList) == 0 {
		condition, err = literal(&planpb.Literal{Value: &planpb.Literal_Bval{Bval: true}}, &planpb.Type{Id: int32(types.T_bool), NotNullable: true})
	} else {
		if err = validateExprFields(n.OnList, inputs); err != nil {
			return nil, err
		}
		condition, err = e.conjunction(n.OnList, inputs)
	}
	if err != nil {
		return nil, err
	}
	var joinType spb.JoinRel_JoinType
	joinWidth := leftWidth + rightWidth
	projectInputs := inputs
	switch n.JoinType {
	case planpb.Node_INNER:
		joinType = spb.JoinRel_JOIN_TYPE_INNER
	case planpb.Node_LEFT:
		joinType = spb.JoinRel_JOIN_TYPE_LEFT
	case planpb.Node_RIGHT:
		joinType = spb.JoinRel_JOIN_TYPE_RIGHT
	case planpb.Node_SEMI:
		if n.IsRightJoin {
			joinType = spb.JoinRel_JOIN_TYPE_RIGHT_SEMI
			joinWidth = rightWidth
			projectInputs = []int{0, rightWidth}
		} else {
			joinType = spb.JoinRel_JOIN_TYPE_LEFT_SEMI
			joinWidth = leftWidth
			projectInputs = []int{leftWidth, 0}
		}
	case planpb.Node_ANTI:
		if n.IsRightJoin {
			joinType = spb.JoinRel_JOIN_TYPE_RIGHT_ANTI
			joinWidth = rightWidth
			projectInputs = []int{0, rightWidth}
		} else {
			joinType = spb.JoinRel_JOIN_TYPE_LEFT_ANTI
			joinWidth = leftWidth
			projectInputs = []int{leftWidth, 0}
		}
	default:
		return nil, notEligiblef(EligibilityOperator, "node %d uses unsupported join type %s", n.NodeId, n.JoinType.String())
	}
	relation := &spb.Rel{RelType: &spb.Rel_Join{Join: &spb.JoinRel{Left: left, Right: right, Expression: condition, Type: joinType}}}
	relation, err = e.applyFilter(relation, n.FilterList, projectInputs)
	if err != nil {
		return nil, err
	}
	return e.applyProject(relation, joinWidth, n.ProjectList, projectInputs)
}

func (e *exporter) read(n *planpb.Node) (*spb.Rel, error) {
	if len(n.Children) != 0 || n.TableDef == nil || n.ObjRef == nil {
		return nil, moerr.NewInternalErrorNoCtxf("substrait: node %d has malformed table scan structure", n.NodeId)
	}
	if n.TableDef.DbId == 0 || n.TableDef.TblId == 0 || uint64(n.ObjRef.Obj) != n.TableDef.TblId {
		return nil, moerr.NewInternalErrorNoCtxf("substrait: node %d has malformed table identity db=%d object=%d table=%d", n.NodeId, n.TableDef.DbId, n.ObjRef.Obj, n.TableDef.TblId)
	}
	if n.TableDef.IsTemporary || n.ScanSnapshot != nil || n.ObjRef.Snapshot != nil || n.ObjRef.PubInfo != nil || (n.TableDef.TableType != "" && n.TableDef.TableType != "r") {
		return nil, notEligiblef(EligibilityPlanShape, "node %d is not a persistent TAE table scan", n.NodeId)
	}
	if n.IndexScanInfo.ProtoSize() != 0 || n.IndexReaderParam != nil {
		return nil, notEligiblef(EligibilityOperator, "node %d carries unsupported index scan semantics", n.NodeId)
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
	if e.validateOnly {
		if e.readSeen == nil {
			e.readSeen = make(map[int32]bool)
		}
		if !e.readSeen[n.NodeId] {
			e.readSeen[n.NodeId] = true
			e.reads = append(e.reads, Read{
				NodeID:        n.NodeId,
				DatabaseID:    n.TableDef.DbId,
				TableID:       n.TableDef.TblId,
				SchemaVersion: n.TableDef.Version,
				Columns:       columns,
				Schema:        schemaBytes,
			})
		}
	}
	value := e.readValues[n.NodeId]
	if !e.validateOnly && len(value) == 0 {
		return nil, moerr.NewInternalErrorNoCtxf("substrait: node %d has no admitted TaeRead", n.NodeId)
	}
	rel := &spb.Rel{RelType: &spb.Rel_Read{Read: &spb.ReadRel{
		BaseSchema: schema,
		ReadType:   &spb.ReadRel_ExtensionTable_{ExtensionTable: &spb.ReadRel_ExtensionTable{Detail: &anypb.Any{TypeUrl: TaeReadTypeURL, Value: value}}},
	}}}
	width := len(schema.Struct.Types)
	rel, err = e.applyFilter(rel, n.FilterList, []int{width})
	if err != nil {
		return nil, err
	}
	return e.applyProject(rel, width, n.ProjectList, []int{width})
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
	inputs := []int{inputWidth}
	if err = validateExprFields(n.GroupBy, inputs); err != nil {
		return nil, err
	}
	groups := make([]*spb.Expression, len(n.GroupBy))
	refs := make([]uint32, len(groups))
	for i := range n.GroupBy {
		groups[i], err = e.expr(n.GroupBy[i], inputs)
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
		if err := validateExprFields(f.Args, inputs); err != nil {
			return nil, err
		}
		args := make([]*spb.FunctionArgument, len(f.Args))
		for j := range f.Args {
			a, xerr := e.expr(f.Args[j], inputs)
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
	relation := &spb.Rel{RelType: &spb.Rel_Aggregate{Aggregate: &spb.AggregateRel{Input: input, GroupingExpressions: groups, Groupings: []*spb.AggregateRel_Grouping{{ExpressionReferences: refs}}, Measures: measures}}}
	aggregateWidth := len(groups) + len(measures)
	relation, err = e.applyFilter(relation, n.FilterList, []int{aggregateWidth})
	if err != nil {
		return nil, err
	}
	return e.applyProject(relation, aggregateWidth, n.ProjectList, []int{aggregateWidth})
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
		if order == nil || order.Expr == nil {
			return nil, moerr.NewInternalErrorNoCtxf("substrait: malformed sort at %d", i)
		}
		switch types.T(order.Expr.Typ.Id) {
		case types.T_float32, types.T_float64:
			return nil, notEligiblef(EligibilityOperator, "floating-point sort at %d has no Sirius ordering equivalence", i)
		}
		if order.Collation != "" || int32(order.Flag)&int32(planpb.OrderBySpec_UNIQUE) != 0 {
			return nil, notEligiblef(EligibilityOperator, "unsupported sort at %d", i)
		}
		if err := validateExprFields([]*planpb.Expr{order.Expr}, []int{inputWidth}); err != nil {
			return nil, err
		}
		x, xerr := e.expr(order.Expr, []int{inputWidth})
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
	relation := &spb.Rel{RelType: &spb.Rel_Sort{Sort: &spb.SortRel{Input: input, Sorts: sorts}}}
	relation, err = e.applyFilter(relation, n.FilterList, []int{inputWidth})
	if err != nil {
		return nil, err
	}
	return e.applyProject(relation, inputWidth, n.ProjectList, []int{inputWidth})
}

func (e *exporter) fetch(input *spb.Rel, n *planpb.Node) (*spb.Rel, error) {
	if n.Limit == nil && n.Offset == nil {
		return input, nil
	}
	fetch := &spb.FetchRel{Input: input}
	if n.Limit != nil {
		count, err := nonnegativeIntLiteral(n.Limit, 0)
		if err != nil {
			if IsNotEligible(err) {
				return nil, err
			}
			return nil, moerr.NewInternalErrorNoCtxf("substrait: limit: %v", err)
		}
		fetch.CountMode = &spb.FetchRel_Count{Count: count}
	}
	if n.Offset != nil {
		offset, err := nonnegativeIntLiteral(n.Offset, 0)
		if err != nil {
			if IsNotEligible(err) {
				return nil, err
			}
			return nil, moerr.NewInternalErrorNoCtxf("substrait: offset: %v", err)
		}
		fetch.OffsetMode = &spb.FetchRel_Offset{Offset: offset}
	}
	return &spb.Rel{RelType: &spb.Rel_Fetch{Fetch: fetch}}, nil
}

func (e *exporter) conjunction(xs []*planpb.Expr, inputs []int) (*spb.Expression, error) {
	if len(xs) == 0 {
		return nil, moerr.NewInternalErrorNoCtxf("substrait: empty filter")
	}
	for _, predicate := range xs {
		if predicate == nil || types.T(predicate.Typ.Id) != types.T_bool {
			return nil, moerr.NewInternalErrorNoCtxf("substrait: filter predicate is not boolean")
		}
	}
	x, err := e.expr(xs[0], inputs)
	if err != nil {
		return nil, err
	}
	for i := 1; i < len(xs); i++ {
		right, xerr := e.expr(xs[i], inputs)
		if xerr != nil {
			return nil, xerr
		}
		x = e.scalar("and", &planpb.Type{Id: int32(types.T_bool)}, x, right)
	}
	return x, nil
}

func (e *exporter) expr(x *planpb.Expr, inputs []int) (*spb.Expression, error) {
	if x == nil {
		return nil, moerr.NewInternalErrorNoCtxf("substrait: nil expression")
	}
	switch v := x.Expr.(type) {
	case *planpb.Expr_Col:
		ordinal, err := fieldOrdinal(v.Col, inputs)
		if err != nil {
			return nil, err
		}
		return field(ordinal), nil
	case *planpb.Expr_Lit:
		return literal(v.Lit, &x.Typ)
	case *planpb.Expr_List:
		return nil, notEligiblef(EligibilityExpression, "an expression list is only valid as an IN argument")
	case *planpb.Expr_F:
		if v.F == nil || v.F.Func == nil {
			return nil, moerr.NewInternalErrorNoCtxf("substrait: malformed function")
		}
		functionID, _ := function.DecodeOverloadID(v.F.Func.Obj)
		switch functionID {
		case function.CAST:
			return e.castExpr(x, v.F, inputs)
		case function.CASE:
			return e.caseExpr(x, v.F, inputs)
		case function.IN:
			return e.inExpr(x, v.F, inputs)
		case function.EXTRACT:
			return e.extractExpr(x, v.F, inputs)
		}
		if functionID == function.UNARY_MINUS && len(v.F.Args) == 1 {
			return e.unaryMinusExpr(x, v.F, inputs)
		}
		name, ok := scalarName(v.F.Func.ObjName)
		if !ok {
			return nil, notEligiblef(EligibilityExpression, "unsupported scalar function %q", v.F.Func.ObjName)
		}
		if want := scalarArity(name); want >= 0 && len(v.F.Args) != want {
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
			args[i], err = e.expr(v.F.Args[i], inputs)
			if err != nil {
				return nil, err
			}
		}
		return e.scalar(name, &x.Typ, args...), nil
	default:
		return nil, notEligiblef(EligibilityExpression, "unsupported expression %T", x.Expr)
	}
}

func (e *exporter) unaryMinusExpr(result *planpb.Expr, call *planpb.Function, inputs []int) (*spb.Expression, error) {
	if len(call.Args) != 1 || call.Args[0] == nil || !isDecimalType(types.T(call.Args[0].Typ.Id)) || semanticTypeFromPlan(&call.Args[0].Typ) != semanticTypeFromPlan(&result.Typ) {
		return nil, notEligiblef(EligibilityExpression, "unsupported unary minus signature")
	}
	supported, err := hasSemanticCapability(semanticScalar, "unary_minus", call.Func, call.Args, &result.Typ)
	if err != nil {
		return nil, err
	}
	if !supported {
		return nil, notEligiblef(EligibilityExpression, "unary minus overload has no declared Sirius semantic equivalence")
	}
	value, err := e.expr(call.Args[0], inputs)
	if err != nil {
		return nil, err
	}
	zero := &planpb.Literal{}
	if types.T(result.Typ.Id) == types.T_decimal64 {
		zero.Value = &planpb.Literal_Decimal64Val{Decimal64Val: &planpb.Decimal64{}}
	} else {
		zero.Value = &planpb.Literal_Decimal128Val{Decimal128Val: &planpb.Decimal128{}}
	}
	zeroExpr, err := literal(zero, &result.Typ)
	if err != nil {
		return nil, err
	}
	return e.scalar("subtract", &result.Typ, zeroExpr, value), nil
}

func fieldOrdinal(column *planpb.ColRef, inputs []int) (int32, error) {
	if column == nil || column.ColPos < 0 || len(inputs) == 0 || len(inputs) > 2 {
		return 0, moerr.NewInternalErrorNoCtx("substrait: invalid column reference")
	}
	relation := column.RelPos
	if len(inputs) == 1 && (relation == 0 || relation == -1 || relation == -2) {
		if int(column.ColPos) >= inputs[0] {
			return 0, moerr.NewInternalErrorNoCtxf("substrait: column ordinal is outside input width %d", inputs[0])
		}
		return column.ColPos, nil
	}
	if len(inputs) == 2 && (relation == 0 || relation == 1) {
		if int(column.ColPos) >= inputs[relation] {
			return 0, moerr.NewInternalErrorNoCtxf("substrait: column ordinal is outside join input %d width %d", relation, inputs[relation])
		}
		if relation == 1 {
			return int32(inputs[0]) + column.ColPos, nil
		}
		return column.ColPos, nil
	}
	return 0, moerr.NewInternalErrorNoCtxf("substrait: relation ordinal %d is invalid for %d inputs", relation, len(inputs))
}

func (e *exporter) castExpr(result *planpb.Expr, call *planpb.Function, inputs []int) (*spb.Expression, error) {
	if len(call.Args) != 2 {
		return nil, notEligiblef(EligibilityExpression, "cast requires a value and target type descriptor")
	}
	supported, err := hasSemanticCapability(semanticScalar, "cast", call.Func, call.Args, &result.Typ)
	if err != nil {
		return nil, err
	}
	if !supported {
		return nil, notEligiblef(EligibilityExpression, "cast overload %q has no declared Sirius semantic equivalence", call.Func.ObjName)
	}
	input, err := e.expr(call.Args[0], inputs)
	if err != nil {
		return nil, err
	}
	target, err := substraitType(&result.Typ)
	if err != nil {
		return nil, err
	}
	return &spb.Expression{RexType: &spb.Expression_Cast_{Cast: &spb.Expression_Cast{
		Type: target, Input: input, FailureBehavior: spb.Expression_Cast_FAILURE_BEHAVIOR_THROW_EXCEPTION,
	}}}, nil
}

func (e *exporter) caseExpr(result *planpb.Expr, call *planpb.Function, inputs []int) (*spb.Expression, error) {
	if len(call.Args) < 3 || len(call.Args)%2 == 0 {
		return nil, notEligiblef(EligibilityExpression, "case requires condition/result pairs and an else expression")
	}
	supported, err := hasSemanticCapability(semanticScalar, "if_then", call.Func, call.Args, &result.Typ)
	if err != nil {
		return nil, err
	}
	if !supported {
		return nil, notEligiblef(EligibilityExpression, "case overload has no declared Sirius semantic equivalence")
	}
	ifThen := &spb.Expression_IfThen{Ifs: make([]*spb.Expression_IfThen_IfClause, 0, len(call.Args)/2)}
	for i := 0; i+1 < len(call.Args)-1; i += 2 {
		if types.T(call.Args[i].Typ.Id) != types.T_bool {
			return nil, notEligiblef(EligibilityExpression, "case condition is not boolean")
		}
		condition, xerr := e.expr(call.Args[i], inputs)
		if xerr != nil {
			return nil, xerr
		}
		value, xerr := e.expr(call.Args[i+1], inputs)
		if xerr != nil {
			return nil, xerr
		}
		ifThen.Ifs = append(ifThen.Ifs, &spb.Expression_IfThen_IfClause{If: condition, Then: value})
	}
	ifThen.Else, err = e.expr(call.Args[len(call.Args)-1], inputs)
	if err != nil {
		return nil, err
	}
	return &spb.Expression{RexType: &spb.Expression_IfThen_{IfThen: ifThen}}, nil
}

func (e *exporter) inExpr(result *planpb.Expr, call *planpb.Function, inputs []int) (*spb.Expression, error) {
	if len(call.Args) < 2 {
		return nil, notEligiblef(EligibilityExpression, "in requires a value and at least one option")
	}
	supported, err := hasSemanticCapability(semanticScalar, "singular_or_list", call.Func, call.Args, &result.Typ)
	if err != nil {
		return nil, err
	}
	if !supported {
		return nil, notEligiblef(EligibilityExpression, "in overload has no declared Sirius semantic equivalence")
	}
	value, err := e.expr(call.Args[0], inputs)
	if err != nil {
		return nil, err
	}
	options := make([]*spb.Expression, 0, len(call.Args)-1)
	for _, argument := range call.Args[1:] {
		if list := argument.GetList(); list != nil {
			for _, item := range list.List {
				option, xerr := e.expr(item, inputs)
				if xerr != nil {
					return nil, xerr
				}
				options = append(options, option)
			}
			continue
		}
		if argument.GetVec() != nil {
			items, xerr := literalVectorOptions(argument.GetVec(), &call.Args[0].Typ)
			if xerr != nil {
				return nil, xerr
			}
			for _, item := range items {
				option, itemErr := e.expr(item, inputs)
				if itemErr != nil {
					return nil, itemErr
				}
				options = append(options, option)
			}
			continue
		}
		option, xerr := e.expr(argument, inputs)
		if xerr != nil {
			return nil, xerr
		}
		options = append(options, option)
	}
	if len(options) == 0 {
		return nil, moerr.NewInternalErrorNoCtx("substrait: IN option list is empty")
	}
	return &spb.Expression{RexType: &spb.Expression_SingularOrList_{SingularOrList: &spb.Expression_SingularOrList{Value: value, Options: options}}}, nil
}

func literalVectorOptions(encoded *planpb.LiteralVec, expected *planpb.Type) (options []*planpb.Expr, err error) {
	if encoded == nil || expected == nil || encoded.Len <= 0 || encoded.Len > maxLiteralVectorValues || len(encoded.Data) == 0 || len(encoded.Data) > MaxPlanBytes {
		return nil, notEligiblef(EligibilityExpression, "folded IN list is empty or exceeds the supported bound")
	}
	// Keep malformed optimizer input total even if a future vector accessor
	// grows an invariant stronger than UnmarshalBinary's wire validation.
	defer func() {
		if recovered := recover(); recovered != nil {
			options = nil
			err = moerr.NewInternalErrorNoCtxf("substrait: malformed folded IN list: %v", recovered)
		}
	}()
	var values vector.Vector
	if unmarshalErr := values.UnmarshalBinary(encoded.Data); unmarshalErr != nil {
		return nil, moerr.NewInternalErrorNoCtxf("substrait: malformed folded IN list: %v", unmarshalErr)
	}
	defer values.Free(nil)
	if values.Length() != int(encoded.Len) || values.GetType() == nil || int32(values.GetType().Oid) != expected.Id || values.GetType().Width != expected.Width || values.GetType().Scale != expected.Scale {
		return nil, moerr.NewInternalErrorNoCtx("substrait: folded IN list type or length mismatch")
	}
	physicalLength := values.Length()
	if values.IsConst() && physicalLength > 0 {
		physicalLength = 1
	}
	options = make([]*planpb.Expr, physicalLength)
	for i := 0; i < physicalLength; i++ {
		value := rule.GetConstantValue(&values, true, uint64(i))
		if value == nil {
			return nil, notEligiblef(EligibilityExpression, "folded IN list uses unsupported type %s", values.GetType().Oid.String())
		}
		value.IsSerialized = encoded.IsSerialized
		typ := *expected
		typ.NotNullable = !value.Isnull
		options[i] = &planpb.Expr{Typ: typ, Expr: &planpb.Expr_Lit{Lit: value}}
	}
	return options, nil
}

func (e *exporter) extractExpr(result *planpb.Expr, call *planpb.Function, inputs []int) (*spb.Expression, error) {
	if len(call.Args) != 2 || call.Args[0].GetLit() == nil {
		return nil, notEligiblef(EligibilityExpression, "extract requires a literal field and one value")
	}
	unit := strings.ToLower(call.Args[0].GetLit().GetSval())
	if unit == "" {
		return nil, notEligiblef(EligibilityExpression, "extract field is empty")
	}
	supported, err := hasSemanticCapability(semanticScalar, "extract", call.Func, call.Args, &result.Typ)
	if err != nil {
		return nil, err
	}
	if !supported {
		return nil, notEligiblef(EligibilityExpression, "extract overload has no declared Sirius semantic equivalence")
	}
	value, err := e.expr(call.Args[1], inputs)
	if err != nil {
		return nil, err
	}
	output, err := substraitType(&result.Typ)
	if err != nil {
		return nil, err
	}
	return &spb.Expression{RexType: &spb.Expression_ScalarFunction_{ScalarFunction: &spb.Expression_ScalarFunction{
		FunctionReference: e.function("extract"),
		Arguments: []*spb.FunctionArgument{
			{ArgType: &spb.FunctionArgument_Enum{Enum: unit}}, valueArg(value),
		},
		OutputType: output,
	}}}, nil
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

func validateExprFields(exprs []*planpb.Expr, inputs []int) error {
	for _, expr := range exprs {
		if expr == nil {
			return moerr.NewInternalErrorNoCtxf("substrait: nil expression")
		}
		switch value := expr.Expr.(type) {
		case *planpb.Expr_Col:
			if _, err := fieldOrdinal(value.Col, inputs); err != nil {
				return err
			}
		case *planpb.Expr_F:
			if value.F == nil || value.F.Func == nil {
				return moerr.NewInternalErrorNoCtxf("substrait: malformed function")
			}
			args := value.F.Args
			functionID, _ := function.DecodeOverloadID(value.F.Func.Obj)
			if functionID == function.CAST {
				if len(args) != 2 || args[1] == nil || args[1].GetT() == nil {
					return moerr.NewInternalErrorNoCtx("substrait: malformed cast target")
				}
				args = args[:1]
			}
			if functionID == function.IN {
				if len(args) < 2 {
					return moerr.NewInternalErrorNoCtx("substrait: malformed IN expression")
				}
				if err := validateExprFields(args[:1], inputs); err != nil {
					return err
				}
				for _, argument := range args[1:] {
					if argument == nil {
						return moerr.NewInternalErrorNoCtx("substrait: nil IN option")
					}
					if argument.GetVec() != nil {
						continue
					}
					if err := validateExprFields([]*planpb.Expr{argument}, inputs); err != nil {
						return err
					}
				}
				continue
			}
			if err := validateExprFields(args, inputs); err != nil {
				return err
			}
		case *planpb.Expr_List:
			if value.List == nil || len(value.List.List) == 0 {
				return moerr.NewInternalErrorNoCtx("substrait: empty expression list")
			}
			if err := validateExprFields(value.List.List, inputs); err != nil {
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
	case types.T_uint32:
		// Substrait has no unsigned integer primitive. Sirius/DuckDB EXTRACT
		// returns a signed i64; the Flight decoder restores MO's uint32 result.
		return &spb.Type{Kind: &spb.Type_I64_{I64: &spb.Type_I64{Nullability: n}}}, nil
	case types.T_float32:
		return &spb.Type{Kind: &spb.Type_Fp32{Fp32: &spb.Type_FP32{Nullability: n}}}, nil
	case types.T_float64:
		return &spb.Type{Kind: &spb.Type_Fp64{Fp64: &spb.Type_FP64{Nullability: n}}}, nil
	case types.T_char:
		return nil, notEligiblef(EligibilityType, "unsupported type %s", types.T(t.Id).String())
	case types.T_varchar:
		if t.Width < 0 {
			return nil, notEligiblef(EligibilityType, "negative varchar width %d", t.Width)
		}
		return &spb.Type{Kind: &spb.Type_Varchar{Varchar: &spb.Type_VarChar{Length: t.Width, Nullability: n}}}, nil
	case types.T_decimal64, types.T_decimal128:
		if t.Width <= 0 || t.Width > 38 || t.Scale < 0 || t.Scale > t.Width {
			return nil, notEligiblef(EligibilityType, "decimal(%d,%d) is outside the supported bound", t.Width, t.Scale)
		}
		return &spb.Type{Kind: &spb.Type_Decimal_{Decimal: &spb.Type_Decimal{Precision: t.Width, Scale: t.Scale, Nullability: n}}}, nil
	case types.T_date:
		// TAE decoding in Sirius subtracts MO_UNIX_EPOCH_DAYS, so both scan
		// values and literals reach DuckDB in Substrait's Unix-day domain.
		return &spb.Type{Kind: &spb.Type_Date_{Date: &spb.Type_Date{Nullability: n}}}, nil
	case types.T_timestamp:
		return nil, notEligiblef(EligibilityType, "unsupported temporal type %s", types.T(t.Id).String())
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
	case *planpb.Literal_U32Val:
		if oid != types.T_uint32 {
			return mismatch()
		}
		return wrap(&spb.Expression_Literal{LiteralType: &spb.Expression_Literal_I64{I64: int64(v.U32Val)}}), nil
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
		if oid != types.T_varchar {
			return mismatch()
		}
		return wrap(&spb.Expression_Literal{LiteralType: &spb.Expression_Literal_VarChar_{VarChar: &spb.Expression_Literal_VarChar{Value: v.Sval, Length: uint32(typ.Width)}}}), nil
	case *planpb.Literal_Dateval:
		if oid != types.T_date {
			return mismatch()
		}
		return wrap(&spb.Expression_Literal{LiteralType: &spb.Expression_Literal_Date{Date: types.Date(v.Dateval).DaysSinceUnixEpoch()}}), nil
	case *planpb.Literal_Decimal64Val:
		if oid != types.T_decimal64 || v.Decimal64Val == nil {
			return mismatch()
		}
		value := make([]byte, 16)
		binary.LittleEndian.PutUint64(value, uint64(v.Decimal64Val.A))
		if v.Decimal64Val.A < 0 {
			binary.LittleEndian.PutUint64(value[8:], math.MaxUint64)
		}
		return wrap(&spb.Expression_Literal{LiteralType: &spb.Expression_Literal_Decimal_{Decimal: &spb.Expression_Literal_Decimal{Value: value, Precision: typ.Width, Scale: typ.Scale}}}), nil
	case *planpb.Literal_Decimal128Val:
		if oid != types.T_decimal128 || v.Decimal128Val == nil {
			return mismatch()
		}
		value := make([]byte, 16)
		binary.LittleEndian.PutUint64(value, uint64(v.Decimal128Val.A))
		binary.LittleEndian.PutUint64(value[8:], uint64(v.Decimal128Val.B))
		return wrap(&spb.Expression_Literal{LiteralType: &spb.Expression_Literal_Decimal_{Decimal: &spb.Expression_Literal_Decimal{Value: value, Precision: typ.Width, Scale: typ.Scale}}}), nil
	default:
		return nil, moerr.NewInternalErrorNoCtxf("substrait: unsupported literal %T", l.Value)
	}
}

func valueArg(x *spb.Expression) *spb.FunctionArgument {
	return &spb.FunctionArgument{ArgType: &spb.FunctionArgument_Value{Value: x}}
}

func scalarName(name string) (string, bool) {
	n := strings.ToLower(name)
	m := map[string]string{"and": "and", "or": "or", "not": "not", "=": "equal", "equal": "equal", "!=": "not_equal", "<>": "not_equal", "<": "lt", "<=": "lte", ">": "gt", ">=": "gte", "is_null": "is_null", "isnull": "is_null", "is_not_null": "is_not_null", "isnotnull": "is_not_null", "<=>": "is_not_distinct_from", "+": "add", "-": "subtract", "*": "multiply", "/": "divide", "%": "modulus", "mod": "modulus", "between": "between", "like": "like", "prefix_eq": "starts_with", "substring": "substring"}
	v, ok := m[n]
	return v, ok
}

func scalarArity(name string) int {
	switch name {
	case "not", "is_null", "is_not_null":
		return 1
	case "between":
		return 3
	case "substring":
		return 3
	case "and", "or":
		return -1
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
	case "avg":
		return "avg", true
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
	case function.MIN, function.MAX, function.SUM, function.AVG:
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
	if ref == nil || out == nil {
		return false, nil
	}
	if len(args) > 3 {
		return hasTPCHSemanticCapability(kind, name, ref, args, out)
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
	if ok && capability.name == name && capability.equivalence != "" {
		return true, nil
	}
	return hasTPCHSemanticCapability(kind, name, ref, args, out)
}

// hasTPCHSemanticCapability covers the parameterized decimal/string/date
// overload families used by the canonical TPC-H plans. It remains exact: the
// bound encoded overload, resolved argument types, result width/scale, and
// result nullability must all agree before a named semantic family is enough.
func hasTPCHSemanticCapability(kind semanticCapabilityKind, name string, ref *planpb.ObjectRef, args []*planpb.Expr, out *planpb.Type) (bool, error) {
	if ref == nil || out == nil || len(args) == 0 {
		return false, nil
	}
	functionID, _ := function.DecodeOverloadID(ref.Obj)
	declared := false
	switch kind {
	case semanticScalar:
		switch name {
		case "and", "or":
			declared = functionID == map[string]int32{"and": function.AND, "or": function.OR}[name] && booleanArgs(args, 2)
		case "equal", "not_equal", "lt", "lte", "gt", "gte":
			declared = functionID == map[string]int32{"equal": function.EQUAL, "not_equal": function.NOT_EQUAL, "lt": function.LESS_THAN, "lte": function.LESS_EQUAL, "gt": function.GREAT_THAN, "gte": function.GREAT_EQUAL}[name] && comparableTPCHArgs(args)
		case "add", "subtract", "multiply", "divide":
			declared = functionID == map[string]int32{"add": function.PLUS, "subtract": function.MINUS, "multiply": function.MULTI, "divide": function.DIV}[name] && decimalArgs(args)
		case "unary_minus":
			declared = functionID == function.UNARY_MINUS && len(args) == 1 && args[0] != nil && isDecimalType(types.T(args[0].Typ.Id))
		case "between":
			declared = functionID == function.BETWEEN && len(args) == 3 && comparableTPCHArgs(args)
		case "like":
			declared = functionID == function.LIKE && varcharArgs(args, 2)
		case "starts_with":
			declared = functionID == function.PREFIX_EQ && varcharArgs(args, 2)
		case "substring":
			declared = functionID == function.SUBSTRING && len(args) == 3 && types.T(args[0].Typ.Id) == types.T_varchar && types.T(args[1].Typ.Id) == types.T_int64 && types.T(args[2].Typ.Id) == types.T_int64
		case "cast":
			declared = functionID == function.CAST && len(args) == 2 && tpchCastType(types.T(args[0].Typ.Id)) && tpchCastType(types.T(out.Id))
		case "if_then":
			declared = functionID == function.CASE && len(args) >= 3 && len(args)%2 == 1 && tpchCaseArgs(args, out)
		case "singular_or_list":
			declared = functionID == function.IN && len(args) >= 2 && (types.T(args[0].Typ.Id) == types.T_int32 || types.T(args[0].Typ.Id) == types.T_varchar)
		case "extract":
			declared = functionID == function.EXTRACT && len(args) == 2 && types.T(args[0].Typ.Id) == types.T_varchar && types.T(args[1].Typ.Id) == types.T_date && types.T(out.Id) == types.T_uint32
		}
	case semanticAggregate:
		switch name {
		case "count":
			declared = (functionID == function.COUNT || functionID == function.STARCOUNT) && len(args) == 1 && (types.T(args[0].Typ.Id) == types.T_int32 || types.T(args[0].Typ.Id) == types.T_int64)
		case "sum":
			declared = functionID == function.SUM && len(args) == 1 && (types.T(args[0].Typ.Id) == types.T_int64 || isDecimalType(types.T(args[0].Typ.Id)))
		case "avg":
			declared = functionID == function.AVG && len(args) == 1 && isDecimalType(types.T(args[0].Typ.Id))
		case "min":
			declared = functionID == function.MIN && len(args) == 1 && isDecimalType(types.T(args[0].Typ.Id))
		case "max":
			declared = functionID == function.MAX && len(args) == 1 && isDecimalType(types.T(args[0].Typ.Id))
		}
	}
	if !declared {
		return false, nil
	}
	if kind == semanticScalar && name == "singular_or_list" {
		// IN is bound against an MO tuple/list pseudo-type, which may later be
		// constant-folded into LiteralVec. Re-resolving that pseudo-signature
		// cannot recover the member types, so validate the already-bound exact
		// overload plus its boolean/nullability contract here; inExpr validates
		// every list or vector member against the left-hand value type.
		if _, exists := function.GetFunctionByIdWithoutError(ref.Obj); !exists || types.T(out.Id) != types.T_bool {
			return false, nil
		}
		return function.DeduceNotNullable(ref.Obj, args) == out.NotNullable, nil
	}
	inputs := make([]types.Type, len(args))
	for i, argument := range args {
		if argument == nil {
			return false, nil
		}
		inputs[i] = types.Type{Oid: types.T(argument.Typ.Id), Width: argument.Typ.Width, Scale: argument.Typ.Scale}
	}
	resolved, err := function.GetFunctionByName(context.Background(), ref.ObjName, inputs)
	if err != nil {
		return false, nil
	}
	if resolved.GetEncodedOverloadID() != ref.Obj {
		return false, nil
	}
	result := resolved.GetReturnType()
	if int32(result.Oid) != out.Id || result.Width != out.Width || result.Scale != out.Scale {
		return false, nil
	}
	notNullable := function.DeduceNotNullable(resolved.GetEncodedOverloadID(), args)
	if kind == semanticAggregate && aggregateCanReturnNullOnEmpty(functionID) && !out.NotNullable {
		return true, nil
	}
	return notNullable == out.NotNullable, nil
}

func isDecimalType(value types.T) bool {
	return value == types.T_decimal64 || value == types.T_decimal128
}

func decimalArgs(args []*planpb.Expr) bool {
	return len(args) == 2 && args[0] != nil && args[1] != nil && isDecimalType(types.T(args[0].Typ.Id)) && isDecimalType(types.T(args[1].Typ.Id))
}

func booleanArgs(args []*planpb.Expr, minimum int) bool {
	if len(args) < minimum {
		return false
	}
	for _, argument := range args {
		if argument == nil || types.T(argument.Typ.Id) != types.T_bool {
			return false
		}
	}
	return true
}

func comparableTPCHArgs(args []*planpb.Expr) bool {
	if len(args) < 2 || args[0] == nil {
		return false
	}
	family := types.T(args[0].Typ.Id)
	if family != types.T_date && family != types.T_int32 && family != types.T_int64 && family != types.T_varchar && !isDecimalType(family) {
		return false
	}
	for _, argument := range args[1:] {
		if argument == nil || types.T(argument.Typ.Id) != family {
			return false
		}
	}
	return true
}

func varcharArgs(args []*planpb.Expr, count int) bool {
	if len(args) != count {
		return false
	}
	for _, argument := range args {
		if argument == nil || types.T(argument.Typ.Id) != types.T_varchar {
			return false
		}
	}
	return true
}

func tpchCastType(value types.T) bool {
	switch value {
	case types.T_int32, types.T_int64, types.T_decimal64, types.T_decimal128, types.T_varchar:
		return true
	default:
		return false
	}
}

func tpchCaseArgs(args []*planpb.Expr, out *planpb.Type) bool {
	for i, argument := range args {
		if argument == nil {
			return false
		}
		if i < len(args)-1 && i%2 == 0 {
			if types.T(argument.Typ.Id) != types.T_bool {
				return false
			}
		} else if argument.Typ.Id != out.Id || argument.Typ.Width != out.Width || argument.Typ.Scale != out.Scale {
			return false
		}
	}
	return true
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
		case types.T_int8, types.T_int16, types.T_int32, types.T_int64, types.T_float32, types.T_float64, types.T_decimal64, types.T_decimal128:
			return true
		}
		return false
	}
	switch name {
	case "and", "or", "not":
		if !isBool(out) || (name != "not" && len(args) < 2) {
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
		if !numeric(out) || len(args) != 2 {
			return notEligiblef(EligibilityExpression, "unsupported %s signature", name)
		}
	case "like", "starts_with":
		if !isBool(out) || len(args) != 2 || types.T(args[0].Typ.Id) != types.T_varchar || types.T(args[1].Typ.Id) != types.T_varchar {
			return notEligiblef(EligibilityExpression, "unsupported %s signature", name)
		}
	case "substring":
		if types.T(out.Id) != types.T_varchar || len(args) != 3 || types.T(args[0].Typ.Id) != types.T_varchar || types.T(args[1].Typ.Id) != types.T_int64 || types.T(args[2].Typ.Id) != types.T_int64 {
			return notEligiblef(EligibilityExpression, "unsupported substring signature")
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
			return 0, notEligiblef(
				EligibilityExpression,
				"fetch value %d exceeds the Substrait signed integer range",
				n.U64Val,
			)
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

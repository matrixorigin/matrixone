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

package substrait

import (
	"strconv"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	spb "github.com/substrait-io/substrait-protobuf/go/substraitpb"
	"google.golang.org/protobuf/proto"
)

const embeddedNamedTable = "__sirius_embedded_v1"

// EmbeddedReadSource selects the query-local producer behind an embedded
// Substrait named-table binding. Zero is deliberately invalid so an omitted
// source never silently selects storage.
type EmbeddedReadSource uint8

const (
	EmbeddedReadMO EmbeddedReadSource = iota + 1
)

// EmbeddedReadBinding binds one plan scan to one native query-local input.
type EmbeddedReadBinding struct {
	BindingID uint64
	Source    EmbeddedReadSource
}

// EmbeddedMORead describes the exact post-scan batch contract produced by MO.
type EmbeddedMORead struct {
	NodeID                  int32
	Database, Table, Schema string
	Columns                 []EmbeddedMOColumn
}

// EmbeddedMOColumn retains both the post-scan logical contract and the
// physical source identity needed to read the planning snapshot safely.
type EmbeddedMOColumn struct {
	Name       string
	Type       planpb.Type
	PhysicalID uint64
	Sequence   uint32
}

// EmbeddedMOReads returns reads in deterministic binding order. Callers assign
// binding IDs starting at one in this order.
func (c *Candidate) EmbeddedMOReads() ([]EmbeddedMORead, error) {
	if c == nil || c.query == nil {
		return nil, moerr.NewInternalErrorNoCtx("substrait: nil embedded candidate")
	}
	if err := c.validateEmbeddedReadOccurrences(); err != nil {
		return nil, err
	}
	result := make([]EmbeddedMORead, len(c.reads))
	for i, read := range c.reads {
		node, err := c.embeddedReadNode(read.NodeID)
		if err != nil {
			return nil, err
		}
		result[i], _, err = embeddedMORead(node)
		if err != nil {
			return nil, err
		}
	}
	return result, nil
}

// BuildEmbedded serializes a plan whose storage scans resolve only through
// explicit query-local named-table bindings. MO bindings replace the complete
// scan because the MO producer already applies its filter, projection and
// fetch. The embedded route accepts only MO readers in this milestone.
func (c *Candidate) BuildEmbedded(bindings map[int32]EmbeddedReadBinding) ([]byte, error) {
	if c == nil || c.query == nil {
		return nil, moerr.NewInternalErrorNoCtx("substrait: nil embedded candidate")
	}
	if err := c.validateEmbeddedBindings(bindings); err != nil {
		return nil, err
	}
	e := exporter{query: c.query, embeddedBindings: bindings}
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
	plan := &spb.Plan{
		Version:    &spb.Version{MajorNumber: 0, MinorNumber: 78, PatchNumber: 0, Producer: "matrixone"},
		Relations:  relations,
		Extensions: e.extensions(),
	}
	wire, err := proto.MarshalOptions{Deterministic: true}.Marshal(plan)
	if err != nil {
		return nil, moerr.NewInternalErrorNoCtxf("substrait: marshal embedded plan: %v", err)
	}
	if len(wire) > MaxPlanBytes {
		return nil, moerr.NewInternalErrorNoCtxf("substrait: embedded plan is %d bytes, maximum is %d", len(wire), MaxPlanBytes)
	}
	return wire, nil
}

func (c *Candidate) validateEmbeddedBindings(bindings map[int32]EmbeddedReadBinding) error {
	if err := c.validateEmbeddedReadOccurrences(); err != nil {
		return err
	}
	if len(bindings) != len(c.reads) {
		return moerr.NewInternalErrorNoCtxf("substrait: embedded binding count mismatch: got %d, want %d", len(bindings), len(c.reads))
	}
	seenBindings := make(map[uint64]int32, len(bindings))
	for ordinal, read := range c.reads {
		binding, ok := bindings[read.NodeID]
		if !ok {
			return moerr.NewInternalErrorNoCtxf("substrait: missing embedded binding for node %d", read.NodeID)
		}
		if binding.BindingID == 0 {
			return moerr.NewInternalErrorNoCtxf("substrait: embedded binding for node %d is zero", read.NodeID)
		}
		if previous, duplicate := seenBindings[binding.BindingID]; duplicate {
			return moerr.NewInternalErrorNoCtxf("substrait: duplicate embedded binding %d for nodes %d and %d", binding.BindingID, previous, read.NodeID)
		}
		seenBindings[binding.BindingID] = read.NodeID
		expected := uint64(ordinal + 1)
		if binding.BindingID != expected {
			return moerr.NewInternalErrorNoCtxf("substrait: embedded binding mismatch for node %d: got %d, want %d", read.NodeID, binding.BindingID, expected)
		}
		if binding.Source != EmbeddedReadMO {
			return moerr.NewInternalErrorNoCtxf("substrait: embedded binding %d has unsupported source %d", binding.BindingID, binding.Source)
		}
		node, err := c.embeddedReadNode(read.NodeID)
		if err != nil {
			return err
		}
		if _, _, err = embeddedMORead(node); err != nil {
			return err
		}
	}
	return nil
}

func (c *Candidate) validateEmbeddedReadOccurrences() error {
	seen := make(map[int32]bool, len(c.reads))
	visiting := make(map[int32]bool)
	var walk func(int32) error
	walk = func(id int32) error {
		if id < 0 || int(id) >= len(c.query.Nodes) || c.query.Nodes[id] == nil {
			return moerr.NewInternalErrorNoCtxf("substrait: embedded read candidate has invalid node %d", id)
		}
		if visiting[id] {
			return moerr.NewInternalErrorNoCtxf("substrait: embedded read candidate is cyclic at node %d", id)
		}
		visiting[id] = true
		defer delete(visiting, id)
		node := c.query.Nodes[id]
		if node.NodeId != id {
			return moerr.NewInternalErrorNoCtxf("substrait: embedded read candidate node %d is misindexed", id)
		}
		if node.NodeType == planpb.Node_TABLE_SCAN {
			if seen[id] {
				return moerr.NewInternalErrorNoCtxf("substrait: embedded read node %d is replayed", id)
			}
			seen[id] = true
			return nil
		}
		for _, child := range node.Children {
			if err := walk(child); err != nil {
				return err
			}
		}
		return nil
	}
	for _, root := range c.query.Steps {
		if err := walk(root); err != nil {
			return err
		}
	}
	if len(seen) != len(c.reads) {
		return moerr.NewInternalErrorNoCtx("substrait: embedded read candidate mismatch")
	}
	for _, read := range c.reads {
		if !seen[read.NodeID] {
			return moerr.NewInternalErrorNoCtxf("substrait: embedded read candidate is missing node %d", read.NodeID)
		}
	}
	return nil
}

func (c *Candidate) embeddedReadNode(id int32) (*planpb.Node, error) {
	if id < 0 || int(id) >= len(c.query.Nodes) {
		return nil, moerr.NewInternalErrorNoCtxf("substrait: embedded read node %d is outside the candidate", id)
	}
	node := c.query.Nodes[id]
	if node == nil || node.NodeId != id || node.NodeType != planpb.Node_TABLE_SCAN || node.TableDef == nil || node.ObjRef == nil {
		return nil, moerr.NewInternalErrorNoCtxf("substrait: embedded read node %d does not match the candidate", id)
	}
	return node, nil
}

func embeddedMORead(node *planpb.Node) (EmbeddedMORead, *spb.NamedStruct, error) {
	physicalSchema, err := namedStruct(node.TableDef)
	if err != nil {
		return EmbeddedMORead{}, nil, err
	}
	visible := node.TableDef.Cols[:len(physicalSchema.Names)]
	ordinals := make([]int32, 0, len(visible))
	outputTypes := make([]planpb.Type, 0, len(visible))
	if len(node.ProjectList) == 0 {
		// MO leaves an empty projection untouched, so its scan emits every
		// TableDef column. Advertising only the visible prefix would mismatch
		// the native binding when the table has a hidden suffix.
		if len(visible) != len(node.TableDef.Cols) {
			return EmbeddedMORead{}, nil, notEligiblef(EligibilityOperator, "embedded MO scan node %d needs a projection to remove hidden columns", node.NodeId)
		}
		for ordinal := range visible {
			ordinals = append(ordinals, int32(ordinal))
			outputTypes = append(outputTypes, visible[ordinal].Typ)
		}
	} else {
		ordinals = make([]int32, len(node.ProjectList))
		outputTypes = make([]planpb.Type, len(node.ProjectList))
		for output, expression := range node.ProjectList {
			if expression == nil || expression.GetCol() == nil {
				return EmbeddedMORead{}, nil, notEligiblef(EligibilityExpression, "embedded MO scan node %d output %d is not a direct source column", node.NodeId, output)
			}
			ordinal, ordinalErr := fieldOrdinal(expression.GetCol(), []int{len(visible)})
			if ordinalErr != nil {
				return EmbeddedMORead{}, nil, ordinalErr
			}
			physicalType := visible[ordinal].Typ
			if expression.Typ.Id != physicalType.Id || expression.Typ.Width != physicalType.Width || expression.Typ.Scale != physicalType.Scale {
				return EmbeddedMORead{}, nil, notEligiblef(EligibilityType, "embedded MO scan node %d output %d type does not match source column %d", node.NodeId, output, ordinal)
			}
			if _, typeErr := substraitType(&expression.Typ); typeErr != nil {
				return EmbeddedMORead{}, nil, typeErr
			}
			ordinals[output] = ordinal
			outputTypes[output] = expression.Typ
		}
	}

	read := EmbeddedMORead{NodeID: node.NodeId, Database: node.ObjRef.DbName, Table: node.ObjRef.ObjName, Schema: node.ObjRef.SchemaName}
	if read.Database == "" {
		read.Database = node.TableDef.DbName
	}
	if read.Table == "" {
		read.Table = node.TableDef.Name
	}
	names := make([]string, len(ordinals))
	types := make([]*spb.Type, len(ordinals))
	read.Columns = make([]EmbeddedMOColumn, len(ordinals))
	for output, ordinal := range ordinals {
		column := visible[ordinal]
		name := "col_" + strconv.Itoa(output)
		typ, typeErr := substraitType(&outputTypes[output])
		if typeErr != nil {
			return EmbeddedMORead{}, nil, typeErr
		}
		names[output] = name
		types[output] = typ
		read.Columns[output] = EmbeddedMOColumn{Name: name, Type: outputTypes[output], PhysicalID: column.ColId, Sequence: column.Seqnum}
	}
	schema := &spb.NamedStruct{Names: names, Struct: &spb.Type_Struct{Types: types, Nullability: spb.Type_NULLABILITY_REQUIRED}}
	return read, schema, nil
}

func embeddedNamedRead(binding EmbeddedReadBinding, schema *spb.NamedStruct) *spb.Rel {
	return &spb.Rel{RelType: &spb.Rel_Read{Read: &spb.ReadRel{
		BaseSchema: schema,
		ReadType: &spb.ReadRel_NamedTable_{NamedTable: &spb.ReadRel_NamedTable{
			Names: []string{embeddedNamedTable, strconv.FormatUint(binding.BindingID, 10)},
		}},
	}}}
}

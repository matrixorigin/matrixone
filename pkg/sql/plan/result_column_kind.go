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
	"strings"
	"unicode"

	pb "github.com/matrixorigin/matrixone/pkg/pb/plan"
)

type resultColumnAddress uint8

const (
	resultColumnExpression resultColumnAddress = iota
	resultColumnOutputSlot
	resultColumnReference
)

// An output slot, its selected expression, and a binding reference are distinct
// addresses. In particular, a selected ColRef must never select a sibling slot.
type resultColumnValue struct {
	node    int32
	address resultColumnAddress
	expr    *pb.Expr
	col     int32
	// References belong to the immutable plan; retain their identity without copying names.
	ref *pb.ColRef
}

type resultColumnValueState struct {
	value    resultColumnValue
	parents  []int
	computed bool
}

// resultColumnKinds belongs to one metadata extraction. Discover each typed
// address once, then propagate affirmative computation through reverse edges.
// A recursive back edge is a dependency, not a cached false answer. Pure cycles
// remain stored; a computed producer reaches every dependent regardless of the
// order in which outputs or recursive producers are visited.
type resultColumnKinds struct {
	query          *pb.Query
	ids            map[resultColumnValue]int
	states         []resultColumnValueState
	expanded       int
	ready          []int
	windows        map[int32]map[*pb.Expr]*pb.WindowSpec
	projections    map[int32]*resultColumnProjectionIndex
	bindings       map[int32]map[int32]int
	timeWindows    map[int32]TimeWindowLayout
	returningImage int32
}

func newResultColumnKinds(query *pb.Query) *resultColumnKinds {
	return &resultColumnKinds{
		query: query, ids: make(map[resultColumnValue]int),
		returningImage: resultColumnReturningImage(query),
	}
}

// appendReturningProjection exposes the target row image through a dedicated
// SINK_SCAN below the RETURNING projection (and optional selector FILTER).
// Locate that owner from the RETURNING step, not from TableDef alone: ordinary
// materialized expressions also have schemas and must retain their producers.
func resultColumnReturningImage(query *pb.Query) int32 {
	if query == nil || !query.HasReturning || query.ReturningStep < 0 || int(query.ReturningStep) >= len(query.Steps) {
		return -1
	}
	nodeID := query.Steps[query.ReturningStep]
	for hops := 0; hops < len(query.Nodes); hops++ {
		if nodeID < 0 || int(nodeID) >= len(query.Nodes) || query.Nodes[nodeID] == nil {
			return -1
		}
		node := query.Nodes[nodeID]
		if node.NodeType == pb.Node_SINK_SCAN {
			if node.TableDef != nil {
				return nodeID
			}
			return -1
		}
		if len(node.Children) != 1 {
			return -1
		}
		nodeID = node.Children[0]
	}
	return -1
}

func (k *resultColumnKinds) kind(node int32, expr *pb.Expr) pb.ResultColumnKind {
	if k.computed(node, expr) {
		return pb.ResultColumnKind_RESULT_COLUMN_KIND_COMPUTED
	}
	// An unresolved reference is not evidence of a computation. Explicit STORED
	// also prevents the frontend's legacy UNKNOWN/origin fallback from widening it.
	return pb.ResultColumnKind_RESULT_COLUMN_KIND_STORED
}

func (k *resultColumnKinds) computed(node int32, expr *pb.Expr) bool {
	root := k.intern(resultColumnValue{node: node, address: resultColumnExpression, expr: expr})
	for k.expanded < len(k.states) {
		id := k.expanded
		k.expanded++
		k.expand(id)
	}
	for len(k.ready) > 0 {
		id := k.ready[len(k.ready)-1]
		k.ready = k.ready[:len(k.ready)-1]
		for _, parent := range k.states[id].parents {
			k.mark(parent)
		}
	}
	return k.states[root].computed
}

func (k *resultColumnKinds) intern(value resultColumnValue) int {
	if id, ok := k.ids[value]; ok {
		return id
	}
	id := len(k.states)
	k.ids[value] = id
	k.states = append(k.states, resultColumnValueState{value: value})
	return id
}

func (k *resultColumnKinds) mark(id int) {
	if !k.states[id].computed {
		k.states[id].computed = true
		k.ready = append(k.ready, id)
	}
}

func (k *resultColumnKinds) depend(id int, value resultColumnValue) {
	child := k.intern(value)
	k.states[child].parents = append(k.states[child].parents, id)
	if k.states[child].computed {
		k.mark(id)
	}
}

func (k *resultColumnKinds) expression(id int, node int32, expr *pb.Expr) {
	k.depend(id, resultColumnValue{node: node, address: resultColumnExpression, expr: expr})
}

func (k *resultColumnKinds) slot(id int, node, col int32) {
	k.depend(id, resultColumnValue{node: node, address: resultColumnOutputSlot, col: col})
}

func (k *resultColumnKinds) reference(id int, node int32, ref *pb.ColRef) {
	if ref == nil {
		return
	}
	k.depend(id, resultColumnValue{node: node, address: resultColumnReference, ref: ref})
}

func (k *resultColumnKinds) node(id int32) *pb.Node {
	if k.query == nil || id < 0 || int(id) >= len(k.query.Nodes) {
		return nil
	}
	return k.query.Nodes[id]
}

func (k *resultColumnKinds) expand(id int) {
	value := k.states[id].value
	node := k.node(value.node)
	if node == nil {
		return
	}
	if value.address == resultColumnOutputSlot {
		if value.col >= 0 && int(value.col) < len(node.ProjectList) {
			// WINDOW synthetic slots are owned by WinSpecList, even though their
			// projection is represented by a ColRef like an ordinary passthrough.
			if node.NodeType == pb.Node_WINDOW {
				win := int(value.col) - (len(node.ProjectList) - len(node.WinSpecList))
				if win >= 0 && win < len(node.WinSpecList) {
					k.window(id, node, node.WinSpecList[win].GetW())
					return
				}
			}
			k.expression(id, value.node, node.ProjectList[value.col])
		}
		return
	}
	if value.address == resultColumnReference {
		ref := value.ref
		if node.NodeType == pb.Node_JOIN {
			k.join(id, node, nil, ref, len(node.BindingTags) == 0)
			return
		}
		if expr := k.projection(value.node, ref); expr != nil {
			k.expression(id, value.node, expr)
		}
		return
	}
	expr := value.expr
	if expr == nil {
		return
	}
	if spec := expr.GetW(); spec != nil {
		k.window(id, node, spec)
		return
	}
	if node.NodeType == pb.Node_WINDOW {
		if k.windows == nil {
			k.windows = make(map[int32]map[*pb.Expr]*pb.WindowSpec)
		}
		specs, ok := k.windows[value.node]
		if !ok {
			specs = make(map[*pb.Expr]*pb.WindowSpec, len(node.WinSpecList))
			first := len(node.ProjectList) - len(node.WinSpecList)
			if first >= 0 {
				for i, spec := range node.WinSpecList {
					specs[node.ProjectList[first+i]] = spec.GetW()
				}
			}
			k.windows[value.node] = specs
		}
		if spec, ok := specs[expr]; ok {
			k.window(id, node, spec)
			return
		}
	}
	ref := expr.GetCol()
	if ref == nil {
		k.mark(id)
		return
	}
	if node.NodeType == pb.Node_JOIN {
		k.join(id, node, expr, ref, false)
		return
	}

	if isResultColumnSourceNode(node.NodeType) {
		return
	}
	if node.NodeType == pb.Node_AGG || node.NodeType == pb.Node_SAMPLE {
		switch ref.RelPos {
		case 0:
			if node.NodeType == pb.Node_SAMPLE {
				// SAMPLE emits group and sampled vectors before forwarded input
				// vectors. This is an emitted-vector position, not a child slot.
				pos := ref.ColPos - int32(len(node.GroupBy)+len(node.AggList))
				if pos >= 0 {
					for _, child := range node.Children {
						k.slot(id, child, pos)
					}
				}
				return
			}
		case -1:
			if ref.ColPos >= 0 && int(ref.ColPos) < len(node.GroupBy) {
				k.inputExpression(id, node, node.GroupBy[ref.ColPos])
			}
			return
		case -2:
			// Remapping numbers aggregate outputs after all group outputs.
			pos := int(ref.ColPos) - len(node.GroupBy)
			if pos >= 0 && pos < len(node.AggList) && node.AggList[pos] != nil {
				if node.NodeType == pb.Node_SAMPLE {
					k.inputExpression(id, node, node.AggList[pos])
				} else {
					k.mark(id)
				}
			}
			return
		}
	}
	switch node.NodeType {
	case pb.Node_TIME_WINDOW:
		// TIME_WINDOW emits aggregates, boundaries, then partition keys;
		// its child AGG emits group keys first. Never treat an emitted slot
		// as the same ordinal in the child. Cache the shared operator layout
		// once per extraction so many BOOL outputs do not rebuild it.
		if k.timeWindows == nil {
			k.timeWindows = make(map[int32]TimeWindowLayout)
		}
		layout, ok := k.timeWindows[value.node]
		if !ok {
			layout = BuildTimeWindowLayout(node)
			k.timeWindows[value.node] = layout
		}
		if ref.ColPos < 0 || ref.ColPos >= layout.ColCnt {
			return
		}
		partition := ref.ColPos - (layout.ColCnt - int32(len(layout.PartitionSlot)))
		if partition >= 0 {
			k.inputExpression(id, node, node.TimeWindowPartitionBy[partition])
		} else {
			// Real aggregates and synthetic window boundaries are computations.
			k.mark(id)
		}
		return
	case pb.Node_VALUE_SCAN:
		cols := node.GetRowsetData().GetCols()
		if ref.ColPos < 0 || int(ref.ColPos) >= len(cols) {
			return
		}
		// Rowset columns are pruned/remapped with the VALUE_SCAN projection.
		// Only values in the referenced column contribute to its provenance.
		for _, row := range cols[ref.ColPos].GetData() {
			producer := row.GetExpr()
			if producer == nil {
				continue
			}
			if producer.GetCol() == nil && producer.GetW() == nil {
				// The ordinary scalar-expression rule is affirmative here. Stop
				// without building one graph state per row of a large VALUES.
				k.mark(id)
				return
			}
			k.expression(id, value.node, producer)
		}
		return
	case pb.Node_SINK_SCAN, pb.Node_RECURSIVE_CTE, pb.Node_RECURSIVE_SCAN:
		if value.node == k.returningImage {
			// A reference here reads a declared target column, not its
			// INSERT/UPDATE assignment. Expressions above this row-image
			// boundary are still classified independently as computations.
			return
		}
		for _, step := range node.SourceStep {
			if step >= 0 && int(step) < len(k.query.Steps) {
				k.slot(id, k.query.Steps[step], ref.ColPos)
			}
		}
		return
	case pb.Node_VECTOR_QUERY_TOP:
		if len(node.Children) == 3 {
			k.slot(id, node.Children[2], ref.ColPos)
		}
		return
	case pb.Node_MINUS, pb.Node_MINUS_ALL, pb.Node_INTERSECT, pb.Node_INTERSECT_ALL:
		// Right inputs control membership, but never provide returned values.
		if len(node.Children) > 0 {
			k.slot(id, node.Children[0], ref.ColPos)
		}
		return
	}
	for _, child := range node.Children {
		k.slot(id, child, ref.ColPos)
	}
}

func (k *resultColumnKinds) join(id int, node *pb.Node, selected *pb.Expr, ref *pb.ColRef, local bool) {
	nodeID := k.states[id].value.node
	if selected == nil {
		if local && ref.RelPos == 0 && ref.ColPos >= 0 && int(ref.ColPos) < len(node.ProjectList) {
			selected = node.ProjectList[ref.ColPos]
		} else {
			selected = k.projection(nodeID, ref)
		}
	}
	route := resultColumnRouteJoinProjection(k.query, node, selected, ref)
	if route.computed {
		k.mark(id)
		return
	}
	if route.childIdx < 0 {
		route.childIdx = k.bindingChild(nodeID, node, route.ref)
	}
	if route.childIdx < 0 {
		return
	}
	child := node.Children[route.childIdx]
	if route.childExpr != nil {
		k.expression(id, child, route.childExpr)
	} else {
		k.reference(id, child, route.ref)
	}
}

func (k *resultColumnKinds) inputExpression(id int, node *pb.Node, expr *pb.Expr) {
	if expr == nil {
		return
	}
	ref := expr.GetCol()
	if ref == nil {
		k.mark(id)
		return
	}
	for _, child := range node.Children {
		k.slot(id, child, ref.ColPos)
	}
}

func (k *resultColumnKinds) window(id int, node *pb.Node, spec *pb.WindowSpec) {
	if spec == nil || spec.WindowFunc == nil {
		k.mark(id)
		return
	}
	if spec.WindowFunc.GetCol() != nil {
		k.inputExpression(id, node, spec.WindowFunc)
		return
	}
	fn := spec.WindowFunc.GetF()
	if fn == nil || fn.Func == nil || len(fn.Args) == 0 {
		k.mark(id)
		return
	}
	switch strings.ToLower(fn.Func.ObjName) {
	case "first_value", "last_value", "nth_value", "lag", "lead":
		k.inputExpression(id, node, fn.Args[0])
		if (strings.EqualFold(fn.Func.ObjName, "lag") || strings.EqualFold(fn.Func.ObjName, "lead")) && len(fn.Args) > 2 {
			fallback := fn.Args[2]
			// NULL does not supply a competing value domain. The offset is
			// control input; only the first argument and default supply values.
			if fallback != nil && !lagLeadArgumentIsNullLiteral(fallback) {
				k.inputExpression(id, node, fallback)
			}
		}
	default:
		k.mark(id)
	}
}

// Indexes are lazy: remapped, selected projections never need either lookup.
// Keep the original first-match rule for legacy identity/name references.
type resultColumnProjectionIndex struct {
	positions map[[2]int32]*pb.Expr
	names     map[resultColumnNameKey]resultColumnNamedProjection
}

type resultColumnNameKey struct {
	qualifiers            uint8
	name, table, database string
}

type resultColumnNamedProjection struct {
	expr    *pb.Expr
	ordinal int
}

// EqualFold uses Unicode simple-fold cycles. Canonicalizing each cycle avoids
// changing the existing name fallback's semantics (e.g. sigma/final sigma).
func resultColumnFoldName(name string) string {
	return strings.Map(func(r rune) rune {
		smallest := r
		for next := unicode.SimpleFold(r); next != r; next = unicode.SimpleFold(next) {
			if next < smallest {
				smallest = next
			}
		}
		return smallest
	}, name)
}

func (k *resultColumnKinds) projection(nodeID int32, ref *pb.ColRef) *pb.Expr {
	node := k.node(nodeID)
	if node == nil || ref == nil {
		return nil
	}
	if k.projections == nil {
		k.projections = make(map[int32]*resultColumnProjectionIndex)
	}
	index := k.projections[nodeID]
	if index == nil {
		index = &resultColumnProjectionIndex{positions: make(map[[2]int32]*pb.Expr, len(node.ProjectList))}
		for _, expr := range node.ProjectList {
			if col := expr.GetCol(); col != nil {
				key := [2]int32{col.RelPos, col.ColPos}
				if _, exists := index.positions[key]; !exists {
					index.positions[key] = expr
				}
			}
		}
		k.projections[nodeID] = index
	}
	if expr := index.positions[[2]int32{ref.RelPos, ref.ColPos}]; expr != nil {
		return expr
	}
	if ref.Name == "" && ref.TblName == "" {
		return nil
	}
	if index.names == nil {
		index.names = make(map[resultColumnNameKey]resultColumnNamedProjection)
		for ordinal, expr := range node.ProjectList {
			col := expr.GetCol()
			if col == nil {
				continue
			}
			name, table, database := resultColumnFoldName(col.Name), resultColumnFoldName(col.TblName), resultColumnFoldName(col.DbName)
			for qualifiers := uint8(0); qualifiers < 4; qualifiers++ {
				key := resultColumnNameKey{qualifiers: qualifiers, name: name}
				if qualifiers&1 != 0 {
					key.table = table
				}
				if qualifiers&2 != 0 {
					key.database = database
				}
				if _, exists := index.names[key]; !exists {
					index.names[key] = resultColumnNamedProjection{expr: expr, ordinal: ordinal}
				}
			}
		}
	}
	key := resultColumnNameKey{name: resultColumnFoldName(ref.Name)}
	tables, databases := []string{""}, []string{""}
	if ref.TblName != "" {
		key.qualifiers |= 1
		tables = append(tables, resultColumnFoldName(ref.TblName))
	}
	if ref.DbName != "" {
		key.qualifiers |= 2
		databases = append(databases, resultColumnFoldName(ref.DbName))
	}
	best := resultColumnNamedProjection{ordinal: len(node.ProjectList)}
	for _, table := range tables {
		for _, database := range databases {
			key.table, key.database = table, database
			candidate, exists := index.names[key]
			if exists && candidate.ordinal < best.ordinal && resultColumnRefsMatchByName(ref, candidate.expr.GetCol()) {
				best = candidate
			}
		}
	}
	return best.expr
}

func (k *resultColumnKinds) bindingChild(nodeID int32, node *pb.Node, ref *pb.ColRef) int {
	if ref == nil {
		return -1
	}
	if k.bindings == nil {
		k.bindings = make(map[int32]map[int32]int)
	}
	owners, exists := k.bindings[nodeID]
	if !exists {
		owners = make(map[int32]int)
		for childIndex, child := range node.Children {
			pending := []int32{child}
			visited := make(map[int32]bool)
			for len(pending) > 0 {
				id := pending[len(pending)-1]
				pending = pending[:len(pending)-1]
				if visited[id] {
					continue
				}
				visited[id] = true
				producer := k.node(id)
				if producer == nil {
					continue
				}
				for _, tag := range producer.BindingTags {
					if owner, seen := owners[tag]; seen && owner != childIndex {
						owners[tag] = -1
					} else {
						owners[tag] = childIndex
					}
				}
				pending = append(pending, producer.Children...)
			}
		}
		k.bindings[nodeID] = owners
	}
	child, found := owners[ref.RelPos]
	if !found {
		return -1
	}
	return child
}

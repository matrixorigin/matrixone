// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package plan

import (
	"github.com/gogo/protobuf/proto"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
)

const CorrelationVersion uint32 = 1

// MainOutputStep preserves the historical last-step contract for ordinary plans.
// Callers validate a correlation descriptor before using its indexes.
func (q *Query) MainOutputStep() int32 {
	if q == nil || len(q.Steps) == 0 {
		return -1
	}
	if q.Correlation != nil {
		return q.Correlation.MainOutputStep
	}
	return int32(len(q.Steps) - 1)
}

// WalkQueryNodes visits each executable node once, including demand-owned roots.
// SourceStep references address the global step catalog. They are not recursive
// calls: recursive CTEs can legitimately revisit an already discovered step.
func WalkQueryNodes(q *Query, visit func(int32, *Node) error) error {
	if q == nil {
		return nil
	}
	pending := append([]int32(nil), q.Steps...)
	if q.Correlation != nil {
		for _, program := range q.Correlation.Programs {
			if program == nil {
				return moerr.NewInternalErrorNoCtx("nil correlation program")
			}
			pending = append(pending, program.RootNode)
		}
	}
	seen := make([]bool, len(q.Nodes))
	for len(pending) > 0 {
		id := pending[len(pending)-1]
		pending = pending[:len(pending)-1]
		if id < 0 || int(id) >= len(q.Nodes) || q.Nodes[id] == nil {
			return moerr.NewInternalErrorNoCtxf("invalid query node %d", id)
		}
		if seen[id] {
			continue
		}
		seen[id] = true
		n := q.Nodes[id]
		if err := visit(id, n); err != nil {
			return err
		}
		pending = append(pending, n.Children...)
		for _, step := range n.SourceStep {
			if step < 0 || int(step) >= len(q.Steps) {
				return moerr.NewInternalErrorNoCtxf("invalid source step %d", step)
			}
			pending = append(pending, q.Steps[step])
		}
	}
	return nil
}

// ValidateCorrelation checks a complete, bound descriptor before publication or
// compilation. Programs and stages use dense one-based ids, while region zero
// is the main statement. Runtime state and SQL text never supply missing links.
func (q *Query) ValidateCorrelation() error {
	if q == nil {
		return nil
	}
	spec := q.Correlation
	if spec == nil {
		return WalkQueryNodes(q, func(_ int32, n *Node) error {
			return VisitExpressionsInOwner(n, func(e *Expr) error {
				return VisitExprTree(e, func(e *Expr) error {
					if e.GetDependent() != nil || e.GetSlot() != nil {
						return moerr.NewInternalErrorNoCtx("correlation expression without descriptor")
					}
					return nil
				})
			})
		})
	}
	invalid := func(s string, args ...any) error {
		return moerr.NewInternalErrorNoCtxf("invalid correlation plan: "+s, args...)
	}
	if spec.Version != CorrelationVersion {
		return invalid("version %d", spec.Version)
	}
	if len(spec.StepOwners) != len(q.Steps) {
		return invalid("step owner count")
	}
	if spec.MainOutputStep < 0 || int(spec.MainOutputStep) >= len(q.Steps) || spec.StepOwners[spec.MainOutputStep] != 0 {
		return invalid("main output step")
	}
	programCount := len(spec.Programs)
	owners := make([]int32, len(q.Nodes))
	for i := range owners {
		owners[i] = -1
	}
	// Each region owns disjoint node state. Sharing must be expressed by a
	// materialized source with an explicit lease, not a cross-owner child edge.
	type root struct {
		node  int32
		owner uint32
	}
	roots := make([]root, 0, len(q.Steps)+programCount)
	for i, id := range q.Steps {
		roots = append(roots, root{id, spec.StepOwners[i]})
	}
	for i, p := range spec.Programs {
		if p == nil || len(p.OutputTypes) == 0 {
			return invalid("program %d has no output", i+1)
		}
		roots = append(roots, root{p.RootNode, uint32(i + 1)})
	}
	for _, r := range roots {
		if uint64(r.owner) > uint64(programCount) {
			return invalid("region %d", r.owner)
		}
		pending := []int32{r.node}
		for len(pending) > 0 {
			id := pending[len(pending)-1]
			pending = pending[:len(pending)-1]
			if id < 0 || int(id) >= len(q.Nodes) || q.Nodes[id] == nil {
				return invalid("node %d", id)
			}
			if owners[id] >= 0 {
				if uint32(owners[id]) != r.owner {
					return invalid("node %d belongs to two regions", id)
				}
				continue
			}
			owners[id] = int32(r.owner)
			n := q.Nodes[id]
			pending = append(pending, n.Children...)
			for _, step := range n.SourceStep {
				if step < 0 || int(step) >= len(q.Steps) || spec.StepOwners[step] != r.owner {
					return invalid("cross-region source step %d", step)
				}
				pending = append(pending, q.Steps[step])
			}
		}
	}
	// Children describe an operator DAG; only SourceStep may carry a recursive
	// CTE back edge. A child cycle would otherwise hang compiler traversal.
	color := make([]uint8, len(q.Nodes))
	type cursor struct {
		node int32
		next int
	}
	for id := range q.Nodes {
		if owners[id] < 0 || color[id] != 0 {
			continue
		}
		stack := []cursor{{node: int32(id)}}
		color[id] = 1
		for len(stack) > 0 {
			top := &stack[len(stack)-1]
			children := q.Nodes[top.node].Children
			if top.next == len(children) {
				color[top.node] = 2
				stack = stack[:len(stack)-1]
				continue
			}
			child := children[top.next]
			top.next++
			if color[child] == 1 {
				return invalid("operator child cycle")
			}
			if color[child] == 0 {
				color[child] = 1
				stack = append(stack, cursor{node: child})
			}
		}
	}
	for i, s := range spec.Stages {
		if s == nil || s.OwnerNode < 0 || int(s.OwnerNode) >= len(owners) || owners[s.OwnerNode] < 0 {
			return invalid("stage %d owner", i+1)
		}
		if s.Kind < CorrelationStage_INPUT || s.Kind > CorrelationStage_ROUTE {
			return invalid("stage %d kind", i+1)
		}
	}
	calls := make([][]uint32, programCount+1)
	occurrences := make(map[uint32]*Expr)
	for id, n := range q.Nodes {
		if owners[id] < 0 {
			continue
		}
		owner := uint32(owners[id])
		err := VisitExpressionsInOwner(n, func(expr *Expr) error {
			return VisitExprTree(expr, func(e *Expr) error {
				if e.GetCorr() != nil || e.GetSub() != nil {
					return invalid("unbound expression at node %d", id)
				}
				if slot := e.GetSlot(); slot != nil {
					if owner == 0 || slot.ProgramId != owner {
						return invalid("slot owner at node %d", id)
					}
					p := spec.Programs[owner-1]
					if uint64(slot.Ordinal) >= uint64(len(p.InputTypes)) || !proto.Equal(&e.Typ, &p.InputTypes[slot.Ordinal]) {
						return invalid("slot type or ordinal at node %d", id)
					}
				}
				call := e.GetDependent()
				if call == nil {
					return nil
				}
				if call.ProgramId == 0 || uint64(call.ProgramId) > uint64(programCount) {
					return invalid("callee %d", call.ProgramId)
				}
				if call.OccurrenceId == 0 {
					return invalid("zero occurrence")
				}
				if call.StageId == 0 || uint64(call.StageId) > uint64(len(spec.Stages)) || spec.Stages[call.StageId-1].OwnerNode != int32(id) {
					return invalid("occurrence %d stage", call.OccurrenceId)
				}
				if prior, ok := occurrences[call.OccurrenceId]; ok && !proto.Equal(prior, e) {
					return invalid("conflicting occurrence %d", call.OccurrenceId)
				}
				occurrences[call.OccurrenceId] = e
				p := spec.Programs[call.ProgramId-1]
				if len(call.Inputs) != len(p.InputTypes) {
					return invalid("program %d input count", call.ProgramId)
				}
				for i, input := range call.Inputs {
					if input == nil || !proto.Equal(&input.Typ, &p.InputTypes[i]) {
						return invalid("program %d input %d type", call.ProgramId, i)
					}
				}
				switch call.Reducer {
				case SubqueryRef_SCALAR:
					if call.Comparison == nil {
						if len(p.OutputTypes) != 1 {
							return invalid("tuple scalar requires comparison")
						}
						expected := p.OutputTypes[0]
						expected.NotNullable = false
						if !proto.Equal(&e.Typ, &expected) {
							return invalid("scalar result type")
						}
					}
				case SubqueryRef_EXISTS, SubqueryRef_NOT_EXISTS:
					if len(call.Lhs) != 0 || call.Comparison != nil {
						return invalid("existential operands")
					}
				case SubqueryRef_IN, SubqueryRef_NOT_IN, SubqueryRef_ANY, SubqueryRef_ALL:
					if call.Comparison == nil {
						return invalid("quantified comparison")
					}
				default:
					return invalid("reducer %d", call.Reducer)
				}
				if call.Reducer != SubqueryRef_SCALAR || call.Comparison != nil {
					if e.Typ.Id != 10 {
						return invalid("boolean reducer result type")
					}
				}
				if call.Comparison != nil {
					if call.Comparison.Typ.Id != 10 {
						return invalid("comparison result type")
					}
					if len(call.Lhs) != len(p.OutputTypes) {
						return invalid("comparison tuple width")
					}
					if err := VisitExprTree(call.Comparison, func(c *Expr) error {
						if c.GetDependent() != nil || c.GetSlot() != nil || c.GetSub() != nil || c.GetCorr() != nil {
							return invalid("comparison must consume materialized tuples")
						}
						if col := c.GetCol(); col != nil {
							if col.ColPos < 0 || col.RelPos < 0 || col.RelPos > 1 {
								return invalid("comparison column")
							}
							types := p.OutputTypes
							if col.RelPos == 0 {
								if int(col.ColPos) >= len(call.Lhs) || call.Lhs[col.ColPos] == nil {
									return invalid("comparison lhs column")
								}
								if !proto.Equal(&c.Typ, &call.Lhs[col.ColPos].Typ) {
									return invalid("comparison lhs type")
								}
							} else if int(col.ColPos) >= len(types) || !proto.Equal(&c.Typ, &types[col.ColPos]) {
								return invalid("comparison rhs type")
							}
						}
						return nil
					}); err != nil {
						return err
					}
				}
				calls[owner] = append(calls[owner], call.ProgramId)
				return nil
			})
		})
		if err != nil {
			return err
		}
	}
	for i, p := range spec.Programs {
		if p.RootNode < 0 || int(p.RootNode) >= len(q.Nodes) || q.Nodes[p.RootNode] == nil {
			return invalid("program %d root", i+1)
		}
		outputs := q.Nodes[p.RootNode].ProjectList
		if len(outputs) != len(p.OutputTypes) {
			return invalid("program %d output arity", i+1)
		}
		for ordinal, output := range outputs {
			if output == nil || !proto.Equal(&output.Typ, &p.OutputTypes[ordinal]) {
				return invalid("program %d output type %d", i+1, ordinal)
			}
		}
	}
	// Kahn's algorithm does not grow the Go stack with correlation depth.
	incoming := make([]int, len(calls))
	for _, edges := range calls {
		for _, target := range edges {
			incoming[target]++
		}
	}
	pending := make([]uint32, 0, len(calls))
	for i, count := range incoming {
		if count == 0 {
			pending = append(pending, uint32(i))
		}
	}
	visited := 0
	for len(pending) > 0 {
		id := pending[len(pending)-1]
		pending = pending[:len(pending)-1]
		visited++
		for _, target := range calls[id] {
			incoming[target]--
			if incoming[target] == 0 {
				pending = append(pending, target)
			}
		}
	}
	if visited != len(calls) {
		return invalid("program call cycle")
	}
	return nil
}

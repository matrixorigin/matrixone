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
	"reflect"

	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
)

// A work tree belongs to one island in one synchronous expression search.
// Records refer to the original clone, never to the currently folded graph.
// Literal and target nodes are read-only; their Src descendants are audited too.
type legacyFloatProbeScratch struct {
	root  *planpb.Expr
	slots []legacyFloatProbeReset
	edges []*planpb.Expr
}

type legacyFloatProbeReset struct {
	node       *planpb.Expr
	function   *planpb.Expr_F
	list       *planpb.Expr_List
	start, end int
}

func (island *legacyFloatSessionIsland) foldInput() *planpb.Expr {
	if !island.scratchChecked {
		island.scratchChecked = true
		nodes, edges, ok := legacyFloatScratchShape(island.source)
		if ok {
			scratch := &legacyFloatProbeScratch{
				root:  DeepCopyExpr(island.source),
				slots: make([]legacyFloatProbeReset, 0, nodes),
				edges: make([]*planpb.Expr, 0, edges),
			}
			scratch.record(scratch.root)
			island.scratch = scratch
		}
	}
	if island.scratch != nil {
		return island.scratch.root
	}
	// Unsupported or malformed structures keep exactly the original clone path.
	return DeepCopyExpr(island.source)
}

func (island *legacyFloatSessionIsland) resetScratch() {
	if island.scratch != nil {
		island.scratch.reset()
	}
}

func (s *legacyFloatProbeScratch) reset() {
	for _, slot := range s.slots {
		if slot.function != nil {
			slot.node.Expr = slot.function
			copy(slot.function.F.Args, s.edges[slot.start:slot.end])
		} else {
			slot.node.Expr = slot.list
			copy(slot.list.List.List, s.edges[slot.start:slot.end])
		}
	}
}

func (s *legacyFloatProbeScratch) record(expr *planpb.Expr) {
	var children []*planpb.Expr
	slot := legacyFloatProbeReset{node: expr, start: len(s.edges)}
	switch payload := expr.Expr.(type) {
	case *planpb.Expr_F:
		slot.function = payload
		children = payload.F.Args
	case *planpb.Expr_List:
		slot.list = payload
		children = payload.List.List
	case *planpb.Expr_Lit:
		if payload.Lit.Src != nil {
			s.record(payload.Lit.Src)
		}
		return
	default:
		return
	}
	s.edges = append(s.edges, children...)
	slot.end = len(s.edges)
	s.slots = append(s.slots, slot)
	for _, child := range children {
		s.record(child)
	}
}

// Count before cloning so malformed inputs retain their original behavior and
// all reset storage is allocated once, without a per-node slice allocation.
func legacyFloatScratchShape(expr *planpb.Expr) (nodes, edges int, ok bool) {
	if expr == nil {
		return 0, 0, false
	}
	var children []*planpb.Expr
	switch payload := expr.Expr.(type) {
	case *planpb.Expr_F:
		if payload == nil || payload.F == nil || payload.F.Func == nil {
			return 0, 0, false
		}
		children = payload.F.Args
	case *planpb.Expr_List:
		if payload == nil || payload.List == nil {
			return 0, 0, false
		}
		children = payload.List.List
	case *planpb.Expr_Lit:
		if payload == nil || payload.Lit == nil || !legacyFloatScratchLiteral(payload.Lit) {
			return 0, 0, false
		}
		if payload.Lit.Src != nil {
			return legacyFloatScratchShape(payload.Lit.Src)
		}
		return 0, 0, true
	case *planpb.Expr_T:
		return 0, 0, payload != nil && payload.T != nil
	default:
		return 0, 0, false
	}
	nodes, edges = 1, len(children)
	for _, child := range children {
		n, e, admitted := legacyFloatScratchShape(child)
		if !admitted {
			return 0, 0, false
		}
		nodes += n
		edges += e
	}
	return nodes, edges, true
}

func legacyFloatScratchLiteral(lit *planpb.Literal) bool {
	if lit.Value == nil {
		return lit.Isnull
	}
	// The oneof is generated, but its pointer and decimal payload can still be
	// nil in malformed in-memory plans. New variants default to the old path.
	switch value := lit.Value.(type) {
	case *planpb.Literal_Decimal64Val:
		return value != nil && value.Decimal64Val != nil
	case *planpb.Literal_Decimal128Val:
		return value != nil && value.Decimal128Val != nil
	case *planpb.Literal_I8Val, *planpb.Literal_I16Val, *planpb.Literal_I32Val, *planpb.Literal_I64Val,
		*planpb.Literal_U8Val, *planpb.Literal_U16Val, *planpb.Literal_U32Val, *planpb.Literal_U64Val,
		*planpb.Literal_Dval, *planpb.Literal_Fval, *planpb.Literal_Sval, *planpb.Literal_Bval,
		*planpb.Literal_Dateval, *planpb.Literal_Timeval, *planpb.Literal_Datetimeval, *planpb.Literal_Timestampval,
		*planpb.Literal_Jsonval, *planpb.Literal_Defaultval, *planpb.Literal_UpdateVal, *planpb.Literal_EnumVal,
		*planpb.Literal_VecVal:
		return !reflect.ValueOf(value).IsNil()
	default:
		return false
	}
}

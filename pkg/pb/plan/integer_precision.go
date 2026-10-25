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
	"fmt"
	"math"
)

// ValidateIntegerPrecision is an admission check, including after plan RPC
// decoding. Derived nodes serialize recipes, not untrusted cached results.
// Zero edges mean a valid but unknown source and prohibit narrowing downstream.
func (q *Query) ValidateIntegerPrecision() error {
	if q == nil || q.IntegerPrecision == nil {
		return nil
	}
	g := q.IntegerPrecision
	bad := func(reason string) error { return fmt.Errorf("invalid integer precision graph: %s", reason) }
	if g.SourceBudget < 65 || g.SourceBudget > math.MaxUint32-2 {
		return bad("source budget")
	}
	if uint64(g.ParameterCount) > g.SourceBudget {
		return bad("parameter count")
	}
	if len(g.Outputs) != len(q.Headings) {
		return bad("output arity")
	}
	arithmetic := make([]bool, len(g.Nodes))
	for i, n := range g.Nodes {
		if n == nil {
			return bad("nil node")
		}
		for _, edge := range n.Inputs {
			if uint64(edge) > uint64(i) {
				return bad("non-topological edge")
			}
		}
		leaf := false
		eligible := false
		switch n.Kind {
		case IntegerPrecisionNode_DECLARED:
			leaf = true
			switch n.Precision {
			case 3, 5, 8, 10:
			case 19:
				if n.Unsigned {
					return bad("declared domain")
				}
			case 20:
				if !n.Unsigned {
					return bad("declared domain")
				}
			default:
				return bad("declared precision")
			}
		case IntegerPrecisionNode_SQL_LITERAL:
			leaf = true
			if n.Precision == 0 || uint64(n.Precision) > g.SourceBudget {
				return bad("literal precision")
			}
		case IntegerPrecisionNode_EXPLICIT_CAST, IntegerPrecisionNode_PARAMETER:
			leaf = true
			p := uint32(20)
			if n.Unsigned {
				p++
			}
			if n.Precision != p || n.Kind == IntegerPrecisionNode_PARAMETER && (n.ParamPos < 0 || uint64(n.ParamPos) >= uint64(g.ParameterCount)) {
				return bad("cast/parameter domain")
			}
		case IntegerPrecisionNode_ADD, IntegerPrecisionNode_MUL:
			if len(n.Inputs) != 2 || n.Unsigned {
				return bad("arithmetic recipe")
			}
			eligible = true
		case IntegerPrecisionNode_REFERENCE:
			if len(n.Inputs) != 1 || n.Unsigned {
				return bad("reference recipe")
			}
		case IntegerPrecisionNode_COMMON_INTEGER:
			if len(n.Inputs) == 0 {
				return bad("common arity")
			}
		default:
			return bad("node kind")
		}
		if leaf {
			if len(n.Inputs) != 0 {
				return bad("leaf arity")
			}
		} else {
			if n.Precision != 0 {
				return bad("derived result must be evaluated")
			}
			for _, edge := range n.Inputs {
				if edge != 0 {
					eligible = eligible || arithmetic[edge-1]
				}
			}
		}
		if n.Arithmetic != eligible {
			return bad("arithmetic provenance")
		}
		arithmetic[i] = eligible
	}
	for _, out := range g.Outputs {
		if out == nil || uint64(out.Node) > uint64(len(g.Nodes)) {
			return bad("output index")
		}
		switch out.RootClass {
		case IntegerPrecisionOutput_DECLARED_SOURCE, IntegerPrecisionOutput_DIRECT_FUNCTION, IntegerPrecisionOutput_SQL_REFERENCE, IntegerPrecisionOutput_SQL_LITERAL:
		default:
			return bad("root class")
		}
		if out.Node == 0 {
			continue
		}
		kind := g.Nodes[out.Node-1].Kind
		switch out.RootClass {
		case IntegerPrecisionOutput_DECLARED_SOURCE:
			if kind != IntegerPrecisionNode_DECLARED && kind != IntegerPrecisionNode_PARAMETER {
				return bad("source root")
			}
		case IntegerPrecisionOutput_DIRECT_FUNCTION:
			if kind != IntegerPrecisionNode_ADD && kind != IntegerPrecisionNode_MUL && kind != IntegerPrecisionNode_COMMON_INTEGER && kind != IntegerPrecisionNode_EXPLICIT_CAST {
				return bad("function root")
			}
		case IntegerPrecisionOutput_SQL_REFERENCE:
			if kind != IntegerPrecisionNode_REFERENCE && kind != IntegerPrecisionNode_COMMON_INTEGER {
				return bad("reference root")
			}
		case IntegerPrecisionOutput_SQL_LITERAL:
			if kind != IntegerPrecisionNode_SQL_LITERAL {
				return bad("literal root")
			}
		}
	}
	return nil
}

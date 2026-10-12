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
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	planbuilder "github.com/matrixorigin/matrixone/pkg/sql/plan"
)

// ExpandEmbeddedMOSteps gives every inlined shared-subtree occurrence its own
// scan node. The native importer inlines ReferenceRel; MO inputs are destructive
// streams, so those occurrences cannot share a read binding. The caller first
// validates the original candidate and uses this query for both export and the
// producer specifications. Flight retains its shared-step contract.
func ExpandEmbeddedMOSteps(query *planpb.Query) (*planpb.Query, error) {
	if query == nil || len(query.Steps) == 0 {
		return nil, moerr.NewInternalErrorNoCtx("substrait: embedded expansion requires a query root")
	}
	if len(query.Steps) == 1 {
		return query, nil
	}
	const maxExpandedNodes = 4096
	if len(query.Nodes) > maxExpandedNodes {
		return nil, notEligiblef(EligibilityPlanShape, "embedded shared-step expansion exceeds node bound")
	}
	result := planbuilder.DeepCopyQuery(query)
	visiting := make(map[int32]bool)
	expanded, reads := 0, 0
	var copyOccurrence func(int32, int, int) (int32, error)
	copyOccurrence = func(id int32, step, depth int) (int32, error) {
		if id < 0 || int(id) >= len(query.Nodes) || query.Nodes[id] == nil || query.Nodes[id].NodeId != id {
			return 0, moerr.NewInternalErrorNoCtx("substrait: malformed embedded shared-step node")
		}
		if visiting[id] {
			return 0, moerr.NewInternalErrorNoCtx("substrait: cyclic embedded shared-step query")
		}
		if expanded >= maxExpandedNodes || depth >= 256 {
			return 0, notEligiblef(EligibilityPlanShape, "embedded shared-step expansion exceeds node/depth bound")
		}
		expanded++
		visiting[id] = true
		defer delete(visiting, id)
		node := planbuilder.DeepCopyNode(query.Nodes[id])
		projectionType := planpb.Node_PROJECT
		if len(node.ProjectList) == 0 {
			projectionType = planpb.Node_FILTER // empty project is an identity
		}
		if node.NodeType == planpb.Node_SINK_SCAN {
			if len(node.SourceStep) != 1 || node.SourceStep[0] < 0 || int(node.SourceStep[0]) >= step || len(node.Children) != 0 {
				return 0, moerr.NewInternalErrorNoCtx("substrait: malformed embedded shared-step reference")
			}
			sourceStep := int(node.SourceStep[0])
			producer := query.Steps[sourceStep]
			if producer < 0 || int(producer) >= len(query.Nodes) || query.Nodes[producer] == nil || query.Nodes[producer].NodeType != planpb.Node_SINK {
				return 0, moerr.NewInternalErrorNoCtx("substrait: embedded shared-step reference requires a sink")
			}
			child, err := copyOccurrence(producer, sourceStep, depth+1)
			if err != nil {
				return 0, err
			}
			if len(node.FilterList) != 0 {
				return 0, notEligiblef(EligibilityPlanShape, "embedded sink scan has unsupported filters")
			}
			node.NodeType, node.SourceStep, node.Children = projectionType, nil, []int32{child}
		} else {
			for i, child := range node.Children {
				copied, err := copyOccurrence(child, step, depth+1)
				if err != nil {
					return 0, err
				}
				node.Children[i] = copied
			}
			if node.NodeType == planpb.Node_SINK {
				if len(node.FilterList) != 0 {
					return 0, notEligiblef(EligibilityPlanShape, "embedded sink has unsupported filters")
				}
				node.NodeType = projectionType
			}
		}
		if node.NodeType == planpb.Node_TABLE_SCAN {
			reads++
			if reads > 16 {
				return 0, notEligiblef(EligibilityPlanShape, "embedded shared-step expansion exceeds sixteen read bindings")
			}
		}
		node.NodeId = int32(len(result.Nodes))
		result.Nodes = append(result.Nodes, node)
		return node.NodeId, nil
	}
	root, err := copyOccurrence(query.Steps[len(query.Steps)-1], len(query.Steps)-1, 0)
	if err != nil {
		return nil, err
	}
	result.Steps = []int32{root}
	return result, nil
}

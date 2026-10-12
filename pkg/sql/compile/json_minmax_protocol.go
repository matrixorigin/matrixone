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

package compile

import (
	"github.com/matrixorigin/matrixone/pkg/pb/pipeline"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
)

func hasJSONMinMaxAggregate(node *plan.Node) bool {
	features, err := plan.RequiredRemoteExpressionFeatures(node)
	return err == nil && features.JSONMinMaxTypedComparison
}

// pipelineExpressionFeatures augments shared expression analysis with the
// aggregate identity retained after lowering. The same derived requirement
// reaches sender, destination and receiver fences without new wire state.
func pipelineExpressionFeatures(p *pipeline.Pipeline) (plan.RemoteExpressionFeatures, error) {
	features, err := plan.RequiredRemoteExpressionFeatures(p)
	if err == nil {
		features.JSONMinMaxTypedComparison = features.JSONMinMaxTypedComparison || pipelineUsesJSONMinMax(p)
	}
	return features, err
}

func pipelineUsesJSONMinMax(p *pipeline.Pipeline) bool {
	if p == nil {
		return false
	}
	for _, instruction := range p.InstructionList {
		if instruction == nil || instruction.Agg == nil {
			continue
		}
		for _, agg := range instruction.Agg.Aggs {
			if agg != nil && plan.IsJSONMinMaxAggregate(agg.Op, agg.Expr) {
				return true
			}
		}
	}
	for _, child := range p.Children {
		if pipelineUsesJSONMinMax(child) {
			return true
		}
	}
	return false
}

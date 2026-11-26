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
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/pipeline"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/aggexec"
	"github.com/stretchr/testify/require"
)

func TestGroupConcatTimeZoneFenceFindsForwardedAggregates(t *testing.T) {
	ts := &plan.Expr{Typ: plan.Type{Id: int32(types.T_timestamp)}}
	for _, distinct := range []bool{false, true} {
		agg := convertToPipelineAggregates([]aggexec.AggFuncExecExpression{
			aggexec.MakeAggFunctionExpression(aggexec.AggIdOfGroupConcat, distinct, []*plan.Expr{ts}, nil, 0),
		})
		child := &pipeline.Pipeline{InstructionList: []*pipeline.Instruction{
			nil, {}, {Agg: &pipeline.Group{Aggs: append([]*pipeline.Aggregate{nil}, agg...)}},
		}}
		forwarded := &pipeline.Pipeline{Children: []*pipeline.Pipeline{nil, child}}
		require.True(t, pipelineRequiresGroupConcatTimeZone(forwarded))
		agg[0].Expr = []*plan.Expr{nil, {Typ: plan.Type{Id: int32(types.T_datetime)}}}
		require.False(t, pipelineRequiresGroupConcatTimeZone(forwarded))
		agg[0].Expr = []*plan.Expr{ts}
		agg[0].Op = -1
		require.False(t, pipelineRequiresGroupConcatTimeZone(forwarded))
	}
}

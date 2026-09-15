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
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/aggexec"
	"github.com/stretchr/testify/require"
)

func TestPipelineRequiresJSONAggregateOpaqueValues(t *testing.T) {
	binaryValue := &planpb.Expr{Typ: planpb.Type{Id: int32(types.T_binary)}}
	textValue := &planpb.Expr{Typ: planpb.Type{Id: int32(types.T_varchar)}}
	array := &pipeline.Aggregate{Op: aggexec.AggIdOfJsonArrayAgg, Expr: []*planpb.Expr{binaryValue}}
	objectKeyOnly := &pipeline.Aggregate{Op: aggexec.AggIdOfJsonObjectAgg, Expr: []*planpb.Expr{binaryValue, textValue}}
	objectValue := &pipeline.Aggregate{Op: aggexec.AggIdOfJsonObjectAgg, Expr: []*planpb.Expr{textValue, binaryValue}}

	p := &pipeline.Pipeline{
		InstructionList: []*pipeline.Instruction{{
			Agg: &pipeline.Group{Aggs: []*pipeline.Aggregate{objectKeyOnly}},
		}},
		Children: []*pipeline.Pipeline{{
			InstructionList: []*pipeline.Instruction{{
				Agg: &pipeline.Group{Aggs: []*pipeline.Aggregate{array}},
			}},
		}},
	}
	required, err := jsonAggregateOpaqueRequirement(p)
	require.NoError(t, err)
	require.True(t, required)

	p.InstructionList[0].Agg.Aggs[0] = objectValue
	p.Children = nil
	required, err = jsonAggregateOpaqueRequirement(p)
	require.NoError(t, err)
	require.True(t, required)

	p.InstructionList[0].Agg.Aggs[0] = objectKeyOnly
	required, err = jsonAggregateOpaqueRequirement(p)
	require.NoError(t, err)
	require.False(t, required)
}

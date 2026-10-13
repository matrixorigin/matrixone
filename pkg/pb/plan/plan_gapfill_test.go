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

package plan_test

import (
	"testing"

	"github.com/gogo/protobuf/proto"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

func TestNodeGapFillBoundsWireRoundTrip(t *testing.T) {
	bound := func(value int64) *plan.Expr {
		return &plan.Expr{
			Typ: plan.Type{Id: int32(types.T_timestamp), Scale: 6},
			Expr: &plan.Expr_Lit{Lit: &plan.Literal{
				Value: &plan.Literal_Timestampval{Timestampval: value},
			}},
		}
	}
	original := &plan.Node{
		NodeType:     plan.Node_TIME_WINDOW,
		GapFillMode:  plan.Node_GAP_FILL_PARTITION,
		GapFillStart: bound(100),
		GapFillEnd:   bound(200),
	}

	data, err := proto.Marshal(original)
	require.NoError(t, err)
	var decoded plan.Node
	require.NoError(t, proto.Unmarshal(data, &decoded))
	require.True(t, proto.Equal(original, &decoded))
}

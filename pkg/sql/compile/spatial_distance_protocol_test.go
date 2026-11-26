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

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/pipeline"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
)

func remoteSpatialDistanceProtocolPipeline(functionID, overloadID int32) *pipeline.Pipeline {
	return &pipeline.Pipeline{InstructionList: []*pipeline.Instruction{{
		ProjectList: []*planpb.Expr{{
			Typ: planpb.Type{Id: int32(types.T_float64)},
			Expr: &planpb.Expr_F{F: &planpb.Function{
				Func: &planpb.ObjectRef{
					Obj:     function.EncodeOverloadID(functionID, overloadID),
					ObjName: "spatial-distance",
				},
			}},
		}},
	}}}
}

func TestSpatialDistanceProtocolFeatureIDsRemainStable(t *testing.T) {
	for _, tc := range []struct {
		name     string
		id       int32
		overload int32
		want     bool
	}{
		{name: "distance", id: function.ST_DISTANCE, overload: 0, want: false},
		{name: "frechet legacy", id: function.ST_FRECHETDISTANCE, overload: 0, want: false},
		{name: "frechet geodetic", id: function.ST_FRECHETDISTANCE, overload: 4, want: true},
		{name: "hausdorff legacy", id: function.ST_HAUSDORFFDISTANCE, overload: 1, want: false},
		{name: "hausdorff geodetic", id: function.ST_HAUSDORFFDISTANCE, overload: 4, want: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			features, err := planpb.RequiredRemoteExpressionFeatures(
				remoteSpatialDistanceProtocolPipeline(tc.id, tc.overload))
			require.NoError(t, err)
			require.Equal(t, tc.want, features.SpatialDistanceSemantics)
		})
	}
}

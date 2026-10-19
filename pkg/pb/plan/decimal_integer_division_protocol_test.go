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
	"github.com/stretchr/testify/require"
	"testing"
)

func TestDecimalIntegerDivisionRemoteFeature(t *testing.T) {
	for _, tc := range []struct {
		name               string
		function, overload int64
		width, scale       int32
		want               bool
	}{
		{"unsigned", 14, 1, 7, 0, true}, {"signed display", 14, 0, 8, 0, true},
		{"legacy", 14, 0, 64, -1, false}, {"unrelated function", 13, 0, 8, 0, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			expr := &Expr{Typ: Type{Id: 24, Width: tc.width, Scale: tc.scale}, Expr: &Expr_F{F: &Function{Func: &ObjectRef{Obj: tc.function<<32 | tc.overload}}}}
			bytes, err := expr.Marshal()
			require.NoError(t, err)
			var decoded Expr
			require.NoError(t, decoded.Unmarshal(bytes))
			features, err := RequiredRemoteExpressionFeatures(&decoded)
			require.NoError(t, err)
			require.Equal(t, tc.want, features.DecimalIntegerDivisionSemantics)
			if tc.want {
				require.True(t, features.Any())
			}
		})
	}
}

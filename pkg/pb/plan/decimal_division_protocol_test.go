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
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDecimalDivisionRemoteFeature(t *testing.T) {
	// Wire type IDs 32..34 are DECIMAL64/128/256; 31 is FLOAT64.
	for _, oid := range []int32{32, 33, 34, 31} {
		for _, overload := range []int32{0, 1} {
			expr := &Expr{Typ: Type{Id: oid}, Expr: &Expr_F{F: &Function{Func: &ObjectRef{Obj: int64(13)<<32 | int64(overload)}}}}
			features, err := RequiredRemoteExpressionFeatures(expr)
			require.NoError(t, err)
			require.Equal(t, oid >= 32 && oid <= 34 && overload == 0, features.DecimalDivisionSemantics, "oid=%v overload=%d", oid, overload)
		}
	}
}

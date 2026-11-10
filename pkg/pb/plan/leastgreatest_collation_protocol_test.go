// Copyright 2026 Matrix Origin
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// http://www.apache.org/licenses/LICENSE-2.0
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

func TestLeastGreatestCollationRemoteFeature(t *testing.T) {
	// Registry IDs: GREATEST=104, LEAST=122. Released overloads 0..3
	// retain their contracts; only appended collation overload 4 needs fencing.
	for _, id := range []int64{104, 122, 999} {
		for overload := range int64(5) {
			expr := &Expr{Typ: Type{Id: 61}, Expr: &Expr_F{F: &Function{
				Func: &ObjectRef{Obj: id<<32 | overload, ObjName: "least"},
			}}}
			data, err := expr.Marshal()
			require.NoError(t, err)
			decoded := &Expr{}
			require.NoError(t, decoded.Unmarshal(data))
			for _, owner := range []any{decoded, []*Expr{decoded}, &Query{Nodes: []*Node{{ProjectList: []*Expr{decoded}}}}} {
				features, err := RequiredRemoteExpressionFeatures(owner)
				require.NoError(t, err)
				require.Equal(t, (id == 104 || id == 122) && overload == 4, features.Any(), "id=%d overload=%d", id, overload)
			}
		}
	}
}

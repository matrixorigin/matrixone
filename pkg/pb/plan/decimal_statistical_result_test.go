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

func TestDecimalStatisticalResultFeatures(t *testing.T) {
	for _, id := range []int64{180, 181, 199, 200} {
		for _, distinct := range []uint64{0, 1 << 63} {
			for _, input := range []int32{32, 33, 34, 31, 23} {
				for _, result := range []int32{31, 33} {
					expr := &Expr{Typ: Type{Id: result}, Expr: &Expr_F{F: &Function{Func: &ObjectRef{Obj: int64(uint64(id<<32) | distinct)}, Args: []*Expr{{Typ: Type{Id: input}}}}}}
					window := &Expr{Typ: expr.Typ, Expr: &Expr_W{W: &WindowSpec{WindowFunc: expr}}}
					for _, owner := range []any{expr, &Query{Nodes: []*Node{{WinSpecList: []*Expr{window}}}}} {
						features, err := RequiredRemoteExpressionFeatures(owner)
						require.NoError(t, err)
						affected := input == 32 || input == 33
						require.Equal(t, affected, features.DecimalStatisticalResultContracts)
						require.Equal(t, affected && result != 31, features.InvalidDecimalStatisticalResultContract)
						if affected {
							require.True(t, features.Any())
						}
					}
				}
			}
		}
	}
}

// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package function

import (
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/stretchr/testify/require"
)

// MySQL treats TRUE/FALSE as 1/0 next to a FLOAT/DOUBLE, the same way it
// already works next to an integer: arithmetic and comparison must bind, with
// both sides evaluated as DOUBLE.
func TestFloatBoolArithmeticAndComparisonBind(t *testing.T) {
	ctx := context.Background()
	boolType := types.T_bool.ToType()
	for _, ft := range []types.Type{types.T_float32.ToType(), types.T_float64.ToType()} {
		for _, name := range []string{"+", "-", "*", "/", "%", "div", "=", "<>", ">", "<", ">=", "<=", "<=>"} {
			for _, args := range [][]types.Type{{ft, boolType}, {boolType, ft}} {
				resolved, err := GetFunctionByName(ctx, name, args)
				require.NoError(t, err, "%s %s %s", args[0].Oid, name, args[1].Oid)
				require.True(t, resolved.needCast, "%s %s", ft.Oid, name)
				require.Equal(t, types.T_float64, resolved.targetTypes[0].Oid, "%s %s", ft.Oid, name)
				require.Equal(t, types.T_float64, resolved.targetTypes[1].Oid, "%s %s", ft.Oid, name)
			}
		}
	}
}

// Copyright 2022 Matrix Origin
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

package binary

import (
	"fmt"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func TestShowVisibleBin(t *testing.T) {
	var out *vector.Vector
	tp := types.T_varchar.ToType()
	buf, err := types.Encode(&tp)
	require.NoError(t, err)
	require.NotNil(t, buf)
	toCode := typNormal
	c1 := makeVec(buf, uint8(toCode))
	out, err = ShowVisibleBin(c1, testutil.NewProc())
	require.NoError(t, err)
	require.NotNil(t, out)
	require.Equal(t, 1, out.Length())
	require.Equal(t, []byte(tp.String()), vector.MustBytesCol(out)[0])
	toCode = typWithLen
	c2 := makeVec(buf, uint8(toCode))
	out, err = ShowVisibleBin(c2, testutil.NewProc())
	require.NoError(t, err)
	require.NotNil(t, out)
	require.Equal(t, 1, out.Length())
	require.Equal(t, fmt.Sprintf("%s(%d)", tp.String(), tp.Width), vector.MustStrCol(out)[0])

	update := new(plan.OnUpdate)
	update.OriginString = "update"
	update.Expr = &plan.Expr{}
	buf, err = types.Encode(update)
	require.NoError(t, err)
	require.NotNil(t, buf)
	toCode = onUpdateExpr
	c3 := makeVec(buf, uint8(toCode))
	out, err = ShowVisibleBin(c3, testutil.NewProc())
	require.NoError(t, err)
	require.NotNil(t, out)
	require.Equal(t, 1, out.Length())
	require.Equal(t, update.OriginString, vector.MustStrCol(out)[0])

	def := new(plan.Default)
	def.OriginString = "default"
	def.Expr = &plan.Expr{}
	buf, err = types.Encode(def)
	require.NoError(t, err)
	require.NotNil(t, buf)
	toCode = defaultExpr
	c4 := makeVec(buf, uint8(toCode))
	out, err = ShowVisibleBin(c4, testutil.NewProc())
	require.NoError(t, err)
	require.NotNil(t, out)
	require.Equal(t, 1, out.Length())
	require.Equal(t, def.OriginString, vector.MustStrCol(out)[0])

}
func makeVec(buf []byte, toCode uint8) []*vector.Vector {
	vec := vector.NewConstBytes(types.T_varchar.ToType(), buf, 1, testutil.TestUtilMp)
	vec2 := vector.NewConstFixed(types.T_uint8.ToType(), toCode, 1, testutil.TestUtilMp)
	return []*vector.Vector{vec, vec2}
}

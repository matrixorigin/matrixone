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

func TestIntegerAddMulExecutionDomains(t *testing.T) {
	ints := []types.T{types.T_int8, types.T_int16, types.T_int32, types.T_int64, types.T_uint8, types.T_uint16, types.T_uint32, types.T_uint64}
	for _, left := range ints {
		for _, right := range ints {
			for _, op := range []string{"+", "*"} {
				t.Run(left.String()+op+right.String(), func(t *testing.T) {
					resolved, err := GetFunctionByName(context.Background(), op, []types.Type{left.ToType(), right.ToType()})
					require.NoError(t, err)
					want := types.T_int64
					if left.IsUnsignedInt() || right.IsUnsignedInt() {
						want = types.T_uint64
					}
					require.Equal(t, want, resolved.GetReturnType().Oid)
					cast, ok := resolved.ShouldDoImplicitTypeCast()
					if left == right && (left == types.T_int64 || left == types.T_uint64) {
						require.False(t, ok)
						cast = []types.Type{left.ToType(), right.ToType()}
					} else {
						require.True(t, ok)
					}
					require.Len(t, cast, 2)
					for i, oid := range []types.T{left, right} {
						target := types.T_int64
						if oid.IsUnsignedInt() {
							target = types.T_uint64
						}
						require.Equal(t, target, cast[i].Oid)
					}
					overload := int32(0)
					if left.IsSignedInt() != right.IsSignedInt() {
						overload = 2
					}
					require.Equal(t, overload, resolved.overloadId)
					l, r, result, ok := ResolveNumericBinaryTypes(op, left.ToType(), right.ToType(), nil)
					require.True(t, ok)
					require.Equal(t, resolved.GetReturnType(), result)
					require.Equal(t, cast[0].Oid, l.Oid)
					require.Equal(t, cast[1].Oid, r.Oid)
				})
			}
		}
	}
	// Independent shared-domain operators retain their own signatures.
	for _, op := range []string{"-", "%"} {
		f, err := GetFunctionByName(context.Background(), op, []types.Type{types.T_int8.ToType(), types.T_int8.ToType()})
		require.NoError(t, err)
		require.Equal(t, types.T_int8, f.GetReturnType().Oid)
	}
}

func TestIntegerLogicalPrecisionSources(t *testing.T) {
	require.Equal(t, uint64(8), IntegerDeclaredPrecision(types.New(types.T_int32, 24, 0)).Raw)
	require.Equal(t, uint64(19), IntegerDeclaredPrecision(types.T_int64.ToType()).Raw)
	require.Equal(t, uint64(20), IntegerCastPrecision(false).Raw)
	k := IntegerLogicalPrecision{Raw: 70, Known: true}
	one := IntegerLogicalPrecision{Raw: 1, Known: true}
	add := IntegerAddMulPrecision(false, k, one, true, true)
	require.Equal(t, uint64(72), add.DisplayLength())
	require.Equal(t, uint64(66), IntegerAddMulPrecision(true, k, one, true, true).DisplayLength())
	require.Equal(t, uint64(67), IntegerAddMulPrecision(false, add, one, false, true).DisplayLength())
	require.False(t, IntegerAddMulPrecision(false, k, IntegerLogicalPrecision{}, true, false).Known)
}

// Copyright 2021 Matrix Origin
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

package vector

import (
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/stretchr/testify/require"
	"testing"
)

func BenchmarkGetStrValue1(b *testing.B) {
	mp := mpool.MustNewZeroNoFixed()

	vecSize := uint64(50000)
	vec := NewVec(types.T_varchar.ToType())
	for i := uint64(0); i < vecSize; i++ {
		err := appendOneBytes(vec, []byte("x"), false, mp)
		require.NoError(b, err)
	}

	g1 := GenerateFunctionStrParameter(vec)

	vv, nn := []byte(nil), false
	for i := 0; i < b.N; i++ {
		for j := uint64(0); j < vecSize; j++ {
			v, n := g1.GetStrValue(j)
			vv, nn = v, n
		}
	}
	_, _ = vv, nn
}

func BenchmarkGetStrValue2(b *testing.B) {
	mp := mpool.MustNewZeroNoFixed()

	vecSize := uint64(50000)
	t2 := types.T_varchar.ToType()
	t2.Width = 10
	vec2 := NewVec(t2)
	for i := uint64(0); i < vecSize; i++ {
		err := appendOneBytes(vec2, []byte("x"), false, mp)
		require.NoError(b, err)
	}

	g2 := GenerateFunctionStrParameter(vec2)

	vv, nn := []byte(nil), false
	for i := 0; i < b.N; i++ {
		for j := uint64(0); j < vecSize; j++ {
			v, n := g2.GetStrValue(j)
			vv, nn = v, n
		}
	}
	_, _ = vv, nn
}

func TestPreExtendAndReset(t *testing.T) {
	mp := mpool.MustNewZeroNoFixed()

	wrapper := NewFunctionResultWrapper(types.T_bool.ToType(), mp)

	result := MustFunctionResult[bool](wrapper)
	fmt.Printf("length is %d, capacity is %d\n", result.vec.Length(), result.vec.Capacity())

	require.NoError(t, wrapper.PreExtendAndReset(10))
	require.Equal(t, 10, len(result.cols))
	require.Equal(t, 10, result.vec.Length())
	fmt.Printf("length is %d, capacity is %d\n", result.vec.Length(), result.vec.Capacity())

	lastCapacity := result.vec.Capacity()
	if lastCapacity > 20 {
		require.NoError(t, wrapper.PreExtendAndReset(20))
		require.Equal(t, 20, len(result.cols))
		require.Equal(t, 20, result.vec.Length())
		require.Equal(t, lastCapacity, result.vec.Capacity())
	} else if lastCapacity > 11 {
		nextLength := lastCapacity - 1
		require.NoError(t, wrapper.PreExtendAndReset(nextLength))
		require.Equal(t, nextLength, len(result.cols))
		require.Equal(t, nextLength, result.vec.Length())
		require.Equal(t, lastCapacity, result.vec.Capacity())
	} else {
		require.NoError(t, wrapper.PreExtendAndReset(20))
		require.Equal(t, 20, len(result.cols))
		require.Equal(t, 20, result.vec.Length())
	}
	fmt.Printf("length is %d, capacity is %d\n", result.vec.Length(), result.vec.Capacity())

	wrapper.Free()
	require.Equal(t, int64(0), mp.CurrNB())
}

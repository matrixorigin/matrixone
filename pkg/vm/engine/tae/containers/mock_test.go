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

package containers

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/stretchr/testify/require"
)

func TestAppendMockFloatsHonorsUnique(t *testing.T) {
	t.Run("resample collisions", func(t *testing.T) {
		values := []float32{1, 1, 2, 2, 3}
		next := 0
		vec := MakeVector(types.T_float32.ToType(), common.DefaultAllocator)
		defer vec.Close()

		appendMockFloats(vec, 3, true, func() float32 {
			value := values[next]
			next++
			return value
		})

		require.Equal(t, 3, vec.Length())
		require.Equal(t, float32(1), vec.Get(0))
		require.Equal(t, float32(2), vec.Get(1))
		require.Equal(t, float32(3), vec.Get(2))
		require.Equal(t, len(values), next)
	})

	t.Run("preserve duplicates when allowed", func(t *testing.T) {
		values := []float64{1, 1, 2}
		next := 0
		vec := MakeVector(types.T_float64.ToType(), common.DefaultAllocator)
		defer vec.Close()

		appendMockFloats(vec, len(values), false, func() float64 {
			value := values[next]
			next++
			return value
		})

		require.Equal(t, len(values), vec.Length())
		require.Equal(t, float64(1), vec.Get(0))
		require.Equal(t, float64(1), vec.Get(1))
		require.Equal(t, float64(2), vec.Get(2))
		require.Equal(t, len(values), next)
	})
}

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

package vector

import (
	"bytes"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/stretchr/testify/require"
)

// TestSetBytesAtSelfAliasWithGrowth verifies that SetBytesAt handles self-aliasing
// correctly when the operation triggers area growth, preventing use-after-free bugs.
func TestSetBytesAtSelfAliasWithGrowth(t *testing.T) {
	mp := mpool.MustNewZeroNoFixed()
	defer mpool.DeleteMPool(mp)

	// Create a VARCHAR vector with controlled capacity to force growth
	vec := NewVec(types.T_varchar.ToType())
	require.NoError(t, vec.PreExtend(10, mp))
	vec.SetLength(10)

	// Fill with small inline values first (won't trigger growth)
	for i := 0; i < 5; i++ {
		require.NoError(t, SetBytesAt(vec, i, []byte("x"), mp))
	}

	// Add a large value at index 5 that will be copied
	largeValue := bytes.Repeat([]byte("a"), 200)
	require.NoError(t, SetBytesAt(vec, 5, largeValue, mp))

	// Now perform self-alias copy from index 5 to index 6, which should trigger growth
	// This is the critical case: GetBytesAt(5) points into vec.area, and SetBytesAt
	// may grow that area before copying, invalidating the source slice.
	srcBytes := vec.GetBytesAt(5)
	require.Equal(t, 200, len(srcBytes))
	require.Equal(t, largeValue, srcBytes)

	// This call used to crash with SIGSEGV in BuildVarlenaNoInline
	require.NoError(t, SetBytesAt(vec, 6, srcBytes, mp))

	// Verify both slots have the correct value
	require.Equal(t, largeValue, vec.GetBytesAt(5))
	require.Equal(t, largeValue, vec.GetBytesAt(6))
}

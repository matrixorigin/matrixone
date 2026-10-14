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

package balanced

import (
	"math"
	"testing"

	"github.com/stretchr/testify/require"
)

// A centroid sum that overflows the element type is rejected: the sum of two float32 MaxFloat32
// points is +Inf, and dividing that by the count gives a +Inf centroid that no distance to it can
// be read from. Clustering fails rather than continuing from one.
func TestMeanRejectsSumOverflow(t *testing.T) {
	data := [][]float32{{math.MaxFloat32, 1}, {math.MaxFloat32, 3}}
	out := make([]float32, 2)

	err := computeMeanFromIndicesInPlace(data, []int{0, 1}, out)
	require.Error(t, err)
	require.Contains(t, err.Error(), "centroid sum overflows")

	out = make([]float32, 2)
	err = computeMeanFromIndicesAndAssignInPlace(data, []int{0, 1}, []int{7, 7}, 7, out)
	require.Error(t, err)
	require.Contains(t, err.Error(), "centroid sum overflows")

	// The ordinary path is unchanged.
	small := [][]float32{{1, 2}, {3, 4}}
	out = make([]float32, 2)
	require.NoError(t, computeMeanFromIndicesInPlace(small, []int{0, 1}, out))
	require.Equal(t, []float32{2, 3}, out)

	out = make([]float32, 2)
	require.NoError(t, computeMeanFromIndicesAndAssignInPlace(small, []int{0, 1}, []int{7, 7}, 7, out))
	require.Equal(t, []float32{2, 3}, out)

	// An empty selection stays a no-op.
	require.NoError(t, computeMeanFromIndicesInPlace(small, nil, out))
}

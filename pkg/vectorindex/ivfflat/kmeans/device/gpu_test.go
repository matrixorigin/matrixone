//go:build gpu

// Copyright 2023 Matrix Origin
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

package device

import (
	"fmt"
	"math/rand/v2"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/vectorindex/metric"
	"github.com/stretchr/testify/require"
)

func TestGpu(t *testing.T) {

	dim := 128
	dsize := 1024
	nlist := 128
	vecs := make([][]float32, dsize)
	for i := range vecs {
		vecs[i] = make([]float32, dim)
		for j := range vecs[i] {
			vecs[i][j] = rand.Float32()
		}
	}

	c, err := NewKMeans[float32](vecs, nlist, 10, 0, metric.Metric_L2Distance, 0, false, 0)
	require.NoError(t, err)

	centers, err := c.Cluster()
	require.NoError(t, err)

	centroids, ok := centers.([][]float32)
	require.True(t, ok)

	for k, center := range centroids {
		fmt.Printf("center[%d] = %v\n", k, center)
	}
}

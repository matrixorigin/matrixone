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

package metric

import (
	"math/rand"
	"testing"
)

func BenchmarkPairWiseDistance(b *testing.B) {
	nX, nY, dim := 8192, 1, 1024
	x := make([][]float32, nX)
	y := make([][]float32, nY)
	for i := range x {
		x[i] = make([]float32, dim)
		for j := range x[i] {
			x[i][j] = rand.Float32()
		}
	}
	for i := range y {
		y[i] = make([]float32, dim)
		for j := range y[i] {
			y[i][j] = rand.Float32()
		}
	}

	b.Run("PairWiseDistance", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_, _ = PairWiseDistance(x, y, Metric_L2sqDistance)
		}
	})

	b.Run("GoPairWiseDistance", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_, _ = GoPairWiseDistance(x, y, Metric_L2sqDistance)
		}
	})
}

func BenchmarkPairWiseDistanceLarge(b *testing.B) {
	nX, nY, dim := 8192, 50, 1024
	x := make([][]float32, nX)
	y := make([][]float32, nY)
	for i := range x {
		x[i] = make([]float32, dim)
		for j := range x[i] {
			x[i][j] = rand.Float32()
		}
	}
	for i := range y {
		y[i] = make([]float32, dim)
		for j := range y[i] {
			y[i][j] = rand.Float32()
		}
	}

	b.Run("PairWiseDistance-Large", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_, _ = PairWiseDistance(x, y, Metric_L2sqDistance)
		}
	})

	b.Run("GoPairWiseDistance-Large", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_, _ = GoPairWiseDistance(x, y, Metric_L2sqDistance)
		}
	})
}

func BenchmarkPairwiseDistanceAsync(b *testing.B) {
	nX, nY, dim := 8192, 50, 1024
	x := make([][]float32, nX)
	y := make([][]float32, nY)
	for i := range x {
		x[i] = make([]float32, dim)
		for j := range x[i] {
			x[i][j] = rand.Float32()
		}
	}
	for i := range y {
		y[i] = make([]float32, dim)
		for j := range y[i] {
			y[i][j] = rand.Float32()
		}
	}

	b.Run("Sync", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_, _ = PairWiseDistance(x, y, Metric_L2sqDistance)
		}
	})

	b.Run("Async", func(b *testing.B) {
		dist := make([]float32, nX*nY)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			handle, err := PairwiseDistanceLaunch(x, y, Metric_L2sqDistance, dist, GPUThresholdSync)
			if err != nil {
				b.Fatal(err)
			}
			_, err = PairwiseDistanceWait(handle, Metric_L2sqDistance)
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

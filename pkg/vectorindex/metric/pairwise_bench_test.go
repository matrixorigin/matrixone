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
	nX, nY, dim := 100, 100, 128
	x := make([]float32, nX*dim)
	y := make([]float32, nY*dim)
	for i := range x {
		x[i] = rand.Float32()
	}
	for i := range y {
		y[i] = rand.Float32()
	}

	b.Run("PairWiseDistance", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_, _ = PairWiseDistance(x, nX, y, nY, dim, Metric_L2sqDistance, 0)
		}
	})

	b.Run("GoPairWiseDistance", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_, _ = GoPairWiseDistance(x, nX, y, nY, dim, Metric_L2sqDistance)
		}
	})
}

func BenchmarkPairWiseDistanceLarge(b *testing.B) {
	nX, nY, dim := 10000, 5, 1024
	x := make([]float32, nX*dim)
	y := make([]float32, nY*dim)
	for i := range x {
		x[i] = rand.Float32()
	}
	for i := range y {
		y[i] = rand.Float32()
	}

	b.Run("PairWiseDistance-Large", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_, _ = PairWiseDistance(x, nX, y, nY, dim, Metric_L2sqDistance, 0)
		}
	})

	b.Run("GoPairWiseDistance-Large", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_, _ = GoPairWiseDistance(x, nX, y, nY, dim, Metric_L2sqDistance)
		}
	})
}

// Copyright 2024 Matrix Origin
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

/*
Benchmark_L2Distance/L2_Distance-10         	                    1570082	      1014 ns/op
Benchmark_L2Distance/Normalize_L2-10        	                    1277733	      1064 ns/op
Benchmark_L2Distance/L2_Distance(v1,_NormalizeL2)-10         	     589376	      1883 ns/op
*/
func Benchmark_L2Distance(b *testing.B) {
	dim := 1024

	b.Run("L2 Distance float64", func(b *testing.B) {
		v1, v2 := randomVectors[float64](b.N, dim), randomVectors[float64](b.N, dim)
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			_, _ = L2Distance[float64](v1[i], v2[i])
		}
	})

	b.Run("L2 Distance float32", func(b *testing.B) {
		v1, v2 := randomVectors[float32](b.N, dim), randomVectors[float32](b.N, dim)
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			_, _ = L2Distance[float32](v1[i], v2[i])
		}
	})

	/*
	b.Run("Normalize L2 float64", func(b *testing.B) {
		v1 := randomVectors[float64](b.N, dim)
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			res := make([]float64, dim)
			_ = NormalizeL2[float64](v1[i], res)
		}
	})

	b.Run("Normalize L2 float32", func(b *testing.B) {
		v1 := randomVectors[float32](b.N, dim)
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			res := make([]float32, dim)
			_ = NormalizeL2[float32](v1[i], res)
		}
	})

	b.Run("L2 Distance(v1, NormalizeL2) float64", func(b *testing.B) {
		v1, v2 := randomVectors[float64](b.N, dim), randomVectors[float64](b.N, dim)
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			res := make([]float64, dim)
			_ = NormalizeL2[float64](v2[i], res)
			_, _ = L2Distance[float64](v1[i], res)
		}
	})
	*/
}

func Benchmark_L2DistanceSq(b *testing.B) {
	dim := 1024

	b.Run("L2 DistanceSq float64", func(b *testing.B) {
		v1, v2 := randomVectors[float64](b.N, dim), randomVectors[float64](b.N, dim)
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			_, _ = L2DistanceSq[float64](v1[i], v2[i])
		}
	})

	b.Run("L2 DistanceSq float32", func(b *testing.B) {
		v1, v2 := randomVectors[float32](b.N, dim), randomVectors[float32](b.N, dim)
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			_, _ = L2DistanceSq[float32](v1[i], v2[i])
		}
	})
}

func Benchmark_L1Distance(b *testing.B) {
	dim := 1024

	b.Run("L1 Distance float64", func(b *testing.B) {
		v1, v2 := randomVectors[float64](b.N, dim), randomVectors[float64](b.N, dim)
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			_, _ = L1Distance[float64](v1[i], v2[i])
		}
	})

	b.Run("L1 Distance float32", func(b *testing.B) {
		v1, v2 := randomVectors[float32](b.N, dim), randomVectors[float32](b.N, dim)
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			_, _ = L1Distance[float32](v1[i], v2[i])
		}
	})
}

func Benchmark_InnerProduct(b *testing.B) {
	dim := 1024

	b.Run("Inner Product float64", func(b *testing.B) {
		v1, v2 := randomVectors[float64](b.N, dim), randomVectors[float64](b.N, dim)
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			_, _ = InnerProduct[float64](v1[i], v2[i])
		}
	})

	b.Run("Inner Product float32", func(b *testing.B) {
		v1, v2 := randomVectors[float32](b.N, dim), randomVectors[float32](b.N, dim)
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			_, _ = InnerProduct[float32](v1[i], v2[i])
		}
	})
}

func Benchmark_CosineDistance(b *testing.B) {
	dim := 1024

	b.Run("Cosine Distance float64", func(b *testing.B) {
		v1, v2 := randomVectors[float64](b.N, dim), randomVectors[float64](b.N, dim)
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			_, _ = CosineDistance[float64](v1[i], v2[i])
		}
	})

	b.Run("Cosine Distance float32", func(b *testing.B) {
		v1, v2 := randomVectors[float32](b.N, dim), randomVectors[float32](b.N, dim)
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			_, _ = CosineDistance[float32](v1[i], v2[i])
		}
	})
}

func Benchmark_CosineSimilarity(b *testing.B) {
	dim := 1024

	b.Run("Cosine Similarity float64", func(b *testing.B) {
		v1, v2 := randomVectors[float64](b.N, dim), randomVectors[float64](b.N, dim)
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			_, _ = CosineSimilarity[float64](v1[i], v2[i])
		}
	})

	b.Run("Cosine Similarity float32", func(b *testing.B) {
		v1, v2 := randomVectors[float32](b.N, dim), randomVectors[float32](b.N, dim)
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			_, _ = CosineSimilarity[float32](v1[i], v2[i])
		}
	})
}

func Benchmark_SphericalDistance(b *testing.B) {
	dim := 1024

	b.Run("Spherical Distance float64", func(b *testing.B) {
		v1, v2 := randomVectors[float64](b.N, dim), randomVectors[float64](b.N, dim)
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			_, _ = SphericalDistance[float64](v1[i], v2[i])
		}
	})

	b.Run("Spherical Distance float32", func(b *testing.B) {
		v1, v2 := randomVectors[float32](b.N, dim), randomVectors[float32](b.N, dim)
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			_, _ = SphericalDistance[float32](v1[i], v2[i])
		}
	})
}

/*
func Benchmark_ScaleInPlace(b *testing.B) {
	dim := 1024

	b.Run("ScaleInPlace float64", func(b *testing.B) {
		v1 := randomVectors[float64](b.N, dim)
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			ScaleInPlace[float64](v1[i], 0.5)
		}
	})

	b.Run("ScaleInPlace float32", func(b *testing.B) {
		v1 := randomVectors[float32](b.N, dim)
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			ScaleInPlace[float32](v1[i], 0.5)
		}
	})
}
*/

func randomVectors[T float32 | float64](size, dim int) [][]T {
	vectors := make([][]T, size)
	for i := range vectors {
		vectors[i] = make([]T, dim)
		for j := 0; j < dim; j++ {
			vectors[i][j] = T(rand.Float64())
		}
	}
	return vectors
}

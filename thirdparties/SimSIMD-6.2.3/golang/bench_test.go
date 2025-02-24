package simsimd

import (
	"math"
	"math/rand"
	"testing"
)

func cosineSimilarity(a, b []float32) float32 {
	var dotProduct float32
	var normA, normB float32
	for i := range a {
		dotProduct += a[i] * b[i]
		normA += a[i] * a[i]
		normB += b[i] * b[i]
	}
	return dotProduct / (float32(math.Sqrt(float64(normA))) * float32(math.Sqrt(float64(normB))))
}

func generateRandomVector(dim int) []float32 {
	vec := make([]float32, dim)
	for i := range vec {
		vec[i] = rand.Float32()
	}
	return vec
}

func BenchmarkCosineSimilarityNative(b *testing.B) {
    first, second := generateRandomVector(1536), generateRandomVector(1536)
    for i := 0; i < b.N; i++ {
        cosineSimilarity(first, second)
    }
}

func BenchmarkCosineSimilaritySIMD(b *testing.B) {
    first, second := generateRandomVector(1536), generateRandomVector(1536)
    for i := 0; i < b.N; i++ {
        CosineF32(first, second)
    }
}
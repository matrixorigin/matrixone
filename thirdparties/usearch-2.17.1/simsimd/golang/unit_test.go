package simsimd

import (
	"math"
	"testing"
)

func TestCosineI8(t *testing.T) {
	a := []int8{1, 0}
	b := []int8{0, 1}

	result := CosineI8(a, b)
	expected := float32(1.0) // Cosine similarity of orthogonal vectors is 0
	if math.Abs(float64(result-expected)) > 1e-3 {
		t.Errorf("Expected %v, got %v", expected, result)
	}
}

func TestCosineF32(t *testing.T) {
	a := []float32{1, 0}
	b := []float32{0, 1}

	result := CosineF32(a, b)
	expected := float32(1.0) // Cosine similarity of orthogonal vectors is 0
	if math.Abs(float64(result-expected)) > 1e-3 {
		t.Errorf("Expected %v, got %v", expected, result)
	}
}

func TestVectorLengthMismatch(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Errorf("The code did not panic")
		}
	}()

	a := []int8{1, 0}
	b := []int8{0}
	_ = CosineI8(a, b) // This should panic
}

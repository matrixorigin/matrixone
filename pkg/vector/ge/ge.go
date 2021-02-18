package ge

import (
	"matrixbase/pkg/container/vector"
)

var (
	i64Ge    func([]int64, []int64, []int64) []int64
	f64Ge    func([]float64, []float64, []int64) []int64
	i64GeOne func(int64, []int64, []int64) []int64
	f64GeOne func(float64, []float64, []int64) []int64

	sGe    func(*vector.Bytes, *vector.Bytes, []int64) []int64
	sGeOne func([]byte, *vector.Bytes, []int64) []int64
)

package gt

import (
	"matrixbase/pkg/container/vector"
)

var (
	i64Gt    func([]int64, []int64, []int64) []int64
	f64Gt    func([]float64, []float64, []int64) []int64
	i64GtOne func(int64, []int64, []int64) []int64
	f64GtOne func(float64, []float64, []int64) []int64

	sGt    func(*vector.Bytes, *vector.Bytes, []int64) []int64
	sGtOne func([]byte, *vector.Bytes, []int64) []int64
)

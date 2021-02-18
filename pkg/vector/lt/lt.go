package lt

import "matrixbase/pkg/container/vector"

var (
	i64Lt    func([]int64, []int64, []int64) []int64
	f64Lt    func([]float64, []float64, []int64) []int64
	i64LtOne func(int64, []int64, []int64) []int64
	f64LtOne func(float64, []float64, []int64) []int64

	sLt    func(*vector.Bytes, *vector.Bytes, []int64) []int64
	sLtOne func([]byte, *vector.Bytes, []int64) []int64
)

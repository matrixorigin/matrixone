package ne

import "matrixbase/pkg/container/vector"

var (
	i64NeOne func(int64, []int64, []int64) []int64
	i64Ne    func([]int64, []int64, []int64) []int64

	f64NeOne func(float64, []float64, []int64) []int64
	f64Ne    func([]float64, []float64, []int64) []int64

	bNeOne func(bool, []bool, []int64) []int64
	bNe    func([]bool, []bool, []int64) []int64

	sNeOne func([]byte, *vector.Bytes, []int64) []int64
	sNe    func(*vector.Bytes, *vector.Bytes, []int64) []int64
)

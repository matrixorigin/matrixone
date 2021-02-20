package le

import (
	"matrixbase/pkg/container/vector"
)

var (
	i64Le    func([]int64, []int64, []int64) []int64
	f64Le    func([]float64, []float64, []int64) []int64
	i64LeOne func(int64, []int64, []int64) []int64
	f64LeOne func(float64, []float64, []int64) []int64

	sLe    func(*vector.Bytes, *vector.Bytes, []int64) []int64
	sLeOne func([]byte, *vector.Bytes, []int64) []int64
)

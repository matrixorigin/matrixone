package eq

import (
	"matrixbase/pkg/container/vector"
)

var (
	i64EqOne func(int64, []int64, []int64) []int64
	i64Eq    func([]int64, []int64, []int64) []int64

	f64EqOne func(float64, []float64, []int64) []int64
	f64Eq    func([]float64, []float64, []int64) []int64

	bEqOne func(bool, []bool, []int64) []int64
	bEq    func([]bool, []bool, []int64) []int64

	sEqOne func([]byte, *vector.Bytes, []int64) []int64
	sEq    func(*vector.Bytes, *vector.Bytes, []int64) []int64
)

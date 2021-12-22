package year

import "github.com/matrixorigin/matrixone/pkg/container/types"

var (
	year func([]types.Date, []int32) []int32
)

func init() {
	year = yearPure
}

func Year(xs []types.Date, rs []int32) []int32 {
	return year(xs, rs)
}

func yearPure(xs []types.Date, rs []int32) []int32 {
	for i, x := range xs {
		rs[i] = x.Year()
	}
	return rs
}

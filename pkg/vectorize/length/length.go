package length

import (
	"matrixone/pkg/container/types"
)

var (
	StrLength func(*types.Bytes, []int64) []int64
)

func strLength(xs *types.Bytes, rs []int64) []int64 {
	for i, n := range xs.Lengths {
		rs[i] = int64(n)
	}
	return rs
}

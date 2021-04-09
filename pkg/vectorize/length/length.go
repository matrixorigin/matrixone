package length

import (
	"matrixone/pkg/container/types"

	"golang.org/x/sys/cpu"
)

var (
	strLength func(*types.Bytes, []int64) []int64
)

func init() {
	if cpu.X86.HasAVX512 {
		strLength = strLengthAvx512
	} else if cpu.X86.HasAVX2 {
		strLength = strLengthAvx2
	} else {
		strLength = strLengthPure
	}
}

func StrLength(xs *types.Bytes, rs []int64) []int64 {
	return strLength(xs, rs)
}

func strLengthAvx2(xs *types.Bytes, rs []int64) []int64 {
	strLengthAvx2Asm(xs.Lengths, rs)
	return rs
}

func strLengthAvx512(xs *types.Bytes, rs []int64) []int64 {
	strLengthAvx512Asm(xs.Lengths, rs)
	return rs
}

func strLengthPure(xs *types.Bytes, rs []int64) []int64 {
	for i, n := range xs.Lengths {
		rs[i] = int64(n)
	}
	return rs
}

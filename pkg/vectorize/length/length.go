package length

import (
	"matrixbase/pkg/container/types"

	"golang.org/x/sys/cpu"
)

var (
	strLength func(*types.Bytes, []int64) []int64
)

func init() {
	if cpu.X86.HasAVX2 {
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
	lengths := xs.Lengths
	n := len(lengths) / 4
	strLengthAvx2Asm(lengths, rs)
	for i, j := n*4, len(lengths); i < j; i++ {
		rs[i] = int64(lengths[i])
	}
	return rs
}

func strLengthAvx512(xs *types.Bytes, rs []int64) []int64 {
	lengths := xs.Lengths
	n := len(lengths) / 8
	strLengthAvx512Asm(lengths, rs)
	for i, j := n*8, len(lengths); i < j; i++ {
		rs[i] = int64(lengths[i])
	}
	return rs
}

func strLengthPure(xs *types.Bytes, rs []int64) []int64 {
	for i, n := range xs.Lengths {
		rs[i] = int64(n)
	}
	return rs
}

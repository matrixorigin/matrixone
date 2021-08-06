// +build amd64

package length

import (
	"matrixone/pkg/container/types"

	"golang.org/x/sys/cpu"
)

func strLengthAvx2Asm(x []uint32, r []int64)
func strLengthAvx512Asm(x []uint32, r []int64)

func init() {
	if cpu.X86.HasAVX512 {
		StrLength = strLengthAvx512
	} else if cpu.X86.HasAVX2 {
		StrLength = strLengthAvx2
	} else {
		StrLength = strLength
	}
}

func strLengthAvx2(xs *types.Bytes, rs []int64) []int64 {
	strLengthAvx2Asm(xs.Lengths, rs)
	return rs
}

func strLengthAvx512(xs *types.Bytes, rs []int64) []int64 {
	strLengthAvx512Asm(xs.Lengths, rs)
	return rs
}

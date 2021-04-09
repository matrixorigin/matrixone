package fastmap

import "matrixone/pkg/internal/cpu"

func findAvx([]uint64, uint64) int

var (
	Find func([]uint64, uint64) int
)

func init() {
	if cpu.X86.HasAVX2 {
		Find = findAvx
	} else {
		Find = findPure
	}
}

func findPure(xs []uint64, v uint64) int {
	for i, x := range xs {
		if x == v {
			return i
		}
	}
	return -1
}

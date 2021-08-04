// +build amd64

package fastmap

import "matrixone/pkg/internal/cpu"

func findAvx2([]uint64, uint64) int

func init() {
	if cpu.X86.HasAVX2 {
		Find = findAvx2
	} else {
		Find = find
	}
}

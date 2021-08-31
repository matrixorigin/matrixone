//go:build amd64
// +build amd64

package fastmap

import "golang.org/x/sys/cpu"

func findAvx2Asm(x []uint64, y uint64) int

func init() {
	if cpu.X86.HasAVX2 {
		Find = findAvx2Asm
	} else {
		Find = find
	}
}

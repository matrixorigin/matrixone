package lengthutf8

import (
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"golang.org/x/sys/cpu"
)

func countUTF8CodePointsAvx2(s []byte, threshold []byte) uint64

var threshold []byte

func init() {
	threshold = []byte{
		0xbf, 0xbf, 0xbf, 0xbf, 0xbf, 0xbf, 0xbf, 0xbf,
		0xbf, 0xbf, 0xbf, 0xbf, 0xbf, 0xbf, 0xbf, 0xbf,
		0xbf, 0xbf, 0xbf, 0xbf, 0xbf, 0xbf, 0xbf, 0xbf,
		0xbf, 0xbf, 0xbf, 0xbf, 0xbf, 0xbf, 0xbf, 0xbf,
	}
	if cpu.X86.HasAVX2 {
		StrLengthUTF8 = strLengthUTF8Avx2
	}
}

func strLengthUTF8Avx2(xs *types.Bytes, rs []uint64) []uint64 {
	for i := range xs.Lengths {
		x := xs.Get(int64(i))
		rs[i] = countUTF8CodePointsAvx2(x, threshold)
	}
	return rs
}

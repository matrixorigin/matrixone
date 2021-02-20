package length

import "matrixbase/pkg/container/vector"

var (
	bytesLength func(*vector.Bytes, []int64) []int64
)

func init() {
	bytesLength = bytesLengthPure
}

func BytesLength(xs *vector.Bytes, rs []int64) []int64 {
	return bytesLength(xs, rs)
}

func bytesLengthPure(xs *vector.Bytes, rs []int64) []int64 {
	for i, n := range xs.Ns {
		rs[i] = int64(n)
	}
	return rs
}

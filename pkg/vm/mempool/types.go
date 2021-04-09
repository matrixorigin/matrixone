package mempool

import "matrixone/pkg/internal/cpu"

const (
	CountSize = 8
	PageSize  = cpu.CacheLinePadSize
)

var PageOffset int

type Mempool struct {
	maxSize int
	buckets []bucket
}

type bucket struct {
	size  int
	nslot int
	slots [][]byte
}

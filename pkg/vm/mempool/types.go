package mempool

import (
	"matrixone/pkg/internal/cpu"
	"unsafe"
)

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
	cnt   int
	size  int
	nslot int
	slots []unsafe.Pointer
}

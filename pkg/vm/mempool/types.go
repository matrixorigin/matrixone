package mempool

import (
	"matrixone/pkg/internal/cpu"
)

const (
	CountSize = 8
	PageSize  = cpu.CacheLinePadSize
)

var PageOffset int

type Mempool struct {
	classes []class
	minSize int
	maxSize int
}

type class struct {
	size      int
	page      []byte
	pageBegin uintptr
	pageEnd   uintptr
	chunks    []chunk
	head      uint64
}

type chunk struct {
	mem  []byte
	aba  uint32 // reslove ABA problem
	next uint64
}

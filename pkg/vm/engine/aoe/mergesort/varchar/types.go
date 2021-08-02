package varchar

import (
	"bytes"
)

type sortElem struct {
	data []byte
	idx  uint32
}

type sortSlice []sortElem

func (x sortSlice) Less(i, j int) bool {
	return bytes.Compare(x[i].data, x[j].data) < 0
}

func (x sortSlice) Swap(i, j int) { x[i], x[j] = x[j], x[i] }

type heapElem struct {
	data []byte
	src  uint16
	next uint32
}

type heapSlice []heapElem

func (x heapSlice) Less(i, j int) bool {
	return bytes.Compare(x[i].data, x[j].data) < 0
}

func (x heapSlice) Swap(i, j int) { x[i], x[j] = x[j], x[i] }

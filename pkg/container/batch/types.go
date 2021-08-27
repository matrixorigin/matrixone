package batch

import (
	"matrixone/pkg/container/vector"
)

type Info struct {
	Alg int
	Ref uint64
	Wg  *WaitGroup
}

type WaitGroup struct {
}

type Batch struct {
	Ro       bool // true: attrs is read only
	Is       []Info
	SelsData []byte
	Sels     []int64
	Attrs    []string
	Vecs     []*vector.Vector
}

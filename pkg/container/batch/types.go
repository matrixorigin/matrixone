package batch

import (
	"matrixone/pkg/container/vector"
	"matrixone/pkg/vm/process"
)

type Reader interface {
	Read(int64, uint64, string, *process.Process) (*vector.Vector, error)
}

type Info struct {
	Len int64
	Ref uint64
	R   Reader
}

type Batch struct {
	Ro       bool // true: attrs is read only
	Is       []Info
	SelsData []byte
	Sels     []int64
	Attrs    []string
	Vecs     []*vector.Vector
}

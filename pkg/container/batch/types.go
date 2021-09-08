package batch

import (
	"matrixone/pkg/container/vector"
)

type Batch struct {
	Ro       bool // true: attrs is read only
	SelsData []byte
	Sels     []int64
	Attrs    []string
	Vecs     []*vector.Vector
}

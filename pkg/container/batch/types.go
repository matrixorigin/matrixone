package batch

import "matrixbase/pkg/container/vector"

type Batch struct {
	Sels  []int64
	Attrs []string
	Vecs  []*vector.Vector
}

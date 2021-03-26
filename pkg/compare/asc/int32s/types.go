package int32s

import (
	"matrixbase/pkg/container/nulls"
	"matrixbase/pkg/container/vector"
)

type compare struct {
	xs [][]int32
	ns []*nulls.Nulls
	vs []*vector.Vector
}

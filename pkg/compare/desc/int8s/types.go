package int8s

import (
	"matrixbase/pkg/container/nulls"
	"matrixbase/pkg/container/vector"
)

type compare struct {
	xs [][]int8
	ns []*nulls.Nulls
	vs []*vector.Vector
}

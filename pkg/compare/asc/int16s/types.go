package int16s

import (
	"matrixbase/pkg/container/nulls"
	"matrixbase/pkg/container/vector"
)

type compare struct {
	xs [][]int16
	ns []*nulls.Nulls
	vs []*vector.Vector
}

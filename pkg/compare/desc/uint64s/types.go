package uint64s

import (
	"matrixbase/pkg/container/nulls"
	"matrixbase/pkg/container/vector"
)

type compare struct {
	xs [][]uint64
	ns []*nulls.Nulls
	vs []*vector.Vector
}
